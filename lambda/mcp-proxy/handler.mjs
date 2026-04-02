/**
 * MCP Proxy Lambda
 *
 * Spawns an MCP server process via stdio, executes a single tool call or
 * lists available tools, then kills the process and returns.
 *
 * Request body:
 *   action: "list_tools" | "call_tool"
 *   server_id: string          — plugin id from catalog
 *   npx_package: string        — npm package to run (e.g. "@anthropic-ai/mcp-server-figma")
 *   env_vars: Record<string, string>  — env vars to inject (credentials mapped)
 *   tool_name?: string         — required for call_tool
 *   tool_input?: object        — required for call_tool
 *
 * Response:
 *   For list_tools:  { tools: Array<{ name, description, inputSchema }> }
 *   For call_tool:   { result: { content: [...], isError?: boolean } }
 */

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client();
const BUCKET = process.env.BUCKET_NAME || "";
const TOOL_CACHE_PREFIX = "config/mcp-tools";

export async function handler(event) {
  const { action, server_id, npx_package, env_vars, tool_name, tool_input } =
    typeof event === "string" ? JSON.parse(event) : event;

  if (!server_id || !npx_package) {
    return { statusCode: 400, error: "Missing server_id or npx_package" };
  }

  // Build environment — inherit PATH/HOME for npx, add user credentials
  const childEnv = {
    PATH: process.env.PATH,
    HOME: process.env.HOME || "/tmp",
    NODE_ENV: "production",
    ...env_vars,
  };

  // Notion MCP expects OPENAPI_MCP_HEADERS as JSON: {"Authorization":"Bearer <token>","Notion-Version":"2022-06-28"}
  // If the value is a raw token (not valid JSON), wrap it automatically.
  if (childEnv.OPENAPI_MCP_HEADERS) {
    try {
      JSON.parse(childEnv.OPENAPI_MCP_HEADERS);
    } catch {
      childEnv.OPENAPI_MCP_HEADERS = JSON.stringify({
        Authorization: `Bearer ${childEnv.OPENAPI_MCP_HEADERS}`,
        "Notion-Version": "2022-06-28",
      });
    }
  }

  let transport;
  let client;

  try {
    transport = new StdioClientTransport({
      command: "npx",
      args: ["-y", npx_package],
      env: childEnv,
    });

    client = new Client({ name: "coderhelm-proxy", version: "1.0.0" });

    // Connect with a timeout — server must initialize within 90s (npx downloads on cold start)
    await Promise.race([
      client.connect(transport),
      timeout(90_000, `MCP server ${server_id} failed to initialize within 90s`),
    ]);

    if (action === "list_tools") {
      const result = await Promise.race([
        client.listTools(),
        timeout(15_000, "list_tools timed out"),
      ]);

      const tools = (result.tools || []).map((t) => ({
        name: t.name,
        description: t.description || "",
        inputSchema: t.inputSchema || { type: "object", properties: {} },
      }));

      // Cache to S3
      if (BUCKET) {
        try {
          await s3.send(
            new PutObjectCommand({
              Bucket: BUCKET,
              Key: `${TOOL_CACHE_PREFIX}/${server_id}.json`,
              Body: JSON.stringify({ server_id, tools, cached_at: new Date().toISOString() }),
              ContentType: "application/json",
            })
          );
        } catch (e) {
          console.warn("Failed to cache tools to S3:", e.message);
        }
      }

      return { tools };
    }

    if (action === "call_tool") {
      if (!tool_name) {
        return { statusCode: 400, error: "Missing tool_name for call_tool" };
      }

      const result = await Promise.race([
        client.callTool({ name: tool_name, arguments: tool_input || {} }),
        timeout(60_000, `Tool ${tool_name} timed out after 60s`),
      ]);

      return {
        result: {
          content: result.content || [],
          isError: result.isError || false,
        },
      };
    }

    return { statusCode: 400, error: `Unknown action: ${action}` };
  } catch (err) {
    console.error(`MCP proxy error [${server_id}/${action}]:`, err);
    return {
      statusCode: 500,
      error: err.message || "MCP proxy internal error",
    };
  } finally {
    try {
      await client?.close();
    } catch {
      // ignore cleanup errors
    }
  }
}

function timeout(ms, message) {
  return new Promise((_, reject) => setTimeout(() => reject(new Error(message)), ms));
}
