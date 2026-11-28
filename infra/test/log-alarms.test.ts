// Alarms that can fire. The services catch and log every error, so Lambda's
// Errors metric stayed at 0 while they failed, and no alarm moved. Every
// install alarms on the structured lines instead, each as a share of the
// function's traffic (metric math against its invocations), never a count,
// into the alerts topic:
//   - ERROR lines of the gateway, the worker, Chat's streaming function and the updater;
//   - GitHub rate-limit lines of the worker;
//   - Chat questions that failed, of every question asked.
//
//   npm test

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as cdk from "aws-cdk-lib";
import { Match, Template } from "aws-cdk-lib/assertions";
import { buildApp } from "../lib/app";
import { resolveConfig } from "../lib/config";
import { LOG_ALARMS, LOG_EVENTS } from "../lib/monitoring-stack";
import { repoPath } from "../lib/paths";

const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chlogalarms-"));
for (const d of ["gateway", "worker", "mcp"]) {
  fs.mkdirSync(path.join(STUBS, d), { recursive: true });
  fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
}
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway");
process.env.WORKER_ZIP = path.join(STUBS, "worker");
process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");

let cached: Template | undefined;
function monitoring(): Template {
  if (cached) return cached;
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chout-")) });
  buildApp(app, resolveConfig({ account: "111122223333", region: "us-west-2", stage: "prod", email: { from: "coderhelm@example.org" }, admins: ["owner@example.org"] }));
  const stack = app.node.children.find((s) => s instanceof cdk.Stack && s.stackName === "coderhelm-prod-monitoring") as cdk.Stack;
  cached = Template.fromStack(stack);
  return cached;
}

function alarm(name: string): any {
  const found = Object.values(monitoring().findResources("AWS::CloudWatch::Alarm", { Properties: { AlarmName: name } }));
  assert.equal(found.length, 1, `one alarm ${name}`);
  return (found[0] as any).Properties;
}

const str = (v: unknown) => JSON.stringify(v);

/** The alarm is metric math over a log filter's count and a measure of traffic, into the alerts topic. */
function isShareOfTraffic(p: any, traffic: RegExp) {
  const math = p.Metrics.find((m: any) => m.Expression);
  assert.ok(math, "metric math");
  assert.ok(math.ReturnData !== false);
  assert.match(math.Expression, /100 \* /, "a percentage");
  assert.match(math.Expression, /^IF\(/, "no division by zero when there is no traffic");
  const inputs = p.Metrics.filter((m: any) => m.MetricStat);
  assert.ok(inputs.every((m: any) => m.ReturnData === false));
  assert.ok(inputs.some((m: any) => traffic.test(str(m.MetricStat.Metric))), str(inputs));
  assert.equal(p.ComparisonOperator, "GreaterThanOrEqualToThreshold");
  assert.equal(p.DatapointsToAlarm, LOG_ALARMS.datapointsToAlarm);
  assert.equal(p.TreatMissingData, "notBreaching");
  assert.ok(str(p.AlarmActions).includes("AlertTopic"), "the alerts topic");
}

test("every service's ERROR lines alarm as a share of its invocations", () => {
  for (const name of ["gateway", "worker", "streaming", "updater"]) {
    const p = alarm(`coderhelm-prod-${name}-error-lines`);
    isShareOfTraffic(p, /"MetricName":"Invocations"/);
    assert.equal(p.Threshold, LOG_ALARMS.errorLinesPercent);
  }
  const filters = Object.values(monitoring().findResources("AWS::Logs::MetricFilter")).map((r: any) => r.Properties);
  // The Rust services' JSON lines, and the Python updater's text lines.
  const errorFilters = filters.filter((f) => f.FilterPattern === '{ $.level = "ERROR" }');
  assert.equal(errorFilters.length, 3, str(filters.map((f) => f.FilterPattern)));
  const updater = filters.filter((f) => f.FilterPattern === '"[ERROR]"');
  assert.equal(updater.length, 1);
  assert.ok(str(updater[0].LogGroupName).includes("UpdaterFunctionName") || str(updater[0].LogGroupName).includes("Updater"), str(updater[0].LogGroupName));
  for (const f of [...errorFilters, ...updater]) assert.equal(f.MetricTransformations[0].DefaultValue, 0);
});

test("GitHub rate limits alarm as a share of the worker's invocations", () => {
  const p = alarm("coderhelm-prod-github-rate-limited");
  isShareOfTraffic(p, /"MetricName":"Invocations"/);
  assert.equal(p.Threshold, LOG_ALARMS.rateLimitedPercent);
  monitoring().hasResourceProperties("AWS::Logs::MetricFilter", {
    FilterPattern: `{ $.event = "${LOG_EVENTS.rateLimited}" }`,
    LogGroupName: Match.anyValue(),
  });
});

test("Chat questions that failed alarm as a share of every question", () => {
  const p = alarm("coderhelm-prod-chat-answer-failed");
  isShareOfTraffic(p, /"MetricName":"ChatAnswered"/);
  assert.match(p.Metrics.find((m: any) => m.Expression).Expression, /failed \+ answered/);
  for (const outcome of ["answered", "failed"]) {
    monitoring().hasResourceProperties("AWS::Logs::MetricFilter", {
      FilterPattern: `{ ($.event = "${LOG_EVENTS.chatAnswer}") && ($.outcome = "${outcome}") }`,
    });
  }
});

test("the lines the alarms read are the lines the services write", () => {
  const budget = fs.readFileSync(repoPath("services/common/src/github_budget.rs"), "utf8");
  assert.ok(budget.includes(`RATE_LIMITED_EVENT: &str = "${LOG_EVENTS.rateLimited}"`));
  const github = fs.readFileSync(repoPath("services/worker/src/clients/github.rs"), "utf8");
  assert.ok(github.includes("event = common::github_budget::RATE_LIMITED_EVENT"));
  const brain = fs.readFileSync(repoPath("services/gateway/src/routes/brain.rs"), "utf8");
  assert.ok(brain.includes(`CHAT_ANSWER_EVENT: &str = "${LOG_EVENTS.chatAnswer}"`));
  for (const outcome of ['outcome = "answered"', 'outcome = "failed"']) assert.ok(brain.includes(outcome), outcome);
  // The updater logs Lambda's text lines through the root logger.
  assert.ok(fs.readFileSync(repoPath("lambda/updater/handler.py"), "utf8").includes("logger = logging.getLogger()"));
  // The services log JSON with the event's fields at the top, which the filters read.
  for (const svc of ["gateway", "worker"]) {
    assert.ok(fs.readFileSync(repoPath(`services/${svc}/src/main.rs`), "utf8").includes(".json().flatten_event(true)"), svc);
  }
});
