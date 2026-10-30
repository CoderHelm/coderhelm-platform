import * as cdk from "aws-cdk-lib";
import { ResolvedConfig } from "./config";
import { DatabaseStack } from "./database-stack";
import { StorageStack } from "./storage-stack";
import { ApiStack } from "./api-stack";
import { WorkerStack } from "./worker-stack";
import { MonitoringStack } from "./monitoring-stack";
import { EmailStack } from "./email-stack";
import { LogAnalyzerStack } from "./log-analyzer-stack";
import { StreamingStack } from "./streaming-stack";
import { WebStack } from "./web-stack";
import { UpdatesStack } from "./updates-stack";
import { AccessStack, harden, stackBase } from "./hardening";

/** Every stack of one deployment, built from its config. */
export function buildApp(app: cdk.App, cfg: ResolvedConfig, opts: { accessStack?: boolean } = {}) {
  const prefix = cfg.prefix;
  const stage = cfg.stage;

  // --- Core Infrastructure ---

  const database = new DatabaseStack(app, `${prefix}-database`, { ...stackBase(cfg), stage, cfg });

  const storage = new StorageStack(app, `${prefix}-storage`, { ...stackBase(cfg), stage, cfg });

  // --- Compute ---

  const api = new ApiStack(app, `${prefix}-api`, {
    ...stackBase(cfg),
    stage,
    cfg,
    table: database.table,
    eventsTable: database.eventsTable,
    usersTable: database.usersTable,
    jiraTokensTable: database.jiraTokensTable,
    plansTable: database.plansTable,
    jiraConfigTable: database.jiraConfigTable,
    reposTable: database.reposTable,
    settingsTable: database.settingsTable,
    secretsKey: database.secretsKey,
    infraTable: database.infraTable,
    billingTable: database.billingTable,
    bannersTable: database.bannersTable,
    mcpConfigsTable: database.mcpConfigsTable,
    awsInsightsTable: database.awsInsightsTable,
    testsTable: database.testsTable,
    waitlistTable: database.waitlistTable,
    bucket: storage.bucket,
  });

  const worker = new WorkerStack(app, `${prefix}-worker`, {
    ...stackBase(cfg),
    stage,
    cfg,
    table: database.table,
    usersTable: database.usersTable,
    plansTable: database.plansTable,
    jiraConfigTable: database.jiraConfigTable,
    reposTable: database.reposTable,
    settingsTable: database.settingsTable,
    infraTable: database.infraTable,
    billingTable: database.billingTable,
    mcpConfigsTable: database.mcpConfigsTable,
    tracesTable: database.tracesTable,
    checkpointsTable: database.checkpointsTable,
    eventsTable: database.eventsTable,
    testsTable: database.testsTable,
    bucket: storage.bucket,
    ticketQueue: api.ticketQueue,
    ciFixQueue: api.ciFixQueue,
    feedbackQueue: api.feedbackQueue,
    webhookQueue: api.webhookQueue,
    dlq: api.dlq,
    mcpProxyFunction: api.mcpProxyFunction,
    encryptionKey: database.encryptionKey,
    secretsKey: database.secretsKey,
  });

  // --- Streaming (SSE via Lambda Function URL + Web Adapter) ---
  // Created before email and log analyzer so a single-origin web stack can
  // sit between them (those two link to the dashboard URL it creates).

  const streaming = new StreamingStack(app, `${prefix}-streaming`, {
    ...stackBase(cfg),
    stage,
    cfg,
    table: database.table,
    plansTable: database.plansTable,
    reposTable: database.reposTable,
    settingsTable: database.settingsTable,
    mcpConfigsTable: database.mcpConfigsTable,
    bucket: storage.bucket,
    mcpProxyFunction: api.mcpProxyFunction,
    secretsKey: database.secretsKey,
    ticketQueue: api.ticketQueue,
    webhookQueue: api.webhookQueue,
  });

  // --- Web (single-origin only): dashboard + one URL for everything ---

  let web: WebStack | undefined;
  if (cfg.layout === "single-origin") {
    web = new WebStack(app, `${prefix}-web`, {
      ...stackBase(cfg),
      cfg,
      httpApi: api.httpApi,
      streamingFunctionUrl: streaming.functionUrl,
      crossRegionReferences: cfg.region !== "us-east-1" && !!cfg.singleOrigin?.hostedZone && !cfg.singleOrigin.certificateArn,
    });
  }

  /** The dashboard's URL: static when known, else the web stack's CloudFront URL. */
  const appUrl = cfg.appUrl ?? web!.publicUrl;

  // --- Email ---

  new EmailStack(app, `${prefix}-email`, {
    ...stackBase(cfg),
    stage,
    cfg,
    appUrl,
    gatewayFunction: api.gatewayFunction,
    workerFunction: worker.workerFunction,
    emailEvents: cfg.layout !== "split",
  });

  // --- Updates (self-hosted installs) ---

  let updates: UpdatesStack | undefined;
  if (cfg.updates.enabled) {
    updates = new UpdatesStack(app, `${prefix}-updates`, {
      ...stackBase(cfg),
      cfg,
      appUrl,
      settingsTable: database.settingsTable,
    });
  }

  // --- Monitoring ---

  new MonitoringStack(app, `${prefix}-monitoring`, {
    ...stackBase(cfg),
    stage,
    cfg,
    gatewayFunction: api.gatewayFunction,
    workerFunction: worker.workerFunction,
    table: database.table,
    ticketQueue: api.ticketQueue,
  });

  // --- Log Analyzer ---

  if (cfg.integrations.logAnalyzer) {
    new LogAnalyzerStack(app, `${prefix}-log-analyzer`, {
      ...stackBase(cfg),
      stage,
      cfg,
      appUrl,
      awsInsightsTable: database.awsInsightsTable,
      settingsTable: database.settingsTable,
      secretsKey: database.secretsKey,
      webhookQueue: api.webhookQueue,
    });
  }

  // --- A hardened install (config `access`, infra/lib/hardening.ts) ---

  if (cfg.access) {
    for (const s of app.node.children) if (s instanceof cdk.Stack) harden(s, cfg);
    // The admin's own stack (an install hardened from the CLI); updates skip it.
    const withAccess = opts.accessStack ?? process.env.CODERHELM_ACCESS_STACK !== "skip";
    if (cfg.access.owner === "app" && withAccess) new AccessStack(app, cfg.access.accessStackName, { cfg });
  }

  return { database, storage, api, worker, streaming, web, updates };
}
