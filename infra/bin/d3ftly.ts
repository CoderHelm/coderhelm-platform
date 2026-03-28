#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { DatabaseStack } from "../lib/database-stack";
import { StorageStack } from "../lib/storage-stack";
import { ApiStack } from "../lib/api-stack";
import { WorkerStack } from "../lib/worker-stack";
import { FrontendStack } from "../lib/frontend-stack";
import { MonitoringStack } from "../lib/monitoring-stack";
import { EmailStack } from "../lib/email-stack";
import { BillingStack } from "../lib/billing-stack";
import { WafStack } from "../lib/waf-stack";

const app = new cdk.App();

const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION ?? "us-east-1",
};

const stage = app.node.tryGetContext("stage") ?? "prod";
const prefix = `d3ftly-${stage}`;

// --- Core Infrastructure ---

const database = new DatabaseStack(app, `${prefix}-database`, {
  env,
  stage,
});

const storage = new StorageStack(app, `${prefix}-storage`, {
  env,
  stage,
});

// --- Compute ---

const api = new ApiStack(app, `${prefix}-api`, {
  env,
  stage,
  table: database.table,
  runsTable: database.runsTable,
  analyticsTable: database.analyticsTable,
  bucket: storage.bucket,
});

const worker = new WorkerStack(app, `${prefix}-worker`, {
  env,
  stage,
  table: database.table,
  runsTable: database.runsTable,
  analyticsTable: database.analyticsTable,
  bucket: storage.bucket,
  ticketQueue: api.ticketQueue,
  ciFixQueue: api.ciFixQueue,
  feedbackQueue: api.feedbackQueue,
});

// --- Email ---

new EmailStack(app, `${prefix}-email`, {
  env,
  stage,
  gatewayFunction: api.gatewayFunction,
  workerFunction: worker.workerFunction,
});

// --- Billing ---

const billing = new BillingStack(app, `${prefix}-billing`, {
  env,
  stage,
  gatewayFunction: api.gatewayFunction,
});

// Inject invoice bucket name into gateway environment
api.gatewayFunction.addEnvironment(
  "INVOICE_BUCKET_NAME",
  billing.invoiceBucket.bucketName
);

// --- WAF (must be us-east-1 for CloudFront) ---

const waf = new WafStack(app, `${prefix}-waf`, {
  env: { account: env.account, region: "us-east-1" },
  stage,
  target: "api",
  crossRegionReferences: true,
});

// --- Frontend ---

const frontend = new FrontendStack(app, `${prefix}-frontend`, {
  env,
  stage,
  webAclArn: waf.webAclArn,
  crossRegionReferences: true,
});

// --- Monitoring ---

new MonitoringStack(app, `${prefix}-monitoring`, {
  env,
  stage,
  gatewayFunction: api.gatewayFunction,
  workerFunction: worker.workerFunction,
  table: database.table,
  ticketQueue: api.ticketQueue,
});
