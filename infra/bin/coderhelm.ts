#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { DatabaseStack } from "../lib/database-stack";
import { StorageStack } from "../lib/storage-stack";
import { ApiStack } from "../lib/api-stack";
import { WorkerStack } from "../lib/worker-stack";
import { MonitoringStack } from "../lib/monitoring-stack";
import { EmailStack } from "../lib/email-stack";
import { BillingStack } from "../lib/billing-stack";

const app = new cdk.App();

const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT ?? "REDACTED_AWS_ACCOUNT_ID",
  region: process.env.CDK_DEFAULT_REGION ?? "us-east-1",
};

const stage = app.node.tryGetContext("stage") ?? "prod";
const prefix = `coderhelm-${stage}`;

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
  eventsTable: database.eventsTable,
  usersTable: database.usersTable,
  bucket: storage.bucket,
});

const worker = new WorkerStack(app, `${prefix}-worker`, {
  env,
  stage,
  table: database.table,
  runsTable: database.runsTable,
  analyticsTable: database.analyticsTable,
  usersTable: database.usersTable,
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

// --- Monitoring ---

new MonitoringStack(app, `${prefix}-monitoring`, {
  env,
  stage,
  gatewayFunction: api.gatewayFunction,
  workerFunction: worker.workerFunction,
  table: database.table,
  ticketQueue: api.ticketQueue,
});
