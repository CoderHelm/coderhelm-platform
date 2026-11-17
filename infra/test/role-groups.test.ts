/**
 * The role stack's read groups (infra/cfn/coderhelm-log-reader.yaml and its
 * update template): the exact statements each parameter adds, as
 * CloudFormation resolves them. Reads only; the WAF log group's one write
 * is the Athena results location, held there by a deny.
 */
import { strict as assert } from "assert";
import { test } from "node:test";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import { CfnInclude } from "aws-cdk-lib/cloudformation-include";
import { decideConditions } from "../lib/own-account-read";
import { repoPath } from "../lib/paths";

const KEY = "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab";
const OFF: Record<string, boolean> = {
  ReadsKnowledgeBases: false,
  ReadsEveryKnowledgeBase: false,
  ReadsImportBuckets: false,
  SyncsKnowledgeBases: false,
  SyncsEveryKnowledgeBase: false,
  ReadsInfrastructure: false,
  ReadsInspector: false,
  ReadsHealth: false,
  AppliesMaintenance: false,
  ReadsWafLogs: false,
  QueriesWafLogs: false,
  NoWafLogQueries: false,
  DecryptsWafLogs: false,
  ReadsDatabases: false,
  ReadsWarehouses: false,
};

/** The update stack's statements for these parameters and conditions, by Sid. */
function statements(parameters: Record<string, string>, on: Record<string, boolean>): Record<string, any> {
  const stack = new cdk.Stack(new cdk.App(), "T");
  const inc = new CfnInclude(stack, "I", {
    templateFile: repoPath("infra/cfn/coderhelm-log-reader-update.yaml"),
    parameters: { RoleName: "CoderHelmLogReader", ...parameters },
  });
  const p = inc.getResource("CoderHelmReadOnlyUpdate") as iam.CfnPolicy;
  const list = decideConditions(stack.resolve(p.policyDocument).Statement, { ...OFF, ...on }) as any[];
  return Object.fromEntries(list.map((s) => [s.Sid, s]));
}

const ACCOUNT = { Ref: "AWS::AccountId" };
const sub = (s: string) => ({ "Fn::Sub": s });

test("the WAF log group with a results location and a key: exact statements", () => {
  const all = statements(
    { WafLogs: "on", WafLogResults: "aws-waf-logs-acme/athena-results/", WafLogKey: KEY },
    { ReadsWafLogs: true, QueriesWafLogs: true, DecryptsWafLogs: true },
  );
  const waf = Object.fromEntries(Object.entries(all).filter(([k]) => /Waf|Query|NoWrites|NeverChange/.test(k)));
  assert.deepEqual(Object.keys(waf).sort(), [
    "DecryptWafLogsThroughS3",
    "FindQueryResults",
    "FindWafLogTables",
    "FindWafLogWorkgroups",
    "ListWafLogBuckets",
    "NeverChangeWafOrItsLogs",
    "NoWritesButQueryResults",
    "QueryWafLogs",
    "ReadWafLogObjects",
    "WriteQueryResultsOnly",
  ]);
  assert.deepEqual(waf.ReadWafLogObjects, { Sid: "ReadWafLogObjects", Effect: "Allow", Action: ["s3:GetObject"], Resource: "arn:aws:s3:::aws-waf-logs-*/*" });
  assert.deepEqual(waf.ListWafLogBuckets, { Sid: "ListWafLogBuckets", Effect: "Allow", Action: ["s3:ListBucket", "s3:GetBucketLocation"], Resource: "arn:aws:s3:::aws-waf-logs-*" });
  assert.deepEqual(waf.FindWafLogTables.Action, ["glue:GetDatabases", "glue:GetDatabase", "glue:GetTables", "glue:GetTable", "glue:GetPartitions"]);
  assert.deepEqual(waf.FindWafLogTables.Resource, [
    sub("arn:aws:glue:*:${AWS::AccountId}:catalog"),
    sub("arn:aws:glue:*:${AWS::AccountId}:database/*"),
    sub("arn:aws:glue:*:${AWS::AccountId}:table/*/*"),
  ]);
  assert.deepEqual(waf.FindWafLogWorkgroups, { Sid: "FindWafLogWorkgroups", Effect: "Allow", Action: ["athena:ListWorkGroups", "athena:GetWorkGroup"], Resource: "*" });
  assert.deepEqual(waf.QueryWafLogs.Action, ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"]);
  // The one write: the results location only, and denied everywhere else.
  const results = sub("arn:aws:s3:::aws-waf-logs-acme/athena-results/*");
  assert.deepEqual(waf.WriteQueryResultsOnly, { Sid: "WriteQueryResultsOnly", Effect: "Allow", Action: ["s3:PutObject", "s3:GetObject"], Resource: results });
  assert.deepEqual(waf.NoWritesButQueryResults, { Sid: "NoWritesButQueryResults", Effect: "Deny", Action: ["s3:PutObject"], NotResource: results });
  assert.equal(JSON.stringify(waf.FindQueryResults.Resource).includes("aws-waf-logs-acme"), true);
  // KMS: that key, through S3 only.
  assert.deepEqual(waf.DecryptWafLogsThroughS3, {
    Sid: "DecryptWafLogsThroughS3",
    Effect: "Allow",
    Action: ["kms:Decrypt"],
    Resource: KEY,
    Condition: { StringLike: { "kms:ViaService": "s3.*.amazonaws.com" } },
  });
  assert.equal(waf.NeverChangeWafOrItsLogs.Effect, "Deny");
  for (const a of ["wafv2:Update*", "athena:Delete*", "glue:Update*", "glue:GetConnection*", "s3:Delete*", "s3:PutBucket*"]) {
    assert.ok(waf.NeverChangeWafOrItsLogs.Action.includes(a), a);
  }
  // Every allow in every group is a read, but the results write.
  const reads = /^(Get|List|Describe|BatchGet|Retrieve|Search|Filter|Simulate|Lookup)|^(StartQuery|StopQuery|StartQueryExecution|StopQueryExecution|GetQueryExecution|InvokeRestApi|BeginTransaction|ExecuteStatement|RollbackTransaction|CancelStatement|GetClusterCredentials)$/;
  const groups = statements(
    { WafLogs: "on", WafLogResults: "aws-waf-logs-acme/athena-results/", WafLogKey: KEY, DatabaseClusters: "arn:aws:rds:us-east-1:111122223333:cluster:app-db", Warehouses: "analytics" },
    { ReadsWafLogs: true, QueriesWafLogs: true, DecryptsWafLogs: true, ReadsDatabases: true, ReadsWarehouses: true },
  );
  for (const s of Object.values(groups)) {
    if (s.Effect !== "Allow") continue;
    for (const a of [].concat(s.Action) as string[]) {
      const verb = a.split(":")[1];
      if (a === "s3:PutObject") assert.equal(s.Sid, "WriteQueryResultsOnly");
      else if (a === "kms:Decrypt") assert.ok(s.Condition?.StringLike?.["kms:ViaService"], s.Sid);
      else assert.ok(reads.test(verb), `${a} in ${s.Sid}`);
    }
  }
});

test("without a results location the WAF log group writes nothing; off, it adds nothing", () => {
  const bare = statements({ WafLogs: "on" }, { ReadsWafLogs: true, NoWafLogQueries: true });
  assert.ok(bare.ReadWafLogObjects && bare.FindWafLogTables && bare.FindWafLogWorkgroups);
  for (const no of ["QueryWafLogs", "WriteQueryResultsOnly", "FindQueryResults", "NoWritesButQueryResults", "DecryptWafLogsThroughS3"]) assert.equal(bare[no], undefined, no);
  assert.deepEqual(bare.NoWrites, { Sid: "NoWrites", Effect: "Deny", Action: ["s3:PutObject"], Resource: "*" });
  const off = statements({}, {});
  for (const k of Object.keys(off)) assert.ok(!/Waf|Query|NoWrites|NeverChange|Database|Warehouse|Commit|OwnResults|ReadOnlyUser/.test(k), k);
});

test("databases and warehouses: only those named, never a commit", () => {
  const s = statements(
    { DatabaseClusters: "arn:aws:rds:us-east-1:111122223333:cluster:app-db,arn:aws:rds:us-east-1:111122223333:cluster:b", Warehouses: "analytics,marts" },
    { ReadsDatabases: true, ReadsWarehouses: true },
  );
  assert.deepEqual(s.QueryChosenDatabases.Resource, ["arn:aws:rds:us-east-1:111122223333:cluster:app-db", "arn:aws:rds:us-east-1:111122223333:cluster:b"]);
  assert.deepEqual(s.ReadOnlyUserPassword.Resource, sub("arn:aws:secretsmanager:*:${AWS::AccountId}:secret:coderhelm-readonly-*"));
  assert.deepEqual(s.NeverCommit, { Sid: "NeverCommit", Effect: "Deny", Action: ["rds-data:CommitTransaction", "rds-data:BatchExecuteStatement"], Resource: "*" });
  const users = JSON.stringify(s.SignInAsTheReadOnlyUser.Resource);
  assert.ok(users.includes("dbuser:${Ids}/coderhelm_readonly") && users.includes("[\"analytics\",\"marts\"]"), users);
  assert.deepEqual(s.ReadOwnResults.Condition, { StringEquals: { "redshift-data:statement-owner-iam-userid": "${aws:userid}" } });
  assert.equal(s.NothingElseInTheWarehouse.Effect, "Deny");
  void ACCOUNT;
});
