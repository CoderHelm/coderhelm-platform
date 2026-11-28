import * as cdk from "aws-cdk-lib";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as sns from "aws-cdk-lib/aws-sns";
import * as actions from "aws-cdk-lib/aws-cloudwatch-actions";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as logs from "aws-cdk-lib/aws-logs";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";

/**
 * The structured lines the services write for these alarms
 * (services/common/src/github_budget.rs RATE_LIMITED_EVENT,
 * services/gateway/src/routes/brain.rs CHAT_ANSWER_EVENT). The services catch
 * and log every error, so Lambda's own Errors metric stays at 0 while they
 * fail: these alarms read the log lines instead.
 */
export const LOG_EVENTS = { rateLimited: "github_rate_limited", chatAnswer: "chat_answer" } as const;

/**
 * Each alarm is a share of the traffic, never a count: an install with ten
 * questions a day and one with ten thousand alarm on the same failing share.
 * The share is held over most of an hour, so one bad minute doesn't page.
 */
export const LOG_ALARMS = {
  period: cdk.Duration.minutes(15),
  evaluationPeriods: 4,
  datapointsToAlarm: 3,
  /** ERROR lines per 100 invocations of the function. */
  errorLinesPercent: 10,
  /** GitHub rate-limit lines per 100 worker invocations. */
  rateLimitedPercent: 20,
  /** Chat questions that failed, of every question asked. */
  chatFailedPercent: 20,
} as const;

interface MonitoringStackProps extends cdk.StackProps {
  stage: string;
  cfg: ResolvedConfig;
  gatewayFunction: lambda.Function;
  workerFunction: lambda.Function;
  /** Chat's streams (infra/lib/streaming-stack.ts). */
  streamingFunction: lambda.Function;
  /** The self-host updater, when the install has one. */
  updaterFunction?: lambda.Function;
  table: dynamodb.TableV2;
  ticketQueue: sqs.Queue;
}

export class MonitoringStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: MonitoringStackProps) {
    super(scope, id, props);

    const prefix = `${props.cfg.prefix}`;

    // Alert topic
    const alertTopic = new sns.Topic(this, "AlertTopic", {
      topicName: `${prefix}-alerts`,
    });

    // Gateway error alarm
    const gatewayErrors = props.gatewayFunction.metricErrors({
      period: cdk.Duration.minutes(5),
    });
    const gatewayAlarm = gatewayErrors.createAlarm(this, "GatewayErrors", {
      alarmName: `${prefix}-gateway-errors`,
      threshold: 5,
      evaluationPeriods: 2,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    gatewayAlarm.addAlarmAction(new actions.SnsAction(alertTopic));

    // Worker error alarm
    const workerErrors = props.workerFunction.metricErrors({
      period: cdk.Duration.minutes(5),
    });
    const workerAlarm = workerErrors.createAlarm(this, "WorkerErrors", {
      alarmName: `${prefix}-worker-errors`,
      threshold: 3,
      evaluationPeriods: 2,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    workerAlarm.addAlarmAction(new actions.SnsAction(alertTopic));

    // DLQ depth alarm (messages failing repeatedly)
    const dlqAlarm = new cloudwatch.Alarm(this, "DlqAlarm", {
      alarmName: `${prefix}-dlq-depth`,
      metric: new cloudwatch.Metric({
        namespace: "AWS/SQS",
        metricName: "ApproximateNumberOfMessagesVisible",
        dimensionsMap: { QueueName: `${prefix}-dlq` },
        period: cdk.Duration.minutes(5),
      }),
      threshold: 1,
      evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    dlqAlarm.addAlarmAction(new actions.SnsAction(alertTopic));

    // Reviews that didn't finish (a model or GitHub service error). Every
    // failed attempt is counted; an alarm fires when every retry of one failed
    // and the PR was told (the stuck-review sweep tries it once more later).
    const workerLogs = logs.LogGroup.fromLogGroupName(
      this,
      "WorkerLogs",
      `/aws/lambda/${prefix}-worker`
    );
    const reviewFailedMetric = new logs.MetricFilter(this, "ReviewFailedFilter", {
      logGroup: workerLogs,
      filterPattern: logs.FilterPattern.literal('"review_failed"'),
      metricNamespace: "CoderHelm/Reviewer",
      metricName: "ReviewFailed",
      metricValue: "1",
    }).metric({ period: cdk.Duration.minutes(15), statistic: "Sum" });
    const reviewGaveUpMetric = new logs.MetricFilter(this, "ReviewGaveUpFilter", {
      logGroup: workerLogs,
      filterPattern: logs.FilterPattern.literal('"review_gave_up"'),
      metricNamespace: "CoderHelm/Reviewer",
      metricName: "ReviewGaveUp",
      metricValue: "1",
    }).metric({ period: cdk.Duration.minutes(15), statistic: "Sum" });
    const reviewGaveUpAlarm = new cloudwatch.Alarm(this, "ReviewGaveUpAlarm", {
      alarmName: `${prefix}-review-gave-up`,
      alarmDescription: "A PR review failed on every retry (service errors)",
      metric: reviewGaveUpMetric,
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    reviewGaveUpAlarm.addAlarmAction(new actions.SnsAction(alertTopic));
    // The hosted side of hybrid: CoderHelm couldn't sign a relayed event or
    // an answer (KMS refused or failed). Installs that pin CoderHelm's keys
    // refuse unsigned messages, so this stops hybrid traffic until fixed.
    if (props.cfg.hybrid) {
      const signFilter = (id: string, logGroup: logs.ILogGroup) =>
        new logs.MetricFilter(this, id, {
          logGroup,
          filterPattern: logs.FilterPattern.literal('"HybridSignError"'),
          metricNamespace: "CoderHelm/Hybrid",
          metricName: "SignError",
          metricValue: "1",
        });
      signFilter("HybridSignErrorWorker", workerLogs);
      signFilter("HybridSignErrorGateway", logs.LogGroup.fromLogGroupName(this, "GatewayLogsForSigning", `/aws/lambda/${prefix}-gateway`));
      const signAlarm = new cloudwatch.Alarm(this, "HybridSignErrorAlarm", {
        alarmName: `${prefix}-hybrid-sign-error`,
        alarmDescription: "CoderHelm couldn't sign what it sends hybrid installs (KMS Sign failed)",
        metric: new cloudwatch.Metric({ namespace: "CoderHelm/Hybrid", metricName: "SignError", period: cdk.Duration.minutes(5), statistic: "Sum" }),
        threshold: 1,
        evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      });
      signAlarm.addAlarmAction(new actions.SnsAction(alertTopic));
    }
    // Many failed attempts in a short window: the model API (or GitHub) is
    // failing for reviews broadly, even while retries are still running.
    const reviewFailedAlarm = new cloudwatch.Alarm(this, "ReviewFailedAlarm", {
      alarmName: `${prefix}-review-failed`,
      alarmDescription: "Reviews failing repeatedly (5+ failed attempts in 15 minutes)",
      metric: reviewFailedMetric,
      threshold: 5,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    reviewFailedAlarm.addAlarmAction(new actions.SnsAction(alertTopic));
    // Each PR the stuck-review sweep picks up, and each job any pass sent
    // again after a transient model error.
    const sweepRequeuedMetric = new logs.MetricFilter(this, "ReviewSweepRequeuedFilter", {
      logGroup: workerLogs,
      filterPattern: logs.FilterPattern.literal('"review_sweep_requeued"'),
      metricNamespace: "CoderHelm/Reviewer",
      metricName: "ReviewSweepRequeued",
      metricValue: "1",
    }).metric({ period: cdk.Duration.minutes(15), statistic: "Sum" });
    // Each run the run guard ended for making no progress (see `run_guard`):
    // a run that loops, stopped before it spent more.
    new logs.MetricFilter(this, "RunLoopStoppedFilter", {
      logGroup: workerLogs,
      filterPattern: logs.FilterPattern.literal('"run_loop_stopped"'),
      metricNamespace: "CoderHelm/Runs",
      metricName: "RunLoopStopped",
      metricValue: "1",
    });
    // Each run the stuck-run sweep found waiting on a person with nothing
    // asked (services/common/src/run_wait.rs): the worker can no longer
    // write one, so any is a bug, and an alarm says so.
    const cleanupLogs = logs.LogGroup.fromLogGroupName(
      this,
      "StuckRunCleanupLogs",
      `/aws/lambda/${prefix}-stuck-run-cleanup`
    );
    const unaskedMetric = new logs.MetricFilter(this, "RunWaitingUnaskedFilter", {
      logGroup: cleanupLogs,
      filterPattern: logs.FilterPattern.literal('"run_waiting_unasked"'),
      metricNamespace: "CoderHelm/Runs",
      metricName: "RunWaitingUnasked",
      metricValue: "1",
    }).metric({ period: cdk.Duration.hours(1), statistic: "Sum" });
    const unaskedAlarm = new cloudwatch.Alarm(this, "RunWaitingUnaskedAlarm", {
      alarmName: `${prefix}-run-waiting-unasked`,
      alarmDescription:
        "A run waited on a person with nothing asked. The stuck-run sweep named its wait and notified the team; find the write that set needs_input without one.",
      metric: unaskedMetric,
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    unaskedAlarm.addAlarmAction(new actions.SnsAction(alertTopic));
    // Each hourly check that found a team which records deploys most days
    // with none recorded for two days while its bots kept working
    // (services/common/src/deploy_stats.rs, `health`). The worker reads the
    // hole back by itself; the alarm says the recording broke.
    const deploysStalledMetric = new logs.MetricFilter(this, "DeployRecordsStalledFilter", {
      logGroup: workerLogs,
      filterPattern: logs.FilterPattern.literal('"deploy_records_stalled"'),
      metricNamespace: "CoderHelm/Analytics",
      metricName: "DeployRecordsStalled",
      metricValue: "1",
    }).metric({ period: cdk.Duration.hours(1), statistic: "Sum" });
    const deploysStalledAlarm = new cloudwatch.Alarm(this, "DeployRecordsStalledAlarm", {
      alarmName: `${prefix}-deploy-records-stalled`,
      alarmDescription:
        "A team that records deploys most days recorded none for 48 hours while runs its bots triggered kept arriving. CoderHelm reads the missed deploys back from GitHub's run history; find why the workflow run webhooks stopped recording them.",
      metric: deploysStalledMetric,
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    deploysStalledAlarm.addAlarmAction(new actions.SnsAction(alertTopic));
    const modelRequeuedMetric = new logs.MetricFilter(this, "ModelErrorRequeuedFilter", {
      logGroup: workerLogs,
      filterPattern: logs.FilterPattern.literal('"model_error_requeued"'),
      metricNamespace: "CoderHelm/Worker",
      metricName: "ModelErrorRequeued",
      metricValue: "1",
    }).metric({ period: cdk.Duration.minutes(15), statistic: "Sum" });

    // Hosted side of hybrid: a CoderHelm address parked by the name check
    // (it pointed somewhere that no longer answered as its install, or a
    // record under it wasn't CoderHelm's), or a record in the zone CoderHelm
    // didn't write and can't place.
    if (props.cfg.hybrid) {
      const parked = new logs.MetricFilter(this, "HybridNameParkedFilter", {
        logGroup: workerLogs,
        filterPattern: logs.FilterPattern.literal('"hybrid_name_parked"'),
        metricNamespace: "CoderHelm/Hybrid",
        metricName: "NameParked",
        metricValue: "1",
      }).metric({ period: cdk.Duration.minutes(15), statistic: "Sum" });
      const unknown = new logs.MetricFilter(this, "HybridNameUnknownRecordFilter", {
        logGroup: workerLogs,
        filterPattern: logs.FilterPattern.literal('"hybrid_name_unknown_record"'),
        metricNamespace: "CoderHelm/Hybrid",
        metricName: "UnknownRecord",
        metricValue: "1",
      }).metric({ period: cdk.Duration.minutes(15), statistic: "Sum" });
      for (const [id, name, description, metric] of [
        ["HybridNameParkedAlarm", "hybrid-name-parked", "A CoderHelm address was parked by the name check", parked],
        ["HybridNameUnknownRecordAlarm", "hybrid-name-unknown-record", "A record in the CoderHelm address zone that CoderHelm didn't write", unknown],
      ] as const) {
        new cloudwatch.Alarm(this, id, {
          alarmName: `${prefix}-${name}`,
          alarmDescription: description,
          metric,
          threshold: 1,
          evaluationPeriods: 1,
          comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
          treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
        }).addAlarmAction(new actions.SnsAction(alertTopic));
      }
    }

    // ── Alarms on what the services log, as a share of their traffic ──────
    const logsOf = (id: string, fn: lambda.Function) =>
      logs.LogGroup.fromLogGroupName(this, `${id}LogGroup`, cdk.Fn.join("", ["/aws/lambda/", fn.functionName]));
    const count = (id: string, logGroup: logs.ILogGroup, pattern: logs.IFilterPattern, metricName: string) =>
      new logs.MetricFilter(this, id, {
        logGroup,
        filterPattern: pattern,
        metricNamespace: "CoderHelm/Logs",
        metricName,
        metricValue: "1",
        defaultValue: 0,
      }).metric({ period: LOG_ALARMS.period, statistic: "Sum" });
    const shareAlarm = (id: string, name: string, description: string, expression: string, usingMetrics: Record<string, cloudwatch.IMetric>, percent: number) => {
      const alarm = new cloudwatch.Alarm(this, id, {
        alarmName: `${prefix}-${name}`,
        alarmDescription: description,
        metric: new cloudwatch.MathExpression({ expression, usingMetrics, period: LOG_ALARMS.period, label: name }),
        threshold: percent,
        evaluationPeriods: LOG_ALARMS.evaluationPeriods,
        datapointsToAlarm: LOG_ALARMS.datapointsToAlarm,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      });
      alarm.addAlarmAction(new actions.SnsAction(alertTopic));
      return alarm;
    };
    // The Rust services log JSON (tracing, `level` at the top); the Python
    // updater logs Lambda's text lines ("[ERROR]\t<time>\t<request>\t<message>").
    const jsonError = logs.FilterPattern.stringValue("$.level", "=", "ERROR");
    const textError = logs.FilterPattern.literal('"[ERROR]"');
    const logged: [string, string, lambda.Function, logs.IFilterPattern][] = [
      ["Gateway", "gateway", props.gatewayFunction, jsonError],
      ["Worker", "worker", props.workerFunction, jsonError],
      ["Streaming", "streaming", props.streamingFunction, jsonError],
      ...(props.updaterFunction ? [["Updater", "updater", props.updaterFunction, textError] as [string, string, lambda.Function, logs.IFilterPattern]] : []),
    ];
    const groups: Record<string, logs.ILogGroup> = {};
    for (const [id, name, fn, pattern] of logged) {
      groups[name] = logsOf(id, fn);
      const errorLines = count(`${id}ErrorLinesFilter`, groups[name], pattern, `${id}ErrorLines`);
      shareAlarm(
        `${id}ErrorLinesAlarm`,
        `${name}-error-lines`,
        `The ${name} logged an ERROR line on ${LOG_ALARMS.errorLinesPercent}% or more of its invocations for most of an hour. Its Lambda Errors stay at 0 because every error is caught: read the ERROR lines in its log group.`,
        "IF(calls > 0, 100 * errors / calls, 0)",
        { errors: errorLines, calls: fn.metricInvocations({ period: LOG_ALARMS.period, statistic: "Sum" }) },
        LOG_ALARMS.errorLinesPercent,
      );
    }
    const rateLimited = count("GitHubRateLimitedFilter", groups.worker, logs.FilterPattern.stringValue("$.event", "=", LOG_EVENTS.rateLimited), "GitHubRateLimited");
    shareAlarm(
      "GitHubRateLimitedAlarm",
      "github-rate-limited",
      `GitHub refused or held back requests on ${LOG_ALARMS.rateLimitedPercent}% or more of the worker's invocations for most of an hour: the installation's hourly budget is spent, and runs, reviews and checks wait for it.`,
      "IF(calls > 0, 100 * limited / calls, 0)",
      { limited: rateLimited, calls: props.workerFunction.metricInvocations({ period: LOG_ALARMS.period, statistic: "Sum" }) },
      LOG_ALARMS.rateLimitedPercent,
    );
    const answered = count("ChatAnsweredFilter", groups.streaming,
      logs.FilterPattern.all(logs.FilterPattern.stringValue("$.event", "=", LOG_EVENTS.chatAnswer), logs.FilterPattern.stringValue("$.outcome", "=", "answered")), "ChatAnswered");
    const chatFailed = count("ChatAnswerFailedFilter", groups.streaming,
      logs.FilterPattern.all(logs.FilterPattern.stringValue("$.event", "=", LOG_EVENTS.chatAnswer), logs.FilterPattern.stringValue("$.outcome", "=", "failed")), "ChatAnswerFailed");
    shareAlarm(
      "ChatAnswerFailedAlarm",
      "chat-answer-failed",
      `Chat couldn't answer ${LOG_ALARMS.chatFailedPercent}% or more of the questions asked for most of an hour (its sign-in check or the model failed). Read the "Chat couldn't answer" lines in the streaming function's log group.`,
      "IF(failed + answered > 0, 100 * failed / (failed + answered), 0)",
      { failed: chatFailed, answered },
      LOG_ALARMS.chatFailedPercent,
    );

    // Dashboard
    const dashboard = new cloudwatch.Dashboard(this, "Dashboard", {
      dashboardName: prefix,
    });

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: "Gateway Invocations & Errors",
        left: [
          props.gatewayFunction.metricInvocations(),
          props.gatewayFunction.metricErrors(),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: "Worker Invocations & Errors",
        left: [
          props.workerFunction.metricInvocations(),
          props.workerFunction.metricErrors(),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: "Gateway Duration",
        left: [props.gatewayFunction.metricDuration()],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: "Worker Duration",
        left: [props.workerFunction.metricDuration()],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: "Reviews that didn't finish",
        left: [reviewFailedMetric, reviewGaveUpMetric, sweepRequeuedMetric, modelRequeuedMetric],
        width: 12,
      })
    );

    new cdk.CfnOutput(this, "AlertTopicArn", { value: alertTopic.topicArn });
  }
}
