import * as cdk from "aws-cdk-lib";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as sns from "aws-cdk-lib/aws-sns";
import * as actions from "aws-cdk-lib/aws-cloudwatch-actions";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as logs from "aws-cdk-lib/aws-logs";
import { Construct } from "constructs";

interface MonitoringStackProps extends cdk.StackProps {
  stage: string;
  gatewayFunction: lambda.Function;
  workerFunction: lambda.Function;
  table: dynamodb.TableV2;
  ticketQueue: sqs.Queue;
}

export class MonitoringStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: MonitoringStackProps) {
    super(scope, id, props);

    const prefix = `coderhelm-${props.stage}`;

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
        left: [reviewFailedMetric, reviewGaveUpMetric],
        width: 12,
      })
    );

    new cdk.CfnOutput(this, "AlertTopicArn", { value: alertTopic.topicArn });
  }
}
