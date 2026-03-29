import * as cdk from "aws-cdk-lib";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as sns from "aws-cdk-lib/aws-sns";
import * as actions from "aws-cdk-lib/aws-cloudwatch-actions";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as sqs from "aws-cdk-lib/aws-sqs";
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
      })
    );

    new cdk.CfnOutput(this, "AlertTopicArn", { value: alertTopic.topicArn });
  }
}
