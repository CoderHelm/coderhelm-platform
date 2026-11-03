/**
 * CoderHelm's mail relay for hybrid installs (the hosted side only; see
 * services/common/src/mail and docs/self-host/hybrid.md).
 *
 * It sends from its own subdomain (`notify.<zone>`) so relayed mail can never
 * harm the main domain's reputation: Easy DKIM, a custom MAIL FROM with SPF,
 * and a DMARC policy of reject, all written as records in the zone. Its
 * configuration set publishes its events (sends, deliveries, bounces,
 * complaints, rejections) to a topic that reaches the worker raw on the
 * ticket queue. Only the gateway (sending) and the
 * worker (pause notices) may send as the relay, and only from its address.
 *
 * A brake keeps relayed mail from ever costing the account its sending (and
 * with it Cognito's sign-up codes and password resets): when the relay's own
 * bounce or complaint rate nears SES's review levels, an alarm turns sending
 * off for the relay's configuration set only. An operator turns it back on
 * after looking (docs/self-host/hybrid.md, "Relay brake"). The full split, the
 * relay in its own AWS account, is the next step written there.
 */
import * as cdk from "aws-cdk-lib";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as cwActions from "aws-cdk-lib/aws-cloudwatch-actions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as route53 from "aws-cdk-lib/aws-route53";
import * as ses from "aws-cdk-lib/aws-ses";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";

/** The relay's configuration set name. */
export function relayConfigSetName(cfg: ResolvedConfig): string {
  return `${cfg.prefix}-relay`;
}

/**
 * The relay brake's levels, below SES's own: SES reviews an account at a 5%
 * bounce rate or a 0.1% complaint rate and may pause it at 10% and 0.5%.
 */
export const RELAY_BRAKE = { bounceRate: 0.04, complaintRate: 0.0008 } as const;

/** Sending as the relay: its identity, through its configuration set, from its address only. */
export function relaySendStatement(cfg: ResolvedConfig, account: string): iam.PolicyStatement | undefined {
  if (!cfg.mailRelay) return undefined;
  return new iam.PolicyStatement({
    sid: "MailRelaySendsOnlyAsTheRelay",
    actions: ["ses:SendEmail"],
    resources: [
      `arn:aws:ses:${cfg.email.sesRegion}:${account}:identity/${cfg.mailRelay.domain}`,
      `arn:aws:ses:${cfg.email.sesRegion}:${account}:configuration-set/${relayConfigSetName(cfg)}`,
    ],
    conditions: { StringEquals: { "ses:FromAddress": cfg.mailRelay.from } },
  });
}

export interface MailRelayProps {
  cfg: ResolvedConfig;
  /** Where bounces and complaints go (raw), for the worker. */
  ticketQueue: sqs.IQueue;
}

export class MailRelay extends Construct {
  public readonly topic: sns.Topic;

  constructor(scope: Construct, id: string, props: MailRelayProps) {
    super(scope, id);
    const cfg = props.cfg;
    const relay = cfg.mailRelay!;
    const zoneName = cfg.hybrid!.zone;
    const zone = cfg.hybrid!.zoneId
      ? route53.PublicHostedZone.fromPublicHostedZoneAttributes(this, "Zone", {
          zoneName,
          hostedZoneId: cfg.hybrid!.zoneId,
        })
      : route53.PublicHostedZone.fromLookup(this, "Zone", { domainName: zoneName });
    const mailFrom = `bounce.${relay.domain}`;

    this.topic = new sns.Topic(this, "Events", {
      topicName: `${cfg.prefix}-relay-mail-events`,
      displayName: "CoderHelm mail relay bounces and complaints",
    });
    this.topic.addSubscription(new subs.SqsSubscription(props.ticketQueue, { rawMessageDelivery: true }));

    const configSet = new ses.ConfigurationSet(this, "ConfigSet", {
      configurationSetName: relayConfigSetName(cfg),
      reputationMetrics: true,
      sendingEnabled: true,
      suppressionReasons: ses.SuppressionReasons.BOUNCES_AND_COMPLAINTS,
    });
    configSet.addEventDestination("BouncesAndComplaints", {
      destination: ses.EventDestination.snsTopic(this.topic),
      // Bounces and complaints suppress and pause; every event is also
      // counted by kind for each install's Deliveries (common::mail::stats).
      events: [
        ses.EmailSendingEvent.SEND,
        ses.EmailSendingEvent.DELIVERY,
        ses.EmailSendingEvent.BOUNCE,
        ses.EmailSendingEvent.COMPLAINT,
        ses.EmailSendingEvent.REJECT,
      ],
    });

    // The brake: the relay's reputation (its configuration set only), and a
    // function that turns its sending off when either alarm fires.
    const brakeFn = new lambda.Function(this, "BrakeFn", {
      functionName: `${cfg.prefix}-relay-brake`,
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "index.handler",
      timeout: cdk.Duration.seconds(30),
      memorySize: 128,
      environment: { CONFIG_SET: relayConfigSetName(cfg) },
      code: lambda.Code.fromInline(
        [
          "import os, boto3",
          "def handler(event, _ctx):",
          "    # An alarm went off: stop relayed mail only (Cognito and the app keep sending).",
          "    boto3.client('sesv2').put_configuration_set_sending_options(",
          "        ConfigurationSetName=os.environ['CONFIG_SET'], SendingEnabled=False)",
          "    print('relay sending turned off by the brake')",
        ].join("\n"),
      ),
    });
    brakeFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["ses:PutConfigurationSetSendingOptions"],
        resources: [
          `arn:${cdk.Aws.PARTITION}:ses:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:configuration-set/${relayConfigSetName(cfg)}`,
        ],
      }),
    );
    const brakeTopic = new sns.Topic(this, "Brake", {
      topicName: `${cfg.prefix}-relay-brake`,
      displayName: "CoderHelm mail relay brake",
    });
    brakeTopic.addSubscription(new subs.LambdaSubscription(brakeFn));
    const rate = (metric: string, threshold: number, id: string) => {
      const alarm = new cloudwatch.Alarm(this, id, {
        alarmName: `${cfg.prefix}-relay-${id.toLowerCase()}`,
        alarmDescription: `Relayed mail's ${metric} is near SES's review level: relay sending is turned off.`,
        metric: new cloudwatch.Metric({
          namespace: "AWS/SES",
          metricName: metric,
          dimensionsMap: { "ses:configuration-set": relayConfigSetName(cfg) },
          statistic: "Maximum",
          period: cdk.Duration.minutes(15),
        }),
        threshold,
        evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      });
      alarm.addAlarmAction(new cwActions.SnsAction(brakeTopic));
      return alarm;
    };
    rate("Reputation.BounceRate", RELAY_BRAKE.bounceRate, "BounceBrake");
    rate("Reputation.ComplaintRate", RELAY_BRAKE.complaintRate, "ComplaintBrake");

    const identity = new ses.EmailIdentity(this, "Identity", {
      identity: ses.Identity.domain(relay.domain),
      configurationSet: configSet,
      dkimSigning: true,
      mailFromDomain: mailFrom,
      mailFromBehaviorOnMxFailure: ses.MailFromBehaviorOnMxFailure.REJECT_MESSAGE,
    });
    // Easy DKIM: the three CNAMEs SES signs with. Their names are tokens
    // that resolve to full names, which CDK can't see; without the trailing
    // dot it appends the zone again (`...notify.<zone>.<zone>`) and SES never
    // finds them.
    identity.dkimRecords.forEach((r, i) => {
      new route53.CnameRecord(this, `Dkim${i + 1}`, {
        zone,
        recordName: `${r.name}.`,
        domainName: r.value,
        ttl: cdk.Duration.hours(1),
      });
    });
    // MAIL FROM: bounces come back to SES, and SPF names only SES.
    new route53.MxRecord(this, "MailFromMx", {
      zone,
      recordName: mailFrom,
      values: [{ priority: 10, hostName: `feedback-smtp.${cfg.email.sesRegion}.amazonses.com` }],
      ttl: cdk.Duration.hours(1),
    });
    new route53.TxtRecord(this, "MailFromSpf", {
      zone,
      recordName: mailFrom,
      values: ["v=spf1 include:amazonses.com -all"],
      ttl: cdk.Duration.hours(1),
    });
    // The sending subdomain itself sends only through SES.
    new route53.TxtRecord(this, "Spf", {
      zone,
      recordName: relay.domain,
      values: ["v=spf1 include:amazonses.com -all"],
      ttl: cdk.Duration.hours(1),
    });
    // DMARC for the subdomain: mail not signed by SES as notify.<zone> is
    // rejected, whatever the main domain's own policy says.
    new route53.TxtRecord(this, "Dmarc", {
      zone,
      recordName: `_dmarc.${relay.domain}`,
      values: ["v=DMARC1; p=reject; sp=reject; adkim=s; aspf=r; pct=100"],
      ttl: cdk.Duration.hours(1),
    });
  }
}
