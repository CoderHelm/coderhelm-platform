/**
 * CoderHelm's mail relay for hybrid installs (the hosted side only; see
 * services/common/src/mail and docs/self-host/hybrid.md).
 *
 * It sends from its own subdomain (`notify.<zone>`) so relayed mail can never
 * harm the main domain's reputation: Easy DKIM, a custom MAIL FROM with SPF,
 * and a DMARC policy of reject, all written as records in the zone. Its
 * configuration set publishes bounces and complaints to a topic that reaches
 * the worker raw on the ticket queue. Only the gateway (sending) and the
 * worker (pause notices) may send as the relay, and only from its address.
 */
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
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
      events: [ses.EmailSendingEvent.BOUNCE, ses.EmailSendingEvent.COMPLAINT],
    });

    const identity = new ses.EmailIdentity(this, "Identity", {
      identity: ses.Identity.domain(relay.domain),
      configurationSet: configSet,
      dkimSigning: true,
      mailFromDomain: mailFrom,
      mailFromBehaviorOnMxFailure: ses.MailFromBehaviorOnMxFailure.REJECT_MESSAGE,
    });
    // Easy DKIM: the three CNAMEs SES signs with.
    identity.dkimRecords.forEach((r, i) => {
      new route53.CnameRecord(this, `Dkim${i + 1}`, {
        zone,
        recordName: r.name,
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
