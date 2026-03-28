import * as cdk from "aws-cdk-lib";
import * as wafv2 from "aws-cdk-lib/aws-wafv2";
import { Construct } from "constructs";

interface WafStackProps extends cdk.StackProps {
  stage: string;
  /** "api" for the platform (stricter limits), "site" for the landing page */
  target: "api" | "site";
}

/**
 * WAFv2 Web ACL for CloudFront distributions.
 *
 * Must be deployed in us-east-1 since CloudFront is global.
 * Rules are kept simple and low-cost — no paid managed rule groups.
 */
export class WafStack extends cdk.Stack {
  public readonly webAclArn: string;

  constructor(scope: Construct, id: string, props: WafStackProps) {
    super(scope, id, props);

    const prefix = `d3ftly-${props.stage}`;
    const isApi = props.target === "api";

    // Rate limits: 500 req/5min for API, 2000 req/5min for landing page
    const rateLimit = isApi ? 500 : 2000;

    const webAcl = new wafv2.CfnWebACL(this, "WebAcl", {
      name: `${prefix}-${props.target}-waf`,
      scope: "CLOUDFRONT",
      defaultAction: { allow: {} },
      visibilityConfig: {
        cloudWatchMetricsEnabled: true,
        metricName: `${prefix}-${props.target}-waf`,
        sampledRequestsEnabled: true,
      },
      rules: [
        // ── Rule 1: Rate limiting per IP ──
        {
          name: "RateLimit",
          priority: 1,
          action: { block: {} },
          statement: {
            rateBasedStatement: {
              limit: rateLimit,
              aggregateKeyType: "IP",
            },
          },
          visibilityConfig: {
            cloudWatchMetricsEnabled: true,
            metricName: `${prefix}-${props.target}-rate-limit`,
            sampledRequestsEnabled: true,
          },
        },

        // ── Rule 2: AWS Managed — Common Rule Set (free tier) ──
        // Blocks known bad inputs: path traversal, log4j, SSRF, etc.
        {
          name: "AWSManagedRulesCommonRuleSet",
          priority: 2,
          overrideAction: { none: {} },
          statement: {
            managedRuleGroupStatement: {
              vendorName: "AWS",
              name: "AWSManagedRulesCommonRuleSet",
              // Exclude SizeRestrictions for API routes that accept larger payloads
              ...(isApi
                ? {
                    excludedRules: [
                      { name: "SizeRestrictions_BODY" },
                    ],
                  }
                : {}),
            },
          },
          visibilityConfig: {
            cloudWatchMetricsEnabled: true,
            metricName: `${prefix}-${props.target}-common-rules`,
            sampledRequestsEnabled: true,
          },
        },

        // ── Rule 3: AWS Managed — Known Bad Inputs (free tier) ──
        // Blocks Java deserialization, host header attacks, etc.
        {
          name: "AWSManagedRulesKnownBadInputsRuleSet",
          priority: 3,
          overrideAction: { none: {} },
          statement: {
            managedRuleGroupStatement: {
              vendorName: "AWS",
              name: "AWSManagedRulesKnownBadInputsRuleSet",
            },
          },
          visibilityConfig: {
            cloudWatchMetricsEnabled: true,
            metricName: `${prefix}-${props.target}-bad-inputs`,
            sampledRequestsEnabled: true,
          },
        },

        // ── Rule 4: AWS Managed — Amazon IP Reputation List (free tier) ──
        // Blocks requests from known malicious IPs (botnets, scanners).
        {
          name: "AWSManagedRulesAmazonIpReputationList",
          priority: 4,
          overrideAction: { none: {} },
          statement: {
            managedRuleGroupStatement: {
              vendorName: "AWS",
              name: "AWSManagedRulesAmazonIpReputationList",
            },
          },
          visibilityConfig: {
            cloudWatchMetricsEnabled: true,
            metricName: `${prefix}-${props.target}-ip-reputation`,
            sampledRequestsEnabled: true,
          },
        },

        // ── Rule 5: Block common bot user-agents ──
        {
          name: "BlockBadBots",
          priority: 5,
          action: { block: {} },
          statement: {
            regexPatternSetReferenceStatement: {
              arn: new wafv2.CfnRegexPatternSet(this, "BotPatterns", {
                scope: "CLOUDFRONT",
                regularExpressionList: [
                  "(?i)(scrapy|httpclient|python-urllib|python-requests|curl\\/|wget\\/|go-http|nikto|sqlmap|nmap|masscan|zgrab)",
                ],
              }).attrArn,
              fieldToMatch: {
                singleHeader: { name: "user-agent" },
              },
              textTransformations: [{ priority: 0, type: "NONE" }],
            },
          },
          visibilityConfig: {
            cloudWatchMetricsEnabled: true,
            metricName: `${prefix}-${props.target}-bad-bots`,
            sampledRequestsEnabled: true,
          },
        },
      ],
    });

    this.webAclArn = webAcl.attrArn;

    new cdk.CfnOutput(this, "WebAclArn", {
      value: webAcl.attrArn,
    });
  }
}
