import * as cdk from "aws-cdk-lib";
import * as ses from "aws-cdk-lib/aws-ses";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { Construct } from "constructs";

interface EmailStackProps extends cdk.StackProps {
  stage: string;
  gatewayFunction: lambda.Function;
  workerFunction: lambda.Function;
}

export class EmailStack extends cdk.Stack {
  public readonly configSet: ses.ConfigurationSet;

  constructor(scope: Construct, id: string, props: EmailStackProps) {
    super(scope, id, props);

    const prefix = `coderhelm-${props.stage}`;

    // SES configuration set for tracking
    this.configSet = new ses.ConfigurationSet(this, "ConfigSet", {
      configurationSetName: `${prefix}-emails`,
      reputationMetrics: true,
      sendingEnabled: true,
    });

    // SES email identity (domain-level verification)
    new ses.EmailIdentity(this, "DomainIdentity", {
      identity: ses.Identity.domain("coderhelm.com"),
      configurationSet: this.configSet,
    });

    // Templated emails
    new ses.CfnTemplate(this, "WelcomeTemplate", {
      template: {
        templateName: `${prefix}-welcome`,
        subjectPart: "Welcome to Coderhelm — your AI coding agent is ready",
        htmlPart: WELCOME_HTML,
        textPart: WELCOME_TEXT,
      },
    });

    new ses.CfnTemplate(this, "RunCompleteTemplate", {
      template: {
        templateName: `${prefix}-run-complete`,
        subjectPart: "Coderhelm completed: {{title}}",
        htmlPart: RUN_COMPLETE_HTML,
        textPart: RUN_COMPLETE_TEXT,
      },
    });

    new ses.CfnTemplate(this, "RunFailedTemplate", {
      template: {
        templateName: `${prefix}-run-failed`,
        subjectPart: "Coderhelm failed: {{title}}",
        htmlPart: RUN_FAILED_HTML,
        textPart: RUN_FAILED_TEXT,
      },
    });

    new ses.CfnTemplate(this, "WeeklySummaryTemplate", {
      template: {
        templateName: `${prefix}-weekly-summary`,
        subjectPart: "Your Coderhelm weekly summary",
        htmlPart: WEEKLY_SUMMARY_HTML,
        textPart: WEEKLY_SUMMARY_TEXT,
      },
    });

    // --- Billing email templates ---

    new ses.CfnTemplate(this, "PaymentReceiptTemplate", {
      template: {
        templateName: `${prefix}-payment-receipt`,
        subjectPart: "Coderhelm payment receipt — ${{amount}}",
        htmlPart: PAYMENT_RECEIPT_HTML,
        textPart: PAYMENT_RECEIPT_TEXT,
      },
    });

    new ses.CfnTemplate(this, "PaymentFailedTemplate", {
      template: {
        templateName: `${prefix}-payment-failed`,
        subjectPart: "Coderhelm payment failed — action required",
        htmlPart: PAYMENT_FAILED_HTML,
        textPart: PAYMENT_FAILED_TEXT,
      },
    });

    new ses.CfnTemplate(this, "SubscriptionCancelledTemplate", {
      template: {
        templateName: `${prefix}-subscription-cancelled`,
        subjectPart: "Your Coderhelm subscription has been cancelled",
        htmlPart: SUBSCRIPTION_CANCELLED_HTML,
        textPart: SUBSCRIPTION_CANCELLED_TEXT,
      },
    });

    new ses.CfnTemplate(this, "InvoiceReadyTemplate", {
      template: {
        templateName: `${prefix}-invoice-ready`,
        subjectPart: "Coderhelm invoice #{{invoice_number}} — ${{amount}}",
        htmlPart: INVOICE_READY_HTML,
        textPart: INVOICE_READY_TEXT,
      },
    });

    // Grant SES send permissions to both lambdas
    const sesPolicy = new iam.PolicyStatement({
      actions: ["ses:SendEmail", "ses:SendTemplatedEmail"],
      resources: ["*"],
      conditions: {
        StringEquals: {
          "ses:FromAddress": `noreply@coderhelm.com`,
        },
      },
    });

    props.gatewayFunction.addToRolePolicy(sesPolicy);
    props.workerFunction.addToRolePolicy(sesPolicy);

    new cdk.CfnOutput(this, "ConfigSetName", {
      value: this.configSet.configurationSetName,
    });
  }
}

// ─── Email templates (Mustache-style {{var}} placeholders for SES) ───

const EMAIL_WRAPPER = (body: string) => `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0a0a0a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0a;padding:40px 0">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#111;border-radius:8px;border:1px solid #222">
<tr><td style="padding:32px 40px 24px">
  <img src="https://coderhelm.com/favicon.svg" width="32" height="32" alt="Coderhelm" style="display:block;margin-bottom:24px">
  ${body}
  <p style="color:#666;font-size:12px;margin-top:32px;padding-top:16px;border-top:1px solid #222">
    You're receiving this because you have notifications enabled for your Coderhelm account.<br>
    <a href="https://app.coderhelm.com/dashboard/settings" style="color:#888">Manage notification preferences</a> · <a href="https://coderhelm.com/privacy" style="color:#888">Privacy</a>
  </p>
</td></tr>
</table>
</td></tr>
</table>
</body>
</html>`;

const WELCOME_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#fff;font-size:24px;margin:0 0 16px">Welcome to Coderhelm</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6">
    Your AI coding agent is ready. Coderhelm is now installed on <strong style="color:#fff">{{org}}</strong>
    with access to <strong style="color:#fff">{{repo_count}}</strong> repositories.
  </p>
  <p style="color:#ccc;font-size:15px;line-height:1.6">
    To get started, assign an issue to <code style="background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#4ade80">coderhelm[bot]</code>
    or add the <code style="background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#4ade80">coderhelm</code> label.
  </p>
  <a href="https://app.coderhelm.com/dashboard" style="display:inline-block;background:#fff;color:#000;padding:12px 24px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:16px">Open Dashboard</a>
`);

const WELCOME_TEXT = `Welcome to Coderhelm!

Your AI coding agent is now installed on {{org}} with access to {{repo_count}} repositories.

To get started, assign an issue to coderhelm[bot] or add the "coderhelm" label.

Dashboard: https://app.coderhelm.com/dashboard

Manage notifications: https://app.coderhelm.com/dashboard/settings`;

const RUN_COMPLETE_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#4ade80;font-size:20px;margin:0 0 16px">✓ Run completed</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    <strong style="color:#fff">{{title}}</strong> on <code style="background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#ccc">{{repo}}</code>
  </p>
  <table style="width:100%;border-collapse:collapse;margin:16px 0">
    <tr><td style="color:#888;padding:4px 0;font-size:14px">PR</td><td style="color:#fff;padding:4px 0;font-size:14px"><a href="{{pr_url}}" style="color:#60a5fa">{{pr_url}}</a></td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Files</td><td style="color:#fff;padding:4px 0;font-size:14px">{{files_modified}} modified</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Duration</td><td style="color:#fff;padding:4px 0;font-size:14px">{{duration}}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Cost</td><td style="color:#fff;padding:4px 0;font-size:14px">\${{cost}}</td></tr>
  </table>
  <a href="https://app.coderhelm.com/dashboard/runs/{{run_id}}" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">View Run</a>
`);

const RUN_COMPLETE_TEXT = `Run completed: {{title}}
Repo: {{repo}}
PR: {{pr_url}}
Files: {{files_modified}} modified
Duration: {{duration}}
Cost: \${{cost}}

View run: https://app.coderhelm.com/dashboard/runs/{{run_id}}`;

const RUN_FAILED_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#ef4444;font-size:20px;margin:0 0 16px">✗ Run failed</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    <strong style="color:#fff">{{title}}</strong> on <code style="background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#ccc">{{repo}}</code>
  </p>
  <div style="background:#1a0000;border:1px solid #3f0000;border-radius:6px;padding:16px;margin:16px 0">
    <p style="color:#fca5a5;font-size:14px;margin:0;font-family:monospace">{{error}}</p>
  </div>
  <a href="https://app.coderhelm.com/dashboard/runs/{{run_id}}" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">View Run</a>
`);

const RUN_FAILED_TEXT = `Run failed: {{title}}
Repo: {{repo}}
Error: {{error}}

View run: https://app.coderhelm.com/dashboard/runs/{{run_id}}`;

const WEEKLY_SUMMARY_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#fff;font-size:20px;margin:0 0 16px">Weekly Summary — {{org}}</h1>
  <p style="color:#888;font-size:14px;margin:0 0 20px">{{period}}</p>
  <table style="width:100%;border-collapse:collapse">
    <tr><td style="color:#888;padding:8px 0;font-size:14px;border-bottom:1px solid #222">Total runs</td><td style="color:#fff;padding:8px 0;font-size:14px;border-bottom:1px solid #222;text-align:right">{{total_runs}}</td></tr>
    <tr><td style="color:#888;padding:8px 0;font-size:14px;border-bottom:1px solid #222">Completed</td><td style="color:#4ade80;padding:8px 0;font-size:14px;border-bottom:1px solid #222;text-align:right">{{completed}}</td></tr>
    <tr><td style="color:#888;padding:8px 0;font-size:14px;border-bottom:1px solid #222">Failed</td><td style="color:#ef4444;padding:8px 0;font-size:14px;border-bottom:1px solid #222;text-align:right">{{failed}}</td></tr>
    <tr><td style="color:#888;padding:8px 0;font-size:14px;border-bottom:1px solid #222">PRs created</td><td style="color:#fff;padding:8px 0;font-size:14px;border-bottom:1px solid #222;text-align:right">{{prs_created}}</td></tr>
    <tr><td style="color:#888;padding:8px 0;font-size:14px">Total cost</td><td style="color:#fff;padding:8px 0;font-size:14px;text-align:right">\${{total_cost}}</td></tr>
  </table>
  <a href="https://app.coderhelm.com/dashboard" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:24px">Open Dashboard</a>
`);

const WEEKLY_SUMMARY_TEXT = `Weekly Summary — {{org}} ({{period}})

Total runs: {{total_runs}}
Completed: {{completed}}
Failed: {{failed}}
PRs created: {{prs_created}}
Total cost: \${{total_cost}}

Dashboard: https://app.coderhelm.com/dashboard`;

// ─── Billing email templates ────────────────────────────────────────

const PAYMENT_RECEIPT_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#4ade80;font-size:20px;margin:0 0 16px">Payment received</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    Thank you for your payment of <strong style="color:#fff">\${{amount}}</strong>.
  </p>
  <table style="width:100%;border-collapse:collapse;margin:16px 0">
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Invoice</td><td style="color:#fff;padding:4px 0;font-size:14px">#{{invoice_number}}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Date</td><td style="color:#fff;padding:4px 0;font-size:14px">{{date}}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Plan</td><td style="color:#fff;padding:4px 0;font-size:14px">{{plan_name}}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Card</td><td style="color:#fff;padding:4px 0;font-size:14px">•••• {{card_last4}}</td></tr>
  </table>
  <a href="{{invoice_url}}" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">Download Invoice</a>
`);

const PAYMENT_RECEIPT_TEXT = `Payment received — \${{amount}}

Invoice: #{{invoice_number}}
Date: {{date}}
Plan: {{plan_name}}
Card: •••• {{card_last4}}

Download invoice: {{invoice_url}}`;

const PAYMENT_FAILED_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#ef4444;font-size:20px;margin:0 0 16px">Payment failed</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    We were unable to charge <strong style="color:#fff">\${{amount}}</strong> to your card ending in <strong style="color:#fff">{{card_last4}}</strong>.
  </p>
  <div style="background:#1a0000;border:1px solid #3f0000;border-radius:6px;padding:16px;margin:16px 0">
    <p style="color:#fca5a5;font-size:14px;margin:0">{{failure_reason}}</p>
  </div>
  <p style="color:#ccc;font-size:15px;line-height:1.6">
    We'll retry automatically in {{next_retry}}. To avoid service interruption, please update your payment method.
  </p>
  <a href="https://app.coderhelm.com/dashboard/billing" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">Update Payment Method</a>
`);

const PAYMENT_FAILED_TEXT = `Payment failed — \${{amount}}

We were unable to charge your card ending in {{card_last4}}.
Reason: {{failure_reason}}

We'll retry automatically in {{next_retry}}. Update your payment method to avoid service interruption:
https://app.coderhelm.com/dashboard/billing`;

const SUBSCRIPTION_CANCELLED_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#f59e0b;font-size:20px;margin:0 0 16px">Subscription cancelled</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    Your Coderhelm subscription has been cancelled. You'll continue to have access until <strong style="color:#fff">{{access_until}}</strong>.
  </p>
  <p style="color:#ccc;font-size:15px;line-height:1.6">
    After that date, Coderhelm will stop processing new tickets for your repositories.
  </p>
  <p style="color:#888;font-size:14px;margin-top:16px">
    Changed your mind? You can resubscribe at any time from your dashboard.
  </p>
  <a href="https://app.coderhelm.com/dashboard/billing" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">Resubscribe</a>
`);

const SUBSCRIPTION_CANCELLED_TEXT = `Subscription cancelled

Your Coderhelm subscription has been cancelled. You'll have access until {{access_until}}.
After that, Coderhelm will stop processing new tickets.

Resubscribe: https://app.coderhelm.com/dashboard/billing`;

const INVOICE_READY_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#fff;font-size:20px;margin:0 0 16px">Invoice #{{invoice_number}}</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    Your invoice for <strong style="color:#fff">\${{amount}}</strong> is ready.
  </p>
  <table style="width:100%;border-collapse:collapse;margin:16px 0">
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Period</td><td style="color:#fff;padding:4px 0;font-size:14px">{{period}}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Plan</td><td style="color:#fff;padding:4px 0;font-size:14px">{{plan_name}}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Runs</td><td style="color:#fff;padding:4px 0;font-size:14px">{{total_runs}}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Usage cost</td><td style="color:#fff;padding:4px 0;font-size:14px">\${{usage_cost}}</td></tr>
  </table>
  <a href="{{invoice_url}}" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">Download Invoice PDF</a>
`);

const INVOICE_READY_TEXT = `Invoice #{{invoice_number}} — \${{amount}}

Period: {{period}}
Plan: {{plan_name}}
Runs: {{total_runs}}
Usage cost: \${{usage_cost}}

Download: {{invoice_url}}`;

