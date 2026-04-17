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

    // --- Account email templates ---

    new ses.CfnTemplate(this, "ResetPasswordTemplate", {
      template: {
        templateName: `${prefix}-reset-password`,
        subjectPart: "Reset your Coderhelm password",
        htmlPart: RESET_PASSWORD_HTML,
        textPart: RESET_PASSWORD_TEXT,
      },
    });

    new ses.CfnTemplate(this, "TeamInviteTemplate", {
      template: {
        templateName: `${prefix}-team-invite`,
        subjectPart: "You've been invited to {{org}} on Coderhelm",
        htmlPart: TEAM_INVITE_HTML,
        textPart: TEAM_INVITE_TEXT,
      },
    });

    new ses.CfnTemplate(this, "VerifyEmailTemplate", {
      template: {
        templateName: `${prefix}-verify-email`,
        subjectPart: "Verify your Coderhelm email",
        htmlPart: VERIFY_EMAIL_HTML,
        textPart: VERIFY_EMAIL_TEXT,
      },
    });

    new ses.CfnTemplate(this, "PasswordChangedTemplate", {
      template: {
        templateName: `${prefix}-password-changed`,
        subjectPart: "Your Coderhelm password was changed",
        htmlPart: PASSWORD_CHANGED_HTML,
        textPart: PASSWORD_CHANGED_TEXT,
      },
    });

    new ses.CfnTemplate(this, "PasskeyAddedTemplate", {
      template: {
        templateName: `${prefix}-passkey-added`,
        subjectPart: "A passkey was added to your Coderhelm account",
        htmlPart: PASSKEY_ADDED_HTML,
        textPart: PASSKEY_ADDED_TEXT,
      },
    });

    new ses.CfnTemplate(this, "PasskeyRemovedTemplate", {
      template: {
        templateName: `${prefix}-passkey-removed`,
        subjectPart: "A passkey was removed from your Coderhelm account",
        htmlPart: PASSKEY_REMOVED_HTML,
        textPart: PASSKEY_REMOVED_TEXT,
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
    <a href="https://app.coderhelm.com/settings" style="color:#888">Manage notification preferences</a> · <a href="https://coderhelm.com/privacy" style="color:#888">Privacy</a>
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
    Your AI coding agent is ready. Connect your GitHub organization and install the Coderhelm app to get started.
  </p>
  <p style="color:#ccc;font-size:15px;line-height:1.6">
    Once installed, assign an issue to <code style="background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#4ade80">coderhelm[bot]</code>
    or add the <code style="background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#4ade80">coderhelm</code> label.
  </p>
  <a href="https://app.coderhelm.com/settings/github" style="display:inline-block;background:#fff;color:#000;padding:12px 24px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:16px">Connect GitHub</a>
`);

const WELCOME_TEXT = `Welcome to Coderhelm!

Your AI coding agent is ready. Connect your GitHub organization and install the Coderhelm app to get started.

Once installed, assign an issue to coderhelm[bot] or add the "coderhelm" label.

Connect GitHub: https://app.coderhelm.com/settings/github

Manage notifications: https://app.coderhelm.com/settings`;

const RUN_COMPLETE_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#4ade80;font-size:20px;margin:0 0 16px">✓ Run completed</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    <strong style="color:#fff">{{title}}</strong> on <code style="background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#ccc">{{repo}}</code>
  </p>
  <table style="width:100%;border-collapse:collapse;margin:16px 0">
    <tr><td style="color:#888;padding:4px 0;font-size:14px">PR</td><td style="color:#fff;padding:4px 0;font-size:14px"><a href="{{pr_url}}" style="color:#60a5fa">{{pr_url}}</a></td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Files</td><td style="color:#fff;padding:4px 0;font-size:14px">{{files_modified}} modified</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Duration</td><td style="color:#fff;padding:4px 0;font-size:14px">{{duration}}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Tokens</td><td style="color:#fff;padding:4px 0;font-size:14px">{{tokens}}</td></tr>
  </table>
  <a href="https://app.coderhelm.com" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">View Run</a>
`);

const RUN_COMPLETE_TEXT = `Run completed: {{title}}
Repo: {{repo}}
PR: {{pr_url}}
Files: {{files_modified}} modified
Duration: {{duration}}
Tokens: {{tokens}}

View run: https://app.coderhelm.com`;

const RUN_FAILED_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#ef4444;font-size:20px;margin:0 0 16px">✗ Run failed</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    <strong style="color:#fff">{{title}}</strong> on <code style="background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#ccc">{{repo}}</code>
  </p>
  <div style="background:#1a0000;border:1px solid #3f0000;border-radius:6px;padding:16px;margin:16px 0">
    <p style="color:#fca5a5;font-size:14px;margin:0;font-family:monospace">{{error}}</p>
  </div>
  <a href="https://app.coderhelm.com" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">View Run</a>
`);

const RUN_FAILED_TEXT = `Run failed: {{title}}
Repo: {{repo}}
Error: {{error}}

View run: https://app.coderhelm.com`;

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
  <a href="https://app.coderhelm.com" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:24px">Open Dashboard</a>
`);

const WEEKLY_SUMMARY_TEXT = `Weekly Summary — {{org}} ({{period}})

Total runs: {{total_runs}}
Completed: {{completed}}
Failed: {{failed}}
PRs created: {{prs_created}}
Total cost: \${{total_cost}}

Dashboard: https://app.coderhelm.com`;

// ─── Account email templates ────────────────────────────────────────

const RESET_PASSWORD_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#fff;font-size:20px;margin:0 0 16px">Reset your password</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    Use the code below to reset your Coderhelm password. This code expires in 15 minutes.
  </p>
  <div style="background:#1a1a1a;border:1px solid #333;border-radius:8px;padding:20px;text-align:center;margin:24px 0">
    <span style="color:#fff;font-size:32px;font-weight:700;letter-spacing:8px;font-family:monospace">{{code}}</span>
  </div>
  <p style="color:#888;font-size:14px;line-height:1.6">
    If you didn't request this, you can safely ignore this email.
  </p>
`);

const RESET_PASSWORD_TEXT = `Reset your Coderhelm password

Your reset code: {{code}}

This code expires in 15 minutes. If you didn't request this, ignore this email.`;

const TEAM_INVITE_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#fff;font-size:20px;margin:0 0 16px">You've been invited</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    <strong style="color:#fff">{{inviter}}</strong> has invited you to join <strong style="color:#fff">{{org}}</strong> on Coderhelm as a <strong style="color:#fff">{{role}}</strong>.
  </p>
  <p style="color:#ccc;font-size:15px;line-height:1.6">
    Coderhelm is an AI coding agent that turns GitHub issues and Jira tickets into production-ready pull requests.
  </p>
  <a href="https://app.coderhelm.com" style="display:inline-block;background:#fff;color:#000;padding:12px 24px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:16px">Accept Invite</a>
`);

const TEAM_INVITE_TEXT = `You've been invited to {{org}} on Coderhelm

{{inviter}} invited you as a {{role}}.

Accept your invite: https://app.coderhelm.com`;

const VERIFY_EMAIL_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#fff;font-size:20px;margin:0 0 16px">Verify your email</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    Enter the code below to verify your Coderhelm account.
  </p>
  <div style="background:#1a1a1a;border:1px solid #333;border-radius:8px;padding:20px;text-align:center;margin:24px 0">
    <span style="color:#fff;font-size:32px;font-weight:700;letter-spacing:8px;font-family:monospace">{{code}}</span>
  </div>
  <p style="color:#888;font-size:14px;line-height:1.6">
    If you didn't create a Coderhelm account, you can safely ignore this email.
  </p>
`);

const VERIFY_EMAIL_TEXT = `Verify your Coderhelm email

Your verification code: {{code}}

If you didn't create a Coderhelm account, ignore this email.`;

const PASSWORD_CHANGED_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#fff;font-size:20px;margin:0 0 16px">Your password was changed</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    The password for your Coderhelm account (<strong style="color:#fff">{{email}}</strong>) was just changed.
  </p>
  <p style="color:#ccc;font-size:15px;line-height:1.6">
    If you made this change, no further action is needed.
  </p>
  <p style="color:#fca5a5;font-size:15px;line-height:1.6">
    If you didn't change your password, please <a href="https://app.coderhelm.com" style="color:#60a5fa">reset it immediately</a> and contact us at support@coderhelm.com.
  </p>
`);

const PASSWORD_CHANGED_TEXT = `Your Coderhelm password was changed

The password for your account ({{email}}) was just changed.

If you made this change, no action is needed.
If you didn't, reset your password immediately: https://app.coderhelm.com

Contact: support@coderhelm.com`;

const PASSKEY_ADDED_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#fff;font-size:20px;margin:0 0 16px">Passkey added to your account</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    A new passkey was registered on your Coderhelm account (<strong style="color:#fff">{{email}}</strong>).
  </p>
  <table style="width:100%;border-collapse:collapse;margin:16px 0">
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Device</td><td style="color:#fff;padding:4px 0;font-size:14px">{{device_name}}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Date</td><td style="color:#fff;padding:4px 0;font-size:14px">{{date}}</td></tr>
  </table>
  <p style="color:#ccc;font-size:15px;line-height:1.6">
    If you made this change, no further action is needed.
  </p>
  <p style="color:#fca5a5;font-size:15px;line-height:1.6">
    If you didn't add this passkey, please <a href="https://app.coderhelm.com/settings" style="color:#60a5fa">review your security settings</a> immediately.
  </p>
`);

const PASSKEY_ADDED_TEXT = `Passkey added to your Coderhelm account

A new passkey was registered on your account ({{email}}).

Device: {{device_name}}
Date: {{date}}

If you made this change, no action is needed.
If you didn't, review your security settings: https://app.coderhelm.com/settings`;

const PASSKEY_REMOVED_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#fff;font-size:20px;margin:0 0 16px">Passkey removed from your account</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    A passkey was removed from your Coderhelm account (<strong style="color:#fff">{{email}}</strong>).
  </p>
  <table style="width:100%;border-collapse:collapse;margin:16px 0">
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Device</td><td style="color:#fff;padding:4px 0;font-size:14px">{{device_name}}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:14px">Date</td><td style="color:#fff;padding:4px 0;font-size:14px">{{date}}</td></tr>
  </table>
  <p style="color:#ccc;font-size:15px;line-height:1.6">
    If you made this change, no further action is needed.
  </p>
  <p style="color:#fca5a5;font-size:15px;line-height:1.6">
    If you didn't remove this passkey, please <a href="https://app.coderhelm.com/settings" style="color:#60a5fa">review your security settings</a> immediately and change your password.
  </p>
`);

const PASSKEY_REMOVED_TEXT = `Passkey removed from your Coderhelm account

A passkey was removed from your account ({{email}}).

Device: {{device_name}}
Date: {{date}}

If you made this change, no action is needed.
If you didn't, review your security settings immediately and change your password: https://app.coderhelm.com/settings`;

