import * as cdk from "aws-cdk-lib";
import * as ses from "aws-cdk-lib/aws-ses";
import * as events from "aws-cdk-lib/aws-events";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as route53 from "aws-cdk-lib/aws-route53";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";

interface EmailStackProps extends cdk.StackProps {
  stage: string;
  cfg: ResolvedConfig;
  /** The dashboard's URL, for links in emails. */
  appUrl: string;
  gatewayFunction: lambda.Function;
  workerFunction: lambda.Function;
  /** Self-hosted: publish the configuration set's events to the default event bus (ApiStack's EmailEventsRule takes them). */
  emailEvents?: boolean;
}

export class EmailStack extends cdk.Stack {
  public readonly configSet: ses.ConfigurationSet;

  constructor(scope: Construct, id: string, props: EmailStackProps) {
    super(scope, id, props);

    const cfg = props.cfg;
    const prefix = cfg.prefix;
    // Fills the deployment's links into a template (see the bottom of this file).
    const fill = (t: string) =>
      t
        .split("__APP_URL__").join(props.appUrl)
        .split("__LOGO_URL__").join(cfg.email.logoUrl ?? `${props.appUrl}/favicon.svg`)
        .split("__PRIVACY_LINK__").join(
          cfg.email.privacyUrl ? ` · <a href="${cfg.email.privacyUrl}" style="color:#888">Privacy</a>` : ""
        )
        .split("__SUPPORT_INBOX__").join(cfg.email.supportInbox);

    // SES configuration set for tracking
    this.configSet = new ses.ConfigurationSet(this, "ConfigSet", {
      configurationSetName: `${prefix}-emails`,
      reputationMetrics: true,
      sendingEnabled: true,
    });
    // A self-hosted install counts its own email by kind (Deliveries): every
    // send, delivery, bounce, complaint and rejection goes to its worker
    // through the default event bus (ApiStack's EmailEventsRule).
    // Mail is sent through this set as the identity's default, Cognito's
    // own sign-up and reset email included once it uses SES.
    if (props.emailEvents) {
      this.configSet.addEventDestination("Events", {
        destination: ses.EventDestination.eventBus(
          events.EventBus.fromEventBusName(this, "DefaultBus", "default")
        ),
        events: [
          ses.EmailSendingEvent.SEND,
          ses.EmailSendingEvent.DELIVERY,
          ses.EmailSendingEvent.BOUNCE,
          ses.EmailSendingEvent.COMPLAINT,
          ses.EmailSendingEvent.REJECT,
        ],
      });
    }

    // SES email identity: a domain (its DKIM records are created for you
    // when it is the single-origin domain's Route 53 zone; otherwise add the
    // three CNAMEs SES shows) or one address (SES emails it a confirmation
    // link). null: the identity is managed elsewhere.
    const identity = cfg.email.identity;
    if (identity) {
      const zoneName = cfg.singleOrigin?.hostedZone;
      let sesIdentity: ses.Identity;
      if (cfg.native) {
        // A native release: the sender is a launch parameter (an address).
        sesIdentity = ses.Identity.email(identity);
      } else if (identity.includes("@")) {
        sesIdentity = ses.Identity.email(identity);
      } else if (zoneName && identity === zoneName) {
        const zone = cfg.singleOrigin?.hostedZoneId
          ? route53.PublicHostedZone.fromPublicHostedZoneAttributes(this, "EmailZone", {
              zoneName,
              hostedZoneId: cfg.singleOrigin.hostedZoneId,
            })
          : route53.PublicHostedZone.fromLookup(this, "EmailZone", { domainName: zoneName });
        sesIdentity = ses.Identity.publicHostedZone(zone as route53.IPublicHostedZone);
      } else {
        sesIdentity = ses.Identity.domain(identity);
      }
      const made = new ses.EmailIdentity(this, "DomainIdentity", {
        identity: sesIdentity,
        configurationSet: this.configSet,
      });
      if (cfg.native) {
        // Only when the install sends through its own SES (EmailTransport=ses).
        const usesSes = new cdk.CfnCondition(this, "UsesSes", { expression: cdk.Fn.conditionEquals(cdk.Fn.ref("EmailTransport"), "ses") });
        (made.node.defaultChild as cdk.CfnResource).cfnOptions.condition = usesSes;
      }
    }

    // Templated emails
    new ses.CfnTemplate(this, "WelcomeTemplate", {
      template: {
        templateName: `${prefix}-welcome`,
        subjectPart: "Welcome to Coderhelm — your AI coding agent is ready",
        htmlPart: fill(WELCOME_HTML),
        textPart: fill(WELCOME_TEXT),
      },
    });

    new ses.CfnTemplate(this, "RunCompleteTemplate", {
      template: {
        templateName: `${prefix}-run-complete`,
        subjectPart: "Coderhelm completed: {{title}}",
        htmlPart: fill(RUN_COMPLETE_HTML),
        textPart: fill(RUN_COMPLETE_TEXT),
      },
    });

    new ses.CfnTemplate(this, "RunFailedTemplate", {
      template: {
        templateName: `${prefix}-run-failed`,
        subjectPart: "Coderhelm failed: {{title}}",
        htmlPart: fill(RUN_FAILED_HTML),
        textPart: fill(RUN_FAILED_TEXT),
      },
    });

    // A webhook endpoint failed for days and was turned off (team admins).
    new ses.CfnTemplate(this, "WebhookDisabledTemplate", {
      template: {
        templateName: `${prefix}-webhook-disabled`,
        subjectPart: "CoderHelm turned off a webhook endpoint: {{host}}",
        htmlPart: fill(WEBHOOK_DISABLED_HTML),
        textPart: fill(WEBHOOK_DISABLED_TEXT),
      },
    });

    new ses.CfnTemplate(this, "WeeklySummaryTemplate", {
      template: {
        templateName: `${prefix}-weekly-summary`,
        subjectPart: "Your Coderhelm weekly summary",
        htmlPart: fill(WEEKLY_SUMMARY_HTML),
        textPart: fill(WEEKLY_SUMMARY_TEXT),
      },
    });

    // --- Account email templates ---

    new ses.CfnTemplate(this, "ResetPasswordTemplate", {
      template: {
        templateName: `${prefix}-reset-password`,
        subjectPart: "Reset your Coderhelm password",
        htmlPart: fill(RESET_PASSWORD_HTML),
        textPart: fill(RESET_PASSWORD_TEXT),
      },
    });

    new ses.CfnTemplate(this, "TeamInviteTemplate", {
      template: {
        templateName: `${prefix}-team-invite`,
        subjectPart: "You've been invited to {{org}} on Coderhelm",
        htmlPart: fill(TEAM_INVITE_HTML),
        textPart: fill(TEAM_INVITE_TEXT),
      },
    });

    new ses.CfnTemplate(this, "VerifyEmailTemplate", {
      template: {
        templateName: `${prefix}-verify-email`,
        subjectPart: "Verify your Coderhelm email",
        htmlPart: fill(VERIFY_EMAIL_HTML),
        textPart: fill(VERIFY_EMAIL_TEXT),
      },
    });

    new ses.CfnTemplate(this, "PasswordChangedTemplate", {
      template: {
        templateName: `${prefix}-password-changed`,
        subjectPart: "Your Coderhelm password was changed",
        htmlPart: fill(PASSWORD_CHANGED_HTML),
        textPart: fill(PASSWORD_CHANGED_TEXT),
      },
    });

    new ses.CfnTemplate(this, "PasskeyAddedTemplate", {
      template: {
        templateName: `${prefix}-passkey-added`,
        subjectPart: "A passkey was added to your Coderhelm account",
        htmlPart: fill(PASSKEY_ADDED_HTML),
        textPart: fill(PASSKEY_ADDED_TEXT),
      },
    });

    new ses.CfnTemplate(this, "PasskeyRemovedTemplate", {
      template: {
        templateName: `${prefix}-passkey-removed`,
        subjectPart: "A passkey was removed from your Coderhelm account",
        htmlPart: fill(PASSKEY_REMOVED_HTML),
        textPart: fill(PASSKEY_REMOVED_TEXT),
      },
    });

    // Token limit emails: one per limit (new tokens, cache read, cache write)
    // at 80% and at 100%. {{limit_name}} names the limit.
    new ses.CfnTemplate(this, "TokenWarningTemplate", {
      template: {
        templateName: `${prefix}-token-warning`,
        subjectPart: "Coderhelm: {{usage_pct}}% of your monthly {{limit_name}} limit used",
        htmlPart: fill(TOKEN_WARNING_HTML),
        textPart: fill(TOKEN_WARNING_TEXT),
      },
    });

    new ses.CfnTemplate(this, "TokenLimitReachedTemplate", {
      template: {
        templateName: `${prefix}-token-limit-reached`,
        subjectPart: "Coderhelm: monthly {{limit_name}} limit reached",
        htmlPart: fill(TOKEN_LIMIT_REACHED_HTML),
        textPart: fill(TOKEN_LIMIT_REACHED_TEXT),
      },
    });

    // Grant SES send permissions to both lambdas
    const sesPolicy = new iam.PolicyStatement({
      actions: ["ses:SendEmail", "ses:SendTemplatedEmail"],
      resources: ["*"],
      conditions: {
        StringEquals: {
          "ses:FromAddress": cfg.email.from,
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
  <img src="__LOGO_URL__" width="32" height="32" alt="Coderhelm" style="display:block;margin-bottom:24px">
  ${body}
  <p style="color:#666;font-size:12px;margin-top:32px;padding-top:16px;border-top:1px solid #222">
    You're receiving this because you have notifications enabled for your Coderhelm account.<br>
    <a href="__APP_URL__/settings" style="color:#888">Manage notification preferences</a>__PRIVACY_LINK__
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
  <a href="__APP_URL__/settings/github" style="display:inline-block;background:#fff;color:#000;padding:12px 24px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:16px">Connect GitHub</a>
`);

const WELCOME_TEXT = `Welcome to Coderhelm!

Your AI coding agent is ready. Connect your GitHub organization and install the Coderhelm app to get started.

Once installed, assign an issue to coderhelm[bot] or add the "coderhelm" label.

Connect GitHub: __APP_URL__/settings/github

Manage notifications: __APP_URL__/settings`;

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
  <a href="__APP_URL__" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">View Run</a>
`);

const RUN_COMPLETE_TEXT = `Run completed: {{title}}
Repo: {{repo}}
PR: {{pr_url}}
Files: {{files_modified}} modified
Duration: {{duration}}
Tokens: {{tokens}}

View run: __APP_URL__`;

const RUN_FAILED_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#ef4444;font-size:20px;margin:0 0 16px">✗ Run failed</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    <strong style="color:#fff">{{title}}</strong> on <code style="background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#ccc">{{repo}}</code>
  </p>
  <div style="background:#1a0000;border:1px solid #3f0000;border-radius:6px;padding:16px;margin:16px 0">
    <p style="color:#fca5a5;font-size:14px;margin:0;font-family:monospace">{{error}}</p>
  </div>
  <a href="__APP_URL__" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">View Run</a>
`);

const RUN_FAILED_TEXT = `Run failed: {{title}}
Repo: {{repo}}
Error: {{error}}

View run: __APP_URL__`;

const WEBHOOK_DISABLED_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#f59e0b;font-size:20px;margin:0 0 16px">A webhook endpoint was turned off</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    Every delivery to <code style="background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#ccc;word-break:break-word">{{url}}</code> has failed since {{failing_since}} ({{days}} days), so CoderHelm stopped sending to it.
  </p>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">{{description}}</p>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    Fix the endpoint, turn it back on in Settings, Webhooks, and replay the events it missed (they are kept 14 days).
  </p>
  <a href="__APP_URL__/settings/webhooks" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">Open Webhooks</a>
`);

const WEBHOOK_DISABLED_TEXT = `A webhook endpoint was turned off

Every delivery to {{url}} has failed since {{failing_since}} ({{days}} days), so CoderHelm stopped sending to it.
{{description}}

Fix the endpoint, turn it back on in Settings, Webhooks, and replay the events it missed (they are kept 14 days).

Open Webhooks: __APP_URL__/settings/webhooks`;

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
  <a href="__APP_URL__" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:24px">Open Dashboard</a>
`);

const WEEKLY_SUMMARY_TEXT = `Weekly Summary — {{org}} ({{period}})

Total runs: {{total_runs}}
Completed: {{completed}}
Failed: {{failed}}
PRs created: {{prs_created}}
Total cost: \${{total_cost}}

Dashboard: __APP_URL__`;

// ─── Token limit templates ──────────────────────────────────────────

const TOKEN_WARNING_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#f59e0b;font-size:20px;margin:0 0 16px">{{usage_pct}}% of your {{limit_name}} limit used</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    Your team has used <strong style="color:#fff">{{used_tokens}}</strong> of its <strong style="color:#fff">{{limit_tokens}}</strong> monthly {{limit_name}} limit for {{month}}.
    When it reaches the limit, Coderhelm stops starting new work until the month ends or the limit is raised.
  </p>
  <a href="__APP_URL__/settings/budget" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">Review token limits</a>
`);

const TOKEN_WARNING_TEXT = `{{usage_pct}}% of your {{limit_name}} limit used

Your team has used {{used_tokens}} of its {{limit_tokens}} monthly {{limit_name}} limit for {{month}}.
When it reaches the limit, Coderhelm stops starting new work until the month ends or the limit is raised.

Token limits: __APP_URL__/settings/budget`;

const TOKEN_LIMIT_REACHED_HTML = EMAIL_WRAPPER(`
  <h1 style="color:#ef4444;font-size:20px;margin:0 0 16px">Monthly {{limit_name}} limit reached</h1>
  <p style="color:#ccc;font-size:15px;line-height:1.6;margin:0 0 16px">
    Your team has used <strong style="color:#fff">{{used_tokens}}</strong> of its <strong style="color:#fff">{{limit_tokens}}</strong> monthly {{limit_name}} limit for {{month}}.
    Coderhelm won't start new work until the month ends or an admin raises or clears this limit.
  </p>
  <a href="__APP_URL__/settings/budget" style="display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px">Review token limits</a>
`);

const TOKEN_LIMIT_REACHED_TEXT = `Monthly {{limit_name}} limit reached

Your team has used {{used_tokens}} of its {{limit_tokens}} monthly {{limit_name}} limit for {{month}}.
Coderhelm won't start new work until the month ends or an admin raises or clears this limit.

Token limits: __APP_URL__/settings/budget`;

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
  <a href="__APP_URL__" style="display:inline-block;background:#fff;color:#000;padding:12px 24px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:16px">Accept Invite</a>
`);

const TEAM_INVITE_TEXT = `You've been invited to {{org}} on Coderhelm

{{inviter}} invited you as a {{role}}.

Accept your invite: __APP_URL__`;

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
    If you didn't change your password, please <a href="__APP_URL__" style="color:#60a5fa">reset it immediately</a> and contact us at __SUPPORT_INBOX__.
  </p>
`);

const PASSWORD_CHANGED_TEXT = `Your Coderhelm password was changed

The password for your account ({{email}}) was just changed.

If you made this change, no action is needed.
If you didn't, reset your password immediately: __APP_URL__

Contact: __SUPPORT_INBOX__`;

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
    If you didn't add this passkey, please <a href="__APP_URL__/settings" style="color:#60a5fa">review your security settings</a> immediately.
  </p>
`);

const PASSKEY_ADDED_TEXT = `Passkey added to your Coderhelm account

A new passkey was registered on your account ({{email}}).

Device: {{device_name}}
Date: {{date}}

If you made this change, no action is needed.
If you didn't, review your security settings: __APP_URL__/settings`;

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
    If you didn't remove this passkey, please <a href="__APP_URL__/settings" style="color:#60a5fa">review your security settings</a> immediately and change your password.
  </p>
`);

const PASSKEY_REMOVED_TEXT = `Passkey removed from your Coderhelm account

A passkey was removed from your account ({{email}}).

Device: {{device_name}}
Date: {{date}}

If you made this change, no action is needed.
If you didn't, review your security settings immediately and change your password: __APP_URL__/settings`;

