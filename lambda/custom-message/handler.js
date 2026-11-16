// Cognito custom-message trigger: the sign-up, resend and reset-password
// codes in the install's own SES templates. Any problem loading a template
// leaves Cognito's default message, so a code always goes out.

const PREFIX = process.env.SES_TEMPLATE_PREFIX;

let real;

/** SES v2 (the Lambda runtime's SDK), made on first use. */
function sesClient() {
  if (!real) {
    const { SESv2Client, GetEmailTemplateCommand } = require("@aws-sdk/client-sesv2");
    const c = new SESv2Client({});
    real = { getTemplate: (name) => c.send(new GetEmailTemplateCommand({ TemplateName: name })) };
  }
  return real;
}

/** The template and subject for a trigger, or null for Cognito's own. */
function pick(triggerSource) {
  switch (triggerSource) {
    case "CustomMessage_SignUp":
    case "CustomMessage_ResendCode":
      return { template: `${PREFIX}-verify-email`, subject: "Verify your Coderhelm email" };
    case "CustomMessage_ForgotPassword":
      return { template: `${PREFIX}-reset-password`, subject: "Reset your Coderhelm password" };
    default:
      return null;
  }
}

/**
 * The email body from a GetEmailTemplate answer. SES v2 answers
 * `{ TemplateContent: { Subject, Text, Html } }` (strings). Null when it has
 * no HTML or no place for the code: Cognito refuses a message without it.
 */
function messageFrom(answer, code) {
  const html = answer && answer.TemplateContent && answer.TemplateContent.Html;
  if (typeof html !== "string" || !html.includes("{{code}}")) return null;
  return html.replace(/\{\{code\}\}/g, code);
}

async function run(event, ses) {
  const chosen = pick(event.triggerSource);
  if (!chosen) return event;
  const code = event.request.codeParameter;
  try {
    const message = messageFrom(await ses.getTemplate(chosen.template), code);
    if (message) {
      event.response.emailSubject = chosen.subject;
      event.response.emailMessage = message;
    } else {
      console.error(`SES template ${chosen.template} has no HTML with {{code}}; Cognito's default message goes out`);
    }
  } catch (err) {
    console.error(`SES template ${chosen.template} not loaded; Cognito's default message goes out:`, err);
  }
  return event;
}

/** Lambda's entry point: only `event` is read (the runtime passes more). */
exports.handler = async (event) => run(event, sesClient());
exports.run = run;
exports.messageFrom = messageFrom;
exports.pick = pick;
