const { SESv2Client, GetEmailTemplateCommand } = require("@aws-sdk/client-sesv2");

const ses = new SESv2Client({});
const PREFIX = process.env.SES_TEMPLATE_PREFIX;
const FROM = process.env.SES_FROM_ADDRESS;

exports.handler = async (event) => {
  const { triggerSource, request, userName } = event;

  const code = request.codeParameter;
  const email = request.userAttributes?.email || "";

  let templateName;
  let subject;

  switch (triggerSource) {
    case "CustomMessage_SignUp":
    case "CustomMessage_ResendCode":
      templateName = `${PREFIX}-verify-email`;
      subject = "Verify your Coderhelm email";
      break;
    case "CustomMessage_ForgotPassword":
      templateName = `${PREFIX}-reset-password`;
      subject = "Reset your Coderhelm password";
      break;
    default:
      // For unhandled triggers, return default Cognito behavior
      return event;
  }

  try {
    const { Template } = await ses.send(
      new GetEmailTemplateCommand({ TemplateName: templateName })
    );

    // Replace {{code}} placeholder with the actual code
    const html = Template.Content.Html.Data.replace(/\{\{code\}\}/g, code);
    const text = Template.Content.Text.Data.replace(/\{\{code\}\}/g, code);

    event.response.emailSubject = subject;
    event.response.emailMessage = html;
  } catch (err) {
    console.error("Failed to load SES template, falling back to default:", err);
    // Fall through — Cognito will use its default message
  }

  return event;
};
