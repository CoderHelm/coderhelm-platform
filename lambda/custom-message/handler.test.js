// node --test lambda/custom-message/handler.test.js
process.env.SES_TEMPLATE_PREFIX = "coderhelm-test";
const test = require("node:test");
const assert = require("node:assert/strict");
const { run, messageFrom } = require("./handler.js");

const event = (triggerSource) => ({
  triggerSource,
  request: { codeParameter: "{####}", userAttributes: { email: "a@example.com" } },
  response: {},
});

// What SES v2 GetEmailTemplate really answers.
const sesAnswer = {
  TemplateName: "coderhelm-test-verify-email",
  TemplateContent: { Subject: "Verify", Text: "Code {{code}}", Html: "<p>Code {{code}}</p>" },
};

test("the SES v2 template answer becomes the message, with Cognito's code placeholder", async () => {
  // The failure: the handler read Template.Content, which SES v2 never
  // returns, so every code went out as Cognito's default text.
  let asked;
  const ses = { getTemplate: async (name) => ((asked = name), sesAnswer) };
  const out = await run(event("CustomMessage_ResendCode"), ses);
  assert.equal(asked, "coderhelm-test-verify-email");
  assert.equal(out.response.emailMessage, "<p>Code {####}</p>");
  assert.equal(out.response.emailSubject, "Verify your Coderhelm email");
  const reset = await run(event("CustomMessage_ForgotPassword"), { getTemplate: async (n) => ((asked = n), sesAnswer) });
  assert.equal(asked, "coderhelm-test-reset-password");
  assert.equal(reset.response.emailSubject, "Reset your Coderhelm password");
});

test("a missing or broken template leaves Cognito's default message", async () => {
  const quiet = console.error;
  console.error = () => {};
  try {
    const missing = await run(event("CustomMessage_SignUp"), {
      getTemplate: async () => {
        throw new Error("NotFoundException");
      },
    });
    assert.deepEqual(missing.response, {});
    const noCode = await run(event("CustomMessage_SignUp"), {
      getTemplate: async () => ({ TemplateContent: { Html: "<p>no code</p>" } }),
    });
    assert.deepEqual(noCode.response, {});
  } finally {
    console.error = quiet;
  }
  assert.equal(messageFrom({ Template: { Content: {} } }, "{####}"), null);
});

test("other triggers are left to Cognito", async () => {
  const ses = { getTemplate: async () => assert.fail("not asked") };
  const out = await run(event("CustomMessage_AdminCreateUser"), ses);
  assert.deepEqual(out.response, {});
});
