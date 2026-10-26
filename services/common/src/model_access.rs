//! Can this install call a model, and if not, what exactly a person must do.
//!
//! One test call per model (one token out) answers it. The answer is mapped
//! here to a code, one sentence on what is wrong and one on the fix, with
//! the AWS or Anthropic console page to do it on. Setup, the provider switch
//! in Settings, System, Models, and the updater's check before an update all
//! show the same words.
//!
//! CoderHelm never accepts terms for anyone: agreeing to Anthropic's model
//! terms, submitting Bedrock's use case form and the AWS Marketplace
//! subscription stay a person's action in the AWS console. The install's
//! roles can't do them (no `aws-marketplace:Subscribe`); these fixes only
//! link to the page and the next check sees the result.

use serde::Serialize;

use crate::model_provider::Provider;

/// What one test call found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Access {
    /// The model's name (`claude-opus-5-5`).
    pub model: String,
    /// What was called: the Bedrock id, or the name on the Anthropic API.
    pub id: String,
    /// `pass`, `fail`, or `skip` (nothing to check with).
    pub status: &'static str,
    /// `ok`, or why not (see [`refusal_code`]).
    pub code: &'static str,
    /// What is true now, in one sentence.
    pub detail: String,
    /// The fix in one sentence (empty when it passes).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub fix: String,
    /// Where to do it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// A dashboard page that helps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub href: Option<String>,
}

/// A test call's raw answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Ok,
    /// The provider refused: its status, and what it said.
    Refused(u16, String),
    /// No answer (network, timeout).
    Unreachable(String),
    /// Anthropic API: no key to check with.
    NoKey,
}

/// Bedrock's console page where model access, the use case form and the
/// model terms are handled.
pub fn bedrock_access_url(region: &str) -> String {
    format!("https://console.aws.amazon.com/bedrock/home?region={region}#/modelaccess")
}

/// Where Bedrock lists which models each region offers.
pub const BEDROCK_REGIONS_URL: &str =
    "https://docs.aws.amazon.com/bedrock/latest/userguide/models-regions.html";
pub const IAM_ROLES_URL: &str = "https://console.aws.amazon.com/iam/home#/roles";
pub const AWS_HEALTH_URL: &str = "https://health.aws.amazon.com/health/status";
pub const ANTHROPIC_KEYS_URL: &str = "https://console.anthropic.com/settings/keys";
pub const ANTHROPIC_STATUS_URL: &str = "https://status.anthropic.com";

/// Bedrock's refusal as a code, one for each fix.
pub fn refusal_code(status: u16, message: &str) -> &'static str {
    let m = message.to_ascii_lowercase();
    if m.contains("use case") || m.contains("use-case") {
        "use_case_required"
    } else if m.contains("being processed") || m.contains("subscription") && m.contains("pending") {
        "subscription_pending"
    } else if m.contains("marketplace")
        || m.contains("payment")
        || m.contains("agreement")
        || m.contains("subscri")
    {
        "marketplace_failed"
    } else if m.contains("not authorized to perform") || m.contains("explicit deny") {
        "iam_denied"
    } else if m.contains("don't have access")
        || m.contains("do not have access")
        || m.contains("access to the model")
    {
        "access_denied"
    } else if m.contains("not available")
        || m.contains("not supported")
        || m.contains("model identifier is invalid")
        || status == 404
    {
        "not_available"
    } else if status == 403 {
        "access_denied"
    } else if status == 429 || status >= 500 {
        "transient"
    } else {
        "unknown"
    }
}

/// One test call's answer, explained.
pub fn explain(
    provider: Provider,
    model: &str,
    id: &str,
    region: &str,
    prefix: &str,
    answer: &Answer,
) -> Access {
    let base = |status, code, detail: String, fix: String, url: Option<String>| Access {
        model: model.to_string(),
        id: id.to_string(),
        status,
        code,
        detail,
        fix,
        url,
        href: None,
    };
    let via = match provider {
        Provider::Bedrock => "Amazon Bedrock",
        Provider::Anthropic => "the Anthropic API",
    };
    match (provider, answer) {
        (_, Answer::Ok) => base(
            "pass",
            "ok",
            format!("{id} answered through {via}."),
            String::new(),
            None,
        ),
        (_, Answer::NoKey) => Access {
            href: Some("/settings/models".into()),
            ..base(
                "skip",
                "no_key",
                format!("No Anthropic API key to check {model} with."),
                "Add your team's key in Settings, AI models, then check again.".into(),
                Some(ANTHROPIC_KEYS_URL.into()),
            )
        },
        (_, Answer::Unreachable(_)) => base(
            "fail",
            "unreachable",
            format!("The test call to {id} got no answer from {via}."),
            "Check again in a minute; if it keeps failing, check the provider's status page."
                .into(),
            Some(match provider {
                Provider::Bedrock => AWS_HEALTH_URL.into(),
                Provider::Anthropic => ANTHROPIC_STATUS_URL.into(),
            }),
        ),
        (Provider::Anthropic, Answer::Refused(status, _)) => match status {
            401 | 403 => Access {
                href: Some("/settings/models".into()),
                ..base(
                    "fail",
                    "key_refused",
                    format!("Anthropic refused the API key the test call to {model} used."),
                    "Create a new key in the Anthropic console and save it in Settings, AI models."
                        .into(),
                    Some(ANTHROPIC_KEYS_URL.into()),
                )
            },
            404 => base(
                "fail",
                "not_available",
                format!("Anthropic doesn't offer {model} to this key."),
                "Check that the key belongs to an organization that can use this model.".into(),
                Some(ANTHROPIC_KEYS_URL.into()),
            ),
            s => base(
                "fail",
                if *s == 429 || *s >= 500 {
                    "transient"
                } else {
                    "unknown"
                },
                format!("The test call to {model} answered {s}."),
                "Check again in a minute; if it keeps failing, check Anthropic's status page."
                    .into(),
                Some(ANTHROPIC_STATUS_URL.into()),
            ),
        },
        (Provider::Bedrock, Answer::Refused(status, msg)) => {
            let code = refusal_code(*status, msg);
            let access = bedrock_access_url(region);
            let (detail, fix, url) = match code {
                "access_denied" => (
                    format!("Bedrock refused {id} in {region}: model access isn't enabled."),
                    format!("Open Model access in the Bedrock console for {region}, enable {model}, then check again."),
                    access,
                ),
                "use_case_required" => (
                    format!("Bedrock refused {id}: Anthropic's use case details haven't been submitted for this AWS account."),
                    "Submit the use case details on the Bedrock console's Model access page (a person fills in the form once per account), then check again.".into(),
                    access,
                ),
                "subscription_pending" => (
                    format!("Bedrock is still setting up the subscription for {model}."),
                    "Wait a few minutes for AWS to finish, then check again.".into(),
                    access,
                ),
                "marketplace_failed" => (
                    format!("Bedrock refused {id}: Anthropic's terms for this model haven't been accepted in AWS Marketplace."),
                    format!("Have someone allowed to subscribe in AWS Marketplace accept the terms for {model} on the Bedrock console's Model access page, then check again."),
                    access,
                ),
                "iam_denied" => (
                    format!("The CoderHelm roles aren't allowed to call {id}."),
                    "Deploy the install again so its roles may call Bedrock models, or remove the policy that denies it, then check again.".into(),
                    IAM_ROLES_URL.into(),
                ),
                "not_available" => (
                    format!("{id} isn't offered in {region}{}.", if prefix.is_empty() { String::new() } else { format!(" with the {prefix} inference profile") }),
                    format!("Pick the inference profile prefix for your region, or set the exact Bedrock id for {model} under Advanced."),
                    BEDROCK_REGIONS_URL.into(),
                ),
                _ => (
                    format!("Bedrock answered {status} to the test call to {id}."),
                    "Check again in a minute; if it keeps failing, check the AWS Health Dashboard.".into(),
                    AWS_HEALTH_URL.into(),
                ),
            };
            base("fail", code, detail, fix, Some(url))
        }
    }
}

/// Whether every result lets the work go ahead (a skip is not a refusal).
pub fn all_pass(results: &[Access]) -> bool {
    results.iter().all(|r| r.status != "fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bedrock(msg: &str, status: u16) -> Access {
        explain(
            Provider::Bedrock,
            "claude-opus-5-5",
            "us.anthropic.claude-opus-5-5",
            "us-east-1",
            "us.",
            &Answer::Refused(status, msg.into()),
        )
    }

    #[test]
    fn every_bedrock_refusal_has_its_own_fix_and_link() {
        let cases = [
            ("You don't have access to the model with the specified model ID.", 403, "access_denied", "Model access", "#/modelaccess"),
            ("Model use case details have not been submitted for this account.", 400, "use_case_required", "use case details", "#/modelaccess"),
            ("Your request is being processed", 400, "subscription_pending", "Wait a few minutes", "#/modelaccess"),
            ("aws-marketplace:Subscribe failed: no valid payment method", 403, "marketplace_failed", "accept the terms", "#/modelaccess"),
            ("Model access agreement not accepted", 403, "marketplace_failed", "accept the terms", "#/modelaccess"),
            ("User: arn:aws:sts::111122223333:assumed-role/x is not authorized to perform: bedrock:Converse", 403, "iam_denied", "call Bedrock models", "iam/home"),
            ("The provided model identifier is invalid.", 400, "not_available", "inference profile prefix", "models-regions"),
            ("Throttled", 429, "transient", "Check again", "health.aws"),
            ("boom", 400, "unknown", "Check again", "health.aws"),
        ];
        for (msg, status, code, fix, url) in cases {
            let a = bedrock(msg, status);
            assert_eq!(a.status, "fail", "{msg}");
            assert_eq!(a.code, code, "{msg}");
            assert!(a.fix.contains(fix), "{msg}: {}", a.fix);
            assert!(a.url.as_deref().unwrap().contains(url), "{msg}");
            assert!(!a.detail.is_empty());
        }
    }

    #[test]
    fn the_fixes_never_claim_coderhelm_accepts_terms() {
        for code_msg in ["use case", "marketplace", "agreement"] {
            let a = bedrock(code_msg, 403);
            assert!(!a.fix.to_lowercase().contains("coderhelm will"));
            assert!(a.fix.contains("console") || a.fix.contains("Model access"));
        }
    }

    #[test]
    fn anthropic_answers_are_explained() {
        let a = |ans: Answer| {
            explain(
                Provider::Anthropic,
                "claude-sonnet-5-5",
                "claude-sonnet-5-5",
                "us-east-1",
                "",
                &ans,
            )
        };
        assert_eq!(a(Answer::Ok).status, "pass");
        assert!(a(Answer::Ok).detail.contains("the Anthropic API"));
        assert_eq!(a(Answer::Refused(401, String::new())).code, "key_refused");
        assert_eq!(a(Answer::Refused(404, String::new())).code, "not_available");
        assert_eq!(a(Answer::Refused(529, String::new())).code, "transient");
        let nokey = a(Answer::NoKey);
        assert_eq!((nokey.status, nokey.code), ("skip", "no_key"));
        assert_eq!(nokey.href.as_deref(), Some("/settings/models"));
        assert_eq!(a(Answer::Unreachable("x".into())).code, "unreachable");
        assert!(all_pass(&[a(Answer::Ok), nokey]));
        assert!(!all_pass(&[a(Answer::Refused(401, String::new()))]));
    }
}
