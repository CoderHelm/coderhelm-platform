//! The updater's model check (lambda/updater): before a release is applied,
//! every model it calls gets one test call through the install's provider
//! as it is now (Settings, System, Models), and each answer comes back
//! explained (common::model_access) for Settings, System and Setup.
//!
//! The updater sends the release's models with their Bedrock ids for each
//! inference profile prefix (the release's `models.json`), so a new model
//! is checked by the id the new release will call. An id an admin set under
//! Advanced still wins. Nothing here accepts terms or subscribes to
//! anything: a refusal is reported, and a person fixes it in the console.

use std::collections::BTreeMap;

use common::model_access::{self, Access, Answer};
use common::model_provider::{self as mp, Provider, Settings};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
pub struct Request {
    pub targets: Vec<Target>,
    /// The team whose key passed the last switch to Anthropic API keys.
    #[serde(default)]
    pub probe_team: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Target {
    pub model: String,
    /// The new release's Bedrock id for each inference profile prefix.
    #[serde(default)]
    pub bedrock_ids: BTreeMap<String, String>,
}

/// The id a target is called by under `s`.
pub fn target_id(s: &Settings, t: &Target) -> String {
    match s.provider {
        Provider::Anthropic => t.model.clone(),
        Provider::Bedrock => s
            .model_ids
            .get(&t.model)
            .cloned()
            .or_else(|| t.bedrock_ids.get(&s.inference_prefix).cloned())
            .unwrap_or_else(|| s.bedrock_id(&t.model)),
    }
}

fn answer_of(r: Result<(), Box<dyn std::error::Error + Send + Sync>>) -> Answer {
    use common::model_retry::{ModelCallError, ModelErrorKind};
    match r {
        Ok(()) => Answer::Ok,
        Err(e) => match e.downcast_ref::<ModelCallError>() {
            Some(m) => match m.kind {
                ModelErrorKind::Status(code) => Answer::Refused(code, m.message.clone()),
                ModelErrorKind::Network => Answer::Unreachable(m.message.clone()),
                // Not sent: the key's team is at its monthly token limit.
                // The key was accepted when it was saved, and a release
                // update must not wait on one team's limit.
                ModelErrorKind::LimitReached => {
                    tracing::warn!(message = %m.message, "Model check skipped: token limit reached");
                    Answer::Ok
                }
                // It answered (cut at one token, or declined "Reply with OK").
                _ => Answer::Ok,
            },
            None => Answer::Unreachable(e.to_string()),
        },
    }
}

/// The settings, ids and results as the updater stores them.
pub fn report(s: &Settings, region: &str, results: &[Access]) -> Value {
    json!({
        "provider": s.provider.as_str(),
        "inference_prefix": s.inference_prefix,
        "region": region,
        "ok": model_access::all_pass(results),
        "results": results,
    })
}

pub async fn run(state: &crate::WorkerState, req: Request) -> Value {
    mp::refresh(&state.dynamo, &state.config.settings_table_name).await;
    let s = mp::current();
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let analytics = std::env::var("ANALYTICS_TABLE_NAME").unwrap_or_default();
    let key = match (s.provider, req.probe_team.as_deref()) {
        (Provider::Anthropic, Some(team)) => team_key(state, team).await,
        _ => None,
    };
    // Bedrock calls are the install's own (the setup meter); a key's are its team's.
    let meter_team = match s.provider {
        Provider::Bedrock => "SETUP".to_string(),
        Provider::Anthropic => req.probe_team.clone().unwrap_or_else(|| "SETUP".into()),
    };
    let meter = common::usage::UsageMeter::dynamo(state.dynamo.clone(), &analytics, &meter_team);
    let mut results = Vec::new();
    for t in &req.targets {
        let id = target_id(&s, t);
        let answer = match s.provider {
            Provider::Bedrock => match mp::any_bedrock_client() {
                Some(b) => {
                    let client =
                        crate::agent::anthropic::AnthropicClient::bedrock(b.clone(), meter.clone());
                    let mut usage = crate::models::TokenUsage::default();
                    answer_of(crate::agent::anthropic::probe(&client, &id, &mut usage).await)
                }
                None => Answer::Unreachable("no Bedrock client".into()),
            },
            Provider::Anthropic => match key.as_deref() {
                Some(k) => {
                    let client =
                        crate::agent::anthropic::AnthropicClient::new(k.to_string(), meter.clone());
                    let mut usage = crate::models::TokenUsage::default();
                    answer_of(crate::agent::anthropic::probe(&client, &id, &mut usage).await)
                }
                None => Answer::NoKey,
            },
        };
        results.push(model_access::explain(
            s.provider,
            &t.model,
            &id,
            &region,
            &s.inference_prefix,
            &answer,
        ));
    }
    report(&s, &region, &results)
}

async fn team_key(state: &crate::WorkerState, team: &str) -> Option<String> {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", A::S(team.to_string()))
        .key("sk", A::S("SETTINGS#MODEL_PROVIDER".into()))
        .send()
        .await
        .ok()?
        .item()?
        .get("api_key")?
        .as_s()
        .ok()
        .filter(|k| !k.is_empty())
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::model_retry::{ModelCallError, ModelErrorKind};

    fn target() -> Target {
        serde_json::from_value(json!({
            "model": "claude-opus-5-5",
            "bedrock_ids": {"us.": "us.anthropic.claude-opus-5-5-v2:0", "eu.": "eu.anthropic.claude-opus-5-5-v2:0"}
        }))
        .unwrap()
    }

    #[test]
    fn the_new_release_id_for_the_install_prefix_is_checked() {
        let mut s = Settings {
            provider: Provider::Bedrock,
            inference_prefix: "eu.".into(),
            ..Default::default()
        };
        assert_eq!(
            target_id(&s, &target()),
            "eu.anthropic.claude-opus-5-5-v2:0"
        );
        // A prefix the release didn't list: the usual rule.
        s.inference_prefix = "apac.".into();
        assert_eq!(target_id(&s, &target()), "apac.anthropic.claude-opus-5-5");
        // An admin's override wins.
        s.model_ids.insert(
            "claude-opus-5-5".into(),
            "arn:aws:bedrock:x:1:application-inference-profile/p".into(),
        );
        assert!(target_id(&s, &target()).starts_with("arn:"));
        // Keys: the model's name.
        assert_eq!(
            target_id(&Settings::default(), &target()),
            "claude-opus-5-5"
        );
    }

    #[test]
    fn call_errors_become_answers() {
        let e = |k| {
            Err(Box::new(ModelCallError::new(k, "m")) as Box<dyn std::error::Error + Send + Sync>)
        };
        assert_eq!(answer_of(Ok(())), Answer::Ok);
        assert_eq!(
            answer_of(e(ModelErrorKind::Status(403))),
            Answer::Refused(403, "m".into())
        );
        assert_eq!(
            answer_of(e(ModelErrorKind::Network)),
            Answer::Unreachable("m".into())
        );
        assert_eq!(answer_of(e(ModelErrorKind::Truncated)), Answer::Ok);
        assert_eq!(answer_of(e(ModelErrorKind::LimitReached)), Answer::Ok);
    }

    #[test]
    fn the_report_says_whether_every_model_answered() {
        let s = Settings {
            provider: Provider::Bedrock,
            ..Default::default()
        };
        let refused = model_access::explain(
            Provider::Bedrock,
            "claude-opus-5-5",
            "us.anthropic.claude-opus-5-5",
            "us-east-1",
            "us.",
            &Answer::Refused(400, "use case details have not been submitted".into()),
        );
        let r = report(&s, "us-east-1", &[refused]);
        assert_eq!(r["ok"], false);
        assert_eq!(r["provider"], "bedrock");
        assert_eq!(r["results"][0]["code"], "use_case_required");
    }
}
