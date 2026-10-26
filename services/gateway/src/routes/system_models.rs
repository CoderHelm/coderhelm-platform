//! Settings, System, Models: how this self-hosted install calls Claude.
//!
//! An install admin switches between Claude on Amazon Bedrock (this AWS
//! account, no key) and Anthropic API keys (each team adds its own in
//! Settings, AI models, as before) at any time. The deploy config's choice
//! is only the first-boot default (common::model_provider).
//!
//! A switch is checked before it is saved: every model the release calls
//! (common::model_ids, the release's manifest) gets one test call through
//! the provider being switched to, the same call Setup makes. When any is
//! refused, nothing changes and the answer says which model, the fix in one
//! sentence and the console page. A saved switch is written to the team's
//! audit log first (no record, no switch), then stored; this instance uses
//! it at once, every other within 30 seconds. A model call already under
//! way finishes on the provider it started with.
//!
//! Why Anthropic keys stay per team: the key already lives on each team's
//! settings item, the worker, gateway and log analyzer all read it there,
//! and each team's usage lands on its own Anthropic account. An install-wide
//! key would need a second place every reader checks and would pool every
//! team's usage on one bill. The check uses the switching admin's team key.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{extract::State, http::StatusCode, Extension, Json};
use common::model_access::{self, Access, Answer};
use common::model_provider::{self as mp, Provider, Settings, Source};
use serde_json::{json, Value};
use tracing::{info, warn};

use super::api::is_platform_admin;
use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn not_admin(s: StatusCode) -> (StatusCode, Json<Value>) {
    refuse(
        s,
        "Only team admins can see or change how the install calls Claude.",
    )
}

fn refuse(code: StatusCode, msg: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({ "error": msg })))
}

/// A self-hosted install's admin, or the refusal.
fn allowed(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    if !super::setup_checks::self_hosted() {
        return Err(refuse(
            StatusCode::NOT_FOUND,
            "Only a self-hosted install changes its model provider here.",
        ));
    }
    if !is_platform_admin(claims) {
        warn!(email = %claims.email, "Models route refused: not an install admin");
        return Err(refuse(
            StatusCode::FORBIDDEN,
            "Only the install's admins can change how it calls Claude.",
        ));
    }
    Ok(())
}

fn region() -> String {
    std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into())
}

/// The settings a request asks for, checked.
pub fn requested(body: &Value) -> Result<Settings, String> {
    let provider = body
        .get("provider")
        .and_then(Value::as_str)
        .and_then(Provider::parse)
        .ok_or("Pick Claude on Amazon Bedrock or Anthropic API keys.")?;
    let mut s = Settings {
        provider,
        ..Default::default()
    };
    if provider == Provider::Bedrock {
        if let Some(p) = body.get("inference_prefix") {
            s.inference_prefix = p
                .as_str()
                .ok_or("The inference profile prefix is text.")?
                .trim()
                .to_string();
        }
        if let Some(m) = body.get("model_ids").filter(|v| !v.is_null()) {
            let map = m
                .as_object()
                .ok_or("Model id overrides are a list of model name and Bedrock id.")?;
            for (k, v) in map {
                let id = v.as_str().unwrap_or("").trim();
                if !id.is_empty() {
                    s.model_ids.insert(k.trim().to_string(), id.to_string());
                }
            }
        }
        s.validate()?;
    }
    Ok(s)
}

/// The test calls a switch makes: every model the release calls, each with
/// the id it is called by under `settings`.
pub fn targets(settings: &Settings) -> Vec<(String, String)> {
    common::model_ids::release_calls()
        .into_iter()
        .map(|m| {
            let id = match settings.provider {
                Provider::Bedrock => settings.bedrock_id(m),
                Provider::Anthropic => m.to_string(),
            };
            (m.to_string(), id)
        })
        .collect()
}

/// Makes the test calls (swapped for a fake in tests: no real model calls).
#[async_trait]
pub trait Prober: Send + Sync {
    async fn bedrock(&self, model: &str, bedrock_id: &str) -> Answer;
    async fn anthropic(&self, api_key: &str, model: &str) -> Answer;
}

/// Stores the switch and its audit entry.
#[async_trait]
pub trait Store: Send + Sync {
    /// The team's Anthropic key, when it has one.
    async fn team_key(&self, team_id: &str) -> Option<String>;
    async fn audit(&self, detail: &str) -> Result<(), String>;
    async fn save(&self, record: &Value) -> Result<(), String>;
}

/// Every test call for a switch, explained.
pub async fn check(
    prober: &dyn Prober,
    store: &dyn Store,
    team_id: &str,
    settings: &Settings,
) -> Vec<Access> {
    let region = region();
    let key = if settings.provider == Provider::Anthropic {
        store.team_key(team_id).await
    } else {
        None
    };
    let mut out = Vec::new();
    for (model, id) in targets(settings) {
        let answer = match settings.provider {
            Provider::Bedrock => prober.bedrock(&model, &id).await,
            Provider::Anthropic => match key.as_deref() {
                Some(k) => prober.anthropic(k, &model).await,
                None => Answer::NoKey,
            },
        };
        let mut a = model_access::explain(
            settings.provider,
            &model,
            &id,
            &region,
            &settings.inference_prefix,
            &answer,
        );
        // A switch to keys needs one key that works: without one nothing
        // could run, so it is refused rather than skipped.
        if a.code == "no_key" {
            a.status = "fail";
            a.detail =
                "Your team hasn't added an Anthropic API key, so the switch can't be checked."
                    .into();
        }
        out.push(a);
    }
    out
}

/// One line for the audit log.
pub fn audit_detail(from: &Settings, to: &Settings) -> String {
    let say = |s: &Settings| match s.provider {
        Provider::Anthropic => "Anthropic API keys".to_string(),
        Provider::Bedrock => format!(
            "Claude on Amazon Bedrock ({}{})",
            if s.inference_prefix.is_empty() {
                "in-region ids".to_string()
            } else {
                format!("{} inference profiles", s.inference_prefix)
            },
            if s.model_ids.is_empty() {
                String::new()
            } else {
                format!(", {} model id override(s)", s.model_ids.len())
            }
        ),
    };
    format!(
        "Changed how the install calls Claude from {} to {}.",
        say(from),
        say(to)
    )
}

/// Check, then save: refused when any model fails (nothing written), or
/// when the audit entry can't be written first.
pub async fn switch(
    prober: &dyn Prober,
    store: &dyn Store,
    team_id: &str,
    actor: &str,
    to: Settings,
) -> Result<(Value, Vec<Access>), (StatusCode, Value)> {
    let results = check(prober, store, team_id, &to).await;
    if !model_access::all_pass(&results) {
        let failing: Vec<&str> = results
            .iter()
            .filter(|r| r.status == "fail")
            .map(|r| r.model.as_str())
            .collect();
        return Err((
            StatusCode::CONFLICT,
            json!({
                "error": format!("Not switched: {} didn't answer. Fix it and check again.", failing.join(" and ")),
                "results": results,
            }),
        ));
    }
    let from = mp::current();
    store.audit(&audit_detail(&from, &to)).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": format!("Not switched: the audit log couldn't be written ({e}).") }),
        )
    })?;
    let at = chrono::Utc::now().to_rfc3339();
    let probe_team = (to.provider == Provider::Anthropic).then_some(team_id);
    let record = mp::stored_record(&to, actor, &at, probe_team);
    store.save(&record).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": format!("Not switched: the setting couldn't be saved ({e}).") }),
        )
    })?;
    Ok((record, results))
}

// ── The real calls ────────────────────────────────────────────────────────

struct Live<'a> {
    state: &'a AppState,
    claims: &'a Claims,
}

fn answer_of(p: super::setup_checks::ModelProbe) -> Answer {
    use super::setup_checks::ModelProbe as P;
    match p {
        P::Ok => Answer::Ok,
        P::NoKey => Answer::NoKey,
        P::Refused(s, m) => Answer::Refused(s, m),
        P::Unreachable(m) => Answer::Unreachable(m),
    }
}

#[async_trait]
impl Prober for Live<'_> {
    async fn bedrock(&self, model: &str, bedrock_id: &str) -> Answer {
        // Metered on the install's setup meter, like Setup's own test call.
        let meter = crate::model_call::chat_meter(self.state, "SETUP");
        answer_of(
            crate::model_call::probe_via(
                crate::model_call::Via::Bedrock(bedrock_id.to_string()),
                &self.state.http,
                "",
                &meter,
                model,
            )
            .await,
        )
    }
    async fn anthropic(&self, api_key: &str, model: &str) -> Answer {
        let meter = crate::model_call::chat_meter(self.state, &self.claims.team_id);
        answer_of(
            crate::model_call::probe_via(
                crate::model_call::Via::Anthropic,
                &self.state.http,
                api_key,
                &meter,
                model,
            )
            .await,
        )
    }
}

#[async_trait]
impl Store for Live<'_> {
    async fn team_key(&self, team_id: &str) -> Option<String> {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        self.state
            .dynamo
            .get_item()
            .table_name(&self.state.config.settings_table_name)
            .key("pk", A::S(team_id.to_string()))
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
    async fn audit(&self, detail: &str) -> Result<(), String> {
        super::roles::record(
            self.state,
            self.claims,
            super::roles::AuditEntry {
                action: "model_provider_switched",
                role_id: "",
                role_name: "",
                detail: detail.to_string(),
            },
        )
        .await
        .map_err(|_| "write failed".to_string())
    }
    async fn save(&self, record: &Value) -> Result<(), String> {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        self.state
            .dynamo
            .put_item()
            .table_name(&self.state.config.settings_table_name)
            .item("pk", A::S(mp::SETTINGS_PK.into()))
            .item("sk", A::S(mp::SETTINGS_SK.into()))
            .item("data", A::S(record.to_string()))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string())
    }
}

/// What the Models section shows.
pub fn view(settings: &Settings, source: Source, stored: Option<&Value>) -> Value {
    let deploy = mp::deploy_default();
    let ids: serde_json::Map<String, Value> = targets(&Settings {
        provider: Provider::Bedrock,
        ..settings.clone()
    })
    .into_iter()
    .map(|(m, id)| (m, json!(id)))
    .collect();
    json!({
        "provider": settings.provider.as_str(),
        "inference_prefix": settings.inference_prefix,
        "model_ids": settings.model_ids,
        "source": source,
        "updated_at": stored.and_then(|s| s.get("updated_at")).cloned().unwrap_or(Value::Null),
        "updated_by": stored.and_then(|s| s.get("updated_by")).cloned().unwrap_or(Value::Null),
        "deploy_default": {
            "provider": deploy.provider.as_str(),
            "inference_prefix": deploy.inference_prefix,
        },
        "region": region(),
        "models": common::model_ids::release_calls(),
        // The Bedrock id each model is called by under these settings.
        "bedrock_ids": ids,
        "prefixes": ["us.", "eu.", "apac.", "global.", ""],
    })
}

async fn stored(state: &AppState) -> Option<Value> {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", A::S(mp::SETTINGS_PK.into()))
        .key("sk", A::S(mp::SETTINGS_SK.into()))
        .send()
        .await
        .ok()?;
    serde_json::from_str(out.item()?.get("data")?.as_s().ok()?).ok()
}

/// GET /api/system/models: the provider in force, where it came from, and
/// the ids each model is called by.
pub async fn get_models(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(not_admin)?;
    allowed(&claims)?;
    let rec = stored(&state).await;
    let (settings, source) = mp::resolve(
        rec.as_ref().map(|r| r.to_string()).as_deref(),
        mp::deploy_default(),
    );
    Ok(Json(view(&settings, source, rec.as_ref())))
}

/// POST /api/system/models/check: the test calls a switch would make, with
/// nothing saved ("Check again").
pub async fn check_models(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(not_admin)?;
    allowed(&claims)?;
    let to = requested(&body).map_err(|m| refuse(StatusCode::BAD_REQUEST, &m))?;
    let live = Live {
        state: &state,
        claims: &claims,
    };
    let results = check(&live, &live, &claims.team_id, &to).await;
    Ok(Json(json!({
        "ok": model_access::all_pass(&results),
        "results": results,
    })))
}

/// PUT /api/system/models: switch, once every model answers.
pub async fn set_models(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(not_admin)?;
    allowed(&claims)?;
    let to = requested(&body).map_err(|m| refuse(StatusCode::BAD_REQUEST, &m))?;
    let live = Live {
        state: &state,
        claims: &claims,
    };
    let (record, results) = switch(&live, &live, &claims.team_id, &claims.email, to.clone())
        .await
        .map_err(|(c, v)| (c, Json(v)))?;
    // This instance at once; the others within 30 seconds.
    mp::set_current(to.clone(), Source::Settings);
    info!(provider = to.provider.as_str(), actor = %claims.email, "model provider switched");
    let mut out = view(&to, Source::Settings, Some(&record));
    out["results"] = json!(results);
    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Fake {
        bedrock: fn(&str) -> Answer,
        anthropic: fn(&str) -> Answer,
        key: Option<&'static str>,
        audit_ok: bool,
        calls: Mutex<Vec<String>>,
        audited: Mutex<Vec<String>>,
        saved: Mutex<Vec<Value>>,
    }

    impl Fake {
        fn new(bedrock: fn(&str) -> Answer) -> Self {
            Fake {
                bedrock,
                anthropic: |_| Answer::Ok,
                key: Some("sk-ant-test"),
                audit_ok: true,
                calls: Mutex::new(vec![]),
                audited: Mutex::new(vec![]),
                saved: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait]
    impl Prober for Fake {
        async fn bedrock(&self, _model: &str, id: &str) -> Answer {
            self.calls.lock().unwrap().push(format!("bedrock {id}"));
            (self.bedrock)(id)
        }
        async fn anthropic(&self, key: &str, model: &str) -> Answer {
            self.calls
                .lock()
                .unwrap()
                .push(format!("anthropic {model} {key}"));
            (self.anthropic)(model)
        }
    }

    #[async_trait]
    impl Store for Fake {
        async fn team_key(&self, _team: &str) -> Option<String> {
            self.key.map(str::to_string)
        }
        async fn audit(&self, detail: &str) -> Result<(), String> {
            if !self.audit_ok {
                return Err("throttled".into());
            }
            self.audited.lock().unwrap().push(detail.to_string());
            Ok(())
        }
        async fn save(&self, record: &Value) -> Result<(), String> {
            self.saved.lock().unwrap().push(record.clone());
            Ok(())
        }
    }

    fn bedrock_eu() -> Settings {
        requested(&json!({"provider": "bedrock", "inference_prefix": "eu."})).unwrap()
    }

    #[test]
    fn a_request_is_read_and_checked() {
        let s = requested(&json!({"provider": "bedrock", "inference_prefix": " apac. ", "model_ids": {"claude-opus-5-5": "arn:aws:bedrock:ap-southeast-2:111122223333:application-inference-profile/x", "claude-sonnet-5-5": ""}})).unwrap();
        assert_eq!(s.inference_prefix, "apac.");
        assert_eq!(s.model_ids.len(), 1);
        assert!(requested(&json!({"provider": "openai"})).is_err());
        assert!(
            requested(&json!({"provider": "bedrock", "inference_prefix": "US"}))
                .unwrap_err()
                .contains("prefix")
        );
        assert!(requested(
            &json!({"provider": "bedrock", "model_ids": {"claude-opus-5-5": "gpt"}})
        )
        .is_err());
        // Keys: no Bedrock fields kept.
        let k = requested(&json!({"provider": "anthropic", "inference_prefix": "eu."})).unwrap();
        assert_eq!(k.provider, Provider::Anthropic);
        assert_eq!(k.inference_prefix, "us.");
    }

    #[test]
    fn every_model_the_release_calls_is_checked_by_its_bedrock_id() {
        let t = targets(&bedrock_eu());
        let models: Vec<&str> = t.iter().map(|(m, _)| m.as_str()).collect();
        assert_eq!(models, common::model_ids::release_calls());
        assert!(t.iter().all(|(m, id)| id == &format!("eu.anthropic.{m}")));
    }

    #[tokio::test]
    async fn a_switch_that_passes_is_audited_then_saved() {
        let f = Fake::new(|_| Answer::Ok);
        let (record, results) = switch(&f, &f, "TEAM#1", "admin@example.com", bedrock_eu())
            .await
            .unwrap();
        assert!(results.iter().all(|r| r.status == "pass"));
        assert_eq!(record["provider"], "bedrock");
        assert_eq!(record["inference_prefix"], "eu.");
        assert_eq!(record["updated_by"], "admin@example.com");
        assert_eq!(f.saved.lock().unwrap().len(), 1);
        let audited = f.audited.lock().unwrap();
        assert_eq!(audited.len(), 1);
        assert!(audited[0].contains("to Claude on Amazon Bedrock (eu. inference profiles)"));
        assert_eq!(
            f.calls.lock().unwrap().len(),
            common::model_ids::release_calls().len()
        );
    }

    #[tokio::test]
    async fn a_refused_model_stops_the_switch_and_says_why() {
        let f = Fake::new(|id| {
            if id.contains("opus") {
                Answer::Refused(
                    400,
                    "Model use case details have not been submitted for this account.".into(),
                )
            } else {
                Answer::Ok
            }
        });
        let (code, body) = switch(&f, &f, "TEAM#1", "a@example.com", bedrock_eu())
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::CONFLICT);
        assert!(body["error"].as_str().unwrap().contains("claude-opus-5-5"));
        let bad = body["results"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["status"] == "fail")
            .unwrap();
        assert_eq!(bad["code"], "use_case_required");
        assert!(bad["url"].as_str().unwrap().contains("#/modelaccess"));
        assert!(!bad["fix"].as_str().unwrap().is_empty());
        // Nothing written.
        assert!(f.saved.lock().unwrap().is_empty());
        assert!(f.audited.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn every_refusal_kind_refuses_the_switch() {
        let kinds: [fn(&str) -> Answer; 6] = [
            |_| Answer::Refused(403, "You don't have access to the model".into()),
            |_| Answer::Refused(400, "use case details have not been submitted".into()),
            |_| Answer::Refused(403, "aws-marketplace:Subscribe failed".into()),
            |_| Answer::Refused(403, "is not authorized to perform: bedrock:Converse".into()),
            |_| Answer::Refused(400, "The provided model identifier is invalid.".into()),
            |_| Answer::Unreachable("timeout".into()),
        ];
        let codes = [
            "access_denied",
            "use_case_required",
            "marketplace_failed",
            "iam_denied",
            "not_available",
            "unreachable",
        ];
        for (k, code) in kinds.into_iter().zip(codes) {
            let f = Fake::new(k);
            let (_, body) = switch(&f, &f, "TEAM#1", "a@example.com", bedrock_eu())
                .await
                .unwrap_err();
            assert_eq!(body["results"][0]["code"], code);
            assert!(f.saved.lock().unwrap().is_empty(), "{code}");
        }
    }

    #[tokio::test]
    async fn a_switch_to_keys_needs_a_working_team_key() {
        let mut f = Fake::new(|_| Answer::Ok);
        f.key = None;
        let to = requested(&json!({"provider": "anthropic"})).unwrap();
        let (_, body) = switch(&f, &f, "TEAM#1", "a@example.com", to.clone())
            .await
            .unwrap_err();
        assert_eq!(body["results"][0]["code"], "no_key");
        assert!(f.calls.lock().unwrap().is_empty());
        // With a key: checked with it, and the team is kept for the updater.
        let f = Fake::new(|_| Answer::Ok);
        let (record, _) = switch(&f, &f, "TEAM#1", "a@example.com", to).await.unwrap();
        assert_eq!(record["probe_team"], "TEAM#1");
        assert!(f.calls.lock().unwrap()[0].ends_with("sk-ant-test"));
        let mut f = Fake::new(|_| Answer::Ok);
        f.anthropic = |_| Answer::Refused(401, "invalid x-api-key".into());
        let to = requested(&json!({"provider": "anthropic"})).unwrap();
        let (_, body) = switch(&f, &f, "TEAM#1", "a@example.com", to)
            .await
            .unwrap_err();
        assert_eq!(body["results"][0]["code"], "key_refused");
    }

    #[tokio::test]
    async fn no_audit_entry_no_switch() {
        let mut f = Fake::new(|_| Answer::Ok);
        f.audit_ok = false;
        let (code, body) = switch(&f, &f, "TEAM#1", "a@example.com", bedrock_eu())
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body["error"].as_str().unwrap().contains("audit log"));
        assert!(f.saved.lock().unwrap().is_empty());
    }

    #[test]
    fn the_audit_line_says_from_and_to() {
        let keys = Settings::default();
        let mut b = bedrock_eu();
        assert_eq!(
            audit_detail(&keys, &b),
            "Changed how the install calls Claude from Anthropic API keys to Claude on Amazon Bedrock (eu. inference profiles)."
        );
        b.inference_prefix = String::new();
        b.model_ids
            .insert("claude-opus-5-5".into(), "anthropic.claude-opus-5-5".into());
        assert!(audit_detail(&b, &keys).contains("in-region ids, 1 model id override(s)"));
    }

    #[test]
    fn the_view_shows_the_ids_and_where_the_setting_came_from() {
        let v = view(
            &bedrock_eu(),
            Source::Settings,
            Some(&json!({"updated_by": "a@example.com", "updated_at": "2026-10-26T00:00:00Z"})),
        );
        assert_eq!(v["provider"], "bedrock");
        assert_eq!(v["source"], "settings");
        assert_eq!(v["updated_by"], "a@example.com");
        assert_eq!(
            v["bedrock_ids"]["claude-opus-5-5"],
            "eu.anthropic.claude-opus-5-5"
        );
        assert!(v["prefixes"].as_array().unwrap().contains(&json!("")));
    }
}
