//! Which service the deployment calls Claude through.
//!
//! Two providers: the Anthropic API with each team's own key (stored in
//! Settings, AI models), or Amazon Bedrock in the install's own AWS account
//! (no key: the services' IAM roles call Bedrock, and the model names a team
//! picks, `claude-opus-5-5`, are mapped to Bedrock ids here).
//!
//! The deploy config sets the first-boot default (`MODEL_PROVIDER`,
//! `BEDROCK_INFERENCE_PREFIX`, `BEDROCK_MODEL_IDS`). On a self-hosted
//! install an admin can change it at any time in Settings, System, Models:
//! the choice is stored on the settings table (`pk = SYSTEM`, `sk = MODELS`,
//! a JSON `data` attribute, the updater's format) and wins over the deploy
//! config. The gateway and the worker read it through [`refresh`], at most
//! once every [`CACHE_FOR`] per instance, so a switch reaches every new
//! model call within that time. A model call already under way finishes on
//! the provider it started with.

use std::collections::{BTreeMap, HashMap};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Where the runtime setting is stored (settings table).
pub const SETTINGS_PK: &str = "SYSTEM";
pub const SETTINGS_SK: &str = "MODELS";
/// How long an instance keeps the setting it read.
pub const CACHE_FOR: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    #[default]
    Anthropic,
    Bedrock,
}

impl Provider {
    pub fn as_str(self) -> &'static str {
        match self {
            Provider::Anthropic => "anthropic",
            Provider::Bedrock => "bedrock",
        }
    }
    pub fn parse(v: &str) -> Option<Self> {
        match v.trim().to_ascii_lowercase().as_str() {
            "anthropic" => Some(Provider::Anthropic),
            "bedrock" => Some(Provider::Bedrock),
            _ => None,
        }
    }
}

/// How the install calls Claude.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    pub provider: Provider,
    /// Bedrock: the inference profile prefix (`us.`, `eu.`, `apac.`,
    /// `global.`), or `""` for in-region model ids.
    #[serde(default = "default_prefix")]
    pub inference_prefix: String,
    /// Bedrock: exact ids for model names (an application inference profile
    /// ARN, for example). Empty for the defaults.
    #[serde(default)]
    pub model_ids: BTreeMap<String, String>,
}

fn default_prefix() -> String {
    "us.".into()
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            provider: Provider::Anthropic,
            inference_prefix: default_prefix(),
            model_ids: BTreeMap::new(),
        }
    }
}

/// Where the setting in force came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    /// The deploy config (nobody has changed it in Settings).
    Deploy,
    /// An admin's choice in Settings, System, Models.
    Settings,
}

impl Settings {
    /// The Bedrock id for a model name under these settings.
    pub fn bedrock_id(&self, model: &str) -> String {
        let map: HashMap<String, String> = self.model_ids.clone().into_iter().collect();
        map_model_id(model, &self.inference_prefix, &map)
    }

    /// The settings, or what is wrong with them in plain words.
    pub fn validate(&self) -> Result<(), String> {
        let p = &self.inference_prefix;
        let ok_prefix = p.is_empty()
            || (p.ends_with('.')
                && (3..=7).contains(&p.len())
                && p[..p.len() - 1].chars().all(|c| c.is_ascii_lowercase()));
        if !ok_prefix {
            return Err(
                "The inference profile prefix is us., eu., apac., global. or empty.".into(),
            );
        }
        for (name, id) in &self.model_ids {
            if name.trim().is_empty() || name.len() > 120 {
                return Err("Each model id override needs the model's name.".into());
            }
            let id = id.trim();
            if id.len() > 300 || !(id.contains("anthropic.") || id.starts_with("arn:aws")) {
                return Err(format!(
                    "The Bedrock id for {name} must be an Anthropic model id, an inference profile id or an inference profile ARN."
                ));
            }
        }
        Ok(())
    }
}

/// The deploy config's settings, from the environment variables it sets.
pub fn deploy_default_from(
    provider: Option<&str>,
    prefix: Option<&str>,
    model_ids: Option<&str>,
) -> Settings {
    Settings {
        provider: if provider_is_bedrock(provider) {
            Provider::Bedrock
        } else {
            Provider::Anthropic
        },
        inference_prefix: prefix.map_or_else(default_prefix, str::to_string),
        model_ids: model_ids
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default(),
    }
}

/// The deploy config's settings (read once).
pub fn deploy_default() -> &'static Settings {
    static D: OnceLock<Settings> = OnceLock::new();
    D.get_or_init(|| {
        let v = |k: &str| std::env::var(k).ok();
        deploy_default_from(
            v("MODEL_PROVIDER").as_deref(),
            v("BEDROCK_INFERENCE_PREFIX").as_deref(),
            v("BEDROCK_MODEL_IDS").as_deref(),
        )
    })
}

/// The setting in force: a stored choice when it reads and is valid, else
/// the deploy config's.
pub fn resolve(stored: Option<&str>, default: &Settings) -> (Settings, Source) {
    let parsed = stored
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .and_then(|v| {
            // The stored record also carries who changed it and when.
            let provider = Provider::parse(v.get("provider")?.as_str()?)?;
            let mut out = Settings {
                provider,
                inference_prefix: v
                    .get("inference_prefix")
                    .and_then(Value::as_str)
                    .map_or_else(|| default.inference_prefix.clone(), str::to_string),
                model_ids: v
                    .get("model_ids")
                    .and_then(|m| serde_json::from_value(m.clone()).ok())
                    .unwrap_or_default(),
            };
            out.inference_prefix = out.inference_prefix.trim().to_string();
            out.validate().ok().map(|_| out)
        });
    match parsed {
        Some(s) => (s, Source::Settings),
        None => (default.clone(), Source::Deploy),
    }
}

/// The record stored for a switch (the updater reads the same fields).
pub fn stored_record(s: &Settings, actor: &str, at: &str, probe_team: Option<&str>) -> Value {
    json!({
        "provider": s.provider.as_str(),
        "inference_prefix": s.inference_prefix,
        "model_ids": s.model_ids,
        "updated_at": at,
        "updated_by": actor,
        // The team whose key passed the switch's check (Anthropic API): the
        // updater checks a new release's models with it.
        "probe_team": probe_team,
    })
}

struct Cached {
    read_at: Option<Instant>,
    settings: Settings,
    source: Source,
}

fn cell() -> &'static RwLock<Option<Cached>> {
    static C: OnceLock<RwLock<Option<Cached>>> = OnceLock::new();
    C.get_or_init(|| RwLock::new(None))
}

/// The setting in force now, and where it came from.
pub fn current_with_source() -> (Settings, Source) {
    match cell().read().unwrap_or_else(|e| e.into_inner()).as_ref() {
        Some(c) => (c.settings.clone(), c.source),
        None => (deploy_default().clone(), Source::Deploy),
    }
}

/// The setting in force now.
pub fn current() -> Settings {
    current_with_source().0
}

/// Use these settings in this instance right away (the gateway, after a
/// switch it saved); other instances pick it up within [`CACHE_FOR`].
pub fn set_current(settings: Settings, source: Source) {
    *cell().write().unwrap_or_else(|e| e.into_inner()) = Some(Cached {
        read_at: Some(Instant::now()),
        settings,
        source,
    });
}

/// Whether the cached setting is older than [`CACHE_FOR`] (or was never read).
fn stale() -> bool {
    cell()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|c| c.read_at)
        .is_none_or(|at| at.elapsed() >= CACHE_FOR)
}

/// Reads the stored setting when the cached one is stale. A failed read
/// keeps what the instance had (or the deploy config), and tries again
/// after [`CACHE_FOR`].
pub async fn refresh(dynamo: &aws_sdk_dynamodb::Client, table: &str) {
    if table.is_empty() || !stale() {
        return;
    }
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let read = dynamo
        .get_item()
        .table_name(table)
        .key("pk", A::S(SETTINGS_PK.into()))
        .key("sk", A::S(SETTINGS_SK.into()))
        .send()
        .await;
    let mut guard = cell().write().unwrap_or_else(|e| e.into_inner());
    match read {
        Ok(out) => {
            let data = out
                .item()
                .and_then(|i| i.get("data"))
                .and_then(|v| v.as_s().ok())
                .cloned();
            let (settings, source) = resolve(data.as_deref(), deploy_default());
            *guard = Some(Cached {
                read_at: Some(Instant::now()),
                settings,
                source,
            });
        }
        Err(e) => {
            tracing::warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "model provider setting not read; keeping the last one");
            let (settings, source) = match guard.as_ref() {
                Some(c) => (c.settings.clone(), c.source),
                None => (deploy_default().clone(), Source::Deploy),
            };
            *guard = Some(Cached {
                read_at: Some(Instant::now()),
                settings,
                source,
            });
        }
    }
}

/// True when the install calls Claude through Amazon Bedrock now.
pub fn is_bedrock() -> bool {
    current().provider == Provider::Bedrock
}

/// Whether a `MODEL_PROVIDER` value means Bedrock.
pub fn provider_is_bedrock(v: Option<&str>) -> bool {
    v.is_some_and(|p| p.trim().eq_ignore_ascii_case("bedrock"))
}

static CLIENT: OnceLock<aws_sdk_bedrockruntime::Client> = OnceLock::new();

/// Keep the Bedrock client the services call Claude with (set at start on
/// every install, so a switch to Bedrock needs no restart).
pub fn set_bedrock_client(client: aws_sdk_bedrockruntime::Client) {
    let _ = CLIENT.set(client);
}

/// The Bedrock client, when the install uses Bedrock now and it was set.
pub fn bedrock_client() -> Option<&'static aws_sdk_bedrockruntime::Client> {
    if is_bedrock() {
        CLIENT.get()
    } else {
        None
    }
}

/// The Bedrock client whatever the provider is now (a run that started on
/// Bedrock with no team key finishes there; a switch's check).
pub fn any_bedrock_client() -> Option<&'static aws_sdk_bedrockruntime::Client> {
    CLIENT.get()
}

/// The Bedrock id for a model name under the setting in force.
pub fn bedrock_model_id(model: &str) -> String {
    current().bedrock_id(model)
}

/// [`bedrock_model_id`]'s rule: an explicit mapping wins; an id that is
/// already a Bedrock id (`anthropic.` in it, or an ARN) is used as given;
/// anything else becomes `<prefix>anthropic.<model>`, with the manifest's
/// Bedrock name for models whose id carries a date.
pub fn map_model_id(model: &str, prefix: &str, map: &HashMap<String, String>) -> String {
    if let Some(id) = map.get(model) {
        return id.clone();
    }
    if model.contains("anthropic.") || model.starts_with("arn:") {
        return model.to_string();
    }
    let name = crate::model_ids::manifest()
        .bedrock_names
        .get(model)
        .map_or(model, String::as_str);
    format!("{prefix}anthropic.{name}")
}

/// A Messages API request body as Bedrock takes it: no model or stream
/// fields (the model is in the call, streaming is the call), Bedrock's
/// version marker added. Everything else, tools and cache_control
/// included, is sent unchanged.
pub fn bedrock_body(body: &Value) -> Value {
    let mut b = body.clone();
    if let Some(o) = b.as_object_mut() {
        o.remove("model");
        o.remove("stream");
        o.insert("anthropic_version".into(), json!("bedrock-2023-05-31"));
    }
    b
}

/// One Bedrock streaming event, framed as the Anthropic API's SSE text so
/// the same reader handles both (`event: <type>\ndata: <json>\n\n`).
pub fn sse_frame(event_json: &[u8]) -> Vec<u8> {
    let ty = serde_json::from_slice::<Value>(event_json)
        .ok()
        .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(String::from))
        .unwrap_or_else(|| "message".into());
    let mut out = format!("event: {ty}\ndata: ").into_bytes();
    out.extend_from_slice(event_json);
    out.extend_from_slice(b"\n\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stored_setting_wins_over_the_deploy_config() {
        let deploy = deploy_default_from(Some("anthropic"), None, None);
        assert_eq!(deploy.provider, Provider::Anthropic);
        assert_eq!(deploy.inference_prefix, "us.");
        // Nothing stored: the deploy config.
        assert_eq!(resolve(None, &deploy), (deploy.clone(), Source::Deploy));
        // A switch to Bedrock in Europe.
        let (s, src) = resolve(
            Some(
                r#"{"provider":"bedrock","inference_prefix":"eu.","model_ids":{},"updated_by":"a@example.com"}"#,
            ),
            &deploy,
        );
        assert_eq!(src, Source::Settings);
        assert_eq!(s.provider, Provider::Bedrock);
        assert_eq!(
            s.bedrock_id("claude-opus-5-5"),
            "eu.anthropic.claude-opus-5-5"
        );
        // No prefix stored: the deploy config's.
        let (s, _) = resolve(Some(r#"{"provider":"bedrock"}"#), &deploy);
        assert_eq!(s.inference_prefix, "us.");
    }

    #[test]
    fn a_broken_stored_setting_falls_back_to_the_deploy_config() {
        let deploy = deploy_default_from(
            Some("bedrock"),
            Some("apac."),
            Some(r#"{"claude-opus-5-5":"apac.anthropic.claude-opus-5-5"}"#),
        );
        assert_eq!(deploy.provider, Provider::Bedrock);
        assert_eq!(deploy.model_ids.len(), 1);
        for bad in [
            "not json",
            r#"{"provider":"openai"}"#,
            r#"{"provider":"bedrock","inference_prefix":"US"}"#,
            r#"{"provider":"bedrock","model_ids":{"claude-opus-5-5":"gpt-4"}}"#,
        ] {
            assert_eq!(
                resolve(Some(bad), &deploy),
                (deploy.clone(), Source::Deploy),
                "{bad}"
            );
        }
    }

    #[test]
    fn settings_are_checked_in_plain_words() {
        let mut s = Settings {
            provider: Provider::Bedrock,
            ..Default::default()
        };
        for ok in ["", "us.", "eu.", "apac.", "global."] {
            s.inference_prefix = ok.into();
            assert!(s.validate().is_ok(), "{ok}");
        }
        for bad in ["us", "US.", "a.", "toolongprefix.", "u s."] {
            s.inference_prefix = bad.into();
            assert!(s.validate().unwrap_err().contains("prefix"), "{bad}");
        }
        s.inference_prefix = "us.".into();
        s.model_ids.insert(
            "claude-opus-5-5".into(),
            "arn:aws:bedrock:us-east-1:111122223333:application-inference-profile/abc".into(),
        );
        assert!(s.validate().is_ok());
        s.model_ids.insert("claude-sonnet-5-5".into(), "gpt".into());
        assert!(s.validate().unwrap_err().contains("claude-sonnet-5-5"));
    }

    #[test]
    fn a_switch_takes_effect_in_this_instance_at_once() {
        let s = Settings {
            provider: Provider::Bedrock,
            inference_prefix: "eu.".into(),
            model_ids: BTreeMap::new(),
        };
        set_current(s.clone(), Source::Settings);
        assert!(is_bedrock());
        assert_eq!(
            bedrock_model_id("claude-sonnet-5-5"),
            "eu.anthropic.claude-sonnet-5-5"
        );
        assert_eq!(current_with_source(), (s, Source::Settings));
        // Fresh: refresh would not read again yet.
        assert!(!stale());
        set_current(Settings::default(), Source::Deploy);
        assert!(!is_bedrock());
    }

    #[test]
    fn the_stored_record_has_what_the_updater_reads() {
        let s = Settings {
            provider: Provider::Bedrock,
            inference_prefix: "eu.".into(),
            model_ids: BTreeMap::new(),
        };
        let r = stored_record(&s, "a@example.com", "2026-10-26T00:00:00Z", None);
        assert_eq!(r["provider"], "bedrock");
        assert_eq!(r["inference_prefix"], "eu.");
        assert_eq!(resolve(Some(&r.to_string()), &Settings::default()).0, s);
    }

    #[test]
    fn provider_switch() {
        assert!(provider_is_bedrock(Some("bedrock")));
        assert!(provider_is_bedrock(Some(" Bedrock ")));
        assert!(!provider_is_bedrock(Some("anthropic")));
        assert!(!provider_is_bedrock(None));
    }

    #[test]
    fn model_ids_map_to_bedrock() {
        let mut map = HashMap::new();
        map.insert(
            "claude-haiku-4-5".to_string(),
            "us.anthropic.claude-haiku-4-5-20251001-v1:0".to_string(),
        );
        assert_eq!(
            map_model_id("claude-opus-5-5", "us.", &map),
            "us.anthropic.claude-opus-5-5"
        );
        assert_eq!(
            map_model_id("claude-opus-5-5", "", &map),
            "anthropic.claude-opus-5-5"
        );
        assert_eq!(
            map_model_id("claude-haiku-4-5", "eu.", &HashMap::new()),
            "eu.anthropic.claude-haiku-4-5-20251001-v1:0"
        );
        assert_eq!(
            map_model_id("claude-haiku-4-5", "eu.", &map),
            "us.anthropic.claude-haiku-4-5-20251001-v1:0"
        );
        assert_eq!(
            map_model_id("eu.anthropic.claude-sonnet-5-5", "us.", &map),
            "eu.anthropic.claude-sonnet-5-5"
        );
        let arn = "arn:aws:bedrock:us-east-1:111122223333:application-inference-profile/abc";
        assert_eq!(map_model_id(arn, "us.", &map), arn);
    }

    #[test]
    fn body_drops_model_and_stream_and_marks_the_version() {
        let b = bedrock_body(&json!({
            "model": "claude-opus-5-5", "stream": true, "max_tokens": 10,
            "messages": [], "tools": [{"name": "t"}]
        }));
        assert_eq!(b.get("model"), None);
        assert_eq!(b.get("stream"), None);
        assert_eq!(b["anthropic_version"], "bedrock-2023-05-31");
        assert_eq!(b["max_tokens"], 10);
        assert_eq!(b["tools"][0]["name"], "t");
    }

    #[test]
    fn events_are_framed_as_sse() {
        let f = sse_frame(br#"{"type":"content_block_delta","index":0}"#);
        assert_eq!(
            String::from_utf8(f).unwrap(),
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0}\n\n"
        );
    }
}
