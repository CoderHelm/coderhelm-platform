//! Saved webhook URLs: sealed, shown only as a hint, write-only through the API.
//!
//! A webhook URL posts as the team, so it is stored like the team's other
//! secrets: encrypted by KMS with the team bound into the encryption context
//! (an item copied under another team will not open). A settings item holds,
//! per channel, `<channel>_webhook_sealed` and `<channel>_webhook_hint`.
//! Teams URLs saved before sealing are plain in `teams_webhook_url`; they are
//! still read, and sealed the next time the setting is saved.
//!
//! API answers carry only `{ "set": bool, "hint": "…" }` per channel. A save
//! keeps the stored URL unless the request sets a new one or clears it, so a
//! page never needs the URL back.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::warn;

use super::{ChannelKind, Channels};
use crate::mcp_credentials::Cipher;

type Item = HashMap<String, AttributeValue>;

const PURPOSE: &str = "notify-webhook";

/// The encryption context: the team and what the value is for.
pub fn context(team_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("team".to_string(), team_id.to_string()),
        ("purpose".to_string(), PURPOSE.to_string()),
    ])
}

/// Where one channel's webhook lives on a settings item.
pub struct Attrs {
    /// Plain URL. Only Teams URLs saved before sealing; never written for Slack.
    pub plain: &'static str,
    pub sealed: &'static str,
    pub hint: &'static str,
}

pub fn attrs(kind: ChannelKind) -> Attrs {
    match kind {
        ChannelKind::Teams => Attrs {
            plain: "teams_webhook_url",
            sealed: "teams_webhook_sealed",
            hint: "teams_webhook_hint",
        },
        ChannelKind::Slack => Attrs {
            plain: "slack_webhook_url",
            sealed: "slack_webhook_sealed",
            hint: "slack_webhook_hint",
        },
    }
}

/// Every attribute a webhook may be stored under, for filters that must never
/// return one.
pub const SECRET_ATTRS: &[&str] = &[
    "teams_webhook_url",
    "teams_webhook_sealed",
    "slack_webhook_url",
    "slack_webhook_sealed",
];

/// Pure: the host and the last four characters, for telling webhooks apart
/// without revealing one ("hooks.slack.com …x9Qa").
pub fn hint(url: &str) -> String {
    let host = url
        .strip_prefix("https://")
        .and_then(|r| r.split(['/', '?', '#', ':']).next())
        .unwrap_or("");
    let chars: Vec<char> = url.chars().collect();
    if chars.len() < 24 || host.is_empty() {
        return "saved".to_string();
    }
    let tail: String = chars[chars.len() - 4..].iter().collect();
    format!("{host} …{tail}")
}

fn s<'a>(item: &'a Item, k: &str) -> Option<&'a str> {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .map(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
}

/// What an item holds for one channel.
#[derive(Clone, PartialEq, Eq)]
pub enum Stored {
    Sealed {
        sealed: String,
        hint: String,
    },
    /// A Teams URL saved before sealing.
    Plain(String),
    None,
}

impl std::fmt::Debug for Stored {
    // Never prints a value.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Stored::Sealed { .. } => "Sealed(..)",
            Stored::Plain(_) => "Plain(..)",
            Stored::None => "None",
        })
    }
}

impl Stored {
    pub fn is_set(&self) -> bool {
        !matches!(self, Stored::None)
    }

    pub fn hint(&self) -> String {
        match self {
            Stored::Sealed { hint, .. } => hint.clone(),
            Stored::Plain(url) => self::hint(url),
            Stored::None => String::new(),
        }
    }

    /// The attributes that keep this value on a rewritten item.
    pub fn to_attrs(&self, kind: ChannelKind) -> Vec<(String, AttributeValue)> {
        let a = attrs(kind);
        match self {
            Stored::Sealed { sealed, hint } => vec![
                (a.sealed.to_string(), AttributeValue::S(sealed.clone())),
                (a.hint.to_string(), AttributeValue::S(hint.clone())),
            ],
            Stored::Plain(url) => vec![(a.plain.to_string(), AttributeValue::S(url.clone()))],
            Stored::None => vec![],
        }
    }
}

/// Pure: what `item` holds for `kind`. Sealed wins over a plain value.
pub fn stored(item: &Item, kind: ChannelKind) -> Stored {
    let a = attrs(kind);
    if let Some(v) = s(item, a.sealed) {
        return Stored::Sealed {
            sealed: v.to_string(),
            hint: s(item, a.hint).unwrap_or("saved").to_string(),
        };
    }
    // Slack URLs are never stored plain; one found there is ignored.
    if kind == ChannelKind::Teams {
        if let Some(v) = s(item, a.plain) {
            return Stored::Plain(v.trim().to_string());
        }
    }
    Stored::None
}

/// Pure: whether `item` holds a webhook for any channel.
pub fn any_set(item: &Item) -> bool {
    ChannelKind::ALL.iter().any(|k| stored(item, *k).is_set())
}

/// Pure: the API's view of one item's webhooks: set or not, and a hint.
pub fn view(item: &Item) -> Value {
    let one = |k: ChannelKind| {
        let st = stored(item, k);
        json!({ "set": st.is_set(), "hint": st.hint() })
    };
    json!({ "teams": one(ChannelKind::Teams), "slack": one(ChannelKind::Slack) })
}

/// Encrypts a webhook URL for `team_id`. Returns text safe to store.
pub async fn seal(cipher: &dyn Cipher, team_id: &str, url: &str) -> Result<String, String> {
    let blob = cipher.encrypt(context(team_id), url.as_bytes()).await?;
    Ok(STANDARD.encode(blob))
}

/// Opens a URL sealed for `team_id`.
pub async fn open_sealed(
    cipher: &dyn Cipher,
    team_id: &str,
    sealed: &str,
) -> Result<String, String> {
    let bytes = STANDARD
        .decode(sealed)
        .map_err(|_| "stored webhook is not valid".to_string())?;
    let plain = cipher.decrypt(context(team_id), &bytes).await?;
    String::from_utf8(plain).map_err(|_| "stored webhook is not text".to_string())
}

/// The URL one channel posts to, opened. `None` when unset or unreadable (a
/// failure is logged, never fatal: the other channel still posts).
pub async fn open(cipher: &dyn Cipher, team_id: &str, st: &Stored) -> Option<String> {
    match st {
        Stored::Sealed { sealed, .. } => match open_sealed(cipher, team_id, sealed).await {
            Ok(url) => Some(url),
            Err(e) => {
                warn!(team_id, error = %e, "Could not open a saved webhook");
                None
            }
        },
        Stored::Plain(url) => Some(url.clone()),
        Stored::None => None,
    }
}

/// Every channel `item` posts to, opened, keeping only URLs their channel
/// accepts.
pub async fn open_all(cipher: &dyn Cipher, team_id: &str, item: &Item) -> Channels {
    let (t, s) = (
        stored(item, ChannelKind::Teams),
        stored(item, ChannelKind::Slack),
    );
    let (teams, slack) = tokio::join!(open(cipher, team_id, &t), open(cipher, team_id, &s));
    Channels { teams, slack }.usable()
}

// ─── Saving ─────────────────────────────────────────────────────────────────

/// A request's change to one channel's webhook.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WebhookInput {
    /// A new URL. Empty keeps the saved one.
    #[serde(default)]
    pub url: String,
    /// Remove the saved URL.
    #[serde(default)]
    pub clear: bool,
}

/// The webhook part of a save request, flattened into each settings body:
/// `{ "teams_webhook": { "url": "…" }, "slack_webhook": { "clear": true } }`.
/// `teams_webhook_url` is the shape saved before Slack: a non-empty value sets
/// the Teams URL, an empty one keeps it.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ChannelsInput {
    #[serde(default)]
    pub teams_webhook: Option<WebhookInput>,
    #[serde(default)]
    pub slack_webhook: Option<WebhookInput>,
    #[serde(default)]
    pub teams_webhook_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Keep,
    Clear,
    Set(String),
}

/// Why a save was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaveError {
    /// The URL is not a webhook of that channel.
    InvalidUrl(ChannelKind),
    /// The URL could not be sealed (no secrets key, KMS down).
    Seal(String),
}

impl ChannelsInput {
    /// Pure: the request's change to `kind`, checked.
    pub fn change(&self, kind: ChannelKind) -> Result<Change, SaveError> {
        let input = match kind {
            ChannelKind::Teams => self.teams_webhook.clone().or_else(|| {
                self.teams_webhook_url.as_ref().map(|u| WebhookInput {
                    url: u.clone(),
                    clear: false,
                })
            }),
            ChannelKind::Slack => self.slack_webhook.clone(),
        };
        let Some(input) = input else {
            return Ok(Change::Keep);
        };
        if input.clear {
            return Ok(Change::Clear);
        }
        let url = input.url.trim();
        if url.is_empty() {
            return Ok(Change::Keep);
        }
        if !kind.valid_url(url) {
            return Err(SaveError::InvalidUrl(kind));
        }
        Ok(Change::Set(url.to_string()))
    }

    /// Pure: refuse a bad URL before anything is written.
    pub fn check(&self) -> Result<(), SaveError> {
        for kind in ChannelKind::ALL {
            self.change(kind)?;
        }
        Ok(())
    }
}

/// The webhook attributes a full-item save writes: kept, cleared or newly
/// sealed per the request, from what `prior` held. Callers `put_item` the
/// whole item, so a cleared channel is simply left out.
pub async fn save_attrs(
    cipher: &dyn Cipher,
    team_id: &str,
    prior: Option<&Item>,
    input: &ChannelsInput,
) -> Result<Vec<(String, AttributeValue)>, SaveError> {
    let empty = Item::new();
    let prior = prior.unwrap_or(&empty);
    let mut out = Vec::new();
    for kind in ChannelKind::ALL {
        match input.change(kind)? {
            Change::Keep => out.extend(stored(prior, kind).to_attrs(kind)),
            Change::Clear => {}
            Change::Set(url) => {
                let sealed = seal(cipher, team_id, &url).await.map_err(SaveError::Seal)?;
                out.extend(
                    Stored::Sealed {
                        sealed,
                        hint: hint(&url),
                    }
                    .to_attrs(kind),
                );
            }
        }
    }
    Ok(out)
}

/// Pure: the item a save produced, for checks such as "on needs a channel".
pub fn as_item(attrs: &[(String, AttributeValue)]) -> Item {
    attrs.iter().cloned().collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Reversible "encryption" bound to the context, like KMS.
    pub(crate) struct Fake;

    fn ctx_key(c: &HashMap<String, String>) -> String {
        let mut v: Vec<_> = c.iter().map(|(k, v)| format!("{k}={v}")).collect();
        v.sort();
        v.join("&")
    }

    #[async_trait::async_trait]
    impl Cipher for Fake {
        async fn encrypt(&self, c: HashMap<String, String>, p: &[u8]) -> Result<Vec<u8>, String> {
            let mut out = format!("{}|", ctx_key(&c)).into_bytes();
            out.extend(p.iter().rev());
            Ok(out)
        }
        async fn decrypt(&self, c: HashMap<String, String>, b: &[u8]) -> Result<Vec<u8>, String> {
            let prefix = format!("{}|", ctx_key(&c)).into_bytes();
            let rest = b.strip_prefix(prefix.as_slice()).ok_or("wrong context")?;
            Ok(rest.iter().rev().copied().collect())
        }
    }

    const TEAMS: &str =
        "https://acme.webhook.office.com/webhookb2/abc-123-def/IncomingWebhook/x9Qa";
    const SLACK: &str = "https://hooks.slack.com/services/T0001/B0001/abcdefghijklmnop";

    fn input(v: Value) -> ChannelsInput {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn hints_never_reveal_the_url() {
        assert_eq!(hint(SLACK), "hooks.slack.com …mnop");
        assert_eq!(hint(TEAMS), "acme.webhook.office.com …x9Qa");
        assert_eq!(hint("https://x.com/a"), "saved");
        assert!(!hint(SLACK).contains("T0001"));
    }

    #[tokio::test]
    async fn a_saved_url_is_sealed_and_shown_only_as_a_hint() {
        let attrs = save_attrs(
            &Fake,
            "TEAM#a",
            None,
            &input(json!({ "teams_webhook": { "url": TEAMS }, "slack_webhook": { "url": SLACK } })),
        )
        .await
        .unwrap();
        let item = as_item(&attrs);
        // Nothing stored or shown carries the URL.
        for (k, v) in &item {
            let v = v.as_s().unwrap();
            assert!(
                !v.contains("abc-123") && !v.contains("abcdefghijklmnop"),
                "{k} = {v}"
            );
        }
        assert!(!item.contains_key("teams_webhook_url"));
        let shown = view(&item).to_string();
        assert!(!shown.contains("abc-123") && !shown.contains("abcdefghijklmnop"));
        assert_eq!(view(&item)["slack"]["set"], true);
        assert_eq!(view(&item)["slack"]["hint"], "hooks.slack.com …mnop");
        // And it opens again, for the same team only.
        let c = open_all(&Fake, "TEAM#a", &item).await;
        assert_eq!(c.teams.as_deref(), Some(TEAMS));
        assert_eq!(c.slack.as_deref(), Some(SLACK));
        let other = open_all(&Fake, "TEAM#b", &item).await;
        assert!(other.is_empty());
    }

    #[tokio::test]
    async fn a_save_keeps_clears_or_replaces_each_channel() {
        let first = as_item(
            &save_attrs(
                &Fake,
                "T",
                None,
                &input(
                    json!({ "teams_webhook": { "url": TEAMS }, "slack_webhook": { "url": SLACK } }),
                ),
            )
            .await
            .unwrap(),
        );
        // No webhook fields: both kept.
        let kept = as_item(
            &save_attrs(&Fake, "T", Some(&first), &input(json!({})))
                .await
                .unwrap(),
        );
        assert_eq!(kept, first);
        // The old page sends the masked field back empty: kept.
        let legacy = as_item(
            &save_attrs(
                &Fake,
                "T",
                Some(&first),
                &input(json!({ "teams_webhook_url": "" })),
            )
            .await
            .unwrap(),
        );
        assert_eq!(legacy, first);
        // Clear Slack only.
        let cleared = as_item(
            &save_attrs(
                &Fake,
                "T",
                Some(&first),
                &input(json!({ "slack_webhook": { "clear": true } })),
            )
            .await
            .unwrap(),
        );
        assert!(stored(&cleared, ChannelKind::Teams).is_set());
        assert!(!stored(&cleared, ChannelKind::Slack).is_set());
    }

    #[tokio::test]
    async fn a_plain_teams_url_saved_before_sealing_is_read_and_kept() {
        let old: Item = HashMap::from([(
            "teams_webhook_url".to_string(),
            AttributeValue::S(TEAMS.into()),
        )]);
        assert_eq!(view(&old)["teams"]["hint"], "acme.webhook.office.com …x9Qa");
        let c = open_all(&Fake, "T", &old).await;
        assert_eq!(c.teams.as_deref(), Some(TEAMS));
        let kept = as_item(
            &save_attrs(&Fake, "T", Some(&old), &input(json!({})))
                .await
                .unwrap(),
        );
        assert_eq!(kept, old);
        // A plain Slack value is never trusted.
        let odd: Item = HashMap::from([(
            "slack_webhook_url".to_string(),
            AttributeValue::S(SLACK.into()),
        )]);
        assert!(!any_set(&odd));
    }

    #[test]
    fn urls_are_checked_per_channel() {
        let bad = input(json!({ "slack_webhook": { "url": TEAMS } }));
        assert_eq!(bad.check(), Err(SaveError::InvalidUrl(ChannelKind::Slack)));
        let bad = input(json!({ "teams_webhook": { "url": SLACK } }));
        assert_eq!(bad.check(), Err(SaveError::InvalidUrl(ChannelKind::Teams)));
        let bad = input(json!({ "teams_webhook_url": "http://insecure.example.com" }));
        assert_eq!(bad.check(), Err(SaveError::InvalidUrl(ChannelKind::Teams)));
        assert!(input(json!({ "slack_webhook": { "url": SLACK } }))
            .check()
            .is_ok());
    }

    #[test]
    fn debug_never_prints_a_value() {
        let s = format!("{:?}", Stored::Plain(TEAMS.into()));
        assert!(!s.contains("office"));
    }
}
