//! The team's `plugins` flag and its plugin settings.
//!
//! Plugins ship off. A platform admin turns the flag on for a team once the
//! release check passes on a test team; until then every plugin route,
//! trigger, sync and run refuses with [`code::FLAG_OFF`](super::code::FLAG_OFF).
//!
//! - `SETTINGS#FLAGS`: `{ "plugins": bool }`.
//! - `SETTINGS#PLUGINS`: the team's autonomy cap and its chat settings.

use serde::{Deserialize, Serialize};

use super::kv::{get_as, put_as, Expect, Kv};
use super::{code, store_err, Refusal};
use crate::tool_registry::Risk;

pub const FLAGS_SK: &str = "SETTINGS#FLAGS";
pub const SETTINGS_SK: &str = "SETTINGS#PLUGINS";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flags {
    #[serde(default)]
    pub plugins: bool,
}

/// True when the team's `plugins` flag is on. A read failure is off.
pub async fn enabled(kv: &dyn Kv, team: &str) -> bool {
    matches!(get_as::<Flags>(kv, team, FLAGS_SK).await, Ok(Some((f, _))) if f.plugins)
}

/// Refuses when the flag is off.
pub async fn require(kv: &dyn Kv, team: &str) -> Result<(), Refusal> {
    if enabled(kv, team).await {
        Ok(())
    } else {
        Err(Refusal::new(
            code::FLAG_OFF,
            "Plugins are not turned on for this team.",
        ))
    }
}

/// Sets the flag (platform admins only; the gateway checks).
pub async fn set(kv: &dyn Kv, team: &str, on: bool) -> Result<Flags, Refusal> {
    let mut f = get_as::<Flags>(kv, team, FLAGS_SK)
        .await
        .map_err(store_err)?
        .map(|(f, _)| f)
        .unwrap_or_default();
    f.plugins = on;
    put_as(kv, team, FLAGS_SK, &f, Expect::Any)
        .await
        .map_err(store_err)?;
    Ok(f)
}

/// The team's plugin settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamSettings {
    /// The highest risk a plugin run may use without a person. `read` or
    /// `write`: consequential tools always wait for an approval, whatever
    /// this says.
    #[serde(default = "default_cap")]
    pub autonomy_cap: Risk,
    /// A Slack workspace id whose slash command may start briefs, and its
    /// signing secret (sealed).
    #[serde(default)]
    pub slack_team_id: String,
    #[serde(default)]
    pub slack_signing_secret_sealed: String,
    /// The token in the Teams outgoing webhook's address, and its HMAC
    /// secret (sealed).
    #[serde(default)]
    pub teams_token: String,
    #[serde(default)]
    pub teams_secret_sealed: String,
}

fn default_cap() -> Risk {
    Risk::Write
}

impl Default for TeamSettings {
    fn default() -> Self {
        Self {
            autonomy_cap: default_cap(),
            slack_team_id: String::new(),
            slack_signing_secret_sealed: String::new(),
            teams_token: String::new(),
            teams_secret_sealed: String::new(),
        }
    }
}

impl TeamSettings {
    /// The cap actually applied: never above write.
    pub fn cap(&self) -> Risk {
        self.autonomy_cap.min(Risk::Write)
    }
}

pub async fn settings(kv: &dyn Kv, team: &str) -> Result<TeamSettings, Refusal> {
    Ok(get_as::<TeamSettings>(kv, team, SETTINGS_SK)
        .await
        .map_err(store_err)?
        .map(|(s, _)| s)
        .unwrap_or_default())
}

pub async fn save_settings(kv: &dyn Kv, team: &str, s: &TeamSettings) -> Result<(), Refusal> {
    if s.autonomy_cap == Risk::Consequential {
        return Err(Refusal::new(
            code::INVALID,
            "The autonomy cap can be read or write. Consequential tools always wait for a person.",
        ));
    }
    put_as(kv, team, SETTINGS_SK, s, Expect::Any)
        .await
        .map_err(store_err)?;
    Ok(())
}

/// The chat index key for a Slack workspace.
pub fn slack_key(workspace: &str) -> String {
    format!("slack|{workspace}")
}

/// The chat index key for a Teams outgoing webhook token.
pub fn teams_key(token: &str) -> String {
    format!("teams|{token}")
}

/// The team a chat index key points at, with the record's revision.
async fn chat_owner(kv: &dyn Kv, key: &str) -> Result<Option<(String, u64)>, Refusal> {
    Ok(kv
        .get(super::kv::CHAT_INDEX, key)
        .await
        .map_err(store_err)?
        .and_then(|r| Some((r.data["team"].as_str()?.to_string(), r.rev))))
}

/// True when `team`'s own settings still name `key` (the index is only a
/// pointer; the team's settings are the truth).
async fn still_names(kv: &dyn Kv, team: &str, key: &str) -> bool {
    match settings(kv, team).await {
        Ok(s) => {
            (!s.slack_team_id.is_empty() && slack_key(&s.slack_team_id) == key)
                || (!s.teams_token.is_empty() && teams_key(&s.teams_token) == key)
        }
        // Unreadable: treat the claim as standing rather than hand it over.
        Err(_) => true,
    }
}

/// Claims `key` in the chat index for `team`. A key another team holds,
/// and still names in its settings, is refused: one workspace or webhook
/// routes to one team. A pointer its team no longer names is taken over.
pub async fn claim_chat(kv: &dyn Kv, team: &str, key: &str) -> Result<(), Refusal> {
    use super::kv::{KvError, CHAT_INDEX};
    let taken = || {
        Refusal::new(
            code::CONFLICT,
            "That Slack workspace or Teams webhook is already linked to another CoderHelm team. Unlink it there first.",
        )
    };
    let data = serde_json::json!({ "team": team });
    let expect = match chat_owner(kv, key).await? {
        None => Expect::Absent,
        Some((owner, _)) if owner == team => return Ok(()),
        Some((owner, rev)) => {
            if still_names(kv, &owner, key).await {
                return Err(taken());
            }
            Expect::Rev(rev)
        }
    };
    match kv.put(CHAT_INDEX, key, &data, expect).await {
        Ok(_) => Ok(()),
        // Someone claimed it meanwhile: theirs.
        Err(KvError::Conflict) => match chat_owner(kv, key).await? {
            Some((owner, _)) if owner == team => Ok(()),
            _ => Err(taken()),
        },
        Err(e) => Err(store_err(e)),
    }
}

/// Drops `key` from the chat index, only while it points at `team`.
pub async fn release_chat(kv: &dyn Kv, team: &str, key: &str) -> Result<(), Refusal> {
    if let Some((owner, _)) = chat_owner(kv, key).await? {
        if owner == team {
            kv.delete(super::kv::CHAT_INDEX, key)
                .await
                .map_err(store_err)?;
        }
    }
    Ok(())
}

/// Points the Slack workspace `id` at `team` in the chat index, and drops the
/// team's previous workspace from it. Empty `id` unlinks. A workspace
/// another team holds is refused and the settings are left as they were.
pub async fn link_slack(
    kv: &dyn Kv,
    team: &str,
    s: &mut TeamSettings,
    id: &str,
) -> Result<(), Refusal> {
    if id == s.slack_team_id {
        if !id.is_empty() {
            claim_chat(kv, team, &slack_key(id)).await?;
        }
        return Ok(());
    }
    if !id.is_empty() {
        claim_chat(kv, team, &slack_key(id)).await?;
    }
    if !s.slack_team_id.is_empty() {
        release_chat(kv, team, &slack_key(&s.slack_team_id)).await?;
    }
    s.slack_team_id = id.to_string();
    Ok(())
}

/// Gives the team a fresh Teams webhook token and claims it.
pub async fn link_teams(kv: &dyn Kv, team: &str, s: &mut TeamSettings) -> Result<(), Refusal> {
    let token = super::new_id();
    claim_chat(kv, team, &teams_key(&token)).await?;
    s.teams_token = token;
    Ok(())
}

/// The team an inbound chat message is for: the index's team, only while
/// that team's settings still name the key.
pub async fn chat_team(kv: &dyn Kv, key: &str) -> Option<String> {
    let (team, _) = chat_owner(kv, key).await.ok().flatten()?;
    let s = settings(kv, &team).await.ok()?;
    let named = (!s.slack_team_id.is_empty() && slack_key(&s.slack_team_id) == key)
        || (!s.teams_token.is_empty() && teams_key(&s.teams_token) == key);
    named.then_some(team)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::kv::Memory;

    /// Team B naming team A's Slack workspace must not take A's slash
    /// commands, and B unlinking must not cut A off.
    #[tokio::test]
    async fn a_slack_workspace_belongs_to_the_team_that_linked_it() {
        let kv = Memory::new();
        let mut a = TeamSettings::default();
        link_slack(&kv, "TEAM#a", &mut a, "T0SHARED").await.unwrap();
        save_settings(&kv, "TEAM#a", &a).await.unwrap();

        let mut b = TeamSettings::default();
        let taken = link_slack(&kv, "TEAM#b", &mut b, "T0SHARED").await;
        assert!(taken.is_err(), "B may not claim A's workspace");
        assert_eq!(b.slack_team_id, "", "B's settings keep no claim");
        assert_eq!(
            chat_team(&kv, &slack_key("T0SHARED")).await.as_deref(),
            Some("TEAM#a")
        );

        // B pretends it had the workspace and unlinks it: A keeps it.
        let mut forged = TeamSettings {
            slack_team_id: "T0SHARED".into(),
            ..Default::default()
        };
        link_slack(&kv, "TEAM#b", &mut forged, "").await.unwrap();
        assert_eq!(
            chat_team(&kv, &slack_key("T0SHARED")).await.as_deref(),
            Some("TEAM#a")
        );
    }

    /// A pointer the old team no longer names (it moved to another
    /// workspace, or the record was left behind) is taken over, and a
    /// pointer that disagrees with its team's settings routes nowhere.
    #[tokio::test]
    async fn a_stale_pointer_routes_nowhere_and_can_be_claimed() {
        use crate::plugins::kv::CHAT_INDEX;
        let kv = Memory::new();
        // Left behind by an older build: points at A, A's settings say nothing.
        kv.put(
            CHAT_INDEX,
            &slack_key("T0MOVED"),
            &serde_json::json!({"team": "TEAM#a"}),
            Expect::Any,
        )
        .await
        .unwrap();
        assert_eq!(chat_team(&kv, &slack_key("T0MOVED")).await, None);
        let mut b = TeamSettings::default();
        link_slack(&kv, "TEAM#b", &mut b, "T0MOVED").await.unwrap();
        save_settings(&kv, "TEAM#b", &b).await.unwrap();
        assert_eq!(
            chat_team(&kv, &slack_key("T0MOVED")).await.as_deref(),
            Some("TEAM#b")
        );
    }

    #[tokio::test]
    async fn a_teams_webhook_token_routes_to_its_own_team() {
        let kv = Memory::new();
        let mut a = TeamSettings::default();
        link_teams(&kv, "TEAM#a", &mut a).await.unwrap();
        save_settings(&kv, "TEAM#a", &a).await.unwrap();
        let mut b = TeamSettings::default();
        link_teams(&kv, "TEAM#b", &mut b).await.unwrap();
        save_settings(&kv, "TEAM#b", &b).await.unwrap();
        assert_ne!(a.teams_token, b.teams_token);
        assert_eq!(
            chat_team(&kv, &teams_key(&a.teams_token)).await.as_deref(),
            Some("TEAM#a")
        );
        assert_eq!(
            chat_team(&kv, &teams_key(&b.teams_token)).await.as_deref(),
            Some("TEAM#b")
        );
        // B can't claim A's token.
        assert!(claim_chat(&kv, "TEAM#b", &teams_key(&a.teams_token))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn off_by_default_and_per_team() {
        let kv = Memory::new();
        assert!(!enabled(&kv, "TEAM#a").await);
        assert_eq!(
            require(&kv, "TEAM#a").await.unwrap_err().code,
            code::FLAG_OFF
        );
        set(&kv, "TEAM#a", true).await.unwrap();
        assert!(enabled(&kv, "TEAM#a").await);
        assert!(!enabled(&kv, "TEAM#b").await, "a flag is one team's");
    }

    #[tokio::test]
    async fn the_cap_never_allows_consequential() {
        let kv = Memory::new();
        assert_eq!(settings(&kv, "T").await.unwrap().cap(), Risk::Write);
        let bad = TeamSettings {
            autonomy_cap: Risk::Consequential,
            ..Default::default()
        };
        assert_eq!(
            save_settings(&kv, "T", &bad).await.unwrap_err().code,
            code::INVALID
        );
        assert_eq!(bad.cap(), Risk::Write);
    }
}
