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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::kv::Memory;

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
