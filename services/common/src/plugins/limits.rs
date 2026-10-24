//! Token limits for plugin runs.
//!
//! A plugin's model calls are tagged `plugin:<id>` and count toward the
//! team's three monthly limits like any other call (`crate::token_limits`).
//! A plugin may also have its own monthly limit (set on install from its
//! manifest's budget, changeable by an admin). Both are checked at the same
//! two points the team limit is: before a run starts, and before each step
//! of the run.

use super::{code, Refusal};

/// Reads usage and limits.
#[async_trait::async_trait]
pub trait UsageRead: Send + Sync {
    /// Tokens (all four kinds) the plugin's calls used this month.
    async fn plugin_month(&self, team: &str, plugin: &str) -> Result<u64, String>;
    /// The reason when one of the team's monthly limits is reached.
    async fn team_limit_reached(&self, team: &str) -> Option<String>;
}

/// Refuses when the team's limit, or the plugin's own, is reached.
pub async fn check(
    usage: &dyn UsageRead,
    team: &str,
    plugin: &str,
    monthly: u64,
) -> Result<(), Refusal> {
    if let Some(reason) = usage.team_limit_reached(team).await {
        return Err(Refusal::new(code::LIMIT_REACHED, reason));
    }
    if monthly > 0 {
        let used = usage.plugin_month(team, plugin).await.unwrap_or(0);
        if used >= monthly {
            return Err(Refusal::new(
                code::LIMIT_REACHED,
                format!(
                    "This plugin used its monthly limit ({} of {} tokens). Raise it on the plugin's page.",
                    crate::token_limits::short_tokens(used),
                    crate::token_limits::short_tokens(monthly)
                ),
            ));
        }
    }
    Ok(())
}

/// The plugin's tokens this month from an analytics month item.
pub fn plugin_tokens(
    item: Option<&std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
    plugin: &str,
) -> u64 {
    let Some(i) = item else { return 0 };
    ["tokens_in", "cache_read", "cache_write", "tokens_out"]
        .iter()
        .map(|f| {
            i.get(&crate::usage::plugin_attr(plugin, f))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<f64>().ok())
                .unwrap_or(0.0)
                .max(0.0) as u64
        })
        .sum()
}

/// The analytics and settings tables.
pub struct Dynamo {
    pub client: aws_sdk_dynamodb::Client,
    pub analytics_table: String,
    pub settings_table: String,
}

#[async_trait::async_trait]
impl UsageRead for Dynamo {
    async fn plugin_month(&self, team: &str, plugin: &str) -> Result<u64, String> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let month = chrono::Utc::now().format("%Y-%m").to_string();
        let out = self
            .client
            .get_item()
            .table_name(&self.analytics_table)
            .key("team_id", AttributeValue::S(team.to_string()))
            .key("period", AttributeValue::S(month))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        Ok(plugin_tokens(out.item(), plugin))
    }

    async fn team_limit_reached(&self, team: &str) -> Option<String> {
        let limits = crate::token_limits::load(&self.client, &self.settings_table, team)
            .await
            .ok()?;
        if limits.is_empty() {
            return None;
        }
        let month = chrono::Utc::now().format("%Y-%m").to_string();
        let totals = crate::token_limits::load_month_usage(
            &self.client,
            &self.analytics_table,
            team,
            &month,
        )
        .await
        .unwrap_or_default();
        limits.reached(&totals).map(|hit| {
            format!(
                "The team's monthly {} limit is reached ({} of {}).",
                hit.kind.label(),
                crate::token_limits::short_tokens(hit.used),
                crate::token_limits::short_tokens(hit.max)
            )
        })
    }
}

/// Usage in memory (tests).
#[derive(Default)]
pub struct Memory {
    pub plugin: std::sync::Mutex<std::collections::HashMap<String, u64>>,
    pub team_reached: std::sync::Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl UsageRead for Memory {
    async fn plugin_month(&self, _team: &str, plugin: &str) -> Result<u64, String> {
        Ok(*self.plugin.lock().unwrap().get(plugin).unwrap_or(&0))
    }
    async fn team_limit_reached(&self, _team: &str) -> Option<String> {
        self.team_reached.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::AttributeValue;

    #[tokio::test]
    async fn the_plugin_limit_and_the_team_limit_both_stop_a_run() {
        let u = Memory::default();
        assert!(check(&u, "T", "p", 100).await.is_ok());
        u.plugin.lock().unwrap().insert("p".into(), 100);
        assert_eq!(
            check(&u, "T", "p", 100).await.unwrap_err().code,
            code::LIMIT_REACHED
        );
        assert!(check(&u, "T", "p", 0).await.is_ok(), "0 is no plugin limit");
        *u.team_reached.lock().unwrap() = Some("team limit".into());
        assert_eq!(
            check(&u, "T", "p", 0).await.unwrap_err().code,
            code::LIMIT_REACHED
        );
    }

    #[test]
    fn plugin_tokens_sum_the_plugins_counters() {
        let mut i = std::collections::HashMap::new();
        for (f, n) in [
            ("tokens_in", "10"),
            ("cache_read", "5"),
            ("cache_write", "3"),
            ("tokens_out", "2"),
            ("cost_usd", "9"),
        ] {
            i.insert(
                crate::usage::plugin_attr("mk", f),
                AttributeValue::N(n.into()),
            );
        }
        assert_eq!(plugin_tokens(Some(&i), "mk"), 20);
        assert_eq!(plugin_tokens(Some(&i), "other"), 0);
    }
}
