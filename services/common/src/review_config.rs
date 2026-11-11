//! One repository's reviewer settings, stored at
//! `pk = team, sk = REVIEW_CONFIG#REPO#<owner>/<name>`.
//!
//! [`view`] is what the settings page reads (defaults filled in) and
//! [`build`] is the item a save writes, every value sanitized on the way in.
//! The reviewer route and Setup both use them, so a value Setup writes goes
//! through exactly the checks the page's save does.
//!
//! The item also holds the reminder webhooks and the release notes email
//! secret. [`build`] carries the secret over from the item before the save;
//! the webhooks are written by the caller (`notify::store::save_attrs`).

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::{json, Value};

pub type Item = HashMap<String, AttributeValue>;

/// The item's sort key.
pub fn sk(repo: &str) -> String {
    format!("REVIEW_CONFIG#REPO#{repo}")
}

pub const DEFAULT_LABEL: &str = "ch-review";
/// Longest review instructions a save accepts.
pub const MAX_INSTRUCTIONS: usize = 20_000;

/// Why a save was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reject {
    /// The instructions are longer than [`MAX_INSTRUCTIONS`].
    TooLarge,
    /// A value is not one the setting takes; the text says which.
    Bad(&'static str),
}

impl std::fmt::Display for Reject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reject::TooLarge => write!(
                f,
                "The review instructions are longer than {MAX_INSTRUCTIONS} characters."
            ),
            Reject::Bad(why) => write!(f, "{why}"),
        }
    }
}

fn item_bool(item: &Item, k: &str, d: bool) -> bool {
    item.get(k)
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(d)
}

fn item_str(item: &Item, k: &str, d: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| d.to_string())
}

fn item_num(item: &Item, k: &str, d: u64) -> u64 {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(d)
}

/// The settings as the page shows them, defaults applied so the form is
/// always whole and safe. Secrets are never in it: the email webhook secret
/// shows only whether it is set, the reminder webhooks only a hint.
pub fn view(item: &Item) -> Value {
    let log_groups: Vec<String> = item
        .get("health_log_groups")
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default();
    json!({
        "enabled": item_bool(item, "enabled", false),
        "label": item_str(item, "label", DEFAULT_LABEL),
        "killed": item_bool(item, "killed", false),
        "instructions": item_str(item, "instructions", ""),
        "auto_merge": item_bool(item, "auto_merge", false),
        "merge_method": item_str(item, "merge_method", "squash"),
        "require_human_approval": item_bool(item, "require_human_approval", true),
        "auto_tag": item_bool(item, "auto_tag", false),
        "tag_mode": item_str(item, "tag_mode", "semver"),
        "tag_prefix": item_str(item, "tag_prefix", "v"),
        "tag_batch_minutes": item_num(item, "tag_batch_minutes", 15),
        "health_check": item_bool(item, "health_check", false),
        "verify_tests": item_bool(item, "verify_tests", false),
        "require_tests": item_bool(item, "require_tests", false),
        "auto_labels": item_bool(item, "auto_labels", false),
        "auto_label_allow": item_str(item, "auto_label_allow", ""),
        "auto_label_requires": item_str(item, "auto_label_requires", ""),
        "auto_label_guide": item_str(item, "auto_label_guide", ""),
        "release_notes": item_bool(item, "release_notes", false),
        "release_notes_branch": item_str(item, "release_notes_branch", ""),
        "release_notes_confluence_space": item_str(item, "release_notes_confluence_space", ""),
        "release_notes_confluence_parent_id": item_str(item, "release_notes_confluence_parent_id", ""),
        "release_notes_confluence_container": item_str(item, "release_notes_confluence_container", "Changelog"),
        "release_notes_email_webhook_url": item_str(item, "release_notes_email_webhook_url", ""),
        // Never returned; the dashboard only learns whether one is set.
        "release_notes_email_webhook_secret_set": !item_str(item, "release_notes_email_webhook_secret", "").is_empty(),
        "release_notes_guide": item_str(item, "release_notes_guide", ""),
        "release_notes_instructions": item_str(item, "release_notes_instructions", ""),
        "deploy_label": item_str(item, "deploy_label", ""),
        "deploy_workflows": item_str(item, "deploy_workflows", ""),
        "health_log_groups": log_groups,
        "reminders_enabled": item_bool(item, "reminders_enabled", false),
        // Where reminders and release notes failures post. Shown only as a
        // hint: a webhook posts as the team.
        "webhooks": crate::notify::store::view(item),
        "reminder_cooldown_hours": item_num(item, "reminder_cooldown_hours", 4),
    })
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// The item a save writes for `repo`, from the request `body` and the item
/// stored before it (`prior`), without the reminder webhooks (the caller adds
/// those). Every value is cut to its range or allowed values; a field left
/// out takes its default, as the page always sends the whole form.
pub fn build(
    team_id: &str,
    repo: &str,
    body: &Value,
    prior: Option<&Item>,
    now: &str,
) -> Result<Item, Reject> {
    let instructions = body["instructions"].as_str().unwrap_or("");
    if instructions.len() > MAX_INSTRUCTIONS {
        return Err(Reject::TooLarge);
    }
    let label = {
        let l = body["label"].as_str().unwrap_or(DEFAULT_LABEL).trim();
        if l.is_empty() || l.len() > 100 {
            DEFAULT_LABEL.to_string()
        } else {
            l.to_string()
        }
    };
    let merge_method = match body["merge_method"].as_str().unwrap_or("squash") {
        m @ ("squash" | "merge" | "rebase") => m,
        _ => "squash",
    };
    let tag_mode = match body["tag_mode"].as_str().unwrap_or("semver") {
        m @ ("semver" | "date") => m,
        _ => "semver",
    };
    let tag_prefix = {
        let t = body["tag_prefix"].as_str().unwrap_or("v");
        if t.len() > 50 {
            "v"
        } else {
            t
        }
    };
    let bounded = |k: &str, max: usize| -> String {
        body[k]
            .as_str()
            .unwrap_or("")
            .trim()
            .chars()
            .take(max)
            .collect()
    };
    // Comma-separated labels added once a pull request is cleared to merge
    // (each triggers the repo's own CI); entries are trimmed worker-side.
    let deploy_label = bounded("deploy_label", 200);
    // Workflows that deploy though their names and jobs don't say where
    // (`file.yml=production`, comma separated): read by deploy stats.
    let deploy_workflows: String = body["deploy_workflows"]
        .as_str()
        .unwrap_or("")
        .trim()
        .chars()
        .filter(|c| !c.is_control())
        .take(500)
        .collect();
    // Reviewer-picked CI labels: free text, bounded. The worker parses and
    // enforces them; nothing outside `auto_label_allow` is ever added.
    let auto_label_allow = bounded("auto_label_allow", 1_000);
    let auto_label_requires = bounded("auto_label_requires", 2_000);
    let auto_label_guide = bounded("auto_label_guide", 5_000);
    // Release notes. The email webhook must be https (the worker posts to
    // it); the Confluence parent is a numeric page id.
    let rn_email = bounded("release_notes_email_webhook_url", 1_000);
    if !rn_email.is_empty() && !rn_email.starts_with("https://") {
        return Err(Reject::Bad(
            "The release notes email webhook must be https.",
        ));
    }
    let rn_parent = bounded("release_notes_confluence_parent_id", 30);
    if !rn_parent.chars().all(|c| c.is_ascii_digit()) {
        return Err(Reject::Bad("The Confluence parent page id is a number."));
    }
    let reminder_cooldown_hours = body["reminder_cooldown_hours"]
        .as_u64()
        .unwrap_or(4)
        .clamp(1, 168);
    // 0 tags every merge at once; otherwise merges within the window share
    // one tag. Capped at 6 hours so a merge can't wait days to deploy.
    let tag_batch_minutes = body["tag_batch_minutes"].as_u64().unwrap_or(15).min(360);
    let log_groups: Vec<AttributeValue> = body["health_log_groups"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .take(20)
                .map(s)
                .collect()
        })
        .unwrap_or_default();
    let flag = |k: &str, d: bool| AttributeValue::Bool(body[k].as_bool().unwrap_or(d));

    let mut item: Item = HashMap::new();
    let mut put = |k: &str, v: AttributeValue| {
        item.insert(k.to_string(), v);
    };
    put("pk", s(team_id));
    put("sk", s(&sk(repo)));
    put("enabled", flag("enabled", false));
    put("label", s(&label));
    put("killed", flag("killed", false));
    put("instructions", s(instructions));
    put("auto_merge", flag("auto_merge", false));
    put("merge_method", s(merge_method));
    put(
        "require_human_approval",
        flag("require_human_approval", true),
    );
    put("auto_tag", flag("auto_tag", false));
    put("tag_mode", s(tag_mode));
    put("tag_prefix", s(tag_prefix));
    put(
        "tag_batch_minutes",
        AttributeValue::N(tag_batch_minutes.to_string()),
    );
    put("health_check", flag("health_check", false));
    put("verify_tests", flag("verify_tests", false));
    put("deploy_label", s(&deploy_label));
    put("deploy_workflows", s(&deploy_workflows));
    put("require_tests", flag("require_tests", false));
    put("auto_labels", flag("auto_labels", false));
    put("auto_label_allow", s(&auto_label_allow));
    put("auto_label_requires", s(&auto_label_requires));
    put("auto_label_guide", s(&auto_label_guide));
    put("release_notes", flag("release_notes", false));
    put(
        "release_notes_branch",
        s(&bounded("release_notes_branch", 200)),
    );
    put(
        "release_notes_confluence_space",
        s(&bounded("release_notes_confluence_space", 100)),
    );
    put("release_notes_confluence_parent_id", s(&rn_parent));
    put(
        "release_notes_confluence_container",
        s(&bounded("release_notes_confluence_container", 200)),
    );
    put("release_notes_email_webhook_url", s(&rn_email));
    put(
        "release_notes_guide",
        s(&bounded("release_notes_guide", 300)),
    );
    put(
        "release_notes_instructions",
        s(&bounded("release_notes_instructions", 5_000)),
    );
    put("reminders_enabled", flag("reminders_enabled", false));
    put(
        "reminder_cooldown_hours",
        AttributeValue::N(reminder_cooldown_hours.to_string()),
    );
    put("updated_at", s(now));
    put("health_log_groups", AttributeValue::L(log_groups));

    // The email webhook secret is write-only: set only when the body carries
    // a new one, cleared on `release_notes_email_webhook_secret_clear`, and
    // otherwise kept (GET never returns it, so the page can't send it back).
    let prior_secret = prior
        .map(|it| item_str(it, "release_notes_email_webhook_secret", ""))
        .unwrap_or_default();
    let new_secret = bounded("release_notes_email_webhook_secret", 500);
    let secret = if body["release_notes_email_webhook_secret_clear"].as_bool() == Some(true) {
        String::new()
    } else if !new_secret.is_empty() {
        new_secret
    } else {
        prior_secret
    };
    item.insert("release_notes_email_webhook_secret".to_string(), s(&secret));
    // The code graph is its own feature with its own switch
    // (`crate::code_graph`); the old `graph_enabled` is carried as it was.
    if let Some(v) = prior.and_then(|it| it.get("graph_enabled")) {
        item.insert("graph_enabled".to_string(), v.clone());
    }
    Ok(item)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_saved_form_reads_back_the_same() {
        let body = json!({
            "enabled": true, "label": "review-me", "instructions": "Look at SQL.",
            "auto_merge": true, "merge_method": "rebase", "require_human_approval": false,
            "tag_mode": "date", "tag_prefix": "rel-", "tag_batch_minutes": 30,
            "reminders_enabled": true, "reminder_cooldown_hours": 12,
            "health_log_groups": ["/aws/lambda/x"],
        });
        let item = build("T", "o/r", &body, None, "now").unwrap();
        let v = view(&item);
        for k in [
            "enabled",
            "label",
            "instructions",
            "auto_merge",
            "merge_method",
            "require_human_approval",
            "tag_mode",
            "tag_prefix",
            "tag_batch_minutes",
            "reminders_enabled",
            "reminder_cooldown_hours",
            "health_log_groups",
        ] {
            assert_eq!(v[k], body[k], "{k}");
        }
        // A save of the page's own view changes nothing.
        let again = build("T", "o/r", &v, Some(&item), "now").unwrap();
        assert_eq!(again, item);
    }

    #[test]
    fn values_are_cut_to_what_the_setting_takes() {
        let item = build(
            "T",
            "o/r",
            &json!({"label": " ", "merge_method": "yolo", "tag_batch_minutes": 9999,
                     "reminder_cooldown_hours": 0}),
            None,
            "now",
        )
        .unwrap();
        let v = view(&item);
        assert_eq!(v["label"], DEFAULT_LABEL);
        assert_eq!(v["merge_method"], "squash");
        assert_eq!(v["tag_batch_minutes"], 360);
        assert_eq!(v["reminder_cooldown_hours"], 1);
        let long = "x".repeat(MAX_INSTRUCTIONS + 1);
        assert_eq!(
            build("T", "o/r", &json!({ "instructions": long }), None, "now"),
            Err(Reject::TooLarge)
        );
        assert!(matches!(
            build(
                "T",
                "o/r",
                &json!({"release_notes_email_webhook_url": "http://x"}),
                None,
                "now"
            ),
            Err(Reject::Bad(_))
        ));
    }

    #[test]
    fn the_secret_is_kept_unless_replaced_or_cleared() {
        let mut prior = Item::new();
        prior.insert("release_notes_email_webhook_secret".into(), s("shh"));
        prior.insert("graph_enabled".into(), AttributeValue::Bool(false));
        let kept = build("T", "o/r", &json!({}), Some(&prior), "now").unwrap();
        assert_eq!(kept["release_notes_email_webhook_secret"], s("shh"));
        assert_eq!(kept["graph_enabled"], AttributeValue::Bool(false));
        assert_eq!(view(&kept)["release_notes_email_webhook_secret_set"], true);
        let cleared = build(
            "T",
            "o/r",
            &json!({"release_notes_email_webhook_secret_clear": true}),
            Some(&prior),
            "now",
        )
        .unwrap();
        assert_eq!(cleared["release_notes_email_webhook_secret"], s(""));
    }
}
