//! What an installation's token may read, for features that work better
//! with a GitHub permission and must still work without it.
//!
//! An installation access token carries the permissions the installation
//! granted (GitHub's `access_tokens` answer, or the broker's). A permission
//! CoderHelm asks for later reaches an installation only once its owner
//! approves it, so a feature reading commit statuses, deployments or
//! advisories meets installations that can't. Such a read is skipped once
//! the token says the permission is missing, and a refusal GitHub sends is
//! remembered, so it is logged once and never again for that installation.
//! A newer token that carries the permission clears the memory.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use serde_json::Value;

/// Whether an installation may use a permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grant {
    Granted,
    Missing,
    /// No token said, and nothing was refused yet: try, and learn.
    Unknown,
}

#[derive(Default)]
struct Known {
    /// The newest token's permissions (name to level); None until one said.
    token: Option<HashMap<String, String>>,
    /// Permissions GitHub refused since that token.
    refused: HashSet<String>,
}

static KNOWN: Mutex<Option<HashMap<u64, Known>>> = Mutex::new(None);

fn with<R>(f: impl FnOnce(&mut HashMap<u64, Known>) -> R) -> R {
    let mut g = KNOWN.lock().unwrap_or_else(|p| p.into_inner());
    f(g.get_or_insert_with(HashMap::new))
}

/// Records a new token's `permissions` object. An absent or empty object
/// says nothing, and is ignored.
pub fn record(installation: u64, permissions: &Value) {
    let Some(map) = permissions.as_object().filter(|m| !m.is_empty()) else {
        return;
    };
    let map: HashMap<String, String> = map
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
        .collect();
    with(|all| {
        let k = all.entry(installation).or_default();
        k.refused.retain(|p| match p.split_once(":write") {
            Some((name, _)) => !map
                .get(name)
                .is_some_and(|l| matches!(l.as_str(), "write" | "admin")),
            None => !map.contains_key(p),
        });
        k.token = Some(map);
    });
}

/// Whether `installation` may use `permission` (read or more).
pub fn grant(installation: u64, permission: &str) -> Grant {
    with(|all| {
        let Some(k) = all.get(&installation) else {
            return Grant::Unknown;
        };
        if k.refused.contains(permission) {
            return Grant::Missing;
        }
        match &k.token {
            Some(t) if t.contains_key(permission) => Grant::Granted,
            Some(_) => Grant::Missing,
            None => Grant::Unknown,
        }
    })
}

/// Whether `installation` may write with `permission` (start a workflow
/// needs `actions: write`; reading its runs only `read`).
pub fn write_grant(installation: u64, permission: &str) -> Grant {
    with(|all| {
        let Some(k) = all.get(&installation) else {
            return Grant::Unknown;
        };
        if k.refused.contains(&format!("{permission}:write")) {
            return Grant::Missing;
        }
        match &k.token {
            Some(t) => match t.get(permission).map(String::as_str) {
                Some("write" | "admin") => Grant::Granted,
                _ => Grant::Missing,
            },
            None => Grant::Unknown,
        }
    })
}

/// Remembers that GitHub refused a write with `permission`. True the first
/// time, so the caller says it once.
pub fn refused_write(installation: u64, permission: &str) -> bool {
    refused(installation, &format!("{permission}:write"))
}

/// True when the installation is known to lack the permission: the read is
/// skipped.
pub fn lacks(installation: u64, permission: &str) -> bool {
    grant(installation, permission) == Grant::Missing
}

/// Remembers that GitHub refused `permission` for `installation`. True the
/// first time, so the caller logs it once.
pub fn refused(installation: u64, permission: &str) -> bool {
    with(|all| {
        all.entry(installation)
            .or_default()
            .refused
            .insert(permission.to_string())
    })
}

/// Whether a GitHub answer is a refusal for want of permission, not a rate
/// limit or a missing thing: a 403, or GitHub's "Resource not accessible by
/// integration" with any status.
pub fn is_refusal(status: u16, body: &Value) -> bool {
    let msg = body["message"].as_str().unwrap_or("").to_ascii_lowercase();
    if msg.contains("rate limit") {
        return false;
    }
    msg.contains("resource not accessible by integration") || status == 403
}

/// The permission's name in GitHub's app settings.
pub fn label(permission: &str) -> &'static str {
    match permission {
        "statuses" => "Commit statuses",
        "deployments" => "Deployments",
        "actions" => "Actions",
        "checks" => "Checks",
        "repository_advisories" => "Repository security advisories",
        "vulnerability_alerts" => "Dependabot alerts",
        "security_events" => "Code scanning alerts",
        "code_quality" => "Code quality",
        "workflows" => "Workflows",
        _ => "this permission",
    }
}

/// A plain sentence for where a feature lives: what CoderHelm does instead,
/// and the permission that lifts the limit.
pub fn limit_note(permission: &str, instead: &str) -> String {
    format!(
        "{instead} because CoderHelm can't read {} on this installation; add {}: read to the GitHub App.",
        label(permission),
        label(permission)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_write_is_known_before_it_is_tried() {
        let i = 990_001;
        assert_eq!(write_grant(i, "actions"), Grant::Unknown);
        record(i, &json!({"actions": "read", "contents": "write"}));
        assert_eq!(grant(i, "actions"), Grant::Granted);
        assert_eq!(write_grant(i, "actions"), Grant::Missing);
        assert_eq!(write_grant(i, "contents"), Grant::Granted);
        // A refusal is remembered once, and a newer token clears it.
        let j = 990_002;
        assert!(refused_write(j, "actions"));
        assert!(!refused_write(j, "actions"));
        assert_eq!(write_grant(j, "actions"), Grant::Missing);
        record(j, &json!({"actions": "write"}));
        assert_eq!(write_grant(j, "actions"), Grant::Granted);
    }

    #[test]
    fn unknown_until_a_token_or_a_refusal_says() {
        assert_eq!(grant(9_000_001, "statuses"), Grant::Unknown);
        assert!(!lacks(9_000_001, "statuses"));
    }

    #[test]
    fn a_token_names_what_it_may_read() {
        record(9_000_002, &json!({"contents": "write", "statuses": "read"}));
        assert_eq!(grant(9_000_002, "statuses"), Grant::Granted);
        assert_eq!(grant(9_000_002, "deployments"), Grant::Missing);
        assert!(lacks(9_000_002, "deployments"));
    }

    #[test]
    fn an_empty_answer_says_nothing() {
        record(9_000_003, &json!({}));
        record(9_000_003, &Value::Null);
        assert_eq!(grant(9_000_003, "statuses"), Grant::Unknown);
    }

    #[test]
    fn a_refusal_is_remembered_and_reported_once() {
        assert!(refused(9_000_004, "deployments"));
        assert!(!refused(9_000_004, "deployments"));
        assert!(lacks(9_000_004, "deployments"));
    }

    #[test]
    fn a_newer_token_with_the_permission_clears_the_refusal() {
        refused(9_000_005, "statuses");
        record(9_000_005, &json!({"statuses": "read"}));
        assert_eq!(grant(9_000_005, "statuses"), Grant::Granted);
    }

    #[test]
    fn a_rate_limit_is_never_a_refusal() {
        assert!(!is_refusal(
            403,
            &json!({"message": "API rate limit exceeded for installation"})
        ));
        assert!(is_refusal(
            403,
            &json!({"message": "Resource not accessible by integration"})
        ));
        assert!(is_refusal(
            404,
            &json!({"message": "Resource not accessible by integration"})
        ));
        assert!(!is_refusal(404, &json!({"message": "Not Found"})));
    }

    #[test]
    fn the_note_names_the_permission_in_plain_words() {
        let n = limit_note("deployments", "Deploys are inferred from workflow names");
        assert_eq!(
            n,
            "Deploys are inferred from workflow names because CoderHelm can't read Deployments \
             on this installation; add Deployments: read to the GitHub App."
        );
    }
}
