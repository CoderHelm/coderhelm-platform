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
        "contents" => "Contents",
        "pull_requests" => "Pull requests",
        "issues" => "Issues",
        "metadata" => "Metadata",
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

// ── A denial, as recorded ──────────────────────────────────────────────────

/// Marks a line about a GitHub App permission the installation lacks as an
/// install-level item: one for the install's admin, shown on the install's
/// setup checks, never a per-pull-request hand-off.
pub const INSTALL_LEVEL: &str = "(an install-level permission)";

/// A concrete refusal GitHub sent: the API that answered, its status, and
/// the permission it needs. The only way CoderHelm may say a permission is
/// missing ("a permission only a person grants"): a claim without one is
/// never made. Built only from an answer that came back, never from a
/// request CoderHelm held for its own rate budget, a rate limit or a
/// network error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denial {
    /// `POST /repos/{owner}/{repo}/actions/workflows/{file}/dispatches`,
    /// with the owner and repository as GitHub named them.
    pub api: String,
    pub status: u16,
    /// GitHub's permission key (`actions`).
    pub permission: String,
    /// `read` or `write`.
    pub level: String,
}

const ANSWERED: &str = "GitHub answered ";

impl Denial {
    /// A denial from an answer: 403 or 404 that is a refusal for want of
    /// permission ([`is_refusal`]), never a rate limit.
    pub fn from_answer(
        method: &str,
        path: &str,
        status: u16,
        body: &Value,
        permission: &str,
        level: &str,
    ) -> Option<Denial> {
        if !matches!(status, 403 | 404) || !is_refusal(status, body) {
            return None;
        }
        Self::new(method, path, status, permission, level)
    }

    /// A denial from a client error's text (`HTTP status client error (403
    /// Forbidden) for url (https://api.github.com/...)`): the status and the
    /// API are read from it. None for a request that was never sent (held
    /// for the rate budget), a rate limit, or any other failure.
    pub fn from_error(method: &str, err: &str, permission: &str, level: &str) -> Option<Denial> {
        let l = err.to_ascii_lowercase();
        if l.contains("not sent") || l.contains("rate limit") || l.contains("budget") {
            return None;
        }
        let status = if l.contains("(403 forbidden)") {
            403
        } else if l.contains("(404 not found)") {
            404
        } else {
            return None;
        };
        if status == 404 && !l.contains("resource not accessible by integration") {
            // A 404 alone is a missing thing as often as a hidden one.
            return None;
        }
        let at = err.find("https://api.github.com")?;
        let rest = &err[at + "https://api.github.com".len()..];
        let end = rest
            .find(|c: char| c == ')' || c == '?' || c.is_whitespace())
            .unwrap_or(rest.len());
        Self::new(method, &rest[..end], status, permission, level)
    }

    fn new(method: &str, path: &str, status: u16, permission: &str, level: &str) -> Option<Denial> {
        let path = path.trim();
        if !path.starts_with('/') || permission.trim().is_empty() {
            return None;
        }
        Some(Denial {
            api: format!("{} {path}", method.trim().to_ascii_uppercase()),
            status,
            permission: permission.trim().to_string(),
            level: if level == "write" { "write" } else { "read" }.to_string(),
        })
    }

    /// The permission as GitHub's app settings name it (`Actions: write`).
    pub fn needs(&self) -> String {
        format!("{}: {}", label(&self.permission), self.level)
    }

    /// The recorded line: the API, its answer and the permission it needs.
    /// [`Denial::parse`] reads it back.
    pub fn line(&self) -> String {
        format!(
            "{ANSWERED}{} to `{}`: the GitHub App needs {}",
            self.status,
            self.api,
            self.needs()
        )
    }

    /// The denial a line records, if it records one (any case).
    pub fn parse(text: &str) -> Option<(u16, String, String)> {
        let l = text.to_ascii_lowercase();
        let at = l.find(&ANSWERED.to_ascii_lowercase())?;
        let rest = &l[at + ANSWERED.len()..];
        let status: u16 = rest.get(..3)?.parse().ok()?;
        if !matches!(status, 403 | 404) {
            return None;
        }
        let api_start = rest.find('`')? + 1;
        let api_len = rest[api_start..].find('`')?;
        let api = rest[api_start..api_start + api_len].to_string();
        if !api.contains(" /") {
            return None;
        }
        let needs_at = rest.find("the github app needs ")? + "the github app needs ".len();
        let needs = rest[needs_at..]
            .split([';', '.', ','])
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        (needs.contains(':')).then_some((status, api, needs))
    }
}

/// The one install-level item for a permission the installation lacks:
/// what it limits, and who lifts it, once for the whole install.
pub fn install_item(permission: &str, level: &str, limits: &str) -> String {
    format!(
        "the GitHub app needs {}: {level} to {limits}; an organization owner accepts the permission update once for the install {INSTALL_LEVEL}",
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

    #[test]
    fn a_denial_needs_a_real_answer_on_a_named_api() {
        let raw = "HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/acme/site/actions/workflows/e2e.yml/dispatches): {\"message\":\"Resource not accessible by integration\"}";
        let d = Denial::from_error("post", raw, "actions", "write").unwrap();
        assert_eq!(
            d.api,
            "POST /repos/acme/site/actions/workflows/e2e.yml/dispatches"
        );
        assert_eq!(
            d.line(),
            "GitHub answered 403 to `POST /repos/acme/site/actions/workflows/e2e.yml/dispatches`: the GitHub App needs Actions: write"
        );
        let (status, api, needs) = Denial::parse(&d.line()).unwrap();
        assert_eq!((status, needs.as_str()), (403, "actions: write"));
        assert!(api.starts_with("post /repos/"));
        // A request held for CoderHelm's own budget was never answered.
        let held = "HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/acme/site/issues/7/comments): not sent, what is left of GitHub's hourly budget is kept";
        assert_eq!(Denial::from_error("GET", held, "issues", "read"), None);
        // A rate limit, a timeout, or a plain 404 is no denial.
        assert_eq!(Denial::from_error("GET", "HTTP status client error (403 Forbidden) for url (https://api.github.com/x): API rate limit exceeded", "actions", "read"), None);
        assert_eq!(
            Denial::from_error("GET", "operation timed out", "actions", "read"),
            None
        );
        assert_eq!(Denial::from_error("GET", "HTTP status client error (404 Not Found) for url (https://api.github.com/repos/a/b/x)", "actions", "read"), None);
        assert_eq!(
            Denial::from_answer(
                "GET",
                "/repos/a/b/x",
                403,
                &serde_json::json!({"message": "API rate limit exceeded"}),
                "actions",
                "read"
            ),
            None
        );
        // Words alone record nothing.
        assert_eq!(
            Denial::parse("the app is not allowed to start workflows; grant Actions: write"),
            None
        );
    }

    #[test]
    fn the_install_item_names_the_permission_once_for_the_install() {
        let t = install_item("actions", "write", "start workflows with workflow_dispatch");
        assert!(t.starts_with("the GitHub app needs Actions: write"), "{t}");
        assert!(t.contains("organization owner accepts the permission update once"));
        assert!(t.ends_with(INSTALL_LEVEL));
    }
}
