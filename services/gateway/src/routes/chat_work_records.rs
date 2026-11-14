//! Chat's findings and records lookups: security findings (dependency
//! advisories and CVEs), infrastructure findings, quarantined tests,
//! reviews, alerts, log findings and plans.
//!
//! Each kind is read the way its dashboard page reads it: the page's own
//! handler, called with the asker's claims at the role the route grants
//! them (`permissions::resolve_access`), so Chat reads exactly what the page
//! would show them and nothing of another team. What comes back is filtered,
//! bounded and turned into citable records here ([`pick`], pure).

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::{Extension, Json};
use serde_json::{json, Value};

use super::chat_work::{clip, words_of, Found};
use crate::models::Claims;
use crate::permissions::{resolve_access, Access, Holder};
use crate::AppState;

/// Most records one lookup returns.
pub const MAX_RESULTS: usize = 10;
/// Longest passage kept for one record.
const MAX_PASSAGE: usize = 1600;

/// What the model asked for.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecordAsk {
    pub kind: String,
    pub words: Vec<String>,
    /// Advisory ids (CVE, GHSA) named, upper case: matched exactly.
    pub ids: Vec<String>,
    pub repo: Option<String>,
    pub pr: Option<u64>,
    /// `open`, `fixed`, `dismissed` or `any`.
    pub state: String,
}

impl RecordAsk {
    pub fn from_input(input: &Value, default_state: &str) -> RecordAsk {
        let s = |k: &str| {
            input[k]
                .as_str()
                .map(str::trim)
                .filter(|v| !v.is_empty() && v.len() <= 200)
                .map(str::to_string)
        };
        let query = s("query").unwrap_or_default();
        let h = super::chat_hints::read(&query);
        let ids = h.advisories;
        let words = words_of(&query)
            .into_iter()
            .filter(|w| !ids.iter().any(|id| id.eq_ignore_ascii_case(w)))
            .collect();
        RecordAsk {
            kind: s("kind").unwrap_or_default(),
            words,
            ids,
            repo: s("repo").map(|r| r.to_lowercase()),
            pr: input["pr"].as_u64(),
            state: s("state")
                .map(|v| v.to_ascii_lowercase())
                .unwrap_or_else(|| default_state.to_string()),
        }
    }
}

/// The claims a handler runs with for this person: their own, at the role
/// the route grants them. `None` when the route refuses them.
pub fn claims_for(claims: &Claims, holder: &Holder, handler: &str) -> Option<Claims> {
    match resolve_access(holder, Some(handler)) {
        Access::Allow { effective_role } => {
            let mut c = claims.clone();
            c.role = effective_role.to_string();
            Some(c)
        }
        Access::Deny { .. } => None,
    }
}

fn of<T: serde::Serialize>(r: Json<T>) -> Value {
    serde_json::to_value(r.0).unwrap_or(Value::Null)
}

fn q<T: serde::de::DeserializeOwned>(v: Value) -> Result<Query<T>, String> {
    serde_json::from_value(v)
        .map(Query)
        .map_err(|e| format!("query: {e}"))
}

/// The rows a kind's page shows this person. Only the asker's team: every
/// handler reads `claims.team_id`.
pub async fn fetch(
    state: &Arc<AppState>,
    claims: &Claims,
    holder: &Holder,
    ask: &RecordAsk,
) -> Result<Vec<Value>, String> {
    let kind = super::chat_work::kind(&ask.kind).ok_or("Pick one of the listed kinds.")?;
    let c = claims_for(claims, holder, kind.handler)
        .ok_or("Your role can't see these in the dashboard.")?;
    let st = State(state.clone());
    let ext = Extension(c.clone());
    let failed = |what: &str| format!("Coderhelm couldn't read the {what} just now.");
    let v: Value = match kind.key {
        "security" => {
            // Every finding of the team (the page lists them a page at a time).
            let all = Box::pin(super::security::load_findings(state, &c.team_id))
                .await
                .map_err(|_| failed("security findings"))?;
            json!({ "findings": all.into_iter().map(|(f, s)| {
                let mut v = json!(f);
                if let Some(sev) = s.severity { v["severity"] = json!(sev); }
                if let Some(t) = s.ticket { v["fix_ticket"] = json!(t); }
                v
            }).collect::<Vec<_>>() })
        }
        "infra" => Box::pin(super::infra_security::findings(st, ext))
            .await
            .map(of)
            .map_err(|_| failed("infrastructure findings"))?,
        "tests" => {
            let mut p = json!({});
            if ask.state != "any" {
                p["state"] = json!("quarantined");
            }
            if let Some(r) = &ask.repo {
                p["repo"] = json!(r);
            }
            Box::pin(super::tests::list_tests(st, ext, q(p)?))
                .await
                .map(of)
                .map_err(|_| failed("tests"))?
        }
        "reviews" => {
            let mut p = json!({ "limit": 100 });
            if let Some(r) = ask.repo.as_deref().filter(|r| r.contains('/')) {
                p["repo"] = json!(r);
            }
            Box::pin(super::reviewer::list_reviews(st, ext, q(p)?))
                .await
                .map(of)
                .map_err(|_| failed("reviews"))?
        }
        "alerts" => Box::pin(super::alerts::list_events(st, ext, q(json!({}))?))
            .await
            .map(of)
            .map_err(|_| failed("alerts"))?,
        "log_findings" => Box::pin(super::log_findings::list_findings(st, ext, q(json!({}))?))
            .await
            .map(of)
            .map_err(|_| failed("log findings"))?,
        "plans" => Box::pin(super::plans::list_plans(
            st,
            ext,
            q(json!({ "limit": "100" }))?,
        ))
        .await
        .map(of)
        .map_err(|_| failed("plans"))?,
        _ => return Err("Pick one of the listed kinds.".into()),
    };
    Ok(rows(kind.key, &v))
}

/// The list in a page's reply.
pub fn rows(kind: &str, v: &Value) -> Vec<Value> {
    let key = match kind {
        "tests" => "tests",
        "reviews" => "reviews",
        "alerts" => "alerts",
        "plans" => "plans",
        _ => "findings",
    };
    v[key].as_array().cloned().unwrap_or_default()
}

fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or("")
}

/// The record's status word, from whichever field the kind keeps it in.
fn status_of(v: &Value) -> String {
    ["status", "state", "verdict"]
        .iter()
        .find_map(|k| v[*k].as_str())
        .unwrap_or("")
        .to_ascii_lowercase()
}

const FIXED: &[&str] = &[
    "fixed",
    "merged",
    "done",
    "resolved",
    "healthy",
    "completed",
];
const DISMISSED: &[&str] = &["dismissed", "withdrawn", "ignored", "aws_managed"];

fn in_state(v: &Value, want: &str) -> bool {
    let st = status_of(v);
    match want {
        "open" => !FIXED.contains(&st.as_str()) && !DISMISSED.contains(&st.as_str()),
        "fixed" => FIXED.contains(&st.as_str()),
        "dismissed" => DISMISSED.contains(&st.as_str()),
        _ => true,
    }
}

/// Fields too long or too internal for a passage.
const SKIP: &[&str] = &[
    "body",
    "history",
    "events",
    "path",
    "evidence",
    "ranges",
    "symbols",
    "resources",
    "executions",
    "task_order",
    "cards",
    "samples",
    "inspector",
    "sources",
    "data",
];

/// A record's fields as plain lines, one level deep.
pub fn lines_of(v: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let Some(obj) = v.as_object() else {
        return out;
    };
    let scalar = |x: &Value| -> Option<String> {
        match x {
            Value::String(t) if !t.trim().is_empty() => Some(clip(t, 300)),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Array(a)
                if !a.is_empty() && a.iter().all(|i| !i.is_object() && !i.is_array()) =>
            {
                Some(clip(
                    &a.iter()
                        .take(8)
                        .map(|i| {
                            i.as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| i.to_string())
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                    300,
                ))
            }
            _ => None,
        }
    };
    for (k, x) in obj {
        if SKIP.contains(&k.as_str()) {
            continue;
        }
        if let Some(t) = scalar(x) {
            out.push(format!("{}: {t}", k.replace('_', " ")));
        } else if let Some(inner) = x.as_object() {
            for (k2, y) in inner {
                if SKIP.contains(&k2.as_str()) {
                    continue;
                }
                if let Some(t) = scalar(y) {
                    out.push(format!(
                        "{} {}: {t}",
                        k.replace('_', " "),
                        k2.replace('_', " ")
                    ));
                }
            }
        }
    }
    out
}

/// The record's date (for ordering and `as_of`).
fn date_of(v: &Value) -> String {
    [
        "updated_at",
        "last_seen_at",
        "last_seen",
        "received_at",
        "created_at",
        "first_seen_at",
    ]
    .iter()
    .find_map(|k| v[*k].as_str().filter(|t| !t.is_empty()))
    .unwrap_or("")
    .to_string()
}

/// The repository a record is about.
fn repo_of(v: &Value) -> String {
    ["repo", "fix_repo"]
        .iter()
        .find_map(|k| v[*k].as_str().filter(|t| !t.is_empty()))
        .unwrap_or("")
        .to_string()
}

/// One record as a citable source: its title, page and passage.
pub fn found(kind: &str, v: &Value) -> Found {
    let enc = |t: &str| urlencoding::encode(t).into_owned();
    let repo = repo_of(v);
    let (title, href, url) = match kind {
        "security" => {
            let adv = v["advisory"]["primary"]
                .as_str()
                .or_else(|| v["advisory"]["ids"][0].as_str())
                .unwrap_or("Advisory");
            (
                format!("{adv} in {} ({})", s(v, "package"), short(&repo)),
                Some(format!("/security/detail?id={}", enc(s(v, "id")))),
                v["pr"]["url"].as_str().map(str::to_string),
            )
        }
        "infra" => (
            clip(s(v, "title"), 120),
            Some(format!(
                "/security/infrastructure?finding={}",
                enc(s(v, "id"))
            )),
            v["pr"]["url"].as_str().map(str::to_string),
        ),
        "tests" => (
            format!("Test: {}", clip(s(v, "title"), 100)),
            Some(format!("/tests/test?id={}", enc(s(v, "id")))),
            None,
        ),
        "reviews" => (
            format!(
                "Review of {}#{}: {}",
                short(&repo),
                v["pr_number"].as_u64().unwrap_or(0),
                s(v, "verdict")
            ),
            Some(format!("/reviewer/detail?sk={}", enc(s(v, "sk")))),
            None,
        ),
        "alerts" => (
            format!("Alert: {}", clip(s(v, "title"), 100)),
            Some(format!("/alerts/detail?id={}", enc(s(v, "id")))),
            None,
        ),
        "log_findings" => (
            format!("Log finding: {}", clip(s(v, "title"), 100)),
            Some("/logs".to_string()),
            None,
        ),
        "plans" => (
            format!("Plan: {}", clip(s(v, "title"), 100)),
            Some(format!("/plans/detail?id={}", enc(s(v, "plan_id")))),
            None,
        ),
        _ => (kind.to_string(), None, None),
    };
    let mut passage = lines_of(v).join("\n");
    if passage.chars().count() > MAX_PASSAGE {
        passage = clip(&passage, MAX_PASSAGE);
    }
    if let Some(h) = &href {
        passage.push_str(&format!("\nPage: {h}"));
    }
    Found {
        kind: if matches!(kind, "security" | "infra" | "tests") {
            "finding"
        } else {
            "record"
        },
        title,
        loc: repo,
        as_of: date_of(v),
        passage,
        href,
        url: url.and_then(|u| common::knowledge::answer::original_url(&u)),
    }
}

fn short(repo: &str) -> &str {
    repo.rsplit('/').next().unwrap_or(repo)
}

/// The rows that match, newest first, bounded.
pub fn pick(kind: &str, rows: &[Value], ask: &RecordAsk) -> Vec<Found> {
    let mut kept: Vec<(usize, String, &Value)> = rows
        .iter()
        .filter(|v| in_state(v, &ask.state))
        .filter(|v| {
            ask.repo.as_ref().is_none_or(|r| {
                let have = repo_of(v).to_lowercase();
                have == *r || short(&have) == short(r)
            })
        })
        .filter(|v| {
            ask.pr.is_none_or(|n| {
                v["pr_number"].as_u64() == Some(n) || v["pr"]["number"].as_u64() == Some(n)
            })
        })
        .filter_map(|v| {
            let text = v.to_string().to_lowercase();
            // An advisory id must be there exactly.
            if !ask.ids.iter().all(|id| text.contains(&id.to_lowercase())) {
                return None;
            }
            let hits = ask
                .words
                .iter()
                .filter(|w| text.contains(w.as_str()))
                .count();
            if !ask.words.is_empty() && hits * 2 < ask.words.len() {
                return None;
            }
            Some((hits, date_of(v), v))
        })
        .collect();
    kept.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    kept.into_iter()
        .take(MAX_RESULTS)
        .map(|(_, _, v)| found(kind, v))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sec(
        id: &str,
        cve: &str,
        package: &str,
        repo: &str,
        status: &str,
        pr: Option<&str>,
    ) -> Value {
        json!({
            "id": id, "repo": repo, "package": package, "version": "1.0.0", "fixed_version": "1.0.1",
            "advisory": {"ids": [cve, "GHSA-aaaa-bbbb-cccc"], "primary": cve, "title": "Prototype pollution"},
            "status": status, "updated_at": "2026-11-10T00:00:00Z",
            "pr": pr.map(|u| json!({"number": 12, "url": u, "state": "merged"})),
        })
    }

    #[test]
    fn a_cve_is_found_by_its_id_in_one_repository_with_its_fixing_pr() {
        let rows = vec![
            sec(
                "f1",
                "CVE-2024-1111",
                "lodash",
                "acme/web",
                "fixed",
                Some("https://github.com/acme/web/pull/12"),
            ),
            sec("f2", "CVE-2024-1111", "lodash", "acme/api", "open", None),
            sec("f3", "CVE-2024-2222", "express", "acme/web", "open", None),
        ];
        let ask = RecordAsk::from_input(
            &json!({"kind": "security", "query": "CVE-2024-1111", "repo": "web", "state": "any"}),
            "open",
        );
        let got = pick("security", &rows, &ask);
        assert_eq!(got.len(), 1, "{got:?}");
        let f = &got[0];
        assert_eq!(f.title, "CVE-2024-1111 in lodash (web)");
        assert_eq!(f.kind, "finding");
        assert!(f.passage.contains("status: fixed"), "{}", f.passage);
        assert_eq!(
            f.url.as_deref(),
            Some("https://github.com/acme/web/pull/12")
        );
        assert_eq!(f.href.as_deref(), Some("/security/detail?id=f1"));
        // Open ones only, unless asked: the fixed one is left out.
        let open = RecordAsk::from_input(
            &json!({"kind": "security", "query": "CVE-2024-1111"}),
            "open",
        );
        let got = pick("security", &rows, &open);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].loc, "acme/api");
    }

    #[test]
    fn open_infra_findings_leave_out_fixed_and_dismissed_ones() {
        let rows: Vec<Value> = ["open", "fixing", "pr_open", "fixed", "aws_managed", "dismissed"]
            .iter()
            .enumerate()
            .map(|(i, st)| json!({"id": format!("i{i}"), "title": format!("Old AMI {i}"), "status": st, "updated_at": format!("2026-11-0{}T00:00:00Z", i + 1)}))
            .collect();
        let ask = RecordAsk::from_input(&json!({"kind": "infra"}), "open");
        let got = pick("infra", &rows, &ask);
        let ids: Vec<&str> = got.iter().map(|f| f.title.as_str()).collect();
        // Newest first.
        assert_eq!(ids, ["Old AMI 2", "Old AMI 1", "Old AMI 0"]);
        assert!(got[0]
            .href
            .as_deref()
            .unwrap()
            .starts_with("/security/infrastructure?finding="));
    }

    #[test]
    fn results_are_bounded_and_newest_first() {
        let rows: Vec<Value> = (0..40)
            .map(|i| json!({"plan_id": format!("p{i}"), "title": "Plan", "status": "draft", "updated_at": format!("2026-10-{:02}T00:00:00Z", i % 28 + 1)}))
            .collect();
        let got = pick(
            "plans",
            &rows,
            &RecordAsk::from_input(&json!({"kind": "plans"}), "any"),
        );
        assert_eq!(got.len(), MAX_RESULTS);
        assert!(got[0].as_of >= got[1].as_of);
        assert_eq!(got[0].kind, "record");
    }

    #[test]
    fn a_handler_runs_at_the_role_its_route_grants_and_never_above() {
        let claims: Claims = serde_json::from_value(json!({
            "sub": "u", "email": "a@example.com", "team_id": "TEAM#one", "role": "viewer",
            "exp": 0, "iat": 0
        }))
        .unwrap();
        let viewer = Holder::builtin("viewer");
        // Alerts are for members and up: a viewer is refused.
        assert!(claims_for(&claims, &viewer, "alerts::list_events").is_none());
        let member = Holder::builtin("member");
        let c = claims_for(&claims, &member, "alerts::list_events").unwrap();
        assert_eq!(c.role, "member");
        assert_eq!(c.team_id, "TEAM#one");
        // A page every role sees runs at the person's own role.
        let c = claims_for(&claims, &viewer, "security::list_findings").unwrap();
        assert_eq!(c.role, "viewer");
    }

    #[test]
    fn passages_leave_out_long_internal_fields() {
        let v = json!({"title": "t", "body": "x".repeat(5000), "status": "open", "pr": {"number": 3, "url": "https://github.com/a/b/pull/3"}});
        let l = lines_of(&v).join("\n");
        assert!(!l.contains("xxxx"));
        assert!(l.contains("pr number: 3"));
    }
}
