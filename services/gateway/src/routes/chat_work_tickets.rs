//! Chat's ticket lookup: the team's Jira tickets by key, title words or
//! text, through the Coderhelm app the team installed in Jira (its
//! get-issues trigger, with the shared secret from the team's `JIRA#config`),
//! and the tickets Coderhelm's runs worked on.
//!
//! A title is looked for in both: Jira's text search (an app version that
//! has it), and the runs' titles, whose ticket keys are then read from Jira
//! by key. When Jira can't be reached, the tickets come from the runs alone,
//! and the answer is told so.

use common::mcp_remote::http::{Http, HttpRequest, Limit};
use serde_json::{json, Value};

use super::chat_work::{clip, Found};

/// Most tickets one lookup returns.
pub const MAX_RESULTS: usize = 6;

/// The team's Jira app, as the ticket lookup uses it.
#[derive(Clone)]
pub struct JiraApp {
    pub issues_url: String,
    pub secret: String,
    /// `https://<site>.atlassian.net`, for links.
    pub site: String,
}

impl std::fmt::Debug for JiraApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JiraApp")
            .field("site", &self.site)
            .finish_non_exhaustive()
    }
}

/// The team's Jira app from its `JIRA#config` record (only the asker's team
/// is read), when it registered the get-issues trigger.
pub async fn load(state: &crate::AppState, team_id: &str) -> Option<JiraApp> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S(
                common::live::jira_app::JIRA_CONFIG_SK.to_string(),
            ),
        )
        .send()
        .await
        .ok()?
        .item?;
    let get = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    Some(JiraApp {
        issues_url: get("get_issues_url").filter(|u| u.starts_with("https://"))?,
        secret: get("forge_secret")?,
        site: get("site_url")
            .filter(|u| u.starts_with("https://"))
            .unwrap_or_default(),
    })
}

/// A ticket key as Jira writes them.
pub fn valid_key(k: &str) -> bool {
    let Some((p, n)) = k.split_once('-') else {
        return false;
    };
    !p.is_empty()
        && p.len() <= 10
        && p.bytes().next().is_some_and(|b| b.is_ascii_uppercase())
        && p.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        && !n.is_empty()
        && n.len() <= 7
        && n.bytes().all(|b| b.is_ascii_digit())
}

/// Asks the app for issues: by keys, or by a text search.
pub async fn ask_jira(
    http: &dyn Http,
    app: &JiraApp,
    keys: &[String],
    query: Option<&str>,
) -> Result<Vec<Value>, String> {
    let mut body = json!({ "forge_secret": app.secret });
    if !keys.is_empty() {
        body["keys"] = json!(keys.iter().take(MAX_RESULTS).collect::<Vec<_>>());
    } else if let Some(q) = query.filter(|q| !q.trim().is_empty()) {
        body["query"] = json!(clip(q, 200));
    } else {
        return Ok(Vec::new());
    }
    let r = http
        .send(HttpRequest::json(&app.issues_url, &body, Limit::Metadata))
        .await
        .map_err(|e| format!("Jira couldn't be reached: {e}"))?;
    if r.status != 200 {
        return Err(format!("Jira's app answered {}", r.status));
    }
    Ok(r.json()
        .and_then(|v| v["issues"].as_array().cloned())
        .unwrap_or_default())
}

/// One Jira issue as a citable record.
pub fn from_jira(issue: &Value, site: &str, runs: &[Found]) -> Found {
    let s = |k: &str| issue[k].as_str().unwrap_or("").trim().to_string();
    let key = s("key");
    let mut lines = vec![format!("Ticket {key}: {}", s("summary"))];
    for (label, k) in [
        ("Status", "status"),
        ("Type", "type"),
        ("Assignee", "assignee"),
        ("Priority", "priority"),
        ("Created", "created"),
        ("Last changed", "updated"),
    ] {
        let v = s(k);
        if !v.is_empty() {
            lines.push(format!("{label}: {v}"));
        }
    }
    if issue["assignee"].as_str() == Some("") {
        lines.push("Assignee: nobody".into());
    }
    let comments: Vec<String> = issue["comments"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| {
            format!(
                "- {} ({}): {}",
                c["author"].as_str().unwrap_or("?"),
                c["created"].as_str().unwrap_or(""),
                clip(c["text"].as_str().unwrap_or(""), 400)
            )
        })
        .collect();
    if !comments.is_empty() {
        lines.push(format!("Latest comments:\n{}", comments.join("\n")));
    }
    let links: Vec<String> = issue["links"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| {
            let url = l["url"].as_str()?;
            Some(format!("- {} {url}", l["title"].as_str().unwrap_or("")))
        })
        .collect();
    if !links.is_empty() {
        lines.push(format!("Linked:\n{}", links.join("\n")));
    }
    let worked: Vec<&Found> = runs
        .iter()
        .filter(|r| r.passage.contains(&format!("Ticket: {key}")))
        .collect();
    if !worked.is_empty() {
        lines.push(format!(
            "Coderhelm runs on it: {}",
            worked
                .iter()
                .map(|r| r.title.clone())
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    let url = (!site.is_empty() && valid_key(&key))
        .then(|| format!("{}/browse/{key}", site.trim_end_matches('/')));
    if let Some(u) = &url {
        lines.push(format!("Link: {u}"));
    }
    Found {
        kind: "ticket",
        title: format!("{key} {}", clip(&s("summary"), 100)),
        loc: "Jira".into(),
        as_of: s("updated"),
        passage: lines.join("\n"),
        href: None,
        url: url.and_then(|u| common::knowledge::answer::original_url(&u)),
    }
}

/// The tickets the runs worked on, from the runs found: one record per
/// ticket, with what Coderhelm knows (Jira's own state isn't in it).
pub fn from_runs(runs: &[Found], site: &str, why: &str) -> Vec<Found> {
    let mut out: Vec<Found> = Vec::new();
    for r in runs {
        let Some(line) = r.passage.lines().find(|l| l.starts_with("Ticket: ")) else {
            continue;
        };
        let ticket = line
            .trim_start_matches("Ticket: ")
            .split(' ')
            .next()
            .unwrap_or("")
            .to_string();
        let jira = line.ends_with("(jira)");
        if ticket.is_empty()
            || out
                .iter()
                .any(|o| o.title.starts_with(&format!("{ticket} ")))
        {
            continue;
        }
        let title = r
            .passage
            .lines()
            .find_map(|l| l.strip_prefix("Title: "))
            .unwrap_or("")
            .to_string();
        let url = (jira && !site.is_empty() && valid_key(&ticket))
            .then(|| format!("{}/browse/{ticket}", site.trim_end_matches('/')));
        let mut passage = format!(
            "Ticket {ticket}: {title}\n{why}\nCoderhelm's latest run on it:\n{}",
            r.passage
        );
        if let Some(u) = &url {
            passage.push_str(&format!("\nLink: {u}"));
        }
        out.push(Found {
            kind: "ticket",
            title: format!("{ticket} {}", clip(&title, 100)),
            loc: r.loc.clone(),
            as_of: r.as_of.clone(),
            passage,
            href: r.href.clone(),
            url: url.and_then(|u| common::knowledge::answer::original_url(&u)),
        });
        if out.len() >= MAX_RESULTS {
            break;
        }
    }
    out
}

/// The ticket keys among runs found (their `Ticket:` lines), Jira's only.
pub fn keys_of(runs: &[Found]) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    for r in runs {
        for l in r.passage.lines().filter(|l| l.ends_with("(jira)")) {
            if let Some(rest) = l.strip_prefix("Ticket: ") {
                let k = rest.split(' ').next().unwrap_or("");
                if valid_key(k) && !keys.iter().any(|x| x == k) && keys.len() < MAX_RESULTS {
                    keys.push(k.to_string());
                }
            }
        }
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::chat_work_prs::tests::fake;
    use crate::routes::chat_work_runs::{record, tests::run};

    fn app() -> JiraApp {
        JiraApp {
            issues_url: "https://app.example.test/issues".into(),
            secret: "s3cret".into(),
            site: "https://team.atlassian.net".into(),
        }
    }

    #[test]
    fn keys_are_jira_keys_only() {
        assert!(valid_key("ABC-123"));
        assert!(!valid_key("abc-123") && !valid_key("ABC-") && !valid_key("GH 1"));
        assert!(!valid_key("plugin:x"));
    }

    #[tokio::test]
    async fn the_app_is_asked_by_key_or_by_words_with_the_secret() {
        let gh = fake(|r| {
            let body: Value =
                serde_json::from_slice(r.body.as_deref().unwrap_or(&b"{}"[..])).unwrap();
            assert_eq!(body["forge_secret"], "s3cret");
            if body["keys"].is_array() {
                (
                    200,
                    json!({"issues": [{"key": "ABC-7", "summary": "Embed the portal", "status": "In Review",
                    "assignee": "Dana", "updated": "2026-11-12T10:00:00.000+0000",
                    "comments": [{"author": "Dana", "created": "2026-11-12", "text": "PR is up"}],
                    "links": [{"title": "PR #5", "url": "https://github.com/acme/web/pull/5"}]}]}),
                )
            } else {
                (200, json!({"issues": []}))
            }
        });
        let got = ask_jira(&gh, &app(), &["ABC-7".into()], None)
            .await
            .unwrap();
        let t = from_jira(&got[0], &app().site, &[]);
        assert_eq!(t.kind, "ticket");
        assert!(t.passage.contains("Status: In Review"));
        assert!(t.passage.contains("Assignee: Dana"));
        assert!(t.passage.contains("PR is up"));
        assert!(t.passage.contains("https://github.com/acme/web/pull/5"));
        assert_eq!(
            t.url.as_deref(),
            Some("https://team.atlassian.net/browse/ABC-7")
        );
        assert_eq!(t.as_of, "2026-11-12T10:00:00.000+0000");
        let none = ask_jira(&gh, &app(), &[], Some("portal")).await.unwrap();
        assert!(none.is_empty());
        assert!(ask_jira(&gh, &app(), &[], None).await.unwrap().is_empty());
        // The secret never shows in a log.
        assert!(!format!("{:?}", app()).contains("s3cret"));
    }

    #[test]
    fn tickets_come_from_the_runs_when_jira_cant_say() {
        let runs = vec![
            record(&run(
                "02",
                "running",
                "ABC-7",
                "Embed the portal",
                "acme/web",
            )),
            record(&run(
                "01",
                "failed",
                "ABC-7",
                "Embed the portal",
                "acme/web",
            )),
            record(&{
                let mut r = run("00", "completed", "GH-4", "Fix the footer", "acme/web");
                r.insert(
                    "ticket_source".into(),
                    aws_sdk_dynamodb::types::AttributeValue::S("github".into()),
                );
                r
            }),
        ];
        assert_eq!(keys_of(&runs), ["ABC-7"]);
        let t = from_runs(
            &runs,
            "https://team.atlassian.net",
            "Jira couldn't be reached.",
        );
        assert_eq!(t.len(), 2);
        assert!(t[0].passage.contains("Jira couldn't be reached."));
        assert!(t[0].passage.contains("Run 02: running"));
        assert_eq!(
            t[0].url.as_deref(),
            Some("https://team.atlassian.net/browse/ABC-7")
        );
        // A GitHub issue has no Jira link.
        assert!(t[1].url.is_none());
    }
}
