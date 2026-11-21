//! Chat's `coderhelm_setup` lookup: what Coderhelm is set up to do for the
//! asker's team, checked when it is called. Each connection is checked
//! with the same test the integrations page uses (GitHub's installation and
//! token, `common::jira_connection` for Jira), Coderhelm's runs over the
//! last 30 days are counted from the runs themselves, and the records the
//! asker may read (reviews, findings, alerts, quarantined tests) are counted
//! from the pages that show them. With it comes what Coderhelm does, in
//! plain words, each with a question to ask.
//!
//! It is what an orientation ("I'm new, what can you do for me?"), a
//! question about what Chat can do, and a correction about a connection are
//! answered from: a claim about connections, counts or state rests on a
//! check made in this answer, and a check that failed is named as that
//! check, never widened to "X isn't connected".

use common::knowledge::answer::{Registry, SourceRef};
use serde_json::{json, Value};

use super::chat_work_runs::Tally;

pub const SETUP: &str = "coderhelm_setup";

/// The days of runs counted.
pub const RUN_DAYS: u32 = 30;

pub fn def() -> Value {
    json!({
        "name": SETUP,
        "description": "What Coderhelm is set up to do for this team, checked now: each connection (GitHub, Jira, connected apps) with exactly what works and what doesn't, the repositories and knowledge bases, how many runs Coderhelm made in the last 30 days and how they ended, how many reviews, findings, alerts and quarantined tests it keeps, the asker's Coderhelm role, what Chat can read for this answer and what it can't do. Also what Coderhelm does, each with an example question. Call it for an orientation (\"I'm new\", \"what can you do for me\"), for whether Chat or Coderhelm can do something, before any claim about what is connected or how much Coderhelm does, and again whenever the person questions such a claim. No input.",
        "input_schema": {"type": "object", "properties": {}}
    })
}

/// One check's outcome, in words for the answer.
#[derive(Debug, Clone, PartialEq)]
pub enum Check {
    /// Works (with what it does).
    Ok(String),
    /// Set up in part: what works and what doesn't, as the check said it.
    Partly(String),
    /// Not set up (the check's own words).
    Off(String),
    /// The check itself failed: what failed, never "not connected".
    Failed(String),
}

impl Check {
    fn words(&self) -> String {
        match self {
            Check::Ok(w) => format!("works: {w}"),
            Check::Partly(w) => format!("set up in part: {w}"),
            Check::Off(w) => format!("not set up: {w}"),
            Check::Failed(w) => format!(
                "couldn't be checked just now ({w}); this says nothing about whether it is set up"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Connection {
    pub name: String,
    pub check: Check,
}

/// A count of records the asker may read.
#[derive(Debug, Clone, PartialEq)]
pub struct Counted {
    pub what: &'static str,
    pub n: Result<usize, String>,
    /// The read stopped at a limit: at least this many.
    pub at_least: bool,
}

/// Everything the lookup found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Setup {
    pub checked_at: String,
    /// The asker's Coderhelm role, and whether it may change integrations.
    pub role: String,
    pub can_change_integrations: bool,
    pub github_linked: bool,
    pub connections: Vec<Connection>,
    pub repos: Vec<String>,
    pub knowledge: Vec<String>,
    pub runs: Option<Result<Tally, String>>,
    pub records: Vec<Counted>,
    /// What Chat can read for this answer, by source.
    pub reads: Vec<String>,
}

/// One thing Coderhelm does.
pub struct Feature {
    pub name: &'static str,
    pub does: &'static str,
    /// Who leans on it most.
    pub helps: &'static str,
    /// A question to ask Chat about it.
    pub ask: &'static str,
}

pub const FEATURES: &[Feature] = &[
    Feature {
        name: "Tickets to pull requests",
        does: "A Jira ticket or GitHub issue with the coderhelm label (or a /coderhelm comment) becomes a run: Coderhelm plans the change, writes it on a coderhelm/ branch, runs the build and tests, reviews its own work and opens the pull request. It then fixes failing CI, answers review comments and resolves conflicts.",
        helps: "engineers, engineering managers",
        ask: "What's Coderhelm running right now?",
    },
    Feature {
        name: "Pull request reviews",
        does: "Reviews pull requests with the review label: line comments, a risk level, and a verdict that asks for changes only for a named, evidenced problem.",
        helps: "engineers, tech leads, engineering leaders",
        ask: "What did Coderhelm's reviews flag this week?",
    },
    Feature {
        name: "Where work stands",
        does: "Chat answers from the team's tickets, pull requests and Coderhelm's runs, with the date each last changed.",
        helps: "engineering leaders, product managers, engineering managers",
        ask: "Where are we on the checkout redesign?",
    },
    Feature {
        name: "What's waiting on you",
        does: "One list of the questions runs stopped on, approvals held for you, reviews asked of you and tickets assigned to you, most urgent first.",
        helps: "everyone",
        ask: "What needs my answer?",
    },
    Feature {
        name: "Security and infrastructure",
        does: "Dependency advisories checked for whether your code reaches them, with fix pull requests; infrastructure findings; the team's AWS resources and where each is defined.",
        helps: "engineering leaders, security, platform and DevOps",
        ask: "Which critical findings are open, and who is fixing them?",
    },
    Feature {
        name: "Alerts, logs and flaky tests",
        does: "CloudWatch alarms become fix pull requests, chosen log groups are read for problems, and flaky tests are found and quarantined.",
        helps: "platform and DevOps, engineers on call",
        ask: "What alerts came in this week and what was done about them?",
    },
    Feature {
        name: "Answers from the code and docs",
        does: "Chat reads the team's code (citing files and lines), its knowledge bases (Notion, Confluence, Google Drive, websites, files) and, when connected, logs, databases and other apps, only reading.",
        helps: "everyone, especially people new to the code",
        ask: "How is our booking flow put together?",
    },
    Feature {
        name: "Team controls",
        does: "Agent permissions (allow, ask or deny each action), custom roles, monthly token limits, usage and audit records, and an API and MCP server for the team's own AI tools.",
        helps: "engineering leaders, admins",
        ask: "What can Coderhelm's agents do without asking us?",
    },
];

/// What Chat itself can't do, whatever is connected.
pub const CHAT_CANNOT: &str = "Chat only reads: it can't change code, settings or records, and it can't run commands, scripts, tests or deploys. To have Coderhelm change code, label a ticket or issue for a run.";

fn count_words(c: &Counted) -> String {
    match &c.n {
        Ok(n) if c.at_least => format!("{}: at least {n}", c.what),
        Ok(n) => format!("{}: {n}", c.what),
        Err(e) => format!("{}: couldn't be counted just now ({e})", c.what),
    }
}

fn runs_words(t: &Tally) -> String {
    if t.started == 0 {
        return format!(
            "Coderhelm's runs in the last {} days: none started.",
            t.days
        );
    }
    let mut parts = vec![format!("{} finished", t.done)];
    for (n, w) in [
        (t.running, "working now"),
        (t.waiting, "waiting on the team"),
        (t.failed, "stopped with an error"),
        (t.stopped, "cancelled or closed"),
    ] {
        if n > 0 {
            parts.push(format!("{n} {w}"));
        }
    }
    let repos: Vec<String> = t.repos.iter().map(|(r, n)| format!("{r} ({n})")).collect();
    format!(
        "Coderhelm's runs in the last {} days: {}{} started ({}); {} opened a pull request; by repository: {}; the newest started {}.",
        t.days,
        if t.capped { "at least " } else { "" },
        t.started,
        parts.join(", "),
        t.with_pr,
        if repos.is_empty() { "none named".to_string() } else { repos.join(", ") },
        t.latest
    )
}

/// The team's setup as a source the answer cites, and what Coderhelm does
/// as another. Returns the text the model reads and the step's summary.
pub fn render(s: &Setup, reg: &mut Registry) -> (String, String) {
    let mut lines = vec![format!(
        "Checked just now ({}). Each line is that check's own result.",
        s.checked_at
    )];
    lines.push("Connections:".into());
    for c in &s.connections {
        lines.push(format!("- {}: {}", c.name, c.check.words()));
    }
    lines.push(format!(
        "Repositories Coderhelm works in: {}{}",
        s.repos.len(),
        if s.repos.is_empty() {
            String::new()
        } else {
            format!(
                " ({}{})",
                s.repos
                    .iter()
                    .take(12)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", "),
                if s.repos.len() > 12 { ", ..." } else { "" }
            )
        }
    ));
    lines.push(format!(
        "Knowledge bases this person can read: {}{}",
        s.knowledge.len(),
        if s.knowledge.is_empty() {
            String::new()
        } else {
            format!(
                " ({})",
                s.knowledge
                    .iter()
                    .take(10)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    ));
    match &s.runs {
        Some(Ok(t)) => lines.push(runs_words(t)),
        Some(Err(e)) => lines.push(format!(
            "Coderhelm's runs couldn't be counted just now ({e})."
        )),
        None => lines.push("Coderhelm's runs: this person's role can't see runs.".into()),
    }
    if !s.records.is_empty() {
        lines.push(format!(
            "Records Coderhelm keeps that this person can see: {}.",
            s.records
                .iter()
                .map(count_words)
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    lines.push(format!(
        "The person asking: Coderhelm role {}{}; GitHub account {}.",
        if s.role.is_empty() {
            "unknown"
        } else {
            &s.role
        },
        if s.can_change_integrations {
            " (can change integrations)"
        } else {
            " (can't change integrations; an admin can)"
        },
        if s.github_linked {
            "linked"
        } else {
            "not linked"
        }
    ));
    if !s.reads.is_empty() {
        lines.push(format!(
            "What Chat can read for this answer: {}.",
            s.reads.join(", ")
        ));
    }
    lines.push(CHAT_CANNOT.into());
    let team = reg.add(SourceRef {
        kind: "coderhelm".into(),
        title: "Coderhelm for this team".into(),
        loc: "Settings and runs".into(),
        as_of: s.checked_at.clone(),
        passage: lines.join("\n"),
        href: Some("/settings/integrations".into()),
        ..Default::default()
    });
    let features: Vec<String> = FEATURES
        .iter()
        .map(|f| {
            format!(
                "- {}: {} Helps most: {}. Ask: \u{201c}{}\u{201d}",
                f.name, f.does, f.helps, f.ask
            )
        })
        .collect();
    let what = reg.add(SourceRef {
        kind: "coderhelm".into(),
        title: "What Coderhelm does".into(),
        loc: "Coderhelm".into(),
        passage: features.join("\n"),
        url: Some("https://docs.coderhelm.com".into()),
        ..Default::default()
    });
    // The source's own text is read whole (the citation keeps an excerpt).
    let text = format!(
        "{}{}A line that couldn't be checked says only that the check failed: say which check and what it said, never that the thing isn't set up.",
        reg.render_with(&team, &lines.join("\n")),
        reg.render_with(&what, &features.join("\n")),
    );
    let gaps = s
        .connections
        .iter()
        .filter(|c| matches!(c.check, Check::Failed(_)))
        .count();
    let summary = if gaps == 0 {
        "Checked the setup".to_string()
    } else {
        format!("Checked the setup, {gaps} check failed")
    };
    (text, summary)
}

// ── Gathering ───────────────────────────────────────────────────────────────

/// GitHub, from what the question already loaded: an installation and
/// whether its token came.
pub fn github_check(installation: Option<u64>, token: bool, repos: usize) -> Check {
    match (installation, token) {
        (Some(_), true) => Check::Ok(format!(
            "Coderhelm's GitHub app is installed and its token works; {repos} repositories enabled"
        )),
        (Some(_), false) => {
            Check::Failed("the GitHub app is installed, but its token couldn't be fetched".into())
        }
        (None, _) => Check::Off("Coderhelm's GitHub app isn't installed for this team".into()),
    }
}

/// Jira, from the one test of "connected" every reader uses.
pub fn jira_check(j: &super::chat_work_tickets::Jira) -> Check {
    if j.app.is_some() {
        return Check::Ok(
            "Jira is connected through the Jira app; Chat can look tickets up, and runs start from labeled tickets".into(),
        );
    }
    let words = j.off_words();
    if words == common::jira_connection::NOT_CONNECTED {
        Check::Off(words.to_string())
    } else {
        // "Jira is connected, but the app hasn't registered ...".
        Check::Partly(words.to_string())
    }
}

/// The connected apps Chat can use, and those that need a sign-in.
pub fn app_checks(servers: &[String], missing: &[super::chat_policy::Missing]) -> Vec<Connection> {
    let mut out = Vec::new();
    if !servers.is_empty() {
        out.push(Connection {
            name: "Connected apps".into(),
            check: Check::Ok(format!("Chat can use {}", servers.join(", "))),
        });
    }
    if !missing.is_empty() {
        out.push(Connection {
            name: "Apps waiting on a sign-in".into(),
            check: Check::Partly(format!(
                "{} {} set up for the team, but this person can't use {} until they sign in or an admin reconnects",
                missing.iter().map(|m| m.name.clone()).collect::<Vec<_>>().join(", "),
                if missing.len() == 1 { "is" } else { "are" },
                if missing.len() == 1 { "it" } else { "them" },
            )),
        });
    }
    out
}

/// The records the asker may read, counted from the pages that show them.
pub async fn count_records(ctx: &super::chat_work::Ctx) -> Vec<Counted> {
    use super::chat_work_records as rec;
    let kinds: [(&str, &'static str, &str); 5] = [
        ("reviews", "pull request reviews", "any"),
        ("security", "open security findings", "open"),
        ("infra", "open infrastructure findings", "open"),
        ("alerts", "alerts", "any"),
        ("tests", "quarantined tests", "open"),
    ];
    let reads = kinds
        .iter()
        .filter(|(k, _, _)| super::chat_work::may(&ctx.readable, k))
        .map(|(k, what, state)| async move {
            let ask = rec::RecordAsk {
                kind: k.to_string(),
                state: state.to_string(),
                ..Default::default()
            };
            let n = Box::pin(rec::fetch(&ctx.state, &ctx.claims, &ctx.holder, &ask))
                .await
                .map(|rows| rows.iter().filter(|r| rec::in_state(r, state)).count());
            Counted {
                what,
                at_least: *k == "reviews" && n.as_ref().is_ok_and(|n| *n >= 100),
                n,
            }
        });
    futures::future::join_all(reads).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::chat_work_tickets::{Jira, JiraApp};

    fn setup() -> Setup {
        Setup {
            checked_at: "2026-11-19 14:00 UTC".into(),
            role: "admin".into(),
            can_change_integrations: true,
            github_linked: true,
            connections: vec![
                Connection {
                    name: "GitHub".into(),
                    check: github_check(Some(7), true, 3),
                },
                Connection {
                    name: "Jira".into(),
                    check: jira_check(&Jira {
                        app: Some(JiraApp {
                            issues_url: "https://x".into(),
                            secret: "s".into(),
                            site: "https://acme.atlassian.net".into(),
                        }),
                        off: String::new(),
                    }),
                },
            ],
            repos: vec!["acme/web".into(), "acme/api".into(), "acme/infra".into()],
            knowledge: vec!["Engineering handbook".into()],
            runs: Some(Ok(Tally {
                days: 30,
                started: 42,
                done: 30,
                waiting: 5,
                failed: 2,
                with_pr: 31,
                repos: vec![("acme/web".into(), 30), ("acme/api".into(), 12)],
                latest: "2026-11-18T22:00:00Z".into(),
                ..Default::default()
            })),
            records: vec![
                Counted {
                    what: "pull request reviews",
                    n: Ok(100),
                    at_least: true,
                },
                Counted {
                    what: "open security findings",
                    n: Err("Coderhelm couldn't read the security findings just now.".into()),
                    at_least: false,
                },
            ],
            reads: vec!["Code".into(), "Tickets, pull requests and runs".into()],
        }
    }

    #[test]
    fn the_setup_is_two_citable_sources_with_every_check_in_its_own_words() {
        let mut reg = Registry::default();
        let (text, summary) = render(&setup(), &mut reg);
        assert_eq!(reg.len(), 2);
        assert!(text.contains("id=\"S1\"") && text.contains("id=\"S2\""));
        assert!(
            text.contains("GitHub: works: Coderhelm's GitHub app is installed"),
            "{text}"
        );
        assert!(text.contains("Jira: works: Jira is connected"), "{text}");
        assert!(
            text.contains(
                "42 started (30 finished, 5 waiting on the team, 2 stopped with an error)"
            ),
            "{text}"
        );
        assert!(text.contains("31 opened a pull request"));
        assert!(text.contains("pull request reviews: at least 100"));
        assert!(text.contains("open security findings: couldn't be counted just now"));
        assert!(text.contains("Coderhelm role admin (can change integrations)"));
        assert!(text.contains(CHAT_CANNOT));
        for f in FEATURES {
            assert!(text.contains(f.name) && text.contains(f.ask), "{}", f.name);
        }
        assert_eq!(summary, "Checked the setup");
        assert!(!text.contains("isn't connected"), "{text}");
    }

    #[test]
    fn a_failed_check_is_named_as_that_check_and_never_as_not_connected() {
        let mut s = setup();
        s.connections[0].check = github_check(Some(7), false, 3);
        s.connections[1].check = jira_check(&Jira {
            app: None,
            off: common::jira_connection::Gap::LookupNotRegistered
                .words()
                .into(),
        });
        let mut reg = Registry::default();
        let (text, summary) = render(&s, &mut reg);
        assert!(text.contains("GitHub: couldn't be checked just now (the GitHub app is installed, but its token couldn't be fetched)"), "{text}");
        assert!(
            text.contains("Jira: set up in part: Jira is connected, but"),
            "{text}"
        );
        assert!(!text.contains("isn't connected"), "{text}");
        assert_eq!(summary, "Checked the setup, 1 check failed");
        // Only a team with no Jira at all hears "not connected", as Jira's own words.
        let off = jira_check(&Jira::default());
        assert_eq!(
            off,
            Check::Off(common::jira_connection::NOT_CONNECTED.into())
        );
        assert_eq!(
            github_check(None, false, 0),
            Check::Off("Coderhelm's GitHub app isn't installed for this team".into())
        );
    }

    #[test]
    fn the_lookup_is_read_only_and_known_to_the_permission_gate() {
        assert_eq!(
            common::agent_policy::registry::builtin(SETUP),
            Some(common::agent_policy::Risk::Read)
        );
        assert!(def()["description"]
            .as_str()
            .unwrap()
            .contains("orientation"));
    }
}
