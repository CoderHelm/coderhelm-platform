//! Follow-up tickets a run files from the ticket it worked on.
//!
//! An audit ticket's deliverable is often "the list, then tickets for the
//! fixes". The run writes the drafts (the plan's `followup_tickets`, or the
//! numbered list under a "Follow-up tickets" heading in proposal.md for a
//! plan written before that field), and CoderHelm files them itself, with the
//! team's own connection:
//!
//! - in the source ticket's Jira project, through the Coderhelm app's
//!   create-ticket trigger, each linked to the source ("relates to"),
//!   typed by the project and labelled `coderhelm`, `coderhelm:<repo>` and
//!   `coderhelm:followup:<source>` ([`labels`]);
//! - for a team with no Jira (a GitHub issue), as issues in the repository
//!   when the GitHub App may write issues;
//! - with neither, the drafts go into the ticket comment and the run
//!   completes. Filing never waits on a person.
//!
//! Each draft is filed once per source ticket ([`draft_id`]): the worker
//! keeps what it filed, and the app finds a ticket it already filed for the
//! same draft, so a retried run never files twice. One comment on the
//! source ticket lists what was filed ([`comment`]). A refusal is said as
//! exactly what was refused ([`refusal`]): a missing project permission by
//! its name, never "connect Jira".

use serde::{Deserialize, Serialize};

/// The tool's name, in the team's agent permissions and in the plan.
pub const TOOL: &str = "file_followup_tickets";

/// The longest summary filed (Jira takes 255 characters).
pub const MAX_SUMMARY: usize = 120;
/// The most drafts one run files.
pub const MAX_DRAFTS: usize = 20;

/// One follow-up ticket as the run drafted it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Draft {
    pub summary: String,
    #[serde(default)]
    pub description: String,
}

/// The id of a draft for one source ticket: stable across runs, so a retry
/// finds what an earlier run filed. Summaries are compared as the app
/// compares them (case, spacing and a trailing dot aside).
pub fn draft_id(source: &str, d: &Draft) -> String {
    let s = normal_summary(&d.summary);
    crate::content_hash(&format!("{}|{s}", source.trim().to_ascii_uppercase()))
        .chars()
        .take(24)
        .collect()
}

/// A summary as two drafts are compared.
pub fn normal_summary(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
        .trim_end_matches(['.', ' '])
        .to_string()
}

/// The Jira project of a ticket key (`ABC` of `ABC-12`).
pub fn project_of(key: &str) -> Option<&str> {
    let (p, n) = key.trim().split_once('-')?;
    let ok = !p.is_empty()
        && p.chars().next().is_some_and(|c| c.is_ascii_uppercase())
        && p.chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        && !n.is_empty()
        && n.chars().all(|c| c.is_ascii_digit());
    ok.then_some(p)
}

/// The labels a follow-up carries: CoderHelm's usual two, and the source's.
pub fn labels(repo: &str, source: &str) -> Vec<String> {
    let mut v = vec!["coderhelm".to_string()];
    if !repo.trim().is_empty() {
        v.push(format!("coderhelm:{}", repo.trim()));
    }
    v.push(format!("coderhelm:followup:{}", source.trim()));
    v
}

fn strip_marks(s: &str) -> String {
    s.replace("**", "").replace('`', "")
}

/// The first sentence of an item, plain, at most [`MAX_SUMMARY`] characters
/// (cut at a word).
pub fn summary_of(item: &str) -> String {
    let text = item.split_whitespace().collect::<Vec<_>>().join(" ");
    // A bold lead ("**Title**: the rest") is the title.
    if let Some(rest) = text.strip_prefix("**") {
        if let Some((title, _)) = rest.split_once("**") {
            let t = title.trim().trim_end_matches(':').trim();
            if !t.is_empty() {
                return clip(&strip_marks(t));
            }
        }
    }
    let plain = strip_marks(&text);
    let first = match plain.find(". ") {
        Some(i) => &plain[..i],
        None => plain.trim_end_matches('.'),
    };
    clip(first.trim())
}

fn clip(s: &str) -> String {
    if s.chars().count() <= MAX_SUMMARY {
        return s.to_string();
    }
    let cut: String = s.chars().take(MAX_SUMMARY).collect();
    match cut.rfind(' ') {
        Some(i) if i > MAX_SUMMARY / 2 => format!("{}...", &cut[..i]),
        _ => format!("{cut}..."),
    }
}

/// Whether a heading names the follow-up tickets to file.
fn is_followup_heading(line: &str) -> bool {
    let l = line.trim_start_matches('#').trim().to_lowercase();
    let l = l
        .replace("follow-up", "followup")
        .replace("follow up", "followup");
    (l.contains("followup") && (l.contains("ticket") || l.contains("issue")))
        || l.contains("tickets to file")
        || l.contains("issues to file")
}

/// A list item's text, when `line` starts one (`1. `, `1) `, `- `, `* `).
fn item_start(line: &str) -> Option<&str> {
    let t = line.trim_start();
    if let Some(rest) = t.strip_prefix("- ").or_else(|| t.strip_prefix("* ")) {
        return Some(rest);
    }
    let digits = t.chars().take_while(|c| c.is_ascii_digit()).count();
    if digits == 0 {
        return None;
    }
    let rest = &t[digits..];
    rest.strip_prefix(". ").or_else(|| rest.strip_prefix(") "))
}

/// The drafts under proposal.md's follow-up heading: each list item (with
/// its indented continuation lines) is one draft. Empty without the heading.
pub fn from_proposal(proposal: &str) -> Vec<Draft> {
    let mut out: Vec<String> = Vec::new();
    let mut inside = false;
    for line in proposal.lines() {
        if line.trim_start().starts_with('#') {
            if inside && !out.is_empty() {
                break;
            }
            inside = is_followup_heading(line);
            continue;
        }
        if !inside {
            continue;
        }
        if let Some(text) = item_start(line).filter(|_| {
            // A nested item belongs to the item above it.
            line.len() - line.trim_start().len() < 2 || out.is_empty()
        }) {
            out.push(text.trim().to_string());
        } else if let Some(last) = out.last_mut() {
            if !line.trim().is_empty() {
                last.push('\n');
                last.push_str(line.trim());
            }
        }
    }
    out.into_iter()
        .filter(|t| !t.trim().is_empty())
        .take(MAX_DRAFTS)
        .map(|t| Draft {
            summary: summary_of(&t),
            description: t,
        })
        .filter(|d| !d.summary.is_empty())
        .collect()
}

/// The drafts a run files: the plan's own list, else proposal.md's.
pub fn drafts(planned: &[Draft], proposal: &str) -> Vec<Draft> {
    let mut v: Vec<Draft> = planned
        .iter()
        .filter(|d| !d.summary.trim().is_empty())
        .take(MAX_DRAFTS)
        .map(|d| Draft {
            summary: clip(&strip_marks(d.summary.trim())),
            description: d.description.trim().to_string(),
        })
        .collect();
    if v.is_empty() {
        v = from_proposal(proposal);
    }
    // One of each: two items with the same summary are one ticket.
    let mut seen = std::collections::HashSet::new();
    v.retain(|d| seen.insert(normal_summary(&d.summary)));
    v
}

/// The description a filed ticket carries: the draft, and where it came from.
pub fn description(d: &Draft, source: &str, source_title: &str, run_url: &str) -> String {
    let mut s = d.description.trim().to_string();
    if s.is_empty() {
        s = d.summary.trim().to_string();
    }
    let from = if source_title.trim().is_empty() {
        source.to_string()
    } else {
        format!("{source} ({})", source_title.trim())
    };
    s.push_str(&format!(
        "\n\nA follow-up of {from}, filed by CoderHelm from that ticket's findings."
    ));
    if !run_url.trim().is_empty() {
        s.push_str(&format!(" The run: {}", run_url.trim()));
    }
    s
}

/// Where the follow-ups go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tracker {
    /// The source ticket's Jira project, through the Coderhelm app.
    Jira { project: String, site: String },
    /// Issues in the repository, through the GitHub App.
    Github { repo: String },
    /// Nowhere CoderHelm may file: the drafts go in the comment, and why.
    Comment(String),
}

/// Where a run files: Jira when the source is a Jira ticket and the app's
/// create-ticket trigger is registered; else GitHub issues when the App may
/// write issues (`issues_write`: `None` when unknown, tried then); else the
/// comment, with why.
pub fn tracker(
    source: &str,
    jira_create: bool,
    site: &str,
    repo: &str,
    issues_write: Option<bool>,
) -> Tracker {
    if jira_create {
        if let Some(p) = project_of(source) {
            return Tracker::Jira {
                project: p.to_string(),
                site: site.trim_end_matches('/').to_string(),
            };
        }
    }
    if issues_write != Some(false) && !repo.trim().is_empty() {
        return Tracker::Github {
            repo: repo.trim().to_string(),
        };
    }
    let why = if repo.trim().is_empty() {
        "this team has no Jira project and no repository CoderHelm files issues in".to_string()
    } else {
        format!(
            "this team has no Jira project to file in, and CoderHelm's GitHub App can't open \
             issues in {repo} (the installation doesn't grant Issues: write)"
        )
    };
    Tracker::Comment(why)
}

/// Whether the link to the source was made.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Linked {
    Yes,
    No(String),
}

/// What became of one draft.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Filed {
    /// Filed now, or found filed by an earlier run (`existing`).
    Ticket {
        key: String,
        url: String,
        existing: bool,
        linked: Linked,
    },
    /// Not filed, and exactly why.
    NotFiled(String),
}

impl Filed {
    pub fn key(&self) -> Option<&str> {
        match self {
            Filed::Ticket { key, .. } => Some(key),
            Filed::NotFiled(_) => None,
        }
    }
}

/// The words for an app's refusal of a follow-up: the status and its JSON
/// answer (`error`, `missing`, `words`, `details`). A missing project
/// permission is named with where a Jira admin grants it; nothing here ever
/// says Jira isn't connected.
pub fn refusal(status: u16, body: &serde_json::Value, site: &str, project: &str) -> String {
    let on = if site.trim().is_empty() {
        "the Jira site".to_string()
    } else {
        site.trim_end_matches('/').to_string()
    };
    let error = body["error"].as_str().unwrap_or("");
    let details = body["details"]
        .as_str()
        .map(|d| d.split_whitespace().collect::<Vec<_>>().join(" "))
        .unwrap_or_default();
    let details = crate::truncate_str(&details, 300);
    match error {
        "missing_permission" => {
            let words = body["words"]
                .as_str()
                .filter(|w| !w.is_empty())
                .unwrap_or("Create Issues");
            format!(
                "the Coderhelm app on {on} lacks the Jira project permission {words} in \
                 {project}. A Jira admin grants it in {project}'s project settings, Permissions, \
                 to the atlassian-addons-project-access role (the role Jira gives its apps)"
            )
        }
        "unauthorized" => format!(
            "the Coderhelm app on {on} didn't accept CoderHelm's shared secret; the app \
             registers again by itself on its next event"
        ),
        "no_issue_type" => format!(
            "{project} has no issue type a ticket can be filed with (only sub-tasks or epics)"
        ),
        "permissions_unreadable" | "issue_types_unreadable" | "create_failed" => {
            let what = match error {
                "permissions_unreadable" => "read its permissions in",
                "issue_types_unreadable" => "read the issue types of",
                _ => "create the ticket in",
            };
            if details.is_empty() {
                format!("the Coderhelm app on {on} couldn't {what} {project} (HTTP {status})")
            } else {
                format!(
                    "the Coderhelm app on {on} couldn't {what} {project} (HTTP {status}): {details}"
                )
            }
        }
        "" if details.is_empty() => format!("the Jira app answered HTTP {status}"),
        e => format!(
            "the Jira app answered HTTP {status}: {}",
            if details.is_empty() { e } else { details }
        ),
    }
}

/// The one comment on the source ticket: the tickets filed (or found filed),
/// what wasn't filed and why, and the drafts themselves when none could be
/// filed. Paragraphs, one per ticket, for Jira's plain comment layout.
pub fn comment(drafts: &[Draft], filed: &[Filed], tracker: &Tracker) -> String {
    let made: Vec<(&Draft, &Filed)> = drafts
        .iter()
        .zip(filed.iter())
        .filter(|(_, f)| f.key().is_some())
        .collect();
    let mut parts: Vec<String> = Vec::new();
    if !made.is_empty() {
        let n = made.len();
        parts.push(format!(
            "Filed {} follow-up {} from this ticket's findings:",
            n,
            if n == 1 { "ticket" } else { "tickets" }
        ));
        for (d, f) in &made {
            if let Filed::Ticket {
                key,
                url,
                existing,
                linked,
            } = f
            {
                let mut line = format!("{key}: {}", d.summary.trim());
                if !url.is_empty() {
                    line.push_str(&format!(" ({url})"));
                }
                if *existing {
                    line.push_str(" [filed earlier]");
                }
                if let Linked::No(why) = linked {
                    line.push_str(&format!(" [not linked to this ticket: {why}]"));
                }
                parts.push(line);
            }
        }
    }
    let missed: Vec<(&Draft, &str)> = drafts
        .iter()
        .zip(filed.iter())
        .filter_map(|(d, f)| match f {
            Filed::NotFiled(why) => Some((d, why.as_str())),
            Filed::Ticket { .. } => None,
        })
        .collect();
    let unfiled_drafts: Vec<&Draft> = match tracker {
        Tracker::Comment(_) => drafts.iter().collect(),
        _ => missed.iter().map(|(d, _)| *d).collect(),
    };
    if let Tracker::Comment(why) = tracker {
        parts.push(format!(
            "The follow-up tickets, as drafted. They weren't filed because {why}:"
        ));
    } else if !missed.is_empty() {
        let mut whys: Vec<&str> = missed.iter().map(|(_, w)| *w).collect();
        whys.dedup();
        parts.push(format!(
            "Not filed, because {}. The drafts:",
            whys.join("; ")
        ));
    }
    for (i, d) in unfiled_drafts.iter().enumerate() {
        let body = d
            .description
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if body.is_empty() || normal_summary(&body) == normal_summary(&d.summary) {
            parts.push(format!("{}. {}", i + 1, d.summary.trim()));
        } else {
            parts.push(format!("{}. {}: {body}", i + 1, d.summary.trim()));
        }
    }
    parts.join("\n\n")
}

/// Whether a step of the plan is the filing of follow-up tickets: it names
/// the tool, or its words say to file (create, open, raise) the follow-up
/// tickets or issues.
pub fn is_filing_step(tool: &str, text: &str) -> bool {
    if tool.trim() == TOOL {
        return true;
    }
    let t = text
        .to_lowercase()
        .replace("follow-up", "followup")
        .replace("follow up", "followup");
    let acts = ["file", "create", "open", "raise", "log "]
        .iter()
        .any(|v| t.contains(v));
    acts && t.contains("followup") && (t.contains("ticket") || t.contains("issue"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROPOSAL: &str = "# Audit\n\n## Answer\n1. not a draft\n\n## Follow-up tickets to file (additive only)\n1. Record in the ticket that `Hero.tsx` and `ContactCard.tsx` are not rendered. There is no code change.\n2. Add alt-required validation to `imageWithAlt` (when an asset is set). Then run typegen.\n3. **Coach images**: require alt, and fill the 3 missing ones.\n   - with setIfMissing\n\n## Scope\nAn audit only.\n";

    #[test]
    fn the_drafts_are_the_list_under_the_follow_up_heading() {
        let d = from_proposal(PROPOSAL);
        assert_eq!(d.len(), 3, "{d:?}");
        assert_eq!(
            d[0].summary,
            "Record in the ticket that Hero.tsx and ContactCard.tsx are not rendered"
        );
        assert_eq!(
            d[1].summary,
            "Add alt-required validation to imageWithAlt (when an asset is set)"
        );
        assert_eq!(d[2].summary, "Coach images");
        assert!(d[2].description.contains("with setIfMissing"));
        assert!(from_proposal("# Plan\n1. a step\n").is_empty());
    }

    #[test]
    fn the_plan_s_own_drafts_win_and_repeats_are_one() {
        let planned = vec![
            Draft {
                summary: "Fix the alt".into(),
                description: "x".into(),
            },
            Draft {
                summary: "fix the  alt.".into(),
                description: "y".into(),
            },
        ];
        let d = drafts(&planned, PROPOSAL);
        assert_eq!(d.len(), 1);
        assert_eq!(drafts(&[], PROPOSAL).len(), 3);
    }

    #[test]
    fn a_draft_keeps_its_id_across_runs() {
        let a = Draft {
            summary: "Fix the alt.".into(),
            ..Default::default()
        };
        let b = Draft {
            summary: "fix  the alt".into(),
            description: "other words".into(),
        };
        assert_eq!(draft_id("ABC-1", &a), draft_id("abc-1", &b));
        assert_ne!(draft_id("ABC-1", &a), draft_id("ABC-2", &a));
    }

    #[test]
    fn the_tracker_is_the_source_project_then_issues_then_the_comment() {
        assert_eq!(
            tracker(
                "ABC-12",
                true,
                "https://x.atlassian.net/",
                "o/r",
                Some(true)
            ),
            Tracker::Jira {
                project: "ABC".into(),
                site: "https://x.atlassian.net".into()
            }
        );
        assert_eq!(
            tracker("#12", false, "", "o/r", None),
            Tracker::Github { repo: "o/r".into() }
        );
        let Tracker::Comment(why) = tracker("#12", false, "", "o/r", Some(false)) else {
            panic!("no way to file");
        };
        assert!(why.contains("Issues: write"), "{why}");
        assert!(!why.to_lowercase().contains("connect"), "{why}");
    }

    #[test]
    fn a_missing_permission_is_named_never_connect_jira() {
        let body = serde_json::json!({"error": "missing_permission", "missing": ["CREATE_ISSUES"], "words": "Create Issues"});
        let w = refusal(403, &body, "https://x.atlassian.net", "ABC");
        assert!(
            w.contains("lacks the Jira project permission Create Issues in ABC"),
            "{w}"
        );
        assert!(!w.to_lowercase().contains("connect jira"), "{w}");
        let w = refusal(
            400,
            &serde_json::json!({"error": "create_failed", "details": "Field 'x' is required"}),
            "",
            "ABC",
        );
        assert!(w.contains("Field 'x' is required"), "{w}");
    }

    #[test]
    fn the_comment_lists_the_new_keys_and_the_drafts_not_filed() {
        let d = from_proposal(PROPOSAL);
        let filed = vec![
            Filed::Ticket {
                key: "ABC-20".into(),
                url: "https://x.atlassian.net/browse/ABC-20".into(),
                existing: false,
                linked: Linked::Yes,
            },
            Filed::Ticket {
                key: "ABC-21".into(),
                url: String::new(),
                existing: true,
                linked: Linked::Yes,
            },
            Filed::NotFiled("the Jira app answered HTTP 500".into()),
        ];
        let t = Tracker::Jira {
            project: "ABC".into(),
            site: String::new(),
        };
        let c = comment(&d, &filed, &t);
        assert!(c.starts_with("Filed 2 follow-up tickets"), "{c}");
        assert!(c.contains("ABC-20: Record in the ticket"), "{c}");
        assert!(c.contains("ABC-21: Add alt-required validation to imageWithAlt (when an asset is set) [filed earlier]"), "{c}");
        assert!(
            c.contains("Not filed, because the Jira app answered HTTP 500"),
            "{c}"
        );
        assert!(c.contains("1. Coach images"), "{c}");
        let none = comment(&d, &[], &Tracker::Comment("no tracker".into()));
        assert!(
            none.contains("They weren't filed because no tracker"),
            "{none}"
        );
        assert!(none.contains("3. Coach images"), "{none}");
    }

    #[test]
    fn filing_steps_are_known_by_tool_or_words() {
        assert!(is_filing_step(TOOL, ""));
        assert!(is_filing_step("", "File the 8 follow-up tickets"));
        assert!(is_filing_step(
            "",
            "Follow-up tickets filed? create them in Jira"
        ));
        assert!(!is_filing_step("", "Post the audit table on the ticket"));
        assert!(!is_filing_step("", "Follow-up: open the settings page"));
    }

    #[test]
    fn project_and_labels() {
        assert_eq!(project_of("ABC-12"), Some("ABC"));
        assert_eq!(project_of("A1_B-3"), Some("A1_B"));
        assert_eq!(project_of("#12"), None);
        assert_eq!(project_of("abc-1"), None);
        assert_eq!(
            labels("o/r", "ABC-12"),
            vec!["coderhelm", "coderhelm:o/r", "coderhelm:followup:ABC-12"]
        );
    }
}
