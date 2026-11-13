//! The repository's guide file, kept up by CoderHelm through pull requests.
//!
//! Every run reads the repository's guide (`AGENTS.md`, `CLAUDE.md` and the
//! others the worker's `INSTRUCTION_FILES` lists). When lessons CoderHelm
//! learned in a repository reach the confidence for it
//! (`rewards::lessons::guide_ready`), CoderHelm proposes them for that file
//! as a small pull request of its own ([`with_lessons`], [`pr_body`]), so
//! the team reviews what it learned like any other change. It never writes
//! to the default branch: the change is one commit on a new branch
//! ([`branch`]) and a pull request from it.
//!
//! It is rate limited by one record per repository ([`Record`],
//! `GUIDEPR#<owner/name>`): at most one guide pull request open at a time,
//! [`MIN_DAYS_BETWEEN`] after one merged, [`DECLINED_PAUSE_DAYS`] after one
//! the team closed without merging ([`may_propose`]). The record is claimed
//! before anything is written to GitHub, so two workers never open two.
//!
//! When the pull request closes ([`settle`]): lessons still in the merged
//! file are marked as in the guide (the run reads them there from then on
//! and they leave the prompt); lessons people took out before merging, and
//! every lesson of a pull request closed without merging, are declined:
//! never proposed again, and counted as contradicted once.

use crate::rewards::lessons::{self, Lesson};
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

/// The guide files a lessons section may go in, first existing wins; with
/// none, [`DEFAULT_FILE`] is created.
pub const GUIDE_FILES: &[&str] = &[
    "AGENTS.md",
    ".github/AGENTS.md",
    "CLAUDE.md",
    ".claude/CLAUDE.md",
];
pub const DEFAULT_FILE: &str = "AGENTS.md";

pub const SECTION_HEADING: &str = "## Lessons from earlier pull requests";
pub const SECTION_START: &str = "<!-- coderhelm:lessons -->";
pub const SECTION_END: &str = "<!-- /coderhelm:lessons -->";
/// The head branch of every guide pull request starts with this.
pub const BRANCH_PREFIX: &str = "coderhelm/guide-lessons-";
pub const RECORD_PREFIX: &str = "GUIDEPR#";

/// Most lessons proposed in one pull request: small enough to review.
pub const MAX_LESSONS_PER_PR: usize = 6;
/// Days after a merged guide pull request before the next one.
pub const MIN_DAYS_BETWEEN: i64 = 14;
/// Days after one the team closed without merging.
pub const DECLINED_PAUSE_DAYS: i64 = 60;
/// A claim whose pull request was never recorded (the worker stopped) is
/// free again after this.
pub const CLAIM_MINUTES: i64 = 60;
/// A proposal that failed at GitHub is tried again after this.
pub const RETRY_HOURS: i64 = 24;
/// Word overlap at which a guide line already says a lesson.
pub const SAID_AT: f64 = 0.6;
/// The guide file is never grown past this by a proposal.
pub const MAX_GUIDE_BYTES: usize = 64_000;

pub const STATE_OPENING: &str = "opening";
pub const STATE_OPEN: &str = "open";
pub const STATE_MERGED: &str = "merged";
pub const STATE_DECLINED: &str = "declined";
pub const STATE_FAILED: &str = "failed";

pub fn record_sk(repo: &str) -> String {
    format!("{RECORD_PREFIX}{repo}")
}

/// Whether `head` is a guide pull request's branch. This only keeps guide
/// pull requests out of what is counted and scored as code work; which pull
/// request is the repository's guide pull request is the record's say.
pub fn is_guide_branch(head: &str) -> bool {
    head.starts_with(BRANCH_PREFIX)
}

/// The branch of a proposal made at `now`.
pub fn branch(now: chrono::DateTime<chrono::Utc>) -> String {
    format!("{BRANCH_PREFIX}{}", now.format("%Y%m%d-%H%M"))
}

/// The repository's guide pull request, as last recorded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Record {
    pub repo: String,
    pub state: String,
    pub pr: u64,
    pub branch: String,
    /// The guide file the lessons went in.
    pub path: String,
    pub lesson_ids: Vec<String>,
    /// When the state was last set.
    pub at: String,
    /// Guide pull requests the team closed without merging, in a row.
    pub declined_in_a_row: u64,
}

/// Why no guide pull request is proposed now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hold {
    /// One is open (or being opened): at most one per repository.
    Open(u64),
    /// The last one merged or was declined too recently; free again at
    /// this time.
    Until(String),
}

/// Pure: may a guide pull request be proposed for the repository now?
pub fn may_propose(r: Option<&Record>, now: chrono::DateTime<chrono::Utc>) -> Result<(), Hold> {
    let Some(r) = r else {
        return Ok(());
    };
    let at = chrono::DateTime::parse_from_rfc3339(&r.at)
        .map(|t| t.with_timezone(&chrono::Utc))
        .unwrap_or(now);
    let wait = match r.state.as_str() {
        STATE_OPEN => return Err(Hold::Open(r.pr)),
        STATE_OPENING => chrono::Duration::minutes(CLAIM_MINUTES),
        STATE_MERGED => chrono::Duration::days(MIN_DAYS_BETWEEN),
        // Each decline in a row doubles the pause: a team that keeps
        // closing them is asked less and less.
        STATE_DECLINED => chrono::Duration::days(
            DECLINED_PAUSE_DAYS * (1i64 << r.declined_in_a_row.clamp(1, 4).saturating_sub(1)),
        ),
        STATE_FAILED => chrono::Duration::hours(RETRY_HOURS),
        _ => return Ok(()),
    };
    let free = at + wait;
    if now >= free {
        Ok(())
    } else if r.state == STATE_OPENING {
        Err(Hold::Open(0))
    } else {
        Err(Hold::Until(free.to_rfc3339()))
    }
}

/// A lesson as one line of the guide's section.
pub fn lesson_line(l: &Lesson) -> String {
    let one_line = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
    let scope = if l.applies_to.trim().is_empty() {
        String::new()
    } else {
        format!("In `{}`: ", l.applies_to.trim().trim_matches('`'))
    };
    let check = if l.check.trim().is_empty() {
        String::new()
    } else {
        format!(" Check: {}", one_line(&l.check))
    };
    format!("- {scope}{}{check}", one_line(&l.text))
}

/// The lines of the guide's lessons section (without the leading `- `).
pub fn section_lines(content: &str) -> Vec<String> {
    let Some(start) = content.find(SECTION_START) else {
        return vec![];
    };
    let rest = &content[start + SECTION_START.len()..];
    let section = &rest[..rest.find(SECTION_END).unwrap_or(rest.len())];
    section
        .lines()
        .map(str::trim)
        .filter_map(|l| l.strip_prefix("- ").or_else(|| l.strip_prefix("* ")))
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

/// Whether the guide already says the lesson: in its lessons section, or
/// in any other line of the file people wrote themselves.
pub fn already_said(content: &str, l: &Lesson) -> bool {
    content
        .lines()
        .map(|line| line.trim().trim_start_matches(['-', '*', ' ']))
        .filter(|line| line.len() >= 20)
        .any(|line| lessons::similarity(line, &l.text) >= SAID_AT)
}

/// Pure: the lessons to propose now: ready for the guide, not said in it
/// already, heaviest first, at most [`MAX_LESSONS_PER_PR`].
pub fn pick(kept: &[Lesson], guide: &str, now: chrono::DateTime<chrono::Utc>) -> Vec<Lesson> {
    let mut ready: Vec<(f64, &Lesson)> = kept
        .iter()
        .filter(|l| lessons::guide_ready(l, now) && !already_said(guide, l))
        .map(|l| (lessons::weight_of(l, now), l))
        .collect();
    ready.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.id.cmp(&b.1.id))
    });
    ready
        .into_iter()
        .take(MAX_LESSONS_PER_PR)
        .map(|(_, l)| l.clone())
        .collect()
}

/// Pure: the guide file with the lessons added to its section (made at the
/// end of the file when there is none). Everything people wrote stays as it
/// is. None when the result would pass [`MAX_GUIDE_BYTES`].
pub fn with_lessons(content: &str, add: &[Lesson]) -> Option<String> {
    let lines: Vec<String> = add.iter().map(lesson_line).collect();
    if lines.is_empty() {
        return None;
    }
    let block = lines.join("\n");
    let out = match (content.find(SECTION_START), content.find(SECTION_END)) {
        (Some(a), Some(b)) if a < b => {
            let head = content[..b].trim_end();
            format!("{head}\n{block}\n{}", &content[b..])
        }
        _ => {
            let head = content.trim_end();
            let gap = if head.is_empty() { "" } else { "\n\n" };
            let title = if head.is_empty() {
                "# Working on this repository\n\n"
            } else {
                ""
            };
            format!(
                "{head}{gap}{title}{SECTION_HEADING}\n\nWhat CoderHelm learned from what happened to its pull requests here. Edit or remove any line; CoderHelm follows the file as it is.\n\n{SECTION_START}\n{block}\n{SECTION_END}\n"
            )
        }
    };
    (out.len() <= MAX_GUIDE_BYTES).then_some(out)
}

pub fn pr_title(n: usize) -> String {
    if n == 1 {
        "Guide: one lesson from earlier pull requests".to_string()
    } else {
        format!("Guide: {n} lessons from earlier pull requests")
    }
}

/// The pull request's description: each lesson with the pull requests and
/// the evidence behind it.
pub fn pr_body(path: &str, repo: &str, add: &[Lesson]) -> String {
    let mut out = format!(
        "This adds what I learned from earlier pull requests in this repository to `{path}`, \
         which I read at the start of every run. Each lesson is below with what it came from. \
         Edit the wording, remove a line you disagree with, or close this without merging: I \
         follow the file as merged, drop what you take out, and do not propose a declined lesson \
         again.\n"
    );
    for l in add {
        out.push_str(&format!(
            "\n### {}\n",
            crate::truncate_str(l.text.trim(), 200)
        ));
        if !l.check.trim().is_empty() {
            out.push_str(&format!("Check: {}\n", l.check.trim()));
        }
        let prs: Vec<String> = l
            .prs
            .iter()
            .filter_map(|p| p.strip_prefix(&format!("{repo}#")))
            .map(|n| format!("#{n}"))
            .collect();
        if !prs.is_empty() {
            out.push_str(&format!("\nFrom {}.\n", prs.join(", ")));
        }
        for e in l.evidence.iter().take(4) {
            out.push_str(&format!("- {}\n", crate::truncate_str(e.trim(), 300)));
        }
    }
    out
}

/// What became of the lessons a guide pull request proposed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Settled {
    /// Still in the merged file.
    pub kept: Vec<String>,
    /// Taken out before merging, or the pull request was closed.
    pub declined: Vec<String>,
}

/// Pure: sorts a closed guide pull request's lessons. `merged_guide` is the
/// guide file on the default branch after the merge; None when the pull
/// request was closed without merging.
pub fn settle(proposed: &[Lesson], merged_guide: Option<&str>) -> Settled {
    let mut out = Settled::default();
    let lines = merged_guide.map(section_lines).unwrap_or_default();
    for l in proposed {
        let line = lesson_line(l);
        let line = line.trim_start_matches("- ");
        // People may reword a line: it is still the lesson while most of
        // its words are there.
        let still = lines
            .iter()
            .any(|kept| kept == line || lessons::similarity(kept, line) >= SAID_AT);
        if merged_guide.is_some() && still {
            out.kept.push(l.id.clone());
        } else {
            out.declined.push(l.id.clone());
        }
    }
    out
}

// ── The record ───────────────────────────────────────────────────────

type Item = HashMap<String, AttributeValue>;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn gs(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn gu(i: &Item, k: &str) -> u64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

impl Record {
    pub fn to_item(&self, team: &str) -> Item {
        Item::from([
            ("pk".to_string(), s(team)),
            ("sk".to_string(), s(&record_sk(&self.repo))),
            ("record_type".to_string(), s("guide_pr")),
            ("repo".to_string(), s(&self.repo)),
            ("state".to_string(), s(&self.state)),
            ("pr".to_string(), AttributeValue::N(self.pr.to_string())),
            ("branch".to_string(), s(&self.branch)),
            ("path".to_string(), s(&self.path)),
            (
                "lesson_ids".to_string(),
                AttributeValue::L(self.lesson_ids.iter().map(|l| s(l)).collect()),
            ),
            ("at".to_string(), s(&self.at)),
            (
                "declined_in_a_row".to_string(),
                AttributeValue::N(self.declined_in_a_row.to_string()),
            ),
        ])
    }

    pub fn from_item(i: &Item) -> Option<Record> {
        if !gs(i, "sk").starts_with(RECORD_PREFIX) {
            return None;
        }
        Some(Record {
            repo: gs(i, "repo"),
            state: gs(i, "state"),
            pr: gu(i, "pr"),
            branch: gs(i, "branch"),
            path: gs(i, "path"),
            lesson_ids: i
                .get("lesson_ids")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
                .unwrap_or_default(),
            at: gs(i, "at"),
            declined_in_a_row: gu(i, "declined_in_a_row"),
        })
    }
}

pub async fn load(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team: &str,
    repo: &str,
) -> Result<Option<Record>, String> {
    dynamo
        .get_item()
        .table_name(table)
        .key("pk", s(team))
        .key("sk", s(&record_sk(repo)))
        .consistent_read(true)
        .send()
        .await
        .map(|r| r.item.as_ref().and_then(Record::from_item))
        .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())
}

/// Writes `next` only when the stored record is still `prev` (by its state
/// and time; None: there is none). False when another worker changed it
/// first: then this one does nothing.
pub async fn swap(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team: &str,
    prev: Option<&Record>,
    next: &Record,
) -> Result<bool, String> {
    let mut put = dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(next.to_item(team)));
    put = match prev {
        None => put.condition_expression("attribute_not_exists(sk)"),
        Some(p) => put
            .condition_expression("#st = :st AND #at = :at")
            .expression_attribute_names("#st", "state")
            .expression_attribute_names("#at", "at")
            .expression_attribute_values(":st", s(&p.state))
            .expression_attribute_values(":at", s(&p.at)),
    };
    match put.send().await {
        Ok(_) => Ok(true),
        Err(e) => {
            let msg = aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string();
            if msg.contains("ConditionalCheckFailed") {
                Ok(false)
            } else {
                Err(msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc
            .with_ymd_and_hms(2026, 10, 20, 9, 30, 0)
            .unwrap()
    }

    fn lesson(id: &str, text: &str, check: &str, prs: &[&str]) -> Lesson {
        Lesson {
            id: id.into(),
            repo: "acme/web".into(),
            agent: "coder".into(),
            text: text.into(),
            check: check.into(),
            seen: prs.len() as u64,
            prs: prs.iter().map(|p| p.to_string()).collect(),
            created_at: "2026-10-01T00:00:00Z".into(),
            last_seen_at: "2026-10-19T00:00:00Z".into(),
            evidence: vec![
                "PR #12: people changed `apps/web/src/api.ts` (\"Use the shared client\")".into(),
            ],
            ..Default::default()
        }
    }

    fn hook() -> Lesson {
        lesson(
            "L1",
            "Fetch through the shared `useApi` hook, never with a bare `fetch` call.",
            "No `fetch(` in the diff under `apps/web/src`.",
            &["acme/web#12", "acme/web#15"],
        )
    }

    fn migrations() -> Lesson {
        let mut l = lesson(
            "L2",
            "Name migration files `NNNN_what.sql`.",
            "The new file starts with four digits.",
            &["acme/web#3", "acme/web#5", "acme/web#8"],
        );
        l.applies_to = "db/migrations".into();
        l
    }

    fn record(state: &str, at: &str) -> Record {
        Record {
            repo: "acme/web".into(),
            state: state.into(),
            pr: 40,
            at: at.into(),
            ..Default::default()
        }
    }

    #[test]
    fn at_most_one_guide_pull_request_is_open_per_repository() {
        assert_eq!(may_propose(None, now()), Ok(()));
        assert_eq!(
            may_propose(Some(&record(STATE_OPEN, "2026-01-01T00:00:00Z")), now()),
            Err(Hold::Open(40)),
            "an open one holds however old it is"
        );
        // Being opened by another worker right now.
        assert_eq!(
            may_propose(Some(&record(STATE_OPENING, "2026-10-20T09:00:00Z")), now()),
            Err(Hold::Open(0))
        );
        // A claim nobody finished is free again after an hour.
        assert_eq!(
            may_propose(Some(&record(STATE_OPENING, "2026-10-20T08:00:00Z")), now()),
            Ok(())
        );
    }

    #[test]
    fn a_merged_or_declined_one_is_followed_by_a_pause() {
        assert!(matches!(
            may_propose(Some(&record(STATE_MERGED, "2026-10-10T00:00:00Z")), now()),
            Err(Hold::Until(_))
        ));
        assert_eq!(
            may_propose(Some(&record(STATE_MERGED, "2026-10-01T00:00:00Z")), now()),
            Ok(())
        );
        let mut declined = record(STATE_DECLINED, "2026-09-01T00:00:00Z");
        declined.declined_in_a_row = 1;
        assert!(matches!(
            may_propose(Some(&declined), now()),
            Err(Hold::Until(_))
        ));
        declined.at = "2026-08-01T00:00:00Z".into();
        assert_eq!(may_propose(Some(&declined), now()), Ok(()));
        // Declined twice in a row: twice the pause.
        declined.declined_in_a_row = 2;
        assert!(matches!(
            may_propose(Some(&declined), now()),
            Err(Hold::Until(_))
        ));
        // A failure at GitHub is retried the next day.
        assert!(may_propose(Some(&record(STATE_FAILED, "2026-10-20T00:00:00Z")), now()).is_err());
        assert_eq!(
            may_propose(Some(&record(STATE_FAILED, "2026-10-19T00:00:00Z")), now()),
            Ok(())
        );
    }

    #[test]
    fn the_section_is_added_once_and_peoples_text_is_left_alone() {
        let before = "# Working on shop\n\nRun `make test` before a commit.\n";
        let after = with_lessons(before, &[hook()]).unwrap();
        assert!(after.starts_with(before.trim_end()));
        assert!(after.contains(SECTION_HEADING));
        assert_eq!(
            section_lines(&after),
            vec!["Fetch through the shared `useApi` hook, never with a bare `fetch` call. Check: No `fetch(` in the diff under `apps/web/src`."]
        );
        // A second proposal goes in the same section, after the first.
        let again = with_lessons(&after, &[migrations()]).unwrap();
        assert_eq!(again.matches(SECTION_START).count(), 1);
        assert_eq!(again.matches(SECTION_HEADING).count(), 1);
        let lines = section_lines(&again);
        assert_eq!(lines.len(), 2);
        assert!(lines[1].starts_with("In `db/migrations`: Name migration files"));
        assert!(again.trim_end().ends_with(SECTION_END));
        // No guide file yet: one is started.
        let fresh = with_lessons("", &[hook()]).unwrap();
        assert!(fresh.starts_with("# Working on this repository"));
        assert_eq!(with_lessons(before, &[]), None);
        // Never grown without bound.
        assert_eq!(with_lessons(&"x".repeat(MAX_GUIDE_BYTES), &[hook()]), None);
    }

    #[test]
    fn only_confident_lessons_the_guide_does_not_say_are_proposed() {
        let once = lesson(
            "L3",
            "Use `zod` for request bodies.",
            "Each handler parses with `zod`.",
            &["acme/web#9"],
        );
        let mut contradicted = migrations();
        contradicted.id = "L4".into();
        contradicted.text = "Keep feature flags in `flags.ts`.".into();
        contradicted.contradicted = 1;
        let kept = vec![hook(), once, contradicted, migrations()];
        let picked: Vec<String> = pick(&kept, "", now()).into_iter().map(|l| l.id).collect();
        assert_eq!(
            picked,
            vec!["L2", "L1"],
            "heaviest first, only the ready ones"
        );
        // People already wrote it in the guide themselves: not proposed.
        let guide = "## Conventions\n- Always fetch through the shared `useApi` hook; never a bare `fetch` call.\n";
        let picked: Vec<String> = pick(&kept, guide, now())
            .into_iter()
            .map(|l| l.id)
            .collect();
        assert_eq!(picked, vec!["L2"]);
        // At most a reviewable handful.
        let many: Vec<Lesson> = (0..10)
            .map(|i| {
                lesson(
                    &format!("M{i}"),
                    &format!("Rule number{i} about `thing{i}` in module{i}."),
                    "A check to run here.",
                    &["acme/web#1", "acme/web#2"],
                )
            })
            .collect();
        assert_eq!(pick(&many, "", now()).len(), MAX_LESSONS_PER_PR);
    }

    #[test]
    fn the_description_names_the_evidence_behind_each_lesson() {
        let body = pr_body("AGENTS.md", "acme/web", &[hook()]);
        assert!(body.contains("`AGENTS.md`"));
        assert!(body.contains("### Fetch through the shared `useApi` hook"));
        assert!(body.contains("From #12, #15."));
        assert!(body.contains("Use the shared client"));
        assert!(body.contains("close this without merging"));
        assert_eq!(pr_title(1), "Guide: one lesson from earlier pull requests");
        assert_eq!(pr_title(3), "Guide: 3 lessons from earlier pull requests");
        assert!(is_guide_branch(&branch(now())));
        assert_eq!(branch(now()), "coderhelm/guide-lessons-20261020-0930");
        assert!(!is_guide_branch("coderhelm/abc-1"));
    }

    #[test]
    fn a_closed_guide_pull_request_settles_each_lesson() {
        let proposed = vec![hook(), migrations()];
        // Closed without merging: the team declined them all.
        let s = settle(&proposed, None);
        assert_eq!(s.declined, vec!["L1", "L2"]);
        assert!(s.kept.is_empty());
        // Merged as proposed.
        let merged = with_lessons("# Guide\n", &proposed).unwrap();
        let s = settle(&proposed, Some(&merged));
        assert_eq!(s.kept, vec!["L1", "L2"]);
        // Merged after people reworded one line and removed the other.
        let edited = merged
            .replace(
                "never with a bare `fetch` call.",
                "not a bare `fetch` call.",
            )
            .lines()
            .filter(|l| !l.contains("migration"))
            .collect::<Vec<_>>()
            .join("\n");
        let s = settle(&proposed, Some(&edited));
        assert_eq!(s.kept, vec!["L1"]);
        assert_eq!(s.declined, vec!["L2"]);
        // Merged, but the section was removed from the file altogether.
        let s = settle(&proposed, Some("# Guide\n"));
        assert_eq!(s.declined, vec!["L1", "L2"]);
    }

    #[test]
    fn the_record_round_trips() {
        let r = Record {
            repo: "acme/web".into(),
            state: STATE_OPEN.into(),
            pr: 40,
            branch: branch(now()),
            path: "AGENTS.md".into(),
            lesson_ids: vec!["L1".into(), "L2".into()],
            at: now().to_rfc3339(),
            declined_in_a_row: 1,
        };
        let item = r.to_item("TEAM#a");
        assert_eq!(item["sk"], s("GUIDEPR#acme/web"));
        assert_eq!(Record::from_item(&item), Some(r));
        assert_eq!(Record::from_item(&Item::new()), None);
    }
}
