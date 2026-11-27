//! What CoderHelm itself decided about a run, read once from its record, for
//! both the run page (`api::get_run`) and Chat's runs lookup
//! (`chat_work_runs::record_with`).
//!
//! Chat was asked why auto-merge was held on a run. The run page said
//! CoderHelm's own review still asked for changes, with the open finding
//! named, but Chat's run record carried only the title, the step, the error
//! and the notes: it read the repository's settings instead, blamed GitHub's
//! branch rules, and the pull request was merged by hand past the finding.
//! The two surfaces read the run record each in their own way, so a fact
//! the page gained never reached Chat.
//!
//! Now there is one reader. [`of_item`] builds [`RunFacts`]; the run page
//! takes its fields from it (destructured whole, so a new fact that the page
//! doesn't show doesn't compile), and Chat renders it with
//! [`RunFacts::lines`] (destructured whole too, so a new fact without a Chat
//! line doesn't compile). The tests hold the rest: every field of the run
//! page is a fact here, a field Chat writes itself, or named as one Chat
//! doesn't need with the reason; and Chat's record shows every fact of a run
//! that has them all.

use serde::Serialize;
use serde_json::Value;

use super::api::{
    CodegenEnvMissing, RoundIncomplete, RunCoveredBy, RunDraftHold, RunMergeStanding, RunNoChange,
    RunPreview, RunToolingFailure, RunWaitingOn,
};
use super::chat_work::clip;
use super::run_search::Item;

/// The run record's field counting the findings open at the run's own last
/// review (written by the worker's own-review rounds).
pub const OWN_REVIEW_OPEN: &str = "own_review_open";
/// The run record's field counting the fix rounds taken on them.
pub const OWN_REVIEW_ROUNDS: &str = "own_review_rounds";
/// The run record's field holding the own review's findings, one by one
/// (the worker's `own_review_ledger::FIELD`).
pub const OWN_REVIEW_LEDGER: &str = "own_review_ledger";

/// CoderHelm's own review of the run's pull request: how many findings are
/// open and what each one is.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct RunOwnReview {
    /// Findings open at CoderHelm's last review of the head. While above
    /// zero, CoderHelm's review asks for changes and auto-merge holds.
    #[schema(example = 1)]
    pub open: u32,
    /// Fix rounds CoderHelm took on its own findings.
    #[schema(example = 2)]
    pub rounds: u32,
    /// The open findings, as the review listed them.
    #[schema(example = json!(["The retry limit is read from the wrong setting (jobs/export.ts:41)"]))]
    pub open_findings: Vec<String>,
    /// Findings CoderHelm's rounds left open after every approach they
    /// have: listed on the pull request as CoderHelm's note, never approved
    /// over.
    #[schema(example = json!([]))]
    pub noted: Vec<String>,
    /// Findings only a person can settle, with what they do.
    #[schema(example = json!([]))]
    pub for_a_person: Vec<String>,
}

/// Every decision CoderHelm recorded on a run, as the run page shows it.
#[derive(Default, Serialize)]
pub struct RunFacts {
    /// Why it stopped, in CoderHelm's words.
    pub error: Option<String>,
    /// How it stopped when it was its own environment, not a question.
    pub stop_kind: Option<String>,
    pub tooling_failure: Option<RunToolingFailure>,
    pub covered_by: Option<RunCoveredBy>,
    pub no_change: Option<RunNoChange>,
    /// What a `needs_input` run waits for from a person.
    pub waiting_on: Option<RunWaitingOn>,
    pub draft_hold: Option<RunDraftHold>,
    /// Where its pull request stands on the way to merge: what holds it.
    pub merge_standing: Option<RunMergeStanding>,
    pub waiting_line: Option<String>,
    pub preview: Option<RunPreview>,
    pub round_incomplete: Option<RoundIncomplete>,
    pub codegen_env_missing: Option<CodegenEnvMissing>,
    pub own_review: Option<RunOwnReview>,
}

fn s<'a>(item: &'a Item, k: &str) -> Option<&'a str> {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .filter(|v| !v.trim().is_empty())
}

fn n(item: &Item, k: &str) -> Option<u32> {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
}

fn json_of<T: serde::de::DeserializeOwned>(item: &Item, k: &str) -> Option<T> {
    s(item, k).and_then(|j| serde_json::from_str(j).ok())
}

/// The own review's state from the run record: `None` before it reviewed.
pub fn own_review_of(item: &Item) -> Option<RunOwnReview> {
    let ledger: Value = json_of(item, OWN_REVIEW_LEDGER).unwrap_or(Value::Null);
    let mut r = RunOwnReview {
        open: n(item, OWN_REVIEW_OPEN).unwrap_or(0),
        rounds: n(item, OWN_REVIEW_ROUNDS).unwrap_or(0),
        ..Default::default()
    };
    for f in ledger["findings"].as_array().into_iter().flatten() {
        let what = f["item"].as_str().unwrap_or("").trim().to_string();
        if what.is_empty() {
            continue;
        }
        let settled = &f["settled"];
        let word = |k: &str| settled[k].as_str().unwrap_or("").trim().to_string();
        match settled["kind"].as_str() {
            None => r.open_findings.push(what),
            Some("noted") => r.noted.push(match word("why") {
                w if w.is_empty() => what,
                w => format!("{what} ({w})"),
            }),
            Some("deferred") => r.for_a_person.push(match word("what") {
                w if w.is_empty() => what,
                w => format!("{what}: {w}"),
            }),
            // Fixed, or ruled out by the ticket: nothing holds on it.
            Some(_) => {}
        }
    }
    let seen = item.contains_key(OWN_REVIEW_OPEN)
        || item.contains_key(OWN_REVIEW_ROUNDS)
        || !r.open_findings.is_empty()
        || !r.noted.is_empty()
        || !r.for_a_person.is_empty();
    seen.then_some(r)
}

/// Every fact CoderHelm recorded on the run.
pub fn of_item(item: &Item) -> RunFacts {
    use super::api;
    let needs_input = s(item, "status") == Some(common::run_status::NEEDS_INPUT);
    RunFacts {
        error: s(item, "error_message")
            .or_else(|| s(item, "error"))
            .map(str::to_string),
        stop_kind: s(item, "stop_kind").map(str::to_string),
        tooling_failure: json_of(item, "tooling_failure"),
        covered_by: json_of(item, "covered_by"),
        no_change: json_of(item, "no_change"),
        waiting_on: needs_input.then(|| api::waiting_on_of(item)).flatten(),
        draft_hold: api::draft_hold_of(item),
        merge_standing: api::merge_standing_of(item),
        waiting_line: api::waiting_line_of(item),
        preview: api::preview_of(item),
        round_incomplete: api::round_incomplete_of(item),
        codegen_env_missing: api::codegen_env_missing_of(item),
        own_review: own_review_of(item),
    }
}

fn list(items: &[String], max: usize) -> String {
    items
        .iter()
        .take(max)
        .map(|i| format!("\n- {}", clip(i, 300)))
        .collect()
}

impl RunFacts {
    /// Each fact as Chat reads it, keyed by its field on the run page.
    pub fn lines(&self) -> Vec<(&'static str, String)> {
        let RunFacts {
            error,
            stop_kind,
            tooling_failure,
            covered_by,
            no_change,
            waiting_on,
            draft_hold,
            merge_standing,
            waiting_line,
            preview,
            round_incomplete,
            codegen_env_missing,
            own_review,
        } = self;
        let mut out: Vec<(&'static str, String)> = Vec::new();
        if let Some(m) = merge_standing {
            let mut l = format!(
                "Merge standing (CoderHelm's own, code {}): {} Next: {} A person has to act: {}.",
                m.code,
                m.text.trim(),
                m.next.trim(),
                if m.needs_person { "yes" } else { "no" }
            );
            if let Some(link) = &m.link {
                l.push_str(&format!(" Set at: {link}"));
            }
            out.push(("merge_standing", l));
        }
        if let Some(r) = own_review {
            let mut l = format!(
                "CoderHelm's own review: {} finding{} open after {} fix round{}",
                r.open,
                if r.open == 1 { "" } else { "s" },
                r.rounds,
                if r.rounds == 1 { "" } else { "s" },
            );
            if r.open > 0 || !r.open_findings.is_empty() {
                l.push_str(
                    "; while a finding is open its review asks for changes and auto-merge holds",
                );
            }
            l.push('.');
            if !r.open_findings.is_empty() {
                l.push_str(&format!("\nOpen findings:{}", list(&r.open_findings, 8)));
            }
            if !r.noted.is_empty() {
                l.push_str(&format!(
                    "\nLeft open as CoderHelm's note (never approved over):{}",
                    list(&r.noted, 8)
                ));
            }
            if !r.for_a_person.is_empty() {
                l.push_str(&format!(
                    "\nOnly a person can settle:{}",
                    list(&r.for_a_person, 8)
                ));
            }
            out.push(("own_review", l));
        }
        if let Some(w) = waiting_line {
            out.push(("waiting_line", format!("Pull request: {}", clip(w, 400))));
        }
        if let Some(w) = waiting_on {
            let mut l = format!("Waiting on a person: {}", clip(&w.action, 200));
            if !w.detail.trim().is_empty() {
                l.push_str(&format!(". {}", clip(&w.detail, 400)));
            }
            if let Some(u) = &w.url {
                l.push_str(&format!(" ({u})"));
            }
            out.push(("waiting_on", l));
        }
        if let Some(h) = draft_hold {
            let reasons: Vec<String> = h
                .reasons
                .iter()
                .map(|r| format!("{}: {}", r.what, r.todo))
                .collect();
            out.push((
                "draft_hold",
                format!(
                    "Draft hold ({}): {}{}",
                    h.kind,
                    clip(&h.summary, 400),
                    list(&reasons, 6)
                ),
            ));
        }
        if let Some(r) = round_incomplete {
            out.push((
                "round_incomplete",
                format!(
                    "Last fix round didn't finish: {}{}",
                    clip(&r.summary, 400),
                    if r.wip_branch.is_empty() {
                        String::new()
                    } else {
                        format!(" Its work so far is on {}.", r.wip_branch)
                    }
                ),
            ));
        }
        if let Some(e) = error {
            out.push(("error", format!("Why it stopped: {}", clip(e, 600))));
        }
        if let Some(k) = stop_kind {
            out.push(("stop_kind", format!("How it stopped: {k}")));
        }
        if let Some(t) = tooling_failure {
            out.push((
                "tooling_failure",
                format!(
                    "A command the run ran failed: `{}`: {}",
                    clip(&t.command, 200),
                    clip(&t.error, 300)
                ),
            ));
        }
        if let Some(c) = codegen_env_missing {
            out.push((
                "codegen_env_missing",
                format!(
                    "Stopped before its pull request: `{}` in {} needs public values the repository doesn't set: {}",
                    clip(&c.command, 200),
                    c.repo,
                    c.vars.join(", ")
                ),
            ));
        }
        if let Some(c) = covered_by {
            out.push((
                "covered_by",
                format!(
                    "Covered by pull request {}#{}{}: {}",
                    c.repo,
                    c.number,
                    if c.url.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", c.url)
                    },
                    clip(&c.reason, 400)
                ),
            ));
        }
        if let Some(c) = no_change {
            let mut l = format!("No change was needed: {}", clip(&c.reason, 600));
            if let Some(p) = &c.open_pr {
                l.push_str(&format!("\nIts open pull request: {}", clip(&p.line, 300)));
            }
            out.push(("no_change", l));
        }
        if let Some(p) = preview {
            let checks: Vec<String> = p
                .checks
                .iter()
                .map(|c| format!("{}: {}", c.name, c.outcome))
                .collect();
            let mut l = format!("Preview: {}", p.stage);
            if !p.url.is_empty() {
                l.push_str(&format!(" at {}", p.url));
            }
            if !checks.is_empty() {
                l.push_str(&format!("; checks: {}", checks.join(", ")));
            }
            if !p.why.trim().is_empty() {
                l.push_str(&format!("; {}", clip(&p.why, 300)));
            }
            out.push(("preview", l));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::AttributeValue as A;
    use serde_json::json;
    use std::collections::{BTreeSet, HashMap};

    /// Fields of the run page Chat's run record writes itself, from the
    /// same record.
    const CHAT_WRITES: &[&str] = &[
        "run_id",
        "status",
        "title",
        "ticket_id",
        "ticket_source",
        "repo",
        "pr_url",
        "prs",
        "current_pass",
        "created_at",
        "updated_at",
        "closed_by",
        "close_reason",
        "questions",
        "progress_notes",
        "pr_review",
    ];

    /// Fields of the run page Chat's run record leaves out, each with why.
    const NOT_FOR_CHAT: &[(&str, &str)] = &[
        ("pr_number", "the pull requests are in `prs`"),
        ("pr_urls", "the pull requests are in `prs`"),
        ("pr_numbers", "the pull requests are in `prs`"),
        ("branch", "the pull request names its branch"),
        ("branches", "the pull request names its branch"),
        ("tokens_in", "cost, not a decision"),
        ("tokens_out", "cost, not a decision"),
        ("cache_read_tokens", "cost, not a decision"),
        ("cache_write_tokens", "cost, not a decision"),
        ("cost_usd", "cost, not a decision"),
        ("duration_s", "cost, not a decision"),
        ("files_modified", "Chat reads the pull request's files"),
        ("mcp_servers", "setup, read by Chat's setup check"),
        ("applied_standards", "setup, read by Chat's setup check"),
        (
            "policy_note",
            "a tool policy note, shown with the tool call",
        ),
        ("github_wait", "a passing GitHub wait, shown while it lasts"),
        (
            "policy_approval",
            "the approval card is in Chat's what-needs-me",
        ),
        ("sign_in_prompts", "sign-ins are in Chat's what-needs-me"),
        ("fix_location", "said by `error` and `no_change`"),
        (
            "outside_change_report",
            "said by `error` for an outside change",
        ),
        (
            "traceability",
            "a per-criterion table, read on the run page",
        ),
        ("exploration", "what the run read, not a decision"),
        ("lesson", "Chat's memory search reads lessons"),
        (
            "closed_at",
            "the close is in `closed_by` and `close_reason`",
        ),
        (
            "closed_from",
            "the close is in `closed_by` and `close_reason`",
        ),
        ("pass_history", "the current step is in `current_pass`"),
        ("questions_url", "the questions are in `questions`"),
        ("answers", "answers continue the run, which Chat reads"),
        ("answers_url", "answers continue the run, which Chat reads"),
        ("continued_run_id", "the continuing run is its own record"),
        ("trigger", "who started it is in the runs list"),
        (
            "checks",
            "live from GitHub: Chat's pull request lookup reads them",
        ),
        (
            "pr_review_comments",
            "live from GitHub: Chat's pull request lookup reads them",
        ),
    ];

    fn run_page_fields() -> BTreeSet<String> {
        use utoipa::PartialSchema;
        let schema = serde_json::to_value(super::super::api::Run::schema()).unwrap();
        schema["properties"]
            .as_object()
            .expect("the run page's schema lists its fields")
            .keys()
            .cloned()
            .collect()
    }

    fn fact_fields() -> BTreeSet<String> {
        serde_json::to_value(RunFacts::default())
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    fn put(i: &mut Item, k: &str, v: Value) {
        i.insert(
            k.into(),
            A::S(match v {
                Value::String(s) => s,
                v => v.to_string(),
            }),
        );
    }

    /// A run with every fact CoderHelm records set.
    pub fn every_fact() -> Item {
        let mut i: Item = HashMap::new();
        put(&mut i, "run_id", json!("01HARBORRUN0000000000000001"));
        put(&mut i, "status", json!("needs_input"));
        put(&mut i, "title", json!("Retry the export job"));
        put(&mut i, "repo", json!("harbor/api"));
        i.insert("pr_number".into(), A::N("12".into()));
        put(&mut i, "error_message", json!("The fix round stopped"));
        put(&mut i, "stop_kind", json!("tooling"));
        put(
            &mut i,
            "tooling_failure",
            json!({"command": "pnpm exec tsc", "error": "tsc: not found", "tried": []}),
        );
        put(
            &mut i,
            "covered_by",
            json!({"repo": "harbor/api", "number": 9, "url": "https://github.com/harbor/api/pull/9",
                   "reason": "It already retries the export", "own": true}),
        );
        put(
            &mut i,
            "no_change",
            json!({"reason": "The job already retries", "checks": ["read jobs/export.ts"],
                   "open_pr": {"repo": "harbor/api", "number": 9, "verdict": "keep",
                               "line": "Pull request #9 stays open"}}),
        );
        put(
            &mut i,
            common::run_wait::FIELD_WAITING_ON,
            json!({"action": "Review pull request #12", "detail": "The work is on the pull request",
                   "url": "https://github.com/harbor/api/pull/12", "since": "2026-03-02T10:00:00Z"}),
        );
        put(
            &mut i,
            common::draft_hold::FIELD,
            json!({"kind": "held", "reasons": [{"what": "A security item is open", "todo": "Fix it on this branch"}],
                   "pr_number": 12, "since": "2026-03-02T10:00:00Z"}),
        );
        put(
            &mut i,
            common::merge_standing::FIELD,
            json!({"code": "review_changes_requested",
                   "text": "CoderHelm's review of the head asks for changes.",
                   "next": "CoderHelm fixes its findings and reviews again.",
                   "needs_person": false}),
        );
        let preview = common::preview::State {
            stage: common::preview::Stage::Checked,
            url: "https://pr-12.preview.harbor.example".into(),
            why: "Pages captured".into(),
            ..Default::default()
        };
        put(&mut i, common::preview::FIELD, json!(preview.to_json()));
        let inc = common::run_round::Incomplete {
            number: 2,
            reason: "the review fix round ran out of time".into(),
            wip_branch: "coderhelm-wip/export".into(),
            at: "2026-03-02T11:00:00Z".into(),
            ..Default::default()
        };
        i.insert(common::run_round::INCOMPLETE_FIELD.into(), inc.to_attr());
        i.insert(
            "codegen_env_missing".into(),
            A::M(HashMap::from([
                ("repo".to_string(), A::S("harbor/web".into())),
                ("command".to_string(), A::S("pnpm run typegen".into())),
                (
                    "vars".to_string(),
                    A::L(vec![A::S("PUBLIC_PROJECT_ID".into())]),
                ),
            ])),
        );
        i.insert(OWN_REVIEW_OPEN.into(), A::N("1".into()));
        i.insert(OWN_REVIEW_ROUNDS.into(), A::N("2".into()));
        put(
            &mut i,
            OWN_REVIEW_LEDGER,
            json!({"findings": [
                {"item": "The retry limit is read from the wrong setting (jobs/export.ts:41)"},
                {"item": "A log line has a typo", "settled": {"kind": "fixed"}},
                {"item": "The export bucket needs a new policy",
                 "settled": {"kind": "deferred", "reason": "permission", "what": "An admin adds it"}},
                {"item": "A flaky check", "settled": {"kind": "noted", "why": "every approach tried"}}
            ]}),
        );
        i
    }

    /// Every field of the run page is a fact Chat reads from here, a field
    /// Chat's record writes itself, or named as one it leaves out. A field
    /// added to the run page without either fails here.
    #[test]
    fn every_run_page_field_is_a_chat_fact_or_named() {
        let facts = fact_fields();
        let named: BTreeSet<String> = CHAT_WRITES
            .iter()
            .map(|s| s.to_string())
            .chain(NOT_FOR_CHAT.iter().map(|(k, _)| k.to_string()))
            .collect();
        let page = run_page_fields();
        let unplaced: Vec<&String> = page
            .iter()
            .filter(|f| !facts.contains(*f) && !named.contains(*f))
            .collect();
        assert!(
            unplaced.is_empty(),
            "run page fields Chat neither reads nor names: {unplaced:?}. Add each to RunFacts \
             (and RunFacts::lines), or to NOT_FOR_CHAT with the reason."
        );
        // Every fact is on the page, and nothing is named twice or stale.
        for f in &facts {
            assert!(page.contains(f), "fact {f} isn't on the run page");
            assert!(!named.contains(f), "fact {f} is also named as not a fact");
        }
        for f in &named {
            assert!(page.contains(f), "{f} is named but isn't on the run page");
        }
    }

    /// Chat's record of a run with every fact shows every one of them.
    #[test]
    fn chat_carries_every_fact_the_run_page_shows() {
        let item = every_fact();
        let facts = of_item(&item);
        let set: BTreeSet<String> = serde_json::to_value(&facts)
            .unwrap()
            .as_object()
            .unwrap()
            .iter()
            .filter(|(_, v)| !v.is_null())
            .map(|(k, _)| k.clone())
            .collect();
        // The fixture keeps up with the facts.
        assert_eq!(set, fact_fields(), "every_fact() leaves a fact unset");
        let lines = facts.lines();
        let keyed: BTreeSet<String> = lines.iter().map(|(k, _)| k.to_string()).collect();
        assert_eq!(keyed, set, "a fact has no line in Chat's record");
        let passage = super::super::chat_work_runs::record(&item).passage;
        for (k, l) in &lines {
            assert!(passage.contains(l.as_str()), "{k} missing from:\n{passage}");
        }
    }

    /// The run page and Chat read the same facts for the same run.
    #[test]
    fn the_run_page_and_chat_show_the_same_facts() {
        let item = every_fact();
        let page = serde_json::to_value(super::super::api::run_of(
            &item,
            "01HARBORRUN0000000000000001",
            Default::default(),
            &Default::default(),
        ))
        .unwrap();
        let facts = serde_json::to_value(of_item(&item)).unwrap();
        for (k, v) in facts.as_object().unwrap() {
            assert_eq!(&page[k], v, "the run page's {k} differs from Chat's");
        }
        assert_eq!(page["merge_standing"]["code"], "review_changes_requested");
    }

    #[test]
    fn the_own_review_lists_what_is_open_and_drops_what_was_settled() {
        let r = own_review_of(&every_fact()).unwrap();
        assert_eq!((r.open, r.rounds), (1, 2));
        assert_eq!(
            r.open_findings,
            ["The retry limit is read from the wrong setting (jobs/export.ts:41)"]
        );
        assert_eq!(
            r.for_a_person,
            ["The export bucket needs a new policy: An admin adds it"]
        );
        assert_eq!(r.noted, ["A flaky check (every approach tried)"]);
        // Before any review: nothing.
        let mut bare = every_fact();
        bare.remove(OWN_REVIEW_OPEN);
        bare.remove(OWN_REVIEW_ROUNDS);
        bare.remove(OWN_REVIEW_LEDGER);
        assert!(own_review_of(&bare).is_none());
    }

    /// The question that went wrong: why auto-merge holds. The record says
    /// it is CoderHelm's own review, with its finding.
    #[test]
    fn a_held_merge_is_explained_by_coderhelms_own_review() {
        let passage = super::super::chat_work_runs::record(&every_fact()).passage;
        assert!(passage.contains("Merge standing (CoderHelm's own, code review_changes_requested)"));
        assert!(passage.contains("auto-merge holds"));
        assert!(passage.contains("jobs/export.ts:41"));
    }
}
