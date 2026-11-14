//! The work around a pull request that is not in its diff: each step with
//! one owner and one state, said in words a person can act on.
//!
//! A small feature's pull request carried a page about its outside steps
//! and nobody could tell what they had to do. One step was listed under
//! "needs access CoderHelm doesn't have" and again as "(owner: CoderHelm)";
//! steps read "CoderHelm does this after the pull request opens" long after
//! it had opened; the same raw `403` with its JSON body and API URL appeared
//! twice; names and lookups CoderHelm had already found were listed as items
//! with "(owner: found by CoderHelm)"; and a query CoderHelm had already run
//! was given back as "How to confirm ... (owner: the owner of the last
//! step)".
//!
//! The rules, for every kind of run:
//!
//! - a step has exactly one [`State`], and its text is generated from that
//!   state every time the description is built ([`Step::line`]);
//! - what a person must do comes first, one numbered line per step: what,
//!   the exact command, when, and why CoderHelm did not, in one sentence
//!   with the way to let it ([`needed`]). With nothing to do the block is
//!   "Nothing." ([`NOTHING_NEEDED`]);
//! - a check CoderHelm can make is made and reported as a result
//!   ([`Checked`]), never listed as a step for somebody else;
//! - errors are translated to a cause a person can act on ([`plain_error`]);
//!   raw queries, ids and responses are evidence, shown collapsed
//!   ([`evidence_block`]); the names of CoderHelm's own tools never appear
//!   ([`plain`]).

use serde::{Deserialize, Serialize};

/// What the lead says when nothing is needed from a person.
pub const NOTHING_NEEDED: &str = "Nothing.";

/// The names of CoderHelm's own tools and triggers: never shown to a reader.
pub const INTERNAL_NAMES: &[(&str, &str)] = &[
    ("query_content", "a content query"),
    ("start_workflow", "starting a workflow"),
    ("write_content", "a content write"),
    ("check_url", "a page check"),
    ("run_checks", "the checks"),
    ("search_code", "a code search"),
];

/// When a step happens.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum When {
    #[default]
    Any,
    BeforeMerge,
    AfterMerge,
}

impl When {
    fn words(self) -> &'static str {
        match self {
            When::Any => "",
            When::BeforeMerge => "Before merging: ",
            When::AfterMerge => "After merging: ",
        }
    }
}

/// The one state a step is in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum State {
    /// Done, with what shows it.
    Done { evidence: String },
    /// CoderHelm does it when this named thing happens (the merge, the step
    /// before it, a workflow that is running).
    Waiting { on: String },
    /// CoderHelm does it once a person approves: who, and where.
    Approval { who: String, place: String },
    /// CoderHelm tried and could not: the cause in plain words, and what
    /// lets it.
    Failed { cause: String, unblock: String },
    /// Only a person can: why, and what would let CoderHelm next time.
    Person { why: String, unblock: String },
}

/// One step outside the diff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub what: String,
    /// The exact command or click, when there is one.
    #[serde(default)]
    pub how: String,
    #[serde(default)]
    pub when: When,
    /// The one owner: `CoderHelm`, or the person or role.
    pub owner: String,
    pub state: State,
}

/// A check CoderHelm made itself.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checked {
    pub what: String,
    pub result: String,
    /// When (a date, or the commit it was made on).
    #[serde(default)]
    pub when: String,
}

impl Checked {
    pub fn line(&self) -> String {
        let mut t = format!(
            "Checked: {}, {}",
            sentence_part(&self.what),
            sentence_part(&self.result)
        );
        if !self.when.trim().is_empty() {
            t.push_str(&format!(", {}", sentence_part(&self.when)));
        }
        t.push('.');
        t
    }
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn sentence_part(s: &str) -> String {
    one_line(s).trim_end_matches(['.', ';', ',']).to_string()
}

/// Whether `how` is a command a person can paste.
pub fn is_command(how: &str) -> bool {
    let h = how.trim().trim_matches('`');
    let first = h.split_whitespace().next().unwrap_or("");
    first.starts_with("./")
        || [
            "cd",
            "npm",
            "npx",
            "pnpm",
            "yarn",
            "make",
            "cargo",
            "git",
            "gh",
            "terraform",
            "kubectl",
            "docker",
            "python",
            "python3",
            "pip",
            "bundle",
            "rake",
            "go",
            "aws",
            "sanity",
            "node",
            "bash",
            "sh",
        ]
        .contains(&first)
}

/// `text` without the names of CoderHelm's own tools.
pub fn plain(text: &str) -> String {
    let mut t = one_line(text);
    for (name, words) in INTERNAL_NAMES {
        t = t.replace(&format!("`{name}`"), words).replace(name, words);
    }
    while t.contains("not done: not done") {
        t = t.replace("not done: not done", "not done");
    }
    t
}

/// A raw error as a cause a person can act on, and what lifts it:
/// `(cause, unblock)`. Never a URL, a status line or a response body.
pub fn plain_error(raw: &str) -> (String, String) {
    let l = raw.to_lowercase();
    let refused = l.contains("resource not accessible by integration")
        || l.contains("403")
        || l.contains("forbidden");
    if refused && l.contains("/actions/") && (l.contains("dispatches") || l.contains("rerun")) {
        return (
            "CoderHelm's GitHub App is not allowed to start workflows in this repository".into(),
            "grant the app \"Actions: write\" (GitHub, Settings, Installed GitHub Apps, CoderHelm, Permissions)".into(),
        );
    }
    if refused && l.contains("github") {
        return (
            "GitHub refused it: CoderHelm's GitHub App lacks a permission in this repository"
                .into(),
            "check the app's permissions (GitHub, Settings, Installed GitHub Apps, CoderHelm)"
                .into(),
        );
    }
    if refused {
        return (
            "the service refused it: the connection CoderHelm uses is not allowed to".into(),
            "give that connection the access, in Settings".into(),
        );
    }
    if l.contains("401") || l.contains("unauthorized") || l.contains("bad credentials") {
        return (
            "the connection's credentials were refused".into(),
            "reconnect it in Settings".into(),
        );
    }
    if l.contains("404") || l.contains("not found") {
        return (
            "it was not found (it does not exist, or CoderHelm cannot see it)".into(),
            "check the name, or give CoderHelm access to it".into(),
        );
    }
    if l.contains("timed out") || l.contains("timeout") || l.contains("502") || l.contains("503") {
        return (
            "the service did not answer in time".into(),
            "nothing: CoderHelm tries again on its next step".into(),
        );
    }
    (strip_machinery(raw), String::new())
}

/// `raw` without URLs, response bodies and status-line noise, cut short.
pub fn strip_machinery(raw: &str) -> String {
    let mut t = one_line(raw);
    // A JSON body, from its first brace to the end.
    if let Some(i) = t.find("{\"") {
        t.truncate(i);
    }
    let mut out: Vec<&str> = Vec::new();
    for w in t.split(' ') {
        let bare = w.trim_matches(|c: char| "()[],;:".contains(c));
        if bare.starts_with("http://") || bare.starts_with("https://") {
            continue;
        }
        out.push(w);
    }
    let t = out.join(" ");
    let t = t
        .replace("HTTP status client error", "the request was refused")
        .replace("HTTP status server error", "the service failed")
        .replace("for url", "")
        .replace("()", "");
    let t = plain(&t);
    let t = t
        .trim()
        .trim_end_matches([':', ',', ';', '('])
        .trim()
        .to_string();
    crate::truncate_str(&t, 200).to_string()
}

impl Step {
    /// Whether a person has something to do for it now.
    pub fn needs_person(&self) -> bool {
        matches!(
            self.state,
            State::Person { .. } | State::Failed { .. } | State::Approval { .. }
        )
    }

    fn how_words(&self) -> String {
        let h = self.how.trim();
        if h.is_empty() {
            String::new()
        } else if is_command(h) {
            format!(": run `{}`", h.trim_matches('`'))
        } else {
            format!(": {}", sentence_part(&plain(h)))
        }
    }

    /// The step as one line, generated from its state.
    pub fn line(&self) -> String {
        let what = sentence_part(&plain(&self.what));
        match &self.state {
            State::Done { evidence } => {
                let e = sentence_part(&plain(evidence));
                if e.is_empty() {
                    format!("Done: {what}.")
                } else {
                    format!("Done: {what} ({e}).")
                }
            }
            State::Waiting { on } => {
                format!("{what}. CoderHelm does this {}.", sentence_part(&plain(on)))
            }
            State::Approval { who, place } => format!(
                "Approve \"{what}\": {} approves it {}, and CoderHelm then does it.",
                sentence_part(who),
                sentence_part(place)
            ),
            State::Failed { cause, unblock } => {
                let mut t = format!(
                    "{}{what}{}. CoderHelm could not: {}.",
                    self.when.words(),
                    self.how_words(),
                    sentence_part(cause)
                );
                if !unblock.trim().is_empty() {
                    t.push_str(&format!(" To let it: {}.", sentence_part(unblock)));
                }
                t
            }
            State::Person { why, unblock } => {
                let mut t = format!("{}{what}{}.", self.when.words(), self.how_words());
                if !why.trim().is_empty() {
                    t.push_str(&format!(
                        " CoderHelm cannot: {}.",
                        sentence_part(&plain(why))
                    ));
                }
                // "none: ..." is a reason there is no way, not a way.
                if !unblock.trim().is_empty() && !unblock.trim().to_lowercase().starts_with("none")
                {
                    t.push_str(&format!(" To let it: {}.", sentence_part(&plain(unblock))));
                }
                t
            }
        }
    }
}

/// The most steps the lead lists.
pub const MAX_NEEDED: usize = 6;

/// What a person has to do, numbered, one line per step with its one
/// owner. A cause several steps share is said once, on the first of them.
/// Empty when nothing is needed.
pub fn needed(steps: &[Step]) -> Vec<String> {
    let mut out = Vec::new();
    let mut said: Vec<String> = Vec::new();
    for s in steps.iter().filter(|s| s.needs_person()) {
        let mut step = s.clone();
        if let State::Failed { cause, unblock } = &mut step.state {
            if said.contains(cause) {
                *cause = "the same reason as above".to_string();
                unblock.clear();
            } else {
                said.push(cause.clone());
            }
        }
        out.push(format!("{} ({})", step.line(), sentence_part(&step.owner)));
    }
    out
}

/// The steps nobody has to act on now (done, or waiting on a named event),
/// for the collapsed detail.
pub fn others(steps: &[Step]) -> Vec<String> {
    steps
        .iter()
        .filter(|s| !s.needs_person())
        .map(Step::line)
        .collect()
}

/// The markers of the evidence block.
pub const EVIDENCE_START: &str = "<!-- coderhelm:evidence -->";
pub const EVIDENCE_END: &str = "<!-- /coderhelm:evidence -->";

/// The steps that are done, the checks CoderHelm made and the raw evidence
/// (queries, ids, responses), as one block for the collapsed part. A step
/// still to come is listed once, with the work not in the pull request.
/// Empty when there is nothing.
pub fn evidence_block(steps: &[Step], checked: &[Checked], raw: &[String]) -> String {
    let others: Vec<String> = steps
        .iter()
        .filter(|s| matches!(s.state, State::Done { .. }))
        .map(Step::line)
        .collect();
    if others.is_empty() && checked.is_empty() && raw.is_empty() {
        return String::new();
    }
    let mut s = format!("{EVIDENCE_START}\n## Steps and evidence\n");
    for l in &others {
        s.push_str(&format!("- {l}\n"));
    }
    for c in checked {
        s.push_str(&format!("- {}\n", c.line()));
    }
    if !raw.is_empty() {
        s.push_str("\nRaw evidence:\n");
        for r in raw {
            s.push_str(&format!("- {}\n", one_line(r)));
        }
    }
    s.push_str(EVIDENCE_END);
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAW_403: &str = "not done: starting `Test Suite` (.github/workflows/test.yml, started with workflow_dispatch) failed: HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/acme/site/actions/workflows/test.yml/dispatches): {\"message\":\"Resource not accessible by integration\",\"documentation_url\":\"https://docs.github.com/rest\",\"status\":\"403\"}";

    #[test]
    fn a_raw_error_becomes_a_cause_and_what_lifts_it() {
        let (cause, unblock) = plain_error(RAW_403);
        assert_eq!(
            cause,
            "CoderHelm's GitHub App is not allowed to start workflows in this repository"
        );
        assert!(unblock.contains("Actions: write"), "{unblock}");
        for t in [&cause, &unblock] {
            assert!(
                !t.contains("http") && !t.contains('{') && !t.contains("403"),
                "{t}"
            );
        }
        let (cause, _) = plain_error("the staging write failed: error sending request for url (https://x.api.example.com/v1/data/mutate/staging): operation timed out");
        assert_eq!(cause, "the service did not answer in time");
        let (cause, unblock) = plain_error("no mutations were given for this step");
        assert_eq!(cause, "no mutations were given for this step");
        assert_eq!(unblock, "");
    }

    #[test]
    fn machinery_is_stripped_from_what_is_left() {
        let t = strip_machinery("not done: not done: calling query_content failed for url (https://api.example.com/q?x=1): {\"error\":\"bad\"}");
        assert_eq!(t, "not done: calling a content query failed");
    }

    fn deploy(state: State) -> Step {
        Step {
            what: "Deploy the staging Studio".into(),
            how: "cd apps/studio && npm run deploy:staging".into(),
            when: When::AfterMerge,
            owner: "a repository admin".into(),
            state,
        }
    }

    #[test]
    fn a_step_reads_from_its_one_state() {
        let (cause, unblock) = plain_error(RAW_403);
        let failed = deploy(State::Failed { cause, unblock });
        assert_eq!(
            failed.line(),
            "After merging: Deploy the staging Studio: run `cd apps/studio && npm run deploy:staging`. CoderHelm could not: CoderHelm's GitHub App is not allowed to start workflows in this repository. To let it: grant the app \"Actions: write\" (GitHub, Settings, Installed GitHub Apps, CoderHelm, Permissions)."
        );
        let done = deploy(State::Done {
            evidence: "the deploy workflow passed".into(),
        });
        assert_eq!(
            done.line(),
            "Done: Deploy the staging Studio (the deploy workflow passed)."
        );
        assert!(!done.needs_person());
        let waiting = deploy(State::Waiting {
            on: "when this pull request merges".into(),
        });
        assert_eq!(
            waiting.line(),
            "Deploy the staging Studio. CoderHelm does this when this pull request merges."
        );
        let approval = deploy(State::Approval {
            who: "an admin of the team".into(),
            place: "on the Agent permissions page".into(),
        });
        assert!(approval.needs_person());
        assert_eq!(
            approval.line(),
            "Approve \"Deploy the staging Studio\": an admin of the team approves it on the Agent permissions page, and CoderHelm then does it."
        );
    }

    #[test]
    fn what_is_needed_is_numbered_once_with_one_owner_each() {
        let (cause, unblock) = plain_error(RAW_403);
        let steps = vec![
            deploy(State::Failed {
                cause: cause.clone(),
                unblock: unblock.clone(),
            }),
            Step {
                what: "Deploy the production Studio".into(),
                how: "cd apps/studio && npm run deploy".into(),
                when: When::AfterMerge,
                owner: "a repository admin".into(),
                state: State::Failed { cause, unblock },
            },
            Step {
                what: "Confirm the warning shows with query_content".into(),
                how: String::new(),
                when: When::Any,
                owner: "CoderHelm".into(),
                state: State::Done {
                    evidence: "one item, n = 392".into(),
                },
            },
        ];
        let lines = needed(&steps);
        assert_eq!(lines.len(), 2);
        assert!(lines[0].ends_with("(a repository admin)"), "{}", lines[0]);
        assert_eq!(lines[0].matches("Actions: write").count(), 1);
        // The shared cause is said once.
        assert!(
            lines[1].contains("the same reason as above"),
            "{}",
            lines[1]
        );
        assert!(!lines[1].contains("Actions: write"), "{}", lines[1]);
        let all = lines.join("\n");
        assert!(!all.contains("403") && !all.contains("https://"), "{all}");
        let rest = others(&steps);
        assert_eq!(
            rest,
            vec!["Done: Confirm the warning shows with a content query (one item, n = 392)."]
        );
        assert!(needed(&rest_only()).is_empty());
    }

    fn rest_only() -> Vec<Step> {
        vec![deploy(State::Done {
            evidence: String::new(),
        })]
    }

    #[test]
    fn checks_and_raw_evidence_go_in_the_collapsed_block() {
        let block = evidence_block(
            &rest_only(),
            &[Checked {
                what: "the count of pages with the field".into(),
                result: "392, as expected".into(),
                when: "on `abc1234`".into(),
            }],
            &["query `count(*[_type == \"page\"])` returned [{\"n\":392}]".to_string()],
        );
        assert!(block.starts_with(EVIDENCE_START) && block.ends_with(EVIDENCE_END));
        assert!(
            block.contains("- Done: Deploy the staging Studio."),
            "{block}"
        );
        assert!(
            block.contains(
                "- Checked: the count of pages with the field, 392, as expected, on `abc1234`."
            ),
            "{block}"
        );
        assert!(block.contains("Raw evidence:\n- query `count"), "{block}");
        assert_eq!(evidence_block(&[], &[], &[]), "");
    }

    #[test]
    fn a_command_is_told_from_words() {
        assert!(is_command("cd apps/studio && npm run deploy"));
        assert!(is_command("`./scripts/deploy.sh staging`"));
        assert!(!is_command(
            "query_content on production with the confirm query"
        ));
        assert!(!is_command("none, because the website doesn't change"));
    }
}
