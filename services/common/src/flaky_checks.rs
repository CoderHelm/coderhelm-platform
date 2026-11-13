//! Flaky or broken: whose failure a red check is.
//!
//! A red check on CoderHelm's pull request is not always the pull request's
//! doing. Before a fix round changes code for it, three things are read:
//!
//! - whether the check also fails on the base branch's head (red before the
//!   change, see `resume::split_base_failures`);
//! - whether the repository has a record of the check failing and then
//!   passing on the same commit of OTHER pull requests ([`Record`]);
//! - whether the failure names a file the pull request changed ([`touch`]).
//!
//! A failure that names a changed file is the change's, whatever the record
//! says. A failure of a check with a record of intermittent failures on
//! unrelated pull requests, which names no changed file, is re-run, not
//! "fixed": changing code that is not wrong makes the pull request worse and
//! the next reader's trust in it lower. The pull request says which check
//! was judged flaky and on what evidence ([`section`]).
//!
//! The record is one item per repository. It learns from what really
//! happened: a commit whose check failed and then passed on a re-run with
//! no change in between is the only observation that counts as flaky.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Sort key prefix of a repository's record (`FLAKY#owner/repo`), under the
/// team's partition of the settings table.
pub const PREFIX: &str = "FLAKY#";

/// Observations older than this say nothing about the check today.
pub const WINDOW_DAYS: i64 = 30;
/// Other pull requests that must have seen the check fail and then pass on
/// one commit before it counts as flaky for this one.
pub const MIN_OTHER_PRS: usize = 2;
/// Observations kept per check, newest last.
pub const MAX_OBSERVATIONS: usize = 20;
/// Checks kept per repository; the ones seen longest ago go first.
pub const MAX_CHECKS: usize = 200;

/// The record's sort key for `owner/repo`.
pub fn sk(repo: &str) -> String {
    format!("{PREFIX}{repo}")
}

/// What was seen of one check on one commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// It failed, was re-run with no change, and passed.
    PassedOnRerun,
    /// It failed for the runner's or a service's reason (a DNS miss, a
    /// runner that went away), read from its log.
    Infra,
}

/// One observation, with what a person needs to check it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub kind: Kind,
    /// RFC 3339.
    pub at: String,
    pub pr: u64,
    pub sha: String,
    /// The line that says so (an infrastructure line), empty when none.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub line: String,
}

/// A repository's flaky checks: observations by check name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    #[serde(default)]
    pub checks: BTreeMap<String, Vec<Observation>>,
}

impl Record {
    pub fn from_json(s: &str) -> Record {
        serde_json::from_str(s).unwrap_or_default()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Adds an observation. The same commit of the same pull request is one
    /// observation however often it is reported (a webhook and a sweep). True
    /// when the record changed.
    pub fn observe(&mut self, check: &str, o: Observation) -> bool {
        let check = check.trim();
        if check.is_empty() {
            return false;
        }
        let list = self.checks.entry(check.to_string()).or_default();
        if list
            .iter()
            .any(|x| x.kind == o.kind && x.pr == o.pr && x.sha == o.sha)
        {
            return false;
        }
        list.push(o);
        if list.len() > MAX_OBSERVATIONS {
            let drop = list.len() - MAX_OBSERVATIONS;
            list.drain(..drop);
        }
        while self.checks.len() > MAX_CHECKS {
            let oldest = self
                .checks
                .iter()
                .min_by_key(|(_, v)| v.last().map(|o| o.at.clone()).unwrap_or_default())
                .map(|(k, _)| k.clone());
            match oldest {
                Some(k) => {
                    self.checks.remove(&k);
                }
                None => break,
            }
        }
        true
    }

    /// The check's observations inside the window on pull requests other
    /// than `pr`, newest last.
    pub fn elsewhere(&self, check: &str, pr: u64, now: DateTime<Utc>) -> Vec<&Observation> {
        let cut = now - Duration::days(WINDOW_DAYS);
        self.checks
            .get(check.trim())
            .map(|l| {
                l.iter()
                    .filter(|o| o.pr != pr && o.kind == Kind::PassedOnRerun)
                    .filter(|o| {
                        DateTime::parse_from_rfc3339(&o.at)
                            .map(|t| t.with_timezone(&Utc) >= cut)
                            .unwrap_or(false)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether the check has a record of intermittent failure on unrelated
    /// pull requests: it failed and then passed on one commit of at least
    /// [`MIN_OTHER_PRS`] other pull requests inside the window.
    pub fn known_flaky(&self, check: &str, pr: u64, now: DateTime<Utc>) -> bool {
        let mut prs: Vec<u64> = self
            .elsewhere(check, pr, now)
            .iter()
            .map(|o| o.pr)
            .collect();
        prs.sort_unstable();
        prs.dedup();
        prs.len() >= MIN_OTHER_PRS
    }
}

/// What a failure's output says about the files the pull request changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Touch {
    /// It names these changed files: the change's own failure.
    Changed(Vec<String>),
    /// It names files, none of them changed by the pull request.
    Elsewhere(Vec<String>),
    /// It names no file.
    Unknown,
}

/// File extensions a failure's output names source files with.
const SOURCE_EXTS: &[&str] = &[
    "rs", "ts", "tsx", "js", "jsx", "mjs", "cjs", "py", "go", "rb", "java", "kt", "swift", "cs",
    "php", "c", "cc", "cpp", "h", "hpp", "vue", "svelte", "scala", "ex", "exs", "tf", "sql",
    "yaml", "yml", "json", "toml", "css", "scss", "html", "sh",
];

/// A token of a log line as a repository path, when it reads as one:
/// `src/a/b.test.ts:12:3`, `./src/a.py`, `/home/runner/work/app/app/src/a.go`.
fn path_in(token: &str) -> Option<String> {
    let t = token.trim_matches(|c: char| {
        !(c.is_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | '@' | '[' | ']'))
    });
    // `file.rs:12:3` and `file.py(12)`: the position goes.
    let t = t.split([':', '(']).next().unwrap_or(t);
    let t = t.trim_end_matches(['.', ',', ')', ']']);
    let (stem, ext) = t.rsplit_once('.')?;
    if stem.is_empty() || !SOURCE_EXTS.contains(&ext.to_ascii_lowercase().as_str()) {
        return None;
    }
    if t.contains("://") || t.len() > 300 {
        return None;
    }
    // A bare file name names nothing in particular; a path does.
    if !t.contains('/') {
        return None;
    }
    let t = t.trim_start_matches("./");
    // A runner's checkout prefix (`/home/runner/work/{repo}/{repo}/`).
    let t = match t.split_once("/work/") {
        Some((_, rest)) if t.starts_with('/') => {
            let mut parts = rest.splitn(3, '/');
            match (parts.next(), parts.next(), parts.next()) {
                (Some(a), Some(b), Some(path)) if a == b => path,
                _ => rest,
            }
        }
        _ => t,
    };
    let t = t.trim_start_matches('/');
    (!t.is_empty() && !t.contains("node_modules/")).then(|| t.to_string())
}

/// The repository paths a failure's output names, in order, each once.
pub fn named_paths(logs: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for token in logs.split(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '`' | '=')) {
        if let Some(p) = path_in(token) {
            if !out.contains(&p) {
                out.push(p);
            }
        }
        if out.len() >= 200 {
            break;
        }
    }
    out
}

/// Whether a path a log names is the changed file `changed`: equal, or one
/// ends with the other at a directory boundary (a log printed relative to a
/// package directory, or with a build prefix).
fn same_file(named: &str, changed: &str) -> bool {
    let ends = |long: &str, short: &str| {
        long.ends_with(short)
            && long[..long.len() - short.len()]
                .chars()
                .last()
                .is_none_or(|c| c == '/')
    };
    named == changed || ends(named, changed) || ends(changed, named)
}

/// What the failure's output says about the pull request's changed files.
pub fn touch(logs: &str, changed: &[String]) -> Touch {
    let named = named_paths(logs);
    if named.is_empty() {
        return Touch::Unknown;
    }
    let hit: Vec<String> = changed
        .iter()
        .filter(|c| named.iter().any(|n| same_file(n, c)))
        .cloned()
        .collect();
    if hit.is_empty() {
        Touch::Elsewhere(named.into_iter().take(5).collect())
    } else {
        Touch::Changed(hit)
    }
}

/// Why a failing check is not the pull request's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotOurs {
    /// It fails on the base branch's head too.
    RedOnBase,
    /// The runner or a service failed, with the line that says so.
    Infra(String),
    /// It failed and then passed on one commit of these other pull requests
    /// (`#pr at sha`), newest last.
    Flaky(Vec<String>),
}

/// Whose failure each failing check is.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Judgement {
    /// Checks the pull request has to fix.
    pub ours: Vec<String>,
    /// Checks it did not cause, with why.
    pub not_ours: Vec<(String, NotOurs)>,
}

impl Judgement {
    /// Nothing here is the pull request's to fix: re-run, don't change code.
    pub fn none_ours(&self) -> bool {
        self.ours.is_empty() && !self.not_ours.is_empty()
    }
}

/// Judges the failing checks of a pull request's head. `failing` is every
/// failing check, `on_base` those failing on the base branch's head too,
/// `touch` what the failure output says about the changed files, `infra`
/// the infrastructure line the output shows.
///
/// A check failing on the base is the base's. For the rest: output that
/// names a changed file makes every one of them the pull request's. An
/// infrastructure line makes them the runner's. Otherwise a check is flaky
/// only with a record on unrelated pull requests; with none it is the pull
/// request's, since a failure that names no changed file can still be
/// caused by one.
pub fn judge(
    failing: &[String],
    on_base: &[String],
    touch: &Touch,
    infra: Option<&str>,
    record: &Record,
    pr: u64,
    now: DateTime<Utc>,
) -> Judgement {
    let mut j = Judgement::default();
    for check in failing {
        if on_base.contains(check) {
            j.not_ours.push((check.clone(), NotOurs::RedOnBase));
            continue;
        }
        if matches!(touch, Touch::Changed(_)) {
            j.ours.push(check.clone());
            continue;
        }
        if let Some(line) = infra {
            j.not_ours
                .push((check.clone(), NotOurs::Infra(line.to_string())));
            continue;
        }
        if record.known_flaky(check, pr, now) {
            let seen = record
                .elsewhere(check, pr, now)
                .iter()
                .map(|o| format!("#{} at `{}`", o.pr, short(&o.sha)))
                .collect();
            j.not_ours.push((check.clone(), NotOurs::Flaky(seen)));
            continue;
        }
        j.ours.push(check.clone());
    }
    j
}

fn short(sha: &str) -> &str {
    sha.get(..7).unwrap_or(sha)
}

/// What happened to the re-run of the checks judged not the pull request's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rerun {
    /// The failed jobs were started again.
    Started,
    /// GitHub refused: the installation may not write Actions.
    NoPermission,
    /// Nothing to start again (a check outside GitHub Actions).
    NotActions,
}

/// Whether GitHub's refusal of a re-run is a missing permission (the App
/// has no Actions write on the repository), not a failure worth a retry.
pub fn rerun_refused_for_permission(error: &str) -> bool {
    let e = error.to_ascii_lowercase();
    e.contains("resource not accessible by integration")
        || (e.contains("403") && (e.contains("actions") || e.contains("must have admin rights")))
}

pub const START: &str = "<!-- coderhelm:ci-notes -->";
pub const END: &str = "<!-- /coderhelm:ci-notes -->";

/// The pull request's section saying which red checks CoderHelm judged not
/// caused by the change, the evidence for each, and what it did about it.
/// Empty when every failing check is the pull request's.
pub fn section(j: &Judgement, base: &str, rerun: &Rerun, touch: &Touch) -> String {
    if j.not_ours.is_empty() {
        return String::new();
    }
    let mut lines: Vec<String> = Vec::new();
    for (check, why) in &j.not_ours {
        lines.push(match why {
            NotOurs::RedOnBase => {
                format!("* `{check}` also fails on the head of `{base}`: red before this change.")
            }
            NotOurs::Infra(line) => format!(
                "* `{check}` failed on the runner or a service, not on the code: `{}`.",
                line.replace('`', "'")
            ),
            NotOurs::Flaky(seen) => format!(
                "* `{check}` has failed and then passed on the same commit of other pull \
                 requests ({}).",
                seen.join(", ")
            ),
        });
    }
    if let Touch::Elsewhere(files) = touch {
        lines.push(format!(
            "* The failure names {}, which this pull request does not change.",
            files
                .iter()
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let did = match rerun {
        Rerun::Started if j.ours.is_empty() => {
            "CoderHelm re-ran the failed jobs and changed no code for them."
        }
        Rerun::Started => {
            "CoderHelm re-ran the failed jobs for these and fixes the others in code."
        }
        Rerun::NoPermission => {
            "CoderHelm changed no code for them. It could not re-run them: its GitHub App has no \
             Actions write permission on this repository (Settings, GitHub App, Repository \
             permissions, Actions: Read and write)."
        }
        Rerun::NotActions => {
            "CoderHelm changed no code for them. They run outside GitHub Actions, so it has no \
             way to start them again."
        }
    };
    format!(
        "{START}\n**Red checks not caused by this change**\n{}\n\n{did}\n{END}",
        lines.join("\n")
    )
}

/// What the fix round is told when a check with a flaky record failed again
/// after its re-run: the record is evidence, the second failure is too.
pub fn fixer_note(j: &Judgement) -> String {
    let flaky: Vec<&str> = j
        .not_ours
        .iter()
        .filter(|(_, w)| matches!(w, NotOurs::Flaky(_)))
        .map(|(c, _)| c.as_str())
        .collect();
    if flaky.is_empty() {
        return String::new();
    }
    format!(
        "Note on {}: the repository's record shows it failing and then passing on one commit of \
         unrelated pull requests, and it failed here again after a re-run. Find what in THIS \
         change it depends on before changing anything; change only code the failure output \
         ties to this change, and never weaken, skip or delete the test.",
        flaky.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn seen(pr: u64, sha: &str, days_ago: i64) -> Observation {
        Observation {
            kind: Kind::PassedOnRerun,
            at: (now() - Duration::days(days_ago)).to_rfc3339(),
            pr,
            sha: sha.to_string(),
            line: String::new(),
        }
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn one_commit_is_one_observation() {
        let mut r = Record::default();
        assert!(r.observe("e2e", seen(11, "aaaaaaa1", 1)));
        assert!(!r.observe("e2e", seen(11, "aaaaaaa1", 0)));
        assert_eq!(r.checks["e2e"].len(), 1);
        assert!(!r.observe("  ", seen(11, "aaaaaaa1", 0)));
    }

    #[test]
    fn flaky_needs_other_pull_requests_inside_the_window() {
        let mut r = Record::default();
        // This pull request's own history is not evidence about itself.
        r.observe("e2e", seen(40, "s1", 1));
        r.observe("e2e", seen(40, "s2", 1));
        assert!(!r.known_flaky("e2e", 40, now()));
        // One other pull request is not a record.
        r.observe("e2e", seen(31, "s3", 2));
        assert!(!r.known_flaky("e2e", 40, now()));
        // Old observations say nothing about today.
        r.observe("e2e", seen(12, "s4", WINDOW_DAYS + 3));
        assert!(!r.known_flaky("e2e", 40, now()));
        r.observe("e2e", seen(33, "s5", 5));
        assert!(r.known_flaky("e2e", 40, now()));
        // An infrastructure failure is not a flaky test.
        let mut infra = Record::default();
        for pr in [1, 2, 3] {
            infra.observe(
                "build",
                Observation {
                    kind: Kind::Infra,
                    ..seen(pr, "s", 1)
                },
            );
        }
        assert!(!infra.known_flaky("build", 40, now()));
    }

    #[test]
    fn the_record_stays_bounded_and_round_trips() {
        let mut r = Record::default();
        for i in 0..(MAX_OBSERVATIONS as u64 + 5) {
            r.observe("e2e", seen(i, "s", 1));
        }
        assert_eq!(r.checks["e2e"].len(), MAX_OBSERVATIONS);
        assert_eq!(r.checks["e2e"][0].pr, 5);
        assert_eq!(Record::from_json(&r.to_json()), r);
        assert_eq!(Record::from_json("not json"), Record::default());
    }

    #[test]
    fn paths_are_read_from_failure_output() {
        let logs = "FAIL apps/web/src/cart/total.test.ts:41:7\n\
                    at /home/runner/work/shop/shop/apps/web/src/cart/total.ts:12:3\n\
                    error[E0308]: mismatched types --> services/api/src/price.rs:88:9\n\
                    see https://example.com/docs/a.html and node_modules/x/y.js and total.ts";
        assert_eq!(
            named_paths(logs),
            strings(&[
                "apps/web/src/cart/total.test.ts",
                "apps/web/src/cart/total.ts",
                "services/api/src/price.rs"
            ])
        );
    }

    #[test]
    fn touch_matches_changed_files_at_directory_boundaries() {
        let changed = strings(&["apps/web/src/cart/total.ts"]);
        // A log printed relative to the package directory.
        assert_eq!(
            touch("FAIL src/cart/total.ts:3:1", &changed),
            Touch::Changed(changed.clone())
        );
        // A file whose name merely ends alike is another file.
        assert_eq!(
            touch("FAIL src/cart/subtotal.ts:3:1", &changed),
            Touch::Elsewhere(strings(&["src/cart/subtotal.ts"]))
        );
        assert_eq!(
            touch("Timeout of 30000ms exceeded", &changed),
            Touch::Unknown
        );
    }

    fn flaky_record() -> Record {
        let mut r = Record::default();
        r.observe("e2e", seen(31, "abcdef1234", 2));
        r.observe("e2e", seen(33, "1234abcdef", 5));
        r
    }

    #[test]
    fn a_failure_naming_a_changed_file_is_the_changes_whatever_the_record_says() {
        let j = judge(
            &strings(&["e2e"]),
            &[],
            &Touch::Changed(strings(&["src/a.ts"])),
            Some("ECONNRESET"),
            &flaky_record(),
            40,
            now(),
        );
        assert_eq!(j.ours, strings(&["e2e"]));
        assert!(!j.none_ours());
    }

    #[test]
    fn a_known_flaky_check_that_names_no_changed_file_is_not_fixed_in_code() {
        let j = judge(
            &strings(&["e2e", "unit"]),
            &strings(&["lint"]),
            &Touch::Elsewhere(strings(&["tests/checkout.spec.ts"])),
            None,
            &flaky_record(),
            40,
            now(),
        );
        // `unit` has no record: a failure that names no changed file can
        // still be caused by one.
        assert_eq!(j.ours, strings(&["unit"]));
        assert_eq!(
            j.not_ours,
            vec![(
                "e2e".to_string(),
                NotOurs::Flaky(strings(&["#31 at `abcdef1`", "#33 at `1234abc`"]))
            )]
        );
        let only = judge(
            &strings(&["e2e", "lint"]),
            &strings(&["lint"]),
            &Touch::Unknown,
            None,
            &flaky_record(),
            40,
            now(),
        );
        assert!(only.none_ours());
        assert_eq!(only.not_ours[1], ("lint".to_string(), NotOurs::RedOnBase));
    }

    #[test]
    fn the_section_names_each_check_its_evidence_and_what_was_done() {
        let j = judge(
            &strings(&["e2e", "lint"]),
            &strings(&["lint"]),
            &Touch::Elsewhere(strings(&["tests/checkout.spec.ts"])),
            None,
            &flaky_record(),
            40,
            now(),
        );
        let touch = Touch::Elsewhere(strings(&["tests/checkout.spec.ts"]));
        let s = section(&j, "main", &Rerun::Started, &touch);
        assert!(s.starts_with(START) && s.ends_with(END));
        assert!(s.contains("`e2e` has failed and then passed on the same commit of other pull requests (#31 at `abcdef1`, #33 at `1234abc`)"));
        assert!(s.contains("`lint` also fails on the head of `main`"));
        assert!(s.contains("`tests/checkout.spec.ts`, which this pull request does not change"));
        assert!(s.contains("re-ran the failed jobs and changed no code"));
        let refused = section(&j, "main", &Rerun::NoPermission, &touch);
        assert!(refused.contains("no Actions write permission"));
        assert!(refused.contains("changed no code"));
        assert_eq!(
            section(&Judgement::default(), "main", &Rerun::Started, &touch),
            ""
        );
    }

    #[test]
    fn a_refused_rerun_is_told_from_a_failed_one() {
        assert!(rerun_refused_for_permission(
            "GitHub API 403: {\"message\":\"Resource not accessible by integration\"}"
        ));
        assert!(!rerun_refused_for_permission("GitHub API 502: bad gateway"));
        assert!(!rerun_refused_for_permission(
            "GitHub API 409: workflow run is already running"
        ));
    }
}
