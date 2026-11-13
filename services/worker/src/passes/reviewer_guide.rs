//! The part of a pull request's description written for its reviewer.
//!
//! The writer says what changed and why (Summary, Changes) and what is open
//! (Open items, Not in this PR). This section adds what a reviewer needs to
//! decide quickly, from records and never from the writer:
//!
//! - **Look first**: the one file where a mistake costs most, and why (the
//!   reason the diff's risk floor gave, else the largest code change).
//! - **Roll back**: how, and what a revert of the code does not undo (a
//!   migration that ran, infrastructure that was applied).
//! - **Verified by**: one line per check that reported on the head commit,
//!   each with its result, the commit and a link to the recorded result. A
//!   check that failed, is still running or was skipped is listed as not
//!   verified; nothing is claimed that no check result backs.
//!
//! It is short by construction: a line that has nothing to say is left out,
//! and a description with no file and no check gets no section at all. It
//! is rebuilt from the current diff and the head's checks whenever the
//! description is ([`refresh`]), so it never describes an earlier commit.

use std::collections::HashMap;

use serde_json::Value;
use tracing::warn;

use super::pr_risk::Floor;
use crate::clients::github::GitHubClient;

pub const START: &str = "<!-- coderhelm:reviewer-guide -->";
pub const END: &str = "<!-- /coderhelm:reviewer-guide -->";

/// Passed checks named one by one; the rest are counted.
const MAX_PASSED_LINES: usize = 8;

fn is_test(path: &str) -> bool {
    let l = path.to_ascii_lowercase();
    l.contains(".test.")
        || l.contains(".spec.")
        || l.contains("_test.")
        || l.contains("__tests__")
        || l.contains("/test/")
        || l.contains("/tests/")
        || l.starts_with("test/")
        || l.starts_with("tests/")
}

/// Files nobody reviews line by line: lockfiles, snapshots, generated code.
fn is_generated(path: &str) -> bool {
    let l = path.to_ascii_lowercase();
    let name = l.rsplit('/').next().unwrap_or(&l);
    name.ends_with(".lock")
        || name.ends_with("-lock.json")
        || name.ends_with("-lock.yaml")
        || name == "go.sum"
        || name.ends_with(".snap")
        || name.ends_with(".min.js")
        || l.contains("/generated/")
        || l.contains("/__snapshots__/")
}

fn added_lines(patch: &str) -> usize {
    patch
        .lines()
        .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
        .count()
}

/// The first path a risk reason names in backticks, when the diff has it.
fn path_in_reason(reason: &str, files: &[(String, String, String)]) -> Option<String> {
    reason
        .split('`')
        .skip(1)
        .step_by(2)
        .find(|p| files.iter().any(|(f, _, _)| f == p))
        .map(str::to_string)
}

/// Where the reviewer looks first, and why: the file the diff's risk floor
/// names, else the code file with the most added lines. None for a diff
/// with nothing to read (only removed, generated or test files).
pub fn look_first(files: &[(String, String, String)], floor: &Floor) -> Option<(String, String)> {
    for reason in &floor.reasons {
        if let Some(path) = path_in_reason(reason, files) {
            return Some((path, reason.clone()));
        }
    }
    files
        .iter()
        .filter(|(p, status, _)| status != "removed" && !is_test(p) && !is_generated(p))
        .map(|(p, _, patch)| (p, added_lines(patch)))
        .filter(|(_, n)| *n > 0)
        // The first of equals, so the answer never depends on file order
        // beyond the diff's own.
        .fold(None::<(&String, usize)>, |best, cur| match best {
            Some(b) if b.1 >= cur.1 => Some(b),
            _ => Some(cur),
        })
        .map(|(p, n)| {
            (
                p.clone(),
                format!(
                    "the largest change to code ({n} added line{})",
                    if n == 1 { "" } else { "s" }
                ),
            )
        })
}

/// What reverting the code leaves in place, by the kind of file changed.
const NOT_UNDONE: &[(&str, &[&str])] = &[
    (
        "a migration that already ran stays applied",
        &["/migrations/", "/migration/", "/migrate/", "db/migrate"],
    ),
    (
        "infrastructure already applied stays as it is until the stack is applied again",
        &[
            ".tf",
            ".tfvars",
            "cloudformation",
            "/cdk/",
            "/infra/",
            "/helm/",
            "/k8s/",
        ],
    ),
];

/// How to roll the change back: a revert of the pull request, and what that
/// revert does not undo, each with the changed file that says so.
pub fn rollback(files: &[(String, String, String)]) -> String {
    let mut caveats: Vec<String> = Vec::new();
    for (what, marks) in NOT_UNDONE {
        let hit = files.iter().find(|(p, status, _)| {
            let l = format!("/{}", p.to_ascii_lowercase());
            status != "removed"
                && marks.iter().any(|m| match m.strip_prefix('.') {
                    // An extension ends the path; a directory is anywhere in it.
                    Some(_) => l.ends_with(m),
                    None => l.contains(m),
                })
        });
        if let Some((p, _, _)) = hit {
            caveats.push(format!("{what} (`{p}`)"));
        }
    }
    if caveats.is_empty() {
        "revert this pull request; nothing outside the code changes with it.".to_string()
    } else {
        format!(
            "revert this pull request. The revert does not undo everything: {}.",
            caveats.join("; ")
        )
    }
}

/// One check's standing on the head commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    pub name: String,
    pub url: String,
    pub state: State,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Passed,
    Failed,
    Running,
    /// Skipped, neutral or cancelled: it reported, and proved nothing.
    DidNotRun,
}

/// The newest run of each check in a `check-runs` listing (commit statuses
/// included, see `common::commit_ci`), by name.
pub fn checks_of(listing: &Value) -> Vec<Check> {
    let mut latest: HashMap<&str, &Value> = HashMap::new();
    let mut order: Vec<&str> = Vec::new();
    for c in listing["check_runs"].as_array().into_iter().flatten() {
        let Some(name) = c["name"].as_str().filter(|n| !n.is_empty()) else {
            continue;
        };
        let ts = c["started_at"].as_str().unwrap_or("");
        match latest.get(name) {
            Some(prev) if prev["started_at"].as_str().unwrap_or("") > ts => {}
            Some(_) => {
                latest.insert(name, c);
            }
            None => {
                order.push(name);
                latest.insert(name, c);
            }
        }
    }
    order
        .into_iter()
        .map(|name| {
            let c = latest[name];
            let state = if c["status"].as_str() != Some("completed") {
                State::Running
            } else {
                match c["conclusion"].as_str().unwrap_or("") {
                    "success" => State::Passed,
                    "skipped" | "neutral" | "cancelled" | "stale" => State::DidNotRun,
                    _ => State::Failed,
                }
            };
            Check {
                name: name.to_string(),
                url: c["html_url"]
                    .as_str()
                    .or_else(|| c["details_url"].as_str())
                    .unwrap_or("")
                    .to_string(),
                state,
            }
        })
        .collect()
}

fn short(sha: &str) -> &str {
    sha.get(..7).unwrap_or(sha)
}

fn linked(c: &Check) -> String {
    if c.url.starts_with("https://") {
        format!("[`{}`]({})", c.name.replace('`', "'"), c.url)
    } else {
        format!("`{}`", c.name.replace('`', "'"))
    }
}

/// The verification lines: what passed on `head` (each with its recorded
/// result), then what did not verify the change and why. Empty when no
/// check reported: no claim is made without a result behind it.
pub fn verified_lines(checks: &[Check], head: &str) -> Vec<String> {
    let mut out = Vec::new();
    let passed: Vec<&Check> = checks.iter().filter(|c| c.state == State::Passed).collect();
    if !passed.is_empty() {
        let shown: Vec<String> = passed
            .iter()
            .take(MAX_PASSED_LINES)
            .map(|c| linked(c))
            .collect();
        let more = passed.len().saturating_sub(MAX_PASSED_LINES);
        out.push(format!(
            "**Verified by:** {}{} passed on `{}`.",
            shown.join(", "),
            if more > 0 {
                format!(" and {more} more")
            } else {
                String::new()
            },
            short(head)
        ));
    }
    let mut not: Vec<String> = Vec::new();
    for (state, word) in [
        (State::Failed, "failed"),
        (State::Running, "still running"),
        (State::DidNotRun, "did not run"),
    ] {
        let names: Vec<String> = checks
            .iter()
            .filter(|c| c.state == state)
            .map(linked)
            .collect();
        if !names.is_empty() {
            not.push(format!("{} ({word})", names.join(", ")));
        }
    }
    if !not.is_empty() {
        out.push(format!(
            "**Not verified on `{}`:** {}.",
            short(head),
            not.join("; ")
        ));
    }
    out
}

/// The section, from the diff's files (path, status, patch), its risk
/// floor, the head commit and that commit's checks. Empty when there is
/// nothing to say.
pub fn section(
    files: &[(String, String, String)],
    floor: &Floor,
    head: &str,
    checks: &[Check],
) -> String {
    let mut lines: Vec<String> = Vec::new();
    if let Some((path, why)) = look_first(files, floor) {
        // A reason that already names the file reads on its own.
        lines.push(if why.contains(&format!("`{path}`")) {
            format!("**Look first:** {why}.")
        } else {
            format!("**Look first:** `{path}`, {why}.")
        });
    }
    if !files.is_empty() {
        lines.push(format!("**Roll back:** {}", rollback(files)));
    }
    lines.extend(verified_lines(checks, head));
    if lines.is_empty() {
        return String::new();
    }
    format!(
        "{START}\n**For the reviewer**\n{}\n{END}",
        lines
            .iter()
            .map(|l| format!("* {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// `body` with the section rebuilt for the current diff and head.
pub fn put(body: &str, section: String) -> String {
    super::completeness::with_section(body, super::completeness::Section::ReviewerGuide, section)
}

/// The section for a pull request as it stands: its current diff's files
/// and floor, and the checks on `head`. An unreadable check listing leaves
/// the verification lines out rather than guessing them.
pub async fn build(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    files: &[(String, String, String)],
    floor: &Floor,
    head: &str,
) -> String {
    let checks = if head.is_empty() {
        Vec::new()
    } else {
        match github.list_ci_for_ref(owner, repo, head).await {
            Ok(listing) => checks_of(&listing),
            Err(e) => {
                warn!(owner, repo, head, error = %e, "Reviewer guide: could not read the head's checks");
                Vec::new()
            }
        }
    };
    section(files, floor, head, &checks)
}

/// Rebuilds the section on an open pull request from what GitHub shows now.
/// Costs no model call. Leaves the description alone when nothing changed
/// or anything can't be read.
pub async fn refresh(github: &GitHubClient, owner: &str, repo: &str, pr_number: u64) {
    let Ok(pr) = github.get_pull_request(owner, repo, pr_number).await else {
        return;
    };
    let (Some(base), Some(head_ref)) = (pr["base"]["ref"].as_str(), pr["head"]["ref"].as_str())
    else {
        return;
    };
    let head = pr["head"]["sha"].as_str().unwrap_or("");
    let body = pr["body"].as_str().unwrap_or("");
    let Ok(diff) = github.get_diff(owner, repo, base, head_ref).await else {
        return;
    };
    let files = super::pr_risk::diff_files(&diff);
    let floor = super::pr_risk::floor(&files, &HashMap::new());
    let out = put(body, build(github, owner, repo, &files, &floor, head).await);
    if out != body {
        if let Err(e) = crate::passes::description::save(github, owner, repo, pr_number, &out).await
        {
            warn!(pr_number, error = %e, "Reviewer guide: could not save the description");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::pr_risk::Level;
    use serde_json::json;

    const HEAD: &str = "0a1b2c3d4e5f60718293a4b5c6d7e8f901234567";

    fn file(path: &str, status: &str, added: usize) -> (String, String, String) {
        (
            path.to_string(),
            status.to_string(),
            format!("@@ -1 +1,{added} @@\n{}", "+line\n".repeat(added)),
        )
    }

    fn low() -> Floor {
        Floor {
            level: Level::Low,
            reasons: Vec::new(),
        }
    }

    fn run(name: &str, status: &str, conclusion: Option<&str>, started: &str) -> Value {
        json!({
            "name": name, "status": status, "conclusion": conclusion,
            "started_at": started,
            "html_url": format!("https://github.com/acme/shop/actions/runs/7/job/{}", name.len()),
        })
    }

    #[test]
    fn the_risk_floors_file_is_looked_at_first() {
        let files = vec![
            file("apps/web/src/pages/cart.tsx", "modified", 90),
            file("apps/web/src/middleware.ts", "modified", 4),
        ];
        let floor = Floor {
            level: Level::Medium,
            reasons: vec!["`apps/web/src/middleware.ts` runs on every request".to_string()],
        };
        let (path, why) = look_first(&files, &floor).unwrap();
        assert_eq!(path, "apps/web/src/middleware.ts");
        assert!(why.contains("every request"));
        // A reason naming a file the diff no longer has is not followed.
        let stale = Floor {
            level: Level::Medium,
            reasons: vec!["`apps/web/src/gone.ts` is shared".to_string()],
        };
        assert_eq!(
            look_first(&files, &stale).unwrap().0,
            "apps/web/src/pages/cart.tsx"
        );
    }

    #[test]
    fn without_a_floor_the_largest_code_change_is_first_never_a_test_or_lockfile() {
        let files = vec![
            file("pnpm-lock.yaml", "modified", 900),
            file("apps/web/src/cart/total.test.ts", "added", 120),
            file("apps/web/src/cart/total.ts", "modified", 31),
            file("apps/web/src/cart/format.ts", "modified", 31),
            file("apps/web/src/cart/old.ts", "removed", 0),
        ];
        let (path, why) = look_first(&files, &low()).unwrap();
        assert_eq!(path, "apps/web/src/cart/total.ts");
        assert_eq!(why, "the largest change to code (31 added lines)");
        assert_eq!(
            look_first(&[file("pnpm-lock.yaml", "modified", 9)], &low()),
            None
        );
    }

    #[test]
    fn rollback_says_what_a_revert_does_not_undo() {
        assert_eq!(
            rollback(&[file("apps/web/src/a.ts", "modified", 3)]),
            "revert this pull request; nothing outside the code changes with it."
        );
        let r = rollback(&[
            file("services/api/migrations/0042_orders.sql", "added", 12),
            file("infra/queues.tf", "modified", 5),
        ]);
        assert!(r.starts_with("revert this pull request. The revert does not undo everything:"));
        assert!(r.contains(
            "a migration that already ran stays applied (`services/api/migrations/0042_orders.sql`)"
        ));
        assert!(r.contains("(`infra/queues.tf`)"));
        // A removed migration file is no new migration.
        assert!(
            !rollback(&[file("services/api/migrations/0001.sql", "removed", 0)])
                .contains("does not undo")
        );
    }

    #[test]
    fn every_verification_claim_points_at_a_recorded_result() {
        let listing = json!({"check_runs": [
            run("unit", "completed", Some("failure"), "2026-02-01T10:00:00Z"),
            // The re-run of the same check is the one that counts.
            run("unit", "completed", Some("success"), "2026-02-01T10:20:00Z"),
            run("lint", "completed", Some("success"), "2026-02-01T10:00:00Z"),
            run("e2e", "in_progress", None, "2026-02-01T10:00:00Z"),
            run("deploy-preview", "completed", Some("skipped"), "2026-02-01T10:00:00Z"),
            run("types", "completed", Some("timed_out"), "2026-02-01T10:00:00Z"),
        ]});
        let checks = checks_of(&listing);
        let lines = verified_lines(&checks, HEAD);
        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0],
            "**Verified by:** [`unit`](https://github.com/acme/shop/actions/runs/7/job/4), \
             [`lint`](https://github.com/acme/shop/actions/runs/7/job/4) passed on `0a1b2c3`."
        );
        assert!(lines[1].starts_with("**Not verified on `0a1b2c3`:**"));
        assert!(lines[1].contains("job/5) (failed)"), "{}", lines[1]);
        assert!(lines[1].contains("job/3) (still running)"), "{}", lines[1]);
        assert!(lines[1].contains("job/14) (did not run)"), "{}", lines[1]);
        // A check that did not pass is never among the verified.
        assert!(!lines[0].contains("e2e") && !lines[0].contains("types"));
    }

    #[test]
    fn no_check_result_means_no_verification_claim() {
        assert!(verified_lines(&[], HEAD).is_empty());
        let s = section(&[file("src/a.ts", "modified", 2)], &low(), HEAD, &[]);
        assert!(s.contains("**Look first:** `src/a.ts`"));
        assert!(s.contains("**Roll back:**"));
        assert!(!s.contains("Verified"));
    }

    #[test]
    fn many_passed_checks_are_counted_not_listed() {
        let checks: Vec<Check> = (0..MAX_PASSED_LINES + 3)
            .map(|i| Check {
                name: format!("job-{i}"),
                url: String::new(),
                state: State::Passed,
            })
            .collect();
        let lines = verified_lines(&checks, HEAD);
        assert!(
            lines[0].contains("`job-7` and 3 more passed"),
            "{}",
            lines[0]
        );
    }

    #[test]
    fn nothing_to_say_is_no_section_and_the_section_is_replaced_in_place() {
        assert_eq!(section(&[], &low(), HEAD, &[]), "");
        let body =
            "Closes #4\n\n**Summary** x\n\n**Risk** **Low**: small.\n\n**Verification**\n1. CI.";
        let first = put(
            body,
            section(&[file("src/a.ts", "modified", 2)], &low(), HEAD, &[]),
        );
        assert!(first.find(START).unwrap() < first.find("**Risk**").unwrap());
        assert!(first.find("**Summary**").unwrap() < first.find(START).unwrap());
        let second = put(
            &first,
            section(&[file("src/b.ts", "modified", 9)], &low(), HEAD, &[]),
        );
        assert_eq!(second.matches(START).count(), 1);
        assert!(second.contains("`src/b.ts`") && !second.contains("`src/a.ts`"));
        assert_eq!(put(&second, String::new()), body);
    }
}
