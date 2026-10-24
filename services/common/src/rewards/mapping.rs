//! From what GitHub and the dashboard report to reward events.
//!
//! Pure functions: the webhook handlers and the worker read the payloads and
//! call these, so the mapping is tested without a network.

use super::event::{RewardEvent, RewardKind, AGENT_CODER, AGENT_REVIEWER};
use serde_json::Value;

/// What is known about the run behind a pull request.
#[derive(Debug, Clone, Default)]
pub struct RunContext {
    pub team_id: String,
    pub run_id: String,
    pub repo: String,
    pub pr: u64,
    pub model: String,
    pub prompt_version: String,
    pub variant: String,
    pub skills: Vec<String>,
    pub plugin: String,
    /// The area most of the change touched.
    pub area: String,
}

impl RunContext {
    fn event(&self, kind: RewardKind, agent: &str, at: &str, dedup: &str) -> RewardEvent {
        let mut e = RewardEvent::new(&self.team_id, kind, agent, at, dedup)
            .with_repo(&self.repo, self.pr)
            .with_source("webhook:pull_request");
        e.run_id = self.run_id.clone();
        e.model = self.model.clone();
        e.prompt_version = self.prompt_version.clone();
        e.variant = self.variant.clone();
        e.skills = self.skills.clone();
        e.plugin = self.plugin.clone();
        e.area = self.area.clone();
        e
    }
}

/// How CI went on the run, read from the run record.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct CiSummary {
    /// Fix cycles started (`ci_fix_attempts`).
    pub fix_attempts: u64,
    /// The last CI result was green.
    pub passed: bool,
    /// CI ever reported on the run.
    pub known: bool,
}

/// The share of the pull request's diff people changed after CoderHelm's
/// last commit, 0..1. `changed` is the lines added and removed between
/// CoderHelm's last commit and the merged head; `total` the PR's.
pub fn edit_fraction(changed: u64, total: u64) -> f64 {
    if changed == 0 {
        return 0.0;
    }
    (changed as f64 / total.max(changed).max(1) as f64).clamp(0.0, 1.0)
}

fn hours_between(from: &str, to: &str) -> Option<f64> {
    let a = chrono::DateTime::parse_from_rfc3339(from).ok()?;
    let b = chrono::DateTime::parse_from_rfc3339(to).ok()?;
    let h = (b - a).num_seconds() as f64 / 3600.0;
    (h >= 0.0).then_some(h)
}

/// The events of a CoderHelm pull request closing.
///
/// * merged, no commits from people → merged as written;
/// * merged with people's commits → merged after edits, valued by how much
///   of the diff they changed (`edit_share`, None when it couldn't be
///   measured: then no edited/unchanged event is guessed);
/// * closed without merging → closed;
/// * merged → time to merge; and CI's first-try or fix-round result.
pub fn pr_closed_events(
    ctx: &RunContext,
    pr: &Value,
    human_commits: Option<u64>,
    edit_share: Option<f64>,
    ci: CiSummary,
) -> Vec<RewardEvent> {
    let mut out = vec![];
    let key = format!("{}#{}", ctx.repo, ctx.pr);
    let opened = pr["created_at"].as_str().unwrap_or("");
    match crate::pr_outcome::outcome(pr) {
        crate::pr_outcome::Outcome::Merged {
            merged_at,
            merged_by,
            ..
        } => {
            match human_commits {
                Some(0) => out.push(
                    ctx.event(
                        RewardKind::PrMergedUnchanged,
                        AGENT_CODER,
                        &merged_at,
                        &format!("merged#{key}"),
                    )
                    .with_detail(&format!("PR #{} merged as written by {merged_by}", ctx.pr)),
                ),
                Some(n) => {
                    if let Some(share) = edit_share {
                        out.push(
                            ctx.event(
                                RewardKind::PrMergedEdited,
                                AGENT_CODER,
                                &merged_at,
                                &format!("merged#{key}"),
                            )
                            .with_value(share)
                            .with_detail(&format!(
                                "PR #{} merged after {n} commit(s) from people, {}% of the diff changed",
                                ctx.pr,
                                (share * 100.0).round() as u64
                            )),
                        );
                    }
                }
                None => {}
            }
            if let Some(h) = hours_between(opened, &merged_at) {
                out.push(
                    ctx.event(
                        RewardKind::TimeToMerge,
                        AGENT_CODER,
                        &merged_at,
                        &format!("ttm#{key}"),
                    )
                    .with_value(h)
                    .with_detail(&format!(
                        "PR #{} merged {:.1} hours after opening",
                        ctx.pr, h
                    )),
                );
            }
            out.extend(ci_event(ctx, &merged_at, &key, ci));
        }
        crate::pr_outcome::Outcome::Closed { closed_at, .. } => {
            out.push(
                ctx.event(
                    RewardKind::PrClosedUnmerged,
                    AGENT_CODER,
                    &closed_at,
                    &format!("closed#{key}"),
                )
                .with_detail(&format!("PR #{} closed without merging", ctx.pr)),
            );
            out.extend(ci_event(ctx, &closed_at, &key, ci));
        }
        crate::pr_outcome::Outcome::Open => {}
    }
    out
}

fn ci_event(ctx: &RunContext, at: &str, key: &str, ci: CiSummary) -> Option<RewardEvent> {
    if !ci.known {
        return None;
    }
    if ci.fix_attempts == 0 && ci.passed {
        return Some(
            ctx.event(
                RewardKind::CiFirstTry,
                AGENT_CODER,
                at,
                &format!("ci#{key}"),
            )
            .with_detail(&format!("CI passed first try on PR #{}", ctx.pr)),
        );
    }
    if ci.fix_attempts > 0 {
        return Some(
            ctx.event(
                RewardKind::CiFixRounds,
                AGENT_CODER,
                at,
                &format!("ci#{key}"),
            )
            .with_value(ci.fix_attempts as f64)
            .with_detail(&format!(
                "CI needed {} fix round(s) on PR #{}",
                ci.fix_attempts, ctx.pr
            )),
        );
    }
    None
}

/// The pull request a GitHub revert pull request undoes: GitHub titles it
/// `Revert "..."` and writes `Reverts owner/name#123` in the body. Only a
/// revert of the same repository counts.
pub fn revert_target(title: &str, body: &str, repo: &str) -> Option<u64> {
    if !title.trim_start().starts_with("Revert") {
        return None;
    }
    let re = regex::Regex::new(r"(?i)\breverts\s+([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#(\d+)").ok()?;
    let cap = re.captures(body)?;
    if let Some(r) = cap.get(1) {
        if !r.as_str().eq_ignore_ascii_case(repo) {
            return None;
        }
    }
    cap.get(2)?.as_str().parse().ok().filter(|n| *n > 0)
}

/// The category of one of CoderHelm's review comments, read from its first
/// line (`🟠 **Title** (tests)` or `(style, not verified)`).
pub fn thread_category(body: &str) -> String {
    let first = body.lines().next().unwrap_or("").trim_end();
    let Some(open) = first.rfind('(') else {
        return String::new();
    };
    if !first.ends_with(')') {
        return String::new();
    }
    let inner = &first[open + 1..first.len() - 1];
    let cat = inner.split(',').next().unwrap_or("").trim();
    cat.to_ascii_lowercase()
}

/// The title of one of CoderHelm's review comments.
pub fn thread_title(body: &str) -> String {
    let first = body.lines().next().unwrap_or("");
    match (first.find("**"), first.rfind("**")) {
        (Some(a), Some(b)) if b > a + 2 => first[a + 2..b].trim().to_string(),
        _ => crate::truncate_str(first.trim(), 120).to_string(),
    }
}

/// A finding event for one of CoderHelm's review threads.
#[allow(clippy::too_many_arguments)]
pub fn finding_event(
    team_id: &str,
    kind: RewardKind,
    repo: &str,
    pr: u64,
    thread_id: &str,
    path: &str,
    body: &str,
    at: &str,
) -> RewardEvent {
    let mut e = RewardEvent::new(
        team_id,
        kind,
        AGENT_REVIEWER,
        at,
        &format!("{}#{thread_id}", kind.key()),
    )
    .with_repo(repo, pr)
    .with_detail(&format!("{} ({path})", thread_title(body)))
    .with_source("worker:review");
    e.category = thread_category(body);
    e.area = super::event::file_area(path);
    e
}

/// An event about a stored review (a rating, a re-review ask), with what
/// the review record says produced it: repository, PR, model, prompt version,
/// variant and the team standards it applied.
pub fn review_event(
    team_id: &str,
    kind: RewardKind,
    review_sk: &str,
    review: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    at: &str,
    dedup: &str,
) -> RewardEvent {
    let gs = |k: &str| {
        review
            .get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let pr = review
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut e =
        RewardEvent::new(team_id, kind, AGENT_REVIEWER, at, dedup).with_repo(&gs("repo"), pr);
    e.review_sk = review_sk.to_string();
    e.model = gs("model");
    e.prompt_version = gs("prompt_version");
    e.variant = gs("prompt_variant");
    e.skills = review
        .get("applied_standards")
        .and_then(|v| match v {
            aws_sdk_dynamodb::types::AttributeValue::L(l) => {
                Some(l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
            }
            aws_sdk_dynamodb::types::AttributeValue::Ss(s) => Some(s.clone()),
            _ => None,
        })
        .unwrap_or_default();
    e
}

/// The pull request numbers of `repo` an alert's text names.
pub fn alert_pr_refs(text: &str, repo: &str) -> Vec<u64> {
    let pat = format!(r"(?i)github\.com/{}/pull/(\d+)", regex::escape(repo));
    let Ok(re) = regex::Regex::new(&pat) else {
        return vec![];
    };
    let mut v: Vec<u64> = re
        .captures_iter(text)
        .filter_map(|c| c.get(1)?.as_str().parse().ok())
        .collect();
    v.sort_unstable();
    v.dedup();
    v
}

/// Commit SHAs (7–40 hex characters, as words) an alert's text names.
pub fn alert_sha_refs(text: &str) -> Vec<String> {
    let Ok(re) = regex::Regex::new(r"\b[0-9a-f]{7,40}\b") else {
        return vec![];
    };
    let mut v: Vec<String> = re
        .find_iter(text)
        .map(|m| m.as_str().to_string())
        // A number made only of digits is a count or an id, not a SHA.
        .filter(|s| s.chars().any(|c| c.is_ascii_alphabetic()))
        .collect();
    v.sort();
    v.dedup();
    v
}

/// Does `sha_ref` (a prefix as alerts print it) name `full_sha`?
pub fn sha_matches(sha_ref: &str, full_sha: &str) -> bool {
    sha_ref.len() >= 7 && !full_sha.is_empty() && full_sha.starts_with(sha_ref)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> RunContext {
        RunContext {
            team_id: "TEAM#a".into(),
            run_id: "r1".into(),
            repo: "acme/web".into(),
            pr: 12,
            model: "claude-opus-5-5".into(),
            prompt_version: "implement-2026-10".into(),
            ..Default::default()
        }
    }

    fn merged() -> Value {
        json!({"state":"closed","merged_at":"2026-10-02T12:00:00Z","created_at":"2026-10-02T00:00:00Z","merged_by":{"login":"pat"}})
    }

    fn kinds(v: &[RewardEvent]) -> Vec<RewardKind> {
        v.iter().filter_map(|e| e.kind).collect()
    }

    #[test]
    fn merged_unchanged() {
        let ci = CiSummary {
            fix_attempts: 0,
            passed: true,
            known: true,
        };
        let v = pr_closed_events(&ctx(), &merged(), Some(0), None, ci);
        assert_eq!(
            kinds(&v),
            vec![
                RewardKind::PrMergedUnchanged,
                RewardKind::TimeToMerge,
                RewardKind::CiFirstTry
            ]
        );
        assert_eq!(v[1].value, 12.0);
        assert!(v.iter().all(|e| e.team_id == "TEAM#a" && e.run_id == "r1"));
        assert!(v[0].detail.contains("merged as written by pat"));
    }

    #[test]
    fn merged_after_edits_measures_the_share() {
        let ci = CiSummary {
            fix_attempts: 2,
            passed: true,
            known: true,
        };
        let v = pr_closed_events(&ctx(), &merged(), Some(3), Some(0.4), ci);
        assert_eq!(
            kinds(&v),
            vec![
                RewardKind::PrMergedEdited,
                RewardKind::TimeToMerge,
                RewardKind::CiFixRounds
            ]
        );
        assert_eq!(v[0].value, 0.4);
        assert!(v[0].detail.contains("40% of the diff"));
        assert_eq!(v[2].value, 2.0);
        // Unmeasured edits are not guessed.
        let v = pr_closed_events(&ctx(), &merged(), Some(3), None, CiSummary::default());
        assert_eq!(kinds(&v), vec![RewardKind::TimeToMerge]);
        // Unknown commits: no merged event either.
        let v = pr_closed_events(&ctx(), &merged(), None, None, CiSummary::default());
        assert_eq!(kinds(&v), vec![RewardKind::TimeToMerge]);
    }

    #[test]
    fn closed_without_merging() {
        let pr = json!({"state":"closed","merged_at":null,"closed_at":"2026-10-03T00:00:00Z","created_at":"2026-10-02T00:00:00Z"});
        let v = pr_closed_events(&ctx(), &pr, Some(0), None, CiSummary::default());
        assert_eq!(kinds(&v), vec![RewardKind::PrClosedUnmerged]);
        assert_eq!(v[0].at, "2026-10-03T00:00:00Z");
        let open = json!({"state":"open"});
        assert!(pr_closed_events(&ctx(), &open, Some(0), None, CiSummary::default()).is_empty());
    }

    #[test]
    fn redelivered_closes_map_to_the_same_keys() {
        let a = pr_closed_events(&ctx(), &merged(), Some(0), None, CiSummary::default());
        let b = pr_closed_events(&ctx(), &merged(), Some(0), None, CiSummary::default());
        let ka: Vec<String> = a.iter().map(RewardEvent::sk).collect();
        let kb: Vec<String> = b.iter().map(RewardEvent::sk).collect();
        assert_eq!(ka, kb);
    }

    #[test]
    fn reverts_are_read_from_githubs_revert_pull_request() {
        assert_eq!(
            revert_target("Revert \"Add retries\"", "Reverts acme/web#12", "acme/web"),
            Some(12)
        );
        assert_eq!(
            revert_target(
                "Revert \"x\"",
                "Reverts #31\n\nbecause it broke",
                "acme/web"
            ),
            Some(31)
        );
        // Another repository's revert, or a PR that only mentions reverts.
        assert_eq!(
            revert_target("Revert \"x\"", "Reverts acme/api#12", "acme/web"),
            None
        );
        assert_eq!(revert_target("Fix login", "Reverts #12", "acme/web"), None);
        assert_eq!(revert_target("Revert \"x\"", "no ref", "acme/web"), None);
    }

    #[test]
    fn edit_share_is_bounded() {
        assert_eq!(edit_fraction(0, 100), 0.0);
        assert_eq!(edit_fraction(25, 100), 0.25);
        assert_eq!(edit_fraction(300, 100), 1.0);
        assert_eq!(edit_fraction(5, 0), 1.0);
    }

    #[test]
    fn thread_categories_and_titles_are_read_from_the_first_line() {
        let b = "🟠 **Missing test for retry** (tests)\n\nAdd a case.";
        assert_eq!(thread_category(b), "tests");
        assert_eq!(thread_title(b), "Missing test for retry");
        assert_eq!(
            thread_category("💬 **Odd name** (Style, not verified)\n\nx"),
            "style"
        );
        assert_eq!(
            thread_category("🔴 **Ask** (question, blocks the merge)"),
            "question"
        );
        assert_eq!(thread_category("plain text"), "");
        let e = finding_event(
            "TEAM#a",
            RewardKind::FindingAccepted,
            "acme/web",
            3,
            "T1",
            "src/api/a.ts",
            b,
            "2026-10-01T00:00:00Z",
        );
        assert_eq!(e.category, "tests");
        assert_eq!(e.area, "src/api");
        assert_eq!(e.agent, "reviewer");
        assert_eq!(e.dedup, "finding_accepted#T1");
    }

    #[test]
    fn ratings_carry_what_produced_the_review() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let review = std::collections::HashMap::from([
            ("repo".to_string(), A::S("acme/web".into())),
            ("pr_number".to_string(), A::N("7".into())),
            ("model".to_string(), A::S("claude-opus-5-5".into())),
            ("prompt_version".to_string(), A::S("review-2026-10".into())),
            ("prompt_variant".to_string(), A::S("focused".into())),
            (
                "applied_standards".to_string(),
                A::L(vec![A::S("api-style".into())]),
            ),
        ]);
        let e = review_event(
            "TEAM#a",
            RewardKind::RatingDown,
            "REVIEW#acme/web#000007#t",
            &review,
            "2026-10-01T00:00:00Z",
            "rating#x",
        );
        assert_eq!(e.repo, "acme/web");
        assert_eq!(e.pr, 7);
        assert_eq!(e.variant, "focused");
        assert_eq!(e.skills, vec!["api-style".to_string()]);
        assert_eq!(e.review_sk, "REVIEW#acme/web#000007#t");
        assert_eq!(e.agent, "reviewer");
    }

    #[test]
    fn alerts_link_by_pr_url_or_commit() {
        let t = "Deploy of https://github.com/acme/web/pull/41 (abc1234) raised 5xx; see github.com/acme/api/pull/9";
        assert_eq!(alert_pr_refs(t, "acme/web"), vec![41]);
        assert_eq!(alert_sha_refs(t), vec!["abc1234".to_string()]);
        assert!(alert_sha_refs("count 1234567 rows").is_empty());
        assert!(sha_matches("abc1234", "abc1234ffff"));
        assert!(!sha_matches("abc", "abc1234ffff"));
        assert!(!sha_matches("abc1234", ""));
    }
}
