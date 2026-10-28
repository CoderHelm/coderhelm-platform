//! "Rebase", "update branch", "sync with main", "merge main in": one intent
//! on CoderHelm's own pull request, bring the branch up to date with its
//! base. How depends on the repository's rules: a real rebase (pushed with a
//! lease) when force pushes are allowed on the branch and every commit on it
//! is CoderHelm's; otherwise the base is merged in, which holds the same
//! code without rewriting history. CoderHelm does it and says what it did in
//! a line that leads with the outcome; it never hands the work back.
//!
//! A model reads the comment (a word match would fire on "this code rebases
//! the timestamps" and miss "can you pull in the latest main?"), and the
//! words it quotes must be in the comment.

use crate::agent::provider::ModelProvider;
use crate::clients::github::{GitHubClient, RebaseResult};
use crate::models::{FeedbackMessage, TicketMessage, TokenUsage};
use crate::WorkerState;
use serde_json::{json, Value};
use tracing::{info, warn};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// A person's request to bring the branch up to date.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Ask {
    /// The words of the request.
    pub quote: String,
    /// The comment asks nothing else, so the update is the whole answer.
    pub only: bool,
    /// They asked for a linear history (no merge commits) in so many words.
    pub linear_history: bool,
    /// The team's instructions say not to force push or rewrite history.
    pub team_forbids_force_push: bool,
}

const SYSTEM: &str = "You read comments a person left on a pull request opened by CoderHelm, an \
AI that changes code, and decide whether they ask CoderHelm to bring the pull request's branch up \
to date with its base branch. \"rebase\", \"rebase on main\", \"update the branch\", \"update \
branch\", \"sync with main\", \"merge main in\", \"pull in the latest main\" are all that one \
request. Not a request: a description of code (\"this rebases the timestamps\"), a remark about \
someone else's branch, or a statement that they will do it themselves. When it is a request, copy \
the exact words of the request into `quote`, say whether the comments ask for anything else as \
well (`only` is true when updating the branch is all they ask), and whether they ask for a linear \
history in so many words (\"linear history\", \"no merge commits\"). Separately, read the team's \
instructions and say whether they forbid force pushes or rewriting a branch's history.";

fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "asks_update": {"type": "boolean"},
            "quote": {"type": "string"},
            "only": {"type": "boolean"},
            "linear_history": {"type": "boolean"},
            "team_forbids_force_push": {"type": "boolean"}
        },
        "required": ["asks_update", "quote", "only", "linear_history", "team_forbids_force_push"],
        "additionalProperties": false
    })
}

fn prompt(comments: &str, team_instructions: &str) -> String {
    let team = if team_instructions.trim().is_empty() {
        "(none)"
    } else {
        team_instructions.trim()
    };
    format!("## The comments\n{comments}\n\n## The team's instructions\n{team}\n")
}

fn squash(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// The model's answer, kept only when the words it quotes are in the
/// comments.
fn parse(answer: &Value, comments: &str) -> Option<Ask> {
    if !answer["asks_update"].as_bool().unwrap_or(false) {
        return None;
    }
    let quote = answer["quote"].as_str().unwrap_or("").trim().to_string();
    if quote.is_empty() || !squash(comments).contains(&squash(&quote)) {
        return None;
    }
    Some(Ask {
        quote,
        only: answer["only"].as_bool().unwrap_or(false),
        linear_history: answer["linear_history"].as_bool().unwrap_or(false),
        // Doubt keeps history as it is: a missing answer is a "no force push".
        team_forbids_force_push: answer["team_forbids_force_push"].as_bool().unwrap_or(true),
    })
}

/// What the person wrote this round, for the reading.
fn comment_text(msg: &FeedbackMessage) -> String {
    let mut parts = Vec::new();
    if !msg.review_body.trim().is_empty() {
        parts.push(msg.review_body.trim().to_string());
    }
    for c in msg.comments.iter().filter(|c| !c.is_context) {
        if !c.body.trim().is_empty() {
            parts.push(c.body.trim().to_string());
        }
    }
    parts.join("\n---\n")
}

/// Whether this round's comments ask to bring the branch up to date. Only a
/// person's comments are read; automated rounds never ask.
pub(crate) async fn asked(
    state: &WorkerState,
    provider: &ModelProvider,
    msg: &FeedbackMessage,
    usage: &mut TokenUsage,
) -> Option<Ask> {
    let comments = comment_text(msg);
    if comments.is_empty() {
        return None;
    }
    let team =
        super::load_team_instructions(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    match crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        SYSTEM,
        &prompt(&comments, &team),
        &schema(),
        true,
        "low",
        512,
        usage,
    )
    .await
    {
        Ok(a) => parse(&a, &comments),
        Err(e) => {
            warn!(run_id = %msg.run_id, error = %e, "Could not read whether the comment asks to update the branch");
            None
        }
    }
}

/// Whether force pushes are allowed on the branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForcePush {
    Allowed,
    Blocked,
    /// The rules couldn't be read: treated as blocked.
    Unknown,
}

/// What decides how the branch is updated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Rules {
    pub force_push: ForcePush,
    pub team_forbids_force_push: bool,
    /// Every commit on the branch is CoderHelm's (`None`: couldn't tell).
    pub own_branch: Option<bool>,
}

/// Why the base was merged in rather than rebased onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeWhy {
    ForcePushNotAllowed,
    OthersCommits,
    RebaseNotClean,
    BranchMoved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Method {
    Rebase,
    Merge(MergeWhy),
}

/// A rebase only where force pushes are allowed (the branch's protection
/// and rulesets, and the team's own rules) and the branch is CoderHelm's.
pub(crate) fn method(r: &Rules) -> Method {
    if r.team_forbids_force_push || r.force_push != ForcePush::Allowed {
        return Method::Merge(MergeWhy::ForcePushNotAllowed);
    }
    if r.own_branch != Some(true) {
        return Method::Merge(MergeWhy::OthersCommits);
    }
    Method::Rebase
}

/// Whether a ruleset blocks force pushes on the branch (`rules/branches`).
pub(crate) fn rulesets_block_force_push(rules: &Value) -> bool {
    rules
        .as_array()
        .map(|a| {
            a.iter()
                .any(|r| matches!(r["type"].as_str(), Some("non_fast_forward")))
        })
        .unwrap_or(false)
}

/// The force push rule from the rulesets and classic protection: blocked
/// if either blocks it; unknown when either couldn't be read.
pub(crate) fn force_push(
    rulesets: Option<&Value>,
    protected: Option<bool>,
    classic: Option<bool>,
) -> ForcePush {
    match rulesets {
        Some(r) if rulesets_block_force_push(r) => return ForcePush::Blocked,
        Some(_) => {}
        None => return ForcePush::Unknown,
    }
    match (protected, classic) {
        (Some(false), _) => ForcePush::Allowed,
        (Some(true), Some(true)) => ForcePush::Allowed,
        (Some(true), Some(false)) => ForcePush::Blocked,
        _ => ForcePush::Unknown,
    }
}

/// How the update went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    UpToDate {
        head: String,
    },
    Rebased {
        sha: String,
    },
    Merged {
        sha: String,
        why: MergeWhy,
    },
    /// Conflicts that need a person's call; nothing pushed.
    Conflict {
        files: Vec<String>,
    },
    Failed {
        why: String,
    },
}

impl Outcome {
    /// Whether a commit went to the pull request (CI runs on it).
    pub fn pushed(&self) -> bool {
        matches!(self, Outcome::Rebased { .. } | Outcome::Merged { .. })
    }
}

fn short(sha: &str) -> &str {
    &sha[..7.min(sha.len())]
}

/// The reply: one or two lines, the outcome first. `squash_hint` adds the
/// squash-on-merge note (only when they asked for a linear history and the
/// repository allows squash merging).
pub(crate) fn reply(outcome: &Outcome, base: &str, squash_hint: bool) -> String {
    let mut out = match outcome {
        Outcome::UpToDate { head } => {
            format!(
                "Already up to date with {base} ({}); nothing to update.",
                short(head)
            )
        }
        Outcome::Rebased { sha } => format!("Done: rebased onto {base} ({}).", short(sha)),
        Outcome::Merged { sha, why } => {
            let because = match why {
                MergeWhy::ForcePushNotAllowed => "since force pushes aren't allowed here",
                MergeWhy::OthersCommits => {
                    "since the branch has commits from other people that a rebase would rewrite"
                }
                MergeWhy::RebaseNotClean => "since its commits didn't replay cleanly",
                MergeWhy::BranchMoved => "since the branch changed while rebasing",
            };
            format!(
                "Done: updated with {base} as a merge commit ({}), {because}. Same code as a rebase.",
                short(sha)
            )
        }
        Outcome::Conflict { files } => {
            let list = files
                .iter()
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", ");
            if list.is_empty() {
                format!("Couldn't update with {base}: the merge conflicts and needs a decision. Nothing was pushed.")
            } else {
                format!("Couldn't update with {base}: these files conflict and need a decision: {list}. Nothing was pushed.")
            }
        }
        Outcome::Failed { why } => {
            format!("Couldn't update with {base} ({why}). Nothing was pushed.")
        }
    };
    if squash_hint && matches!(outcome, Outcome::Merged { .. }) {
        out.push_str(" Squash on merge gives a linear history.");
    }
    out
}

/// Whether the round's start-of-round update may push. An update the
/// person asked for is the one push they asked for, so a stored "one push"
/// never skips it; the plain conflict merge is an extra push and does.
pub(crate) fn may_update(
    constraints: &super::review_fix::constraints::Constraints,
    ask: Option<&Ask>,
) -> bool {
    constraints.may_update_branch(ask.is_some())
}

/// The branch operations an update needs; GitHub in production.
#[async_trait::async_trait]
pub(crate) trait Branches: Sync {
    async fn head(&self, branch: &str) -> Result<String, Error>;
    /// How many commits of `base` the branch lacks.
    async fn behind(&self, base: &str, branch: &str) -> Result<u64, Error>;
    async fn rebase(&self, branch: &str, base: &str, lease: &str) -> Result<RebaseResult, Error>;
    /// Merges the base in, resolving mechanical conflicts. The new head, or
    /// `None` when conflicts are left.
    async fn merge(&self, branch: &str, base: &str) -> Result<Option<String>, Error>;
    /// Files both sides changed since they parted.
    async fn conflicting_files(&self, branch: &str, base: &str) -> Vec<String>;
}

/// Brings the branch up to date with the base by `method`, in one push. A
/// rebase that can't be done cleanly falls back to a merge.
pub(crate) async fn update(
    ops: &impl Branches,
    branch: &str,
    base: &str,
    method: Method,
) -> Outcome {
    let head = match ops.head(branch).await {
        Ok(h) => h,
        Err(e) => {
            return Outcome::Failed {
                why: format!("the branch couldn't be read: {e}"),
            }
        }
    };
    match ops.behind(base, branch).await {
        Ok(0) => return Outcome::UpToDate { head },
        Ok(_) => {}
        Err(e) => {
            return Outcome::Failed {
                why: format!("the branches couldn't be compared: {e}"),
            }
        }
    }
    let why = match method {
        Method::Merge(why) => why,
        Method::Rebase => match ops.rebase(branch, base, &head).await {
            Ok(RebaseResult::Rebased(sha)) => return Outcome::Rebased { sha },
            Ok(RebaseResult::UpToDate) => return Outcome::UpToDate { head },
            Ok(RebaseResult::NotClean(why)) => {
                info!(
                    branch,
                    why, "Rebase not clean — merging the base in instead"
                );
                MergeWhy::RebaseNotClean
            }
            Ok(RebaseResult::Moved) => MergeWhy::BranchMoved,
            Err(e) => {
                warn!(branch, error = %e, "Rebase failed — merging the base in instead");
                MergeWhy::RebaseNotClean
            }
        },
    };
    match ops.merge(branch, base).await {
        Ok(Some(sha)) => Outcome::Merged { sha, why },
        Ok(None) => Outcome::Conflict {
            files: ops.conflicting_files(branch, base).await,
        },
        Err(e) => Outcome::Failed {
            why: format!("the merge failed: {e}"),
        },
    }
}

/// [`Branches`] on GitHub; merges go through the pass's conflict
/// resolution (mechanical conflicts resolved, checked, one merge commit).
struct OnGithub<'a> {
    state: &'a WorkerState,
    github: &'a GitHubClient,
    provider: &'a ModelProvider,
    ticket: &'a TicketMessage,
    usage: std::sync::Mutex<TokenUsage>,
}

#[async_trait::async_trait]
impl Branches for OnGithub<'_> {
    async fn head(&self, branch: &str) -> Result<String, Error> {
        self.github
            .get_ref(&self.ticket.repo_owner, &self.ticket.repo_name, branch)
            .await
    }

    async fn behind(&self, base: &str, branch: &str) -> Result<u64, Error> {
        let cmp = self
            .github
            .get_diff(
                &self.ticket.repo_owner,
                &self.ticket.repo_name,
                base,
                branch,
            )
            .await?;
        Ok(cmp["behind_by"].as_u64().unwrap_or(0))
    }

    async fn rebase(&self, branch: &str, base: &str, lease: &str) -> Result<RebaseResult, Error> {
        self.github
            .rebase_branch(
                &self.ticket.repo_owner,
                &self.ticket.repo_name,
                branch,
                base,
                lease,
            )
            .await
    }

    async fn merge(&self, branch: &str, base: &str) -> Result<Option<String>, Error> {
        let mut usage = TokenUsage::default();
        let resolved = super::pr::resolve_conflicts(
            self.state,
            self.ticket,
            self.github,
            branch,
            self.provider,
            &mut usage,
        )
        .await;
        self.usage.lock().unwrap().merge(&usage);
        resolved?;
        // Merged when the branch now has all of the base.
        if self.behind(base, branch).await? == 0 {
            Ok(Some(self.head(branch).await?))
        } else {
            Ok(None)
        }
    }

    async fn conflicting_files(&self, branch: &str, base: &str) -> Vec<String> {
        let (o, r) = (&self.ticket.repo_owner, &self.ticket.repo_name);
        let Ok(cmp) = self.github.get_diff(o, r, base, branch).await else {
            return Vec::new();
        };
        let Some(fork) = cmp
            .pointer("/merge_base_commit/sha")
            .and_then(|v| v.as_str())
        else {
            return Vec::new();
        };
        let ours = self.github.compare_changed_files(o, r, fork, branch).await;
        let theirs = self.github.compare_changed_files(o, r, fork, base).await;
        match (ours, theirs) {
            (Ok(ours), Ok(theirs)) => ours.into_iter().filter(|f| theirs.contains(f)).collect(),
            _ => Vec::new(),
        }
    }
}

/// The repository's rules for this branch.
async fn read_rules(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    branch: &str,
    base: &str,
    ask: &Ask,
) -> Rules {
    let rulesets = github.branch_rules(owner, repo, branch).await.ok();
    let protected = github.branch_is_protected(owner, repo, branch).await.ok();
    let classic = if protected == Some(true) {
        github
            .force_pushes_allowed(owner, repo, branch)
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    Rules {
        force_push: force_push(rulesets.as_ref(), protected, classic),
        team_forbids_force_push: ask.team_forbids_force_push,
        own_branch: github
            .only_app_commits(owner, repo, base, branch)
            .await
            .ok(),
    }
}

/// Does the update the person asked for and returns the reply line and
/// whether a commit was pushed.
pub(crate) async fn run(
    state: &WorkerState,
    github: &GitHubClient,
    provider: &ModelProvider,
    ticket: &TicketMessage,
    branch: &str,
    ask: &Ask,
    usage: &mut TokenUsage,
) -> (String, bool) {
    let (owner, repo, base) = (&ticket.repo_owner, &ticket.repo_name, &ticket.base_branch);
    let rules = read_rules(github, owner, repo, branch, base, ask).await;
    let how = method(&rules);
    let ops = OnGithub {
        state,
        github,
        provider,
        ticket,
        usage: std::sync::Mutex::new(TokenUsage::default()),
    };
    let outcome = update(&ops, branch, base, how).await;
    usage.merge(&ops.usage.lock().unwrap());
    info!(branch, ?rules, ?how, ?outcome, "Branch update asked for");
    let squash_hint = ask.linear_history
        && matches!(outcome, Outcome::Merged { .. })
        && github.allows_squash_merge(owner, repo).await == Some(true);
    (reply(&outcome, base, squash_hint), outcome.pushed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A branch on a fake host: what it holds and what was pushed.
    struct Fake {
        head: String,
        behind: u64,
        rebase: Option<RebaseResult>,
        /// The merge's new head, `None` for conflicts left.
        merge: Option<String>,
        conflicts: Vec<String>,
        calls: Mutex<Vec<String>>,
    }

    impl Fake {
        fn new(behind: u64) -> Self {
            Fake {
                head: "aaaaaaa1111".into(),
                behind,
                rebase: None,
                merge: Some("bbbbbbb2222".into()),
                conflicts: vec![],
                calls: Mutex::new(vec![]),
            }
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl Branches for Fake {
        async fn head(&self, _: &str) -> Result<String, Error> {
            Ok(self.head.clone())
        }
        async fn behind(&self, _: &str, _: &str) -> Result<u64, Error> {
            Ok(self.behind)
        }
        async fn rebase(&self, _: &str, _: &str, lease: &str) -> Result<RebaseResult, Error> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("rebase lease={lease}"));
            Ok(self.rebase.clone().unwrap())
        }
        async fn merge(&self, _: &str, _: &str) -> Result<Option<String>, Error> {
            self.calls.lock().unwrap().push("merge".into());
            Ok(self.merge.clone())
        }
        async fn conflicting_files(&self, _: &str, _: &str) -> Vec<String> {
            self.conflicts.clone()
        }
    }

    fn rules(force_push: ForcePush, own: bool) -> Rules {
        Rules {
            force_push,
            team_forbids_force_push: false,
            own_branch: Some(own),
        }
    }

    #[tokio::test]
    async fn force_pushes_not_allowed_merge_the_base_in_and_say_so() {
        // The branch's rules block force pushes: a merge, never a rebase.
        let m = method(&rules(ForcePush::Blocked, true));
        assert_eq!(m, Method::Merge(MergeWhy::ForcePushNotAllowed));
        let fake = Fake::new(3);
        let out = update(&fake, "coderhelm/x", "main", m).await;
        assert_eq!(fake.calls(), vec!["merge"]);
        assert!(out.pushed());
        let text = reply(&out, "main", false);
        assert_eq!(
            text,
            "Done: updated with main as a merge commit (bbbbbbb), since force pushes aren't allowed here. Same code as a rebase."
        );
        // Never hands the work back or talks about tools.
        for banned in ["locally", "force-push", "tool", "please"] {
            assert!(!text.to_lowercase().contains(banned), "{banned}");
        }
        // The team's own rules count the same as the branch's.
        let mut r = rules(ForcePush::Allowed, true);
        r.team_forbids_force_push = true;
        assert_eq!(method(&r), Method::Merge(MergeWhy::ForcePushNotAllowed));
        // Rules that couldn't be read keep history as it is.
        assert_eq!(
            method(&rules(ForcePush::Unknown, true)),
            Method::Merge(MergeWhy::ForcePushNotAllowed)
        );
        // Squash on merge only when they asked for a linear history and the
        // repository allows it.
        assert!(reply(&out, "main", true).ends_with("Squash on merge gives a linear history."));
    }

    #[tokio::test]
    async fn force_pushes_allowed_on_its_own_branch_rebase_with_a_lease() {
        let m = method(&rules(ForcePush::Allowed, true));
        assert_eq!(m, Method::Rebase);
        let mut fake = Fake::new(2);
        fake.rebase = Some(RebaseResult::Rebased("ccccccc3333".into()));
        let out = update(&fake, "coderhelm/x", "main", m).await;
        // One push: the rebase, leased on the head it read; no merge.
        assert_eq!(fake.calls(), vec!["rebase lease=aaaaaaa1111"]);
        assert_eq!(
            reply(&out, "main", true),
            "Done: rebased onto main (ccccccc)."
        );
        // Someone else's commits on it: merged, not rewritten.
        assert_eq!(
            method(&rules(ForcePush::Allowed, false)),
            Method::Merge(MergeWhy::OthersCommits)
        );
        // A rebase that doesn't replay cleanly falls back to one merge.
        let mut fake = Fake::new(2);
        fake.rebase = Some(RebaseResult::NotClean("commit abc conflicts".into()));
        let out = update(&fake, "coderhelm/x", "main", m).await;
        assert_eq!(fake.calls(), vec!["rebase lease=aaaaaaa1111", "merge"]);
        assert_eq!(
            out,
            Outcome::Merged {
                sha: "bbbbbbb2222".into(),
                why: MergeWhy::RebaseNotClean
            }
        );
    }

    #[tokio::test]
    async fn already_up_to_date_pushes_nothing_and_says_so() {
        for m in [Method::Rebase, Method::Merge(MergeWhy::ForcePushNotAllowed)] {
            let fake = Fake::new(0);
            let out = update(&fake, "coderhelm/x", "main", m).await;
            assert!(fake.calls().is_empty());
            assert!(!out.pushed());
            assert_eq!(
                reply(&out, "main", true),
                "Already up to date with main (aaaaaaa); nothing to update."
            );
        }
    }

    #[tokio::test]
    async fn conflicts_left_after_resolution_list_the_files_and_push_nothing() {
        let mut fake = Fake::new(4);
        fake.merge = None;
        fake.conflicts = vec!["src/a.rs".into(), "src/b.rs".into()];
        let out = update(
            &fake,
            "coderhelm/x",
            "main",
            Method::Merge(MergeWhy::ForcePushNotAllowed),
        )
        .await;
        assert!(!out.pushed());
        assert_eq!(
            reply(&out, "main", false),
            "Couldn't update with main: these files conflict and need a decision: `src/a.rs`, `src/b.rs`. Nothing was pushed."
        );
    }

    #[test]
    fn an_asked_update_runs_even_when_the_requester_asked_for_one_push() {
        let one_push = super::super::review_fix::constraints::Constraints {
            single_push: true,
            ..Default::default()
        };
        let ask = Ask {
            quote: "rebase".into(),
            only: true,
            ..Default::default()
        };
        // The update they asked for is the one push.
        assert!(may_update(&one_push, Some(&ask)));
        // The plain conflict merge is an extra push: skipped.
        assert!(!may_update(&one_push, None));
        assert!(may_update(&Default::default(), None));
    }

    #[test]
    fn the_branch_s_rules_decide_force_pushes() {
        let none = json!([]);
        let blocked = json!([{"type": "non_fast_forward"}, {"type": "pull_request"}]);
        assert_eq!(
            force_push(Some(&none), Some(false), None),
            ForcePush::Allowed
        );
        assert_eq!(
            force_push(Some(&blocked), Some(false), None),
            ForcePush::Blocked
        );
        assert_eq!(
            force_push(Some(&none), Some(true), Some(false)),
            ForcePush::Blocked
        );
        assert_eq!(
            force_push(Some(&none), Some(true), Some(true)),
            ForcePush::Allowed
        );
        assert_eq!(
            force_push(Some(&none), Some(true), None),
            ForcePush::Unknown
        );
        assert_eq!(force_push(None, Some(false), None), ForcePush::Unknown);
    }

    #[test]
    fn rebase_update_branch_and_sync_are_one_request_quoted_from_the_comment() {
        let comment = "@coderhelm rebase";
        let a = parse(
            &json!({"asks_update": true, "quote": "rebase", "only": true, "linear_history": false, "team_forbids_force_push": false}),
            comment,
        )
        .unwrap();
        assert!(a.only);
        assert!(!a.team_forbids_force_push);
        // Words that aren't in the comment are not a request.
        assert!(parse(
            &json!({"asks_update": true, "quote": "sync with main", "only": true, "linear_history": false, "team_forbids_force_push": false}),
            comment,
        )
        .is_none());
        assert!(parse(
            &json!({"asks_update": false, "quote": "", "only": false, "linear_history": false, "team_forbids_force_push": false}),
            comment,
        )
        .is_none());
        // Whitespace and case don't matter.
        assert!(parse(
            &json!({"asks_update": true, "quote": "Sync  with main", "only": false, "linear_history": true, "team_forbids_force_push": true}),
            "please sync with\nmain and fix the typo",
        )
        .is_some_and(|a| !a.only && a.linear_history && a.team_forbids_force_push));
    }
}
