//! Stale pull request cleanup: the settings, what counts as activity, and
//! what the sweep does with each open pull request. Shared by the gateway
//! (settings, the dashboard list, `@coderhelm keep open`) and the worker
//! (the sweep, `worker::passes::stale_prs`).
//!
//! The rules:
//!
//! - Off until a team turns it on, and then a dry run (a preview, no writes)
//!   until someone turns the dry run off.
//! - A pull request goes stale after [`StaleConfig::stale_days`] with no
//!   activity from a person. Coderhelm adds the stale label and posts the
//!   warning, then closes it [`StaleConfig::close_days`] later if nothing
//!   happened in between. Closing keeps the branch unless the team chose to
//!   delete it, and never deletes a fork's branch.
//! - Activity is read from the pull request's timeline, never from
//!   `updated_at`: Coderhelm's own label and comment (and every other bot's)
//!   move `updated_at`, so a tool counting it un-stales its own warning or
//!   keeps a pull request alive with bot noise. Commits count (Coderhelm's
//!   too: they are work on the branch); a person's comment, review, review
//!   comment, label change, review request, draft change or reopen counts.
//!   Other bots' commits, CI runs and pushes to the base branch don't.
//! - A pull request with new activity after the warning, or whose stale
//!   label a person removed, is un-staled: the label comes off on its own.
//! - Exempt: a label from [`StaleConfig::exempt_labels`], a draft (when
//!   [`StaleConfig::exempt_drafts`]), an exempt author, a pull request kept
//!   open from the dashboard or with `@coderhelm keep open`, one a Coderhelm
//!   run is working on, and one Coderhelm promised to come back to on its
//!   own (a waiting draft hold, auto-merge waiting for CI).
//!
//! Records live in the settings table under the team (`pk` = team id):
//! [`TEAM_SK`] and [`repo_sk`] hold the settings (a repository's own
//! settings replace the team's whole), [`exempt_sk`] a kept-open pull
//! request, [`log_sk`] every action taken, [`preview_sk`] the latest plan.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The team's settings (`pk` = team id).
pub const TEAM_SK: &str = "STALE_PR_CONFIG#TEAM";
/// Every settings record starts with this.
pub const CONFIG_PREFIX: &str = "STALE_PR_CONFIG#";
const REPO_PREFIX: &str = "STALE_PR_CONFIG#REPO#";
/// A kept-open pull request.
pub const EXEMPT_PREFIX: &str = "STALE_PR_EXEMPT#";
/// One action the sweep took.
pub const LOG_PREFIX: &str = "STALE_PR_LOG#";
/// The latest plan for one repository.
pub const PREVIEW_PREFIX: &str = "STALE_PR_PREVIEW#";
/// The attribute holding a settings record's JSON.
pub const ATTR_CONFIG: &str = "config";
/// How long an action stays in the log.
pub const LOG_TTL_DAYS: i64 = 90;
/// Written into every comment Coderhelm posts for this, so its own comments
/// are recognized.
pub const MARKER: &str = "<!-- coderhelm:stale-pr -->";

pub fn repo_sk(repo: &str) -> String {
    format!("{REPO_PREFIX}{repo}")
}

/// The repository a settings sort key names, `None` for the team's.
pub fn repo_of_sk(sk: &str) -> Option<&str> {
    sk.strip_prefix(REPO_PREFIX).filter(|r| !r.is_empty())
}

pub fn exempt_sk(repo: &str, pr: u64) -> String {
    format!("{EXEMPT_PREFIX}{repo}#{pr:06}")
}

pub fn exempt_prefix(repo: &str) -> String {
    format!("{EXEMPT_PREFIX}{repo}#")
}

pub fn log_sk(at: &DateTime<Utc>, repo: &str, pr: u64) -> String {
    format!(
        "{LOG_PREFIX}{}#{repo}#{pr:06}",
        at.format("%Y-%m-%dT%H:%M:%S%.3fZ")
    )
}

pub fn preview_sk(repo: &str) -> String {
    format!("{PREVIEW_PREFIX}{repo}")
}

/// Which pull requests the cleanup looks at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// Every open pull request in the repository.
    #[default]
    All,
    /// Only the pull requests Coderhelm opened.
    Own,
}

pub const DEFAULT_STALE_DAYS: u32 = 21;
pub const DEFAULT_CLOSE_DAYS: u32 = 7;
pub const DEFAULT_LABEL: &str = "stale";
pub const DEFAULT_MAX_CLOSES: u32 = 10;
pub const DEFAULT_EXEMPT_LABELS: [&str; 4] = ["keep-open", "pinned", "security", "do-not-close"];

pub const DEFAULT_WARN_MESSAGE: &str = "This pull request has had no activity for {stale_days} days, so Coderhelm marked it stale. It will be closed in {close_days} days if nothing happens here.\n\nTo keep it open, push a commit, leave a comment, remove the `{label}` label, or comment `@coderhelm keep open`.";

pub const DEFAULT_CLOSE_MESSAGE: &str = "Coderhelm closed this pull request: it was marked stale {close_days} days ago and nothing happened since. Nothing is lost: the commits and this conversation stay here.\n\nTo pick it back up, reopen this pull request. It won't be marked stale again until it has gone {stale_days} days without activity.";

/// A team's (or one repository's) stale pull request settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StaleConfig {
    pub enabled: bool,
    /// Days with no activity before a pull request is marked stale.
    pub stale_days: u32,
    /// Days after the warning before it is closed.
    pub close_days: u32,
    /// The label that marks it.
    pub label: String,
    /// The warning comment. `{stale_days}`, `{close_days}` and `{label}` are
    /// filled in.
    pub warn_message: String,
    /// The comment left when it is closed (same placeholders).
    pub close_message: String,
    /// Pull requests with any of these labels are never marked.
    pub exempt_labels: Vec<String>,
    /// Drafts are never marked.
    pub exempt_drafts: bool,
    /// Pull requests by these GitHub logins (people or bots) are never marked.
    pub exempt_authors: Vec<String>,
    pub scope: Scope,
    /// Delete the branch when closing. Never for a fork, never for a branch
    /// another open pull request is based on.
    pub delete_branch: bool,
    /// At most this many pull requests closed per repository per sweep.
    pub max_closes_per_run: u32,
    /// Work out what would happen and show it, change nothing on GitHub.
    pub dry_run: bool,
}

impl Default for StaleConfig {
    fn default() -> Self {
        StaleConfig {
            enabled: false,
            stale_days: DEFAULT_STALE_DAYS,
            close_days: DEFAULT_CLOSE_DAYS,
            label: DEFAULT_LABEL.to_string(),
            warn_message: DEFAULT_WARN_MESSAGE.to_string(),
            close_message: DEFAULT_CLOSE_MESSAGE.to_string(),
            exempt_labels: DEFAULT_EXEMPT_LABELS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            exempt_drafts: true,
            exempt_authors: vec![],
            scope: Scope::All,
            delete_branch: false,
            max_closes_per_run: DEFAULT_MAX_CLOSES,
            dry_run: true,
        }
    }
}

const MAX_MESSAGE: usize = 2_000;
const MAX_LABEL: usize = 50;
const MAX_LIST: usize = 30;

fn clean_label(s: &str) -> String {
    let s: String = s.trim().chars().filter(|c| !c.is_control()).collect();
    crate::truncate_str(&s, MAX_LABEL).trim().to_string()
}

fn clean_list(items: &[String], max_len: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for i in items {
        let v = clean_label(i);
        let v = crate::truncate_str(&v, max_len).to_string();
        if !v.is_empty() && !out.iter().any(|o| o.eq_ignore_ascii_case(&v)) {
            out.push(v);
        }
        if out.len() >= MAX_LIST {
            break;
        }
    }
    out
}

impl StaleConfig {
    /// The settings as kept: every number in range, the label and messages
    /// never empty (an empty one takes the default), lists trimmed and
    /// without duplicates.
    pub fn sanitized(mut self) -> Self {
        let d = StaleConfig::default();
        self.stale_days = self.stale_days.clamp(1, 365);
        self.close_days = self.close_days.clamp(1, 90);
        self.label = clean_label(&self.label);
        if self.label.is_empty() || self.label.contains(',') {
            self.label = d.label;
        }
        for (m, def) in [
            (&mut self.warn_message, &d.warn_message),
            (&mut self.close_message, &d.close_message),
        ] {
            let t = m.trim();
            *m = if t.is_empty() {
                def.clone()
            } else {
                crate::truncate_str(t, MAX_MESSAGE).to_string()
            };
        }
        self.exempt_labels = clean_list(&self.exempt_labels, MAX_LABEL);
        self.exempt_authors = clean_list(&self.exempt_authors, 39 + "[bot]".len());
        self.max_closes_per_run = self.max_closes_per_run.clamp(1, 50);
        self
    }

    /// From a settings record's JSON; `None` when it can't be read.
    pub fn from_json(s: &str) -> Option<Self> {
        serde_json::from_str::<StaleConfig>(s)
            .ok()
            .map(StaleConfig::sanitized)
    }

    fn fill(&self, text: &str) -> String {
        text.replace("{stale_days}", &self.stale_days.to_string())
            .replace("{close_days}", &self.close_days.to_string())
            .replace("{label}", &self.label)
    }

    /// The warning comment, with its marker.
    pub fn warn_comment(&self) -> String {
        format!("{}\n\n{MARKER}", self.fill(&self.warn_message))
    }

    /// The closing comment, with its marker. `branch_deleted`: the branch
    /// went with it, and the comment says how to get it back.
    pub fn close_comment(&self, branch_deleted: bool) -> String {
        let mut body = self.fill(&self.close_message);
        if branch_deleted {
            body.push_str(
                "\n\nThe branch was deleted with it. Use **Restore branch** on this pull request to bring it back before reopening.",
            );
        }
        format!("{body}\n\n{MARKER}")
    }

    pub fn exempt_label<'a>(&self, labels: &'a [String]) -> Option<&'a str> {
        labels
            .iter()
            .find(|l| self.exempt_labels.iter().any(|e| e.eq_ignore_ascii_case(l)))
            .map(String::as_str)
    }

    pub fn author_exempt(&self, login: &str) -> bool {
        self.exempt_authors
            .iter()
            .any(|a| a.eq_ignore_ascii_case(login.trim_start_matches('@')))
    }
}

/// The settings a repository uses: its own when it has them, else the team's,
/// else the defaults. A repository's settings replace the team's whole.
pub fn effective(team: Option<&StaleConfig>, repo: Option<&StaleConfig>) -> StaleConfig {
    repo.or(team).cloned().unwrap_or_default()
}

// ── Who did it ───────────────────────────────────────────────────────

/// Coderhelm's own account.
pub fn is_own(login: &str) -> bool {
    login.to_ascii_lowercase().contains("coderhelm")
}

/// A bot account: GitHub's `type` says so, or the login ends in `[bot]`.
pub fn is_bot(user: &Value) -> bool {
    let login = user["login"].as_str().unwrap_or("");
    user["type"].as_str() == Some("Bot") || login.to_ascii_lowercase().ends_with("[bot]")
}

/// Activity by a person: not a bot, not Coderhelm.
fn by_person(user: &Value) -> bool {
    let login = user["login"].as_str().unwrap_or("");
    !login.is_empty() && !is_bot(user) && !is_own(login)
}

/// A commit's author is a bot other than Coderhelm (GitHub's bot addresses
/// and names end in `[bot]`).
fn bot_commit(ev: &Value) -> bool {
    let who = |k: &str| {
        let name = ev[k]["name"].as_str().unwrap_or("").to_ascii_lowercase();
        let email = ev[k]["email"].as_str().unwrap_or("").to_ascii_lowercase();
        (name, email)
    };
    let (name, email) = who("author");
    if is_own(&name) || is_own(&email) {
        return false;
    }
    name.ends_with("[bot]") || email.contains("[bot]@")
}

fn time(v: &Value) -> Option<DateTime<Utc>> {
    v.as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))
}

/// The last thing that counts as activity on a pull request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Activity {
    pub at: DateTime<Utc>,
    /// What it was, in plain words ("a comment").
    pub what: String,
    /// Who, when known (a GitHub login or a commit author's name).
    pub by: String,
}

/// The newest activity from `pr` (the pull request) and its `timeline`
/// (GitHub's issue timeline, oldest first). The pull request being opened
/// is the floor. `label` is the stale label: adding it is never activity.
pub fn last_activity(pr: &Value, timeline: &[Value], label: &str) -> Activity {
    let mut best = Activity {
        at: time(&pr["created_at"]).unwrap_or(DateTime::<Utc>::MIN_UTC),
        what: "opened".to_string(),
        by: pr["user"]["login"].as_str().unwrap_or("").to_string(),
    };
    let mut consider = |at: Option<DateTime<Utc>>, what: &str, by: &str| {
        if let Some(at) = at {
            if at > best.at {
                best = Activity {
                    at,
                    what: what.to_string(),
                    by: by.to_string(),
                };
            }
        }
    };
    for ev in timeline {
        let kind = ev["event"].as_str().unwrap_or("");
        let actor = if ev["actor"].is_object() {
            &ev["actor"]
        } else {
            &ev["user"]
        };
        let login = actor["login"].as_str().unwrap_or("");
        match kind {
            "committed" => {
                if !bot_commit(ev) {
                    let at = time(&ev["committer"]["date"]).or_else(|| time(&ev["author"]["date"]));
                    consider(at, "a commit", ev["author"]["name"].as_str().unwrap_or(""));
                }
            }
            "head_ref_force_pushed" => {
                if !is_bot(actor) || is_own(login) {
                    consider(time(&ev["created_at"]), "a force push", login);
                }
            }
            "commented" if by_person(actor) => {
                consider(time(&ev["created_at"]), "a comment", login);
            }
            "reviewed" if by_person(actor) => {
                consider(time(&ev["submitted_at"]), "a review", login);
            }
            "line-commented" => {
                for c in ev["comments"].as_array().into_iter().flatten() {
                    if by_person(&c["user"]) {
                        consider(
                            time(&c["created_at"]),
                            "a review comment",
                            c["user"]["login"].as_str().unwrap_or(""),
                        );
                    }
                }
            }
            "labeled" if by_person(actor) => {
                let name = ev["label"]["name"].as_str().unwrap_or("");
                if !name.eq_ignore_ascii_case(label) {
                    consider(time(&ev["created_at"]), "a label change", login);
                }
            }
            "unlabeled" if by_person(actor) => {
                let name = ev["label"]["name"].as_str().unwrap_or("");
                let what = if name.eq_ignore_ascii_case(label) {
                    "the stale label removed"
                } else {
                    "a label change"
                };
                consider(time(&ev["created_at"]), what, login);
            }
            "ready_for_review" | "convert_to_draft" | "reopened" | "review_requested"
            | "assigned" | "renamed" | "milestoned"
                if by_person(actor) =>
            {
                consider(time(&ev["created_at"]), "an update", login);
            }
            _ => {}
        }
    }
    best
}

/// When the stale label went on (the newest `labeled` event for it) and
/// whether Coderhelm put it there.
pub fn marked(timeline: &[Value], label: &str) -> Option<(DateTime<Utc>, bool)> {
    timeline
        .iter()
        .filter(|ev| {
            ev["event"].as_str() == Some("labeled")
                && ev["label"]["name"]
                    .as_str()
                    .is_some_and(|n| n.eq_ignore_ascii_case(label))
        })
        .filter_map(|ev| {
            let at = time(&ev["created_at"])?;
            Some((at, is_own(ev["actor"]["login"].as_str().unwrap_or(""))))
        })
        .max_by_key(|(at, _)| *at)
}

// ── What to do ───────────────────────────────────────────────────────

/// Why a pull request is left alone even though it is quiet, in plain words;
/// `None` when nothing exempts it. Run and hold reasons are looked up by the
/// worker and passed in `held`.
pub struct Exemption<'a> {
    pub cfg: &'a StaleConfig,
    pub draft: bool,
    pub labels: &'a [String],
    pub author: &'a str,
    /// Kept open by a person (dashboard or comment): who.
    pub kept_open_by: Option<&'a str>,
    /// Coderhelm is working on it, or promised to come back to it.
    pub held: Option<&'a str>,
}

impl Exemption<'_> {
    pub fn reason(&self) -> Option<String> {
        if let Some(by) = self.kept_open_by {
            return Some(if by.is_empty() {
                "kept open".to_string()
            } else {
                format!("kept open by {by}")
            });
        }
        if let Some(why) = self.held {
            return Some(why.to_string());
        }
        if self.cfg.scope == Scope::Own && !is_own(self.author) {
            return Some("not opened by Coderhelm".to_string());
        }
        if self.draft && self.cfg.exempt_drafts {
            return Some("a draft".to_string());
        }
        if let Some(l) = self.cfg.exempt_label(self.labels) {
            return Some(format!("labelled {l}"));
        }
        if self.cfg.author_exempt(self.author) {
            return Some(format!("opened by {}, who is exempt", self.author));
        }
        None
    }
}

/// What one pull request needs now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Nothing: active, or exempt and not marked.
    Leave { why: String },
    /// Quiet for long enough: add the label and post the warning.
    Mark,
    /// Marked, and activity came back (or it became exempt): take the label off.
    Unmark { why: String },
    /// Marked and still quiet: closes at `closes_at`.
    Wait { closes_at: DateTime<Utc> },
    /// Marked and the grace period is over: close it.
    Close,
}

impl Plan {
    pub fn action(&self) -> &'static str {
        match self {
            Plan::Leave { .. } => "leave",
            Plan::Mark => "mark",
            Plan::Unmark { .. } => "unmark",
            Plan::Wait { .. } => "wait",
            Plan::Close => "close",
        }
    }
}

/// What the sweep knows about one pull request.
pub struct Facts {
    pub now: DateTime<Utc>,
    pub last: Activity,
    /// The stale label is on it now: when it went on and whether by Coderhelm.
    pub marked: Option<(DateTime<Utc>, bool)>,
    /// Why it is exempt, if it is.
    pub exempt: Option<String>,
}

/// The one decision. Pure; the sweep acts on it.
pub fn decide(cfg: &StaleConfig, f: &Facts) -> Plan {
    if let Some(why) = &f.exempt {
        return match f.marked {
            // Only Coderhelm's own label is taken off for an exemption: a
            // label a person put on stays theirs.
            Some((_, true)) => Plan::Unmark {
                why: format!("it is {why}"),
            },
            _ => Plan::Leave {
                why: format!("it is {why}"),
            },
        };
    }
    match f.marked {
        Some((at, _)) => {
            if f.last.at > at {
                Plan::Unmark {
                    why: format!("{} after it was marked", f.last.what),
                }
            } else {
                let closes_at = at + Duration::days(cfg.close_days as i64);
                if f.now >= closes_at {
                    Plan::Close
                } else {
                    Plan::Wait { closes_at }
                }
            }
        }
        None => {
            if f.now - f.last.at >= Duration::days(cfg.stale_days as i64) {
                Plan::Mark
            } else {
                Plan::Leave {
                    why: "active".to_string(),
                }
            }
        }
    }
}

/// Whether a branch may be deleted when its pull request is closed: the
/// team asked for it, the branch is in the same repository (never a fork's),
/// it isn't the default branch, and no other open pull request is based on
/// it (a stacked change).
pub fn may_delete_branch(cfg: &StaleConfig, pr: &Value, open_prs: &[Value]) -> bool {
    if !cfg.delete_branch {
        return false;
    }
    let head_repo = pr["head"]["repo"]["full_name"].as_str().unwrap_or("");
    let base_repo = pr["base"]["repo"]["full_name"].as_str().unwrap_or("");
    let branch = pr["head"]["ref"].as_str().unwrap_or("");
    let default = pr["base"]["repo"]["default_branch"].as_str().unwrap_or("");
    if head_repo.is_empty() || !head_repo.eq_ignore_ascii_case(base_repo) || branch.is_empty() {
        return false;
    }
    if branch == default || pr["head"]["repo"]["fork"].as_bool() == Some(true) {
        return false;
    }
    !open_prs
        .iter()
        .any(|o| o["number"] != pr["number"] && o["base"]["ref"].as_str() == Some(branch))
}

/// `@coderhelm keep open` (and its plain variants), the words after the
/// mention. "keep this open", "don't close", "do not close", "keep open
/// please".
pub fn is_keep_open(text: &str) -> bool {
    let t = text
        .trim()
        .trim_end_matches(['.', '!'])
        .trim()
        .to_lowercase()
        .replace('’', "'");
    let t = t.split_whitespace().collect::<Vec<_>>().join(" ");
    let t = t.strip_prefix("please ").unwrap_or(&t);
    let t = t.strip_suffix(" please").unwrap_or(t);
    matches!(
        t,
        "keep open"
            | "keep this open"
            | "keep it open"
            | "keep this pr open"
            | "keep this pull request open"
            | "don't close"
            | "dont close"
            | "do not close"
            | "don't close this"
            | "do not close this"
            | "not stale"
    )
}

/// One entry of the dashboard's plan for a repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PlanItem {
    pub pr: u64,
    pub title: String,
    pub author: String,
    pub url: String,
    /// `mark`, `close`, `unmark`, `wait` or `exempt`.
    pub action: String,
    /// Why, in plain words.
    pub why: String,
    /// The last activity (RFC 3339) and what it was.
    pub last_activity: String,
    pub last_what: String,
    /// When it happens next (RFC 3339): closes at, or would be marked at.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub next_at: String,
}

/// An action the sweep took, as logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Marked,
    Closed,
    Unstaled,
    KeptOpen,
    AllowedToClose,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Marked => "marked",
            Action::Closed => "closed",
            Action::Unstaled => "unstaled",
            Action::KeptOpen => "kept_open",
            Action::AllowedToClose => "allowed_to_close",
        }
    }
}

/// The reason a closed run shows.
pub fn run_close_reason(pr: u64, stale_days: u32, close_days: u32) -> String {
    format!(
        "Closed as stale: pull request #{pr} had no activity for {} days",
        stale_days + close_days
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn pr() -> Value {
        json!({"number": 7, "created_at": "2026-09-01T00:00:00Z", "user": {"login": "ana", "type": "User"}})
    }

    fn person(login: &str) -> Value {
        json!({"login": login, "type": "User"})
    }

    fn bot(login: &str) -> Value {
        json!({"login": login, "type": "Bot"})
    }

    #[test]
    fn defaults_are_off_and_a_dry_run() {
        let c = StaleConfig::default();
        assert!(!c.enabled);
        assert!(c.dry_run);
        assert_eq!((c.stale_days, c.close_days), (21, 7));
        assert!(c.exempt_drafts);
        assert!(!c.delete_branch);
        assert_eq!(
            c.exempt_labels,
            vec!["keep-open", "pinned", "security", "do-not-close"]
        );
        // A record missing fields takes the defaults for them.
        let c = StaleConfig::from_json(r#"{"enabled":true}"#).unwrap();
        assert!(c.enabled && c.dry_run && c.stale_days == 21);
    }

    #[test]
    fn settings_are_kept_in_range() {
        let c = StaleConfig {
            stale_days: 0,
            close_days: 1000,
            label: "  ".into(),
            warn_message: " ".into(),
            exempt_labels: vec![" pinned ".into(), "PINNED".into(), "".into()],
            max_closes_per_run: 0,
            ..Default::default()
        }
        .sanitized();
        assert_eq!(
            (c.stale_days, c.close_days, c.max_closes_per_run),
            (1, 90, 1)
        );
        assert_eq!(c.label, "stale");
        assert_eq!(c.warn_message, DEFAULT_WARN_MESSAGE);
        assert_eq!(c.exempt_labels, vec!["pinned"]);
    }

    #[test]
    fn a_repository_override_replaces_the_team_settings() {
        let team = StaleConfig {
            enabled: true,
            stale_days: 30,
            ..Default::default()
        };
        let repo = StaleConfig {
            enabled: false,
            ..Default::default()
        };
        assert_eq!(effective(Some(&team), None).stale_days, 30);
        assert!(!effective(Some(&team), Some(&repo)).enabled);
        assert_eq!(effective(None, None), StaleConfig::default());
    }

    #[test]
    fn messages_fill_in_their_numbers_and_say_how_to_reopen() {
        let c = StaleConfig::default();
        let w = c.warn_comment();
        assert!(w.contains("21 days") && w.contains("7 days") && w.contains("`stale`"));
        assert!(w.contains("@coderhelm keep open") && w.ends_with(MARKER));
        let close = c.close_comment(false);
        assert!(close.contains("reopen this pull request"));
        assert!(!close.contains("deleted"));
        assert!(c.close_comment(true).contains("Restore branch"));
        for text in [w, close] {
            assert!(!text.contains('→') && !text.to_lowercase().contains("phase"));
        }
    }

    #[test]
    fn coderhelm_and_bot_comments_never_count_as_activity() {
        let tl = vec![
            json!({"event": "commented", "actor": person("ana"), "created_at": "2026-09-05T00:00:00Z"}),
            // Coderhelm's own warning and label, a CI bot's comment.
            json!({"event": "labeled", "actor": bot("coderhelm[bot]"), "label": {"name": "stale"}, "created_at": "2026-09-30T00:00:00Z"}),
            json!({"event": "commented", "actor": bot("coderhelm[bot]"), "created_at": "2026-09-30T00:00:01Z"}),
            json!({"event": "commented", "actor": bot("vercel[bot]"), "created_at": "2026-10-02T00:00:00Z"}),
            json!({"event": "reviewed", "user": bot("coderhelm[bot]"), "submitted_at": "2026-10-03T00:00:00Z"}),
            json!({"event": "committed", "author": {"name": "dependabot[bot]", "email": "49699333+dependabot[bot]@users.noreply.github.com", "date": "2026-10-04T00:00:00Z"}, "committer": {"date": "2026-10-04T00:00:00Z"}}),
        ];
        let a = last_activity(&pr(), &tl, "stale");
        assert_eq!(a.at, t("2026-09-05T00:00:00Z"));
        assert_eq!(a.what, "a comment");
        assert_eq!(a.by, "ana");
    }

    #[test]
    fn commits_reviews_and_review_comments_count() {
        let commit = json!({"event": "committed", "author": {"name": "Ana", "email": "ana@example.com", "date": "2026-09-10T00:00:00Z"}, "committer": {"date": "2026-09-11T00:00:00Z"}});
        assert_eq!(
            last_activity(&pr(), &[commit], "stale").at,
            t("2026-09-11T00:00:00Z")
        );
        // Coderhelm's commits are work on the branch.
        let own = json!({"event": "committed", "author": {"name": "coderhelm[bot]", "email": "1+coderhelm[bot]@users.noreply.github.com"}, "committer": {"date": "2026-09-12T00:00:00Z"}});
        assert_eq!(last_activity(&pr(), &[own], "stale").what, "a commit");
        let review = json!({"event": "reviewed", "user": person("bo"), "submitted_at": "2026-09-13T00:00:00Z"});
        assert_eq!(last_activity(&pr(), &[review], "stale").what, "a review");
        let line = json!({"event": "line-commented", "comments": [
            {"user": person("bo"), "created_at": "2026-09-14T00:00:00Z"},
            {"user": bot("coderhelm[bot]"), "created_at": "2026-09-20T00:00:00Z"}
        ]});
        let a = last_activity(&pr(), &[line], "stale");
        assert_eq!(
            (a.at, a.what.as_str()),
            (t("2026-09-14T00:00:00Z"), "a review comment")
        );
        // Removing the stale label is activity; a person adding it is not.
        let tl = vec![
            json!({"event": "labeled", "actor": person("bo"), "label": {"name": "Stale"}, "created_at": "2026-09-15T00:00:00Z"}),
        ];
        assert_eq!(last_activity(&pr(), &tl, "stale").what, "opened");
        let tl = vec![
            json!({"event": "unlabeled", "actor": person("bo"), "label": {"name": "stale"}, "created_at": "2026-09-16T00:00:00Z"}),
        ];
        assert_eq!(
            last_activity(&pr(), &tl, "stale").what,
            "the stale label removed"
        );
    }

    #[test]
    fn the_label_time_and_who_put_it_on() {
        let tl = vec![
            json!({"event": "labeled", "actor": person("bo"), "label": {"name": "stale"}, "created_at": "2026-09-01T00:00:00Z"}),
            json!({"event": "labeled", "actor": bot("coderhelm[bot]"), "label": {"name": "stale"}, "created_at": "2026-09-22T00:00:00Z"}),
            json!({"event": "labeled", "actor": person("bo"), "label": {"name": "bug"}, "created_at": "2026-09-23T00:00:00Z"}),
        ];
        assert_eq!(
            marked(&tl, "stale"),
            Some((t("2026-09-22T00:00:00Z"), true))
        );
        assert_eq!(marked(&tl, "other"), None);
    }

    fn facts(now: &str, last: &str, marked: Option<(&str, bool)>) -> Facts {
        Facts {
            now: t(now),
            last: Activity {
                at: t(last),
                what: "a comment".into(),
                by: "ana".into(),
            },
            marked: marked.map(|(a, own)| (t(a), own)),
            exempt: None,
        }
    }

    #[test]
    fn warned_after_the_quiet_days_then_closed_after_the_grace() {
        let c = StaleConfig::default();
        // 20 days quiet: left alone. 21: marked.
        assert_eq!(
            decide(
                &c,
                &facts("2026-09-21T00:00:00Z", "2026-09-01T00:00:00Z", None)
            )
            .action(),
            "leave"
        );
        assert_eq!(
            decide(
                &c,
                &facts("2026-09-22T00:00:00Z", "2026-09-01T00:00:00Z", None)
            ),
            Plan::Mark
        );
        // Marked on the 22nd: waits 7 days, then closes.
        let marked = Some(("2026-09-22T00:00:00Z", true));
        assert_eq!(
            decide(
                &c,
                &facts("2026-09-28T23:00:00Z", "2026-09-01T00:00:00Z", marked)
            ),
            Plan::Wait {
                closes_at: t("2026-09-29T00:00:00Z")
            }
        );
        assert_eq!(
            decide(
                &c,
                &facts("2026-09-29T00:00:00Z", "2026-09-01T00:00:00Z", marked)
            ),
            Plan::Close
        );
    }

    #[test]
    fn activity_after_the_warning_unstales() {
        let c = StaleConfig::default();
        let p = decide(
            &c,
            &facts(
                "2026-10-10T00:00:00Z",
                "2026-09-25T00:00:00Z",
                Some(("2026-09-22T00:00:00Z", true)),
            ),
        );
        assert_eq!(
            p,
            Plan::Unmark {
                why: "a comment after it was marked".into()
            }
        );
    }

    #[test]
    fn exemptions_keep_it_open_and_take_only_our_label_off() {
        let c = StaleConfig::default();
        let labels = vec!["Pinned".to_string()];
        let ex = |draft: bool,
                  labels: &[String],
                  author: &str,
                  kept: Option<&str>,
                  held: Option<&str>| {
            Exemption {
                cfg: &c,
                draft,
                labels,
                author,
                kept_open_by: kept,
                held,
            }
            .reason()
        };
        assert_eq!(
            ex(false, &labels, "ana", None, None).as_deref(),
            Some("labelled Pinned")
        );
        assert_eq!(ex(true, &[], "ana", None, None).as_deref(), Some("a draft"));
        assert_eq!(
            ex(false, &[], "ana", Some("bo"), None).as_deref(),
            Some("kept open by bo")
        );
        assert_eq!(
            ex(
                false,
                &[],
                "ana",
                None,
                Some("a Coderhelm run is working on it")
            )
            .as_deref(),
            Some("a Coderhelm run is working on it")
        );
        assert_eq!(ex(false, &[], "ana", None, None), None);
        let authors = StaleConfig {
            exempt_authors: vec!["renovate[bot]".into()],
            ..Default::default()
        };
        assert!(Exemption {
            cfg: &authors,
            draft: false,
            labels: &[],
            author: "Renovate[bot]",
            kept_open_by: None,
            held: None
        }
        .reason()
        .is_some());
        let drafts_too = StaleConfig {
            exempt_drafts: false,
            ..Default::default()
        };
        assert!(Exemption {
            cfg: &drafts_too,
            draft: true,
            labels: &[],
            author: "ana",
            kept_open_by: None,
            held: None
        }
        .reason()
        .is_none());
        let own = StaleConfig {
            scope: Scope::Own,
            ..Default::default()
        };
        assert!(Exemption {
            cfg: &own,
            draft: false,
            labels: &[],
            author: "ana",
            kept_open_by: None,
            held: None
        }
        .reason()
        .is_some());
        assert!(Exemption {
            cfg: &own,
            draft: false,
            labels: &[],
            author: "coderhelm[bot]",
            kept_open_by: None,
            held: None
        }
        .reason()
        .is_none());

        let mut f = facts(
            "2026-10-10T00:00:00Z",
            "2026-09-01T00:00:00Z",
            Some(("2026-09-22T00:00:00Z", true)),
        );
        f.exempt = Some("a draft".into());
        assert_eq!(decide(&c, &f).action(), "unmark");
        // A person's own stale label stays.
        f.marked = Some((t("2026-09-22T00:00:00Z"), false));
        assert_eq!(decide(&c, &f).action(), "leave");
        f.marked = None;
        assert_eq!(decide(&c, &f).action(), "leave");
    }

    #[test]
    fn per_repo_settings_change_the_timing() {
        let repo = StaleConfig {
            stale_days: 3,
            close_days: 1,
            ..Default::default()
        };
        let c = effective(Some(&StaleConfig::default()), Some(&repo));
        assert_eq!(
            decide(
                &c,
                &facts("2026-09-04T00:00:00Z", "2026-09-01T00:00:00Z", None)
            ),
            Plan::Mark
        );
        assert_eq!(
            decide(
                &c,
                &facts(
                    "2026-09-05T00:00:00Z",
                    "2026-09-01T00:00:00Z",
                    Some(("2026-09-04T00:00:00Z", true))
                )
            ),
            Plan::Close
        );
    }

    #[test]
    fn branches_are_kept_by_default_and_never_deleted_for_forks() {
        let same = json!({"number": 1, "head": {"ref": "feat", "repo": {"full_name": "acme/web", "fork": false}}, "base": {"ref": "main", "repo": {"full_name": "acme/web", "default_branch": "main"}}});
        let fork = json!({"number": 2, "head": {"ref": "feat", "repo": {"full_name": "someone/web", "fork": true}}, "base": {"ref": "main", "repo": {"full_name": "acme/web", "default_branch": "main"}}});
        let gone = json!({"number": 3, "head": {"ref": "feat", "repo": null}, "base": {"ref": "main", "repo": {"full_name": "acme/web", "default_branch": "main"}}});
        assert!(!may_delete_branch(&StaleConfig::default(), &same, &[]));
        let on = StaleConfig {
            delete_branch: true,
            ..Default::default()
        };
        assert!(may_delete_branch(&on, &same, &[]));
        assert!(!may_delete_branch(&on, &fork, &[]));
        assert!(!may_delete_branch(&on, &gone, &[]));
        // A stacked change based on the branch keeps it.
        let stacked = json!({"number": 9, "base": {"ref": "feat"}});
        assert!(!may_delete_branch(&on, &same, &[stacked]));
    }

    #[test]
    fn keep_open_commands() {
        for s in [
            "keep open",
            "Keep this open!",
            "please keep it open",
            "don’t close",
            "do not close.",
        ] {
            assert!(is_keep_open(s), "{s}");
        }
        for s in ["", "review", "keep open the door later", "close"] {
            assert!(!is_keep_open(s), "{s}");
        }
    }

    #[test]
    fn keys() {
        assert_eq!(repo_of_sk(&repo_sk("acme/web")), Some("acme/web"));
        assert_eq!(repo_of_sk(TEAM_SK), None);
        assert_eq!(exempt_sk("acme/web", 7), "STALE_PR_EXEMPT#acme/web#000007");
        assert!(exempt_sk("acme/web", 7).starts_with(&exempt_prefix("acme/web")));
        assert!(log_sk(&t("2026-09-01T00:00:00Z"), "acme/web", 7)
            .starts_with("STALE_PR_LOG#2026-09-01T00:00:00.000Z#"));
    }
}
