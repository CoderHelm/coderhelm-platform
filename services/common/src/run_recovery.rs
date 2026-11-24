//! What a run does with an error from one of its passes, decided in one
//! place, before anything is marked failed.
//!
//! Every error a pass returns is read here and put in one of four kinds:
//!
//! - [`Kind::Transient`]: the network, a 5xx, a rate limit or throttling, a
//!   timeout. The run tries again after a backoff.
//! - [`Kind::Repairable`]: a state CoderHelm can put right itself (a branch
//!   or ref that isn't there, a base that moved or was deleted, a branch
//!   that conflicts with its base, a pull request of its own that was
//!   closed, a missing label, an expired token). The run repairs it, then
//!   goes on with the same pass from its checkpoint.
//! - [`Kind::NeedsPerson`]: something only a person has (the human-only
//!   list in [`crate::handback`]). Only this kind may stop a run, and then
//!   as "needs you", naming what the person does.
//! - [`Kind::Unknown`]: anything else. It is treated as repairable but
//!   unknown: tried again once, then the run stops as "needs you" with the
//!   error said in plain words.
//!
//! The same error met again without progress takes the next repair
//! strategy, never the same one twice ([`next`]); the attempts are kept on
//! the run record ([`Ledger`], [`FIELD_LEDGER`]) so every step and the
//! self-heal sweep read the same history.

use serde::{Deserialize, Serialize};

use crate::handback::HumanOnly;

/// Run attribute holding the [`Ledger`] JSON.
pub const FIELD_LEDGER: &str = "recovery";
/// Run attribute holding the error as it was raised (the run page and
/// `error_message` carry the plain words).
pub const FIELD_ERROR_DETAIL: &str = "error_detail";
/// Run attribute holding the error's kind code ([`Kind::code`]).
pub const FIELD_ERROR_KIND: &str = "error_kind";
/// Run attribute: the continuation message the self-heal sweep sends to
/// resume the run from its checkpoint (a `WorkerMessage` as JSON).
pub const FIELD_RESUME_MESSAGE: &str = "resume_message";

/// Tries of a transient error with no progress before the run stops.
pub const MAX_TRANSIENT_TRIES: usize = 5;
/// Tries of an error nobody classified before the run stops.
pub const MAX_UNKNOWN_TRIES: usize = 1;
/// The longest backoff (SQS takes up to 900 seconds).
pub const MAX_BACKOFF_SECS: u32 = 600;

/// What kind of error a pass met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Transient,
    Repairable,
    NeedsPerson,
    Unknown,
}

impl Kind {
    pub fn code(self) -> &'static str {
        match self {
            Kind::Transient => "transient",
            Kind::Repairable => "repairable",
            Kind::NeedsPerson => "needs_person",
            Kind::Unknown => "unknown",
        }
    }

    /// Whether a run that stopped on this kind is resumed by the self-heal
    /// sweep (and so may be parked as failed): only what CoderHelm can get
    /// past by itself.
    pub fn resumable(self) -> bool {
        matches!(self, Kind::Transient | Kind::Repairable)
    }
}

/// A state CoderHelm can repair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "repair", rename_all = "snake_case")]
pub enum Repair {
    /// A compare or ref read answered 404: `head` (or `base`) isn't in
    /// `owner/repo`. Empty names when the error didn't say.
    MissingRef {
        owner: String,
        repo: String,
        base: String,
        head: String,
    },
    /// The branch is behind its base, conflicts with it, or was rebased
    /// under the run (not a fast forward).
    BranchBehind {
        owner: String,
        repo: String,
        branch: String,
    },
    /// A pull request of the run's own was closed.
    ClosedOwnPr {
        owner: String,
        repo: String,
        number: u64,
    },
    /// A label the run adds doesn't exist in the repository.
    MissingLabel {
        owner: String,
        repo: String,
        name: String,
    },
    /// The installation token expired or was refused as bad credentials.
    ExpiredToken,
}

impl Repair {
    /// The ways to repair it, tried in order: an error met again without
    /// progress takes the next one.
    pub fn strategies(&self) -> &'static [Strategy] {
        match self {
            Repair::MissingRef { .. } => &[
                Strategy::ReconcileCheckpoint,
                Strategy::RecreateBranch,
                Strategy::RestartFromPlan,
            ],
            Repair::BranchBehind { .. } => &[Strategy::UpdateFromBase, Strategy::RestartFromPlan],
            Repair::ClosedOwnPr { .. } => &[Strategy::ReopenPr, Strategy::RestartFromPlan],
            Repair::MissingLabel { .. } => &[Strategy::CreateLabel],
            Repair::ExpiredToken => &[Strategy::RefreshToken, Strategy::RefreshToken],
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Repair::MissingRef { .. } => "missing_ref",
            Repair::BranchBehind { .. } => "branch_behind",
            Repair::ClosedOwnPr { .. } => "closed_own_pr",
            Repair::MissingLabel { .. } => "missing_label",
            Repair::ExpiredToken => "expired_token",
        }
    }
}

/// One way to repair a [`Repair`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    /// The checkpoint names a branch that isn't in the run's repository
    /// (another repository's branch in a multi-repo run, or one never
    /// pushed): the checkpoint goes back to the plan, so the pass starts on
    /// the branch that repository's own name gives, created before any
    /// compare. A deleted base moves to the repository's default branch.
    ReconcileCheckpoint,
    /// The run's own missing branch is created from the base.
    RecreateBranch,
    /// The pass starts again from the plan (never planned again): its
    /// branch is set up afresh by the usual branch start, which keeps a
    /// branch with an open pull request and never drops a person's commits.
    RestartFromPlan,
    /// The base is merged into the branch.
    UpdateFromBase,
    /// The run's own closed pull request is reopened.
    ReopenPr,
    /// The label is created.
    CreateLabel,
    /// A new installation token is fetched.
    RefreshToken,
}

impl Strategy {
    pub fn code(self) -> &'static str {
        match self {
            Strategy::ReconcileCheckpoint => "reconcile_checkpoint",
            Strategy::RecreateBranch => "recreate_branch",
            Strategy::RestartFromPlan => "restart_from_plan",
            Strategy::UpdateFromBase => "update_from_base",
            Strategy::ReopenPr => "reopen_pr",
            Strategy::CreateLabel => "create_label",
            Strategy::RefreshToken => "refresh_token",
        }
    }
}

/// An error, read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classified {
    pub kind: Kind,
    pub repair: Option<Repair>,
    /// For [`Kind::NeedsPerson`]: why only a person can do it.
    pub person: Option<HumanOnly>,
    /// The error in plain words: never a URL, a JSON body or a status dump.
    pub plain: String,
    /// The same error, met again, has the same signature.
    pub signature: String,
}

/// A GitHub HTTP error as `status_error_message` and reqwest write it:
/// "HTTP status client error (404 Not Found) for url (https://...)".
#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpError {
    status: u16,
    phrase: String,
    url: String,
}

fn http_error(text: &str) -> Option<HttpError> {
    let at = text.find("HTTP status ")?;
    let rest = &text[at..];
    let open = rest.find('(')?;
    let close = rest[open..].find(')')? + open;
    let inside = &rest[open + 1..close];
    let (code, phrase) = inside.split_once(' ').unwrap_or((inside, ""));
    let status = code.trim().parse::<u16>().ok()?;
    let url = rest
        .find("for url (")
        .map(|u| {
            let s = &rest[u + "for url (".len()..];
            s[..s.find(')').unwrap_or(s.len())].to_string()
        })
        .unwrap_or_default();
    Some(HttpError {
        status,
        phrase: phrase.trim().to_string(),
        url,
    })
}

/// `owner`, `repo` and the path after them, from a GitHub API URL.
fn repo_path(url: &str) -> Option<(String, String, String)> {
    let at = url.find("/repos/")?;
    let rest = &url[at + "/repos/".len()..];
    let rest = rest.split(['?', '#']).next().unwrap_or(rest);
    let mut parts = rest.splitn(3, '/');
    let owner = parts.next()?.to_string();
    let repo = parts.next()?.to_string();
    let path = parts.next().unwrap_or("").to_string();
    (!owner.is_empty() && !repo.is_empty()).then_some((owner, repo, path))
}

fn contains_any(t: &str, marks: &[&str]) -> bool {
    marks.iter().any(|m| t.contains(m))
}

/// Marks of an error that passes by itself.
const TRANSIENT_MARKS: &[&str] = &[
    "too many requests",
    "rate limit",
    "secondary rate",
    "throttl",
    "slow down",
    "provisionedthroughputexceeded",
    "requestlimitexceeded",
    "serviceunavailable",
    "service unavailable",
    "internalservererror",
    "internal server error",
    "bad gateway",
    "gateway timeout",
    "temporarily unavailable",
    "overloaded",
    "timed out",
    "timeout",
    "time limit",
    "connection reset",
    "connection refused",
    "connection closed",
    "broken pipe",
    "error sending request",
    "dispatch failure",
    "dns error",
    "unexpected eof",
    "worker process was terminated",
    "no response",
];

/// Reads an error (its text with its causes, `run_fail::error_text`).
pub fn classify(text: &str) -> Classified {
    let raw = text.trim();
    let t = raw.to_lowercase();
    let http = http_error(raw);
    let mk = |kind: Kind, repair: Option<Repair>, person: Option<HumanOnly>| {
        let plain = plain_words(raw, kind, repair.as_ref(), http.as_ref());
        let signature = signature(raw, repair.as_ref());
        Classified {
            kind,
            repair,
            person,
            plain,
            signature,
        }
    };
    if raw.is_empty() {
        return mk(Kind::Unknown, None, None);
    }

    // Only a person has it.
    if let Some(h) = crate::handback::human_only_in(raw) {
        return mk(Kind::NeedsPerson, None, Some(h));
    }
    if t.contains("limit exceeded") && t.contains("settings") {
        return mk(Kind::NeedsPerson, None, Some(HumanOnly::UsageLimit));
    }
    if http.as_ref().is_some_and(|h| h.status == 403)
        && !contains_any(&t, &["rate limit", "secondary rate"])
    {
        return mk(Kind::NeedsPerson, None, Some(HumanOnly::MissingAccess));
    }

    // A token CoderHelm fetches again.
    if http.as_ref().is_some_and(|h| h.status == 401)
        || contains_any(
            &t,
            &[
                "bad credentials",
                "token expired",
                "token has expired",
                "expired token",
            ],
        )
    {
        return mk(Kind::Repairable, Some(Repair::ExpiredToken), None);
    }

    // A pause.
    if let Some(h) = &http {
        if h.status >= 500 || h.status == 429 || h.status == 408 {
            return mk(Kind::Transient, None, None);
        }
    }
    if contains_any(&t, TRANSIENT_MARKS) {
        return mk(Kind::Transient, None, None);
    }

    let (owner, repo, path) = http
        .as_ref()
        .and_then(|h| repo_path(&h.url))
        .unwrap_or_default();

    // A state CoderHelm puts right.
    if http.as_ref().is_some_and(|h| h.status == 404) {
        if let Some(refs) = path.strip_prefix("compare/") {
            let refs = refs.replace("%2F", "/").replace("%2f", "/");
            let (base, head) = refs
                .split_once("...")
                .or_else(|| refs.split_once(".."))
                .unwrap_or(("", refs.as_str()));
            return mk(
                Kind::Repairable,
                Some(Repair::MissingRef {
                    owner,
                    repo,
                    base: base.to_string(),
                    head: head.to_string(),
                }),
                None,
            );
        }
        for prefix in ["git/ref/heads/", "git/refs/heads/", "branches/"] {
            if let Some(b) = path.strip_prefix(prefix) {
                return mk(
                    Kind::Repairable,
                    Some(Repair::MissingRef {
                        owner,
                        repo,
                        base: String::new(),
                        head: b.to_string(),
                    }),
                    None,
                );
            }
        }
    }
    if let Some(base) = deleted_base(raw) {
        return mk(
            Kind::Repairable,
            Some(Repair::MissingRef {
                owner: base.0,
                repo: base.1,
                base: base.2,
                head: String::new(),
            }),
            None,
        );
    }
    if contains_any(
        &t,
        &[
            "reference does not exist",
            "no commit found for",
            "branch not found",
            "not a valid ref",
        ],
    ) {
        return mk(
            Kind::Repairable,
            Some(Repair::MissingRef {
                owner,
                repo,
                base: String::new(),
                head: String::new(),
            }),
            None,
        );
    }
    if contains_any(
        &t,
        &[
            "merge conflict",
            "not mergeable",
            "not a fast forward",
            "not a fast-forward",
            "head branch was modified",
            "base branch was modified",
            "is behind its base",
        ],
    ) {
        let branch = path
            .strip_prefix("git/refs/heads/")
            .unwrap_or("")
            .to_string();
        return mk(
            Kind::Repairable,
            Some(Repair::BranchBehind {
                owner,
                repo,
                branch,
            }),
            None,
        );
    }
    if t.contains("label") && contains_any(&t, &["does not exist", "not found"]) {
        let name = backticked(raw).unwrap_or_default();
        return mk(
            Kind::Repairable,
            Some(Repair::MissingLabel { owner, repo, name }),
            None,
        );
    }
    if contains_any(
        &t,
        &["pull request is closed", "pr is closed", "was closed"],
    ) {
        let number = path
            .strip_prefix("pulls/")
            .and_then(|p| p.split('/').next())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        return mk(
            Kind::Repairable,
            Some(Repair::ClosedOwnPr {
                owner,
                repo,
                number,
            }),
            None,
        );
    }
    mk(Kind::Unknown, None, None)
}

/// The words [`deleted_base_error`] writes.
const DELETED_BASE: &str = "the base branch `";

/// The error a compare raises when its base is what's missing: the
/// classifier reads it back as a [`Repair::MissingRef`] with the base set.
pub fn deleted_base_error(owner: &str, repo: &str, base: &str) -> String {
    format!("{DELETED_BASE}{base}` doesn't exist in {owner}/{repo} (it was deleted or renamed)")
}

fn deleted_base(raw: &str) -> Option<(String, String, String)> {
    let at = raw.find(DELETED_BASE)?;
    let rest = &raw[at + DELETED_BASE.len()..];
    let end = rest.find('`')?;
    let base = rest[..end].to_string();
    let rest = rest[end..].strip_prefix("` doesn't exist in ")?;
    let full = rest.split_whitespace().next()?;
    let (owner, repo) = full.split_once('/')?;
    Some((owner.to_string(), repo.to_string(), base))
}

fn backticked(raw: &str) -> Option<String> {
    let a = raw.find('`')?;
    let b = raw[a + 1..].find('`')? + a + 1;
    Some(raw[a + 1..b].to_string())
}

/// The same error has the same signature: ids, numbers and hashes are
/// dropped, a repair is named by what it repairs.
pub fn signature(raw: &str, repair: Option<&Repair>) -> String {
    if let Some(r) = repair {
        return match r {
            Repair::MissingRef {
                owner,
                repo,
                base,
                head,
            } => format!("missing_ref:{owner}/{repo}:{base}...{head}").to_lowercase(),
            Repair::BranchBehind {
                owner,
                repo,
                branch,
            } => format!("branch_behind:{owner}/{repo}:{branch}").to_lowercase(),
            Repair::ClosedOwnPr {
                owner,
                repo,
                number,
            } => format!("closed_own_pr:{owner}/{repo}#{number}").to_lowercase(),
            Repair::MissingLabel { owner, repo, name } => {
                format!("missing_label:{owner}/{repo}:{name}").to_lowercase()
            }
            Repair::ExpiredToken => "expired_token".into(),
        };
    }
    let mut out = String::new();
    let mut last_hash = false;
    for c in raw.to_lowercase().chars() {
        if c.is_ascii_digit() {
            if !last_hash {
                out.push('#');
            }
            last_hash = true;
        } else {
            out.push(c);
            last_hash = false;
        }
    }
    let squashed = out.split_whitespace().collect::<Vec<_>>().join(" ");
    crate::truncate_str(&squashed, 200).to_string()
}

/// Text with every URL and JSON body taken out.
fn without_dumps(raw: &str) -> String {
    let mut s = raw.to_string();
    // "for url (...)" and anything that follows a JSON body.
    if let Some(i) = s.find(" for url (") {
        let tail = &s[i + " for url (".len()..];
        let end = tail.find(')').map(|e| i + " for url (".len() + e + 1);
        s = match end {
            Some(e) => format!("{}{}", &s[..i], &s[e..]),
            None => s[..i].to_string(),
        };
    }
    if let Some(i) = s.find(": {") {
        s.truncate(i);
    }
    if let Some(i) = s.find('{') {
        s.truncate(i);
    }
    let words: Vec<&str> = s
        .split_whitespace()
        .filter(|w| !w.starts_with("http://") && !w.starts_with("https://"))
        .collect();
    words
        .join(" ")
        .trim_end_matches([':', ',', ';', ' '])
        .to_string()
}

fn plain_words(raw: &str, kind: Kind, repair: Option<&Repair>, http: Option<&HttpError>) -> String {
    let status = http
        .map(|h| format!("{} {}", h.status, h.phrase).trim().to_string())
        .unwrap_or_default();
    let said = |what: &str| {
        if status.is_empty() {
            format!("{what}.")
        } else {
            format!("{what} (GitHub answered {status}).")
        }
    };
    match repair {
        Some(Repair::MissingRef {
            owner,
            repo,
            base,
            head,
        }) => {
            let at = if owner.is_empty() {
                String::new()
            } else {
                format!(" in {owner}/{repo}")
            };
            match (base.is_empty(), head.is_empty()) {
                (_, false) if !base.is_empty() => said(&format!(
                    "The branch `{head}` isn't{at} yet, so it couldn't be compared with `{base}`"
                )),
                (_, false) => said(&format!("The branch `{head}` isn't{at}")),
                (false, true) => format!(
                    "The base branch `{base}` isn't{at} any more (it was deleted or renamed)."
                ),
                (true, true) => said(&format!("A branch or commit the run uses isn't{at}")),
            }
        }
        Some(Repair::BranchBehind { branch, .. }) => said(&format!(
            "The branch{} is behind its base or conflicts with it",
            if branch.is_empty() {
                String::new()
            } else {
                format!(" `{branch}`")
            }
        )),
        Some(Repair::ClosedOwnPr { number, .. }) => {
            if *number > 0 {
                format!("CoderHelm's pull request #{number} was closed.")
            } else {
                "CoderHelm's pull request was closed.".to_string()
            }
        }
        Some(Repair::MissingLabel { name, .. }) => {
            format!("The label `{name}` doesn't exist in the repository.")
        }
        Some(Repair::ExpiredToken) => {
            "GitHub refused CoderHelm's access token (it expired or was revoked).".to_string()
        }
        None => match kind {
            Kind::Transient if http.is_some() => {
                format!("GitHub didn't answer for a moment ({status}).")
            }
            Kind::Transient => {
                let what = without_dumps(raw);
                format!(
                    "A service the run uses didn't answer for a moment: {}.",
                    crate::truncate_str(what.trim_end_matches('.'), 240)
                )
            }
            _ => {
                let what = without_dumps(raw);
                let what = if what.is_empty() {
                    if status.is_empty() {
                        "The run stopped on an error without a message".to_string()
                    } else {
                        format!("GitHub answered {status}")
                    }
                } else {
                    what
                };
                format!(
                    "{}.",
                    crate::truncate_str(what.trim_end_matches('.'), 400).trim_end_matches('.')
                )
            }
        },
    }
}

/// One attempt to get past an error.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub signature: String,
    pub kind: String,
    /// The strategy taken (its code), or `retry`.
    pub strategy: String,
    /// Where the run stood (`progress_mark`): the same error at the same
    /// mark is a repeat with no progress.
    pub mark: String,
    #[serde(default)]
    pub at: String,
}

/// The attempts a run made to get past its errors.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ledger {
    #[serde(default)]
    pub attempts: Vec<Attempt>,
    /// The code versions the self-heal sweep resumed the run at: at most
    /// once per run per deploy.
    #[serde(default)]
    pub healed_at: Vec<String>,
}

/// Attempts kept on the record (oldest dropped first).
const MAX_ATTEMPTS_KEPT: usize = 40;

impl Ledger {
    pub fn from_json(s: &str) -> Ledger {
        serde_json::from_str(s).unwrap_or_default()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".into())
    }

    /// Earlier attempts at this error with no progress since.
    pub fn repeats(&self, signature: &str, mark: &str) -> usize {
        self.attempts
            .iter()
            .filter(|a| a.signature == signature && a.mark == mark)
            .count()
    }

    pub fn record(&mut self, c: &Classified, strategy: &str, mark: &str, at: &str) {
        self.attempts.push(Attempt {
            signature: c.signature.clone(),
            kind: c.kind.code().to_string(),
            strategy: strategy.to_string(),
            mark: mark.to_string(),
            at: at.to_string(),
        });
        if self.attempts.len() > MAX_ATTEMPTS_KEPT {
            let drop = self.attempts.len() - MAX_ATTEMPTS_KEPT;
            self.attempts.drain(..drop);
        }
    }

    /// Whether the self-heal sweep already resumed the run at `version`.
    pub fn healed_at_version(&self, version: &str) -> bool {
        self.healed_at.iter().any(|v| v == version)
    }

    pub fn mark_healed(&mut self, version: &str) {
        if !self.healed_at_version(version) {
            self.healed_at.push(version.to_string());
            if self.healed_at.len() > 10 {
                self.healed_at.remove(0);
            }
        }
    }
}

/// Where a run stands for the ledger: the pass of its checkpoint and its
/// step's progress mark (branch head and gate).
pub fn progress_mark(checkpoint_pass: &str, step_mark: &str) -> String {
    format!("{checkpoint_pass}|{step_mark}")
}

/// What the run does next about an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// Try the pass again after `delay_secs`.
    Retry { delay_secs: u32 },
    /// Run this repair, then resume the pass from its checkpoint.
    Repair { repair: Repair, strategy: Strategy },
    /// Stop as "needs you".
    NeedsYou(NeedsYou),
}

/// A run stopped for a person: what they do, and why, in plain words.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeedsYou {
    pub why: HumanOnly,
    /// One action ("Restore the branch `main` in acme/site, then press Retry").
    pub action: String,
    /// Why, in plain words.
    pub detail: String,
}

/// The backoff before try `n` (0-based) of a transient error.
pub fn backoff_secs(n: usize) -> u32 {
    let secs = 30u32.saturating_mul(1u32 << n.min(6) as u32);
    secs.min(MAX_BACKOFF_SECS)
}

/// The decision for an error met at `mark`, given what was tried before.
pub fn next(c: &Classified, ledger: &Ledger, mark: &str) -> Next {
    let repeats = ledger.repeats(&c.signature, mark);
    match c.kind {
        Kind::NeedsPerson => Next::NeedsYou(needs_person(c)),
        Kind::Transient => {
            if repeats >= MAX_TRANSIENT_TRIES {
                Next::NeedsYou(NeedsYou {
                    why: HumanOnly::Unrepaired,
                    action: "Press Retry once the service is answering again".into(),
                    detail: format!(
                        "{} CoderHelm tried again {repeats} times with longer waits each time and \
                         it still didn't answer.",
                        c.plain
                    ),
                })
            } else {
                Next::Retry {
                    delay_secs: backoff_secs(repeats),
                }
            }
        }
        Kind::Repairable => {
            let repair = c.repair.clone().unwrap_or(Repair::MissingRef {
                owner: String::new(),
                repo: String::new(),
                base: String::new(),
                head: String::new(),
            });
            match repair.strategies().get(repeats) {
                Some(s) => Next::Repair {
                    strategy: *s,
                    repair,
                },
                None => Next::NeedsYou(unrepaired(c, &repair, repeats)),
            }
        }
        Kind::Unknown => {
            if repeats >= MAX_UNKNOWN_TRIES {
                Next::NeedsYou(unknown_stop(c))
            } else {
                Next::Retry {
                    delay_secs: backoff_secs(0),
                }
            }
        }
    }
}

/// The stop for an error nobody classified, after its one retry.
pub fn unknown_stop(c: &Classified) -> NeedsYou {
    NeedsYou {
        why: HumanOnly::Unrepaired,
        action: "Look at what stopped the run, put it right, then press Retry".into(),
        detail: format!(
            "CoderHelm met an error it doesn't know how to repair, tried again once, and met it \
             again: {}",
            c.plain
        ),
    }
}

fn needs_person(c: &Classified) -> NeedsYou {
    let why = c.person.unwrap_or(HumanOnly::Unrepaired);
    let action = match why {
        HumanOnly::Credential => "Add the credential the run needs, then press Retry",
        HumanOnly::MissingAccess => "Give CoderHelm the access the run needs, then press Retry",
        HumanOnly::ProductDecision => "Make the product decision on the ticket, then press Retry",
        HumanOnly::AmbiguousTicket => "Say on the ticket which reading is meant, then press Retry",
        HumanOnly::HeldByAnother => {
            "Finish or hand over the other work on this ticket, then press Retry"
        }
        HumanOnly::UsageLimit => {
            "Raise or clear the team's usage limit in Settings, then press Retry"
        }
        HumanOnly::Approval => "Answer the approval the team's agent policy asks for",
        HumanOnly::PersonAction => "Look at the run's last action, then press Retry",
        HumanOnly::Unrepaired => "Look at what stopped the run, put it right, then press Retry",
    };
    NeedsYou {
        why,
        action: action.into(),
        detail: c.plain.clone(),
    }
}

fn unrepaired(c: &Classified, repair: &Repair, tried: usize) -> NeedsYou {
    let action = match repair {
        Repair::MissingRef {
            owner,
            repo,
            base,
            head,
        } if head.is_empty() && !base.is_empty() => format!(
            "Restore the branch `{base}` in {owner}/{repo} or set the repository's default branch, \
             then press Retry"
        ),
        Repair::MissingRef { .. } => {
            "Check the run's branches on GitHub, then press Retry".to_string()
        }
        Repair::BranchBehind { .. } => {
            "Resolve the conflicts with the base branch on the pull request, then press Retry"
                .to_string()
        }
        Repair::ClosedOwnPr { .. } => {
            "Reopen the pull request, or press Retry to start a new one".to_string()
        }
        Repair::MissingLabel { name, .. } => {
            format!("Create the label `{name}` in the repository, then press Retry")
        }
        Repair::ExpiredToken => {
            "Check the CoderHelm GitHub app's installation (reinstall it if it was removed), then \
             press Retry"
                .to_string()
        }
    };
    NeedsYou {
        why: if matches!(repair, Repair::ExpiredToken) {
            HumanOnly::Credential
        } else {
            HumanOnly::Unrepaired
        },
        action,
        detail: format!(
            "{} CoderHelm tried {tried} way{} to repair it and the run met the same error each time.",
            c.plain,
            if tried == 1 { "" } else { "s" }
        ),
    }
}

/// How a stop for `reason` ends the run, for the one failed-status write
/// (`run_fail`): parked as `failed` for the self-heal sweep when CoderHelm
/// gets past it by itself, else "needs you" with what a person does. An
/// error nobody classified never ends a run as `failed`.
pub fn ending(reason: &str) -> Ending {
    let c = classify(reason);
    if c.kind.resumable() {
        Ending::Parked(c)
    } else {
        let needs = match c.kind {
            Kind::NeedsPerson => needs_person(&c),
            _ => unknown_stop(&c),
        };
        Ending::NeedsYou(c, needs)
    }
}

/// See [`ending`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
    Parked(Classified),
    NeedsYou(Classified, NeedsYou),
}

#[cfg(test)]
mod tests {
    use super::*;

    const INCIDENT: &str = "HTTP status client error (404 Not Found) for url \
        (https://api.github.com/repos/acme/infra-live/compare/main...coderhelm/tk-1-web-api): \
        {\"message\":\"Not Found\",\"documentation_url\":\"https://docs.github.com/rest/commits/commits#compare-two-commits\",\"status\":\"404\"}";

    #[test]
    fn a_compare_404_is_a_missing_branch_the_run_repairs() {
        let c = classify(INCIDENT);
        assert_eq!(c.kind, Kind::Repairable);
        assert_eq!(
            c.repair,
            Some(Repair::MissingRef {
                owner: "acme".into(),
                repo: "infra-live".into(),
                base: "main".into(),
                head: "coderhelm/tk-1-web-api".into(),
            })
        );
        assert!(!c.plain.contains("http"), "{}", c.plain);
        assert!(!c.plain.contains('{'), "{}", c.plain);
        assert!(c
            .plain
            .contains("`coderhelm/tk-1-web-api` isn't in acme/infra-live yet"));
    }

    #[test]
    fn kinds_of_errors() {
        for t in [
            "HTTP status server error (502 Bad Gateway) for url (https://api.github.com/repos/a/b/pulls)",
            "HTTP status client error (429 Too Many Requests) for url (https://api.github.com/x)",
            "operation timed out",
            "dispatch failure: io error: connection reset by peer",
            "ThrottlingException: Rate exceeded",
            "Run timed out (worker process was terminated)",
            "The step reached its time limit and the run couldn't continue in a fresh step: x.",
        ] {
            assert_eq!(classify(t).kind, Kind::Transient, "{t}");
        }
        assert_eq!(
            classify("HTTP status client error (401 Unauthorized) for url (https://api.github.com/x): Bad credentials").repair,
            Some(Repair::ExpiredToken)
        );
        assert_eq!(
            classify("HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/a/b/pulls): Resource not accessible by integration").kind,
            Kind::NeedsPerson
        );
        assert_eq!(
            classify("Monthly token limit exceeded (5 / 4 tokens). Increase your limit in Settings under Token Limit.").person,
            Some(HumanOnly::UsageLimit)
        );
        assert!(matches!(
            classify("HTTP status client error (409 Conflict) for url (https://api.github.com/repos/a/b/merges): Merge conflict").repair,
            Some(Repair::BranchBehind { .. })
        ));
        assert_eq!(
            classify(&deleted_base_error("a", "b", "develop")).repair,
            Some(Repair::MissingRef {
                owner: "a".into(),
                repo: "b".into(),
                base: "develop".into(),
                head: String::new()
            })
        );
        assert_eq!(
            classify("Multi-repo pipeline produced no PRs").kind,
            Kind::Unknown
        );
    }

    #[test]
    fn a_repeat_without_progress_takes_the_next_strategy_then_stops_for_a_person() {
        let c = classify(INCIDENT);
        let mut l = Ledger::default();
        let mut taken = Vec::new();
        loop {
            match next(&c, &l, "implement|abc") {
                Next::Repair { strategy, .. } => {
                    taken.push(strategy);
                    l.record(&c, strategy.code(), "implement|abc", "t");
                }
                Next::NeedsYou(n) => {
                    assert!(n.detail.contains("tried 3 ways"));
                    assert!(!n.detail.contains("api.github.com"));
                    break;
                }
                Next::Retry { .. } => panic!("a repairable error is repaired"),
            }
        }
        assert_eq!(
            taken,
            vec![
                Strategy::ReconcileCheckpoint,
                Strategy::RecreateBranch,
                Strategy::RestartFromPlan
            ]
        );
        // Progress since (a new mark) starts the strategies over.
        assert!(matches!(
            next(&c, &l, "implement|def"),
            Next::Repair {
                strategy: Strategy::ReconcileCheckpoint,
                ..
            }
        ));
    }

    #[test]
    fn transient_errors_back_off_then_stop() {
        let c = classify(
            "HTTP status server error (503 Service Unavailable) for url (https://api.github.com/x)",
        );
        let mut l = Ledger::default();
        let mut delays = Vec::new();
        while let Next::Retry { delay_secs } = next(&c, &l, "m") {
            delays.push(delay_secs);
            l.record(&c, "retry", "m", "t");
        }
        assert_eq!(delays, vec![30, 60, 120, 240, 480]);
        assert!(delays.iter().all(|d| *d <= MAX_BACKOFF_SECS));
    }

    #[test]
    fn an_unclassified_error_is_retried_once_then_needs_you_never_failed() {
        let c = classify("Something nobody has seen before: frobnicator returned 7");
        assert_eq!(c.kind, Kind::Unknown);
        let mut l = Ledger::default();
        assert!(matches!(next(&c, &l, "m"), Next::Retry { .. }));
        l.record(&c, "retry", "m", "t");
        match next(&c, &l, "m") {
            Next::NeedsYou(n) => {
                assert_eq!(n.why, HumanOnly::Unrepaired);
                assert!(n.detail.contains("frobnicator returned"));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            ending("Something nobody has seen before"),
            Ending::NeedsYou(..)
        ));
    }

    #[test]
    fn signatures_ignore_ids() {
        assert_eq!(
            signature("run 01ABC failed at step 3", None),
            signature("run 01ABC failed at step 4", None)
        );
    }

    #[test]
    fn plain_words_never_carry_a_dump() {
        let c = classify(
            "HTTP status client error (422 Unprocessable Entity) for url (https://api.github.com/repos/a/b/pulls): {\"message\":\"Validation Failed\"}",
        );
        assert_eq!(c.kind, Kind::Unknown);
        assert!(!c.plain.contains("https://"), "{}", c.plain);
        assert!(!c.plain.contains('{'), "{}", c.plain);
    }

    #[test]
    fn the_ledger_round_trips_and_heals_once_per_version() {
        let mut l = Ledger::default();
        l.mark_healed("v1");
        l.mark_healed("v1");
        assert_eq!(l.healed_at, vec!["v1"]);
        let back = Ledger::from_json(&l.to_json());
        assert!(back.healed_at_version("v1"));
        assert!(!back.healed_at_version("v2"));
        assert_eq!(Ledger::from_json("not json"), Ledger::default());
    }
}
