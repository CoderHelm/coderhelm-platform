//! A run whose ticket an open pull request already covers
//! (`common::stop_rules::covering`, the decision and its words).
//!
//! The case it was built for: a ticket's earlier run had CoderHelm's pull
//! request open. A second ticket ("merge and deploy the first") got its own
//! CoderHelm pull request, stacked and holding the first ticket's work. A
//! retry of the first ticket's run rebuilt its branch on the second pull
//! request's, did most of its tasks, then ended as "covered by an open pull
//! request" with only Open and Retry: two pull requests, the work on neither.
//!
//! Now, for CoderHelm's own covering pull request ([`OwnPr`]):
//!
//! - it lacks something: the run's work goes onto its branch (a fast forward
//!   when the run's branch is built on it; else the run's own change rebuilt
//!   on it file by file, `restack`), its description gets this ticket's
//!   section, the ticket's earlier pull request is marked superseded, and
//!   the run ends linked to it;
//! - it has everything: the run ends saying so, and offers to close the
//!   earlier pull request;
//! - its run is still working on it: nothing is written (one writer per
//!   branch, `pr_holder`).
//!
//! A person's pull request is never pushed to, edited or closed. Every write
//! here goes through [`OwnPr`], which only [`OwnPr::verify`] makes, from the
//! pull request as GitHub shows it now, and which checks again
//! ([`common::own_pr::judge`]) right before each write. Nothing here calls
//! GitHub's write endpoints except the [`Cover`] methods those writes use.
//! Before writing, the run counts as a pass (`run_guard::on_pass`): a run the
//! loop guard or its ceiling stopped never gets here.

use common::own_pr::{self, NotOwn, PrFacts};
use common::run_status::{self, EndedValues};
use common::stop_rules::covering::{self as cov, Carried, Outcome, Record, Superseded};
use common::stop_rules::existing_work::{Covered, OpenPr};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::WorkerState;

use super::{attr_n, attr_s, restack, RunUsageValues};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// `head` compared with `base`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Compared {
    /// GitHub's word: `ahead`, `behind`, `identical` or `diverged`.
    pub status: String,
    /// Commits `head` has that `base` doesn't.
    pub ahead: usize,
    pub files: Vec<String>,
}

/// A rebuild of the run's own change on another branch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Rebuilt {
    pub tip: String,
    /// Files both changed whose two versions couldn't be merged.
    pub unresolved: Vec<String>,
}

/// What the covered path reads and writes. The two raw writes are only
/// called by [`OwnPr`].
#[async_trait::async_trait]
pub trait Cover: Send + Sync {
    /// The pull request, as GitHub shows it now.
    async fn read_pr(&self, number: u64) -> Result<serde_json::Value, Error>;
    /// This app's bot login, when it can be read.
    async fn bot_login(&self) -> Option<String>;
    /// Whether one of the team's runs recorded the pull request.
    async fn opened_by_run(&self, number: u64) -> bool;
    async fn head(&self, branch: &str) -> Result<String, Error>;
    async fn compare(&self, base: &str, head: &str) -> Result<Compared, Error>;
    /// `branch`'s own change (its commits past `base`) rebuilt on `onto`'s
    /// head; `branch` moves there.
    async fn rebuild(
        &mut self,
        branch: &str,
        base: &str,
        onto: &str,
        onto_pr: u64,
        message: &str,
    ) -> Result<Rebuilt, String>;
    /// Moves `branch` forward from `lease` to `to` (never forced).
    async fn raw_fast_forward(&self, branch: &str, lease: &str, to: &str) -> Result<bool, Error>;
    async fn raw_set_body(&self, number: u64, body: &str) -> Result<(), Error>;
}

/// Why a write to a pull request didn't happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// It isn't CoderHelm's (or no longer open): nothing was written.
    NotOwn(NotOwn),
    /// Its branch moved since it was read: nothing was written.
    Moved,
    GitHub(String),
}

impl Refused {
    pub fn say(&self, number: u64) -> String {
        match self {
            Refused::NotOwn(n) => n.say(number),
            Refused::Moved => format!("#{number}'s branch moved while the work was carried over"),
            Refused::GitHub(e) => format!("GitHub refused the change to #{number} ({e})"),
        }
    }
}

/// A pull request CoderHelm created: open, opened by its bot, on a branch in
/// the repository, recorded by one of the team's runs. Only [`OwnPr::verify`]
/// makes one, and each write checks it again first.
#[derive(Debug, Clone)]
pub struct OwnPr {
    facts: PrFacts,
    bot: Option<String>,
    opened_by_run: bool,
}

impl OwnPr {
    /// Reads the pull request fresh and judges it.
    pub async fn verify<C: Cover + ?Sized>(c: &C, number: u64) -> Result<OwnPr, (NotOwn, PrFacts)> {
        let facts = match c.read_pr(number).await {
            Ok(v) => match PrFacts::from_github(&v) {
                Some(f) => f,
                None => {
                    return Err((
                        NotOwn::Unread("GitHub's answer had no number".into()),
                        PrFacts::default(),
                    ))
                }
            },
            Err(e) => return Err((NotOwn::Unread(e.to_string()), PrFacts::default())),
        };
        let bot = c.bot_login().await;
        let opened_by_run = c.opened_by_run(number).await;
        match own_pr::judge(&facts, bot.as_deref(), opened_by_run) {
            Ok(()) => Ok(OwnPr {
                facts,
                bot,
                opened_by_run,
            }),
            Err(e) => Err((e, facts)),
        }
    }

    fn check(&self) -> Result<(), Refused> {
        own_pr::judge(&self.facts, self.bot.as_deref(), self.opened_by_run).map_err(Refused::NotOwn)
    }

    pub fn facts(&self) -> &PrFacts {
        &self.facts
    }

    /// Moves its branch forward to `to`, only from the head it had when read.
    pub async fn fast_forward<C: Cover + ?Sized>(&self, c: &C, to: &str) -> Result<(), Refused> {
        self.check()?;
        match c
            .raw_fast_forward(&self.facts.head, &self.facts.head_sha, to)
            .await
        {
            Ok(true) => Ok(()),
            Ok(false) => Err(Refused::Moved),
            Err(e) => Err(Refused::GitHub(e.to_string())),
        }
    }

    /// Replaces its description.
    pub async fn set_body<C: Cover + ?Sized>(&self, c: &C, body: &str) -> Result<(), Refused> {
        self.check()?;
        c.raw_set_body(self.facts.number, body)
            .await
            .map_err(|e| Refused::GitHub(e.to_string()))
    }
}

/// What the run brings to the covered path.
#[derive(Debug, Clone, Default)]
pub struct Inputs {
    pub ticket: String,
    /// The ticket's link line (`Closes #N`, `Source ticket: KEY`).
    pub ticket_line: String,
    /// The run's branch and the branch it was built from; `None` before
    /// the run has built anything.
    pub run_branch: Option<(String, String)>,
    /// The run's planned tasks it didn't do.
    pub unfinished: Vec<String>,
    /// The ticket's earlier pull request CoderHelm opened, if any (other
    /// than the covering one).
    pub earlier: Option<u64>,
}

/// What the covered path came to.
#[derive(Debug, Clone, PartialEq)]
pub enum Taken {
    /// The pull request merged or closed meanwhile: the run takes its normal
    /// path.
    Gone(NotOwn),
    /// The run ends with this record (`covered_by`).
    End(Box<Record>),
}

/// The record's pull request fields from what GitHub shows now.
fn fill(rec: &mut Record, f: &PrFacts) {
    if !f.repo.is_empty() {
        rec.repo = f.repo.clone();
    }
    if !f.url.is_empty() {
        rec.url = f.url.clone();
    }
    if !f.title.is_empty() {
        rec.title = f.title.clone();
    }
    if !f.author.is_empty() {
        rec.author = f.author.clone();
    }
}

/// Decides and does what a covered run does about `covered` (see the
/// module's docs). Writes only through [`OwnPr`].
pub async fn take<C: Cover>(c: &mut C, covered: &Covered, inp: &Inputs) -> Taken {
    let number = covered.pr.number;
    let own = match OwnPr::verify(c, number).await {
        Err((e, _)) if e.gone() => return Taken::Gone(e),
        Err((e, facts)) => {
            let mut rec = Record::new(covered, false, Outcome::Person);
            fill(&mut rec, &facts);
            rec.note = match &e {
                NotOwn::Person(_) => cov::person_note(number, &rec.author),
                other => format!(
                    "{} {}",
                    cov::person_note(number, &rec.author),
                    capitalize(&other.say(number))
                ),
            };
            return Taken::End(Box::new(rec));
        }
        Ok(o) => o,
    };
    let mut rec = Record::new(covered, true, Outcome::Continued);
    fill(&mut rec, own.facts());
    let open = cov::open_items(&inp.unfinished, &covered.missing);
    let carried = match &inp.run_branch {
        Some((branch, base)) => match carry(c, &own, branch, base).await {
            Ok(x) => x,
            Err(why) => {
                rec.outcome = Outcome::NotCarried.as_str().into();
                rec.note = cov::not_carried_note(number, &why, branch);
                rec.open_items = open;
                return Taken::End(Box::new(rec));
            }
        },
        None => None,
    };
    if cov::complete(carried.as_ref(), &open) {
        rec.outcome = Outcome::Complete.as_str().into();
        rec.note = cov::complete_note(number);
    } else {
        let section = cov::section(&inp.ticket, &inp.ticket_line, carried.as_ref(), &open);
        let body = cov::put_section(&own.facts().body, &inp.ticket, &section);
        if let Err(r) = own.set_body(c, &body).await {
            warn!(pr = number, why = %r.say(number), "Could not add the ticket to the covering pull request's description");
        }
        rec.note = cov::continued_note(number, carried.as_ref(), &open);
    }
    rec.carried = carried;
    rec.open_items = open;
    if let Some(n) = inp.earlier.filter(|n| *n != number) {
        rec.superseded = supersede(c, n, number).await;
    }
    Taken::End(Box::new(rec))
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => format!("{}{}.", f.to_uppercase(), c.as_str()),
        None => String::new(),
    }
}

/// The run's work onto the covering pull request's branch. `None` when it
/// already has it.
async fn carry<C: Cover>(
    c: &mut C,
    own: &OwnPr,
    branch: &str,
    base: &str,
) -> Result<Option<Carried>, String> {
    let n = own.facts().number;
    let onto = own.facts().head.clone();
    if branch == onto {
        return Ok(None);
    }
    let cmp = c
        .compare(&onto, branch)
        .await
        .map_err(|e| format!("the two branches couldn't be compared ({e})"))?;
    let (tip, rebuilt) = match cmp.status.as_str() {
        "identical" | "behind" => return Ok(None),
        "ahead" => (
            c.head(branch)
                .await
                .map_err(|e| format!("`{branch}` couldn't be read ({e})"))?,
            false,
        ),
        "diverged" => {
            let message = format!("Carry this ticket's change over to #{n}");
            let r = c.rebuild(branch, base, &onto, n, &message).await?;
            if !r.unresolved.is_empty() {
                return Err(format!(
                    "#{n} also changed {} and the two versions couldn't be merged",
                    r.unresolved
                        .iter()
                        .map(|p| format!("`{p}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            (r.tip, true)
        }
        other => return Err(format!("GitHub compared the branches as \"{other}\"")),
    };
    own.fast_forward(c, &tip).await.map_err(|r| r.say(n))?;
    Ok(Some(Carried {
        branch: onto,
        head: tip,
        commits: cmp.ahead,
        files: cmp.files,
        rebuilt,
    }))
}

/// Marks the ticket's earlier pull request superseded in its description.
/// `None` when it isn't CoderHelm's own or no longer open.
async fn supersede<C: Cover>(c: &C, earlier: u64, covering: u64) -> Option<Superseded> {
    let pr = match OwnPr::verify(c, earlier).await {
        Ok(p) => p,
        Err((e, _)) => {
            info!(pr = earlier, why = %e.say(earlier), "The earlier pull request is left as it is");
            return None;
        }
    };
    let body = cov::mark_superseded(&pr.facts().body, covering);
    if let Err(r) = pr.set_body(c, &body).await {
        warn!(pr = earlier, why = %r.say(earlier), "Could not mark the earlier pull request superseded");
    }
    Some(Superseded {
        repo: pr.facts().repo.clone(),
        number: earlier,
        url: pr.facts().url.clone(),
        title: pr.facts().title.clone(),
        closed: false,
        closed_by: String::new(),
    })
}

// ── The run's side ──────────────────────────────────────────────────────────

/// The covering pull request a run decided, before it built anything, to
/// continue in (kept on the run's checks ledger across steps).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Remembered {
    pub number: u64,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub missing: Vec<String>,
}

impl Remembered {
    pub fn of(c: &Covered) -> Remembered {
        Remembered {
            number: c.pr.number,
            reason: c.reason.clone(),
            missing: c.missing.clone(),
        }
    }

    /// The covered check's answer again; the pull request's own fields are
    /// read fresh by [`take`].
    pub fn covered(&self, repo: &str) -> Covered {
        Covered {
            pr: OpenPr {
                repo: repo.to_string(),
                number: self.number,
                ..Default::default()
            },
            signals: Default::default(),
            reason: self.reason.clone(),
            missing: self.missing.clone(),
        }
    }
}

/// [`Cover`] on GitHub and the team's run records.
pub struct GhCover<'a> {
    pub state: &'a WorkerState,
    pub github: &'a GitHubClient,
    pub team_id: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
    /// Merges a file both branches changed; without one, such a file stops
    /// the carry-over.
    pub merger: Option<restack::ModelMerger<'a>>,
}

/// The team's runs that recorded `owner/name#number`, newest first.
pub async fn runs_for_pr(
    state: &WorkerState,
    team_id: &str,
    repo_full: &str,
    number: u64,
) -> Vec<String> {
    let url = format!("https://github.com/{repo_full}/pull/{number}");
    let mut out = Vec::new();
    let mut start = None;
    for _ in 0..10 {
        let page = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .index_name("repo-index")
            .key_condition_expression("team_repo = :tr")
            .filter_expression("pr_number = :n OR contains(pr_urls, :u)")
            .expression_attribute_values(":tr", attr_s(&format!("{team_id}#{repo_full}")))
            .expression_attribute_values(":n", attr_n(number))
            .expression_attribute_values(":u", attr_s(&url))
            .scan_index_forward(false)
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(page) = page else {
            break;
        };
        out.extend(
            page.items()
                .iter()
                .filter_map(|i| i.get("run_id").and_then(|v| v.as_s().ok()).cloned()),
        );
        match page.last_evaluated_key {
            Some(k) if !k.is_empty() && out.len() < 20 => start = Some(k),
            _ => break,
        }
    }
    out
}

#[async_trait::async_trait]
impl Cover for GhCover<'_> {
    async fn read_pr(&self, number: u64) -> Result<serde_json::Value, Error> {
        self.github
            .get_pull_request(self.owner, self.repo, number)
            .await
    }

    async fn bot_login(&self) -> Option<String> {
        self.github.bot_login().await
    }

    async fn opened_by_run(&self, number: u64) -> bool {
        !runs_for_pr(
            self.state,
            self.team_id,
            &format!("{}/{}", self.owner, self.repo),
            number,
        )
        .await
        .is_empty()
    }

    async fn head(&self, branch: &str) -> Result<String, Error> {
        self.github.get_ref(self.owner, self.repo, branch).await
    }

    async fn compare(&self, base: &str, head: &str) -> Result<Compared, Error> {
        let d = self
            .github
            .get_diff(self.owner, self.repo, base, head)
            .await?;
        Ok(Compared {
            status: d["status"].as_str().unwrap_or("").to_string(),
            ahead: d["ahead_by"].as_u64().unwrap_or(0) as usize,
            files: d["files"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|f| f["filename"].as_str().map(String::from))
                .collect(),
        })
    }

    async fn rebuild(
        &mut self,
        branch: &str,
        base: &str,
        onto: &str,
        onto_pr: u64,
        message: &str,
    ) -> Result<Rebuilt, String> {
        let known = Default::default();
        let gh = restack::Gh {
            github: self.github,
            owner: self.owner,
            repo: self.repo,
            known: &known,
        };
        let job = restack::Job {
            branch,
            onto,
            own: restack::Own::NotCopies { base, copies: &[] },
            message,
            onto_pr,
        };
        let r = match self.merger.as_mut() {
            Some(m) => restack::rebuild(&gh, m, &job).await,
            None => restack::rebuild(&gh, &mut restack::NoMerger, &job).await,
        };
        r.map(|r| Rebuilt {
            tip: r.tip,
            unresolved: r.unresolved,
        })
        .map_err(|h| h.say(&format!("{}/{}", self.owner, self.repo)))
    }

    async fn raw_fast_forward(&self, branch: &str, lease: &str, to: &str) -> Result<bool, Error> {
        self.github
            .fast_forward(self.owner, self.repo, branch, lease, to)
            .await
    }

    async fn raw_set_body(&self, number: u64, body: &str) -> Result<(), Error> {
        self.github
            .update_pr_body(self.owner, self.repo, number, body)
            .await
    }
}

/// Before the run builds anything: when the covering pull request is
/// CoderHelm's own and the covered check named what it lacks, the branch to
/// build on (stacked on it). `None` otherwise: the covered path decides.
pub async fn builds_on(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    covered: &Covered,
) -> Option<super::stack::Dependency> {
    if covered.missing.is_empty() {
        return None;
    }
    let probe = GhCover {
        state,
        github,
        team_id: &msg.team_id,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        merger: None,
    };
    let own = OwnPr::verify(&probe, covered.pr.number).await.ok()?;
    let f = own.facts();
    Some(super::stack::Dependency {
        number: f.number,
        branch: f.head.clone(),
        title: f.title.clone(),
        why: format!(
            "It is CoderHelm's own open pull request that already covers this ticket; this \
             change adds what it lacked ({}) and goes onto its branch.",
            covered.missing.join("; ")
        ),
        ..Default::default()
    })
}

/// The run page's note when the run builds on the covering pull request.
pub fn builds_on_note(covered: &Covered, dep: &super::stack::Dependency) -> String {
    format!(
        "#{n} is CoderHelm's own open pull request and already covers most of this ticket, but \
         not {}. This run builds that on #{n}'s branch `{}` and carries it into #{n}: one pull \
         request for the work, not two.",
        covered.missing.join("; "),
        dep.branch,
        n = dep.number
    )
}

/// The run's planned tasks its branch doesn't do (required ones only).
pub async fn unfinished_tasks(
    github: &GitHubClient,
    msg: &TicketMessage,
    branch: &str,
    tasks_md: &str,
) -> Vec<String> {
    let tasks =
        super::plan_coverage::tasks_for(github, &msg.repo_owner, &msg.repo_name, branch, tasks_md)
            .await;
    if tasks.is_empty() {
        return Vec::new();
    }
    let Some(changes) = super::plan_coverage::plan_changes(
        github,
        &msg.repo_owner,
        &msg.repo_name,
        &msg.base_branch,
        branch,
    )
    .await
    else {
        return Vec::new();
    };
    changes
        .coverage(&tasks)
        .required_missing()
        .into_iter()
        .map(|t| t.task.clone())
        .collect()
}

/// The ticket's link line for a pull request's description.
pub fn ticket_line(msg: &TicketMessage) -> String {
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        format!("Closes #{}", msg.issue_number)
    } else {
        format!("Source ticket: {}", msg.ticket_id)
    }
}

/// How the run goes on after the covered path.
pub enum Next {
    /// The run ended (its record says how).
    Ended,
    /// The covering pull request merged or closed: the normal path.
    Normal,
}

/// The covered path in a run: decides, writes (only to CoderHelm's own pull
/// requests, while no other run writes their branches), records the run's
/// end and notes the ticket. `Err` when the loop guard or the run's ceiling
/// stopped the run: nothing is written then.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    run_id: &str,
    start: &std::time::Instant,
    covered: &Covered,
    inp: &Inputs,
) -> Result<Next, Error> {
    let (owner, repo) = (msg.repo_owner.as_str(), msg.repo_name.as_str());
    let repo_full = format!("{owner}/{repo}");
    let number = covered.pr.number;
    // This is a pass like any other: a run the loop guard or its ceiling
    // stopped goes no further.
    super::run_guard::on_pass(state, &msg.team_id, run_id).await?;
    // One writer per branch: the covering pull request's own runs (another
    // ticket's) must not be working on it while this run writes it.
    let holders: Vec<String> = runs_for_pr(state, &msg.team_id, &repo_full, number)
        .await
        .into_iter()
        .filter(|r| r != run_id)
        .collect();
    let guards = match super::pr_holder::claim(state, &msg.team_id, run_id, &holders).await {
        super::pr_holder::Claim::Free(g) => g,
        super::pr_holder::Claim::Busy {
            run_id: holder,
            why,
        } => {
            // Read only, to tell whose it is: nothing is written.
            let probe = GhCover {
                state,
                github,
                team_id: &msg.team_id,
                owner,
                repo,
                merger: None,
            };
            let mut rec = Record::new(covered, false, Outcome::Busy);
            match OwnPr::verify(&probe, number).await {
                Err((e, _)) if e.gone() => return Ok(Next::Normal),
                Err((NotOwn::Person(_), facts)) => {
                    fill(&mut rec, &facts);
                    rec.outcome = Outcome::Person.as_str().into();
                    rec.note = cov::person_note(number, &rec.author);
                }
                Err((_, facts)) => {
                    fill(&mut rec, &facts);
                    rec.note = super::pr_holder::busy_note(number, &holder, &why);
                }
                Ok(o) => {
                    fill(&mut rec, o.facts());
                    rec.own = true;
                    rec.note = super::pr_holder::busy_note(number, &holder, &why);
                }
            }
            finish(state, msg, run_id, github, &rec, usage, start).await;
            return Ok(Next::Ended);
        }
    };
    let taken = {
        let mut c = GhCover {
            state,
            github,
            team_id: &msg.team_id,
            owner,
            repo,
            merger: Some(restack::ModelMerger {
                state,
                provider,
                usage: &mut *usage,
                theirs: format!("pull request #{number}'s branch"),
            }),
        };
        take(&mut c, covered, inp).await
    };
    drop(guards);
    match taken {
        Taken::Gone(e) => {
            info!(run_id, pr = number, why = %e.say(number), "The covering pull request is gone: the normal path");
            super::add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!(
                    "{}, so this run goes on as usual.",
                    capitalize(&e.say(number)).trim_end_matches('.')
                ),
            )
            .await;
            Ok(Next::Normal)
        }
        Taken::End(rec) => {
            finish(state, msg, run_id, github, &rec, usage, start).await;
            Ok(Next::Ended)
        }
    }
}

/// Ends the run as covered: the note on the run page, the ticket's note, the
/// record (`covered_by`), status `covered_by_pr`.
pub async fn finish(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    github: &GitHubClient,
    rec: &Record,
    usage: &mut TokenUsage,
    start: &std::time::Instant,
) {
    info!(
        run_id,
        pr = rec.number,
        outcome = %rec.outcome,
        "Run covered by an open pull request"
    );
    super::add_progress_note(state, &msg.team_id, run_id, &rec.note).await;
    match msg.source {
        TicketSource::Github if msg.issue_number > 0 => {
            if let Err(e) = github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.issue_number,
                    &cov::ticket_note(rec, true),
                )
                .await
            {
                warn!(run_id, error = %e, "Could not note the covering PR on the issue");
            }
        }
        TicketSource::Jira if !msg.ticket_id.is_empty() => {
            if let Err(e) = super::post_jira_comment(
                state,
                &msg.team_id,
                &msg.ticket_id,
                &cov::ticket_note(rec, false),
                "covered",
                "Already being fixed",
            )
            .await
            {
                warn!(run_id, error = %e, "Could not note the covering PR on the ticket");
            }
        }
        _ => {}
    }
    let status = run_status::COVERED_BY_PR;
    let delta = usage.unrecorded();
    let written = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, covered_by = :cb, error_message = :em, \
             tokens_in = if_not_exists(tokens_in, :uzero) + :ti, tokens_out = if_not_exists(tokens_out, :uzero) + :to, \
             cache_read_tokens = if_not_exists(cache_read_tokens, :uzero) + :crt, \
             cache_write_tokens = if_not_exists(cache_write_tokens, :uzero) + :cwt, \
             cost_usd = if_not_exists(cost_usd, :uzero) + :c, duration_s = :d, updated_at = :t, current_pass = :cp",
        )
        .condition_expression(run_status::not_ended("#s"))
        .ended_values()
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s(status))
        .expression_attribute_values(":sri", attr_s(&format!("{status}#{run_id}")))
        .expression_attribute_values(
            ":cb",
            attr_s(&serde_json::to_string(rec).unwrap_or_default()),
        )
        .expression_attribute_values(":em", attr_s(&rec.note))
        .usage_values(&delta)
        .expression_attribute_values(":d", attr_n(start.elapsed().as_secs()))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":cp", attr_s("done"))
        .send()
        .await;
    if written.is_ok() {
        usage.mark_recorded(&delta);
    } else if let Err(e) = written {
        warn!(run_id, error = %e, "Could not record the covered run");
    }
    super::webhooks::emit_run(state, &msg.team_id, run_id, status).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::stop_rules::existing_work::Signals;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Mutex;

    const BOT: &str = "coderhelm[bot]";

    /// A repository with pull requests and branches; records every write.
    #[derive(Default)]
    struct Fake {
        prs: HashMap<u64, serde_json::Value>,
        recorded: Vec<u64>,
        heads: Mutex<HashMap<String, String>>,
        compares: HashMap<(String, String), Compared>,
        rebuilt: Option<Rebuilt>,
        writes: Mutex<Vec<String>>,
    }

    impl Fake {
        fn pr(&mut self, number: u64, author: &str, kind: &str, head: &str, sha: &str) {
            self.prs.insert(
                number,
                json!({
                    "number": number,
                    "state": "open",
                    "merged": false,
                    "title": format!("PR {number}"),
                    "body": format!("## Summary\nPR {number}."),
                    "html_url": format!("https://github.com/acme/web/pull/{number}"),
                    "user": {"login": author, "type": kind},
                    "head": {"ref": head, "sha": sha, "repo": {"full_name": "acme/web"}},
                    "base": {"ref": "main", "repo": {"full_name": "acme/web"}}
                }),
            );
            self.heads
                .lock()
                .unwrap()
                .insert(head.to_string(), sha.to_string());
        }

        fn writes(&self) -> Vec<String> {
            self.writes.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl Cover for Fake {
        async fn read_pr(&self, number: u64) -> Result<serde_json::Value, Error> {
            self.prs
                .get(&number)
                .cloned()
                .ok_or_else(|| "not found".into())
        }
        async fn bot_login(&self) -> Option<String> {
            Some(BOT.into())
        }
        async fn opened_by_run(&self, number: u64) -> bool {
            self.recorded.contains(&number)
        }
        async fn head(&self, branch: &str) -> Result<String, Error> {
            Ok(self
                .heads
                .lock()
                .unwrap()
                .get(branch)
                .cloned()
                .unwrap_or_default())
        }
        async fn compare(&self, base: &str, head: &str) -> Result<Compared, Error> {
            Ok(self
                .compares
                .get(&(base.to_string(), head.to_string()))
                .cloned()
                .unwrap_or_default())
        }
        async fn rebuild(
            &mut self,
            branch: &str,
            _base: &str,
            _onto: &str,
            _onto_pr: u64,
            _message: &str,
        ) -> Result<Rebuilt, String> {
            let r = self.rebuilt.clone().ok_or("no rebuild")?;
            self.heads
                .lock()
                .unwrap()
                .insert(branch.to_string(), r.tip.clone());
            Ok(r)
        }
        async fn raw_fast_forward(
            &self,
            branch: &str,
            lease: &str,
            to: &str,
        ) -> Result<bool, Error> {
            let mut heads = self.heads.lock().unwrap();
            if heads.get(branch).map(String::as_str) != Some(lease) {
                return Ok(false);
            }
            heads.insert(branch.to_string(), to.to_string());
            self.writes
                .lock()
                .unwrap()
                .push(format!("push {branch} {to}"));
            Ok(true)
        }
        async fn raw_set_body(&self, number: u64, body: &str) -> Result<(), Error> {
            self.writes
                .lock()
                .unwrap()
                .push(format!("body #{number}: {body}"));
            Ok(())
        }
    }

    const RUN_BRANCH: &str = "coderhelm/abc-1";
    const COVERING_BRANCH: &str = "coderhelm/abc-2";

    /// The live case: the ticket's earlier pull request #41 (the run's
    /// branch), the other ticket's #52 covering it, the run's branch rebuilt
    /// on #52's and ahead of it.
    fn live() -> Fake {
        let mut f = Fake::default();
        f.pr(41, BOT, "Bot", RUN_BRANCH, "r1");
        f.pr(52, BOT, "Bot", COVERING_BRANCH, "c1");
        f.recorded = vec![41, 52];
        f.heads
            .lock()
            .unwrap()
            .insert(RUN_BRANCH.into(), "r9".into());
        f.compares.insert(
            (COVERING_BRANCH.into(), RUN_BRANCH.into()),
            Compared {
                status: "ahead".into(),
                ahead: 3,
                files: vec!["src/register.ts".into(), "src/helpers/a.ts".into()],
            },
        );
        f
    }

    fn covered(missing: &[&str]) -> Covered {
        Covered {
            pr: OpenPr {
                repo: "acme/web".into(),
                number: 52,
                ..Default::default()
            },
            signals: Signals {
                ticket_key: true,
                ..Default::default()
            },
            reason: "It implements the scope.".into(),
            missing: missing.iter().map(|m| m.to_string()).collect(),
        }
    }

    fn inputs(unfinished: &[&str]) -> Inputs {
        Inputs {
            ticket: "ABC-1".into(),
            ticket_line: "Source ticket: ABC-1".into(),
            run_branch: Some((RUN_BRANCH.into(), COVERING_BRANCH.into())),
            unfinished: unfinished.iter().map(|m| m.to_string()).collect(),
            earlier: Some(41),
        }
    }

    fn ended(t: Taken) -> Record {
        match t {
            Taken::End(r) => *r,
            other => panic!("expected an end, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn own_covering_pull_request_is_continued_with_what_it_lacks() {
        let mut f = live();
        let rec = ended(
            take(
                &mut f,
                &covered(&["Register the two extra helper files"]),
                &inputs(&["Wire the settings page"]),
            )
            .await,
        );
        assert_eq!(rec.outcome, "continued");
        assert!(rec.own);
        let carried = rec.carried.clone().unwrap();
        assert_eq!(carried.commits, 3);
        assert_eq!(carried.branch, COVERING_BRANCH);
        assert!(!carried.rebuilt);
        assert_eq!(
            rec.open_items,
            vec![
                "Wire the settings page",
                "Register the two extra helper files"
            ]
        );
        let w = f.writes();
        // The covering branch moved forward to the run's head, from the head
        // it was read at.
        assert_eq!(w[0], format!("push {COVERING_BRANCH} r9"));
        // Its description names this ticket, what came over and what's open.
        assert!(w[1].starts_with("body #52: ## Summary\nPR 52."));
        assert!(w[1].contains("## Also carries ABC-1"));
        assert!(w[1].contains("- [ ] Register the two extra helper files"));
        // The earlier pull request is marked superseded, never closed.
        assert!(w[2].starts_with("body #41: **Superseded by #52"));
        assert_eq!(w.len(), 3);
        let s = rec.superseded.unwrap();
        assert_eq!((s.number, s.closed), (41, false));
        assert!(rec.note.starts_with("Continued in #52"));
    }

    #[tokio::test]
    async fn a_diverged_branch_is_rebuilt_on_the_covering_one_then_moved_forward() {
        let mut f = live();
        f.compares.insert(
            (COVERING_BRANCH.into(), RUN_BRANCH.into()),
            Compared {
                status: "diverged".into(),
                ahead: 2,
                files: vec!["src/register.ts".into()],
            },
        );
        f.rebuilt = Some(Rebuilt {
            tip: "t7".into(),
            unresolved: vec![],
        });
        let rec = ended(take(&mut f, &covered(&[]), &inputs(&[])).await);
        assert_eq!(rec.outcome, "continued");
        assert!(rec.carried.unwrap().rebuilt);
        assert_eq!(f.writes()[0], format!("push {COVERING_BRANCH} t7"));
        // A file both changed that couldn't be merged: nothing is pushed.
        let mut f = live();
        f.compares.insert(
            (COVERING_BRANCH.into(), RUN_BRANCH.into()),
            Compared {
                status: "diverged".into(),
                ahead: 2,
                files: vec![],
            },
        );
        f.rebuilt = Some(Rebuilt {
            tip: "t8".into(),
            unresolved: vec!["src/register.ts".into()],
        });
        let rec = ended(take(&mut f, &covered(&[]), &inputs(&[])).await);
        assert_eq!(rec.outcome, "not_carried");
        assert!(rec.note.contains("couldn't be merged"));
        assert!(f.writes().is_empty(), "{:?}", f.writes());
    }

    #[tokio::test]
    async fn a_complete_covering_pull_request_ends_with_no_push() {
        let mut f = live();
        f.compares.insert(
            (COVERING_BRANCH.into(), RUN_BRANCH.into()),
            Compared {
                status: "behind".into(),
                ..Default::default()
            },
        );
        let rec = ended(take(&mut f, &covered(&[]), &inputs(&[])).await);
        assert_eq!(rec.outcome, "complete");
        assert!(rec.carried.is_none());
        assert!(rec
            .note
            .starts_with("#52 already has everything this ticket asks for"));
        let w = f.writes();
        assert!(!w.iter().any(|w| w.starts_with("push")), "{w:?}");
        assert!(!w.iter().any(|w| w.starts_with("body #52")), "{w:?}");
        // The redundant earlier pull request is still marked, to be closed
        // on a person's click.
        assert_eq!(w.len(), 1);
        assert!(w[0].starts_with("body #41: **Superseded by #52"));
        assert_eq!(rec.superseded.unwrap().number, 41);
    }

    #[tokio::test]
    async fn a_persons_pull_request_is_never_touched() {
        for (login, kind) in [
            ("octocat", "User"),
            ("coderhelm-fan", "User"),
            ("ci[bot]", "Bot"),
        ] {
            let mut f = live();
            f.pr(52, login, kind, COVERING_BRANCH, "c1");
            let rec =
                ended(take(&mut f, &covered(&["Register the helpers"]), &inputs(&["x"])).await);
            assert_eq!(rec.outcome, "person", "{login}");
            assert!(!rec.own);
            assert!(rec
                .note
                .starts_with(&format!("{login}'s pull request #52 covers this ticket")));
            assert!(f.writes().is_empty(), "{login}: {:?}", f.writes());
            // The earlier own pull request isn't marked either: nothing went
            // anywhere.
            assert!(rec.superseded.is_none());
        }
        // CoderHelm's bot on a pull request no run of the team recorded.
        let mut f = live();
        f.recorded = vec![41];
        let rec = ended(take(&mut f, &covered(&[]), &inputs(&[])).await);
        assert_eq!(rec.outcome, "person");
        assert!(f.writes().is_empty());
    }

    #[tokio::test]
    async fn a_covering_pull_request_merged_or_closed_meanwhile_is_the_normal_path() {
        for (state, merged) in [("closed", true), ("closed", false)] {
            let mut f = live();
            let pr = f.prs.get_mut(&52).unwrap();
            pr["state"] = json!(state);
            pr["merged"] = json!(merged);
            let t = take(&mut f, &covered(&["x"]), &inputs(&["y"])).await;
            assert_eq!(t, Taken::Gone(NotOwn::NotOpen { merged }));
            assert!(f.writes().is_empty());
        }
    }

    #[tokio::test]
    async fn a_covering_branch_that_moved_is_not_overwritten() {
        let mut f = live();
        f.heads
            .lock()
            .unwrap()
            .insert(COVERING_BRANCH.into(), "someone-pushed".into());
        let rec = ended(take(&mut f, &covered(&[]), &inputs(&[])).await);
        assert_eq!(rec.outcome, "not_carried");
        assert!(rec.note.contains("moved while the work was carried over"));
        assert!(f.writes().is_empty());
    }

    /// The check is in the write itself: an `OwnPr` for a person's pull
    /// request (as if a path skipped [`OwnPr::verify`]) refuses to push to,
    /// or edit, it.
    #[tokio::test]
    async fn the_write_refuses_a_pull_request_coderhelm_did_not_create_even_if_bypassed() {
        let mut f = live();
        f.pr(60, "octocat", "User", "feature/x", "p1");
        f.recorded.push(60);
        let facts = PrFacts::from_github(&f.prs[&60]).unwrap();
        let forged = OwnPr {
            facts,
            bot: Some(BOT.into()),
            opened_by_run: true,
        };
        assert_eq!(
            forged.fast_forward(&f, "evil").await,
            Err(Refused::NotOwn(NotOwn::Person("octocat".into())))
        );
        assert!(matches!(
            forged.set_body(&f, "x").await,
            Err(Refused::NotOwn(NotOwn::Person(_)))
        ));
        // A fork, or a pull request no run recorded, the same.
        let mut fork = f.prs[&52].clone();
        fork["head"]["repo"]["full_name"] = json!("someone/web");
        let forged = OwnPr {
            facts: PrFacts::from_github(&fork).unwrap(),
            bot: Some(BOT.into()),
            opened_by_run: true,
        };
        assert_eq!(
            forged.set_body(&f, "x").await,
            Err(Refused::NotOwn(NotOwn::Fork))
        );
        let forged = OwnPr {
            facts: PrFacts::from_github(&f.prs[&52]).unwrap(),
            bot: Some(BOT.into()),
            opened_by_run: false,
        };
        assert_eq!(
            forged.fast_forward(&f, "x").await,
            Err(Refused::NotOwn(NotOwn::NoRun))
        );
        assert!(f.writes().is_empty(), "{:?}", f.writes());
        // Verify itself never hands out one for a person's pull request.
        assert!(OwnPr::verify(&f, 60).await.is_err());
    }

    /// No path here reaches GitHub's write endpoints except through the
    /// [`Cover`] methods [`OwnPr`] guards; nothing in the covered path closes
    /// a pull request.
    #[test]
    fn only_own_pr_writes_to_a_covering_pull_request() {
        let src = include_str!("covering.rs");
        let body = src.split("#[cfg(test)]").next().unwrap();
        for raw in [
            "close_pull_request(",
            "update_pr_title(",
            "move_with_lease(",
            "create_merge_commit(",
            "merge_branch(",
            "batch_write(",
            "write_file(",
        ] {
            assert!(!body.contains(raw), "{raw} in the covered path");
        }
        // Each raw write is called once, by OwnPr, and the impl that forwards
        // it to GitHub.
        assert_eq!(body.matches(".raw_fast_forward(").count(), 1);
        assert_eq!(body.matches(".raw_set_body(").count(), 1);
        assert_eq!(body.matches(".fast_forward(self.owner").count(), 1);
        assert_eq!(body.matches(".update_pr_body(").count(), 1);
        let guarded = |name: &str| {
            let at = body.find(&format!("pub async fn {name}<")).unwrap();
            let f = &body[at..];
            let check = f.find("self.check()?").unwrap();
            let raw = f.find(".raw_").unwrap();
            check < raw
        };
        assert!(guarded("fast_forward") && guarded("set_body"));
    }

    /// The covered path counts as a pass before it writes anything: the loop
    /// guard and the run's ceiling (`run_guard::on_pass`) stop it first.
    #[test]
    fn the_loop_guard_and_ceiling_come_before_any_write() {
        let src = include_str!("covering.rs");
        let run = &src[src.find("pub async fn run(").unwrap()..];
        let guard = run
            .find("run_guard::on_pass(state, &msg.team_id, run_id).await?")
            .unwrap();
        let claim = run.find("pr_holder::claim(").unwrap();
        let take = run.find("take(&mut c, covered, inp)").unwrap();
        assert!(guard < claim && claim < take);
    }
}
