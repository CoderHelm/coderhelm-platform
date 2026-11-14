//! The one place a pull request's description is written from.
//!
//! A dependency run's pull request had the right code after a review round
//! and the wrong description: its table still showed the plan (five packages
//! as major version jumps the final manifest no longer makes), its headline
//! counted advisories nobody had compared with the final lockfile, and the
//! round that fixed the code said it could not edit the description and left
//! the corrections to a person.
//!
//! A description is a function of the head. Three rules keep it one:
//!
//! - [`save`] is the only call of the GitHub client's body write in the
//!   pipeline. Every writer (the first description, a section CoderHelm
//!   owns, a rewrite) sends its text through it, so no path can write a
//!   description this module does not see. A test reads the sources and
//!   fails on a call anywhere else.
//! - [`head_moved`] is what every path that moves the head calls (a CI fix,
//!   a review fix, feedback, a bump taken out): the description's prose is
//!   rewritten from the whole diff, and a dependency pull request's table,
//!   headline, "What to check" and Proof are rebuilt from what the lockfile
//!   on the head resolves ([`dependency_sections`]). The model is never the
//!   source of a version or a count.
//! - A round's account that hands an edit of the description to a person is
//!   caught ([`round_account`]): the sentence goes, and the rebuild runs
//!   even when the round pushed no code.
//!
//! A description is also a view of recorded facts, and nothing else:
//!
//! - [`build`] gathers the facts of the head (`description_facts::facts`:
//!   the diff, the head's check results, the ticket, a dependency update's
//!   lockfile facts, the steps outside the code) and gives the body its
//!   final shape (`description_facts::shape`): every written sentence is
//!   held against the facts, the lead a reader sees first is rendered from
//!   them, and everything else is collapsed. It runs for every kind of run.
//! - The stamp carries a digest of the text the fact check reads. A writer
//!   that changes that text any other way makes the description not current,
//!   so the check runs again before anything rests on it; a writer that only
//!   changes one of CoderHelm's own record sections does not.
//! - The model's rewrite is checked against the facts before it is saved
//!   ([`save_checked`]), and the reader pass ([`Reader`]) can only fill the
//!   lead's summary and check with what `pr_reader::accept` verifies.
//! - [`save`] lays every write out (`pr_layout::layout`), so no writer can
//!   leave a section in sight that belongs below, or a fold unbalanced.

use tracing::warn;

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::TokenUsage;
use crate::WorkerState;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Writes a pull request's description. The only caller of the client's
/// body write: a sentence that says the description can't be edited, or
/// leaves an edit of it to a person, never reaches a description.
pub async fn save(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    body: &str,
) -> Result<(), Error> {
    let body = clean(body);
    // What a reader sees first stays first, whoever wrote: the lead, then
    // everything else collapsed (a body never built from facts is left).
    let body = common::pr_layout::layout(&body);
    github.update_pr_body(owner, repo, number, &body).await
}

/// [`save`] for text a model wrote: every sentence in it is held against
/// `facts` first, and one the facts do not bear out is rewritten from them
/// or removed. With no facts in hand the text is saved as it is, and is not
/// current until [`reconcile`] has checked it (see [`is_current`]).
pub async fn save_checked(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    body: &str,
    facts: Option<&common::pr_facts::Facts>,
) -> Result<(), Error> {
    let checked = match facts {
        Some(f) => {
            let (text, corrections) = common::pr_claims::enforce(body, f);
            note_corrections(number, &corrections);
            text
        }
        None => body.to_string(),
    };
    save(github, owner, repo, number, &checked).await
}

fn note_corrections(number: u64, corrections: &[common::pr_claims::Correction]) {
    for c in corrections {
        warn!(
            pr_number = number,
            kind = ?c.kind,
            said = %common::truncate_str(&c.said, 200),
            why = %c.why,
            rewritten = c.wrote.is_some(),
            "A statement in the description did not match the recorded facts"
        );
    }
}

/// The description without what CoderHelm renders around the written text
/// (the lead, the folds, the steps and evidence): what a model is given to
/// rewrite, so it can neither reword a rendered part nor lose a fold.
pub fn flat(body: &str) -> String {
    let flat = common::pr_layout::unfold(body);
    let mut out = flat;
    for (start, end) in [
        (common::pr_facts::LEAD_START, common::pr_facts::LEAD_END),
        (
            common::pr_steps::EVIDENCE_START,
            common::pr_steps::EVIDENCE_END,
        ),
    ] {
        if let (Some(a), Some(b)) = (out.find(start), out.find(end)) {
            if b > a {
                out = format!("{}{}", &out[..a], out[b + end.len()..].trim_start());
            }
        }
    }
    out
}

/// A description about to be written for the first time, held against the
/// diff it is of: a change the diff does not contain never opens a pull
/// request. The head's checks have not run yet, so nothing is judged about
/// them here.
pub fn checked_for_diff(body: &str, diff: &serde_json::Value) -> String {
    let files = super::pr_risk::diff_files(diff);
    let facts = common::pr_facts::Facts {
        files: files
            .iter()
            .map(|(path, status, _)| common::pr_facts::FileChange {
                path: path.clone(),
                status: status.clone(),
                previous: String::new(),
            })
            .collect(),
        files_complete: files.len() < super::description_facts::COMPARE_FILE_LIMIT,
        checks_read: false,
        ..Default::default()
    };
    let (text, corrections) = common::pr_claims::enforce(body, &facts);
    note_corrections(0, &corrections);
    text
}

/// A model to ask for the reader pass (`common::pr_reader`).
pub struct Reader<'a> {
    pub provider: &'a ModelProvider,
    pub usage: &'a mut TokenUsage,
}

/// `body` as [`save`] writes it.
pub fn clean(body: &str) -> std::borrow::Cow<'_, str> {
    match common::description_handoff::scrub(body) {
        Some(scrubbed) => {
            warn!("A description said it could not be edited; that sentence was taken out");
            // The replacement sentence is for a round's account, not for
            // the description itself.
            std::borrow::Cow::Owned(
                scrubbed
                    .lines()
                    .filter(|l| {
                        l.trim().trim_start_matches(['-', '*', '>']).trim()
                            != common::description_handoff::REBUILT
                    })
                    .map(|l| {
                        l.replace(common::description_handoff::REBUILT, "")
                            .trim_end()
                            .to_string()
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
        }
        None => std::borrow::Cow::Borrowed(body),
    }
}

/// The pull request whose head moved.
pub struct Moved<'a> {
    pub state: &'a WorkerState,
    pub github: &'a GitHubClient,
    pub team_id: &'a str,
    pub installation_id: u64,
    pub owner: &'a str,
    pub repo: &'a str,
    pub number: u64,
    /// The run's ticket: a security ticket's pull request has the sections
    /// rebuilt from its lockfiles.
    pub ticket_id: &'a str,
    pub team_instructions: &'a str,
    /// What a person with write access asked to change in the description
    /// (empty when nobody asked).
    pub requests: &'a [String],
}

/// The head of an open pull request moved, or a person asked for a change to
/// its description: the description is rebuilt from the branch. Returns
/// what the rewrite changed (see `pr::refresh_description`).
pub async fn head_moved(
    m: &Moved<'_>,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Option<Vec<String>> {
    // The facts of the head, read before the rewrite: what the model writes
    // is held against them before it is saved.
    let before = gather(m.state, m.github, m.team_id, m.owner, m.repo, m.number).await;
    let facts = before.as_ref().ok().map(|g| g.facts(None));
    let changed = super::pr::refresh_description(
        m.state,
        m.github,
        (m.team_id, m.installation_id),
        m.owner,
        m.repo,
        m.number,
        m.team_instructions,
        m.requests,
        facts.as_ref(),
        provider,
        usage,
    )
    .await;
    // After the rewrite, so what the lockfile resolves is the last word on
    // every version and count, the lead is rendered from the facts, and the
    // description names the head it is of. The reader pass reads the
    // result once, as its reviewer will.
    build(
        m.state,
        m.github,
        m.team_id,
        m.owner,
        m.repo,
        m.number,
        m.ticket_id,
        true,
        Some(Reader { provider, usage }),
    )
    .await;
    changed
}

/// A dependency pull request's table, headline, "What to check" and Proof,
/// rebuilt from what the lockfile on its head resolves, and the stamp of
/// the head they are of. Costs no model call.
pub async fn dependency_sections(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
    ticket_id: &str,
) {
    rebuild(state, github, team_id, owner, repo, number, ticket_id).await;
}

// ── The head a description was built for ───────────────────────────────

/// A hidden line naming the head commit the description's computed
/// sections were built for. A description with no stamp, or another
/// head's, is rebuilt before anything rests on it.
pub const STAMP_START: &str = "<!-- coderhelm:built-for ";
const STAMP_END: &str = " -->";

/// The head the description says it was built for.
pub fn stamp_of(body: &str) -> Option<&str> {
    stamp_parts(body).map(|(head, _)| head)
}

/// The stamp's head and, when it carries one, the digest of the text the
/// fact check read.
fn stamp_parts(body: &str) -> Option<(&str, Option<&str>)> {
    let at = body.rfind(STAMP_START)? + STAMP_START.len();
    let rest = &body[at..];
    let inner = rest[..rest.find(STAMP_END)?].trim();
    let mut parts = inner.split_whitespace();
    let head = parts.next().filter(|h| !h.is_empty())?;
    Some((head, parts.next()))
}

fn without_stamp(body: &str) -> String {
    body.lines()
        .filter(|l| !l.trim_start().starts_with(STAMP_START))
        .collect::<Vec<_>>()
        .join("\n")
}

/// A digest of the text of `body` the fact check reads (`pr_claims`): the
/// written prose and the open items, never the sections CoderHelm renders
/// from records.
pub fn checked_digest(body: &str) -> String {
    common::pr_claims::checked_digest(&without_stamp(body))
}

/// `body` stamped as built for `head` (one stamp, on its own last line),
/// with the digest of the text that was checked.
pub fn with_stamp(body: &str, head: &str) -> String {
    let body = without_stamp(body);
    if head.is_empty() {
        return body.trim_end().to_string();
    }
    let digest = checked_digest(&body);
    format!(
        "{}\n\n{STAMP_START}{head} {digest}{STAMP_END}",
        body.trim_end()
    )
}

/// Whether the description was built for `head` and the text the fact check
/// read is still the text that is there. A stamp from before the digest
/// answers for its head alone.
pub fn is_current(body: &str, head: &str) -> bool {
    match stamp_parts(body) {
        Some((h, digest)) => {
            !head.is_empty() && h == head && digest.is_none_or(|d| d == checked_digest(body))
        }
        None => false,
    }
}

/// How a reconcile ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciled {
    /// The description was already built for the head (or the pull request
    /// is closed).
    Current,
    /// It was rebuilt and stamped for the head.
    Rebuilt,
    /// It could not be read or written: it may still be of another head.
    Failed(String),
}

/// Rebuilds the description when its stamp is not the pull request's head.
/// Every place that rests on the description calls this first: each step
/// that pushed (`reconcile_moved`), the checks and settle step, a review of
/// CoderHelm's own pull request, the pull request watch, and the end of the
/// run, which never completes while the two differ.
pub async fn reconcile(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
    ticket_id: &str,
) -> Reconciled {
    build(
        state, github, team_id, owner, repo, number, ticket_id, false, None,
    )
    .await
}

/// [`reconcile`] with a model for the reader pass: the first description of
/// a pull request, read once as its reviewer will.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_read(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
    ticket_id: &str,
    reader: Reader<'_>,
) -> Reconciled {
    build(
        state,
        github,
        team_id,
        owner,
        repo,
        number,
        ticket_id,
        true,
        Some(reader),
    )
    .await
}

/// [`reconcile`] whatever the stamp says: something the description shows
/// changed on the same head (its CI ended, the model rewrote its prose).
pub async fn rebuild(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
    ticket_id: &str,
) -> Reconciled {
    build(
        state, github, team_id, owner, repo, number, ticket_id, true, None,
    )
    .await
}

/// What was read about a pull request for one build.
struct Gathered {
    pr: serde_json::Value,
    files: Vec<(String, String, String)>,
    floor: super::pr_risk::Floor,
    checks: Option<Vec<super::reviewer_guide::Check>>,
    run: RunBits,
    run_url: String,
}

/// What the run behind a pull request holds for its description.
#[derive(Default)]
struct RunBits {
    ticket_id: String,
    title: String,
    source: String,
    issue_number: u64,
    decision: Option<super::fix_location::FixDecision>,
    /// Said once on the pull request about its title's form.
    convention_note: String,
    /// What the title's form was read from.
    convention_evidence: String,
}

impl Gathered {
    fn head(&self) -> &str {
        self.pr["head"]["sha"].as_str().unwrap_or("")
    }

    /// The facts of the head, with `dependency` when it is a dependency
    /// update's pull request.
    fn facts(
        &self,
        dependency: Option<common::pr_facts::DependencyFacts>,
    ) -> common::pr_facts::Facts {
        self.facts_of(self.pr["body"].as_str().unwrap_or(""), dependency)
    }

    fn facts_of(
        &self,
        body: &str,
        dependency: Option<common::pr_facts::DependencyFacts>,
    ) -> common::pr_facts::Facts {
        let reference = match self.run.source.as_str() {
            "github" if self.run.issue_number > 0 => format!("#{}", self.run.issue_number),
            _ => self.run.ticket_id.clone(),
        };
        let why = (!reference.is_empty()
            && !common::security_fixes::is_ticket(&self.run.ticket_id))
        .then(|| common::pr_facts::Why {
            reference,
            title: self.run.title.clone(),
            url: String::new(),
        });
        super::description_facts::facts(&super::description_facts::Inputs {
            head: self.head(),
            body,
            files: &self.files,
            floor: &self.floor,
            checks: self.checks.as_deref(),
            why,
            dependency,
            outside: self
                .run
                .decision
                .as_ref()
                .and_then(|d| d.outside_change.as_ref()),
            run_url: &self.run_url,
            notes: Some(self.run.convention_note.clone())
                .filter(|n| !n.trim().is_empty())
                .into_iter()
                .collect(),
            evidence: Some(self.run.convention_evidence.clone())
                .filter(|e| !e.trim().is_empty())
                .map(|e| format!("The title and commits follow {e}."))
                .into_iter()
                .collect(),
        })
    }
}

/// The attributes on a run that carry what its title's form was read from
/// and what the pull request says once about it (`conventions`).
pub const CONVENTION_EVIDENCE: &str = "convention_evidence";
pub const CONVENTION_NOTE: &str = "convention_note";

async fn run_bits(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
) -> (RunBits, String) {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let Some(run_id) =
        super::review_pr::lookup_run_by_pr(state, team_id, owner, repo, number).await
    else {
        return (RunBits::default(), String::new());
    };
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", A::S(team_id.to_string()))
        .key("run_id", A::S(run_id.clone()))
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .unwrap_or_default();
    let get = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    (
        RunBits {
            ticket_id: get("ticket_id"),
            title: get("title"),
            source: get("ticket_source"),
            issue_number: item
                .get("issue_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0),
            decision: super::outside_exec::decision_of(&item),
            convention_note: get(CONVENTION_NOTE),
            convention_evidence: get(CONVENTION_EVIDENCE),
        },
        run_id,
    )
}

/// Reads what a build needs: the pull request, its diff, the head's checks
/// and the run behind it.
async fn gather(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
) -> Result<Gathered, String> {
    let pr = github
        .get_pull_request(owner, repo, number)
        .await
        .map_err(|e| format!("the pull request could not be read: {e}"))?;
    let head = pr["head"]["sha"].as_str().unwrap_or("").to_string();
    let files = match (pr["base"]["ref"].as_str(), pr["head"]["ref"].as_str()) {
        (Some(base), Some(head_ref)) => match github.get_diff(owner, repo, base, head_ref).await {
            Ok(diff) => super::pr_risk::diff_files(&diff),
            Err(e) => {
                warn!(pr_number = number, error = %e, "The diff could not be read; nothing is judged against it");
                Vec::new()
            }
        },
        _ => Vec::new(),
    };
    let floor = super::pr_risk::floor(&files, &std::collections::HashMap::new());
    let checks = if head.is_empty() {
        None
    } else {
        github
            .list_ci_for_ref(owner, repo, &head)
            .await
            .ok()
            .map(|l| super::reviewer_guide::checks_of(&l))
    };
    let (run, run_id) = run_bits(state, team_id, owner, repo, number).await;
    let app = common::public_urls::app_url(&state.config.stage);
    let run_url = if run_id.is_empty() || app.is_empty() {
        String::new()
    } else {
        format!("{app}/runs/detail?id={run_id}")
    };
    Ok(Gathered {
        pr,
        files,
        floor,
        checks,
        run,
        run_url,
    })
}

/// Asks the reader pass's one question. `None` when the model did not
/// answer: the lead then keeps what it had, or the records.
async fn ask_reader(
    state: &WorkerState,
    reader: Reader<'_>,
    facts: &common::pr_facts::Facts,
    body: &str,
) -> Option<String> {
    let written = super::description_facts::written(body);
    if written.trim().is_empty() {
        return None;
    }
    match crate::agent::provider::converse_simple(
        state,
        reader.provider,
        reader.provider.primary_model_id(),
        common::pr_reader::SYSTEM,
        &common::pr_reader::prompt(facts, &written),
        reader.usage,
    )
    .await
    {
        Ok(t) => Some(t),
        Err(e) => {
            warn!(error = %e, "The reader pass got no answer; the lead keeps what the records say");
            None
        }
    }
}

/// CoderHelm's own review threads that said the description was behind the
/// code are settled by the rebuild: they are resolved. Only threads its own
/// reviewer opened, never a person's. An app that may not resolve threads
/// is said once and the threads stay open.
async fn resolve_own_description_threads(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
) {
    let Ok(threads) = github.list_bot_review_threads(owner, repo, number).await else {
        return;
    };
    for t in threads
        .iter()
        .filter(|t| !t.resolved && super::own_review::says_description_is_behind(&t.body))
    {
        if let Err(e) = github.resolve_review_thread(&t.id).await {
            if common::github_grants::refused(github.installation_id(), "resolve_review_thread") {
                warn!(pr_number = number, error = %e, "CoderHelm's own review thread about the description could not be resolved (the app needs Pull requests: write); it stays open");
            }
            return;
        }
        tracing::info!(pr_number = number, thread = %t.id, "Resolved CoderHelm's own review thread about the description: it was rebuilt");
    }
}

#[allow(clippy::too_many_arguments)]
async fn build(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
    ticket_id: &str,
    force: bool,
    reader: Option<Reader<'_>>,
) -> Reconciled {
    if number == 0 {
        return Reconciled::Current;
    }
    let pr = match github.get_pull_request(owner, repo, number).await {
        Ok(p) => p,
        Err(e) => return Reconciled::Failed(format!("the pull request could not be read: {e}")),
    };
    if pr["state"].as_str().is_some_and(|s| s != "open") {
        return Reconciled::Current;
    }
    let body = pr["body"].as_str().unwrap_or("").to_string();
    let head = pr["head"]["sha"].as_str().unwrap_or("").to_string();
    if head.is_empty() {
        return Reconciled::Failed("the pull request names no head".into());
    }
    if !force && is_current(&body, &head) {
        return Reconciled::Current;
    }
    // Everything computed from the branch, for this head: a dependency
    // pull request's table, counts, "What to check" and Proof, and the
    // lockfile facts they are written from.
    let computed = if common::security_fixes::is_ticket(ticket_id) {
        super::security_fixes::at_head(
            state,
            github,
            team_id,
            &format!("{owner}/{repo}"),
            number,
            ticket_id,
            &pr,
        )
        .await
    } else {
        None
    };
    let (base, dependency) = match computed {
        Some((b, d)) => (b, Some(d)),
        None => (body.clone(), None),
    };
    // The facts of this head, and the description as a view of them: every
    // written sentence checked, the lead rendered, the rest collapsed.
    let next = match gather(state, github, team_id, owner, repo, number).await {
        Ok(g) if g.head() == head => {
            let facts = g.facts_of(&base, dependency);
            let reply = match reader {
                Some(r) => ask_reader(state, r, &facts, &base).await,
                None => None,
            };
            let shaped = super::description_facts::shape(
                &base,
                pr["title"].as_str().unwrap_or(""),
                facts,
                reply.as_deref(),
            );
            note_corrections(number, &shaped.corrections);
            tracing::info!(
                pr_number = number,
                corrected = shaped.corrections.len(),
                out_of_the_ordinary = common::pr_facts::anomalies(&shaped.facts).len(),
                needed_from_a_person = common::pr_facts::needed_lines(&shaped.facts).len(),
                "Description shaped from the facts of the head"
            );
            shaped.body
        }
        // The head moved while it was read, or it could not be read: the
        // computed sections are saved, and the next reconcile builds again.
        Ok(_) => {
            return Reconciled::Failed("the head moved while the description was built".into())
        }
        Err(why) => {
            warn!(
                pr_number = number,
                why, "The facts of the head could not be read; the description keeps its shape"
            );
            base
        }
    };
    let next = with_stamp(&next, &head);
    if next == body {
        return Reconciled::Current;
    }
    match save(github, owner, repo, number, &next).await {
        Ok(()) => {
            tracing::info!(pr_number = number, %head, was = stamp_of(&body).unwrap_or("none"), "Description rebuilt for the head");
            resolve_own_description_threads(github, owner, repo, number).await;
            Reconciled::Rebuilt
        }
        Err(e) => Reconciled::Failed(format!("the description could not be saved: {e}")),
    }
}

/// [`reconcile`] for a run's pull request, its ticket read from the run.
pub async fn reconcile_run(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    run_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
) -> Reconciled {
    let ticket = run_ticket(state, team_id, run_id).await;
    reconcile(state, github, team_id, owner, repo, number, &ticket).await
}

async fn run_ticket(state: &WorkerState, team_id: &str, run_id: &str) -> String {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", A::S(team_id.to_string()))
        .key("run_id", A::S(run_id.to_string()))
        .projection_expression("ticket_id")
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| i.get("ticket_id").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default()
}

/// Every branch CoderHelm moved while handling a message gets its pull
/// request's description rebuilt for the new head. Called once, where
/// every message ends (`main::handle_message`); the branches come from the
/// one place every request leaves the GitHub client, so no code path that
/// pushes has to remember anything.
pub async fn reconcile_moved(state: &WorkerState, team_id: Option<&str>) {
    let moved = crate::clients::github::take_moved_branches();
    let Some(team_id) = team_id else {
        return;
    };
    for m in moved {
        let Ok(github) = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            m.installation_id,
            &state.http,
        ) else {
            continue;
        };
        let Ok(Some(pr)) = github
            .find_open_pr_for_branch(&m.owner, &m.repo, &m.branch)
            .await
        else {
            continue;
        };
        let Some(number) = pr["number"].as_u64() else {
            continue;
        };
        // Only a run's own pull request: its run names its ticket.
        let Some(run_id) =
            super::review_pr::lookup_run_by_pr(state, team_id, &m.owner, &m.repo, number).await
        else {
            continue;
        };
        if let Reconciled::Failed(why) =
            reconcile_run(state, &github, team_id, &run_id, &m.owner, &m.repo, number).await
        {
            warn!(pr_number = number, why, "The description could not be rebuilt after a push; the next step that reads it rebuilds it");
        }
    }
}

/// How an [`edit`] ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Edited {
    Saved(String),
    Unchanged,
    /// The pull request's head is no longer the one the edit is about.
    HeadMoved,
    Failed(String),
}

/// Changes one part of a description on the description as GitHub has it
/// now, never on a copy read earlier: a review that ran for minutes once
/// wrote back the copy it had read at its start, with its own section
/// added, and undid every write made in between. `of_head`: the head the
/// change is about; when the pull request has moved past it, nothing is
/// written (a review of a superseded head says nothing about this one).
pub async fn edit(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    of_head: Option<&str>,
    change: impl FnOnce(&str) -> String,
) -> Edited {
    let pr = match github.get_pull_request(owner, repo, number).await {
        Ok(p) => p,
        Err(e) => return Edited::Failed(e.to_string()),
    };
    let body = pr["body"].as_str().unwrap_or("");
    let head = pr["head"]["sha"].as_str().unwrap_or("");
    if of_head.is_some_and(|h| !h.is_empty() && !head.is_empty() && h != head) {
        return Edited::HeadMoved;
    }
    let next = change(body);
    // As it would be written: a change that lays out to what is there is no
    // change, and costs no write.
    if next == body || common::pr_layout::layout(&clean(&next)) == body {
        return Edited::Unchanged;
    }
    match save(github, owner, repo, number, &next).await {
        Ok(()) => Edited::Saved(next),
        Err(e) => Edited::Failed(e.to_string()),
    }
}

/// A round's account, checked: `(the account without any sentence that
/// hands the description to a person, whether there was one)`. A `true`
/// means the description is rebuilt now, whatever the round pushed.
pub fn round_account(account: &str) -> (String, bool) {
    match common::description_handoff::scrub(account) {
        Some(clean) => (clean, true),
        None => (account.to_string(), false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every source file of the worker, with its path from `src/`.
    fn sources() -> Vec<(String, String)> {
        fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<(String, String)>) {
            for e in std::fs::read_dir(dir).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, root, out);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push((
                        p.strip_prefix(root)
                            .unwrap()
                            .to_string_lossy()
                            .replace('\\', "/"),
                        std::fs::read_to_string(&p).unwrap(),
                    ));
                }
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut out = vec![];
        walk(&root, &root, &mut out);
        assert!(out.len() > 100, "the worker's sources were not found");
        out
    }

    /// The code of a file: no comment lines, and nothing from its tests on.
    fn code(text: &str) -> String {
        let end = text.find("#[cfg(test)]").unwrap_or(text.len());
        text[..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The guard: a new call site that writes a description some other way
    /// fails here.
    #[test]
    fn the_description_is_written_from_one_place() {
        let call = [".update_pr", "_body("].concat();
        let mut calls: Vec<String> = vec![];
        let mut through = 0;
        for (path, text) in sources() {
            let code = code(&text);
            // The client defines the write; it never calls it.
            if path != "clients/github.rs" {
                for _ in code.matches(&call) {
                    calls.push(path.clone());
                }
            }
            // A description sent as a raw request would pass the client's
            // method by.
            if path != "clients/github.rs" {
                for l in code.lines() {
                    assert!(
                        !(l.contains("/pulls/") && l.contains(".patch(")),
                        "{path} patches a pull request itself: {l}"
                    );
                }
            }
            through += code.matches("description::save(").count();
        }
        assert_eq!(
            calls,
            vec!["passes/description.rs".to_string()],
            "a description is written only through description::save"
        );
        assert!(through >= 20, "the writers go through it: {through}");
        // And the client has one method that replaces a description.
        let client = include_str!("../clients/github.rs");
        assert_eq!(
            client.matches("\n    pub async fn update_pr_body(").count(),
            1
        );
    }

    /// Every write is laid out and every rebuild is a view of the facts of
    /// its head: the one save lays the body out, the model's text is held
    /// against the facts before it is saved, and a rebuild shapes the body
    /// from the facts (fact check, reader pass, lead, folds) and settles
    /// CoderHelm's own thread about the description. No other module runs
    /// the fact check or the layout on a description by itself.
    #[test]
    fn every_write_is_laid_out_and_every_rebuild_is_shaped_from_the_facts() {
        let me = code(include_str!("description.rs"));
        let body_of = |start: &str, end: &str| {
            let s = &me[me.find(start).unwrap_or_else(|| panic!("{start}"))..];
            s[..s.find(end).unwrap_or_else(|| panic!("{end}"))].to_string()
        };
        let save = body_of("\npub async fn save(", "\npub async fn save_checked(");
        assert!(save.contains("clean(body)"));
        assert!(save.contains("pr_layout::layout("));
        let checked = body_of("\npub async fn save_checked(", "\nfn note_corrections(");
        assert!(checked.contains("pr_claims::enforce("));
        assert!(checked.contains("save(github"));
        let build = body_of("\nasync fn build(", "\npub async fn reconcile_run(");
        for step in [
            "facts_of(",
            "ask_reader(",
            "description_facts::shape(",
            "with_stamp(",
            "save(github",
            "resolve_own_description_threads(",
        ] {
            assert!(build.contains(step), "a rebuild runs {step}");
        }
        // The shaping runs in one place: the eval and the incident cases
        // (tests) run the same function.
        for (path, text) in sources() {
            let code = code(&text);
            if path.starts_with("passes/incidents/")
                || [
                    "passes/description.rs",
                    "passes/description_facts.rs",
                    "passes/description_eval.rs",
                ]
                .contains(&path.as_str())
            {
                continue;
            }
            assert!(
                !code.contains("description_facts::shape("),
                "{path} shapes a description outside the rebuild"
            );
        }
    }

    /// Every path that moves a pull request's head rebuilds its
    /// description, through this module.
    #[test]
    fn every_path_that_moves_the_head_rebuilds_the_description() {
        let resume = include_str!("resume.rs");
        let round = &resume[resume.find("\nasync fn run_fix_round(").unwrap()..];
        let round = &round[..round[1..].find("\nasync fn ").unwrap()];
        let pushed = round.find("Landed::Pushed { files }").unwrap();
        assert!(round[pushed..].contains("description::head_moved("));
        assert!(
            round.contains("description::round_account("),
            "a round's account is checked for a handed-off description"
        );
        // A bump CI took out.
        let revert = &resume[resume.find("\nasync fn revert_dep_commit(").unwrap()..];
        let revert = &revert[..revert[1..].find("\nasync fn ").unwrap_or(revert.len() - 1)];
        assert!(revert.contains("description::dependency_sections("));
        // Feedback from a person or a reviewer.
        let feedback = include_str!("feedback.rs");
        assert!(feedback.contains("description::head_moved("));
        assert!(feedback.contains("description::round_account("));
        // Nothing calls the rewrite past this module.
        for (path, text) in sources() {
            // The incident cases quote the pipeline's source; they call nothing.
            if path == "passes/description.rs"
                || path == "passes/pr.rs"
                || path.starts_with("passes/incidents/")
            {
                continue;
            }
            assert!(
                !code(&text).contains("pr::refresh_description("),
                "{path} rewrites the description past description::head_moved"
            );
        }
        // The dependency sections come from the head in every writer.
        let sec = code(include_str!("security_fixes.rs"));
        assert_eq!(sec.matches("sec::pr_body_linked(").count(), 0);
        assert!(sec.matches("sec::pr_body_at_head(").count() >= 3);
    }

    #[test]
    fn a_description_names_the_head_it_was_built_for() {
        let body = "## Summary\n\nKept glob-walk on the 2 line.\n";
        assert_eq!(stamp_of(body), None);
        assert!(!is_current(body, "aaa111"));
        let stamped = with_stamp(body, "aaa111");
        assert_eq!(stamp_of(&stamped), Some("aaa111"));
        assert!(is_current(&stamped, "aaa111"));
        assert!(!is_current(&stamped, "bbb222"), "another head: rebuilt");
        // One stamp, replaced in place, wherever a writer left the old one.
        let moved = format!("{stamped}\n\n## Open items\n- One thing.\n");
        let again = with_stamp(&moved, "bbb222");
        assert_eq!(again.matches(STAMP_START).count(), 1);
        assert_eq!(stamp_of(&again), Some("bbb222"));
        assert!(again.contains("- One thing."));
        assert_eq!(
            with_stamp(&again, "bbb222"),
            again,
            "stamping is idempotent"
        );
    }

    /// Every place that rests on the description reconciles it first, and
    /// a run never completes while stamp and head differ.
    #[test]
    fn everything_that_rests_on_the_description_reconciles_it_first() {
        let main = include_str!("../main.rs");
        let handle = &main[main.find("\nasync fn handle_message(").unwrap()..];
        let handle = &handle[..handle.find("\nasync fn handle_message_inner(").unwrap()];
        assert!(handle.contains("description::reconcile_moved("));
        let pr = include_str!("pr.rs");
        let checks = &pr[pr.find("pub async fn apply_checks(").unwrap()..];
        let checks = &checks[..checks.find("\n}\n").unwrap()];
        assert!(
            checks.contains("description::reconcile("),
            "the checks and settle step"
        );
        let run = &pr[pr.find("\npub async fn run(").unwrap()..];
        let run = &run[..run.find("\nasync fn notify_alert_pr(").unwrap()];
        assert!(
            run.contains("description::reconcile_read("),
            "the first description, read once as its reviewer will"
        );
        assert!(
            run.contains("description::checked_for_diff("),
            "and held against its diff before the pull request opens"
        );
        let review = include_str!("review_pr.rs");
        assert!(
            review.contains("description::reconcile_run("),
            "before a review of its own pull request"
        );
        assert!(
            review.contains("description::edit("),
            "a review writes on the description as it is now"
        );
        let watch = include_str!("pr_watch.rs");
        assert!(
            watch.contains("description::reconcile_run("),
            "the pull request watch"
        );
        let resume = include_str!("resume.rs");
        let done = &resume[resume.find("\nasync fn complete_run_with_status(").unwrap()..];
        let gate = done
            .find("description::reconcile(")
            .expect("the end of the run");
        let redrive = done.find("redrive_stale_items(").unwrap();
        assert!(gate < redrive, "before anything else at completion");
        assert!(
            done[gate..redrive].contains("return Err("),
            "a run never completes on another head's description"
        );
        // The dependency sections are computed in one function, called only
        // from here.
        for (path, text) in sources() {
            if path == "passes/description.rs" || path.starts_with("passes/incidents/") {
                continue;
            }
            assert!(
                !code(&text).contains("security_fixes::at_head("),
                "{path} builds the dependency sections past description::reconcile"
            );
        }
    }

    #[test]
    fn a_description_never_says_it_cannot_be_edited() {
        let body = "## Summary\n\nKept glob-walk on the 2 line.\n\n## Open items\n- I can't edit the PR description from here.\n- The studio build was not run.\n";
        let out = clean(body);
        assert!(!out.contains("can't edit"), "{out}");
        assert!(out.contains("- The studio build was not run."), "{out}");
        assert!(!out.contains(common::description_handoff::REBUILT), "{out}");
        let fine = "## Summary\n\nKept glob-walk on the 2 line.\n";
        assert!(matches!(clean(fine), std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn a_rounds_handoff_becomes_the_rebuild() {
        let (account, rebuild) = round_account(
            "Scoped the override to the 2 line. I can't edit the PR description from here, so the table still says 4.0.4.",
        );
        assert!(rebuild);
        assert_eq!(
            account,
            format!(
                "Scoped the override to the 2 line. {}",
                common::description_handoff::REBUILT
            )
        );
        assert_eq!(
            round_account("Scoped the override to the 2 line."),
            ("Scoped the override to the 2 line.".to_string(), false)
        );
    }
}
