//! The two-way proof of a pull request: each ticket criterion that names a
//! page or what a person sees, run end to end on the base branch's
//! deployment (staging, where it must fail) and on the pull request's
//! preview (where it must pass), before the pull request is called done.
//! The rules are `common::e2e_proof`'s.
//!
//! - [`prove`] runs once per head, from the pull request watch, once the
//!   head's preview is up and its own end-to-end checks passed. Each
//!   criterion's test is the one the plan's `## Criteria map` names, else a
//!   changed spec that loads the criterion's page. It runs in the sandbox
//!   through `ci_repro::two_way`, the same runner a red check is
//!   reproduced with, and both results are kept on the run
//!   (`e2e_proof`) and said on the run page.
//! - What the coder must do (a test that passes on both, a criterion that
//!   fails on the preview, a page with no test) goes back to it as a fix
//!   round (`resume::queue_own_review_round`), and the ready decision waits
//!   for it ([`standing`]).
//! - A side that couldn't run is labelled unverified in the pull request
//!   with its reason; it never passes silently.

use common::e2e_proof::{self as ep, Proof};
use common::preview::{Stage, State};
use tracing::{info, warn};

use super::attr_s;
use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// Criteria proven per head at most: each is a sandbox run.
pub const MAX_CRITERIA: usize = 4;
/// Spec files read from the head at most.
const MAX_SPECS: usize = 8;
/// The check name a proof's record carries.
pub const CHECK: &str = "two-way proof";

/// The proofs kept on the run.
pub(crate) async fn load(state: &WorkerState, team_id: &str, run_id: &str) -> Vec<Proof> {
    run_strings(state, team_id, run_id, &[ep::FIELD])
        .await
        .remove(0)
        .map(|s| ep::from_json(&s))
        .unwrap_or_default()
}

async fn save(state: &WorkerState, team_id: &str, run_id: &str, proofs: &[Proof]) {
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #f = :v")
        .expression_attribute_names("#f", ep::FIELD)
        .expression_attribute_values(":v", attr_s(&ep::to_json(proofs)))
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not keep the two-way proof on the run");
    }
}

/// String attributes of the run, in the order asked.
async fn run_strings(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    keys: &[&str],
) -> Vec<Option<String>> {
    let item = if run_id.is_empty() {
        None
    } else {
        state
            .dynamo
            .get_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(team_id))
            .key("run_id", attr_s(run_id))
            .send()
            .await
            .ok()
            .and_then(|o| o.item)
    };
    keys.iter()
        .map(|k| {
            item.as_ref()
                .and_then(|i| i.get(*k))
                .and_then(|v| v.as_s().ok())
                .cloned()
        })
        .collect()
}

/// The run's criteria that need an end-to-end proof: the ticket's, else
/// the plan's criteria map (`ep::criteria_or_map`), so a run whose ticket
/// text gives none (an empty issue body) is still proven on what its plan
/// said it does.
async fn criteria_of(state: &WorkerState, team_id: &str, run_id: &str) -> Vec<ep::Criterion> {
    let v = run_strings(state, team_id, run_id, &["title", "ticket_body", "repo"]).await;
    let (title, body, repo) = (
        v[0].clone().unwrap_or_default(),
        v[1].clone().unwrap_or_default(),
        v[2].clone().unwrap_or_default(),
    );
    let acceptance = super::stop_judge::acceptance(&body);
    let from_ticket = ep::criteria(&title, &body, &acceptance);
    let map = match repo.split_once('/') {
        Some((owner, name)) if !from_ticket.iter().any(ep::Criterion::needs_e2e) => {
            ep::parse_map(&design_of(state, team_id, run_id, owner, name).await)
        }
        _ => Vec::new(),
    };
    ep::criteria_or_map(&title, &body, &acceptance, &map)
        .into_iter()
        .filter(ep::Criterion::needs_e2e)
        .collect()
}

// ── The served page ─────────────────────────────────────────────────────────

/// The hosts a served-page fetch may reach: the preview's and the base
/// deployment's, both from CoderHelm's own records, never from a model or
/// the change. Every other `web_check` rule (https, public addresses,
/// redirects re-checked) still holds.
struct OwnDeployments(Vec<String>);

#[async_trait::async_trait]
impl super::web_check::HostGate for OwnDeployments {
    async fn allowed(&self, host: &str) -> bool {
        self.0.iter().any(|h| h.eq_ignore_ascii_case(host))
    }
}

fn host_of(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
}

/// `page` (a site path) on the deployment at `origin`.
pub fn page_url(origin: &str, page: &str) -> String {
    let page = page.trim();
    if page.starts_with("https://") || page.starts_with("http://") {
        return page.to_string();
    }
    format!(
        "{}/{}",
        origin.trim().trim_end_matches('/'),
        page.trim_start_matches('/')
    )
}

/// One plain GET of `url` as the server sends it: no cookies, no script
/// run. The page's view for the judgment comes back with it.
async fn fetch_served(url: &str, gate: &OwnDeployments) -> (ep::Served, String) {
    let req = common::coder_shell::Fetch {
        urls: vec![url.to_string()],
        head: false,
        follow: true,
        headers: Vec::new(),
        body: true,
    };
    match super::web_check::fetch_page(url, &req, gate, &super::web_check::Net).await {
        Ok(f) => {
            let view = ep::served_view(&String::from_utf8_lossy(&f.body));
            let note = f.stopped.clone().unwrap_or_default();
            (
                ep::Served {
                    url: url.to_string(),
                    fetched: f.status > 0,
                    status: f.status,
                    note,
                    ..Default::default()
                },
                view,
            )
        }
        Err(e) => (
            ep::Served {
                url: url.to_string(),
                note: e.text().to_string(),
                ..Default::default()
            },
            String::new(),
        ),
    }
}

/// One thing the judgment reads: a criterion on one side's served page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedCheck {
    pub criterion: String,
    /// `preview` or `staging`.
    pub side: &'static str,
    pub url: String,
    pub view: String,
}

pub const SERVED_SYSTEM: &str = "You check whether a web page, exactly as its server sends it \
(its HTML before any script runs: what a visitor with JavaScript off, a crawler, or `curl` gets), \
satisfies one acceptance criterion of a ticket. Scripts were taken out of the page; JSON-LD \
structured data was kept. Answer only from the page text given: `yes` when the page shows what \
the criterion asks, `no` when it doesn't (the content is missing, only a placeholder or empty \
container is there, or it would need a script to appear), `cant_tell` when the criterion can't \
be judged from served HTML at all. Quote the part of the page that settles it, or say in a few \
words what is missing.";

/// The judgment's prompt: each distinct page once, then the checks.
pub fn served_prompt(checks: &[ServedCheck]) -> String {
    let mut urls: Vec<&str> = Vec::new();
    for c in checks {
        if !urls.contains(&c.url.as_str()) {
            urls.push(&c.url);
        }
    }
    let mut out = String::new();
    for (i, u) in urls.iter().enumerate() {
        let view = checks
            .iter()
            .find(|c| c.url == *u)
            .map(|c| c.view.as_str())
            .unwrap_or("");
        out.push_str(&format!(
            "<page id=\"{i}\" url=\"{u}\">\n{view}\n</page>\n\n"
        ));
    }
    out.push_str("Checks:\n");
    for (i, c) in checks.iter().enumerate() {
        let page = urls.iter().position(|u| *u == c.url).unwrap_or(0);
        out.push_str(&format!("{i}. page {page} ({}): {}\n", c.side, c.criterion));
    }
    out
}

pub fn served_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "verdicts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "check": {"type": "integer"},
                        "shown": {"type": "string", "enum": ["yes", "no", "cant_tell"]},
                        "quote": {"type": "string"}
                    },
                    "required": ["check", "shown", "quote"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["verdicts"],
        "additionalProperties": false
    })
}

/// Each check's verdict from the judgment's answer: `(shown, quote)`, with
/// `None` for a check it didn't answer or couldn't tell.
pub fn apply_served(v: &serde_json::Value, n: usize) -> Vec<(Option<bool>, String)> {
    let mut out = vec![(None, String::new()); n];
    for item in v["verdicts"].as_array().into_iter().flatten() {
        let Some(i) = item["check"]
            .as_u64()
            .map(|i| i as usize)
            .filter(|i| *i < n)
        else {
            continue;
        };
        let shown = match item["shown"].as_str() {
            Some("yes") => Some(true),
            Some("no") => Some(false),
            _ => None,
        };
        let quote =
            common::truncate_str(item["quote"].as_str().unwrap_or("").trim(), 300).to_string();
        out[i] = (shown, quote);
    }
    out
}

/// The served pages judged against their criteria. A page the server
/// didn't answer with is settled without asking; a judgment that can't be
/// asked leaves each one unjudged (never a pass).
async fn judge_served(
    state: &WorkerState,
    team_id: &str,
    checks: &[ServedCheck],
) -> Vec<(Option<bool>, String)> {
    if checks.is_empty() {
        return Vec::new();
    }
    let provider = match crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Served-page proof: no model to judge with");
            return vec![(None, format!("no model to judge with ({e})")); checks.len()];
        }
    };
    let mut usage = crate::models::TokenUsage::default();
    match crate::agent::anthropic::converse_json(
        &provider.client(),
        provider.judgment_model_id(),
        SERVED_SYSTEM,
        &served_prompt(checks),
        &served_schema(),
        true,
        common::stop_judge::JUDGMENT_EFFORT,
        4096,
        &mut usage,
    )
    .await
    {
        Ok(v) => apply_served(&v, checks.len()),
        Err(e) => {
            warn!(error = %e, "Served-page proof: the judgment didn't answer");
            vec![(None, format!("the judgment didn't answer ({e})")); checks.len()]
        }
    }
}

/// The served-page legs of each criterion about the served page
/// (`Criterion::about_served`), by criterion text: its page fetched on the
/// preview and on the base deployment, and judged.
async fn served_legs(
    state: &WorkerState,
    github: &GitHubClient,
    p: super::preview::Pr<'_>,
    st: &State,
    criteria: &[ep::Criterion],
) -> std::collections::HashMap<String, (ep::Served, Option<ep::Served>)> {
    let mut out = std::collections::HashMap::new();
    let wanted: Vec<&ep::Criterion> = criteria
        .iter()
        .filter(|c| c.about_served() && c.page.is_some())
        .collect();
    if wanted.is_empty() || st.url.trim().is_empty() {
        return out;
    }
    let tree: Vec<String> = github
        .get_tree(p.owner, p.repo, &st.head)
        .await
        .map(|t| t.into_iter().map(|e| e.path).collect())
        .unwrap_or_default();
    let configured =
        super::ci_repro::test_address(state, p.team_id, &format!("{}/{}", p.owner, p.repo)).await;
    let base = super::ci_repro::base_address(
        github,
        (p.owner, p.repo, &st.head),
        &tree,
        configured.as_deref(),
    )
    .await;
    let gate = OwnDeployments(
        [Some(st.url.as_str()), base.as_deref()]
            .into_iter()
            .flatten()
            .filter_map(host_of)
            .collect(),
    );
    let mut fetched: std::collections::HashMap<String, (ep::Served, String)> =
        std::collections::HashMap::new();
    let mut checks: Vec<ServedCheck> = Vec::new();
    let mut slots: Vec<(String, &'static str)> = Vec::new();
    for c in &wanted {
        let page = c.page.as_deref().unwrap_or("");
        let sides = [
            ("preview", Some(st.url.as_str())),
            ("staging", base.as_deref()),
        ];
        for (side, origin) in sides {
            let Some(origin) = origin else { continue };
            let url = page_url(origin, page);
            if !fetched.contains_key(&url) {
                let got = fetch_served(&url, &gate).await;
                fetched.insert(url.clone(), got);
            }
            let (served, view) = &fetched[&url];
            if served.ok() && !view.is_empty() {
                checks.push(ServedCheck {
                    criterion: c.text.clone(),
                    side,
                    url: url.clone(),
                    view: view.clone(),
                });
                slots.push((c.text.clone(), side));
            }
        }
    }
    let verdicts = judge_served(state, p.team_id, &checks).await;
    for c in &wanted {
        let page = c.page.as_deref().unwrap_or("");
        let leg = |side: &str, origin: &str| {
            let url = page_url(origin, page);
            let mut s = fetched
                .get(&url)
                .map(|(s, _)| s.clone())
                .unwrap_or_default();
            if let Some(i) = slots.iter().position(|(t, sd)| *t == c.text && *sd == side) {
                if let Some((shown, quote)) = verdicts.get(i) {
                    s.shown = *shown;
                    if shown.is_some() {
                        s.quote = quote.clone();
                    } else if s.note.is_empty() {
                        s.note = quote.clone();
                    }
                }
            }
            s
        };
        let change = leg("preview", &st.url);
        let base_leg = base.as_deref().map(|b| leg("staging", b));
        out.insert(c.text.clone(), (change, base_leg));
    }
    out
}

/// The plan's design.md for the run's ticket and repository, empty when
/// it can't be read.
pub(crate) async fn design_of(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    owner: &str,
    repo: &str,
) -> String {
    let v = run_strings(state, team_id, run_id, &["ticket_id"]).await;
    let Some(ticket) = v[0].clone() else {
        return String::new();
    };
    let key = format!(
        "{}/design.md",
        super::openspec_prefix(team_id, &ticket, owner, repo)
    );
    match state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(&key)
        .send()
        .await
    {
        Ok(o) => o
            .body
            .collect()
            .await
            .map(|b| String::from_utf8_lossy(&b.into_bytes()).to_string())
            .unwrap_or_default(),
        Err(_) => String::new(),
    }
}

/// Runs each criterion's test both ways on the head, once per head, once
/// the preview is up. Returns the run's proofs and whether new ones were
/// made now.
pub(crate) async fn prove(
    state: &WorkerState,
    github: &GitHubClient,
    p: super::preview::Pr<'_>,
    st: &State,
    branch: &str,
) -> (Vec<Proof>, bool) {
    let all = load(state, p.team_id, p.run_id).await;
    if !matches!(st.stage, Stage::Ready | Stage::Checked)
        || st.url.trim().is_empty()
        || st.head.is_empty()
        || !ep::on_head(&all, &st.head).is_empty()
    {
        return (all, false);
    }
    let criteria = criteria_of(state, p.team_id, p.run_id).await;
    if criteria.is_empty() {
        return (all, false);
    }
    let map = ep::parse_map(&design_of(state, p.team_id, p.run_id, p.owner, p.repo).await);
    // The end-to-end specs the pull request changes, as they are on the head.
    let base = github
        .get_pull_request(p.owner, p.repo, p.number)
        .await
        .ok()
        .and_then(|pr| pr["base"]["ref"].as_str().map(str::to_string))
        .unwrap_or_else(|| "main".to_string());
    let changed: Vec<String> = github
        .compare_files_with_patches(p.owner, p.repo, &base, &st.head)
        .await
        .map(|v| {
            v.into_iter()
                .filter(|(path, status, _)| status != "removed" && ep::is_e2e_path(path))
                .map(|(path, _, _)| path)
                .collect()
        })
        .unwrap_or_default();
    let mut specs: Vec<(String, String)> = Vec::new();
    for path in changed.iter().take(MAX_SPECS) {
        if let Ok(t) = github.read_file(p.owner, p.repo, path, &st.head).await {
            specs.push((path.clone(), t));
        }
    }
    let ctx = common::agent_policy::GateCtx::new(p.team_id, common::agent_policy::Agent::Coder)
        .repo(&format!("{}/{}", p.owner, p.repo))
        .run(p.run_id);
    let mut proofs = all;
    let criteria: Vec<ep::Criterion> = criteria.into_iter().take(MAX_CRITERIA).collect();
    // What the server sends for each criterion about it, no script run: a
    // browser test runs the page's scripts and can pass while the server
    // sends none of it.
    let mut served = served_legs(state, github, p, st, &criteria).await;
    for c in &criteria {
        // The plan's test when it is on the head, else a changed spec that
        // loads the page.
        let planned = ep::line_for(c, &map)
            .filter(|m| !m.test.trim().is_empty())
            .map(|m| m.test.clone())
            .filter(|t| {
                let spec = common::ci_repro::spec_and_title(t).0;
                specs.iter().any(|(p, _)| p.ends_with(&spec))
            });
        let test = planned.or_else(|| ep::find_test(c, &specs));
        let record = match test.as_deref() {
            Some(t) => {
                super::ci_repro::two_way(
                    state,
                    github,
                    ctx.clone(),
                    super::ci_repro::Target {
                        team_id: p.team_id,
                        run_id: p.run_id,
                        owner: p.owner,
                        repo: p.repo,
                        pr: p.number,
                        branch,
                        head: &st.head,
                    },
                    CHECK,
                    t,
                )
                .await
            }
            None => common::ci_repro::Record {
                head: st.head.clone(),
                check: CHECK.into(),
                source: "sandbox".into(),
                at: chrono::Utc::now().to_rfc3339(),
                ..Default::default()
            },
        };
        let (served_change, served_base) = match served.remove(&c.text) {
            Some((change, base)) => (Some(change), base),
            None => (None, None),
        };
        let proof = Proof {
            criterion: c.text.clone(),
            page: c.page.clone().unwrap_or_default(),
            test: test.unwrap_or_default(),
            record,
            served_base,
            served_change,
        };
        info!(run_id = p.run_id, outcome = ?ep::outcome(&proof), "Two-way proof of a criterion");
        super::add_progress_note(
            state,
            p.team_id,
            p.run_id,
            &format!(
                "Two-way proof (staging, then the preview): {}",
                proof.line()
            ),
        )
        .await;
        proofs = ep::upsert(&proofs, proof);
    }
    save(state, p.team_id, p.run_id, &proofs).await;
    (proofs, true)
}

/// Where the two-way proof stands for the ready decision.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Standing {
    /// No criterion needs it, or the preview isn't up yet.
    #[default]
    None,
    /// The preview is up and the criteria aren't proven on this head yet.
    Pending,
    /// The coder is fixing what it sent back (that many items).
    Fixing(usize),
    /// Done; the criteria a side of which couldn't run, each a note.
    Done(Vec<String>),
}

/// The two-way proof's standing on `head` (`preview`: the head's preview).
pub(crate) async fn standing(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    head: &str,
    preview: Option<&State>,
) -> Standing {
    if run_id.is_empty() || head.is_empty() {
        return Standing::None;
    }
    let proofs = load(state, team_id, run_id).await;
    if !ep::on_head(&proofs, head).is_empty() {
        return standing_of(&proofs, head);
    }
    let up =
        preview.is_some_and(|p| p.head == head && matches!(p.stage, Stage::Ready | Stage::Checked));
    if up && !criteria_of(state, team_id, run_id).await.is_empty() {
        Standing::Pending
    } else {
        Standing::None
    }
}

/// [`standing`] from the proofs made on `head`.
pub fn standing_of(proofs: &[Proof], head: &str) -> Standing {
    let work = ep::coder_items(proofs, head);
    if !work.is_empty() {
        return Standing::Fixing(work.len());
    }
    Standing::Done(
        ep::unverified(proofs, head)
            .into_iter()
            .map(|(what, why)| format!("unverified: {what}: {why}"))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::ci_repro::{Leg, Record};

    fn proof(base: Leg, change: Leg) -> Proof {
        Proof {
            criterion: "Only one chip shows".into(),
            page: "/acme/slips".into(),
            test: "e2e/slips.spec.ts › one chip".into(),
            record: Record {
                head: "abc1234".into(),
                base,
                change,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn leg(ran: bool, passed: u32, failed: u32) -> Leg {
        Leg {
            ran,
            passed,
            failed,
            note: if ran { "" } else { "no browser" }.into(),
            ..Default::default()
        }
    }

    #[test]
    fn what_the_ready_decision_reads() {
        let both_green = proof(leg(true, 1, 0), leg(true, 1, 0));
        assert_eq!(standing_of(&[both_green], "abc1234"), Standing::Fixing(1));
        let proven = proof(leg(true, 0, 1), leg(true, 1, 0));
        assert_eq!(standing_of(&[proven], "abc1234"), Standing::Done(vec![]));
        let half = proof(leg(false, 0, 0), leg(true, 1, 0));
        match standing_of(&[half], "abc1234") {
            Standing::Done(notes) => {
                assert_eq!(notes.len(), 1);
                assert!(notes[0].contains("no browser"), "{notes:?}");
            }
            s => panic!("{s:?}"),
        }
    }
}
