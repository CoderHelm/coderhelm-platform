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

/// The run's ticket criteria that need an end-to-end proof.
async fn criteria_of(state: &WorkerState, team_id: &str, run_id: &str) -> Vec<ep::Criterion> {
    let v = run_strings(state, team_id, run_id, &["title", "ticket_body"]).await;
    let (title, body) = (
        v[0].clone().unwrap_or_default(),
        v[1].clone().unwrap_or_default(),
    );
    ep::criteria(&title, &body, &super::stop_judge::acceptance(&body))
        .into_iter()
        .filter(ep::Criterion::needs_e2e)
        .collect()
}

/// The plan's design.md for the run's ticket and repository, empty when
/// it can't be read.
async fn design_of(
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
    for c in criteria.iter().take(MAX_CRITERIA) {
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
        let proof = Proof {
            criterion: c.text.clone(),
            page: c.page.clone().unwrap_or_default(),
            test: test.unwrap_or_default(),
            record,
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
            page: "/acme/camps".into(),
            test: "e2e/camps.spec.ts › one chip".into(),
            record: Record {
                head: "abc1234".into(),
                base,
                change,
                ..Default::default()
            },
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
