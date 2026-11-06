//! The replay suite: incidents where CoderHelm stopped, asked, blocked or
//! looped when it should have gone on, replayed through the real decision
//! code and graded on the decision, not on wording.
//!
//! Each case is one JSON file in `cases/` (see `README.md` beside this
//! file). Its `case` names the decision path it replays:
//!
//! - `stop_judge`: a picture straight to `common::stop_judge::judge`.
//! - `ticket_route`: triage's intent, the planner's answer, the fix-location
//!   fallback and route, then the stop judge when the route would ask.
//! - `registration`: the new-feature registration rule, from the files.
//! - `run_loop`: a run's continuations through the run guard, the checks'
//!   ledger, the ceiling and the stop judge's licence for each extra cycle.
//! - `review_settled`: a review of a commit whose earlier threads are all
//!   settled, through `named_holding` and `review_verdict::decide`.
//! - `blocker_comment`: a person's comment naming a blocker on an unchanged
//!   commit, through `review_policy` and `review_verdict::decide`.
//! - `deploy_count`: workflow runs through `common::deploy_stats`.
//!
//! Where the stop judge is involved the case carries the model's recorded
//! answers for `k` trials (pass^k: every trial must pass, k = 3), so CI is
//! deterministic. `replay_live` (ignored by default) puts the same pictures
//! to the real model `k` times: `REPLAY_LIVE=1 ANTHROPIC_API_KEY=...
//! cargo test -p worker replay::replay_live -- --ignored`.

use std::path::{Path, PathBuf};

use common::stop_judge::{self as judge, JudgeModel, Picture};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::passes::stop_judge::Recorded;

/// One case file.
#[derive(Debug, Clone, Deserialize)]
struct Case {
    name: String,
    #[allow(dead_code)]
    about: String,
    case: String,
    /// Trials; each must pass (pass^k).
    #[serde(default)]
    k: Option<usize>,
    /// The model's recorded answers, one list per trial, in call order.
    #[serde(default)]
    replies: Vec<Vec<Value>>,
    #[serde(default)]
    input: Value,
    expect: Value,
}

impl Case {
    fn trials(&self) -> usize {
        self.k
            .unwrap_or(if self.replies.is_empty() { 1 } else { 3 })
    }
}

fn cases_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src/passes/replay/cases")
}

fn load() -> Vec<(String, Case)> {
    let mut out = Vec::new();
    let mut paths: Vec<PathBuf> = std::fs::read_dir(cases_dir())
        .expect("the replay cases folder")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    paths.sort();
    for p in paths {
        let file = p.file_name().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(&p).unwrap();
        let case: Case =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file} doesn't read: {e}"));
        out.push((file, case));
    }
    out
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or("")
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// The model a trial uses: its recorded answers, or (live) the real model.
enum Model {
    Recorded(Vec<Value>),
    #[allow(dead_code)]
    Live(LiveModel),
}

/// Runs the judge on a picture with this trial's model.
fn judged(model: &Model, picture: Picture) -> judge::Judged {
    match model {
        Model::Recorded(answers) => {
            block_on(judge::judge(&Recorded::new(answers.clone()), picture))
        }
        Model::Live(live) => block_on(judge::judge(live, picture)),
    }
}

// ── The cases ──────────────────────────────────────────────────────────────

fn want_verdict(expect: &Value, j: &judge::Judged) -> Result<(), String> {
    if let Some(v) = expect["verdict"].as_str() {
        if j.verdict().as_str() != v {
            return Err(format!(
                "verdict {} ({:?}), want {v}",
                j.verdict().as_str(),
                j.verdict()
            ));
        }
    }
    if expect["never_ask"].as_bool() == Some(true) && j.question().is_some() {
        return Err("asked a person".into());
    }
    Ok(())
}

fn run_stop_judge(c: &Case, model: &Model) -> Result<(), String> {
    let picture: Picture =
        serde_json::from_value(c.input["picture"].clone()).map_err(|e| format!("picture: {e}"))?;
    let j = judged(model, picture);
    want_verdict(&c.expect, &j)
}

fn run_ticket_route(c: &Case, model: &Model) -> Result<(), String> {
    use crate::passes::fix_location::{self, Route};
    use crate::passes::triage::{self, TicketIntent};
    let i = &c.input;
    let ticket_type = i["ticket_type"].as_str();
    let intent = triage::settle_intent(
        TicketIntent::parse(s(i, "model_intent")),
        s(i, "title"),
        s(i, "body"),
        ticket_type,
    );
    let tasks = s(i, "tasks");
    let decision = match fix_location::parse_detailed(s(i, "planner_answer")) {
        Ok(d) => Some(d),
        Err(problem) => fix_location::fallback(intent, tasks, &format!("{problem:?}")),
    };
    let mut route = fix_location::route_for_plan(decision.as_ref(), tasks);
    let mut asked = false;
    if route == Route::Ask {
        let mut p = Picture::new(
            judge::Kind::Ask,
            "fix_location",
            &format!(
                "the plan doesn't settle where the fix belongs: {}",
                decision.as_ref().map(|d| d.cause.as_str()).unwrap_or("")
            ),
        );
        p.ticket_title = s(i, "title").into();
        p.ticket_body = s(i, "body").into();
        p.plan = tasks.into();
        let j = judged(model, p);
        if j.proceeds() {
            route = Route::Code;
        } else {
            asked = true;
        }
    }
    let want_route = s(&c.expect, "route");
    let got = match route {
        Route::Code => "code",
        Route::CodeAndOutside => "code_and_outside",
        Route::OutsideOnly => "outside_only",
        Route::Ask => "ask",
    };
    if got != want_route {
        return Err(format!(
            "route {got}, want {want_route} (intent {})",
            intent.as_str()
        ));
    }
    if c.expect["asked"].as_bool() == Some(false) && asked {
        return Err("asked a person".into());
    }
    if let Some(want) = c.expect["intent"].as_str() {
        if intent.as_str() != want {
            return Err(format!("intent {}, want {want}", intent.as_str()));
        }
    }
    Ok(())
}

fn run_registration(c: &Case, _: &Model) -> Result<(), String> {
    use crate::passes::feature::registration::*;
    let i = &c.input;
    let tree = strings(&i["tree"]);
    let files: Vec<(String, String)> = i["files"]
        .as_object()
        .map(|o| {
            o.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        })
        .unwrap_or_default();
    let added = s(i, "added");
    let added_text = s(i, "added_text");
    let items = new_items(&[added.to_string()], &tree);
    let Some(item) = items.first() else {
        // Not even a candidate item: nothing to register.
        return check_blocking(&c.expect, 0);
    };
    let sibs = siblings(item, &tree, None);
    let parent = added.rsplit_once('/').map(|(p, _)| p).unwrap_or("");
    let sib_files: Vec<(String, String)> = sibs
        .iter()
        .filter_map(|sib| {
            files
                .iter()
                .find(|(p, _)| {
                    p.rsplit_once('/').is_some_and(|(dir, name)| {
                        dir == parent && name.split('.').next() == Some(sib.as_str())
                    })
                })
                .cloned()
        })
        .collect();
    let registry_files: Vec<(String, String)> = files
        .iter()
        .filter(|(p, _)| !sib_files.iter().any(|(q, _)| q == p))
        .cloned()
        .collect();
    let keep = registration_places(item, &sibs, &registry_files);
    let places: Vec<(String, String, bool)> = registry_files
        .iter()
        .filter_map(|(p, t)| {
            keep.iter()
                .find(|(k, _)| k == p)
                .map(|(_, r)| (p.clone(), t.clone(), *r))
        })
        .collect();
    let registries: Vec<&str> = places
        .iter()
        .filter(|(_, _, r)| *r)
        .map(|(_, t, _)| t.as_str())
        .collect();
    let kind = kind_of(Some((added, added_text)), &sib_files, &registries, &sibs);
    let lookup = |p: &str| {
        registry_files
            .iter()
            .find(|(q, _)| q == p)
            .map(|(_, t)| t.clone())
    };
    let missing = judge(item, &sibs, &kind, &places, &lookup);
    check_blocking(&c.expect, blocking(&missing).len())
}

fn check_blocking(expect: &Value, n: usize) -> Result<(), String> {
    let want = expect["blocking"].as_u64().unwrap_or(0) as usize;
    if n != want {
        return Err(format!("{n} blocking registration item(s), want {want}"));
    }
    Ok(())
}

fn run_loop(c: &Case, model: &Model) -> Result<(), String> {
    use crate::passes::run_ceiling::Totals;
    use crate::passes::run_guard::{GuardState, Ledger, Progress, MAX_PROOF_FIX_ROUNDS};
    use crate::passes::stop_judge::CycleLicence;
    let i = &c.input;
    let gaps: Vec<usize> = i["gaps"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_u64())
                .map(|n| n as usize)
                .collect()
        })
        .unwrap_or_default();
    let continuations = i["continuations"].as_u64().unwrap_or(11) as usize;
    let usd = i["usd_per_step"].as_f64().unwrap_or(4.0);
    let mut answers = match model {
        Model::Recorded(a) => a.clone().into_iter(),
        Model::Live(_) => Vec::new().into_iter(),
    };
    let mut guard = GuardState::default();
    let mut totals = Totals::default();
    let mut ledger = Ledger::default();
    guard.apply(&Progress::WorkDone);
    let mut pr_open_at: Option<usize> = None;
    let mut stopped_at: Option<usize> = None;
    let mut fix_rounds = 0u32;
    for step in 0..continuations {
        ledger.on_step_start();
        // Finished work opens its draft first; the checks run on it.
        if pr_open_at.is_none() {
            pr_open_at = Some(step);
            guard.apply(&Progress::PrOpen);
        }
        let open = gaps.get(step % gaps.len().max(1)).copied().unwrap_or(0);
        let head = format!("h{step}");
        if !ledger.proof.unchanged(&head) {
            ledger.proof.checked(&head, open);
            guard.apply(&Progress::Findings("proof", open));
        }
        if open > 0 && ledger.proof.may_fix(MAX_PROOF_FIX_ROUNDS) {
            let licence = match CycleLicence::first(ledger.proof.rounds) {
                Some(l) => Some(l),
                None => {
                    let mut p = Picture::new(
                        judge::Kind::ExtraCycle,
                        "proof_check",
                        &format!(
                            "the proof check still reports {open} gap(s) after {} cycle(s)",
                            ledger.proof.rounds
                        ),
                    );
                    p.evidence = (0..open)
                        .map(|n| judge::Evidence {
                            source: "proof check".into(),
                            finding: format!("gap {}", n + 1),
                            checked: true,
                            ..Default::default()
                        })
                        .collect();
                    p.budget.cycles = ledger.proof.rounds;
                    p.budget.cycle_cap = MAX_PROOF_FIX_ROUNDS;
                    let j = match model {
                        Model::Recorded(_) => {
                            let a: Vec<Value> = answers.next().into_iter().collect();
                            block_on(judge::judge(&Recorded::new(a), p))
                        }
                        Model::Live(live) => block_on(judge::judge(live, p)),
                    };
                    CycleLicence::judged(&j)
                }
            };
            if let Some(l) = licence {
                ledger.proof.start_fix(l);
                ledger.proof.end_fix();
                fix_rounds += 1;
            }
        }
        guard.on_continuation(usd);
        totals.continuations += 1;
        totals.spend_usd += usd;
        if guard.trip().is_some() || totals.over().is_some() {
            stopped_at = Some(step);
            break;
        }
    }
    let e = &c.expect;
    if e["draft_pr"].as_bool() == Some(true) && pr_open_at.is_none() {
        return Err("no draft pull request".into());
    }
    if let Some(max) = e["max_continuations"].as_u64() {
        let used = stopped_at.map(|s| s + 1).unwrap_or(continuations) as u64;
        if used > max {
            return Err(format!("{used} continuations, want at most {max}"));
        }
    }
    if let Some(max) = e["max_spend_usd"].as_f64() {
        if totals.spend_usd > max {
            return Err(format!(
                "spent ${:.2}, want at most ${max:.2}",
                totals.spend_usd
            ));
        }
    }
    if let Some(max) = e["max_fix_rounds"].as_u64() {
        if u64::from(fix_rounds) > max {
            return Err(format!("{fix_rounds} fix rounds, want at most {max}"));
        }
    }
    if e["stops"].as_bool() == Some(true) && stopped_at.is_none() {
        return Err("the run never stopped".into());
    }
    Ok(())
}

fn run_review_settled(c: &Case, _: &Model) -> Result<(), String> {
    use crate::passes::review_agent::thread_holds_merge;
    use crate::passes::review_policy::named_holding;
    use crate::passes::review_verdict::{self as rv, Context, Decision, Input, StandingReview};
    let i = &c.input;
    let head = s(i, "head");
    let open: Vec<(String, u64, String)> = i["threads"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|t| t["settled"].as_bool() != Some(true))
        .filter(|t| thread_holds_merge(s(t, "body")))
        .map(|t| {
            (
                s(t, "path").to_string(),
                t["line"].as_u64().unwrap_or(0),
                s(t, "body").to_string(),
            )
        })
        .collect();
    let verdict = rv::Verdict::from_holding(named_holding(&[], &open, &[], false, &[], &[]));
    let ctx = Context {
        standing: Some(StandingReview {
            verdict: s(i, "standing").to_string(),
            failed: false,
            head_sha: head.to_string(),
        }),
        self_authored: false,
    };
    let d = rv::decide(
        Input::Review {
            verdict: verdict.event(),
            has_news: false,
            explicit: false,
            already_answered: false,
        },
        &ctx,
    );
    let want = s(&c.expect, "event");
    match d {
        Decision::Post { event, .. } if event == want => {}
        other => return Err(format!("{other:?}, want a posted {want}")),
    }
    if !rv::may_post(want, &verdict, None) {
        return Err(format!("the chokepoint refuses {want}"));
    }
    Ok(())
}

fn run_blocker_comment(c: &Case, _: &Model) -> Result<(), String> {
    use crate::passes::review_agent::VerdictChange;
    use crate::passes::review_policy::{
        answers_from_standing, standing_answer, verdict_stands_note,
    };
    use crate::passes::review_verdict::{self as rv, Context, Decision, Input, StandingReview};
    let i = &c.input;
    let head = s(i, "head");
    let standing = s(i, "standing");
    let comment = s(i, "comment");
    let holding = strings(&i["holding"]);
    // Nothing the verdict rests on changed: only words.
    let changed: Vec<crate::passes::review_fingerprint::Changed> = Vec::new();
    let from_standing = answers_from_standing(s(i, "trigger"), &changed, true);
    let ctx = Context {
        standing: Some(StandingReview {
            verdict: standing.to_string(),
            failed: false,
            head_sha: head.to_string(),
        }),
        self_authored: false,
    };
    let change = match s(i, "reply_reads") {
        "block" => VerdictChange::Block,
        "clear" => VerdictChange::Clear,
        _ => VerdictChange::None,
    };
    let d = rv::decide(
        Input::Reply {
            change,
            more_than_words: false,
        },
        &ctx,
    );
    let e = &c.expect;
    if e["second_review"].as_bool() == Some(false) && matches!(d, Decision::Review) {
        return Err("a second paid review".into());
    }
    if e["answered"].as_bool() == Some(true) && !from_standing && s(i, "trigger") != "reply" {
        return Err("no answer".into());
    }
    // The answer is whole: fixed words from facts, never a person's words
    // spliced into a template.
    let answer = if from_standing {
        standing_answer(head, standing, &holding)
    } else {
        verdict_stands_note(head, standing)
    };
    if e["no_spliced_text"].as_bool() == Some(true) {
        let snippet: String = comment.chars().take(24).collect();
        if !snippet.trim().is_empty() && answer.contains(snippet.trim()) {
            return Err(format!("the answer splices the comment: {answer}"));
        }
        if answer.trim().is_empty() {
            return Err("an empty answer".into());
        }
    }
    Ok(())
}

fn run_deploy_count(c: &Case, _: &Model) -> Result<(), String> {
    use common::deploy_stats::*;
    let i = &c.input;
    let bot = s(i, "bot_login");
    let default_branch = s(i, "default_branch");
    let setting = parse_deploy_workflows(s(i, "deploy_workflows"));
    let mut by_cause: std::collections::BTreeMap<String, u64> = Default::default();
    for run in i["runs"].as_array().into_iter().flatten() {
        if !started_by_coderhelm(run, bot) {
            continue;
        }
        let is_tag = run["is_tag"].as_bool().unwrap_or(false);
        let targets = run_targets(run, None, None, &setting);
        if targets.is_empty() {
            continue;
        }
        *by_cause
            .entry(cause_of(run, default_branch, is_tag).as_str().to_string())
            .or_default() += targets.len() as u64;
    }
    let want: std::collections::BTreeMap<String, u64> =
        serde_json::from_value(c.expect["by_cause"].clone()).map_err(|e| e.to_string())?;
    if by_cause != want {
        return Err(format!("deploys by cause {by_cause:?}, want {want:?}"));
    }
    Ok(())
}

fn run_one(c: &Case, model: &Model) -> Result<(), String> {
    match c.case.as_str() {
        "stop_judge" => run_stop_judge(c, model),
        "ticket_route" => run_ticket_route(c, model),
        "registration" => run_registration(c, model),
        "run_loop" => run_loop(c, model),
        "review_settled" => run_review_settled(c, model),
        "blocker_comment" => run_blocker_comment(c, model),
        "deploy_count" => run_deploy_count(c, model),
        other => Err(format!("unknown case kind `{other}`")),
    }
}

/// Every case, every trial (pass^k).
pub(crate) fn replay_all(live: Option<&LiveModel>) -> (usize, Vec<String>) {
    let mut failures = Vec::new();
    let mut trials = 0;
    for (file, c) in load() {
        let k = c.trials();
        for t in 0..k {
            trials += 1;
            let model = match live {
                Some(l) => Model::Live(l.clone()),
                None => Model::Recorded(c.replies.get(t).cloned().unwrap_or_default()),
            };
            if let Err(e) = run_one(&c, &model) {
                failures.push(format!("{file} ({}) trial {}/{k}: {e}", c.name, t + 1));
            }
        }
    }
    (trials, failures)
}

/// The real model, for the live mode.
#[derive(Clone)]
pub(crate) struct LiveModel {
    key: String,
    model: String,
}

impl JudgeModel for LiveModel {
    async fn answer(&self, system: &str, prompt: &str, schema: &Value) -> Result<Value, String> {
        let client = crate::agent::anthropic::AnthropicClient::new(
            self.key.clone(),
            common::usage::UsageMeter::memory("replay").0,
        );
        let mut usage = crate::models::TokenUsage::default();
        crate::agent::anthropic::converse_json(
            &client,
            &self.model,
            system,
            prompt,
            schema,
            true,
            judge::JUDGMENT_EFFORT,
            8_000,
            &mut usage,
        )
        .await
        .map_err(|e| e.to_string())
    }
}

#[test]
fn replay_every_incident_pass_k() {
    let (trials, failures) = replay_all(None);
    assert!(trials >= 6, "found the cases ({trials} trials)");
    assert!(
        failures.is_empty(),
        "replay failures:\n{}",
        failures.join("\n")
    );
}

#[test]
fn every_case_with_a_model_call_has_k_recorded_trials() {
    for (file, c) in load() {
        if !c.replies.is_empty() {
            assert!(c.trials() >= 3, "{file}: pass^k needs k >= 3");
            assert_eq!(
                c.replies.len(),
                c.trials(),
                "{file}: one recorded answer list per trial"
            );
        }
        assert!(!c.name.trim().is_empty(), "{file}: a name");
    }
}

#[test]
fn the_seed_incidents_are_all_here() {
    let kinds: Vec<String> = load().into_iter().map(|(_, c)| c.case).collect();
    for k in [
        "ticket_route",
        "registration",
        "run_loop",
        "review_settled",
        "blocker_comment",
        "deploy_count",
    ] {
        assert!(kinds.iter().any(|x| x == k), "no `{k}` case");
    }
}

/// A case that would pass whatever the code did is no case: a wrong
/// expectation fails.
#[test]
fn a_wrong_expectation_fails() {
    for (file, mut c) in load() {
        if c.case != "ticket_route" {
            continue;
        }
        c.expect = json!({"route": "ask"});
        let model = Model::Recorded(c.replies.first().cloned().unwrap_or_default());
        assert!(run_one(&c, &model).is_err(), "{file}");
        return;
    }
    panic!("no ticket_route case");
}

/// The live mode: the real model on every picture, `k` times.
#[test]
#[ignore]
fn replay_live() {
    if std::env::var("REPLAY_LIVE").ok().as_deref() != Some("1") {
        return;
    }
    let key = std::env::var("ANTHROPIC_API_KEY").expect("ANTHROPIC_API_KEY");
    let model = std::env::var("REPLAY_MODEL").unwrap_or_else(|_| "claude-opus-5-5".into());
    let (trials, failures) = replay_all(Some(&LiveModel { key, model }));
    assert!(trials > 0);
    assert!(
        failures.is_empty(),
        "live replay failures:\n{}",
        failures.join("\n")
    );
}

/// An exported decision (`common::decision_log::fixture`) reads as a case,
/// and the suite tells a wrong one apart: its recorded answer held where
/// people's act says it should have gone on.
#[test]
fn an_exported_decision_reads_as_a_case() {
    let mut p = Picture::new(judge::Kind::Block, "review", "a nit");
    p.evidence = vec![judge::Evidence {
        id: "E1".into(),
        source: "review".into(),
        finding: "tests fail".into(),
        location: "src/a.rs:1".into(),
        checked: true,
    }];
    let hold = json!({"verdict":"hold","assumption":"","question":"","person_only":"",
        "tried":[],"options":[],"default":"","deadline_hours":0,"blocker":"tests fail",
        "evidence":["E1"],"category":"correctness","location":"src/a.rs:1"});
    let usage_free = vec![hold.clone()];
    let j = block_on(judge::judge(&Recorded::new(usage_free), p.clone()));
    let mut rec = common::decision_log::Record::new(
        &common::decision_log::Subject::Pr {
            repo: "acme/site".into(),
            number: 3,
        },
        &j,
        &p,
        "h1",
    );
    rec.outcome = Some(common::decision_log::Outcome::DismissedReview);
    let c: Case = serde_json::from_value(common::decision_log::fixture(&rec)).expect("reads");
    assert_eq!(c.case, "stop_judge");
    assert_eq!(c.trials(), 1);
    let model = Model::Recorded(c.replies[0].clone());
    // The recorded answer held: the exported case fails until the judge
    // decides it right and three trials are recorded.
    assert!(run_one(&c, &model).is_err());
}

/// Records the real model's answers for every case with a model call:
/// `REPLAY_RECORD=1 ANTHROPIC_API_KEY=... cargo test -p worker
/// replay::replay_record -- --ignored`. Each case's `replies` becomes `k`
/// fresh trials; review the diff before committing it.
#[test]
#[ignore]
fn replay_record() {
    if std::env::var("REPLAY_RECORD").ok().as_deref() != Some("1") {
        return;
    }
    let key = std::env::var("ANTHROPIC_API_KEY").expect("ANTHROPIC_API_KEY");
    let model = std::env::var("REPLAY_MODEL").unwrap_or_else(|_| "claude-opus-5-5".into());
    let live = LiveModel { key, model };
    for entry in std::fs::read_dir(cases_dir()).unwrap().flatten() {
        let path = entry.path();
        let text = std::fs::read_to_string(&path).unwrap();
        let mut v: Value = serde_json::from_str(&text).unwrap();
        if v["case"] != "stop_judge" {
            continue;
        }
        let c: Case = serde_json::from_value(v.clone()).unwrap();
        let picture: Picture = serde_json::from_value(c.input["picture"].clone()).unwrap();
        let k = c.trials().max(3);
        let mut trials = Vec::new();
        for _ in 0..k {
            let j = block_on(judge::judge(&live, picture.clone()));
            trials.push(Value::Array(j.replies().to_vec()));
        }
        v["replies"] = Value::Array(trials);
        v["k"] = json!(k);
        std::fs::write(&path, serde_json::to_string_pretty(&v).unwrap() + "\n").unwrap();
    }
}
