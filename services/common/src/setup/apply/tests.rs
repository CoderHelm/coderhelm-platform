use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::{json, Value};

use super::*;
use crate::plugins::kv::Memory;
use crate::setup::plan::{Choice, Decision, ItemDecision, Rec};
use crate::setup::registry::{Entry, Kind, REGISTRY};
use crate::setup::store::MemoryStore;
use crate::setup::{Evidence, Source};

const T: &str = "TEAM#1";
const REPO: &str = "acme/web";

fn all_repos() -> Vec<String> {
    vec![REPO.to_string(), "acme/api".to_string()]
}

fn ctx<'a>(repo: &'a str, all: &'a [String]) -> Ctx<'a> {
    Ctx {
        repo,
        all_repos: all,
        by: "admin@acme.io",
        now: "2026-11-11T00:00:00Z".into(),
    }
}

/// A valid value other than the default, `n` picks among several.
fn sample(e: &Entry, n: usize) -> Value {
    let d = registry::default_value(e);
    match e.kind {
        Kind::Bool => json!(!d.as_bool().unwrap()),
        Kind::Int { min, max } => {
            let picks = [min + 1 + n as i64, max - n as i64, min];
            json!(picks
                .into_iter()
                .find(|p| (min..=max).contains(p) && json!(*p) != d)
                .unwrap())
        }
        Kind::Choice { options } => json!(options
            .iter()
            .filter(|o| json!(o) != d)
            .nth(n % (options.len() - 1))
            .unwrap()),
        Kind::Text { .. } => match e.id {
            "git_conventions.branch" => {
                json!(["ch/{ticket_lower}", "work/{ticket_lower}-{slug}"][n % 2])
            }
            "git_conventions.commit" | "git_conventions.pr_title" => {
                json!(["{ticket} {title}", "[{ticket}] {title}"][n % 2])
            }
            "tags.tag_prefix" => json!(["rel-", "r"][n % 2]),
            _ => json!(format!("Sample {n} for {}", e.id)),
        },
    }
}

fn mk_run(choice: Choice, mode: Mode, repos: &[&str]) -> Run {
    Run {
        run_id: crate::plugins::new_id(),
        state: None,
        created_at: crate::plugins::now_rfc3339(),
        by: "admin@acme.io".into(),
        mode,
        automatic: false,
        choice,
        only_new: false,
        repos: repos.iter().map(|r| r.to_string()).collect(),
        scope: REGISTRY.iter().map(|e| e.id.to_string()).collect(),
        estimate_tokens: 0,
        decisions: vec![],
        applied_at: None,
        undo: None,
        lacks: Default::default(),
    }
}

async fn plan_repo(
    store: &MemoryStore,
    kv: &Memory,
    run: &Run,
    repo: &str,
    recs: &HashMap<&'static str, Rec>,
) -> RepoDoc {
    let current = registry::read_all(store, repo).await.unwrap();
    let applied = runs::applied(kv, T, repo).await.unwrap();
    let mut doc = RepoDoc::new(&run.run_id, repo);
    doc.state = RepoState::Ready;
    doc.items = plan::items(repo, &run.scope, &current, recs, applied.as_ref(), None);
    runs::put_repo(kv, T, &doc).await.unwrap();
    doc
}

/// A recommendation for every entry: sample `n`.
fn recs(n: usize) -> HashMap<&'static str, Rec> {
    REGISTRY
        .iter()
        .map(|e| {
            (
                e.id,
                Rec {
                    value: sample(e, n),
                    source: Source::Detected,
                    evidence: vec![Evidence::text("fixture")],
                },
            )
        })
        .collect()
}

fn admin() -> Actor<'static> {
    Actor {
        email: "admin@acme.io",
        kind: ActorKind::Person,
        missing: &holds_all,
    }
}

async fn values(store: &MemoryStore, repo: &str) -> HashMap<&'static str, registry::Current> {
    registry::read_all(store, repo).await.unwrap()
}

#[tokio::test]
async fn every_entry_round_trips_through_its_own_validation() {
    let all = all_repos();
    for e in REGISTRY {
        let store = MemoryStore::new(T);
        let before = registry::read(&store, e, REPO).await.unwrap();
        assert_eq!(before.value, registry::default_value(e), "{} default", e.id);
        assert!(!before.stored, "{}", e.id);
        for n in 0..2 {
            let v = sample(e, n);
            registry::write(&store, e, &ctx(REPO, &all), &v)
                .await
                .unwrap_or_else(|err| panic!("{}: {err}", e.id));
            let back = registry::read(&store, e, REPO).await.unwrap();
            assert_eq!(back.value, v, "{} read back", e.id);
            assert!(back.stored, "{}", e.id);
        }
        // A value the setting can't take is refused, never stored.
        let bad = match e.kind {
            Kind::Bool => json!("yes"),
            Kind::Int { max, .. } => json!(max + 1),
            Kind::Text { max, .. } => json!("x".repeat(max + 1)),
            Kind::Choice { .. } => json!("nope"),
        };
        assert!(
            registry::write(&store, e, &ctx(REPO, &all), &bad)
                .await
                .is_err(),
            "{}",
            e.id
        );
    }
}

#[tokio::test]
async fn a_write_changes_only_its_own_setting() {
    let all = all_repos();
    let store = MemoryStore::new(T);
    for e in REGISTRY {
        registry::write(&store, e, &ctx(REPO, &all), &sample(e, 0))
            .await
            .unwrap();
    }
    let set = values(&store, REPO).await;
    for e in REGISTRY {
        registry::write(&store, e, &ctx(REPO, &all), &sample(e, 1))
            .await
            .unwrap();
        let now = values(&store, REPO).await;
        for o in REGISTRY.iter().filter(|o| o.id != e.id) {
            let expect = if REGISTRY.iter().position(|x| x.id == o.id)
                < REGISTRY.iter().position(|x| x.id == e.id)
            {
                sample(o, 1)
            } else {
                set[o.id].value.clone()
            };
            assert_eq!(now[o.id].value, expect, "writing {} changed {}", e.id, o.id);
        }
    }
}

#[tokio::test]
async fn a_reviewer_write_keeps_the_webhooks_and_the_secret() {
    let store = MemoryStore::new(T);
    let mut item = crate::review_config::build(T, REPO, &json!({}), None, "now").unwrap();
    item.insert(
        "release_notes_email_webhook_secret".into(),
        AttributeValue::S("shh".into()),
    );
    item.insert(
        "teams_webhook_sealed".into(),
        AttributeValue::S("sealed".into()),
    );
    store.put(item).await.unwrap();
    let e = registry::get("reviewer.enabled").unwrap();
    registry::write(&store, e, &ctx(REPO, &all_repos()), &json!(true))
        .await
        .unwrap();
    let after = store
        .get(&crate::review_config::sk(REPO))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after["release_notes_email_webhook_secret"],
        AttributeValue::S("shh".into())
    );
    assert_eq!(
        after["teams_webhook_sealed"],
        AttributeValue::S("sealed".into())
    );
    assert_eq!(after["enabled"], AttributeValue::Bool(true));
    assert!(store
        .indexes()
        .contains(&crate::review_config_index::NAME.to_string()));
}

#[tokio::test]
async fn keep_mine_never_changes_a_setting_that_was_set() {
    let all = all_repos();
    for mode in [Mode::Guided, Mode::Auto] {
        for e in REGISTRY {
            let store = MemoryStore::new(T);
            let kv = Memory::new();
            // The person set this one setting.
            let mine = sample(e, 0);
            registry::write(&store, e, &ctx(REPO, &all), &mine)
                .await
                .unwrap();
            let run = mk_run(Choice::KeepMine, mode, &[REPO]);
            runs::put_run(&kv, T, &run, crate::plugins::kv::Expect::Absent)
                .await
                .unwrap();
            let mut doc = plan_repo(&store, &kv, &run, REPO, &recs(1)).await;
            let (res, _) = apply_repo(&store, &kv, &run, &mut doc, &all, &admin()).await;
            assert_eq!(res.state, ResultState::Done, "{}: {:?}", e.id, res.error);
            let now = registry::read(&store, e, REPO).await.unwrap();
            assert_eq!(now.value, mine, "{mode:?}: keep_mine changed {}", e.id);
        }
    }
}

#[tokio::test]
async fn keep_mine_takes_the_recommendation_where_nothing_was_set() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let all = all_repos();
    let run = mk_run(Choice::KeepMine, Mode::Guided, &[REPO]);
    let mut doc = plan_repo(&store, &kv, &run, REPO, &recs(0)).await;
    let (res, _) = apply_repo(&store, &kv, &run, &mut doc, &all, &admin()).await;
    assert_eq!(res.state, ResultState::Done, "{:?}", res.error);
    let now = values(&store, REPO).await;
    for e in REGISTRY {
        // Anything that merges or closes stays a preview.
        let want = if e.preview_only {
            registry::default_value(e)
        } else {
            sample(e, 0)
        };
        assert_eq!(now[e.id].value, want, "{}", e.id);
    }
    let previews = REGISTRY.iter().filter(|e| e.preview_only).count();
    // One audit event per change.
    let events = audit::for_filter(
        &kv,
        T,
        &audit::Filter {
            action: Some("setup".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(events.len(), res.changed);
    assert_eq!(res.changed, REGISTRY.len() - previews);
    // Applying again changes nothing.
    let mut doc = runs::get_repo(&kv, T, &run.run_id, REPO)
        .await
        .unwrap()
        .unwrap();
    let (again, _) = apply_repo(&store, &kv, &run, &mut doc, &all, &admin()).await;
    assert_eq!(again.changed, 0);
}

#[tokio::test]
async fn a_setting_set_after_the_analysis_is_still_kept() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let all = all_repos();
    let run = mk_run(Choice::KeepMine, Mode::Guided, &[REPO]);
    let mut doc = plan_repo(&store, &kv, &run, REPO, &recs(0)).await;
    let e = registry::get("reviewer.label").unwrap();
    registry::write(&store, e, &ctx(REPO, &all), &json!("by-hand"))
        .await
        .unwrap();
    apply_repo(&store, &kv, &run, &mut doc, &all, &admin()).await;
    assert_eq!(
        registry::read(&store, e, REPO).await.unwrap().value,
        "by-hand"
    );
}

#[tokio::test]
async fn use_recommended_overwrites_and_decisions_win() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let all = all_repos();
    let label = registry::get("reviewer.label").unwrap();
    let focus = registry::get("reviewer.instructions").unwrap();
    registry::write(&store, label, &ctx(REPO, &all), &json!("mine"))
        .await
        .unwrap();
    let mut run = mk_run(Choice::UseRecommended, Mode::Guided, &[REPO]);
    run.decisions = vec![ItemDecision {
        repo: REPO.into(),
        setting_id: focus.id.into(),
        decision: Decision::Edit,
        value: Some(json!("Edited focus")),
    }];
    let mut doc = plan_repo(&store, &kv, &run, REPO, &recs(1)).await;
    let o = plan::overwrites(REPO, &doc.items, run.choice, &run.decisions);
    assert!(o
        .iter()
        .any(|x| x.setting_id == label.id && x.current == json!("mine")));
    apply_repo(&store, &kv, &run, &mut doc, &all, &admin()).await;
    assert_eq!(
        registry::read(&store, label, REPO).await.unwrap().value,
        sample(label, 1)
    );
    assert_eq!(
        registry::read(&store, focus, REPO).await.unwrap().value,
        "Edited focus"
    );
}

#[tokio::test]
async fn a_hand_change_after_setup_shows_as_custom() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let all = all_repos();
    let run = mk_run(Choice::KeepMine, Mode::Guided, &[REPO]);
    let mut doc = plan_repo(&store, &kv, &run, REPO, &recs(0)).await;
    apply_repo(&store, &kv, &run, &mut doc, &all, &admin()).await;
    let e = registry::get("stale_prs.stale_days").unwrap();
    registry::write(&store, e, &ctx(REPO, &all), &json!(40))
        .await
        .unwrap();
    let later = mk_run(Choice::KeepMine, Mode::Guided, &[REPO]);
    let doc = plan_repo(&store, &kv, &later, REPO, &recs(0)).await;
    let custom: Vec<&str> = doc
        .items
        .iter()
        .filter(|i| i.custom)
        .map(|i| i.setting_id.as_str())
        .collect();
    assert_eq!(custom, vec![e.id]);
}

#[tokio::test]
async fn undo_restores_exactly_what_was_there() {
    let all = all_repos();
    for preset in [false, true] {
        let store = MemoryStore::new(T);
        let kv = Memory::new();
        if preset {
            // Some settings set before the run (and the run overwrites them).
            for e in REGISTRY.iter().step_by(3) {
                registry::write(&store, e, &ctx(REPO, &all), &sample(e, 1))
                    .await
                    .unwrap();
            }
        }
        let before = values(&store, REPO).await;
        let run = mk_run(Choice::UseRecommended, Mode::Guided, &[REPO]);
        runs::put_run(&kv, T, &run, crate::plugins::kv::Expect::Absent)
            .await
            .unwrap();
        plan_repo(&store, &kv, &run, REPO, &recs(0)).await;
        apply_run(&store, &kv, &run.run_id, None, &all, &admin())
            .await
            .unwrap();
        assert_ne!(values(&store, REPO).await, before);
        let rep = undo(&store, &kv, &run.run_id, None, &all, &admin())
            .await
            .unwrap();
        assert!(rep.skipped.is_empty(), "{:?}", rep.skipped);
        let after = values(&store, REPO).await;
        for e in REGISTRY {
            assert_eq!(after[e.id].value, before[e.id].value, "{} value", e.id);
            assert_eq!(after[e.id].stored, before[e.id].stored, "{} stored", e.id);
        }
        let (r, _) = runs::get_run(&kv, T, &run.run_id).await.unwrap().unwrap();
        assert_eq!(r.state, Some(RunState::Undone));
    }
}

#[tokio::test]
async fn undo_skips_and_lists_a_setting_changed_since() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let all = all_repos();
    let run = mk_run(Choice::KeepMine, Mode::Guided, &[REPO]);
    runs::put_run(&kv, T, &run, crate::plugins::kv::Expect::Absent)
        .await
        .unwrap();
    plan_repo(&store, &kv, &run, REPO, &recs(0)).await;
    apply_run(&store, &kv, &run.run_id, None, &all, &admin())
        .await
        .unwrap();
    let e = registry::get("reminders.cooldown_hours").unwrap();
    registry::write(&store, e, &ctx(REPO, &all), &json!(99))
        .await
        .unwrap();
    let rep = undo(&store, &kv, &run.run_id, None, &all, &admin())
        .await
        .unwrap();
    assert_eq!(rep.skipped.len(), 1);
    assert_eq!(rep.skipped[0].setting_id, e.id);
    assert_eq!(registry::read(&store, e, REPO).await.unwrap().value, 99);
}

#[tokio::test]
async fn undo_of_single_items() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let all = all_repos();
    let run = mk_run(Choice::KeepMine, Mode::Auto, &[REPO]);
    runs::put_run(&kv, T, &run, crate::plugins::kv::Expect::Absent)
        .await
        .unwrap();
    plan_repo(&store, &kv, &run, REPO, &recs(0)).await;
    apply_run(&store, &kv, &run.run_id, None, &all, &admin())
        .await
        .unwrap();
    let label = registry::get("reviewer.label").unwrap();
    let graph = registry::get("code_graph.enabled").unwrap();
    let pick = vec![
        (REPO.to_string(), label.id.to_string()),
        (REPO.to_string(), graph.id.to_string()),
    ];
    let rep = undo(&store, &kv, &run.run_id, Some(&pick), &all, &admin())
        .await
        .unwrap();
    assert_eq!(rep.restored, 2);
    assert!(rep.partial);
    assert_eq!(
        registry::read(&store, label, REPO).await.unwrap().value,
        registry::default_value(label)
    );
    assert_eq!(
        registry::read(&store, graph, REPO).await.unwrap(),
        registry::Current {
            value: json!(true),
            stored: false,
            row: false,
        }
    );
    // Everything else stays applied, and the run isn't undone.
    let other = registry::get("reviewer.enabled").unwrap();
    assert_eq!(
        registry::read(&store, other, REPO).await.unwrap().value,
        sample(other, 0)
    );
    let (r, _) = runs::get_run(&kv, T, &run.run_id).await.unwrap().unwrap();
    assert_eq!(r.state, Some(RunState::Done));
    // The rest can still be undone afterwards.
    let rep = undo(&store, &kv, &run.run_id, None, &all, &admin())
        .await
        .unwrap();
    assert!(rep.skipped.is_empty(), "{:?}", rep.skipped);
    let previews = REGISTRY.iter().filter(|e| e.preview_only).count();
    assert_eq!(rep.restored, REGISTRY.len() - 2 - previews);
}

#[tokio::test]
async fn a_retry_covers_only_the_repos_that_failed() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let all = all_repos();
    let run = mk_run(Choice::KeepMine, Mode::Guided, &[REPO, "acme/api"]);
    runs::put_run(&kv, T, &run, crate::plugins::kv::Expect::Absent)
        .await
        .unwrap();
    plan_repo(&store, &kv, &run, REPO, &recs(0)).await;
    plan_repo(&store, &kv, &run, "acme/api", &recs(0)).await;
    store.fail_writes(Some("STALE_PR_CONFIG#REPO#acme/api"));
    let (r, results, _) = apply_run(&store, &kv, &run.run_id, None, &all, &admin())
        .await
        .unwrap();
    assert_eq!(r.state, Some(RunState::Failed));
    let state = |repo: &str| results.iter().find(|x| x.repo == repo).unwrap().state;
    assert_eq!(state(REPO), ResultState::Done);
    assert_eq!(state("acme/api"), ResultState::Failed);
    let before = audit::for_filter(&kv, T, &audit::Filter::default())
        .await
        .len();
    store.fail_writes(None);
    let (r, results, _) = apply_run(&store, &kv, &run.run_id, None, &all, &admin())
        .await
        .unwrap();
    assert_eq!(r.state, Some(RunState::Done));
    assert!(results.iter().all(|x| x.state == ResultState::Done));
    // Only the failed repo's stale settings were written on the retry.
    let added = audit::for_filter(&kv, T, &audit::Filter::default())
        .await
        .len()
        - before;
    let stale = REGISTRY
        .iter()
        .filter(|e| e.id.starts_with("stale_prs."))
        .count();
    assert_eq!(added, stale);
}

#[tokio::test]
async fn a_setting_the_person_cant_edit_fails_and_says_what_it_needs() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let all = all_repos();
    let run = mk_run(Choice::KeepMine, Mode::Guided, &[REPO]);
    let mut doc = plan_repo(&store, &kv, &run, REPO, &recs(0)).await;
    let no_security = |cap: &str| (cap == "security").then(|| "Manage security fixes".to_string());
    let actor = Actor {
        email: "ops@acme.io",
        kind: ActorKind::Person,
        missing: &no_security,
    };
    let (res, _) = apply_repo(&store, &kv, &run, &mut doc, &all, &actor).await;
    assert_eq!(res.state, ResultState::Failed);
    let failed: Vec<&str> = res.failed.iter().map(|f| f.setting_id.as_str()).collect();
    assert_eq!(failed, vec!["security.enabled", "security.open_prs"]);
    assert!(res
        .error
        .unwrap()
        .contains("Needs the \"Manage security fixes\" permission"));
    assert!(
        !registry::read(&store, registry::get("security.enabled").unwrap(), REPO)
            .await
            .unwrap()
            .stored
    );
    let previews = REGISTRY.iter().filter(|e| e.preview_only).count();
    assert_eq!(res.changed, REGISTRY.len() - 2 - previews);
}

#[tokio::test]
async fn auto_mode_lands_acts_settings_on_their_safe_start() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let all = all_repos();
    let mut f = crate::setup::facts::tests::web_app();
    f.required_checks.clear(); // no checks required on this one
    let found = crate::setup::facts::detect(&f);
    let inferred = HashMap::from([(
        "auto_merge.enabled",
        crate::setup::ai::Inferred {
            value: json!(true),
            evidence: vec![Evidence::text("x")],
        },
    )]);
    let recs = plan::recommend(&f, &found, &inferred, chrono::Utc::now());
    let run = mk_run(Choice::KeepMine, Mode::Auto, &[REPO]);
    let mut doc = plan_repo(&store, &kv, &run, REPO, &recs).await;
    let (res, _) = apply_repo(&store, &kv, &run, &mut doc, &all, &admin()).await;
    let v = |id: &str| {
        let e = registry::get(id).unwrap();
        let store = &store;
        async move { registry::read(store, e, REPO).await.unwrap().value }
    };
    assert_eq!(v("stale_prs.enabled").await, true);
    assert_eq!(v("stale_prs.dry_run").await, true);
    assert_eq!(v("auto_merge.enabled").await, false);
    for id in [
        "tags.auto_tag",
        "release_notes.enabled",
        "reminders.enabled",
        "tests.fix_prs",
    ] {
        assert_eq!(v(id).await, false, "{id}");
    }
    assert_eq!(res.summary.previews, 1);
    assert!(res.summary.text.starts_with("Turned on"));
    // Every change keeps its evidence for review afterwards.
    assert!(doc.changes.iter().all(|c| !c.evidence.is_empty()));
}

#[test]
fn a_budget_overrun_stops_an_auto_run_before_anything_is_applied() {
    assert_eq!(budget_gate(Mode::Auto, true, true), BudgetGate::Stop);
    assert_eq!(budget_gate(Mode::Guided, true, true), BudgetGate::SkipAi);
    assert_eq!(budget_gate(Mode::Auto, true, false), BudgetGate::Proceed);
    assert_eq!(budget_gate(Mode::Auto, false, true), BudgetGate::Proceed);
}

#[tokio::test]
async fn a_run_still_analyzing_cant_be_applied() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let run = mk_run(Choice::KeepMine, Mode::Guided, &[REPO]);
    runs::put_run(&kv, T, &run, crate::plugins::kv::Expect::Absent)
        .await
        .unwrap();
    runs::put_repo(&kv, T, &RepoDoc::new(&run.run_id, REPO))
        .await
        .unwrap();
    let err = apply_run(&store, &kv, &run.run_id, None, &all_repos(), &admin())
        .await
        .unwrap_err();
    assert_eq!(err, ApplyRefused::NotReady(RunState::Analyzing));
}

#[tokio::test]
async fn auto_merge_starts_as_a_preview_and_switch_on_is_undoable() {
    let store = MemoryStore::new(T);
    let kv = Memory::new();
    let all = all_repos();
    let mut f = crate::setup::facts::tests::web_app();
    f.merged[0].merged_at = Some(chrono::Utc::now().to_rfc3339());
    let inferred = HashMap::from([(
        "auto_merge.enabled",
        crate::setup::ai::Inferred {
            value: json!(true),
            evidence: vec![Evidence::text("Approved pull requests merge as they are")],
        },
    )]);
    let recs = plan::recommend(
        &f,
        &crate::setup::facts::detect(&f),
        &inferred,
        chrono::Utc::now(),
    );
    assert!(recs["auto_merge.enabled"].evidence[0]
        .text
        .contains("would have merged 1 pull requests"));
    for choice in [Choice::KeepMine, Choice::UseRecommended] {
        let store = MemoryStore::new(T);
        let run = mk_run(choice, Mode::Auto, &[REPO]);
        runs::put_run(&kv, T, &run, crate::plugins::kv::Expect::Absent)
            .await
            .unwrap();
        plan_repo(&store, &kv, &run, REPO, &recs).await;
        let (_, results, _) = apply_run(&store, &kv, &run.run_id, None, &all, &admin())
            .await
            .unwrap();
        let am = registry::get("auto_merge.enabled").unwrap();
        assert_eq!(
            registry::read(&store, am, REPO).await.unwrap().value,
            false,
            "{choice:?}"
        );
        assert!(results[0].summary.previews >= 1);
    }
    // One click turns the preview on; undo of that item puts it back.
    let run = mk_run(Choice::KeepMine, Mode::Auto, &[REPO]);
    runs::put_run(&kv, T, &run, crate::plugins::kv::Expect::Absent)
        .await
        .unwrap();
    plan_repo(&store, &kv, &run, REPO, &recs).await;
    apply_run(&store, &kv, &run.run_id, None, &all, &admin())
        .await
        .unwrap();
    for id in ["auto_merge.enabled", "stale_prs.enabled"] {
        let c = switch_on(&store, &kv, &run.run_id, REPO, id, &all, &admin())
            .await
            .unwrap()
            .expect("switched on");
        let (e, v) = registry::switch_on_target(id).unwrap();
        assert_eq!(c.setting_id, e.id);
        assert_eq!(registry::read(&store, e, REPO).await.unwrap().value, v);
    }
    // Again: nothing to do.
    assert!(switch_on(
        &store,
        &kv,
        &run.run_id,
        REPO,
        "auto_merge.enabled",
        &all,
        &admin()
    )
    .await
    .unwrap()
    .is_none());
    assert!(switch_on(
        &store,
        &kv,
        &run.run_id,
        REPO,
        "reviewer.label",
        &all,
        &admin()
    )
    .await
    .is_err());
    let am = registry::get("auto_merge.enabled").unwrap();
    let pick = vec![(REPO.to_string(), am.id.to_string())];
    let rep = undo(&store, &kv, &run.run_id, Some(&pick), &all, &admin())
        .await
        .unwrap();
    assert_eq!(rep.restored, 1);
    assert_eq!(registry::read(&store, am, REPO).await.unwrap().value, false);
    // A guided run applies it only on a person's explicit pick.
    let store = MemoryStore::new(T);
    let mut run = mk_run(Choice::KeepMine, Mode::Guided, &[REPO]);
    run.decisions = vec![ItemDecision {
        repo: REPO.into(),
        setting_id: am.id.into(),
        decision: Decision::Use,
        value: None,
    }];
    let mut doc = plan_repo(&store, &kv, &run, REPO, &recs).await;
    apply_repo(&store, &kv, &run, &mut doc, &all, &admin()).await;
    assert_eq!(registry::read(&store, am, REPO).await.unwrap().value, true);
}
