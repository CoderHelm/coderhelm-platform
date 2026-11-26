//! dt. A few pull requests' release records sat for good with their
//! production deploy at "approval" or "pending", while production already
//! ran a later release tag that held their merges.
//!
//! Each release record is moved by a chain of delayed tick messages, one
//! tick sending the next. When a chain stopped (an old bug, a lost message,
//! a failed send, a crash), nothing resumed it: only a new merge on that
//! same pull request armed it again. The release catch-up looks only at a
//! default-branch head no tag holds, so it skipped these. And had a tick
//! run, a production tag step waiting on its card would have asked again
//! (or, approved, cut a new tag) for a change already in production.
//!
//! Guarantee: every save records when the next tick is due. The review
//! sweep resumes a record with work left whose tick is overdue (an older
//! record without that time is judged by its last write), one normal tick
//! on a fresh chain, claimed against the item as read, so two sweeps never
//! both resume it and a chain still running is never doubled. GitHub's
//! budget is asked first. After three resumes in a row that got no tick
//! through, the record is left with its last error. A resumed tick whose
//! merge a release tag already holds follows that tag's deploy run and says
//! its outcome once, at the time it went out; it never cuts a tag or asks a
//! card, and a card asked before it shipped expires. A merge no tag holds
//! yet goes the normal way.
//!
//! The first sweep found the records with a filtered scan of the settings
//! table capped at a number of pages; on a large settings table it never
//! reached them. The sweep now reads a settings index of the teams with a
//! per-PR release record (each save adds the team; a daily full scan
//! rebuilds it), then each team's records by key, every page to the end.

use serde_json::json;

use crate::passes::release_plan::{self, On, Status, Step};
use crate::passes::release_run::{self, fake, Meta, Next, Record, Stall};

const MERGE: &str = "5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f";
const NOW: i64 = 1_900_000_000;
const DAY: i64 = 86_400;

const DEPLOY_PRODUCTION: &str = r#"
name: Deploy Production
on:
  push:
    tags: ['v*.*.*']
jobs:
  deploy:
    environment: production
    runs-on: ubuntu-latest
    steps:
      - run: ./deploy.sh
"#;

fn tag_step(status: Status) -> Step {
    let m = release_plan::mechanics(
        &[(
            ".github/workflows/deploy-production.yml".into(),
            DEPLOY_PRODUCTION.into(),
        )],
        None,
    );
    let mut s = release_plan::follow_steps(&m)[0].clone();
    assert!(matches!(s.trigger, Some(On::Tag(_))));
    s.status = status;
    if status == Status::Approval {
        s.approval_id = "card-1".into();
        s.detail = "approval card `card-1` on the Agent permissions page".into();
    }
    s
}

/// A record as an older worker left it: no tick time, no write id, its
/// last write days ago.
fn left_behind(status: Status) -> (Record, Meta) {
    let rec = Record {
        steps: vec![tag_step(status)],
        head_branch: "fix/wing-timeouts".into(),
        base_branch: "main".into(),
        chain_id: "old-chain".into(),
        armed_at: NOW - 9 * DAY,
        ..Default::default()
    };
    let meta = Meta {
        updated_at: Some(NOW - 6 * DAY),
        ..Default::default()
    };
    (rec, meta)
}

#[test]
fn a_record_left_at_approval_or_pending_is_resumed() {
    for status in [Status::Approval, Status::Pending] {
        let (rec, meta) = left_behind(status);
        assert_eq!(release_run::stall(&rec, &meta, NOW), Stall::Resume);
    }
    // The key names the repository and pull request the tick is for.
    assert_eq!(
        release_run::parse_sk("RELEASE#harbor/wing-api#000007"),
        Some(("harbor".into(), "wing-api".into(), 7))
    );
}

#[test]
fn a_resume_is_a_fresh_chain_claimed_once() {
    let (rec, meta) = left_behind(Status::Approval);
    let fresh = release_run::resumed(&rec, 41, "run-1", NOW);
    // The stopped chain's tick, should it still arrive, is dropped; the
    // release gets a fresh age, so it does not park on its first tick.
    assert_ne!(fresh.chain_id, rec.chain_id);
    assert_eq!((fresh.armed_at, fresh.installation_id), (NOW, 41));
    assert_eq!(fresh.run_id, "run-1");
    // A second sweep that read the same item loses the claim: the claim's
    // write changed it.
    let after_claim = Meta {
        write_id: "w-new".into(),
        updated_at: Some(NOW),
        next_tick_at: Some(NOW),
        resume_attempts: 1,
        last_error: String::new(),
    };
    assert!(!meta.still(&after_claim));
    // And a sweep reading after the claim sees the tick in flight.
    assert_eq!(
        release_run::stall(&fresh, &after_claim, NOW + 60),
        Stall::Live
    );
    let src = include_str!("../release_run.rs");
    let claim = &src[src.find("async fn claim(").unwrap()..];
    assert!(claim.contains(".condition_expression(cond)"));
    let sweep = &src[src.find("pub async fn resume_stalled(").unwrap()..];
    let gate = sweep
        .find("budget.allows(state, iid, TICK_REQUESTS)")
        .unwrap();
    let claimed = sweep.find("claim(state, &team_id, &sk, &fresh").unwrap();
    let sent = sweep.find("send(state, &msg, 0)").unwrap();
    assert!(gate < claimed && claimed < sent);
}

#[test]
fn a_shipped_merge_follows_the_tag_that_holds_it() {
    // Production runs `v2.4.0`; `v2.3.0` was the first tag with the merge.
    let looked = vec![
        (("v2.4.0".to_string(), "c240".to_string()), true),
        (("v2.3.0".to_string(), "c230".to_string()), true),
        (("v2.2.0".to_string(), "c220".to_string()), false),
    ];
    let (tag, sha) = release_run::first_tag_holding(&looked).unwrap();
    assert_eq!((tag.as_str(), sha.as_str()), ("v2.3.0", "c230"));
    // No release tag holds it yet: the normal way (a tag sweep, or a card
    // as the team's settings say).
    assert!(release_run::first_tag_holding(&looked[2..]).is_none());

    let (mut rec, _) = left_behind(Status::Approval);
    assert_eq!(release_run::next(&rec.steps, 0, true), Next::Start);
    assert!(release_run::approval_line(&rec.steps).is_some());
    release_run::follow_shipped(&mut rec.steps[0], &tag, &sha);
    let s = &mut rec.steps[0];
    assert_eq!((s.status, s.tag.as_str()), (Status::Running, "v2.3.0"));
    // Followed, not asked: the approval line on the pull request goes.
    assert_eq!(release_run::next(&rec.steps, 0, true), Next::Check);
    assert!(release_run::approval_line(&rec.steps).is_none());

    // The tag's deploy run, finished long before the tick read it.
    let run = json!({
        "id": 3003, "head_sha": "c230", "status": "completed",
        "conclusion": "success", "html_url": "https://x/runs/3003",
        "created_at": "2030-03-14T09:00:00Z", "updated_at": "2030-03-14T09:12:00Z",
    });
    let s = &mut rec.steps[0];
    assert!(release_run::pick_run(std::slice::from_ref(&run), s.started_at, &s.tag_sha).is_some());
    assert!(release_run::run_outcome(&run).unwrap().is_ok());
    s.status = Status::Done;
    s.run_id = 3003;
    s.finished_at = release_run::finished_at(&run);
    let line = release_run::deploy_outcome_line(
        s,
        Status::Running,
        "harbor",
        "wing-api",
        release_run::said_at(s),
    )
    .unwrap();
    assert_eq!(
        line,
        "Deployed to production in `v2.3.0` at 09:12 UTC \
         (https://github.com/harbor/wing-api/actions/runs/3003)."
    );
    let _ = MERGE;
}

#[test]
fn a_shipped_merge_is_never_tagged_or_asked() {
    let src = include_str!("../release_run.rs");
    let tick = &src[src.find("async fn tick(").unwrap()..];
    // The look for a tag that holds the merge comes before the team's
    // release and before the gate (which would ask a card, or cut a tag).
    let shipped = tick.find("match tag_holding(").unwrap();
    let team = tick.find("team_release(").unwrap();
    let gate = tick.find(".check(Request {").unwrap();
    assert!(shipped < team && team < gate);
    // A card asked before it shipped expires.
    assert!(tick.contains("retire_card(state, &msg.team_id, &s.approval_id, &tag)"));
}

#[test]
fn resumes_that_never_get_through_stop_with_their_error() {
    let (rec, mut meta) = left_behind(Status::Pending);
    meta.resume_attempts = release_run::MAX_RESUMES;
    meta.last_error = "GitHub answered 404".into();
    assert_eq!(release_run::stall(&rec, &meta, NOW), Stall::GaveUp);
    // A resumed chain's failed tick waits for the sweep (which counts it),
    // unless GitHub's budget asked it to wait.
    assert!(!release_run::retry_after_failure(DAY, 1, None));
    assert!(release_run::retry_after_failure(DAY, 1, Some(600)));
    assert!(release_run::retry_after_failure(DAY, 0, None));
}

#[test]
fn the_sweep_runs_after_the_release_catch_up() {
    let main = include_str!("../../main.rs");
    let catch_up = main
        .find("passes::review_actions::release_catch_up(&state)")
        .unwrap();
    let resume = main
        .find("passes::release_run::resume_stalled(&state)")
        .unwrap();
    assert!(catch_up < resume);
}

#[tokio::test]
async fn a_record_on_a_large_settings_table_is_found_and_resumed() {
    // Synthetic: many pages of other teams' items, then one team with a
    // release record left at approval and many release tag records.
    let table = fake::Table::new(25);
    for n in 0..2_000 {
        table.put(fake::item(
            &format!("TEAM#crowd{:04}", n / 4),
            &format!("ANALYTICS#runs#{n:05}"),
        ));
    }
    let (rec, meta) = left_behind(Status::Approval);
    let sk = "RELEASE#harbor/wing-api#000007";
    table.put(fake::release_item("TEAM#harbor", sk, &rec, &meta));
    for v in 0..120 {
        table.put(fake::item(
            "TEAM#harbor",
            &format!("RELEASE#harbor/wing-api#v2.{v}.0"),
        ));
    }
    // Far past any page cap the old scan had.
    let pages = table.scan_pages();
    let page_of_record = pages
        .iter()
        .position(|p| {
            p.iter()
                .any(|i| i.get("sk").and_then(|v| v.as_s().ok()).map(String::as_str) == Some(sk))
        })
        .unwrap();
    assert!(page_of_record >= 50);

    // Saved before the index existed: the rebuild's full scan finds it. A
    // tag record never makes a team a member on its own.
    table.rebuild_index();
    assert_eq!(table.members(), vec!["TEAM#harbor".to_string()]);
    let found = release_run::find_stalled(&table, NOW).await.unwrap();
    assert_eq!(found.stalled.len(), 1);
    let (team, got_sk, got, got_meta) = &found.stalled[0];
    assert_eq!((team.as_str(), got_sk.as_str()), ("TEAM#harbor", sk));
    assert_eq!(release_run::stall(got, got_meta, NOW), Stall::Resume);
    // The team's partition is read to its last page, past the tag records.
    assert!(*table.pages_read.lock().unwrap() >= 5);

    // Once the record is gone, the team leaves the index.
    let empty = fake::Table::new(25);
    empty.add_member("TEAM#harbor");
    empty.put(fake::item("TEAM#harbor", "RELEASE#harbor/wing-api#v2.0.0"));
    let found = release_run::find_stalled(&empty, NOW).await.unwrap();
    assert_eq!(found.dropped, vec!["TEAM#harbor".to_string()]);
    assert!(empty.members().is_empty());

    let src = include_str!("../release_run.rs");
    let sweep = &src[src.find("pub async fn resume_stalled(").unwrap()..];
    let sweep = &sweep[..sweep.find("\n}\n").unwrap()];
    assert!(!sweep.contains(".scan()"));
}
