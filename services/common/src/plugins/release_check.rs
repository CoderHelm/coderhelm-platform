//! The release check, end to end, with every outside system mocked.
//!
//! The `plugins` flag turns on for teams only when this passes on a test
//! team: a release produces marketing drafts; an editor who isn't the
//! requester approves one for a set time; it publishes then, and a cancel
//! before that time stops it; a new plugin version that adds a tool stays
//! on the old version until approved; a tool missing from the manifest
//! can't be called; the plugin limit stops a run; every step shows in the
//! audit log; and the usage still adds up.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use chrono::{Duration, Utc};
use serde_json::{json, Value};

use super::audit::{self, action};
use super::connect::tests::{FakeConnections, FakeHttp, XorCipher};
use super::connect::{NoRemote, ToolDef};
use super::drafts::{self, Actor, Status};
use super::install::tests::{push, widened, with_source, TEAM};
use super::install::{self, Offer};
use super::kv::Memory;
use super::limits;
use super::publish;
use super::run::{self, Agent, MemoryRuns, RunStatus, ToolExec};
use super::schedule::{MemoryQueue, MemoryScheduler};
use super::{code, flag};
use crate::messages::{PluginPublishMessage, PluginRunMessage};

/// An agent that writes the release's drafts, tries a tool the manifest
/// doesn't grant, reads the CMS, and asks to publish directly.
struct LaunchWriter {
    seen_tag: Mutex<String>,
    seen_tools: Mutex<Vec<String>>,
    results: Mutex<Vec<String>>,
    calls: Vec<(String, Value)>,
}

impl LaunchWriter {
    fn new(calls: Vec<(&str, Value)>) -> Self {
        Self {
            seen_tag: Mutex::new(String::new()),
            seen_tools: Mutex::new(Vec::new()),
            results: Mutex::new(Vec::new()),
            calls: calls.into_iter().map(|(n, v)| (n.to_string(), v)).collect(),
        }
    }
}

#[async_trait::async_trait]
impl Agent for LaunchWriter {
    async fn run(
        &self,
        _team: &str,
        system: &str,
        user: &str,
        tools: &[ToolDef],
        exec: &dyn ToolExec,
    ) -> Result<(String, u64, f64), String> {
        // Every model call in here is tagged with the plugin.
        *self.seen_tag.lock().unwrap() = crate::usage::current_tag();
        assert!(
            system.contains("Read the release notes"),
            "the agent's own brief"
        );
        assert!(
            system.contains("brand-voice"),
            "the plugin's skills are listed"
        );
        assert!(user.contains("v2.0.0"), "the release is the input");
        *self.seen_tools.lock().unwrap() = tools.iter().map(|t| t.name.clone()).collect();
        for (n, v) in &self.calls {
            if exec.stop() {
                break;
            }
            let r = exec.call(n, v).await;
            self.results.lock().unwrap().push(r);
        }
        Ok(("Wrote the launch drafts.".into(), 4200, 0.05))
    }
}

struct World {
    kv: Memory,
    sched: MemoryScheduler,
    queue: MemoryQueue,
    usage: limits::Memory,
    conns: FakeConnections,
    http: FakeHttp,
    runs: MemoryRuns,
}

impl World {
    fn new() -> Self {
        Self {
            kv: Memory::new(),
            sched: MemoryScheduler::default(),
            queue: MemoryQueue::default(),
            usage: limits::Memory::default(),
            conns: FakeConnections {
                tools: [(
                    "sanity".to_string(),
                    vec![
                        "get_document".to_string(),
                        "create_draft".to_string(),
                        "delete_document".to_string(),
                        "update_document".to_string(),
                    ],
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
            http: FakeHttp::default(),
            runs: MemoryRuns::default(),
        }
    }

    fn run_deps<'a>(&'a self, agent: &'a dyn Agent) -> run::Deps<'a> {
        run::Deps {
            kv: &self.kv,
            usage: &self.usage,
            agent,
            conns: &self.conns,
            http: &self.http,
            cipher: &XorCipher,
            knowledge: &run::NoKnowledge,
            runs: &self.runs,
        }
    }

    fn publish_deps(&self) -> publish::Deps<'_> {
        publish::Deps {
            kv: &self.kv,
            sched: &self.sched,
            http: &self.http,
            conns: &self.conns,
            cipher: &XorCipher,
        }
    }

    /// Runs every queued plugin_run message.
    async fn work_runs(&self, agent: &dyn Agent) -> Vec<run::RunRecord> {
        let mut out = Vec::new();
        for m in self.queue.take() {
            if m["type"] == "plugin_run" {
                let msg: PluginRunMessage = serde_json::from_value(m).unwrap();
                out.push(run::execute(&self.run_deps(agent), &msg).await);
            }
        }
        out
    }

    /// Publishes every queued (or fired) plugin_publish message.
    async fn work_publishes(&self, msgs: Vec<Value>) -> Vec<publish::Outcome> {
        let mut out = Vec::new();
        for m in msgs {
            if m["type"] == "plugin_publish" {
                let msg: PluginPublishMessage = serde_json::from_value(m).unwrap();
                out.push(
                    publish::run(&self.publish_deps(), &msg, Utc::now())
                        .await
                        .unwrap(),
                );
            }
        }
        out
    }
}

fn editors() -> install::Approvers {
    install::Approvers {
        members: vec!["editor@a.io".into()],
        min: 1,
    }
}

#[tokio::test]
async fn the_release_check() {
    let w = World::new();
    let _ = NoRemote; // remote OAuth connections are not needed: a pasted key is set below

    // The flag is off by default: nothing runs.
    assert!(run::on_release(
        &w.kv,
        &w.queue,
        Some(&w.runs),
        &w.usage,
        TEAM,
        "acme/app",
        "v2.0.0",
        "notes"
    )
    .await
    .is_empty());
    flag::set(&w.kv, TEAM, true).await.unwrap();

    // Install: an admin asks, a second admin approves; pinned by hash.
    let (repo, src) = with_source(&w.kv).await;
    let p = install::install(&w.kv, TEAM, &src, "plugins/marketing", "admin@a.io", 2)
        .await
        .unwrap();
    let hash = p.pending.unwrap().version.hash;
    assert_eq!(
        install::approve(&w.kv, TEAM, "marketing", &hash, "admin@a.io", 2)
            .await
            .unwrap_err()
            .code,
        code::NEEDS_SECOND_ADMIN
    );
    let p = install::approve(&w.kv, TEAM, "marketing", &hash, "admin2@a.io", 2)
        .await
        .unwrap();
    assert!(p.active());
    install::set_approvers(
        &w.kv,
        TEAM,
        "marketing",
        vec!["editor@a.io".into()],
        1,
        Some("both".into()),
        "admin@a.io",
    )
    .await
    .unwrap();
    let vals = BTreeMap::from([("linkedin".to_string(), "li-key-123".to_string())]);
    install::set_settings(&w.kv, &XorCipher, TEAM, "marketing", &vals, "admin@a.io")
        .await
        .unwrap();

    // A release produces marketing drafts.
    let agent = LaunchWriter::new(vec![
        ("read_plugin_skill", json!({"name": "brand-voice"})),
        ("sanity__get_document", json!({"id": "release-page"})),
        ("sanity__delete_document", json!({"id": "home"})),
        ("write_file", json!({"path": "x", "content": "y"})),
        (
            "save_draft",
            json!({"kind": "blog", "title": "Version 2.0 is here", "content": "Blog body.", "channel": "blog"}),
        ),
        (
            "save_draft",
            json!({"kind": "social", "title": "2.0 on LinkedIn", "content": "We shipped 2.0.", "channel": "linkedin", "publish": true}),
        ),
        (
            "save_draft",
            json!({"kind": "social", "title": "2.0 variant B", "content": "2.0 is out.", "channel": "linkedin", "publish": true}),
        ),
        (
            "publish_post",
            json!({"title": "Direct", "content": "Posting now"}),
        ),
    ]);
    let started = run::on_release(
        &w.kv,
        &w.queue,
        Some(&w.runs),
        &w.usage,
        TEAM,
        "acme/app",
        "v2.0.0",
        "Notes for v2.0.0",
    )
    .await;
    assert_eq!(started.len(), 1);
    let ran = w.work_runs(&agent).await;
    assert_eq!(ran[0].status, RunStatus::Completed, "{:?}", ran[0].reason);
    assert_eq!(
        *agent.seen_tag.lock().unwrap(),
        "plugin:marketing",
        "usage is tagged with the plugin"
    );
    assert_eq!(ran[0].tokens, 4200);

    // A tool missing from the manifest can't be called, and isn't offered.
    let seen = agent.seen_tools.lock().unwrap().clone();
    assert!(seen.contains(&"sanity__get_document".to_string()));
    assert!(!seen.contains(&"sanity__delete_document".to_string()));
    assert!(!seen.contains(&"write_file".to_string()));
    let results = agent.results.lock().unwrap().clone();
    assert!(
        results[2].contains("isn't one of this plugin's tools"),
        "{}",
        results[2]
    );
    assert!(results[3].contains("isn't one of this plugin's tools"));
    assert!(!w
        .conns
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(_, t, _)| t == "delete_document"));
    // A consequential tool in a run only queues an approval.
    assert!(results[7].contains("Queued for approval"), "{}", results[7]);
    assert!(
        w.http.sent.lock().unwrap().is_empty(),
        "nothing was sent during the run"
    );

    let drafts_now = drafts::list(&w.kv, TEAM, Some(Status::InReview), Some("marketing"), None)
        .await
        .unwrap();
    assert_eq!(drafts_now.len(), 4, "three drafts and one queued action");
    assert!(drafts_now.iter().all(|d| d.requested_by == "release"));
    let post = drafts_now
        .iter()
        .find(|d| d.title == "2.0 on LinkedIn")
        .unwrap()
        .clone();
    let variant = drafts_now
        .iter()
        .find(|d| d.title == "2.0 variant B")
        .unwrap()
        .clone();

    // The editor edits one variant, then approves it for a set time.
    let editor = Actor::new("editor@a.io", false);
    let post = drafts::edit(
        &w.kv,
        &w.sched,
        TEAM,
        &post.id,
        "We shipped 2.0. Read more.",
        None,
        1,
        &editor,
    )
    .await
    .unwrap();
    assert_eq!(post.version, 2);
    // The editor edited it but the release asked for it: they may approve.
    let now = Utc::now();
    let at = now + Duration::days(1);
    // An outsider can't; an old version can't be approved.
    assert_eq!(
        drafts::approve(
            &w.kv,
            &w.sched,
            &w.queue,
            TEAM,
            &post.id,
            2,
            Some(at),
            &editors(),
            &Actor::new("someone@a.io", false),
            "dashboard",
            now
        )
        .await
        .unwrap_err()
        .code,
        code::NOT_IN_GROUP
    );
    assert_eq!(
        drafts::approve(
            &w.kv,
            &w.sched,
            &w.queue,
            TEAM,
            &post.id,
            1,
            Some(at),
            &editors(),
            &editor,
            "dashboard",
            now
        )
        .await
        .unwrap_err()
        .code,
        code::STALE_VERSION
    );
    let scheduled = drafts::approve(
        &w.kv,
        &w.sched,
        &w.queue,
        TEAM,
        &post.id,
        2,
        Some(at),
        &editors(),
        &editor,
        "dashboard",
        now,
    )
    .await
    .unwrap();
    assert_eq!(scheduled.status, Status::Scheduled);
    // The other variant is approved for the same time, then cancelled.
    drafts::approve(
        &w.kv,
        &w.sched,
        &w.queue,
        TEAM,
        &variant.id,
        1,
        Some(at),
        &editors(),
        &editor,
        "email",
        now,
    )
    .await
    .unwrap();
    drafts::cancel(&w.kv, &w.sched, TEAM, &variant.id, &editors(), &editor)
        .await
        .unwrap();

    // Nothing goes out before the time.
    assert!(w.sched.due(now + Duration::hours(23)).is_empty());
    assert!(w.http.sent.lock().unwrap().is_empty());
    // At the time, the approved version is published, once.
    let fired = w.sched.due(at + Duration::minutes(1));
    assert_eq!(fired.len(), 1, "the cancelled one never fires");
    let again = fired.clone();
    let outcomes = w.work_publishes(fired).await;
    assert!(
        matches!(&outcomes[0], publish::Outcome::Published(Some(u)) if u.contains("posts/1")),
        "{outcomes:?}"
    );
    let sent = w.http.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].0, "li-key-123",
        "the sealed key is opened for the request"
    );
    assert_eq!(sent[0].2, "https://api.linkedin.com/rest/posts");
    assert_eq!(
        sent[0].3["content"], "We shipped 2.0. Read more.",
        "exactly the approved version"
    );
    // A duplicate delivery sends nothing more.
    let dup = w.work_publishes(again).await;
    assert!(matches!(dup[0], publish::Outcome::Refused(_)));
    assert_eq!(w.http.sent.lock().unwrap().len(), 1);
    let variant_now = drafts::get(&w.kv, TEAM, &variant.id).await.unwrap().0;
    assert_eq!(variant_now.status, Status::Cancelled);

    // A new plugin version that adds a tool stays on the old version until
    // an admin approves it.
    push(&repo, "b2b2b2b2", &widened());
    let offer = install::sync_plugin(&w.kv, &repo, TEAM, None, "marketing", "system", false)
        .await
        .unwrap();
    assert!(matches!(offer, Offer::Waiting(_)));
    let p = install::get(&w.kv, TEAM, "marketing").await.unwrap().0;
    assert_eq!(p.current.as_ref().unwrap().sha, "a1a1a1a1");
    // A run now still can't call the new tool.
    let tries = LaunchWriter::new(vec![("sanity__update_document", json!({"id": "x"}))]);
    let inputs = BTreeMap::from([("brief".to_string(), json!("Webinar invite"))]);
    run::request(
        &w.kv,
        &w.queue,
        Some(&w.runs),
        &w.usage,
        TEAM,
        "marketing",
        "manual",
        inputs.clone(),
        None,
        None,
        "marketer@a.io",
    )
    .await
    .unwrap();
    let briefs = LaunchWriterBrief(tries);
    let r = w.work_runs(&briefs).await;
    assert_eq!(r[0].status, RunStatus::Completed);
    assert!(briefs.0.results.lock().unwrap()[0].contains("isn't one of this plugin's tools"));
    // Approved, the new tool is granted.
    let pending = p.pending.unwrap().version.hash;
    install::approve(&w.kv, TEAM, "marketing", &pending, "admin2@a.io", 2)
        .await
        .unwrap();
    let snap = install::current_snapshot(
        &w.kv,
        TEAM,
        &install::get(&w.kv, TEAM, "marketing").await.unwrap().0,
    )
    .await
    .unwrap();
    assert!(snap.manifest.grant().allows("sanity__update_document"));

    // The plugin limit stops a run, before it starts and in the middle.
    install::set_limit(&w.kv, TEAM, "marketing", 10_000, "admin@a.io")
        .await
        .unwrap();
    w.usage
        .plugin
        .lock()
        .unwrap()
        .insert("marketing".into(), 10_000);
    let refused = run::request(
        &w.kv,
        &w.queue,
        Some(&w.runs),
        &w.usage,
        TEAM,
        "marketing",
        "manual",
        inputs.clone(),
        None,
        None,
        "marketer@a.io",
    )
    .await;
    assert_eq!(refused.unwrap_err().code, code::LIMIT_REACHED);
    w.usage.plugin.lock().unwrap().insert("marketing".into(), 0);
    run::request(
        &w.kv,
        &w.queue,
        Some(&w.runs),
        &w.usage,
        TEAM,
        "marketing",
        "manual",
        inputs.clone(),
        None,
        None,
        "marketer@a.io",
    )
    .await
    .unwrap();
    w.usage
        .plugin
        .lock()
        .unwrap()
        .insert("marketing".into(), 10_000);
    let stopped = LaunchWriterBrief(LaunchWriter::new(vec![(
        "save_draft",
        json!({"kind": "email", "title": "t", "content": "c"}),
    )]));
    let r = w.work_runs(&stopped).await;
    assert_eq!(r[0].status, RunStatus::Refused);
    assert_eq!(r[0].code, code::LIMIT_REACHED);
    assert!(r[0].artifacts.is_empty(), "no step ran past the limit");

    // Every step shows in the audit log.
    let all = audit::for_filter(&w.kv, TEAM, &audit::Filter::default()).await;
    let acts: BTreeSet<&str> = all.iter().map(|e| e.action.as_str()).collect();
    for want in [
        action::SOURCE_ADDED,
        action::SOURCE_SYNCED,
        action::INSTALL_REQUESTED,
        action::INSTALLED,
        action::PLUGIN_SETTINGS,
        action::RUN_REQUESTED,
        action::RUN_STARTED,
        action::TOOL_CALLED,
        action::TOOL_REFUSED,
        action::TOOL_QUEUED,
        action::DRAFT_SAVED,
        action::DRAFT_EDITED,
        action::APPROVAL_REFUSED,
        action::APPROVED,
        action::SCHEDULED,
        action::CANCELLED,
        action::PUBLISHED,
        action::PUBLISH_REFUSED,
        action::UPDATE_WAITING,
        action::UPDATE_APPROVED,
        action::RUN_FINISHED,
        action::RUN_REFUSED,
    ] {
        assert!(acts.contains(want), "audit is missing {want}: {acts:?}");
    }
    // Each tool call names its tool and risk; the publish names the version
    // and who approved is on the approval.
    assert!(all
        .iter()
        .filter(|e| e.action == action::TOOL_CALLED)
        .all(|e| !e.tool.is_empty() && !e.risk.is_empty()));
    let published = all.iter().find(|e| e.action == action::PUBLISHED).unwrap();
    assert_eq!(
        (published.tool.as_str(), published.risk.as_str()),
        ("publish_post", "consequential")
    );
    assert!(published.detail.starts_with("version 2"));
    let final_post = drafts::get(&w.kv, TEAM, &post.id).await.unwrap().0;
    assert_eq!(final_post.approval.unwrap().decided_by, "editor@a.io");

    // The runs are listed with the team's runs.
    assert!(w
        .runs
        .written
        .lock()
        .unwrap()
        .iter()
        .any(|(t, r)| t == TEAM && r.status == RunStatus::Completed));

    // Team isolation: another team sees and reaches none of it.
    let other = "TEAM#b";
    flag::set(&w.kv, other, true).await.unwrap();
    assert!(install::list(&w.kv, other).await.unwrap().is_empty());
    assert!(drafts::list(&w.kv, other, None, None, None)
        .await
        .unwrap()
        .is_empty());
    assert!(run::list(&w.kv, other, None, 50).await.unwrap().is_empty());
    assert!(audit::for_filter(&w.kv, other, &audit::Filter::default())
        .await
        .is_empty());
    let forged = PluginPublishMessage {
        team_id: other.into(),
        artifact_id: post.id.clone(),
        version: 2,
        approval_id: String::new(),
    };
    assert_eq!(
        publish::run(&w.publish_deps(), &forged, Utc::now())
            .await
            .unwrap_err()
            .code,
        code::NOT_FOUND
    );
    let foreign_run = PluginRunMessage {
        team_id: other.into(),
        run_id: "r-x".into(),
        plugin_id: "marketing".into(),
        trigger: "manual".into(),
        requested_by: "x@b.io".into(),
        ..Default::default()
    };
    let r = run::execute(&w.run_deps(&stopped), &foreign_run).await;
    assert_eq!(r.status, RunStatus::Refused);
    assert_eq!(r.code, code::NOT_FOUND);
}

/// The same agent for a brief (the input has no release).
struct LaunchWriterBrief(LaunchWriter);

#[async_trait::async_trait]
impl Agent for LaunchWriterBrief {
    async fn run(
        &self,
        _team: &str,
        _system: &str,
        _user: &str,
        tools: &[ToolDef],
        exec: &dyn ToolExec,
    ) -> Result<(String, u64, f64), String> {
        *self.0.seen_tools.lock().unwrap() = tools.iter().map(|t| t.name.clone()).collect();
        for (n, v) in &self.0.calls {
            if exec.stop() {
                break;
            }
            let r = exec.call(n, v).await;
            self.0.results.lock().unwrap().push(r);
        }
        Ok(("ok".into(), 10, 0.0))
    }
}

#[tokio::test]
async fn write_tools_wait_for_a_person_under_a_read_only_cap() {
    let w = World::new();
    flag::set(&w.kv, TEAM, true).await.unwrap();
    let (_, src) = with_source(&w.kv).await;
    install::install(&w.kv, TEAM, &src, "plugins/marketing", "admin@a.io", 1)
        .await
        .unwrap();
    let mut s = flag::settings(&w.kv, TEAM).await.unwrap();
    s.autonomy_cap = crate::tool_registry::Risk::Read;
    flag::save_settings(&w.kv, TEAM, &s).await.unwrap();
    let agent = LaunchWriterBrief(LaunchWriter::new(vec![
        ("sanity__create_draft", json!({"title": "Post"})),
        ("sanity__get_document", json!({"id": "a"})),
    ]));
    let inputs = BTreeMap::from([("brief".to_string(), json!("x"))]);
    run::request(
        &w.kv,
        &w.queue,
        Some(&w.runs),
        &w.usage,
        TEAM,
        "marketing",
        "manual",
        inputs,
        None,
        None,
        "m@a.io",
    )
    .await
    .unwrap();
    w.work_runs(&agent).await;
    let res = agent.0.results.lock().unwrap().clone();
    assert!(res[0].contains("Queued for approval"), "{res:?}");
    let calls = w.conns.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1, "only the read ran");
    assert_eq!(calls[0].1, "get_document");
}

#[tokio::test]
async fn a_run_for_a_moved_version_or_a_turned_off_plugin_is_refused() {
    let w = World::new();
    flag::set(&w.kv, TEAM, true).await.unwrap();
    let (_, src) = with_source(&w.kv).await;
    install::install(&w.kv, TEAM, &src, "plugins/marketing", "admin@a.io", 1)
        .await
        .unwrap();
    let agent = LaunchWriterBrief(LaunchWriter::new(vec![]));
    let msg = PluginRunMessage {
        team_id: TEAM.into(),
        run_id: "r1".into(),
        plugin_id: "marketing".into(),
        hash: "not-the-pinned-hash".into(),
        trigger: "manual".into(),
        requested_by: "m@a.io".into(),
        ..Default::default()
    };
    let r = run::execute(&w.run_deps(&agent), &msg).await;
    assert_eq!(r.code, code::STALE_VERSION);
    install::set_enabled(&w.kv, TEAM, "marketing", false, "admin@a.io")
        .await
        .unwrap();
    let inputs = BTreeMap::from([("brief".to_string(), json!("x"))]);
    assert_eq!(
        run::request(
            &w.kv,
            &w.queue,
            Some(&w.runs),
            &w.usage,
            TEAM,
            "marketing",
            "manual",
            inputs,
            None,
            None,
            "m@a.io"
        )
        .await
        .unwrap_err()
        .code,
        code::WRONG_STATE
    );
}

#[tokio::test]
async fn results_come_back_as_lessons_for_the_next_run() {
    let w = World::new();
    flag::set(&w.kv, TEAM, true).await.unwrap();
    let (repo, src) = with_source(&w.kv).await;
    // A version that reads results back through PostHog.
    let with_results = crate::plugins::manifest::tests::MARKETING
        .replace(
            "  - {name: publish_post",
            "  - {ref: \"mcp:posthog\", allow: [query_*]}\n  - {name: publish_post",
        )
        .replace(
            "budget:",
            "results: {via: \"mcp:posthog\", tool: query_insight, after_days: 7}\nbudget:",
        );
    push(&repo, "a1a1a1a1", &with_results);
    crate::plugins::source::sync(&w.kv, &repo, TEAM, None, &src, true)
        .await
        .unwrap();
    install::install(&w.kv, TEAM, &src, "plugins/marketing", "admin@a.io", 1)
        .await
        .unwrap();
    let vals = BTreeMap::from([("linkedin".to_string(), "k".to_string())]);
    install::set_settings(&w.kv, &XorCipher, TEAM, "marketing", &vals, "admin@a.io")
        .await
        .unwrap();
    let a = drafts::save(
        &w.kv,
        TEAM,
        &drafts::tests::origin("release"),
        drafts::tests::post(true),
    )
    .await
    .unwrap();
    let approvers = install::Approvers::default();
    drafts::approve(
        &w.kv,
        &w.sched,
        &w.queue,
        TEAM,
        &a.id,
        1,
        None,
        &approvers,
        &Actor::new("admin@a.io", true),
        "dashboard",
        Utc::now(),
    )
    .await
    .unwrap();
    let msgs = w.queue.take();
    w.work_publishes(msgs).await;
    // A results check is scheduled a week out.
    let fired = w.sched.due(Utc::now() + Duration::days(8));
    assert_eq!(fired[0]["type"], "plugin_results");
    let msg: crate::messages::PluginResultsMessage =
        serde_json::from_value(fired[0].clone()).unwrap();
    let lesson = publish::results(&w.publish_deps(), &msg)
        .await
        .unwrap()
        .unwrap();
    assert!(lesson.numbers.contains("views"));
    let lessons = publish::lessons(&w.kv, TEAM, "marketing").await;
    assert_eq!(lessons.len(), 1);
    let p = install::get(&w.kv, TEAM, "marketing").await.unwrap().0;
    let snap = install::current_snapshot(&w.kv, TEAM, &p).await.unwrap();
    let sys = run::system_prompt("brief", &snap.manifest, &snap, &lessons, &BTreeMap::new());
    assert!(sys.contains("How earlier published drafts did"));
}

/// The public example in `plugins/marketing` reads, checks and grants what
/// its README says.
#[tokio::test]
async fn the_example_marketing_plugin_is_valid() {
    struct Disk(std::path::PathBuf);
    #[async_trait::async_trait]
    impl super::package::RepoRead for Disk {
        async fn resolve(&self, _r: &str) -> Result<String, String> {
            Ok("0000000".into())
        }
        async fn tree(&self, _sha: &str) -> Result<Vec<(String, u64)>, String> {
            fn walk(base: &std::path::Path, dir: &std::path::Path, out: &mut Vec<(String, u64)>) {
                for e in std::fs::read_dir(dir).unwrap().flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        walk(base, &p, out);
                    } else {
                        let rel = p
                            .strip_prefix(base)
                            .unwrap()
                            .to_string_lossy()
                            .replace('\\', "/");
                        out.push((rel, e.metadata().unwrap().len()));
                    }
                }
            }
            let mut out = Vec::new();
            walk(&self.0, &self.0.join("plugins"), &mut out);
            Ok(out)
        }
        async fn read(&self, _sha: &str, path: &str) -> Result<String, String> {
            std::fs::read_to_string(self.0.join(path)).map_err(|e| e.to_string())
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let disk = Disk(root);
    let tree = super::package::RepoRead::tree(&disk, "x").await.unwrap();
    assert!(super::package::plugin_roots(&tree).contains(&"plugins/marketing".to_string()));
    let pkg = super::package::read(&disk, "x", "plugins/marketing")
        .await
        .unwrap();
    let m = &pkg.manifest;
    assert_eq!(m.id(), "marketing");
    assert_eq!(
        pkg.items
            .iter()
            .filter(|i| i.kind == crate::skills::Kind::Agent)
            .count(),
        2
    );
    assert_eq!(
        pkg.items
            .iter()
            .filter(|i| i.kind == crate::skills::Kind::Skill)
            .count(),
        2
    );
    let g = m.grant();
    use crate::tool_registry::Risk;
    assert_eq!(g.risk("publish_post"), Some(Risk::Consequential));
    assert_eq!(g.risk("send_email"), Some(Risk::Consequential));
    assert_eq!(g.risk("generate_image"), Some(Risk::Write));
    assert_eq!(g.risk("sanity__create_draft"), Some(Risk::Write));
    assert_eq!(g.risk("posthog__query_insight"), Some(Risk::Read));
    assert!(!g.allows("sanity__delete_document"));
    assert!(m.results.is_some());
    // The LinkedIn body fills in the approved text and the author setting.
    let spec = m.named_tool("publish_post").unwrap();
    let fields = BTreeMap::from([
        ("title", "T".to_string()),
        ("content", "Body".to_string()),
        ("kind", "social".into()),
        ("channel", "linkedin".into()),
    ]);
    let settings = BTreeMap::from([(
        "linkedin_author".to_string(),
        "urn:li:organization:1".to_string(),
    )]);
    let body = super::manifest::render_body(spec.body.as_ref().unwrap(), &fields, &settings);
    assert_eq!(body["commentary"], "Body");
    assert_eq!(body["author"], "urn:li:organization:1");
    // No customer names or brand content in the public example.
    for (path, text) in pkg
        .items
        .iter()
        .map(|i| (i.path.clone(), i.body.clone()))
        .chain(std::iter::once((
            "plugin.yaml".into(),
            pkg.manifest_text.clone(),
        )))
    {
        let lower = text.to_lowercase();
        for word in ["acme", "@"] {
            assert!(
                !lower.contains(word) || word == "@" && !lower.contains(".com>"),
                "{path} mentions {word}"
            );
        }
    }
}
