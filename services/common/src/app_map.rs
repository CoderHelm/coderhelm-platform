//! The app map: CoderHelm's adapter around Floorplan (vendored at
//! `vendor/floorplan`, MIT, https://github.com/haydenschmitty/floorplan).
//!
//! Floorplan does the mapping: its extractor finds every repository's
//! files, imports, endpoints, requests and screens, matches requests to
//! endpoints across repositories, and its builder writes one map
//! (`floorplan/graph@1`) with Health, screens and the tour. The worker runs
//! it in the run sandbox (`worker::passes::app_map`, driver in
//! `services/worker/floorplan/driver.mjs`) and does the model jobs its
//! `workflow.json` describes with the team's own model, metered.
//!
//! This module is what CoderHelm adds, kept out of the vendored files:
//!
//! - **Storage**: the map, the run state and the step's job in the bucket
//!   (`app-map/{team}/...`), the build status in the settings table
//!   (`pk = AM#{team}`, `sk = STATUS`).
//! - **Overlays in Floorplan's own fields**, so its viewer shows them
//!   natively: CoderHelm's recent runs become the map's `recent` (tasks and
//!   items, the shape `recent-work.json` becomes), and CoderHelm's security,
//!   infrastructure, log and alert findings become `health.issues`, marked as
//!   CoderHelm's; Floorplan's own checks stay, marked as Floorplan's, and one
//!   that names the same file and line as a CoderHelm finding is dropped
//!   ([`with_overlays`]).
//! - **The page's copy** ([`for_viewer`]): the sandbox's folder left out, and
//!   marked as an exported, read-only map so the viewer never calls a server.
//! - **Lookups** for the read API and Ask ([`overview`], [`part_view`],
//!   [`file_view`], [`screen_view`], [`search`], [`sub_map`]), in the spirit
//!   of Floorplan's `graph-query.js`.
//! - **Answer checks** ([`check_answer`]): a model job's answer against the
//!   schema `workflow.json` gives it, before it is written.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// The Floorplan release vendored with this build.
pub const FLOORPLAN_VERSION: &str = include_str!("../../../vendor/floorplan/VERSION");
/// Floorplan's job file: the model jobs, their instructions and schemas.
pub const WORKFLOW_JSON: &str =
    include_str!("../../../vendor/floorplan/skills/floorplan/workflow.json");
/// The version of what extracts the map: a hash of the sandbox's driver,
/// CoderHelm's detectors and the vendored Floorplan's release (`build.rs`).
/// Each map carries the version that made it (the job, the status and
/// `graph.coderhelm.extractor`); a map made by another version is built
/// again in full, once ([`queue_for_extractor`]), so a release that finds
/// more shows it without anyone pressing "Build the map again".
pub const EXTRACTOR_VERSION: &str = env!("CODERHELM_APP_MAP_EXTRACTOR");
/// The map format this adapter reads.
pub const GRAPH_SCHEMA: &str = "floorplan/graph@1";
/// Largest map served to the page (bytes of JSON).
pub const MAX_GRAPH_BYTES: usize = 25 * 1024 * 1024;

// ── Storage ────────────────────────────────────────────────────────────────

pub fn map_pk(team_id: &str) -> String {
    format!("AM#{team_id}")
}

fn s3_prefix(team_id: &str) -> String {
    // A team id may hold `#`: keep keys to plain characters.
    let safe: String = team_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("app-map/{safe}")
}

/// The built map (gzipped JSON).
pub fn graph_key(team_id: &str) -> String {
    format!("{}/graph.json.gz", s3_prefix(team_id))
}

/// The page's copy of the map ([`viewer_copy`]), written with the map: gzipped
/// JSON stored with `Content-Encoding: gzip`, so the browser reads it
/// straight from the bucket (a short-lived signed URL) and unzips it itself.
pub fn view_key(team_id: &str) -> String {
    format!("{}/view.json.gz", s3_prefix(team_id))
}

/// The map's small index ([`MapIndex`], gzipped JSON), written with the map:
/// what the read API needs to place Recent work and Health on it without
/// reading the map itself.
pub fn index_key(team_id: &str) -> String {
    format!("{}/index.json.gz", s3_prefix(team_id))
}

/// Floorplan's own files from the last run (a gzipped tar of `floorplan/`).
pub fn state_key(team_id: &str) -> String {
    format!("{}/state.tgz", s3_prefix(team_id))
}

/// The run in progress: heads, answers waiting for the next build, attempts.
pub fn job_key(team_id: &str) -> String {
    format!("{}/job.json", s3_prefix(team_id))
}

/// The journeys the model is to write after a build (the sandbox's
/// candidates and the entries to cover), until it has.
pub fn flows_todo_key(team_id: &str) -> String {
    format!("{}/flows-todo.json.gz", s3_prefix(team_id))
}

/// The worksheets a description step had no time left for, continued in
/// the next step.
/// What tracing journeys has cost a team today (`{day, usd}`).
pub fn flows_spend_key(team_id: &str) -> String {
    format!("{}/flows-spend.json", s3_prefix(team_id))
}

/// Who is tracing a team's journeys now (`{until}`), so a message
/// delivered twice runs once.
pub fn flows_lease_key(team_id: &str) -> String {
    format!("{}/flows-lease.json", s3_prefix(team_id))
}

/// When a team's journeys step may next run (`{seq, at}`): exactly one
/// message carries `seq` as its turn; any other is an older resume and is
/// dropped, and one that arrives before `at` waits for it.
pub fn flows_next_key(team_id: &str) -> String {
    format!("{}/flows-next.json", s3_prefix(team_id))
}

pub fn describe_key(team_id: &str) -> String {
    format!("{}/describe.json.gz", s3_prefix(team_id))
}

pub const STATUS_SK: &str = "STATUS";
/// A build queued or running this long is treated as stuck.
pub const STALE_SECS: i64 = 45 * 60;
/// A turn the worker took this long ago without a word since is taken to
/// have died with its worker (a Lambda runs at most 15 minutes), so the same
/// message, delivered again, may take it again.
pub const LEASE_SECS: i64 = 16 * 60;

/// The build's steps in order, by key; the page words each one. `done` is
/// the map written.
pub const STAGES: [&str; 4] = ["reading", "parts", "describing", "done"];

/// Where the team's map build is.
///
/// One build at a time: a build has an id (`build`), and each message of it
/// takes its numbered turn (`turn`), so a second press of "Build the map
/// again", or a message delivered twice, finds the build already running and
/// does nothing ([`queue_next`], [`take_turn`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    /// `queued`, `building`, `ready` or `failed`.
    pub state: String,
    /// When the status last changed (a running build's last sign of life).
    pub at: i64,
    /// What the build is doing now, in plain words.
    #[serde(default)]
    pub step: String,
    #[serde(default)]
    pub error: String,
    /// The build this status is about.
    #[serde(default)]
    pub build: String,
    /// Turns of the build taken so far.
    #[serde(default)]
    pub turn: u32,
    /// When the build was asked for.
    #[serde(default)]
    pub started: i64,
    /// One of [`STAGES`].
    #[serde(default)]
    pub stage: String,
    /// Repositories this build reads.
    #[serde(default)]
    pub repos: u32,
    /// When the map that is there now was finished (0: not known).
    #[serde(default)]
    pub built: i64,
    /// Repositories that map was made from.
    #[serde(default)]
    pub built_repos: u32,
    /// Repositories the map left out, whole or in part, because every
    /// repository together is larger than the snapshot holds (`owner/name`),
    /// so the page can say which.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cut: Vec<String>,
    /// The extractor ([`EXTRACTOR_VERSION`]) the map there now was made by
    /// (empty: a map from before maps carried it).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub extractor: String,
    /// The extractor a build was last asked for because the map was made by
    /// another one, so each release asks once: a build for it that fails is
    /// not asked for again on every visit.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub asked_extractor: String,
    /// Repositories the journeys step couldn't read from GitHub on its last
    /// pass (`owner/name`): the journeys through them wait and are traced
    /// once they can be read, and the list clears itself then
    /// ([`unread_line`] words it for the page).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unread: Vec<String>,
}

/// The status's plain line about repositories the journeys couldn't read
/// (empty when every one could).
pub fn unread_line(unread: &[String]) -> String {
    let names: Vec<&str> = unread
        .iter()
        .map(|r| r.rsplit('/').next().unwrap_or(r))
        .collect();
    match names.as_slice() {
        [] => String::new(),
        [one] => format!(
            "{one} couldn't be read from GitHub just now, so the journeys through it wait; they are traced once it can be read, and every other journey goes on."
        ),
        [rest @ .., last] => format!(
            "{} and {last} couldn't be read from GitHub just now, so the journeys through them wait; they are traced once they can be read, and every other journey goes on.",
            rest.join(", ")
        ),
    }
}

impl Status {
    pub fn is_stale(&self, now: i64) -> bool {
        matches!(self.state.as_str(), "queued" | "building") && now - self.at > STALE_SECS
    }

    /// A build is queued or running (and not stuck).
    pub fn is_running(&self, now: i64) -> bool {
        matches!(self.state.as_str(), "queued" | "building") && !self.is_stale(now)
    }
}

/// A new build, unless one is already queued or running: `None` then, and
/// the running build stands. What is known about the last map is kept.
pub fn queue_next(cur: Option<&Status>, now: i64, build: &str) -> Option<Status> {
    if cur.is_some_and(|s| s.is_running(now)) {
        return None;
    }
    let prev = cur.cloned().unwrap_or_default();
    Some(Status {
        state: "queued".into(),
        at: now,
        step: "Waiting to start".into(),
        error: String::new(),
        build: build.to_string(),
        turn: 0,
        started: now,
        stage: STAGES[0].into(),
        repos: if prev.repos > 0 {
            prev.repos
        } else {
            prev.built_repos
        },
        built: prev.built,
        built_repos: prev.built_repos,
        // The map that is there now still left these out.
        cut: prev.cut,
        extractor: prev.extractor,
        asked_extractor: prev.asked_extractor,
        unread: prev.unread,
    })
}

/// Whether the map there now was made by another extractor than `running`
/// and no build has been asked for `running` yet.
pub fn made_by_another_extractor(cur: Option<&Status>, has_map: bool, running: &str) -> bool {
    has_map && cur.is_none_or(|s| s.extractor != running && s.asked_extractor != running)
}

/// A new build because the map there now was made by another extractor
/// than `running`, marked as asked for it: `None` when the map is the
/// running extractor's, a build was already asked for it, or a build is
/// queued or running (which builds with the running extractor anyway).
pub fn queue_for_extractor(
    cur: Option<&Status>,
    now: i64,
    build: &str,
    running: &str,
) -> Option<Status> {
    if !made_by_another_extractor(cur, true, running) {
        return None;
    }
    let mut s = queue_next(cur, now, build)?;
    s.asked_extractor = running.to_string();
    Some(s)
}

/// Whether the running build was asked for because the map was made by
/// another extractor (the page says it is being rebuilt with the release's
/// improvements).
pub fn rebuilding_for_release(s: &Status, now: i64) -> bool {
    s.is_running(now) && s.asked_extractor == EXTRACTOR_VERSION && s.extractor != EXTRACTOR_VERSION
}

/// The worker taking turn `turn` of build `build` (one message of it):
/// `None` when the message is not the build's next one (a second message for
/// a build already running, a message delivered twice, a build replaced by a
/// newer one), and the worker does nothing with it. A message from before
/// builds had ids (`build` empty) carries on a build that has none.
pub fn take_turn(cur: Option<&Status>, build: &str, turn: u32, now: i64) -> Option<Status> {
    let s = cur?;
    if s.build != build {
        return None;
    }
    let expected = if turn == 0 { "queued" } else { "building" };
    let next = s.turn == turn && s.state == expected;
    // The same message again, after the worker that took it died.
    let again = s.turn == turn + 1 && s.state == "building" && now - s.at > LEASE_SECS;
    if !next && !again {
        return None;
    }
    let mut out = s.clone();
    out.state = "building".into();
    out.turn = turn + 1;
    out.at = now;
    out.error.clear();
    Some(out)
}

/// The running build's progress: `None` when `build` is no longer the one
/// running.
pub fn progress(
    cur: Option<&Status>,
    build: &str,
    stage: Option<&str>,
    step: &str,
    now: i64,
) -> Option<Status> {
    let s = cur.filter(|s| s.build == build && s.state == "building")?;
    let mut out = s.clone();
    if let Some(st) = stage {
        out.stage = st.into();
    }
    out.step = step.into();
    out.at = now;
    Some(out)
}

/// The build finished: `repos` is how many repositories the map covers (0
/// with a note when there was nothing to map).
pub fn finished(
    cur: Option<&Status>,
    build: &str,
    repos: u32,
    note: &str,
    now: i64,
) -> Option<Status> {
    let s =
        cur.filter(|s| s.build == build && matches!(s.state.as_str(), "queued" | "building"))?;
    let mut out = s.clone();
    out.state = "ready".into();
    out.stage = STAGES[3].into();
    out.step.clear();
    out.error = note.into();
    out.at = now;
    if repos > 0 {
        out.repos = repos;
        out.built = now;
        out.built_repos = repos;
    }
    Some(out)
}

/// The build stopped, and why in plain words.
pub fn failed(cur: Option<&Status>, build: &str, why: &str, now: i64) -> Option<Status> {
    let s =
        cur.filter(|s| s.build == build && matches!(s.state.as_str(), "queued" | "building"))?;
    let mut out = s.clone();
    out.state = "failed".into();
    out.error = why.into();
    out.at = now;
    Some(out)
}

/// How long a build usually takes for this many repositories, in seconds
/// (low and high): the sandbox reads and builds the map two or three times,
/// and the model works out the parts and describes each one.
pub fn estimate_secs(repos: u32) -> (i64, i64) {
    let r = repos.max(1) as i64;
    let low = (3 + r) * 60;
    let high = ((6 + 2 * r) * 60).min(STALE_SECS);
    (low.min(high), high)
}

type Dynamo = aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue as A;

/// The status and its version (0: an item from before versions, or none).
async fn read_status(
    dynamo: &Dynamo,
    table: &str,
    team_id: &str,
) -> Result<(Option<Status>, Option<i64>), String> {
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", A::S(map_pk(team_id)))
        .key("sk", A::S(STATUS_SK.into()))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(e)))?;
    let Some(item) = out.item() else {
        return Ok((None, None));
    };
    let st = item
        .get("json")
        .and_then(|v| v.as_s().ok())
        .and_then(|j| serde_json::from_str(j).ok());
    let v = item
        .get("v")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    Ok((st, Some(v)))
}

pub async fn status(dynamo: &Dynamo, table: &str, team_id: &str) -> Option<Status> {
    read_status(dynamo, table, team_id).await.ok()?.0
}

/// What [`update_status`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Updated {
    /// The new status was written.
    Written(Status),
    /// The change didn't apply to the status as it is, which is kept.
    Kept(Option<Status>),
}

/// Changes the status with `change` (given the status as it is, it returns
/// the new one, or `None` to leave it), written only if nobody wrote it in
/// between: each write carries the next version, conditioned on the one it
/// read, and a write that lost the race reads again and asks `change` again.
pub async fn update_status(
    dynamo: &Dynamo,
    table: &str,
    team_id: &str,
    change: impl Fn(Option<&Status>) -> Option<Status>,
) -> Result<Updated, String> {
    update_versioned(
        || read_status(dynamo, table, team_id),
        |next: Status, version: Option<i64>| put_status(dynamo, table, team_id, next, version),
        change,
    )
    .await
}

/// [`update_status`]'s loop over any versioned store: `read` gives the
/// status and its version, `put` writes the next one conditioned on that
/// version (`Ok(false)` when another write came first).
async fn update_versioned<R, RF, P, PF>(
    read: R,
    put: P,
    change: impl Fn(Option<&Status>) -> Option<Status>,
) -> Result<Updated, String>
where
    R: Fn() -> RF,
    RF: std::future::Future<Output = Result<(Option<Status>, Option<i64>), String>>,
    P: Fn(Status, Option<i64>) -> PF,
    PF: std::future::Future<Output = Result<bool, String>>,
{
    for _ in 0..5 {
        let (cur, version) = read().await?;
        let Some(next) = change(cur.as_ref()) else {
            return Ok(Updated::Kept(cur));
        };
        if put(next.clone(), version).await? {
            return Ok(Updated::Written(next));
        }
    }
    Err("the map's build status kept changing; try again".into())
}

/// Writes `next` if the status is still at `version` (`Ok(false)` if not).
async fn put_status(
    dynamo: &Dynamo,
    table: &str,
    team_id: &str,
    next: Status,
    version: Option<i64>,
) -> Result<bool, String> {
    let v = version.unwrap_or(0);
    let mut put = dynamo
        .put_item()
        .table_name(table)
        .item("pk", A::S(map_pk(team_id)))
        .item("sk", A::S(STATUS_SK.into()))
        .item(
            "json",
            A::S(serde_json::to_string(&next).unwrap_or_default()),
        )
        .item("state", A::S(next.state.clone()))
        .item("at", A::N(next.at.to_string()))
        .item("v", A::N((v + 1).to_string()));
    put = match version {
        None => put.condition_expression("attribute_not_exists(pk)"),
        Some(0) => put
            .condition_expression("attribute_not_exists(#v)")
            .expression_attribute_names("#v", "v"),
        Some(v) => put
            .condition_expression("#v = :v")
            .expression_attribute_names("#v", "v")
            .expression_attribute_values(":v", A::N(v.to_string())),
    };
    match put.send().await {
        Ok(_) => Ok(true),
        Err(e)
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
        {
            Ok(false)
        }
        Err(e) => Err(format!(
            "{}",
            aws_sdk_dynamodb::error::DisplayErrorContext(e)
        )),
    }
}

/// Asks for a build: `Ok(Ok(new))` when it is queued (send its message),
/// `Ok(Err(running))` when a build is already queued or running for the
/// team, which stands.
pub async fn queue_build(
    dynamo: &Dynamo,
    table: &str,
    team_id: &str,
) -> Result<Result<Status, Option<Status>>, String> {
    let now = chrono::Utc::now().timestamp();
    let build = format!("{:032x}", rand::random::<u128>());
    match update_status(dynamo, table, team_id, |cur| queue_next(cur, now, &build)).await? {
        Updated::Written(s) => Ok(Ok(s)),
        Updated::Kept(cur) => Ok(Err(cur)),
    }
}

/// Asks for a build because the team's map was made by another extractor
/// than this release's ([`queue_for_extractor`]): `Ok(Some(new))` when it is
/// queued (send its message), `Ok(None)` when no build is needed or one is
/// already asked for, queued or running. The same conditional write as
/// [`queue_build`], so two readers at once queue one build.
pub async fn queue_release_build(
    dynamo: &Dynamo,
    table: &str,
    team_id: &str,
) -> Result<Option<Status>, String> {
    let now = chrono::Utc::now().timestamp();
    let build = format!("{:032x}", rand::random::<u128>());
    match update_status(dynamo, table, team_id, |cur| {
        queue_for_extractor(cur, now, &build, EXTRACTOR_VERSION)
    })
    .await?
    {
        Updated::Written(s) => Ok(Some(s)),
        Updated::Kept(_) => Ok(None),
    }
}

pub fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let _ = gz.write_all(bytes);
    gz.finish().unwrap_or_default()
}

/// Gunzips at most `max` bytes; `None` past it or on bad data.
pub fn gunzip(bytes: &[u8], max: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .take(max as u64 + 1)
        .read_to_end(&mut out)
        .ok()?;
    (out.len() <= max).then_some(out)
}

/// The team's map as built, if any. Only ever the team's own key.
pub async fn load_graph(s3: &aws_sdk_s3::Client, bucket: &str, team_id: &str) -> Option<Value> {
    load_graph_dated(s3, bucket, team_id).await.map(|(g, _)| g)
}

/// The team's map and when it was written (unix seconds, 0 if not known).
pub async fn load_graph_dated(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    team_id: &str,
) -> Option<(Value, i64)> {
    let obj = s3
        .get_object()
        .bucket(bucket)
        .key(graph_key(team_id))
        .send()
        .await
        .ok()?;
    let at = obj.last_modified().map(|t| t.secs()).unwrap_or(0);
    let bytes = obj.body.collect().await.ok()?.into_bytes();
    let json = gunzip(&bytes, MAX_GRAPH_BYTES)?;
    let v: Value = serde_json::from_slice(&json).ok()?;
    (v["schema"] == GRAPH_SCHEMA).then_some((v, at))
}

/// When the team's map was written (unix seconds, 0 if not known), without
/// reading it; `None` when there is no map.
pub async fn graph_written_at(s3: &aws_sdk_s3::Client, bucket: &str, team_id: &str) -> Option<i64> {
    let head = s3
        .head_object()
        .bucket(bucket)
        .key(graph_key(team_id))
        .send()
        .await
        .ok()?;
    Some(head.last_modified().map(|t| t.secs()).unwrap_or(0))
}

/// How many repositories a built map covers.
pub fn repo_count(graph: &Value) -> u32 {
    graph["coderhelm"]["repos"]
        .as_object()
        .map(|m| m.len() as u32)
        .unwrap_or(0)
}

/// Whether the team has a built map, without reading it (Chat offers the
/// map lookups only then, and reads the map at its first lookup).
pub async fn has_graph(s3: &aws_sdk_s3::Client, bucket: &str, team_id: &str) -> bool {
    s3.head_object()
        .bucket(bucket)
        .key(graph_key(team_id))
        .send()
        .await
        .is_ok()
}

// ── Repositories in the map ────────────────────────────────────────────────

/// The folder each repository gets in the map: its name, or `owner-name`
/// when two repositories share a name. Stable for the same set of repos.
pub fn folders(repos: &[String]) -> BTreeMap<String, String> {
    let mut by_name: HashMap<String, usize> = HashMap::new();
    for r in repos {
        let name = r.rsplit('/').next().unwrap_or(r).to_ascii_lowercase();
        *by_name.entry(name).or_default() += 1;
    }
    repos
        .iter()
        .map(|r| {
            let name = r.rsplit('/').next().unwrap_or(r);
            let clash = by_name
                .get(&name.to_ascii_lowercase())
                .copied()
                .unwrap_or(0)
                > 1;
            let folder = if clash {
                r.replace('/', "-")
            } else {
                name.to_string()
            };
            let folder: String = folder
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                        c
                    } else {
                        '-'
                    }
                })
                .collect();
            (r.clone(), folder)
        })
        .collect()
}

/// The map's `coderhelm.repos`: folder to `owner/name`, kept on the map so
/// overlays can place a run's or a finding's paths.
fn repo_of_folder(graph: &Value) -> HashMap<String, String> {
    graph["coderhelm"]["repos"]
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(f, r)| Some((f.clone(), r.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Records which folder is which repository on a freshly built map.
pub fn stamp_repos(graph: &mut Value, folders: &BTreeMap<String, String>) {
    let m: Map<String, Value> = folders
        .iter()
        .map(|(repo, folder)| (folder.clone(), json!(repo)))
        .collect();
    graph["coderhelm"] = json!({
        "repos": m,
        "floorplan": FLOORPLAN_VERSION.trim(),
        "extractor": EXTRACTOR_VERSION,
    });
}

/// Records on the map the repositories its snapshot left out, whole or in
/// part (`owner/name`), so the page can say so.
pub fn stamp_cut(graph: &mut Value, cut: &[String]) {
    if !cut.is_empty() {
        graph["coderhelm"]["cut"] = json!(cut);
    }
}

fn arr<'a>(v: &'a Value, k: &str) -> impl Iterator<Item = &'a Value> {
    v[k].as_array().into_iter().flatten()
}

/// The parent of each part, read once.
fn parents(graph: &Value) -> HashMap<&str, &str> {
    arr(graph, "nodes")
        .filter_map(|n| Some((n["id"].as_str()?, n["parent"].as_str()?)))
        .collect()
}

fn top_of(parents: &HashMap<&str, &str>, id: &str) -> String {
    let mut cur = id;
    for _ in 0..20 {
        match parents.get(cur) {
            Some(p) => cur = p,
            None => break,
        }
    }
    cur.to_string()
}

fn top_node(graph: &Value, id: &str) -> String {
    top_of(&parents(graph), id)
}

/// A repository path on the map (`folder/path`), when the map has that file.
/// `path` may already start with `owner/name/` (a run across several
/// repositories keeps paths that way).
pub fn map_path(graph: &Value, repo: &str, path: &str) -> Option<String> {
    MapIndex::of(graph).map_path(repo, path)
}

/// One of Floorplan's own checks, as the index keeps it: enough to tell
/// whether a CoderHelm finding stands at the same place, and to count it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexIssue {
    pub id: String,
    pub severity: String,
    /// (file on the map, line or 0).
    #[serde(default)]
    pub locations: Vec<(String, u64)>,
}

/// What the read API needs to place CoderHelm's Recent work and Health on a
/// map, without the map: written at build time with the map
/// ([`index_key`]), a few hundred bytes per file on the map instead of the
/// whole map, and every placement a lookup.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapIndex {
    /// Folder on the map to `owner/name`.
    pub repos: BTreeMap<String, String>,
    /// Every file on the map (`folder/path`).
    pub files: BTreeSet<String>,
    /// `owner/name` (lower case) to the top-level part holding most of its
    /// files: where a finding with no file on the map sits.
    pub repo_parts: BTreeMap<String, String>,
    /// Floorplan's own checks, in the map's order.
    pub issues: Vec<IndexIssue>,
    /// Whether the map has a Health section at all.
    pub has_health: bool,
    /// What the map covers, counted at build time (the Knowledge page's
    /// card). Empty on an index written before it was counted.
    #[serde(default)]
    pub coverage: Coverage,
}

/// What a map covers, for the Knowledge page: screens per app, outside
/// services, cloud links and repositories.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Coverage {
    /// Whether this was counted (an older index has no counts).
    #[serde(default)]
    pub counted: bool,
    #[serde(default)]
    pub screens: BTreeMap<String, u32>,
    #[serde(default)]
    pub outside_services: u32,
    /// Links between code and the cloud: what deploys or starts it.
    #[serde(default)]
    pub cloud_links: u32,
    #[serde(default)]
    pub repos: u32,
    #[serde(default)]
    pub journeys: u32,
    /// Journeys still being traced (entries waiting for a later pass).
    #[serde(default)]
    pub journeys_pending: u32,
}

impl Coverage {
    pub fn of(graph: &Value) -> Self {
        let kind = |id: &str| {
            id.strip_prefix("n:")
                .and_then(|n| arr(graph, "nodes").find(|x| x["id"] == n))
                .and_then(|x| x["kind"].as_str())
                .unwrap_or("")
                .to_string()
        };
        let screens = screen_counts(graph)
            .as_object()
            .map(|o| {
                o.iter()
                    .map(|(k, v)| (k.clone(), v.as_u64().unwrap_or(0) as u32))
                    .collect()
            })
            .unwrap_or_default();
        Self {
            counted: true,
            screens,
            outside_services: arr(graph, "nodes")
                .filter(|n| n["kind"] == "external")
                .count() as u32,
            cloud_links: arr(graph, "links")
                .filter(|l| {
                    [&l["a"], &l["b"]]
                        .iter()
                        .any(|e| e.as_str().is_some_and(|id| kind(id) == "infra"))
                })
                .count() as u32,
            repos: arr(&graph["meta"], "repos")
                .count()
                .max(repo_of_folder(graph).len()) as u32,
            journeys: arr(graph, "flows").count() as u32,
            journeys_pending: graph["coderhelm"]["journeys_pending"].as_u64().unwrap_or(0) as u32,
        }
    }
}

impl MapIndex {
    pub fn of(graph: &Value) -> Self {
        let repos: BTreeMap<String, String> = repo_of_folder(graph).into_iter().collect();
        let files = arr(graph, "files")
            .filter_map(|f| f["p"].as_str().map(str::to_string))
            .collect();
        let parents = parents(graph);
        let mut count: BTreeMap<&str, BTreeMap<String, usize>> = BTreeMap::new();
        for f in arr(graph, "files") {
            if let (Some(r), Some(n)) = (f["r"].as_str(), f["n"].as_str()) {
                *count
                    .entry(r)
                    .or_default()
                    .entry(top_of(&parents, n))
                    .or_default() += 1;
            }
        }
        let repo_parts = repos
            .iter()
            .filter_map(|(folder, repo)| {
                let part = count
                    .get(folder.as_str())?
                    .iter()
                    .max_by_key(|(_, c)| **c)
                    .map(|(n, _)| n.clone())?;
                Some((repo.to_ascii_lowercase(), part))
            })
            .collect();
        let issues = arr(&graph["health"], "issues")
            .filter(|i| i["source"].is_null() || i["source"] == "floorplan")
            .map(|i| IndexIssue {
                id: i["id"].as_str().unwrap_or("").to_string(),
                severity: i["severity"].as_str().unwrap_or("low").to_string(),
                locations: locations(i).into_iter().collect(),
            })
            .collect();
        Self {
            repos,
            files,
            repo_parts,
            issues,
            has_health: graph.get("health").is_some(),
            coverage: Coverage::of(graph),
        }
    }

    /// A repository path on the map (`folder/path`), when the map has that
    /// file. `path` may already start with `owner/name/`.
    pub fn map_path(&self, repo: &str, path: &str) -> Option<String> {
        let path = path.trim_start_matches("./");
        for (folder, r) in &self.repos {
            if let Some(rest) = path.strip_prefix(&format!("{r}/")) {
                let p = format!("{folder}/{rest}");
                return self.files.contains(&p).then_some(p);
            }
        }
        let folder = self
            .repos
            .iter()
            .find(|(_, r)| r.eq_ignore_ascii_case(repo))
            .map(|(f, _)| f)?;
        let p = format!("{folder}/{path}");
        self.files.contains(&p).then_some(p)
    }

    /// The top-level part holding most of a repository's files.
    pub fn repo_node(&self, repo: &str) -> Option<String> {
        self.repo_parts.get(&repo.to_ascii_lowercase()).cloned()
    }
}

// ── Overlays ───────────────────────────────────────────────────────────────

/// A CoderHelm run, for the map's Recent work.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItem {
    pub run_id: String,
    pub repo: String,
    pub ticket: String,
    pub title: String,
    /// Why, from the run's plan.
    pub why: String,
    pub pr_url: String,
    pub at: String,
    pub status: String,
    pub files: Vec<String>,
}

/// A CoderHelm finding, for the map's Health.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthItem {
    /// `security`, `infrastructure`, `logs` or `alert`.
    pub kind: String,
    pub id: String,
    pub title: String,
    /// As the finding says it (P0..P3, critical, important, high, ...).
    pub severity: String,
    pub repo: String,
    /// (path in the repository, line or 0).
    pub files: Vec<(String, u32)>,
    /// The dashboard page that shows it.
    pub link: String,
}

/// Floorplan's three levels from a finding's own word.
pub fn severity_level(s: &str) -> &'static str {
    match s.to_ascii_lowercase().as_str() {
        "p0" | "p1" | "critical" | "high" | "error" => "high",
        "p2" | "important" | "medium" | "moderate" | "warning" => "medium",
        _ => "low",
    }
}

/// The map's `recent` from CoderHelm's runs: one task per run, one item per
/// file it changed that is on the map. A run with no file on the map is left
/// out.
pub fn recent_from_runs(idx: &MapIndex, runs: &[WorkItem]) -> Option<Value> {
    let mut tasks = Vec::new();
    let mut items = Vec::new();
    for w in runs {
        let mut ids: Vec<String> = w
            .files
            .iter()
            .filter_map(|f| idx.map_path(&w.repo, f))
            .map(|p| format!("f:{p}"))
            .collect();
        ids.dedup();
        if ids.is_empty() {
            continue;
        }
        let title = if w.ticket.is_empty() {
            crate::truncate_str(&w.title, 80).to_string()
        } else {
            format!("{}: {}", w.ticket, crate::truncate_str(&w.title, 70))
        };
        let mut summary = w.why.clone();
        if !w.pr_url.is_empty() {
            if !summary.is_empty() {
                summary.push(' ');
            }
            summary.push_str(&format!("Pull request: {}", w.pr_url));
        }
        // Nobody asked the map for this: Floorplan shows `request` as "You
        // asked", so it stays empty, as its workflow says.
        tasks
            .push(json!({ "title": title, "request": "", "summary": summary, "status": w.status }));
        let task = tasks.len() - 1;
        let what = if w.ticket.is_empty() {
            "this work".to_string()
        } else {
            w.ticket.clone()
        };
        for id in ids {
            items.push(json!({ "id": id, "task": task, "change": "changed", "note": format!("Changed by Coderhelm's run for {what}") }));
        }
    }
    (!items.is_empty()).then(|| {
        json!({ "updatedAt": chrono::Utc::now().to_rfc3339(), "tasks": tasks, "items": items, "source": "coderhelm" })
    })
}

/// CoderHelm's findings as Floorplan health issues. A finding in a
/// repository the map doesn't have is left out.
pub fn issues_from_findings(idx: &MapIndex, found: &[HealthItem]) -> Vec<Value> {
    let label = |k: &str| match k {
        "security" => "security",
        "infrastructure" => "infrastructure",
        "logs" => "logs",
        _ => "alert",
    };
    found
        .iter()
        .filter_map(|h| {
            let placed: Vec<(String, u32)> = h
                .files
                .iter()
                .filter_map(|(f, line)| Some((idx.map_path(&h.repo, f)?, *line)))
                .collect();
            let mut items: Vec<String> = placed.iter().map(|(p, _)| format!("f:{p}")).collect();
            items.dedup();
            if items.is_empty() {
                items.push(format!("n:{}", idx.repo_node(&h.repo)?));
            }
            Some(json!({
                "id": format!("coderhelm:{}:{}", h.kind, h.id),
                "kind": format!("coderhelm-{}", h.kind),
                "severity": severity_level(&h.severity),
                "title": format!("Coderhelm {}: {}", label(&h.kind), h.title),
                "detail": format!("Found by Coderhelm. See it in Coderhelm: {}", h.link),
                "items": items,
                "notes": {},
                "locations": placed.iter().filter(|(_, l)| *l > 0).map(|(p, l)| json!({"file": p, "line": l})).collect::<Vec<_>>(),
                "fix": "",
                "source": "coderhelm",
                "link": h.link,
            }))
        })
        .collect()
}

fn locations(issue: &Value) -> BTreeSet<(String, u64)> {
    arr(issue, "locations")
        .filter_map(|l| {
            Some((
                l["file"].as_str()?.to_string(),
                l["line"].as_u64().unwrap_or(0),
            ))
        })
        .collect()
}

/// Floorplan's checks labelled as Floorplan's, with Health's counts: the
/// map as the page's copy holds it before any of CoderHelm's overlays.
fn labelled(mut graph: Value) -> Value {
    let mut issues: Vec<Value> = graph["health"]["issues"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for i in issues.iter_mut() {
        if i["source"].is_null() {
            i["source"] = json!("floorplan");
            if let Some(t) = i["title"].as_str() {
                i["title"] = json!(format!("Floorplan check: {t}"));
            }
        }
    }
    if graph.get("health").is_some() || !issues.is_empty() {
        graph["health"]["summary"] = json!(summary_of(
            issues
                .iter()
                .map(|i| i["severity"].as_str().unwrap_or("low"))
        ));
        graph["health"]["issues"] = json!(issues);
    }
    graph
}

fn summary_of<'a>(severities: impl Iterator<Item = &'a str>) -> Value {
    let (mut high, mut medium, mut low) = (0, 0, 0);
    for s in severities {
        match s {
            "high" => high += 1,
            "medium" => medium += 1,
            _ => low += 1,
        }
    }
    json!({"high": high, "medium": medium, "low": low})
}

/// CoderHelm's overlays for a map, from its index alone: a small payload the
/// page lays over its copy of the map ([`apply_overlays`]).
///
/// - `recent` (with `runs`): the map's Recent work, `null` for none;
/// - `health` (with `found`): CoderHelm's findings to `add`, the ids of
///   Floorplan's checks to `drop` (one at the same file and line as a
///   CoderHelm finding is not shown twice), and Health's `summary`.
pub fn overlays(idx: &MapIndex, runs: Option<&[WorkItem]>, found: Option<&[HealthItem]>) -> Value {
    let mut out = json!({});
    if let Some(runs) = runs {
        out["recent"] = recent_from_runs(idx, runs).unwrap_or(Value::Null);
    }
    if let Some(found) = found {
        let ours = issues_from_findings(idx, found);
        let taken: BTreeSet<(String, u64)> = ours.iter().flat_map(locations).collect();
        let (dropped, kept): (Vec<&IndexIssue>, Vec<&IndexIssue>) = idx
            .issues
            .iter()
            .partition(|i| i.locations.iter().any(|l| taken.contains(l)));
        if idx.has_health || !idx.issues.is_empty() || !ours.is_empty() {
            let summary = summary_of(
                kept.iter()
                    .map(|i| i.severity.as_str())
                    .chain(ours.iter().map(|i| i["severity"].as_str().unwrap_or("low"))),
            );
            out["health"] = json!({
                "add": ours,
                "drop": dropped.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
                "summary": summary,
            });
        }
    }
    out
}

/// Lays [`overlays`] over the page's copy of the map, as the page does.
pub fn apply_overlays(mut view: Value, o: &Value) -> Value {
    if let Some(r) = o.get("recent") {
        if r.is_null() {
            if let Some(m) = view.as_object_mut() {
                m.remove("recent");
            }
        } else {
            view["recent"] = r.clone();
        }
    }
    if let Some(h) = o.get("health").filter(|h| h.is_object()) {
        let drop: BTreeSet<&str> = arr(h, "drop").filter_map(Value::as_str).collect();
        let mut issues: Vec<Value> = arr(&view["health"], "issues")
            .filter(|i| {
                i["source"] != "floorplan" || !i["id"].as_str().is_some_and(|id| drop.contains(id))
            })
            .cloned()
            .collect();
        issues.extend(arr(h, "add").cloned());
        view["health"]["issues"] = json!(issues);
        view["health"]["summary"] = h["summary"].clone();
    }
    view
}

/// The map with CoderHelm's overlays in Floorplan's own fields. With
/// `runs`, they replace the map's recent work; with `found`, CoderHelm's
/// findings join Floorplan's checks (each marked with its `source`; a
/// Floorplan issue at the same file and line as a CoderHelm one is dropped).
/// Floorplan's own checks are always labelled as Floorplan's.
pub fn with_overlays(
    graph: Value,
    runs: Option<&[WorkItem]>,
    found: Option<&[HealthItem]>,
) -> Value {
    let idx = MapIndex::of(&graph);
    apply_overlays(labelled(graph), &overlays(&idx, runs, found))
}

/// The copy the page gets: read-only (Floorplan's exported mode, so the
/// viewer never calls a server), naming no agent, without the sandbox's
/// folder.
pub fn for_viewer(mut graph: Value, now: &str) -> Value {
    graph["meta"]["root"] = Value::Null;
    graph["meta"]["agent"] = json!("none");
    graph["meta"]["exported"] = json!({ "at": now, "by": "Coderhelm" });
    graph
}

/// The page's copy of a freshly built map ([`view_key`]): Floorplan's checks
/// labelled, read-only, without the sandbox's folder. CoderHelm's overlays
/// are laid over it on the page.
pub fn viewer_copy(graph: &Value, now: &str) -> Value {
    for_viewer(labelled(graph.clone()), now)
}

/// The page's copy and the index of a map, gzipped, as they are stored.
pub fn derived(graph: &Value, now: &str) -> (Vec<u8>, Vec<u8>) {
    let view = gzip(viewer_copy(graph, now).to_string().as_bytes());
    let index = gzip(&serde_json::to_vec(&MapIndex::of(graph)).unwrap_or_default());
    (view, index)
}

/// Writes the page's copy and the index of `graph` next to it (the worker
/// after each build; the read API once for a map built before they
/// existed). The copy is stored as JSON with `Content-Encoding: gzip`, so the
/// browser unzips it itself.
pub async fn store_derived(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    team_id: &str,
    graph: &Value,
) -> Result<MapIndex, String> {
    let (view, index) = derived(graph, &chrono::Utc::now().to_rfc3339());
    s3.put_object()
        .bucket(bucket)
        .key(view_key(team_id))
        .content_type("application/json")
        .content_encoding("gzip")
        .cache_control("private, no-cache")
        .body(aws_sdk_s3::primitives::ByteStream::from(view))
        .send()
        .await
        .map_err(|e| format!("app map: could not keep the page's copy: {e}"))?;
    // The index last: a reader that finds it current finds the copy current.
    s3.put_object()
        .bucket(bucket)
        .key(index_key(team_id))
        .content_type("application/gzip")
        .body(aws_sdk_s3::primitives::ByteStream::from(index))
        .send()
        .await
        .map_err(|e| format!("app map: could not keep the map's index: {e}"))?;
    Ok(MapIndex::of(graph))
}

/// The team's map index and when it was written (unix seconds), if any.
pub async fn load_index(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    team_id: &str,
) -> Option<(MapIndex, i64)> {
    let obj = s3
        .get_object()
        .bucket(bucket)
        .key(index_key(team_id))
        .send()
        .await
        .ok()?;
    let at = obj.last_modified().map(|t| t.secs()).unwrap_or(0);
    let bytes = obj.body.collect().await.ok()?.into_bytes();
    let json = gunzip(&bytes, MAX_GRAPH_BYTES)?;
    Some((serde_json::from_slice(&json).ok()?, at))
}

// ── Lookups (read API and Ask) ─────────────────────────────────────────────

fn node(graph: &Value, id: &str) -> Option<Value> {
    arr(graph, "nodes").find(|n| n["id"] == id).cloned()
}

/// Copies the fields `keys` that `from` has (and are not null) into `to`.
fn keep(to: &mut Value, from: &Value, keys: &[&str]) {
    for k in keys {
        if !from[*k].is_null() {
            to[*k] = from[*k].clone();
        }
    }
}

/// Where something on the map was found: up to `n` `file:line` places,
/// with how each was found (`code`, or `inferred` from a name, a domain or
/// an env key) — CoderHelm's detectors put these on what they add.
fn evidence_json(v: &Value, n: usize) -> Value {
    json!(arr(v, "evidence")
        .take(n)
        .map(|e| json!({"at": format!("{}:{}", e["file"].as_str().unwrap_or(""), e["line"].as_u64().unwrap_or(1)), "how": e["how"]}))
        .collect::<Vec<_>>())
}

/// A part's own record: outside services say which vendor and how they were
/// found, infra says which resource.
fn node_json(x: &Value) -> Value {
    let mut out =
        json!({"id": x["id"], "label": x["label"], "kind": x["kind"], "summary": x["summary"]});
    keep(
        &mut out,
        x,
        &[
            "level", "vendor", "how", "inferred", "category", "env", "services", "resource",
            "domains",
        ],
    );
    if x.get("evidence").is_some() {
        out["evidence"] = evidence_json(x, 5);
    }
    out
}

/// A short record for a map id (`n:`, `f:`, `r:`, `screen:`).
fn describe_ref(graph: &Value, id: &str) -> Value {
    if let Some(n) = id.strip_prefix("n:") {
        if let Some(x) = node(graph, n) {
            let mut d = node_json(&x);
            d["id"] = json!(id);
            if let Some(o) = d.as_object_mut() {
                o.remove("evidence");
            }
            return d;
        }
    } else if id.starts_with("screen:") {
        if let Some(s) = arr(graph, "screens").find(|s| s["id"] == id) {
            return screen_json(s);
        }
    } else if let Some(p) = id.strip_prefix("f:") {
        if let Some(f) = arr(graph, "files").find(|f| f["p"] == p) {
            return json!({"id": id, "path": p, "name": f["name"], "description": f["d"], "part": f["n"]});
        }
    } else if let Some(i) = id.strip_prefix("r:").and_then(|i| i.parse::<usize>().ok()) {
        if let Some(r) = graph["routes"].get(i) {
            return json!({"id": id, "method": r["m"], "path": r["p"], "name": r["name"], "description": r["d"], "file": r["f"]});
        }
    }
    json!({ "id": id })
}

/// A screen in short: where it is, which app and navigator hold it, and
/// whether it needs a signed-in user. A screen a dynamic route dispatches
/// to by type also says the type, its renderer and where it is declared,
/// the route file that dispatches to it, and whether its URL pattern was
/// read from the code (`url_from`) or is the route's own.
fn screen_json(s: &Value) -> Value {
    let mut out = json!({"id": s["id"], "url": s["url"], "name": s["name"], "app": s["app"], "file": s["file"]});
    let nav = &s["nav"];
    if nav.is_object() {
        keep(&mut out, nav, &["navigator", "group", "gate"]);
        out["app"] = nav["app"].clone();
        if let Some(route) = nav["dispatcher"].as_str() {
            out["dispatch"] = json!({
                "type": nav["type"],
                "renderer": nav["renderer"],
                "renderer_at": first_at(nav, s["file"].as_str().unwrap_or("")),
                "route_file": route,
                "url_from": nav["urlFrom"],
            });
            keep(&mut out["dispatch"], nav, &["types"]);
        }
    }
    out
}

/// The screens in the order the overview lists them: an app's own screens
/// (routes, screens a route dispatches to, app screens) before the content
/// editor's document kinds and tools, which a large studio would otherwise
/// push off the list.
fn screens_listed(graph: &Value) -> Vec<&Value> {
    let mut v: Vec<&Value> = arr(graph, "screens").collect();
    v.sort_by_key(|s| matches!(s["kind"].as_str(), Some("page-kind" | "tool")));
    v
}

/// One link as the lookups return it. Links CoderHelm's detectors added
/// carry where they were found; `nav` links name their screens, `trigger`
/// links the function they start, `webhook` links the endpoint called back.
fn link_json(graph: &Value, l: &Value) -> Value {
    let mut out = json!({
        "from": describe_ref(graph, l["a"].as_str().unwrap_or("")),
        "to": describe_ref(graph, l["b"].as_str().unwrap_or("")),
        "kind": l["k"],
        "labels": l["labels"],
        "inferred": l["inferred"],
    });
    if l.get("evidence").is_some() {
        out["evidence"] = evidence_json(l, 3);
    }
    if let Some(to) = l["to"].as_str() {
        out["screen"] = describe_ref(graph, to);
    }
    for (k, key) in [("unit", "runs"), ("route", "endpoint")] {
        if let Some(r) = l[k].as_str() {
            out[key] = describe_ref(graph, r);
        }
    }
    keep(&mut out, l, &["tables", "via"]);
    out
}

/// The whole map in short: parts, screens, Health counts, repositories.
pub fn overview(graph: &Value) -> Value {
    json!({
        "project": graph["project"],
        "parts": arr(graph, "nodes").map(|n| json!({"id": n["id"], "parent": n["parent"], "label": n["label"], "kind": n["kind"], "summary": n["summary"], "files": n["stats"]["files"]})).collect::<Vec<_>>(),
        "screens": screens_listed(graph).into_iter().take(80).map(screen_json).collect::<Vec<_>>(),
        "screen_count": screen_counts(graph),
        "journeys": arr(graph, "flows").take(60).map(|f| json!({"id": f["id"], "title": f["title"], "steps": f["steps"].as_array().map(|a| a.len()).unwrap_or(0)})).collect::<Vec<_>>(),
        "journeys_still_being_traced": graph["coderhelm"]["journeys_pending"].as_u64().unwrap_or(0),
        "health": graph["health"]["summary"],
        "repos": graph["meta"]["repos"],
        "counts": graph["meta"]["counts"],
    })
}

/// How many screens each app has (the overview lists the first 80).
fn screen_counts(graph: &Value) -> Value {
    let mut by: BTreeMap<String, usize> = BTreeMap::new();
    for s in arr(graph, "screens") {
        let app = s["nav"]["app"]
            .as_str()
            .or(s["app"].as_str())
            .unwrap_or("app");
        *by.entry(app.to_string()).or_default() += 1;
    }
    json!(by)
}

/// One part: its sub-parts, files, endpoints, and the lines in and out.
pub fn part_view(graph: &Value, id: &str) -> Option<Value> {
    let n = node(graph, id)?;
    let children: Vec<Value> = arr(graph, "nodes")
        .filter(|c| c["parent"] == id)
        .map(|c| {
            let mut x = node_json(c);
            if let Some(o) = x.as_object_mut() {
                o.remove("evidence");
            }
            x
        })
        .collect();
    let mut inside: BTreeSet<String> = BTreeSet::from([id.to_string()]);
    let mut grew = true;
    while grew {
        grew = false;
        for c in arr(graph, "nodes") {
            if let (Some(cid), Some(p)) = (c["id"].as_str(), c["parent"].as_str()) {
                if inside.contains(p) && inside.insert(cid.to_string()) {
                    grew = true;
                }
            }
        }
    }
    let files: Vec<&Value> = arr(graph, "files")
        .filter(|f| f["n"].as_str().is_some_and(|n| inside.contains(n)))
        .collect();
    let paths: BTreeSet<&str> = files.iter().filter_map(|f| f["p"].as_str()).collect();
    let routes: Vec<Value> = arr(graph, "routes")
        .enumerate()
        .filter(|(_, r)| r["f"].as_str().is_some_and(|f| paths.contains(f)))
        .map(|(i, r)| json!({"id": format!("r:{i}"), "method": r["m"], "path": r["p"], "name": r["name"], "description": r["d"], "file": r["f"], "line": r["l"]}))
        .collect();
    let mine = |x: &str| {
        x.strip_prefix("n:").is_some_and(|n| inside.contains(n))
            || x.strip_prefix("f:").is_some_and(|p| paths.contains(p))
    };
    let links: Vec<Value> = arr(graph, "links")
        .filter(|l| l["a"].as_str().is_some_and(mine) != l["b"].as_str().is_some_and(mine))
        .take(200)
        .map(|l| link_json(graph, l))
        .collect();
    let mut part = node_json(&n);
    part["details"] = n["details"].clone();
    part["stats"] = n["stats"].clone();
    Some(json!({
        "part": part,
        "children": children,
        "files": files.iter().take(400).map(|f| json!({"path": f["p"], "name": f["name"], "description": f["d"]})).collect::<Vec<_>>(),
        "endpoints": routes,
        "links": links,
    }))
}

/// One file: what it is, its endpoints, and what it connects to.
pub fn file_view(graph: &Value, path: &str) -> Option<Value> {
    let f = arr(graph, "files").find(|f| f["p"] == path)?;
    let id = format!("f:{path}");
    let links: Vec<Value> = arr(graph, "links")
        .filter(|l| l["a"] == id.as_str() || l["b"] == id.as_str())
        .take(200)
        .map(|l| link_json(graph, l))
        .collect();
    let routes: Vec<Value> = arr(graph, "routes")
        .enumerate()
        .filter(|(_, r)| r["f"] == path)
        .map(|(i, r)| json!({"id": format!("r:{i}"), "method": r["m"], "path": r["p"], "name": r["name"], "description": r["d"], "line": r["l"]}))
        .collect();
    // the screens this file is, and the screens it registers (a navigator file)
    let screens: Vec<Value> = arr(graph, "screens")
        .filter(|s| {
            s["file"] == path
                || arr(&s["nav"], "evidence")
                    .next()
                    .is_some_and(|e| e["file"] == path)
        })
        .take(100)
        .map(screen_json)
        .collect();
    Some(json!({
        "file": {"path": f["p"], "name": f["name"], "description": f["d"], "part": f["n"], "repo": f["r"], "language": f["l"], "lines": f["loc"]},
        "endpoints": routes,
        "screens": screens,
        "links": links,
    }))
}

/// Everything behind one screen, as Floorplan traced it, each step described.
pub fn screen_view(graph: &Value, id_or_url: &str) -> Option<Value> {
    let q = id_or_url.trim();
    let s = arr(graph, "screens")
        .find(|s| s["id"] == q || s["url"] == q)
        .or_else(|| {
            // its name, as the overview and search list it ("Member checkout")
            arr(graph, "screens").find(|s| {
                s["name"]
                    .as_str()
                    .is_some_and(|n| !q.is_empty() && n.eq_ignore_ascii_case(q))
            })
        })?;
    let steps: Vec<Value> = arr(s, "behind")
        .map(|b| {
            let mut d = describe_ref(graph, b["id"].as_str().unwrap_or(""));
            d["note"] = b["note"].clone();
            d["step"] = b["kind"].clone();
            d
        })
        .collect();
    let mut screen = screen_json(s);
    screen["description"] = s["description"].clone();
    if s["nav"].is_object() {
        screen["evidence"] = evidence_json(&s["nav"], 3);
    }
    // where it leads, and what leads to it
    let id = s["id"].as_str().unwrap_or("");
    let file = format!("f:{}", s["file"].as_str().unwrap_or(""));
    let nav: Vec<Value> = arr(graph, "links")
        .filter(|l| l["k"] == "nav" && (l["to"] == id || l["a"] == file.as_str()))
        .take(40)
        .map(|l| link_json(graph, l))
        .collect();
    Some(json!({
        "screen": screen,
        "behind": steps,
        "navigation": nav,
    }))
}

/// One journey (flow) by its id or the words of its title: each step in
/// order, what happens there, the map item it stands on and the file:line
/// it cites.
pub fn flow_view(graph: &Value, id_or_words: &str) -> Option<Value> {
    let q = id_or_words.trim();
    if q.is_empty() {
        return None;
    }
    let words: Vec<String> = q
        .split_whitespace()
        .map(|w| w.to_ascii_lowercase())
        .collect();
    let f = arr(graph, "flows").find(|f| f["id"] == q).or_else(|| {
        arr(graph, "flows").find(|f| {
            let hay = format!(
                "{} {} {}",
                f["id"].as_str().unwrap_or(""),
                f["title"].as_str().unwrap_or(""),
                f["description"].as_str().unwrap_or("")
            )
            .to_ascii_lowercase()
            .replace(['-', '_'], " ");
            words.iter().all(|w| hay.contains(w.as_str()))
        })
    })?;
    let linked = f["coderhelm"]["linked"].as_array();
    let steps: Vec<Value> = arr(f, "steps")
        .enumerate()
        .map(|(i, s)| {
            let mut d = describe_ref(graph, s["ref"].as_str().unwrap_or(""));
            d["step"] = json!(i + 1);
            d["note"] = s["note"].clone();
            // the screen the step stands on, when its file holds several
            if let Some(sc) = s["screen"].as_str().filter(|x| x.starts_with("screen:")) {
                d["screen"] = describe_ref(graph, sc);
            }
            if let Some(ev) = s["evidence"].as_array() {
                d["cites"] = json!(ev
                    .iter()
                    .filter_map(|e| Some(format!(
                        "{}:{}",
                        e["file"].as_str()?,
                        e["line"].as_u64()?
                    )))
                    .collect::<Vec<_>>());
            }
            if linked.and_then(|l| l.get(i)) == Some(&json!(false)) {
                d["inferred"] = json!(true);
            }
            d
        })
        .collect();
    Some(json!({
        "journey": {"id": f["id"], "title": f["title"], "description": f["description"]},
        "steps": steps,
    }))
}

/// Parts, files, endpoints and screens whose names, descriptions or paths
/// hold every word of the query.
pub fn search(graph: &Value, q: &str) -> Value {
    let words: Vec<String> = q
        .split_whitespace()
        .map(|w| w.to_ascii_lowercase())
        .take(8)
        .collect();
    if words.is_empty() {
        return json!({"parts": [], "files": [], "endpoints": [], "screens": [], "journeys": []});
    }
    let hit = |texts: &[&Value]| {
        let hay: String = texts
            .iter()
            .filter_map(|t| t.as_str())
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        words.iter().all(|w| hay.contains(w))
    };
    json!({
        "parts": arr(graph, "nodes").filter(|n| hit(&[&n["label"], &n["summary"], &n["id"], &n["vendor"], &json!(arr(n, "env").chain(arr(n, "hosts")).chain(arr(n, "domains")).chain(arr(n, "services")).filter_map(|x| x.as_str()).collect::<Vec<_>>().join(" "))])).take(20).map(|n| { let mut x = node_json(n); if let Some(o) = x.as_object_mut() { o.remove("evidence"); } x }).collect::<Vec<_>>(),
        "files": arr(graph, "files").filter(|f| hit(&[&f["p"], &f["name"], &f["d"]])).take(30).map(|f| json!({"path": f["p"], "name": f["name"], "description": f["d"], "part": f["n"]})).collect::<Vec<_>>(),
        "endpoints": arr(graph, "routes").enumerate().filter(|(_, r)| hit(&[&r["m"], &r["p"], &r["name"], &r["d"]])).take(30).map(|(i, r)| json!({"id": format!("r:{i}"), "method": r["m"], "path": r["p"], "name": r["name"], "file": r["f"]})).collect::<Vec<_>>(),
        "screens": arr(graph, "screens").filter(|s| hit(&[&s["url"], &s["name"], &s["description"], &s["nav"]["app"], &s["nav"]["navigator"], &s["nav"]["group"], &s["nav"]["type"], &s["nav"]["renderer"]])).take(20).map(screen_json).collect::<Vec<_>>(),
        "journeys": arr(graph, "flows").filter(|f| hit(&[&f["id"], &f["title"], &f["description"]])).take(20).map(|f| json!({"id": f["id"], "title": f["title"]})).collect::<Vec<_>>(),
    })
}

// ── Impact: what changing one thing reaches ───────────────────────────────

/// Most items [`impact`] lists in each of its groups; the rest are counted.
pub const IMPACT_CAP: usize = 20;

/// The methods an endpoint target may start with (`POST /slips/:id`).
const METHODS: [&str; 9] = [
    "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "ANY", "ALL",
];

/// What an impact target named.
enum Target {
    /// Endpoints by index in `routes`.
    Endpoints(Vec<usize>),
    File(String),
    Part(String),
}

/// `file:line` of the first evidence on `v`, else `fallback` at line 1, so
/// every citation is a real place.
fn first_at(v: &Value, fallback: &str) -> String {
    arr(v, "evidence")
        .find_map(|e| {
            Some(format!(
                "{}:{}",
                e["file"].as_str().filter(|f| !f.is_empty())?,
                e["line"].as_u64().unwrap_or(1).max(1)
            ))
        })
        .unwrap_or_else(|| format!("{fallback}:1"))
}

fn route_at(r: &Value) -> String {
    format!(
        "{}:{}",
        r["f"].as_str().unwrap_or(""),
        r["l"].as_u64().unwrap_or(1).max(1)
    )
}

fn resolve(graph: &Value, target: &str) -> Option<Target> {
    let t = target.trim().trim_start_matches("./");
    if t.is_empty() {
        return None;
    }
    if let Some(i) = t.strip_prefix("r:").and_then(|i| i.parse::<usize>().ok()) {
        return graph["routes"].get(i).map(|_| Target::Endpoints(vec![i]));
    }
    let same_path = |a: &str, b: &str| a.trim_end_matches('/') == b.trim_end_matches('/');
    if let Some((m, p)) = t.split_once(char::is_whitespace) {
        let m = m.to_ascii_uppercase();
        let p = p.trim();
        if METHODS.contains(&m.as_str()) && p.starts_with('/') {
            let hits: Vec<usize> = arr(graph, "routes")
                .enumerate()
                .filter(|(_, r)| {
                    r["p"].as_str().is_some_and(|rp| same_path(rp, p))
                        && r["m"]
                            .as_str()
                            .is_some_and(|rm| rm.eq_ignore_ascii_case(&m) || m == "ANY")
                })
                .map(|(i, _)| i)
                .collect();
            return (!hits.is_empty()).then_some(Target::Endpoints(hits));
        }
    }
    if arr(graph, "files").any(|f| f["p"] == t) {
        return Some(Target::File(t.to_string()));
    }
    let id = t.strip_prefix("n:").unwrap_or(t);
    if node(graph, id).is_some() {
        return Some(Target::Part(id.to_string()));
    }
    if t.starts_with('/') {
        let hits: Vec<usize> = arr(graph, "routes")
            .enumerate()
            .filter(|(_, r)| r["p"].as_str().is_some_and(|rp| same_path(rp, t)))
            .map(|(i, _)| i)
            .collect();
        if !hits.is_empty() {
            return Some(Target::Endpoints(hits));
        }
    }
    None
}

/// Keeps the first [`IMPACT_CAP`] of `items`, counting the rest in `more`.
fn capped(items: Vec<Value>, key: &str, more: &mut Map<String, Value>) -> Value {
    if items.len() > IMPACT_CAP {
        more.insert(key.into(), json!(items.len() - IMPACT_CAP));
    }
    json!(items.into_iter().take(IMPACT_CAP).collect::<Vec<_>>())
}

/// What changing one thing reaches across the whole app, in one answer:
/// `target` is a file on the map (`harbor-api/src/bookings.js`), an
/// endpoint (`POST /slips/:id/bookings`, or `r:0`) or a part's id. Lists the
/// callers in other repositories and in the same one, the screens that use
/// it (with their app and whether they need a signed-in user), the journeys
/// that pass through it, the outside services it reaches (and those that
/// call it), the cloud resources that start or deploy it, and the data links
/// (tables). Every item cites a `file:line`; each group holds at most
/// [`IMPACT_CAP`] items and counts the rest in `more`. `None` when the map
/// has no such thing.
pub fn impact(graph: &Value, target: &str) -> Option<Value> {
    let t = resolve(graph, target)?;
    let routes: Vec<&Value> = arr(graph, "routes").collect();
    let file_of = |p: &str| arr(graph, "files").find(|f| f["p"] == p);
    let repo_of = |p: &str| {
        file_of(p)
            .and_then(|f| f["r"].as_str())
            .map(str::to_string)
            .unwrap_or_else(|| p.split('/').next().unwrap_or("").to_string())
    };
    // What the target is, as map ids and file paths.
    let mut paths: BTreeSet<String> = BTreeSet::new();
    let mut eps: Vec<usize> = Vec::new();
    let mut parts: BTreeSet<String> = BTreeSet::new();
    let described = match &t {
        Target::Endpoints(is) => {
            eps = is.clone();
            for i in is {
                if let Some(f) = routes[*i]["f"].as_str() {
                    paths.insert(f.to_string());
                }
            }
            let r = routes[is[0]];
            json!({"kind": "endpoint", "method": r["m"], "path": r["p"], "name": r["name"], "at": route_at(r)})
        }
        Target::File(p) => {
            paths.insert(p.clone());
            let f = file_of(p).cloned().unwrap_or(Value::Null);
            json!({"kind": "file", "path": p, "name": f["name"], "part": f["n"], "repo": f["r"], "at": format!("{p}:1")})
        }
        Target::Part(id) => {
            parts.insert(id.clone());
            let mut grew = true;
            while grew {
                grew = false;
                for c in arr(graph, "nodes") {
                    if let (Some(cid), Some(p)) = (c["id"].as_str(), c["parent"].as_str()) {
                        if parts.contains(p) && parts.insert(cid.to_string()) {
                            grew = true;
                        }
                    }
                }
            }
            for f in arr(graph, "files") {
                if f["n"].as_str().is_some_and(|n| parts.contains(n)) {
                    if let Some(p) = f["p"].as_str() {
                        paths.insert(p.to_string());
                    }
                }
            }
            let n = node(graph, id).unwrap_or(Value::Null);
            let at = paths
                .iter()
                .next()
                .map(|p| format!("{p}:1"))
                .unwrap_or_else(|| first_at(&n, ""));
            json!({"kind": "part", "id": id, "label": n["label"], "part_kind": n["kind"], "at": at})
        }
    };
    if !matches!(t, Target::Endpoints(_)) {
        eps = routes
            .iter()
            .enumerate()
            .filter(|(_, r)| r["f"].as_str().is_some_and(|f| paths.contains(f)))
            .map(|(i, _)| i)
            .collect();
    }
    let ep_ids: BTreeSet<String> = eps.iter().map(|i| format!("r:{i}")).collect();
    let ep_labels: BTreeSet<String> = eps
        .iter()
        .map(|i| {
            format!(
                "{} {}",
                routes[*i]["m"].as_str().unwrap_or(""),
                routes[*i]["p"].as_str().unwrap_or("")
            )
            .to_ascii_uppercase()
        })
        .collect();
    let only_endpoints = matches!(t, Target::Endpoints(_));
    let inside = |id: &str| -> bool {
        if let Some(p) = id.strip_prefix("f:") {
            return paths.contains(p);
        }
        if let Some(n) = id.strip_prefix("n:") {
            return parts.contains(n);
        }
        ep_ids.contains(id)
    };
    let home: BTreeSet<String> = paths.iter().map(|p| repo_of(p)).collect();
    let kind_of = |id: &str| -> String {
        id.strip_prefix("n:")
            .and_then(|n| node(graph, n))
            .and_then(|n| n["kind"].as_str().map(str::to_string))
            .unwrap_or_default()
    };
    // A link into an endpoint target counts when it is for one of them.
    let for_endpoint = |l: &Value| -> bool {
        if !only_endpoints {
            return true;
        }
        if l["route"].as_str().is_some_and(|r| ep_ids.contains(r)) {
            return true;
        }
        // An import of the endpoint's file is not a call to the endpoint.
        if !matches!(l["k"].as_str(), Some("http" | "webhook")) {
            return false;
        }
        let labels: Vec<String> = arr(l, "labels")
            .filter_map(|x| x.as_str().map(|s| s.to_ascii_uppercase()))
            .collect();
        labels.is_empty() || labels.iter().any(|x| ep_labels.contains(x))
    };

    let mut elsewhere = Vec::new();
    let mut here = Vec::new();
    let mut outside = Vec::new();
    let mut cloud = Vec::new();
    let mut data = Vec::new();
    for l in arr(graph, "links") {
        let (a, b) = (l["a"].as_str().unwrap_or(""), l["b"].as_str().unwrap_or(""));
        let k = l["k"].as_str().unwrap_or("");
        let (a_in, b_in) = (inside(a), inside(b));
        if a_in == b_in && k != "data" {
            continue;
        }
        let other = if a_in { b } else { a };
        let other_kind = kind_of(other);
        match k {
            "data" if a_in || b_in => data.push(json!({
                "tables": l["tables"],
                "from": describe_ref(graph, a)["path"],
                "to": describe_ref(graph, b)["path"],
                "labels": l["labels"],
                "at": first_at(l, a.strip_prefix("f:").unwrap_or("")),
            })),
            "data" => {}
            _ if other_kind == "infra" => cloud.push(json!({
                "id": other,
                "label": describe_ref(graph, other)["label"],
                "resource": node(graph, other.trim_start_matches("n:")).map(|n| n["resource"].clone()).unwrap_or(Value::Null),
                "how": k,
                "labels": l["labels"],
                "starts": l["unit"].as_str().map(|u| describe_ref(graph, u)["label"].clone()),
                "at": first_at(l, if a_in { a } else { b }.trim_start_matches("f:")),
            })),
            "nav" => {}
            _ if b_in && other.starts_with("f:") && for_endpoint(l) => {
                let p = other.trim_start_matches("f:");
                let repo = repo_of(p);
                let item = json!({
                    "from": p,
                    "repo": repo,
                    "kind": k,
                    "labels": l["labels"],
                    "at": first_at(l, p),
                });
                if home.contains(&repo) {
                    here.push(item);
                } else {
                    elsewhere.push(item);
                }
            }
            _ if other_kind == "external" && (a_in || for_endpoint(l)) => {
                let n = node(graph, other.trim_start_matches("n:")).unwrap_or(Value::Null);
                outside.push(json!({
                    "id": other,
                    "label": n["label"],
                    "vendor": n["vendor"],
                    "direction": if a_in { "uses" } else { "calls in" },
                    "via": if a_in { a } else { b }.trim_start_matches("f:"),
                    "labels": l["labels"],
                    "at": first_at(l, if a_in { a } else { b }.trim_start_matches("f:")),
                }));
            }
            _ => {}
        }
    }
    // Outside services reached a few steps away, through what the target
    // imports or calls.
    let mut seen: BTreeSet<String> = paths.iter().map(|p| format!("f:{p}")).collect();
    let mut front: Vec<String> = seen.iter().cloned().collect();
    for _ in 0..3 {
        let mut next = Vec::new();
        for l in arr(graph, "links") {
            let (a, b) = (l["a"].as_str().unwrap_or(""), l["b"].as_str().unwrap_or(""));
            if !front.iter().any(|f| f == a) || !matches!(l["k"].as_str(), Some("import" | "http"))
            {
                continue;
            }
            if b.starts_with("f:") && seen.len() < 400 && seen.insert(b.to_string()) {
                next.push(b.to_string());
            } else if kind_of(b) == "external"
                && !inside(a)
                && !outside.iter().any(|o| o["id"] == b)
            {
                let n = node(graph, b.trim_start_matches("n:")).unwrap_or(Value::Null);
                outside.push(json!({
                    "id": b,
                    "label": n["label"],
                    "vendor": n["vendor"],
                    "direction": "reaches",
                    "via": a.trim_start_matches("f:"),
                    "labels": l["labels"],
                    "at": first_at(l, a.trim_start_matches("f:")),
                }));
            }
        }
        if next.is_empty() {
            break;
        }
        front = next;
    }
    // Screens: the screen is one of the files, stands on one (or on one of
    // the endpoints), or a navigator among them registers it.
    let screens: Vec<Value> = arr(graph, "screens")
        .filter(|s| {
            let behind = arr(s, "behind").any(|b| b["id"].as_str().is_some_and(&inside));
            let file = s["file"].as_str().is_some_and(|f| paths.contains(f));
            let nav = arr(&s["nav"], "evidence")
                .next()
                .and_then(|e| e["file"].as_str())
                .is_some_and(|f| paths.contains(f));
            if only_endpoints {
                arr(s, "behind").any(|b| b["id"].as_str().is_some_and(|i| ep_ids.contains(i)))
            } else {
                behind || file || nav
            }
        })
        .map(|s| {
            let mut x = screen_json(s);
            x["gate"] = s["nav"]["gate"].clone();
            x["at"] = json!(if s["nav"].is_object() {
                first_at(&s["nav"], s["file"].as_str().unwrap_or(""))
            } else {
                format!("{}:1", s["file"].as_str().unwrap_or(""))
            });
            x
        })
        .collect();
    let screen_ids: BTreeSet<String> = screens
        .iter()
        .filter_map(|s| s["id"].as_str().map(str::to_string))
        .collect();
    // Journeys passing through it: a step stands on it, or on one of its
    // screens, or cites one of its files.
    let journeys: Vec<Value> = arr(graph, "flows")
        .filter_map(|f| {
            let mut steps = Vec::new();
            let mut cites = Vec::new();
            for (i, s) in arr(f, "steps").enumerate() {
                let r = s["ref"].as_str().unwrap_or("");
                let cited: Vec<String> = arr(s, "evidence")
                    .filter_map(|e| {
                        Some(format!(
                            "{}:{}",
                            e["file"].as_str()?,
                            e["line"].as_u64().unwrap_or(1).max(1)
                        ))
                    })
                    .collect();
                let on_cite = !only_endpoints
                    && arr(s, "evidence")
                        .any(|e| e["file"].as_str().is_some_and(|p| paths.contains(p)));
                let on_screen = s["screen"].as_str().is_some_and(|x| screen_ids.contains(x));
                if inside(r) || screen_ids.contains(r) || on_screen || on_cite {
                    steps.push(i + 1);
                    cites.extend(cited);
                }
            }
            (!steps.is_empty()).then(|| {
                if cites.is_empty() {
                    cites.extend(paths.iter().next().map(|p| format!("{p}:1")));
                }
                cites.dedup();
                json!({"id": f["id"], "title": f["title"], "steps": steps, "at": cites})
            })
        })
        .collect();
    let endpoints: Vec<Value> = eps
        .iter()
        .map(|i| {
            let r = routes[*i];
            json!({"id": format!("r:{i}"), "method": r["m"], "path": r["p"], "name": r["name"], "at": route_at(r)})
        })
        .collect();
    let elsewhere_repos: BTreeSet<String> = elsewhere
        .iter()
        .filter_map(|c| c["repo"].as_str().map(str::to_string))
        .collect();
    let mut more = Map::new();
    Some(json!({
        "target": described,
        "endpoints": capped(endpoints, "endpoints", &mut more),
        "callers_elsewhere": capped(elsewhere, "callers_elsewhere", &mut more),
        "other_repos": elsewhere_repos,
        "callers_here": capped(here, "callers_here", &mut more),
        "screens": capped(screens, "screens", &mut more),
        "journeys": capped(journeys, "journeys", &mut more),
        "outside_services": capped(outside, "outside_services", &mut more),
        "cloud": capped(cloud, "cloud", &mut more),
        "data": capped(data, "data", &mut more),
        "more": more,
    }))
}

/// Every `file:line` the map itself names: its evidence (parts, links,
/// screens, journey steps), its endpoints, and the first line of each file.
pub fn known_places(graph: &Value) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = BTreeSet::new();
    for f in arr(graph, "files") {
        if let Some(p) = f["p"].as_str() {
            out.insert(format!("{p}:1"));
        }
    }
    for r in arr(graph, "routes") {
        out.insert(route_at(r));
    }
    let mut stack = vec![graph];
    while let Some(x) = stack.pop() {
        match x {
            Value::Object(o) => {
                if let Some(ev) = o.get("evidence").and_then(|e| e.as_array()) {
                    for e in ev {
                        if let Some(f) = e["file"].as_str() {
                            out.insert(format!("{f}:{}", e["line"].as_u64().unwrap_or(1).max(1)));
                        }
                    }
                }
                stack.extend(o.values().filter(|v| v.is_object() || v.is_array()));
            }
            Value::Array(a) => stack.extend(a.iter().filter(|v| v.is_object() || v.is_array())),
            _ => {}
        }
    }
    out
}

/// Whether `cite` (`path:line`) is a real place on the map: one the map
/// itself names ([`known_places`], what its lookups cite), or a line within
/// the code lines it counted for that file. `repo` (`owner/name`) places a
/// path written from the repository's root, or one starting with
/// `owner/name/`.
pub fn real_cite(graph: &Value, repo: &str, cite: &str) -> bool {
    real_cite_in(&known_places(graph), graph, repo, cite)
}

/// [`real_cite`] with the map's places read once.
pub fn real_cite_in(places: &BTreeSet<String>, graph: &Value, repo: &str, cite: &str) -> bool {
    let Some((path, line)) = cite.trim().rsplit_once(':') else {
        return false;
    };
    let Ok(line) = line.trim().parse::<u64>() else {
        return false;
    };
    let path = path.trim().trim_matches('`');
    let on_map = if arr(graph, "files").any(|f| f["p"] == path) {
        Some(path.to_string())
    } else {
        map_path(graph, repo, path)
    };
    let Some(p) = on_map else {
        return false;
    };
    if line == 0 {
        return false;
    }
    if places.contains(&format!("{p}:{line}")) {
        return true;
    }
    let lines = arr(graph, "files")
        .find(|f| f["p"] == p.as_str())
        .and_then(|f| f["loc"].as_u64())
        .unwrap_or(0);
    line <= lines
}

/// The top-level parts a set of map ids belongs to (for a mini map).
pub fn parts_of(graph: &Value, ids: &[String]) -> BTreeSet<String> {
    let part_of_file = |p: &str| {
        arr(graph, "files")
            .find(|f| f["p"] == p)
            .and_then(|f| f["n"].as_str())
            .map(|n| top_node(graph, n))
    };
    let mut out = BTreeSet::new();
    for id in ids {
        let part = if let Some(n) = id.strip_prefix("n:") {
            node(graph, n).map(|_| top_node(graph, n))
        } else if let Some(p) = id.strip_prefix("f:") {
            part_of_file(p)
        } else if let Some(i) = id.strip_prefix("r:").and_then(|i| i.parse::<usize>().ok()) {
            graph["routes"]
                .get(i)
                .and_then(|r| r["f"].as_str())
                .and_then(part_of_file)
        } else {
            node(graph, id).map(|_| top_node(graph, id))
        };
        out.extend(part);
    }
    out
}

/// A smaller map holding only `parts` (and their sub-parts), their files,
/// endpoints and the links among them: Ask's mini map, drawn by the same
/// viewer.
pub fn sub_map(graph: &Value, parts: &BTreeSet<String>) -> Value {
    let keep: BTreeSet<String> = arr(graph, "nodes")
        .filter_map(|n| n["id"].as_str())
        .filter(|id| parts.contains(&top_node(graph, id)))
        .map(str::to_string)
        .collect();
    let files: Vec<Value> = arr(graph, "files")
        .filter(|f| f["n"].as_str().is_some_and(|n| keep.contains(n)))
        .cloned()
        .collect();
    let paths: BTreeSet<String> = files
        .iter()
        .filter_map(|f| f["p"].as_str().map(str::to_string))
        .collect();
    let mut renumber: HashMap<usize, usize> = HashMap::new();
    let mut routes = Vec::new();
    for (i, r) in arr(graph, "routes").enumerate() {
        if r["f"].as_str().is_some_and(|f| paths.contains(f)) {
            renumber.insert(i, routes.len());
            routes.push(r.clone());
        }
    }
    let remap = |id: &str| -> Option<String> {
        if let Some(n) = id.strip_prefix("n:") {
            return keep.contains(n).then(|| id.to_string());
        }
        if let Some(p) = id.strip_prefix("f:") {
            return paths.contains(p).then(|| id.to_string());
        }
        let i = id.strip_prefix("r:")?.parse::<usize>().ok()?;
        renumber.get(&i).map(|n| format!("r:{n}"))
    };
    let links: Vec<Value> = arr(graph, "links")
        .filter_map(|l| {
            let a = remap(l["a"].as_str()?)?;
            let b = remap(l["b"].as_str()?)?;
            let mut l = l.clone();
            l["a"] = json!(a);
            l["b"] = json!(b);
            Some(l)
        })
        .collect();
    let mut out = graph.clone();
    out["nodes"] = json!(arr(graph, "nodes")
        .filter(|n| n["id"].as_str().is_some_and(|i| keep.contains(i)))
        .cloned()
        .collect::<Vec<_>>());
    out["files"] = json!(files);
    out["routes"] = json!(routes);
    out["links"] = json!(links);
    // Journeys stay when every step is still on the smaller map.
    out["flows"] = json!(arr(graph, "flows")
        .filter_map(|f| {
            let mut f = f.clone();
            for s in f["steps"].as_array_mut()? {
                let r = remap(s["ref"].as_str()?)?;
                s["ref"] = json!(r);
            }
            Some(f)
        })
        .collect::<Vec<_>>());
    out["tour"] = json!([]);
    out["screens"] = json!([]);
    if let Some(o) = out.as_object_mut() {
        o.remove("recent");
        o.remove("health");
    }
    out
}

// ── The map lookups (Chat and /mcp) ────────────────────────────────────────

/// The map lookups, in the order they are offered.
pub const TOOLS: [&str; 7] = [
    "map_overview",
    "map_find",
    "map_part",
    "map_screen",
    "map_file",
    "map_flow",
    "map_impact",
];

/// What `map_impact` is told to be for, the same words everywhere it is
/// offered (runs, Chat and `/mcp`).
pub const IMPACT_DESCRIPTION: &str = "Before changing shared code or an endpoint: everything on the app map that changing one thing reaches, in one answer. Give a file's path on the map (repository folder first, like harbor-api/src/bookings.js), an endpoint (method and path, like POST /slips/:id/bookings) or a part's id. Returns the callers in other repositories and in the same one, the screens that use it (their app and whether they need a signed-in user), the journeys that pass through it, the outside services it reaches or that call it, the cloud resources that start or deploy it, and the data links (tables), each with the file:line it rests on.";

pub fn is_tool(name: &str) -> bool {
    TOOLS.contains(&name)
}

/// The map lookups, as Chat's model reads them (`input_schema`).
pub fn tool_defs() -> Vec<Value> {
    let obj = |props: Value, req: &[&str]| json!({"type": "object", "properties": props, "required": req});
    vec![
        json!({"name": TOOLS[0], "description": "The app map in short: every part (id, name, kind, what it does), the screens, the journeys (user flows such as sign in or checkout), the Health counts and the repositories. Start here for how the app fits together.", "input_schema": obj(json!({}), &[])}),
        json!({"name": TOOLS[1], "description": "Search the app map: parts, files, endpoints, screens and journeys whose names, descriptions or paths hold every word.", "input_schema": obj(json!({"query": {"type": "string"}}), &["query"])}),
        json!({"name": TOOLS[2], "description": "One part of the app map by id: its sub-parts, files, endpoints, and the lines in and out (imports, web requests matched to endpoints, outside services).", "input_schema": obj(json!({"id": {"type": "string"}}), &["id"])}),
        json!({"name": TOOLS[3], "description": "Everything behind one screen, by its id or its address (like /slips/:id), step by step: the files it uses, the requests it makes, the server code that answers and the outside services behind it.", "input_schema": obj(json!({"screen": {"type": "string"}}), &["screen"])}),
        json!({"name": TOOLS[4], "description": "One file by its path on the app map (repository folder first, like harbor-api/src/bookings.js): what it is, its endpoints and what it connects to.", "input_schema": obj(json!({"path": {"type": "string"}}), &["path"])}),
        json!({"name": TOOLS[5], "description": "One journey on the app map (a user flow such as sign in, password reset or checkout), by its id or the words of its title (like \"mobile sign in\"): every step in order, from the screen through the requests, the server code and outside services to the next screen, each with what happens there and the file:line it cites. Use it to walk someone through a flow.", "input_schema": obj(json!({"journey": {"type": "string"}}), &["journey"])}),
        json!({"name": TOOLS[6], "description": IMPACT_DESCRIPTION, "input_schema": obj(json!({"target": {"type": "string", "description": "A file's path on the map, an endpoint (METHOD /path) or a part's id."}}), &["target"])}),
    ]
}

/// The map lookups over one team's map. Records the map ids its lookups
/// returned, for the mini map of the parts an answer involves.
#[derive(Debug, Clone)]
pub struct Lookups {
    pub graph: Value,
    pub ids: Vec<String>,
}

impl Lookups {
    pub fn new(graph: Value) -> Self {
        Self {
            graph,
            ids: Vec::new(),
        }
    }

    fn note(&mut self, v: &Value) {
        let mut stack = vec![v];
        while let Some(x) = stack.pop() {
            match x {
                Value::Object(o) => {
                    if let Some(id) = o.get("id").and_then(|i| i.as_str()) {
                        self.ids.push(id.to_string());
                    }
                    if let Some(p) = o.get("path").and_then(|i| i.as_str()) {
                        self.ids.push(format!("f:{p}"));
                    }
                    stack.extend(o.values());
                }
                Value::Array(a) => stack.extend(a.iter()),
                _ => {}
            }
        }
    }

    /// Runs one lookup: (what it found, its label). What it found holds an
    /// `error` when the map has no such thing.
    pub fn lookup(&mut self, name: &str, input: &Value) -> (Value, String) {
        let s = |k: &str| input[k].as_str().unwrap_or("").trim().to_string();
        let v = match name {
            "map_overview" => overview(&self.graph),
            "map_find" => search(&self.graph, &s("query")),
            "map_part" => part_view(&self.graph, &s("id")).unwrap_or_else(
                || json!({"error": "No part has that id; use map_overview or map_find for ids."}),
            ),
            "map_screen" => screen_view(&self.graph, &s("screen")).unwrap_or_else(
                || json!({"error": "No screen has that id or address; use map_overview."}),
            ),
            "map_file" => file_view(&self.graph, &s("path"))
                .unwrap_or_else(|| json!({"error": "That file isn't on the map."})),
            "map_flow" => flow_view(&self.graph, &s("journey")).unwrap_or_else(
                || json!({"error": "No journey has that id or title; map_overview lists them."}),
            ),
            "map_impact" => impact(&self.graph, &s("target")).unwrap_or_else(
                || json!({"error": "Nothing on the map has that path, endpoint or id; use map_find for them."}),
            ),
            _ => json!({"error": "Unknown lookup."}),
        };
        let label = label(name, input);
        if v.get("error").is_none() && !matches!(name, "map_overview" | "map_find") {
            self.note(&v);
        }
        (v, label)
    }

    /// Runs one lookup and registers what it found as a citable source.
    /// Returns (what the model reads, the label, failed).
    pub fn cite(
        &mut self,
        reg: &mut crate::knowledge::answer::Registry,
        name: &str,
        input: &Value,
    ) -> (String, String, bool) {
        let (v, label) = self.lookup(name, input);
        let failed = v.get("error").is_some();
        let text = crate::truncate_str(&v.to_string(), 30_000).to_string();
        if failed {
            return (text, label, true);
        }
        let id = reg.add(crate::knowledge::answer::SourceRef {
            kind: SOURCE_KIND.into(),
            title: label.clone(),
            loc: "App map".into(),
            passage: crate::truncate_str(&text, 1_200).to_string(),
            href: Some(page_link(&self.parts())),
            ..Default::default()
        });
        (format!("[{id}] {text}"), label, false)
    }

    /// The top-level parts the lookups so far touched.
    pub fn parts(&self) -> Vec<String> {
        parts_of(&self.graph, &self.ids).into_iter().collect()
    }

    /// The parts the lookups touched, as a smaller map the same viewer
    /// draws, with their ids; `None` when they touched none.
    pub fn mini_map(&self, now: &str) -> Option<Value> {
        let parts: BTreeSet<String> = parts_of(&self.graph, &self.ids);
        if parts.is_empty() {
            return None;
        }
        Some(json!({
            "graph": for_viewer(sub_map(&self.graph, &parts), now),
            "parts": parts.into_iter().collect::<Vec<_>>(),
        }))
    }
}

/// What a lookup is shown as: "Searched the app map for slip".
pub fn label(name: &str, input: &Value) -> String {
    let s = |k: &str| crate::truncate_str(input[k].as_str().unwrap_or("").trim(), 80).to_string();
    match name {
        "map_overview" => "Read the app map".into(),
        "map_find" => format!("Searched the app map for {}", s("query")),
        "map_part" => format!("Opened the part {} on the app map", s("id")),
        "map_screen" => format!("Followed the screen {} on the app map", s("screen")),
        "map_file" => format!("Opened {} on the app map", s("path")),
        "map_flow" => format!("Walked through the journey {} on the app map", s("journey")),
        "map_impact" => format!(
            "Looked up what changing {} reaches on the app map",
            s("target")
        ),
        _ => name.to_string(),
    }
}

/// The source kind of a map lookup.
pub const SOURCE_KIND: &str = "app_map";

/// The App map page, focused on `parts` when there are any.
pub fn page_link(parts: &[String]) -> String {
    if parts.is_empty() {
        return "/app-map".into();
    }
    let ids: Vec<String> = parts
        .iter()
        .take(10)
        .map(|p| {
            p.chars()
                .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
                .collect()
        })
        .collect();
    format!("/app-map?focus={}", ids.join(","))
}

// ── Model answers ──────────────────────────────────────────────────────────

/// Problems with `v` against `schema`, for the JSON Schema subset Floorplan's
/// workflow uses (type, properties, required, items, enum, pattern,
/// minItems, minLength, additionalProperties). Empty when it fits.
pub fn check_answer(schema: &Value, v: &Value) -> Vec<String> {
    let mut out = Vec::new();
    check_at(schema, v, "answer", &mut out);
    out.truncate(30);
    out
}

fn check_at(schema: &Value, v: &Value, at: &str, out: &mut Vec<String>) {
    if let Some(t) = schema["type"].as_str() {
        let ok = match t {
            "object" => v.is_object(),
            "array" => v.is_array(),
            "string" => v.is_string(),
            "number" => v.is_number(),
            "integer" => v.is_i64() || v.is_u64(),
            "boolean" => v.is_boolean(),
            "null" => v.is_null(),
            _ => true,
        };
        if !ok {
            out.push(format!("{at} should be a {t}"));
            return;
        }
    }
    if let Some(e) = schema["enum"].as_array() {
        if !e.contains(v) {
            out.push(format!("{at} should be one of {}", Value::Array(e.clone())));
        }
    }
    if let (Some(p), Some(s)) = (schema["pattern"].as_str(), v.as_str()) {
        if let Ok(re) = regex::Regex::new(p) {
            if !re.is_match(s) {
                out.push(format!("{at} doesn't match {p}"));
            }
        }
    }
    if let (Some(n), Some(s)) = (schema["minLength"].as_u64(), v.as_str()) {
        if (s.chars().count() as u64) < n {
            out.push(format!("{at} is too short"));
        }
    }
    if let Some(a) = v.as_array() {
        if let Some(n) = schema["minItems"].as_u64() {
            if (a.len() as u64) < n {
                out.push(format!("{at} needs at least {n} items"));
            }
        }
        if schema["items"].is_object() {
            for (i, x) in a.iter().enumerate() {
                check_at(&schema["items"], x, &format!("{at}[{i}]"), out);
            }
        }
    }
    if let Some(o) = v.as_object() {
        let props = schema["properties"].as_object();
        for r in arr(schema, "required").filter_map(|r| r.as_str()) {
            if !o.contains_key(r) {
                out.push(format!("{at}.{r} is missing"));
            }
        }
        for (k, x) in o {
            match props.and_then(|p| p.get(k)) {
                Some(s) => check_at(s, x, &format!("{at}.{k}"), out),
                None => match &schema["additionalProperties"] {
                    Value::Bool(false) => out.push(format!("{at}.{k} is not allowed")),
                    s if s.is_object() => check_at(s, x, &format!("{at}.{k}"), out),
                    _ => {}
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_build_at_a_time() {
        let now = 10_000;
        let first = queue_next(None, now, "b1").unwrap();
        assert_eq!(
            (first.state.as_str(), first.build.as_str()),
            ("queued", "b1")
        );
        // A second ask while it is queued, then while it runs: refused.
        assert!(queue_next(Some(&first), now + 3, "b2").is_none());
        let running = take_turn(Some(&first), "b1", 0, now + 5).unwrap();
        assert!(queue_next(Some(&running), now + 8, "b3").is_none());
        // Stuck past STALE_SECS: a new build may replace it.
        assert!(queue_next(Some(&running), now + 5 + STALE_SECS + 1, "b4").is_some());
        // Finished or failed: a new build, keeping when the last map was made.
        let done = finished(Some(&running), "b1", 12, "", now + 300).unwrap();
        assert_eq!((done.built, done.built_repos), (now + 300, 12));
        let again = queue_next(Some(&done), now + 400, "b5").unwrap();
        assert_eq!(
            (again.built, again.built_repos, again.repos),
            (now + 300, 12, 12)
        );
        let f = failed(Some(&again), "b5", "GitHub didn't answer", now + 401).unwrap();
        assert!(queue_next(Some(&f), now + 402, "b6").is_some());
    }

    #[test]
    fn a_map_from_another_extractor_queues_one_build() {
        let now = 10_000;
        let old = Status {
            state: "ready".into(),
            at: now,
            built: now - 86_400,
            built_repos: 3,
            extractor: "older".into(),
            ..Default::default()
        };
        // Made by another extractor: one build, marked as asked for this one.
        let q = queue_for_extractor(Some(&old), now, "b1", "new").unwrap();
        assert_eq!(
            (q.state.as_str(), q.asked_extractor.as_str()),
            ("queued", "new")
        );
        assert_eq!((q.extractor.as_str(), q.built_repos), ("older", 3));
        // Asked again while it is queued, while it runs, and after it failed:
        // nothing more is queued.
        assert!(queue_for_extractor(Some(&q), now + 1, "b2", "new").is_none());
        let running = take_turn(Some(&q), "b1", 0, now + 2).unwrap();
        assert!(queue_for_extractor(Some(&running), now + 3, "b3", "new").is_none());
        let f = failed(Some(&running), "b1", "GitHub didn't answer", now + 4).unwrap();
        assert!(queue_for_extractor(Some(&f), now + 5, "b4", "new").is_none());
        // A map from before maps carried the version counts as another's.
        let unstamped = Status {
            extractor: String::new(),
            ..old.clone()
        };
        assert!(queue_for_extractor(Some(&unstamped), now, "b5", "new").is_some());
        // The next release asks once more.
        assert!(queue_for_extractor(Some(&f), now + 5, "b6", "newer").is_some());
    }

    #[test]
    fn a_map_from_this_extractor_queues_none() {
        let now = 10_000;
        let same = Status {
            state: "ready".into(),
            at: now,
            extractor: "new".into(),
            ..Default::default()
        };
        assert!(queue_for_extractor(Some(&same), now, "b1", "new").is_none());
        assert!(!made_by_another_extractor(Some(&same), true, "new"));
        // No map at all: the first build is the first visit's, not this.
        assert!(!made_by_another_extractor(None, false, "new"));
        // A push's build already running builds with this extractor.
        let pushed = queue_next(
            Some(&Status {
                extractor: "older".into(),
                ..same.clone()
            }),
            now,
            "p1",
        )
        .unwrap();
        assert!(queue_for_extractor(Some(&pushed), now + 1, "b2", "new").is_none());
    }

    #[tokio::test]
    async fn two_readers_at_once_queue_one_build() {
        use std::sync::{Arc, Mutex};
        let old = Status {
            state: "ready".into(),
            at: 10_000,
            extractor: "older".into(),
            ..Default::default()
        };
        // The status table: the status and its version.
        let store = Arc::new(Mutex::new((Some(old), 1_i64)));
        // Both readers read the old status before either writes.
        let both_read = Arc::new(tokio::sync::Barrier::new(2));
        let ask = |build: &'static str| {
            let store = store.clone();
            let both_read = both_read.clone();
            let first = Arc::new(Mutex::new(true));
            async move {
                update_versioned(
                    || {
                        let (store, both_read, first) =
                            (store.clone(), both_read.clone(), first.clone());
                        async move {
                            let seen = store.lock().unwrap().clone();
                            let wait = std::mem::replace(&mut *first.lock().unwrap(), false);
                            if wait {
                                both_read.wait().await;
                            }
                            Ok((seen.0, Some(seen.1)))
                        }
                    },
                    |next: Status, version: Option<i64>| {
                        let store = store.clone();
                        async move {
                            let mut s = store.lock().unwrap();
                            if Some(s.1) != version {
                                return Ok(false);
                            }
                            *s = (Some(next), s.1 + 1);
                            Ok(true)
                        }
                    },
                    |cur| queue_for_extractor(cur, 10_100, build, "new"),
                )
                .await
                .unwrap()
            }
        };
        let (a, b) = tokio::join!(ask("b1"), ask("b2"));
        let written = [&a, &b]
            .iter()
            .filter(|u| matches!(u, Updated::Written(_)))
            .count();
        assert_eq!(written, 1, "{a:?} {b:?}");
        let (st, v) = store.lock().unwrap().clone();
        assert_eq!(v, 2);
        assert_eq!(st.unwrap().asked_extractor, "new");
    }

    #[test]
    fn each_message_takes_its_own_turn_once() {
        let now = 10_000;
        let q = queue_next(None, now, "b1").unwrap();
        // Another build's message, or a turn out of order: nothing.
        assert!(take_turn(Some(&q), "other", 0, now).is_none());
        assert!(take_turn(Some(&q), "b1", 1, now).is_none());
        let t1 = take_turn(Some(&q), "b1", 0, now).unwrap();
        assert_eq!((t1.state.as_str(), t1.turn), ("building", 1));
        // The same start message delivered again: skipped.
        assert!(take_turn(Some(&t1), "b1", 0, now + 2).is_none());
        let t2 = take_turn(Some(&t1), "b1", 1, now + 60).unwrap();
        assert_eq!(t2.turn, 2);
        assert!(take_turn(Some(&t2), "b1", 1, now + 61).is_none());
        // Delivered again after its worker went quiet past the lease: taken again.
        assert!(take_turn(Some(&t2), "b1", 1, now + 60 + LEASE_SECS + 1).is_some());
        // A message from before build ids carries on a build without one.
        let legacy = Status {
            state: "building".into(),
            at: now,
            turn: 0,
            ..Default::default()
        };
        assert!(take_turn(Some(&legacy), "", 0, now).is_none());
        assert!(take_turn(
            Some(&Status {
                state: "queued".into(),
                ..legacy.clone()
            }),
            "",
            0,
            now
        )
        .is_some());
        // Progress and the end only for the build that is running.
        assert!(progress(Some(&t2), "other", Some("parts"), "x", now).is_none());
        assert_eq!(
            progress(Some(&t2), "b1", Some("parts"), "x", now)
                .unwrap()
                .stage,
            "parts"
        );
        assert_eq!(
            progress(Some(&t2), "b1", None, "y", now).unwrap().stage,
            "reading"
        );
        assert!(finished(Some(&t2), "other", 3, "", now).is_none());
    }

    #[test]
    fn the_estimate_grows_with_the_repositories() {
        assert_eq!(estimate_secs(0), estimate_secs(1));
        assert_eq!(estimate_secs(1), (4 * 60, 8 * 60));
        assert_eq!(estimate_secs(12), (15 * 60, 30 * 60));
        let (low, high) = estimate_secs(200);
        assert!(low <= high && high <= STALE_SECS);
    }

    /// The map Floorplan builds for the made-up harbor app (a web app and an
    /// API in two repositories), written by
    /// services/worker/floorplan/make-fixture.mjs and checked against the
    /// vendored release by driver.test.mjs.
    fn harbor() -> Value {
        let mut g: Value = serde_json::from_str(include_str!(
            "../../worker/floorplan/fixtures/harbor-graph.json"
        ))
        .unwrap();
        stamp_repos(
            &mut g,
            &folders(&[
                "harbor/harbor-web".into(),
                "harbor/harbor-api".into(),
                "harbor/harbor-mobile".into(),
                "harbor/harbor-infra".into(),
            ]),
        );
        g
    }

    #[test]
    fn repositories_get_stable_folders() {
        let f = folders(&[
            "harbor/web".into(),
            "marina/web".into(),
            "harbor/api".into(),
        ]);
        assert_eq!(f["harbor/web"], "harbor-web");
        assert_eq!(f["marina/web"], "marina-web");
        assert_eq!(f["harbor/api"], "api");
        let g = harbor();
        assert_eq!(
            map_path(&g, "harbor/harbor-api", "src/billing.js").as_deref(),
            Some("harbor-api/src/billing.js")
        );
        assert_eq!(
            map_path(&g, "harbor/harbor-web", "harbor/harbor-api/src/bookings.js").as_deref(),
            Some("harbor-api/src/bookings.js")
        );
        assert_eq!(map_path(&g, "harbor/harbor-api", "src/missing.js"), None);
        assert_eq!(map_path(&g, "other/repo", "src/billing.js"), None);
    }

    #[test]
    fn runs_become_the_maps_own_recent_work() {
        let runs = vec![
            WorkItem {
                run_id: "r1".into(),
                repo: "harbor/harbor-api".into(),
                ticket: "DOCK-42".into(),
                title: "Stop double bookings".into(),
                why: "Two boaters could book one slip.".into(),
                pr_url: "https://github.example/harbor/harbor-api/pull/7".into(),
                files: vec!["src/bookings.js".into(), "README.md".into()],
                ..Default::default()
            },
            WorkItem {
                run_id: "r2".into(),
                repo: "elsewhere/x".into(),
                files: vec!["a.js".into()],
                ..Default::default()
            },
        ];
        let g = with_overlays(harbor(), Some(&runs), None);
        let r = &g["recent"];
        assert_eq!(r["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(r["tasks"][0]["title"], "DOCK-42: Stop double bookings");
        assert_eq!(r["tasks"][0]["request"], "");
        assert!(r["tasks"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("pull/7"));
        assert_eq!(
            r["items"],
            json!([{"id": "f:harbor-api/src/bookings.js", "task": 0, "change": "changed", "note": "Changed by Coderhelm's run for DOCK-42"}])
        );
        // No run on the map: no recent work at all.
        assert!(with_overlays(harbor(), Some(&runs[1..]), None)
            .get("recent")
            .is_none());
    }

    #[test]
    fn findings_join_floorplans_checks_labelled_and_never_twice() {
        let found = vec![
            HealthItem {
                kind: "security".into(),
                id: "f1".into(),
                title: "Prototype pollution in a parser".into(),
                severity: "P1".into(),
                repo: "harbor/harbor-api".into(),
                files: vec![("package.json".into(), 0), ("src/bookings.js".into(), 5)],
                link: "/security/detail?id=f1".into(),
            },
            HealthItem {
                kind: "logs".into(),
                id: "l1".into(),
                title: "Bookings time out".into(),
                severity: "warning".into(),
                repo: "harbor/harbor-web".into(),
                files: vec![],
                link: "/logs?finding=l1".into(),
            },
        ];
        let base = with_overlays(harbor(), None, None);
        let theirs = base["health"]["issues"].as_array().unwrap();
        assert!(!theirs.is_empty());
        assert!(theirs.iter().all(|i| i["source"] == "floorplan"
            && i["title"]
                .as_str()
                .unwrap()
                .starts_with("Floorplan check: ")));
        let g = with_overlays(harbor(), None, Some(&found));
        let issues = g["health"]["issues"].as_array().unwrap();
        let ours: Vec<&Value> = issues
            .iter()
            .filter(|i| i["source"] == "coderhelm")
            .collect();
        assert_eq!(ours.len(), 2);
        assert_eq!(ours[0]["severity"], "high");
        assert_eq!(ours[0]["items"], json!(["f:harbor-api/src/bookings.js"]));
        assert_eq!(ours[0]["link"], "/security/detail?id=f1");
        // A finding with no file sits on the repository's part.
        assert_eq!(ours[1]["items"], json!(["n:web"]));
        // Floorplan's check at the same file and line is not shown twice.
        assert!(!issues
            .iter()
            .any(|i| i["source"] == "floorplan" && i["id"] == "no-auth:harbor-api"));
        assert_eq!(g["health"]["summary"]["high"], 1);
    }

    /// The page's copy and the index are written together from a built map;
    /// overlays placed with the stored index and laid over the stored copy
    /// are what the map with its overlays always was.
    #[test]
    fn overlays_from_the_index_match_the_map_with_its_overlays() {
        let runs = [WorkItem {
            repo: "harbor/harbor-api".into(),
            ticket: "DOCK-42".into(),
            title: "Stop double bookings".into(),
            files: vec!["src/bookings.js".into()],
            ..Default::default()
        }];
        let found = [
            HealthItem {
                kind: "security".into(),
                id: "f1".into(),
                title: "Prototype pollution".into(),
                severity: "P1".into(),
                repo: "harbor/harbor-api".into(),
                files: vec![("src/bookings.js".into(), 5)],
                link: "/security/detail?id=f1".into(),
            },
            HealthItem {
                kind: "logs".into(),
                id: "l1".into(),
                title: "Bookings time out".into(),
                severity: "warning".into(),
                repo: "harbor/harbor-web".into(),
                files: vec![],
                link: "/logs?finding=l1".into(),
            },
        ];
        let now = "2026-11-21T00:00:00Z";
        let (view, index) = derived(&harbor(), now);
        let view: Value = serde_json::from_slice(&gunzip(&view, MAX_GRAPH_BYTES).unwrap()).unwrap();
        let idx: MapIndex =
            serde_json::from_slice(&gunzip(&index, MAX_GRAPH_BYTES).unwrap()).unwrap();
        assert_eq!(idx, MapIndex::of(&harbor()));
        assert_eq!(view["meta"]["exported"]["by"], "Coderhelm");
        assert!(view["meta"]["root"].is_null());
        for (r, f) in [
            (Some(&runs[..]), Some(&found[..])),
            (Some(&runs[..]), None),
            (None, Some(&found[..])),
            (Some(&[][..]), Some(&[][..])),
        ] {
            let o = overlays(&idx, r, f);
            let mut want = for_viewer(with_overlays(harbor(), r, f), now);
            let mut got = apply_overlays(view.clone(), &o);
            // Recent work is stamped when it is made.
            for g in [&mut want, &mut got] {
                if let Some(r) = g.get_mut("recent") {
                    r["updatedAt"] = json!("");
                }
            }
            assert_eq!(got, want);
        }
        // The Floorplan check at the same file and line is dropped by id.
        let o = overlays(&idx, None, Some(&found));
        assert_eq!(o["health"]["drop"], json!(["no-auth:harbor-api"]));
        assert_eq!(idx.repo_node("HARBOR/harbor-web").as_deref(), Some("web"));
    }

    /// A made-up map the size of a large team's: `files` files in 12 parts
    /// across 9 repositories, three lines per file, 20 Floorplan checks.
    fn synthetic(files: usize) -> Value {
        let repos: Vec<String> = (0..9).map(|i| format!("dockyard/yard{i}")).collect();
        let folders = folders(&repos);
        let nodes: Vec<Value> = (0..12)
            .map(|i| json!({"id": format!("part{i}"), "parent": null, "label": format!("Dock area {i}"), "kind": "service", "summary": "Looks after berths.", "stats": {"files": files / 12}}))
            .collect();
        let fs: Vec<Value> = (0..files)
            .map(|i| {
                let r = &repos[i % 9];
                json!({"p": format!("{}/src/area{}/berth_{i}_keeper.ts", folders[r], i % 40), "n": format!("part{}", i % 12), "r": folders[r], "l": "typescript", "loc": 120, "name": format!("Berth keeper {i}"), "d": "Keeps one berth's bookings, its gate code and the boats that may use it, and tells the office when it changes.", "h": "0123456789abcdef", "s": [], "sig": ""})
            })
            .collect();
        let links: Vec<Value> = (0..files * 3)
            .map(|i| json!({"a": format!("f:{}", fs[i % files]["p"].as_str().unwrap()), "b": format!("f:{}", fs[(i * 7 + 13) % files]["p"].as_str().unwrap()), "k": "import", "w": 1}))
            .collect();
        let issues: Vec<Value> = (0..20)
            .map(|i| json!({"id": format!("open-endpoints:{}", fs[i * 17]["p"].as_str().unwrap()), "kind": "open-endpoints", "severity": if i % 3 == 0 { "high" } else { "medium" }, "title": "An endpoint anyone can call", "detail": "", "items": [format!("f:{}", fs[i * 17]["p"].as_str().unwrap())], "notes": {}, "locations": [{"file": fs[i * 17]["p"], "line": 10}], "fix": ""}))
            .collect();
        let mut g = json!({
            "schema": GRAPH_SCHEMA, "project": {"name": "Dockyard", "summary": "Berths and bookings."},
            "meta": {"root": "/tmp/x", "agent": "claude", "repos": repos},
            "nodes": nodes, "files": fs, "links": links, "routes": [], "flows": [], "tour": [], "screens": [],
            "health": {"issues": issues, "checked": [], "appWide": []},
        });
        stamp_repos(&mut g, &folders);
        g
    }

    /// What a page view costs at a large team's map size: before, the read API
    /// read and parsed the whole map, placed each run's and finding's files
    /// by scanning the map's files for each one, and sent the whole map back;
    /// now it reads the small index, places them by lookup and sends a
    /// signed address with the overlays. `APP_MAP_BENCH_GRAPH=<graph.json.gz>`
    /// measures a map on disk instead of the made-up one.
    #[test]
    fn a_page_view_reads_the_small_index_never_the_whole_map() {
        use std::time::Instant;
        let graph = match std::env::var("APP_MAP_BENCH_GRAPH") {
            Ok(p) => serde_json::from_slice(
                &gunzip(&std::fs::read(p).unwrap(), MAX_GRAPH_BYTES).unwrap(),
            )
            .unwrap(),
            Err(_) => synthetic(4_000),
        };
        let paths: Vec<(String, String)> = arr(&graph, "files")
            .filter_map(|f| {
                let folder = f["r"].as_str()?;
                let repo = graph["coderhelm"]["repos"][folder].as_str()?;
                let p = f["p"].as_str()?.strip_prefix(&format!("{folder}/"))?;
                Some((repo.to_string(), p.to_string()))
            })
            .collect();
        // The most the page asks for: 60 runs of 20 files, 200 findings of 2.
        let runs: Vec<WorkItem> = (0..60)
            .map(|i| WorkItem {
                repo: paths[i % paths.len()].0.clone(),
                ticket: format!("DOCK-{i}"),
                title: "Tidy the berths".into(),
                files: (0..20)
                    .map(|j| paths[(i * 31 + j * 7) % paths.len()].1.clone())
                    .collect(),
                ..Default::default()
            })
            .collect();
        let found: Vec<HealthItem> = (0..200)
            .map(|i| HealthItem {
                kind: "security".into(),
                id: format!("f{i}"),
                title: "An advisory".into(),
                severity: "P2".into(),
                repo: paths[i % paths.len()].0.clone(),
                files: if i % 4 == 0 {
                    vec![]
                } else {
                    vec![
                        (paths[(i * 13) % paths.len()].1.clone(), 3),
                        ("package.json".into(), 0),
                    ]
                },
                link: format!("/security/detail?id=f{i}"),
            })
            .collect();
        let stored = gzip(graph.to_string().as_bytes());
        let (view, index) = derived(&graph, "2026-11-21T00:00:00Z");

        // Before: the whole map read, the overlays placed on it, all of it sent.
        let t = Instant::now();
        let g: Value = serde_json::from_slice(&gunzip(&stored, MAX_GRAPH_BYTES).unwrap()).unwrap();
        let mut placed = 0;
        for w in &runs {
            placed += w
                .files
                .iter()
                .filter(|f| map_path(&g, &w.repo, f).is_some())
                .count();
        }
        for h in &found {
            placed += h
                .files
                .iter()
                .filter(|(f, _)| map_path(&g, &h.repo, f).is_some())
                .count();
        }
        let before_body = json!({"status": {}, "graph": for_viewer(with_overlays(g, Some(&runs), Some(&found)), "now")}).to_string();
        let before = t.elapsed();

        // Now: the index read, the overlays placed with it, an address sent.
        let t = Instant::now();
        let idx: MapIndex =
            serde_json::from_slice(&gunzip(&index, MAX_GRAPH_BYTES).unwrap()).unwrap();
        let o = overlays(&idx, Some(&runs), Some(&found));
        let after_body = json!({"status": {}, "graph": null, "graph_url": "https://bucket.s3.us-east-1.amazonaws.com/app-map/t/view.json.gz?X-Amz-Signature=0", "overlays": o}).to_string();
        let after = t.elapsed();

        eprintln!(
            "app map page view: {} files, {} placed; before {:?} and {} bytes sent; now {:?} and {} bytes sent; the map read by the browser: {} bytes gzipped, index {} bytes gzipped",
            arr(&graph, "files").count(),
            placed,
            before,
            before_body.len(),
            after,
            after_body.len(),
            view.len(),
            index.len(),
        );
        assert!(placed > 0);
        assert!(after_body.len() < 1_000_000, "{} bytes", after_body.len());
        assert!(
            index.len() * 4 < stored.len(),
            "the index is a fraction of the map"
        );
        // Generous for a debug build on a shared runner; a release build is
        // a few milliseconds.
        assert!(after.as_millis() < 1_000, "{after:?}");
        assert!(after < before);
        // The page lays the overlays over the copy and gets the same map.
        let view: Value = serde_json::from_slice(&gunzip(&view, MAX_GRAPH_BYTES).unwrap()).unwrap();
        let mut got = apply_overlays(view, &o);
        let mut want = for_viewer(
            with_overlays(graph, Some(&runs), Some(&found)),
            "2026-11-21T00:00:00Z",
        );
        for g in [&mut want, &mut got] {
            g["recent"]["updatedAt"] = json!("");
        }
        assert_eq!(got["health"], want["health"]);
        assert_eq!(got["recent"], want["recent"]);
    }

    #[test]
    fn the_page_gets_a_read_only_copy_without_the_sandbox_folder() {
        let g = for_viewer(harbor(), "2026-11-21T00:00:00Z");
        assert!(g["meta"]["root"].is_null());
        assert_eq!(g["meta"]["agent"], "none");
        assert_eq!(g["meta"]["exported"]["by"], "Coderhelm");
    }

    #[test]
    fn lookups_follow_the_map() {
        let g = harbor();
        let o = overview(&g);
        let parts = o["parts"].as_array().unwrap();
        assert_eq!(parts.iter().filter(|p| p["parent"].is_null()).count(), 8);
        assert_eq!(o["screen_count"], json!({"harbor-mobile": 3, "web": 1}));
        let p = part_view(&g, "api").unwrap();
        assert_eq!(p["endpoints"][0]["path"], "/slips/:id/bookings");
        assert!(p["links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["from"]["path"] == "harbor-web/lib/slips.ts" && l["kind"] == "http"));
        assert!(part_view(&g, "nope").is_none());
        let f = file_view(&g, "harbor-api/src/billing.js").unwrap();
        assert!(f["links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["to"]["label"] == "Stripe"));
        let s = screen_view(&g, "/slips/:id").unwrap();
        let steps: Vec<&str> = s["behind"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|b| b["step"].as_str())
            .collect();
        assert_eq!(steps.first(), Some(&"screen"));
        assert_eq!(steps.last(), Some(&"outside"));
        assert_eq!(search(&g, "book slip")["endpoints"][0]["method"], "POST");
        let parts = parts_of(&g, &["f:harbor-web/lib/slips.ts".into(), "r:0".into()]);
        assert_eq!(
            parts,
            BTreeSet::from(["api".to_string(), "web".to_string()])
        );
        let mini = sub_map(&g, &BTreeSet::from(["api".to_string()]));
        assert_eq!(mini["files"].as_array().unwrap().len(), 7);
        assert!(mini["links"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l["a"].as_str().unwrap().starts_with("f:harbor-api")));
    }

    /// What CoderHelm's detectors add reads plainly in the lookups: outside
    /// services say how they were found, infra says what it starts, links
    /// say where they were found, screens say their app and sign-in gate.
    #[test]
    fn lookups_show_services_infra_screens_and_where_each_was_found() {
        let g = harbor();
        // an outside service found only from env names: inferred, with its env and evidence
        let t = part_view(&g, "ext-tidewatch").unwrap();
        assert_eq!(t["part"]["how"], "env");
        assert_eq!(t["part"]["inferred"], true);
        assert_eq!(t["part"]["env"], json!(["TIDEWATCH_API_URL"]));
        assert_eq!(t["part"]["evidence"][0]["at"], "harbor-api/src/tides.js:1");
        let into = &t["links"][0];
        assert_eq!(into["from"]["path"], "harbor-api/src/tides.js");
        assert_eq!(into["kind"], "http");
        assert_eq!(into["evidence"][0]["how"], "inferred");
        // Stripe, drawn by the architecture job, gains how it was found and its callback
        let s = part_view(&g, "stripe").unwrap();
        assert_eq!(s["part"]["how"], "catalog");
        assert!(s["links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["kind"] == "webhook"
                && l["to"]["path"] == "harbor-api/src/webhooks.js"
                && l["endpoint"]["path"] == "/webhooks/stripe"));
        // infra: the cloud part lists what runs, and the bucket starts the receipt code
        let c = part_view(&g, "cloud").unwrap();
        let kids: Vec<&str> = c["children"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|k| k["resource"].as_str())
            .collect();
        assert!(
            kids.contains(&"lambda") && kids.contains(&"bucket"),
            "{kids:?}"
        );
        let f = file_view(&g, "harbor-api/src/receipts/index.js").unwrap();
        let links = f["links"].as_array().unwrap();
        let trig = links.iter().find(|l| l["kind"] == "trigger").unwrap();
        assert_eq!(trig["from"]["resource"], "bucket");
        assert_eq!(trig["runs"]["label"], "Harbor receipts function");
        assert_eq!(trig["inferred"], false);
        assert!(links.iter().any(|l| l["kind"] == "deploy"
            && l["evidence"].as_array().unwrap().iter().any(|e| e["at"]
                .as_str()
                .unwrap()
                .starts_with("harbor-api/.github/workflows/deploy-receipts.yml"))));
        // a nightly load writes the table an entity holds
        let e = file_view(&g, "harbor-api/src/entities/slip.entity.ts").unwrap();
        assert!(e["links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["kind"] == "data"
                && l["from"]["path"] == "harbor-infra/dags/nightly_slips.py"
                && l["tables"] == json!(["slip"])));
        // screens: the navigator file lists the screens it registers, with their gates
        let nav = file_view(&g, "harbor-mobile/app/navigation/RootNavigator.tsx").unwrap();
        let gates: Vec<(&str, &str)> = nav["screens"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| (s["name"].as_str().unwrap(), s["gate"].as_str().unwrap()))
            .collect();
        assert_eq!(
            gates,
            vec![
                ("SignIn", "signed_out"),
                ("SlipDetail", "signed_in"),
                ("Slips", "signed_in")
            ]
        );
        let sv = screen_view(&g, "screen:mobile:/(Stack)/SlipDetail").unwrap();
        assert_eq!(sv["screen"]["app"], "harbor-mobile");
        assert_eq!(
            sv["screen"]["evidence"][0]["at"],
            "harbor-mobile/app/navigation/RootNavigator.tsx:18"
        );
        let came_from = &sv["navigation"][0];
        assert_eq!(came_from["kind"], "nav");
        assert_eq!(
            came_from["from"]["path"],
            "harbor-mobile/app/screens/Slips.tsx"
        );
        assert_eq!(came_from["screen"]["name"], "SlipDetail");
        // search finds services by their env names and screens by their app
        assert_eq!(
            search(&g, "tidewatch_api_url")["parts"][0]["id"],
            "ext-tidewatch"
        );
        assert_eq!(
            search(&g, "harbor-mobile signin")["screens"][0]["gate"],
            "signed_out"
        );
    }

    /// A content editor's many document kinds never push an app's own
    /// screens (routes, the pages a route dispatches to) off the overview.
    #[test]
    fn the_overview_lists_app_screens_before_editor_kinds() {
        let mut g = harbor();
        let kinds: Vec<Value> = (0..90)
            .map(|i| json!({"id": format!("screen:studio:/(structure)/k{i}"), "url": format!("/(structure)/k{i}"), "name": format!("Kind {i}"), "kind": "page-kind", "file": "harbor-web/lib/slips.ts", "behind": []}))
            .collect();
        let mut all = kinds;
        all.extend(g["screens"].as_array().unwrap().iter().cloned());
        all.push(json!({"id": "screen:web:/(notice)/*", "url": "/(notice)/*", "name": "Notice page", "kind": "screen", "fw": "dispatch", "file": "harbor-web/lib/slips.ts", "behind": [],
            "nav": {"app": "harbor-web", "dispatcher": "harbor-web/app/slips/[id]/page.tsx", "type": "notice", "renderer": "NoticeRenderer", "urlFrom": "route", "evidence": [{"file": "harbor-web/lib/slips.ts", "line": 2}]}}));
        g["screens"] = json!(all);
        let o = overview(&g);
        let listed = o["screens"].as_array().unwrap();
        assert_eq!(listed.len(), 80);
        assert!(listed.iter().any(|s| s["url"] == "/slips/:id"));
        let notice = listed.iter().find(|s| s["name"] == "Notice page").unwrap();
        assert_eq!(
            notice["dispatch"]["renderer_at"],
            "harbor-web/lib/slips.ts:2"
        );
        assert_eq!(
            notice["dispatch"]["route_file"],
            "harbor-web/app/slips/[id]/page.tsx"
        );
        // a screen nothing dispatches carries no dispatch record
        let slip = listed.iter().find(|s| s["url"] == "/slips/:id").unwrap();
        assert!(slip.get("dispatch").is_none());
    }

    /// The harbor map with one journey through the booking.
    pub(crate) fn harbor_with_journey() -> Value {
        let mut g = harbor();
        g["flows"] = json!([{
            "id": "book-a-slip", "title": "Book a slip", "description": "",
            "steps": [
                {"ref": "f:harbor-web/app/slips/[id]/page.tsx", "note": "The boater opens a slip.", "evidence": [{"file": "harbor-web/app/slips/[id]/page.tsx", "line": 3}]},
                {"ref": "r:0", "note": "The page asks to book it.", "evidence": [{"file": "harbor-web/lib/slips.ts", "line": 2}, {"file": "harbor-api/src/bookings.js", "line": 5}]},
                {"ref": "n:stripe", "note": "Stripe charges the card.", "evidence": [{"file": "harbor-api/src/billing.js", "line": 1}]},
            ],
        }]);
        g
    }

    /// Every `at` in an answer, wherever it is.
    fn cites_in(v: &Value) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![v];
        while let Some(x) = stack.pop() {
            match x {
                Value::Object(o) => {
                    match o.get("at") {
                        Some(Value::String(s)) => out.push(s.clone()),
                        Some(Value::Array(a)) => {
                            out.extend(a.iter().filter_map(|s| s.as_str().map(str::to_string)))
                        }
                        _ => {}
                    }
                    stack.extend(o.values());
                }
                Value::Array(a) => stack.extend(a.iter()),
                _ => {}
            }
        }
        out
    }

    /// An endpoint other repositories call: its callers there with their
    /// lines, the screens on it with their app and gate, the journey through
    /// it; every citation a real place on the map.
    #[test]
    fn impact_of_an_endpoint_names_its_callers_elsewhere() {
        let g = harbor_with_journey();
        let v = impact(&g, "POST /slips/:id/bookings").unwrap();
        assert_eq!(v["target"]["kind"], "endpoint");
        assert_eq!(v["target"]["at"], "harbor-api/src/bookings.js:5");
        let callers: Vec<(&str, &str)> = v["callers_elsewhere"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| (c["from"].as_str().unwrap(), c["at"].as_str().unwrap()))
            .collect();
        assert_eq!(
            callers,
            [
                (
                    "harbor-mobile/app/screens/SlipDetail.tsx",
                    "harbor-mobile/app/screens/SlipDetail.tsx:6"
                ),
                ("harbor-web/lib/slips.ts", "harbor-web/lib/slips.ts:2"),
            ]
        );
        assert_eq!(v["other_repos"], json!(["harbor-mobile", "harbor-web"]));
        // An import of the endpoint's file is not a call to it.
        assert_eq!(v["callers_here"], json!([]));
        let screens = v["screens"].as_array().unwrap();
        let detail = screens
            .iter()
            .find(|s| s["id"] == "screen:mobile:/(Stack)/SlipDetail")
            .unwrap();
        assert_eq!(detail["app"], "harbor-mobile");
        assert_eq!(detail["gate"], "signed_in");
        assert_eq!(
            detail["at"],
            "harbor-mobile/app/navigation/RootNavigator.tsx:18"
        );
        assert!(screens.iter().any(|s| s["id"] == "screen:web:/slips/:id"));
        assert_eq!(v["journeys"][0]["id"], "book-a-slip");
        assert_eq!(v["journeys"][0]["steps"], json!([2]));
        // Stripe is reached through billing.js, which bookings.js imports.
        let stripe = v["outside_services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|o| o["id"] == "n:stripe")
            .unwrap();
        assert_eq!(stripe["direction"], "reaches");
        assert_eq!(stripe["at"], "harbor-api/src/billing.js:1");
        for c in cites_in(&v) {
            assert!(real_cite(&g, "", &c), "{c} isn't a real place");
        }
        // The same endpoint by its id; a path alone; nothing for a stranger.
        assert_eq!(
            impact(&g, "r:0").unwrap()["callers_elsewhere"],
            v["callers_elsewhere"]
        );
        assert_eq!(
            impact(&g, "/slips/:id/bookings").unwrap()["target"]["kind"],
            "endpoint"
        );
        assert!(impact(&g, "DELETE /slips/:id/bookings").is_none());
        assert!(impact(&g, "nowhere.js").is_none());
    }

    /// A file behind a screen and a journey: the screen with its gate, the
    /// journey's step, and what the file reaches.
    #[test]
    fn impact_of_a_file_behind_a_screen_and_a_journey() {
        let g = harbor_with_journey();
        let v = impact(&g, "harbor-web/lib/slips.ts").unwrap();
        assert_eq!(v["target"]["kind"], "file");
        let s = &v["screens"][0];
        assert_eq!(s["id"], "screen:web:/slips/:id");
        assert_eq!(s["at"], "harbor-web/app/slips/[id]/page.tsx:1");
        assert_eq!(v["journeys"][0]["id"], "book-a-slip");
        assert_eq!(v["journeys"][0]["steps"], json!([2]));
        assert!(v["journeys"][0]["at"]
            .as_array()
            .unwrap()
            .contains(&json!("harbor-web/lib/slips.ts:2")));
        // The page that imports it is its caller in the same repository.
        assert_eq!(
            v["callers_here"][0]["from"],
            "harbor-web/app/slips/[id]/page.tsx"
        );
        // The mobile sign-in screen is registered by the navigator file.
        let nav = impact(&g, "harbor-mobile/app/navigation/RootNavigator.tsx").unwrap();
        let gates: Vec<(&str, &str)> = nav["screens"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| (s["name"].as_str().unwrap(), s["gate"].as_str().unwrap()))
            .collect();
        assert!(gates.contains(&("SignIn", "signed_out")));
        assert!(gates.contains(&("Slips", "signed_in")));
        // The table a nightly job writes.
        let e = impact(&g, "harbor-api/src/entities/slip.entity.ts").unwrap();
        assert_eq!(e["data"][0]["tables"], json!(["slip"]));
        assert_eq!(e["data"][0]["at"], "harbor-infra/dags/nightly_slips.py:6");
        for c in cites_in(&v)
            .into_iter()
            .chain(cites_in(&nav))
            .chain(cites_in(&e))
        {
            assert!(real_cite(&g, "", &c), "{c} isn't a real place");
        }
    }

    /// The code a Lambda runs: the function that deploys it and the bucket
    /// whose uploads start it, each where the infrastructure says so.
    #[test]
    fn impact_of_a_lambda_names_what_deploys_and_starts_it() {
        let g = harbor();
        let v = impact(&g, "harbor-api/src/receipts/index.js").unwrap();
        let cloud = v["cloud"].as_array().unwrap();
        let deploy = cloud.iter().find(|c| c["how"] == "deploy").unwrap();
        assert_eq!(deploy["id"], "n:infra-harbor-receipts-function");
        assert_eq!(deploy["resource"], "lambda");
        assert_eq!(deploy["at"], "harbor-infra/main.tf:5");
        let trigger = cloud.iter().find(|c| c["how"] == "trigger").unwrap();
        assert_eq!(trigger["id"], "n:infra-bucket-uploads");
        assert_eq!(trigger["labels"], json!(["s3:ObjectCreated:*"]));
        assert_eq!(trigger["starts"], "Harbor receipts function");
        assert_eq!(trigger["at"], "harbor-infra/main.tf:28");
        assert_eq!(v["outside_services"][0]["label"], "Amazon S3");
        // A webhook endpoint: the outside service that calls in.
        let w = impact(&g, "POST /webhooks/stripe").unwrap();
        let s = &w["outside_services"].as_array().unwrap();
        assert!(s
            .iter()
            .any(|o| o["id"] == "n:stripe" && o["direction"] == "calls in"));
        // A part: everything in it.
        let p = impact(&g, "api").unwrap();
        assert_eq!(p["target"]["kind"], "part");
        assert_eq!(p["endpoints"].as_array().unwrap().len(), 2);
        assert_eq!(p["other_repos"], json!(["harbor-mobile", "harbor-web"]));
        // The lookup, its label and its place among the tools.
        let mut l = Lookups::new(g);
        let (found, label) = l.lookup("map_impact", &json!({"target": "POST /slips/:id/bookings"}));
        assert!(found.get("error").is_none());
        assert_eq!(
            label,
            "Looked up what changing POST /slips/:id/bookings reaches on the app map"
        );
        assert!(l.parts().contains(&"api".to_string()));
        assert!(l.lookup("map_impact", &json!({"target": "x"})).0["error"].is_string());
        assert!(is_tool("map_impact"));
        assert!(tool_defs().iter().any(
            |d| d["name"] == "map_impact" && d["input_schema"]["required"] == json!(["target"])
        ));
    }

    /// A citation is a file on the map and a line inside it.
    #[test]
    fn real_citations_are_files_and_lines_on_the_map() {
        let g = harbor();
        assert!(real_cite(&g, "", "harbor-api/src/bookings.js:5"));
        assert!(real_cite(&g, "harbor/harbor-api", "src/bookings.js:5"));
        assert!(real_cite(&g, "", "harbor/harbor-api/src/bookings.js:5"));
        assert!(!real_cite(&g, "", "harbor-api/src/bookings.js:9999"));
        assert!(!real_cite(&g, "", "harbor-api/src/nowhere.js:1"));
        assert!(!real_cite(&g, "", "harbor-api/src/bookings.js"));
        assert!(!real_cite(&g, "", "harbor-api/src/bookings.js:0"));
    }

    /// What the Knowledge page's card counts, from the map at build time.
    #[test]
    fn coverage_counts_screens_services_cloud_and_repos() {
        let c = MapIndex::of(&harbor()).coverage;
        assert!(c.counted);
        assert_eq!(
            c.screens,
            BTreeMap::from([("harbor-mobile".into(), 3), ("web".into(), 1)])
        );
        assert_eq!(c.outside_services, 4);
        assert_eq!(c.cloud_links, 2);
        assert_eq!(c.repos, 4);
        // An index written before the counts reads as not counted.
        let old: MapIndex = serde_json::from_value(json!({
            "repos": {}, "files": [], "repo_parts": {}, "issues": [], "has_health": false
        }))
        .unwrap();
        assert!(!old.coverage.counted);
    }

    /// A journey: found by id or title words, each step described with
    /// what happens there and the lines it cites; listed in the overview
    /// and search; kept on a mini map that holds every step.
    #[test]
    fn a_journey_is_walked_step_by_step() {
        let mut g = harbor();
        g["flows"] = json!([{
            "id": "book-a-slip", "title": "Book a slip", "description": "", "custom": true,
            "coderhelm": {"linked": [true, true, false]},
            "steps": [
                {"ref": "f:harbor-web/app/slips/[id]/page.tsx", "note": "The boater opens a slip.", "evidence": [{"file": "harbor-web/app/slips/[id]/page.tsx", "line": 3}]},
                {"ref": "r:0", "note": "The page asks to book it.", "evidence": [{"file": "harbor-web/lib/slips.ts", "line": 2}, {"file": "harbor-api/src/bookings.js", "line": 5}]},
                {"ref": "n:stripe", "note": "Stripe charges the card.", "evidence": [{"file": "harbor-api/src/billing.js", "line": 1}]},
            ],
        }]);
        for q in ["book-a-slip", "book slip", "Book a SLIP"] {
            let v = flow_view(&g, q).unwrap();
            assert_eq!(v["journey"]["title"], "Book a slip", "{q}");
        }
        assert!(flow_view(&g, "sign in").is_none());
        assert!(flow_view(&g, "  ").is_none());
        let v = flow_view(&g, "book-a-slip").unwrap();
        let steps = v["steps"].as_array().unwrap();
        assert_eq!(steps[1]["method"], "POST");
        assert_eq!(
            steps[1]["cites"],
            json!(["harbor-web/lib/slips.ts:2", "harbor-api/src/bookings.js:5"])
        );
        assert_eq!(steps[2]["label"], "Stripe");
        assert_eq!(steps[2]["inferred"], true);
        assert!(steps[1].get("inferred").is_none());
        assert_eq!(
            overview(&g)["journeys"],
            json!([{"id": "book-a-slip", "title": "Book a slip", "steps": 3}])
        );
        assert_eq!(search(&g, "slip")["journeys"][0]["id"], "book-a-slip");
        let mut b = Lookups::new(g.clone());
        let (_, label) = b.lookup("map_flow", &json!({"journey": "book a slip"}));
        assert_eq!(
            label,
            "Walked through the journey book a slip on the app map"
        );
        assert_eq!(b.parts(), vec!["api", "stripe", "web"]);
        // On a mini map that holds every step it stays; otherwise it goes.
        let all = sub_map(
            &g,
            &BTreeSet::from(["api".into(), "web".into(), "stripe".into()]),
        );
        assert_eq!(all["flows"][0]["steps"][1]["ref"], "r:0");
        let api = sub_map(&g, &BTreeSet::from(["api".to_string()]));
        assert_eq!(api["flows"], json!([]));
        assert!(TOOLS.contains(&"map_flow") && tool_defs().iter().any(|d| d["name"] == "map_flow"));
    }

    /// The lookups Chat and /mcp share: they answer from the map, register
    /// each answer as a source that links to the page focused on its parts,
    /// and draw the parts they touched.
    #[test]
    fn the_lookups_answer_from_the_map_cite_it_and_draw_what_they_touched() {
        let mut b = Lookups::new(harbor());
        let mut reg = crate::knowledge::answer::Registry::default();
        let (text, label, failed) =
            b.cite(&mut reg, "map_screen", &json!({"screen": "/slips/:id"}));
        assert!(!failed && text.starts_with("[S1] "), "{text}");
        assert_eq!(label, "Followed the screen /slips/:id on the app map");
        let s = reg.get("S1").unwrap();
        assert_eq!(s.kind, SOURCE_KIND);
        assert_eq!(s.href.as_deref(), Some("/app-map?focus=api,stripe,web"));
        let (v, _) = b.lookup("map_screen", &json!({"screen": "/slips/:id"}));
        assert_eq!(
            v["behind"].as_array().unwrap().last().unwrap()["step"],
            "outside"
        );
        // Nothing found is no source.
        let (_, _, failed) = b.cite(&mut reg, "map_part", &json!({"id": "nope"}));
        assert!(failed);
        assert_eq!(reg.len(), 1);
        let mini = b.mini_map("2026-11-21T00:00:00Z").unwrap();
        let parts: Vec<&str> = mini["graph"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|n| n["id"].as_str())
            .collect();
        assert_eq!(parts, vec!["web", "api", "stripe"]);
        assert_eq!(mini["parts"], json!(["api", "stripe", "web"]));
        assert_eq!(mini["graph"]["meta"]["exported"]["by"], "Coderhelm");
        // The overview and a search touch no part: no mini map.
        let mut b = Lookups::new(harbor());
        b.lookup("map_overview", &json!({}));
        b.lookup("map_find", &json!({"query": "slip"}));
        assert!(b.mini_map("now").is_none());
        let names: Vec<String> = tool_defs()
            .iter()
            .filter_map(|d| d["name"].as_str().map(str::to_string))
            .collect();
        assert_eq!(names, TOOLS);
        assert!(is_tool("map_part") && !is_tool("search_code"));
        assert_eq!(page_link(&[]), "/app-map");
        assert_eq!(
            page_link(&["a b".into(), "c".into()]),
            "/app-map?focus=ab,c"
        );
    }

    #[test]
    fn answers_are_checked_against_the_workflows_schemas() {
        let wf: Value = serde_json::from_str(WORKFLOW_JSON).unwrap();
        let arch: Value = serde_json::from_str(include_str!(
            "../../worker/floorplan/architecture.harbor.json"
        ))
        .unwrap();
        assert_eq!(
            check_answer(&wf["schemas"]["architecture"], &arch),
            Vec::<String>::new()
        );
        let bad = json!({"project": {}, "nodes": [{"id": 3}], "extra": true});
        let problems = check_answer(&wf["schemas"]["architecture"], &bad);
        assert!(
            problems
                .iter()
                .any(|p| p.contains("project.name is missing")),
            "{problems:?}"
        );
        assert!(problems.iter().any(|p| p.contains("extra is not allowed")));
        let desc: Value = serde_json::from_str(include_str!(
            "../../worker/floorplan/descriptions.harbor.json"
        ))
        .unwrap();
        for d in desc.as_object().unwrap().values() {
            assert!(check_answer(&wf["schemas"]["descriptions"], d).is_empty());
        }
        // CoderHelm's recent work fits Floorplan's recent-work answer.
        let g = with_overlays(
            harbor(),
            Some(&[WorkItem {
                repo: "harbor/harbor-api".into(),
                title: "Tidy the server".into(),
                files: vec!["src/server.js".into()],
                ..Default::default()
            }]),
            None,
        );
        let as_answer = json!({"tasks": g["recent"]["tasks"].as_array().unwrap().iter().map(|t| json!({
            "title": t["title"], "request": t["request"], "summary": t["summary"],
            "items": [{"ref": "harbor-api/src/server.js", "change": "changed", "note": "Tidied."}]
        })).collect::<Vec<_>>()});
        assert_eq!(
            check_answer(&wf["schemas"]["recent_work"], &as_answer),
            Vec::<String>::new()
        );
        assert!(FLOORPLAN_VERSION.starts_with("floorplan--v"));
    }
}
