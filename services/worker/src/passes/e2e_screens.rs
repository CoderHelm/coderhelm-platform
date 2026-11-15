//! The Before and After table of CoderHelm's own pull request, filled from
//! the screenshots its end-to-end run took (the rules are
//! `common::e2e::shots`).
//!
//! When a run on the pull request's head is read (`e2e::ingest`), the
//! tests that show the change are chosen ([`shots::pick`]), and the first
//! one with a screenshot on the head is kept with its screenshot from the
//! newest passing run on the pull request's base branch, when one is kept
//! there too.
//! The images go to the install's own bucket, the record to the settings
//! table for the head, and the description is rebuilt: its table links each
//! image through the API with a signed link that expires
//! ([`screens_for`]). A head keeps the first pair found: each run keeps its
//! images under its own keys, then saves the head's record only when none
//! is there, so a later or concurrent run on the same head changes nothing.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::e2e::coverage::TestMap;
use common::e2e::messages::TestIngest;
use common::e2e::model::ParsedTest;
use common::e2e::report_source::Location;
use common::e2e::shots::{self, Image, Kept, Side};
use common::e2e::store::Store;
use tracing::{info, warn};

use super::test_fix::{CiReports, KeptRun, Reports, S3Reports};
use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// Tests tried for a screenshot on the head.
const MAX_TRIED: usize = 3;

fn record_sk(repo: &str, sha: &str) -> String {
    format!("E2E_SHOTS#{repo}#{sha}")
}

pub async fn load(state: &WorkerState, team_id: &str, repo: &str, sha: &str) -> Option<Kept> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s(&record_sk(repo, sha)))
        .send()
        .await
        .ok()?
        .item?;
    serde_json::from_str(item.get("data")?.as_s().ok()?).ok()
}

/// Saves the record of a head unless one is there; whether this one is.
/// The record lives as long as its links work.
async fn claim(state: &WorkerState, team_id: &str, repo: &str, sha: &str, k: &Kept) -> bool {
    match state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", super::attr_s(team_id))
        .item("sk", super::attr_s(&record_sk(repo, sha)))
        .item(
            "data",
            super::attr_s(&serde_json::to_string(k).unwrap_or_default()),
        )
        .item("ttl", AttributeValue::N(k.expires_at.to_string()))
        .condition_expression("attribute_not_exists(sk)")
        .send()
        .await
    {
        Ok(_) => true,
        Err(e)
            if e.as_service_error()
                .is_some_and(|s| s.is_conditional_check_failed_exception()) =>
        {
            info!(repo, "Another run on this head kept its screenshots first");
            false
        }
        Err(e) => {
            warn!(error = %e, repo, "Could not save the end-to-end screenshots of a head");
            false
        }
    }
}

/// The Before and After table for a pull request's head, when a run on it
/// kept screenshots. `None` without a signing secret or a public API
/// address: GitHub fetches the images from the API, so a link must reach
/// it.
pub async fn screens_for(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    sha: &str,
) -> Option<common::pr_body::Screens> {
    let secret = state.secrets.jwt_secret.as_str();
    let api = common::public_urls::api_url(&state.config.stage);
    if secret.is_empty() || !api.starts_with("https://") {
        return None;
    }
    let k = load(state, team_id, repo, sha).await?;
    Some(shots::screens(&k, |key| {
        shots::url(api, &shots::sign(secret, team_id, key, k.expires_at))
    }))
}

/// Report files held in memory: an uploaded report, or the raw reports kept
/// in the install's bucket for an upload.
struct Held(Vec<(String, Vec<u8>)>);

#[async_trait::async_trait]
impl Reports for Held {
    fn label(&self, _run_id: u64, _attempt: u32) -> String {
        "the uploaded report".into()
    }

    async fn runs(&self) -> Vec<KeptRun> {
        Vec::new()
    }

    async fn reports(&self, _run_id: u64, _attempt: u32) -> Result<Vec<(String, Vec<u8>)>, String> {
        Ok(self.0.clone())
    }

    async fn file(&self, _run_id: u64, _attempt: u32, path: &str) -> Option<Vec<u8>> {
        self.0
            .iter()
            .find(|(n, _)| n == path)
            .map(|(_, b)| b.clone())
    }

    async fn names(&self, _run_id: u64, _attempt: u32) -> Vec<String> {
        self.0.iter().map(|(n, _)| n.clone()).collect()
    }
}

/// Where one run's reports are read.
enum Source<'a> {
    Held(Held),
    S3(Box<S3Reports>),
    Ci(CiReports<'a>),
}

impl Source<'_> {
    fn reports(&self) -> &dyn Reports {
        match self {
            Source::Held(r) => r,
            Source::S3(r) => r.as_ref(),
            Source::Ci(r) => r,
        }
    }
}

/// The raw reports kept in the install's bucket for an uploaded run
/// (`e2e::ingest` keeps them within a budget).
async fn kept_raw(state: &WorkerState, team_id: &str, repo: &str, run_key: &str) -> Held {
    let marker = common::e2e::store::raw_report_key(team_id, repo, run_key, "x");
    let prefix = marker.trim_end_matches('x').to_string();
    let mut out = Vec::new();
    let Ok(listed) = state
        .s3
        .list_objects_v2()
        .bucket(&state.config.bucket_name)
        .prefix(&prefix)
        .max_keys(20)
        .send()
        .await
    else {
        return Held(out);
    };
    for o in listed.contents() {
        let (Some(key), size) = (o.key(), o.size().unwrap_or(0)) else {
            continue;
        };
        if size < 0 || size as usize > common::e2e::parse::MAX_REPORT_BYTES {
            continue;
        }
        let Ok(obj) = state
            .s3
            .get_object()
            .bucket(&state.config.bucket_name)
            .key(key)
            .send()
            .await
        else {
            continue;
        };
        if let Ok(body) = obj.body.collect().await {
            let name = key.strip_prefix(&prefix).unwrap_or(key).to_string();
            out.push((name, body.into_bytes().to_vec()));
        }
    }
    Held(out)
}

/// `{run_id}-{attempt}`, the key of a run read from GitHub.
fn run_of(run_key: &str) -> Option<(u64, u32)> {
    let (run, attempt) = run_key.split_once('-')?;
    Some((run.parse().ok()?, attempt.parse().ok()?))
}

/// The screenshots of `wanted` (test ids) in one run, by test id.
async fn images(
    r: &dyn Reports,
    run_id: u64,
    attempt: u32,
    wanted: &[String],
) -> HashMap<String, Vec<u8>> {
    let mut out = HashMap::new();
    let Ok(files) = r.reports(run_id, attempt).await else {
        return out;
    };
    let mut names: Option<Vec<String>> = None;
    for (name, bytes) in &files {
        for s in shots::shots_in(name, bytes) {
            if !wanted.contains(&s.test_id) {
                continue;
            }
            let image = match s.image {
                Image::Inline(b) => Some(b),
                Image::File(p) => {
                    if names.is_none() {
                        names = Some(r.names(run_id, attempt).await);
                    }
                    match shots::resolve(name, &p, names.as_deref().unwrap_or_default()) {
                        Some(n) => r.file(run_id, attempt, &n).await,
                        None => None,
                    }
                }
            };
            if let Some(b) =
                image.filter(|b| b.len() <= shots::MAX_IMAGE && shots::image_type(b).is_some())
            {
                out.insert(s.test_id, b);
            }
        }
    }
    out
}

/// Keeps one image in the install's bucket under `key`; whether it was.
async fn put(state: &WorkerState, key: &str, bytes: Vec<u8>) -> bool {
    let Some(content_type) = shots::image_type(&bytes) else {
        return false;
    };
    match state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(key)
        .content_type(content_type)
        .body(bytes.into())
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(error = %e, "Could not keep an end-to-end screenshot");
            false
        }
    }
}

/// Everything a run on a pull request's head brings for its table.
pub struct Head<'a> {
    pub msg: &'a TestIngest,
    /// This run's key in the store.
    pub run_key: &'a str,
    pub pr: u64,
    pub tests: &'a [ParsedTest],
    /// The reports this run was read from (an upload's report).
    pub reports: &'a [(String, Vec<u8>)],
    /// The files the pull request changes.
    pub changed: &'a [String],
    pub map: Option<&'a TestMap>,
}

/// Keeps the screenshots that show the change of CoderHelm's own pull
/// request from one run on its head, and rebuilds its description with
/// them. `scoped` is a test's id in the store.
pub async fn keep(
    state: &WorkerState,
    gh: &GitHubClient,
    st: &Store<'_>,
    h: Head<'_>,
    scoped: impl Fn(&ParsedTest) -> String,
) {
    let msg = h.msg;
    let (owner, name) = (msg.repo_owner.as_str(), msg.repo_name.as_str());
    let repo = format!("{owner}/{name}");
    if state.secrets.jwt_secret.is_empty() || msg.head_sha.is_empty() {
        return;
    }
    let picked = shots::pick(h.tests, h.changed, h.map);
    if picked.is_empty()
        || load(state, &msg.team_id, &repo, &msg.head_sha)
            .await
            .is_some()
    {
        return;
    }
    let location = st
        .settings(&msg.team_id)
        .await
        .ok()
        .and_then(|s| s.report_source(&repo).and_then(|r| r.location().ok()));
    let location = location.as_ref();
    // Where the team keeps the repository's reports, else the run's
    // artifacts: the same places the failure attribution reads.
    let open = |run_id: u64| async move {
        if let Some(l @ Location::S3 { .. }) = location {
            let mut tried = Vec::new();
            if let Some(r) = super::test_fix::open_s3(state, &msg.team_id, l, &mut tried).await {
                return Source::S3(Box::new(r));
            }
        }
        Source::Ci(CiReports::new(gh, owner, name, vec![(run_id, 1)]))
    };
    // The head's run.
    let head = if msg.upload_key.is_some() {
        Source::Held(Held(h.reports.to_vec()))
    } else {
        open(msg.run_id).await
    };
    let tried: Vec<&(&ParsedTest, shots::Why)> = picked.iter().take(MAX_TRIED).collect();
    let ids: Vec<String> = tried.iter().map(|(t, _)| t.id()).collect();
    let mut after = images(head.reports(), msg.run_id, msg.run_attempt, &ids).await;
    let Some((t, why, after_bytes)) = tried
        .into_iter()
        .find_map(|(t, w)| after.remove(&t.id()).map(|b| (*t, w, b)))
    else {
        info!(repo = %repo, pr = h.pr, tests = ids.len(), "No screenshot on the head for the tests that show the change");
        return;
    };
    // The same test on the branch the pull request goes into: its newest
    // passing run there.
    let Some(base) = gh
        .get_pull_request(owner, name, h.pr)
        .await
        .ok()
        .and_then(|p| p["base"]["ref"].as_str().map(str::to_string))
        .filter(|b| !b.is_empty())
    else {
        warn!(repo = %repo, pr = h.pr, "Could not read the pull request's base branch; no screenshots kept");
        return;
    };
    let newest = st
        .record(&msg.team_id, &scoped(t))
        .await
        .ok()
        .flatten()
        .and_then(|r| shots::newest_passing(&r.history, &base).cloned());
    let mut before: Option<(Vec<u8>, String)> = None;
    if let Some(e) = newest {
        let (src, run_id, attempt, run_url) = if e.run_key.starts_with("up-") {
            (
                Source::Held(kept_raw(state, &msg.team_id, &repo, &e.run_key).await),
                0,
                1,
                String::new(),
            )
        } else if let Some((run_id, attempt)) = run_of(&e.run_key) {
            (
                open(run_id).await,
                run_id,
                attempt,
                format!("https://github.com/{repo}/actions/runs/{run_id}"),
            )
        } else {
            (Source::Held(Held(Vec::new())), 0, 1, String::new())
        };
        before = images(src.reports(), run_id, attempt, &[t.id()])
            .await
            .remove(&t.id())
            .map(|b| (b, run_url));
    }
    let key_of = |side: &str, bytes: &[u8]| {
        shots::image_type(bytes).map(|ct| {
            shots::object_key(
                &msg.team_id,
                &repo,
                h.pr,
                &msg.head_sha,
                h.run_key,
                &t.id(),
                side,
                shots::extension(ct),
            )
        })
    };
    let Some(after_key) = key_of("after", &after_bytes) else {
        return;
    };
    let before = before.and_then(|(b, run_url)| key_of("before", &b).map(|key| (b, key, run_url)));
    let kept = Kept {
        pr: h.pr,
        test: t.full_title(),
        why: why.words(),
        base,
        before: before.as_ref().map(|(_, key, run_url)| Side {
            key: key.clone(),
            run_url: run_url.clone(),
        }),
        after: Side {
            key: after_key.clone(),
            run_url: msg.html_url.clone(),
        },
        expires_at: chrono::Utc::now().timestamp() + shots::LINK_VALID_SECS,
    };
    if !put(state, &after_key, after_bytes).await {
        return;
    }
    if let Some((b, key, _)) = before {
        if !put(state, &key, b).await {
            return;
        }
    }
    if !claim(state, &msg.team_id, &repo, &msg.head_sha, &kept).await {
        return;
    }
    info!(repo = %repo, pr = h.pr, test = %kept.test, before = kept.before.is_some(), "End-to-end screenshots kept for the Before and After table");
    if let super::description::Reconciled::Failed(why) =
        super::description::rebuild_pr(state, gh, &msg.team_id, owner, name, h.pr).await
    {
        warn!(repo = %repo, pr = h.pr, why, "The description was not rebuilt with the screenshots; the next rebuild shows them");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_github_run_key_names_its_run_and_attempt() {
        assert_eq!(run_of("123-2"), Some((123, 2)));
        assert_eq!(run_of("up-1-1-abcdef"), None);
        assert_eq!(run_of("nope"), None);
    }

    #[tokio::test]
    async fn an_attachment_is_read_from_the_file_its_report_names() {
        let png = vec![0x89, b'P', b'N', b'G', 0, 1];
        let report = serde_json::json!({
            "config": {},
            "suites": [{"title": "a.spec.ts", "file": "a.spec.ts", "specs": [
                {"title": "shows it", "file": "a.spec.ts", "tests": [{"results": [
                    {"status": "passed", "attachments": [
                        {"name": "screenshot", "contentType": "image/png",
                         "path": "/runner/work/test-results/a-shows-it/test-finished-1.png"}]}]}]}
            ]}]
        });
        let held = Held(vec![
            (
                "pw/results.json".into(),
                serde_json::to_vec(&report).unwrap(),
            ),
            (
                "pw/test-results/a-shows-it/test-finished-1.png".into(),
                png.clone(),
            ),
        ]);
        let tests = common::e2e::parse::parse_any(
            "results.json",
            &serde_json::to_vec(&report).unwrap(),
            &Default::default(),
        )
        .unwrap();
        let id = tests[0].id();
        let got = images(&held, 1, 1, std::slice::from_ref(&id)).await;
        assert_eq!(got.get(&id), Some(&png));
        // A test nobody asked for is not read.
        assert!(images(&held, 1, 1, &["other".into()]).await.is_empty());
    }
}
