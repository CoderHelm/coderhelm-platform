//! Where E2E results are kept: one DynamoDB table (items below) plus run
//! details and raw reports in the data bucket.
//!
//! | pk | sk | what |
//! |---|---|---|
//! | `T#{team}` | `SETTINGS` | the team's settings |
//! | `T#{team}` | `REPO#{repo}` | a repo with results: default branch, installation |
//! | `REPO#{repo}` | `TEAM#{team}` | reverse lookup for CI calls signed by GitHub |
//! | `T#{team}#RUNS#{repo}` | `{started_at}#{run_key}` | a run (expires) |
//! | `T#{team}#RUNKEY` | `{run_key}` | which repo a run belongs to; claims ingestion once; what reading it found |
//! | `T#{team}#QNOTE` | `{repo}#{pr}` | the PR comment that says only quarantined tests failed |
//! | `T#{team}#TESTS` | `{test id}` | a test's summary (the dashboard's list) |
//! | `T#{team}#TEST#{id}` | `RECORD` | a test's full record: history, events |
//! | `T#{team}#SIGS` | `{repo}#{signature}` | a failure group |
//! | `T#{team}#RECS` | `{id}` | a test recommendation |
//! | `T#{team}#PR#{repo}#{pr}` | `{sha}` | E2E triage for a PR head (expires) |
//! | `T#{team}` | `COVERAGE#{repo}` | areas, their tests and untested routes |
//! | `T#{team}` | `CI#{repo}` | results and quarantine calls seen per workflow |
//! | `T#{team}` | `SETUP#{repo}` | the setup pull request and the last workflow check |
//!
//! Every item keeps its payload as JSON in `data`.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};

use super::ci_setup::{CiState, SetupPr};
use super::model::{IngestOutcome, Recommendation, Run, Settings, TestRecord};

/// Runs, run keys and PR triage expire after this.
pub const RETENTION_DAYS: i64 = 120;

type Item = HashMap<String, AttributeValue>;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn ttl(days: i64) -> AttributeValue {
    AttributeValue::N(
        (chrono::Utc::now() + chrono::Duration::days(days))
            .timestamp()
            .to_string(),
    )
}

fn data<T: DeserializeOwned>(item: &Item) -> Option<T> {
    item.get("data")
        .and_then(|v| v.as_s().ok())
        .and_then(|d| serde_json::from_str(d).ok())
}

fn team_pk(team: &str) -> String {
    format!("T#{team}")
}

/// Bucket key for a run's details.
pub fn run_detail_key(team: &str, repo: &str, run_key: &str) -> String {
    format!("e2e/{}/{}/runs/{run_key}.json", sanitize(team), repo)
}

/// Bucket key for a raw report kept for reference.
pub fn raw_report_key(team: &str, repo: &str, run_key: &str, name: &str) -> String {
    format!(
        "e2e/{}/{}/raw/{run_key}/{}",
        sanitize(team),
        repo,
        safe_file_name(name)
    )
}

/// A file name with anything but letters, digits, `.`, `_` and `-` replaced.
pub fn safe_file_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "._-".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Bucket key for a report uploaded from CI.
pub fn upload_key(team: &str, repo: &str, name: &str) -> String {
    format!(
        "e2e/{}/{}/uploads/{}/{}",
        sanitize(team),
        repo,
        ulid::Ulid::new(),
        safe_file_name(name)
    )
}

fn sanitize(team: &str) -> String {
    team.replace('#', "_")
}

/// The outcome of the newest attempt among a run's claim items (`{run_id}-{attempt}`).
/// A claim without a result is still being read.
fn newest_outcome(items: &[Item], run_id: u64) -> Option<IngestOutcome> {
    let prefix = format!("{run_id}-");
    items
        .iter()
        .filter_map(|i| {
            let sk = i.get("sk")?.as_s().ok()?;
            let attempt: u32 = sk.strip_prefix(&prefix)?.parse().ok()?;
            Some((attempt, i))
        })
        .max_by_key(|(a, _)| *a)
        .map(|(_, i)| {
            data::<Value>(i)
                .and_then(|v| serde_json::from_value(v["result"].clone()).ok())
                .unwrap_or(IngestOutcome::Reading)
        })
}

/// One failure, for its failure group.
pub struct Failure<'a> {
    pub repo: &'a str,
    pub signature: &'a str,
    pub message: &'a str,
    pub selector: Option<&'a str>,
    pub test_id: &'a str,
    pub at: &'a str,
}

pub struct Store<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
}

impl<'a> Store<'a> {
    pub fn new(dynamo: &'a aws_sdk_dynamodb::Client, table: &'a str) -> Self {
        Store { dynamo, table }
    }

    pub fn enabled(&self) -> bool {
        !self.table.is_empty()
    }

    async fn get(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
        self.dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(pk))
            .key("sk", s(sk))
            .send()
            .await
            .map(|o| o.item().cloned())
            .map_err(|e| format!("e2e get: {e}"))
    }

    async fn put(
        &self,
        pk: &str,
        sk: &str,
        value: &impl Serialize,
        extra: Vec<(&str, AttributeValue)>,
    ) -> Result<(), String> {
        let body = serde_json::to_string(value).map_err(|e| e.to_string())?;
        let mut req = self
            .dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(pk))
            .item("sk", s(sk))
            .item("data", s(&body));
        for (k, v) in extra {
            req = req.item(k, v);
        }
        req.send()
            .await
            .map(|_| ())
            .map_err(|e| format!("e2e put: {e}"))
    }

    /// Every item under `pk`, optionally from a starting sort key backwards.
    async fn query_all(
        &self,
        pk: &str,
        newest_first: bool,
        limit: Option<i32>,
        before: Option<&str>,
    ) -> Result<(Vec<Item>, Option<String>), String> {
        let mut items = Vec::new();
        let mut start: Option<Item> =
            before.map(|b| HashMap::from([("pk".to_string(), s(pk)), ("sk".to_string(), s(b))]));
        loop {
            let mut req = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :pk")
                .expression_attribute_values(":pk", s(pk))
                .scan_index_forward(!newest_first)
                .set_exclusive_start_key(start.take());
            if let Some(l) = limit {
                req = req.limit(l - items.len() as i32);
            }
            let out = req.send().await.map_err(|e| format!("e2e query: {e}"))?;
            items.extend(out.items().iter().cloned());
            let last = out.last_evaluated_key().cloned();
            let full = limit.is_some_and(|l| items.len() as i32 >= l);
            match last {
                Some(k) if !full => start = Some(k),
                Some(k) => {
                    let next = k.get("sk").and_then(|v| v.as_s().ok()).cloned();
                    return Ok((items, next));
                }
                None => return Ok((items, None)),
            }
        }
    }

    // ── settings and repos ──────────────────────────────────────────────

    pub async fn settings(&self, team: &str) -> Result<Settings, String> {
        Ok(self
            .get(&team_pk(team), "SETTINGS")
            .await?
            .and_then(|i| data(&i))
            .unwrap_or_default())
    }

    pub async fn put_settings(&self, team: &str, settings: &Settings) -> Result<(), String> {
        self.put(&team_pk(team), "SETTINGS", settings, vec![]).await
    }

    /// Records that a repo of the team has results, and lets CI calls signed
    /// by GitHub for that repo find the team. `repo_id` pins the repo, so a
    /// renamed or re-created repo with the same name can't speak for it.
    pub async fn upsert_repo(
        &self,
        team: &str,
        repo: &str,
        installation_id: u64,
        repo_id: u64,
        default_branch: &str,
    ) -> Result<(), String> {
        let v = json!({ "repo": repo, "installation_id": installation_id, "repo_id": repo_id, "default_branch": default_branch });
        self.put(&team_pk(team), &format!("REPO#{repo}"), &v, vec![])
            .await?;
        self.put(
            &format!("REPO#{}", repo.to_lowercase()),
            &format!("TEAM#{team}"),
            &v,
            vec![],
        )
        .await
    }

    pub async fn repos(&self, team: &str) -> Result<Vec<Value>, String> {
        let out = self
            .dynamo
            .query()
            .table_name(self.table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", s(&team_pk(team)))
            .expression_attribute_values(":p", s("REPO#"))
            .send()
            .await
            .map_err(|e| format!("e2e repos: {e}"))?;
        Ok(out.items().iter().filter_map(data::<Value>).collect())
    }

    pub async fn repo(&self, team: &str, repo: &str) -> Result<Option<Value>, String> {
        Ok(self
            .get(&team_pk(team), &format!("REPO#{repo}"))
            .await?
            .and_then(|i| data(&i)))
    }

    /// Teams that receive results for a repo: (team, repo record).
    pub async fn teams_for_repo(&self, repo: &str) -> Result<Vec<(String, Value)>, String> {
        let (items, _) = self
            .query_all(&format!("REPO#{}", repo.to_lowercase()), false, None, None)
            .await?;
        Ok(items
            .iter()
            .filter_map(|i| {
                let team = i.get("sk")?.as_s().ok()?.strip_prefix("TEAM#")?.to_string();
                Some((team, data::<Value>(i)?))
            })
            .collect())
    }

    // ── CI setup ────────────────────────────────────────────────────────

    /// Results and quarantine calls seen for a repo.
    pub async fn ci_state(&self, team: &str, repo: &str) -> Result<CiState, String> {
        Ok(self
            .get(&team_pk(team), &format!("CI#{repo}"))
            .await?
            .and_then(|i| data(&i))
            .unwrap_or_default())
    }

    /// Reads, changes and writes a repo's CI state. Two runs finishing at
    /// once can lose one count; the next run corrects it.
    pub async fn update_ci_state(
        &self,
        team: &str,
        repo: &str,
        change: impl FnOnce(&mut CiState),
    ) -> Result<(), String> {
        let mut ci = self.ci_state(team, repo).await?;
        change(&mut ci);
        self.put(&team_pk(team), &format!("CI#{repo}"), &ci, vec![])
            .await
    }

    /// The setup pull request of a repo and the last check of its workflow
    /// (`{"pr": SetupPr, "check": {...}, "checked_at": ...}`).
    pub async fn setup(&self, team: &str, repo: &str) -> Result<Value, String> {
        Ok(self
            .get(&team_pk(team), &format!("SETUP#{repo}"))
            .await?
            .and_then(|i| data(&i))
            .unwrap_or_else(|| json!({})))
    }

    pub async fn put_setup(&self, team: &str, repo: &str, v: &Value) -> Result<(), String> {
        self.put(&team_pk(team), &format!("SETUP#{repo}"), v, vec![])
            .await
    }

    /// Changes only the pull request part of a repo's setup record.
    pub async fn put_setup_pr(&self, team: &str, repo: &str, pr: &SetupPr) -> Result<(), String> {
        let mut v = self.setup(team, repo).await?;
        v["pr"] = json!(pr);
        self.put_setup(team, repo, &v).await
    }

    // ── runs ────────────────────────────────────────────────────────────

    /// Claims a run for ingestion. False when it was already taken, so a
    /// duplicate webhook or a re-delivered message does nothing.
    pub async fn claim_run(
        &self,
        team: &str,
        run_key: &str,
        repo: &str,
        started_at: &str,
    ) -> Result<bool, String> {
        let v = json!({ "repo": repo, "started_at": started_at });
        let r = self
            .dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(&format!("T#{team}#RUNKEY")))
            .item("sk", s(run_key))
            .item("data", s(&v.to_string()))
            .item("ttl", ttl(RETENTION_DAYS))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;
        match r {
            Ok(_) => Ok(true),
            Err(e)
                if e.as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(e) => Err(format!("e2e claim run: {e}")),
        }
    }

    pub async fn run_ref(
        &self,
        team: &str,
        run_key: &str,
    ) -> Result<Option<(String, String)>, String> {
        Ok(self
            .get(&format!("T#{team}#RUNKEY"), run_key)
            .await?
            .and_then(|i| {
                let v: Value = data(&i)?;
                Some((
                    v["repo"].as_str()?.to_string(),
                    v["started_at"].as_str().unwrap_or("").to_string(),
                ))
            }))
    }

    /// Records what reading a claimed run found, next to its claim.
    pub async fn finish_run(
        &self,
        team: &str,
        run_key: &str,
        repo: &str,
        started_at: &str,
        outcome: &IngestOutcome,
    ) -> Result<(), String> {
        let v = json!({ "repo": repo, "started_at": started_at, "result": outcome });
        self.put(
            &format!("T#{team}#RUNKEY"),
            run_key,
            &v,
            vec![("ttl", ttl(RETENTION_DAYS))],
        )
        .await
    }

    /// What reading the newest attempt of a workflow run found. `None` when
    /// no attempt of it has been claimed yet.
    pub async fn latest_outcome(
        &self,
        team: &str,
        run_id: u64,
    ) -> Result<Option<IngestOutcome>, String> {
        let out = self
            .dynamo
            .query()
            .table_name(self.table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :run)")
            .expression_attribute_values(":pk", s(&format!("T#{team}#RUNKEY")))
            .expression_attribute_values(":run", s(&format!("{run_id}-")))
            .send()
            .await
            .map_err(|e| format!("e2e run outcome: {e}"))?;
        Ok(newest_outcome(out.items(), run_id))
    }

    /// The comment id of a PR's "only quarantined tests failed" note.
    pub async fn pr_note(&self, team: &str, repo: &str, pr: u64) -> Result<Option<u64>, String> {
        Ok(self
            .get(&format!("T#{team}#QNOTE"), &format!("{repo}#{pr}"))
            .await?
            .and_then(|i| data::<Value>(&i))
            .and_then(|v| v["comment_id"].as_u64()))
    }

    pub async fn put_pr_note(
        &self,
        team: &str,
        repo: &str,
        pr: u64,
        comment_id: u64,
    ) -> Result<(), String> {
        self.put(
            &format!("T#{team}#QNOTE"),
            &format!("{repo}#{pr}"),
            &json!({ "comment_id": comment_id }),
            vec![("ttl", ttl(RETENTION_DAYS))],
        )
        .await
    }

    pub async fn put_run(&self, team: &str, run: &Run) -> Result<(), String> {
        self.put(
            &format!("T#{team}#RUNS#{}", run.repo),
            &format!("{}#{}", run.started_at, run.run_key),
            run,
            vec![("ttl", ttl(RETENTION_DAYS))],
        )
        .await
    }

    pub async fn get_run(
        &self,
        team: &str,
        repo: &str,
        started_at: &str,
        run_key: &str,
    ) -> Result<Option<Run>, String> {
        Ok(self
            .get(
                &format!("T#{team}#RUNS#{repo}"),
                &format!("{started_at}#{run_key}"),
            )
            .await?
            .and_then(|i| data(&i)))
    }

    /// Runs for one repo, newest first, `limit` at a time.
    pub async fn runs(
        &self,
        team: &str,
        repo: &str,
        limit: i32,
        before: Option<&str>,
    ) -> Result<(Vec<Run>, Option<String>), String> {
        let (items, next) = self
            .query_all(
                &format!("T#{team}#RUNS#{repo}"),
                true,
                Some(limit.clamp(1, 200)),
                before,
            )
            .await?;
        Ok((items.iter().filter_map(data).collect(), next))
    }

    /// Runs since a time, for trends.
    pub async fn runs_since(
        &self,
        team: &str,
        repo: &str,
        since: &str,
    ) -> Result<Vec<Run>, String> {
        let mut out = Vec::new();
        let mut start: Option<Item> = None;
        loop {
            let res = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :pk AND sk >= :since")
                .expression_attribute_values(":pk", s(&format!("T#{team}#RUNS#{repo}")))
                .expression_attribute_values(":since", s(since))
                .set_exclusive_start_key(start.take())
                .send()
                .await
                .map_err(|e| format!("e2e runs since: {e}"))?;
            out.extend(res.items().iter().filter_map(data::<Run>));
            match res.last_evaluated_key() {
                Some(k) if out.len() < 20_000 => start = Some(k.clone()),
                _ => return Ok(out),
            }
        }
    }

    // ── tests ───────────────────────────────────────────────────────────

    pub async fn record(&self, team: &str, id: &str) -> Result<Option<TestRecord>, String> {
        Ok(self
            .get(&format!("T#{team}#TEST#{id}"), "RECORD")
            .await?
            .and_then(|i| data(&i)))
    }

    /// Saves a test's record and its summary together.
    pub async fn put_record(&self, team: &str, r: &TestRecord) -> Result<(), String> {
        self.put(&format!("T#{team}#TEST#{}", r.id), "RECORD", r, vec![])
            .await?;
        let summary = super::stats::summary(r, chrono::Utc::now());
        self.put(&format!("T#{team}#TESTS"), &r.id, &summary, vec![])
            .await
    }

    /// Every test summary of the team (optionally one repo).
    pub async fn summaries(&self, team: &str, repo: Option<&str>) -> Result<Vec<Value>, String> {
        let (items, _) = self
            .query_all(&format!("T#{team}#TESTS"), false, None, None)
            .await?;
        Ok(items
            .iter()
            .filter_map(data::<Value>)
            .filter(|v| repo.is_none_or(|r| v["repo"].as_str() == Some(r)))
            .collect())
    }

    // ── failure groups ──────────────────────────────────────────────────

    /// Adds one failure to its group.
    pub async fn add_failure(&self, team: &str, f: &Failure<'_>) -> Result<(), String> {
        let (repo, sig, message, selector, test_id, at) =
            (f.repo, f.signature, f.message, f.selector, f.test_id, f.at);
        let pk = format!("T#{team}#SIGS");
        let sk = format!("{repo}#{sig}");
        let mut g: Value = self.get(&pk, &sk).await?.and_then(|i| data(&i)).unwrap_or_else(|| {
            json!({ "signature": sig, "repo": repo, "message": message, "selector": selector, "test_ids": [], "days": {}, "first_seen": at })
        });
        let day = at.get(..10).unwrap_or(at).to_string();
        let days = g["days"]
            .as_object_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        let mut days: serde_json::Map<String, Value> = days
            .into_iter()
            .filter(|(d, _)| d.as_str() >= day_minus(&day, 30).as_str())
            .collect();
        let n = days.get(&day).and_then(|v| v.as_u64()).unwrap_or(0);
        days.insert(day, json!(n + 1));
        g["days"] = Value::Object(days);
        let mut ids: Vec<String> = g["test_ids"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        if !ids.iter().any(|i| i == test_id) {
            ids.push(test_id.to_string());
            ids.truncate(50);
        }
        g["test_ids"] = json!(ids);
        g["message"] = json!(message);
        g["last_seen"] = json!(at);
        self.put(&pk, &sk, &g, vec![]).await
    }

    pub async fn set_group_cause(
        &self,
        team: &str,
        repo: &str,
        sig: &str,
        cause: &str,
    ) -> Result<(), String> {
        let pk = format!("T#{team}#SIGS");
        let sk = format!("{repo}#{sig}");
        if let Some(mut g) = self.get(&pk, &sk).await?.and_then(|i| data::<Value>(&i)) {
            g["cause"] = json!(cause);
            self.put(&pk, &sk, &g, vec![]).await?;
        }
        Ok(())
    }

    pub async fn groups(&self, team: &str, repo: Option<&str>) -> Result<Vec<Value>, String> {
        let (items, _) = self
            .query_all(&format!("T#{team}#SIGS"), false, None, None)
            .await?;
        Ok(items
            .iter()
            .filter_map(data::<Value>)
            .filter(|v| repo.is_none_or(|r| v["repo"].as_str() == Some(r)))
            .collect())
    }

    // ── recommendations ─────────────────────────────────────────────────

    pub async fn put_recommendation(&self, team: &str, r: &Recommendation) -> Result<(), String> {
        self.put(&format!("T#{team}#RECS"), &r.id, r, vec![]).await
    }

    pub async fn recommendation(
        &self,
        team: &str,
        id: &str,
    ) -> Result<Option<Recommendation>, String> {
        Ok(self
            .get(&format!("T#{team}#RECS"), id)
            .await?
            .and_then(|i| data(&i)))
    }

    pub async fn recommendations(
        &self,
        team: &str,
        repo: Option<&str>,
    ) -> Result<Vec<Recommendation>, String> {
        let (items, _) = self
            .query_all(&format!("T#{team}#RECS"), false, None, None)
            .await?;
        Ok(items
            .iter()
            .filter_map(data::<Recommendation>)
            .filter(|r| repo.is_none_or(|x| r.repo == x))
            .collect())
    }

    // ── coverage ────────────────────────────────────────────────────────

    /// The latest coverage picture for a repo: areas, their tests, route gaps.
    pub async fn put_coverage(
        &self,
        team: &str,
        repo: &str,
        coverage: &Value,
    ) -> Result<(), String> {
        self.put(
            &team_pk(team),
            &format!("COVERAGE#{repo}"),
            coverage,
            vec![],
        )
        .await
    }

    pub async fn coverage(&self, team: &str, repo: &str) -> Result<Option<Value>, String> {
        Ok(self
            .get(&team_pk(team), &format!("COVERAGE#{repo}"))
            .await?
            .and_then(|i| data(&i)))
    }

    // ── PR triage ───────────────────────────────────────────────────────

    pub async fn put_triage(
        &self,
        team: &str,
        repo: &str,
        pr: u64,
        sha: &str,
        triage: &Value,
    ) -> Result<(), String> {
        self.put(
            &format!("T#{team}#PR#{repo}#{pr}"),
            sha,
            triage,
            vec![("ttl", ttl(30))],
        )
        .await
    }

    pub async fn triage(
        &self,
        team: &str,
        repo: &str,
        pr: u64,
        sha: &str,
    ) -> Result<Option<Value>, String> {
        Ok(self
            .get(&format!("T#{team}#PR#{repo}#{pr}"), sha)
            .await?
            .and_then(|i| data(&i)))
    }
}

/// `YYYY-MM-DD` minus `days`, as `YYYY-MM-DD`.
pub fn day_minus(day: &str, days: i64) -> String {
    chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d")
        .map(|d| {
            (d - chrono::Duration::days(days))
                .format("%Y-%m-%d")
                .to_string()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_keys_are_safe() {
        assert_eq!(
            run_detail_key("TEAM#1", "acme/web", "9-1"),
            "e2e/TEAM_1/acme/web/runs/9-1.json"
        );
        assert_eq!(
            raw_report_key("TEAM#1", "acme/web", "9-1", "../x y.json"),
            "e2e/TEAM_1/acme/web/raw/9-1/.._x_y.json"
        );
        assert_eq!(day_minus("2026-03-01", 1), "2026-02-28");
    }

    fn claim(sk: &str, data: Option<Value>) -> Item {
        let mut i = HashMap::from([("sk".to_string(), s(sk))]);
        if let Some(d) = data {
            i.insert("data".into(), s(&d.to_string()));
        }
        i
    }

    #[test]
    fn the_newest_attempt_decides_and_a_bare_claim_is_still_reading() {
        let read = |rescued: bool| json!({ "repo": "acme/web", "result": { "outcome": "read", "failed": 1, "rescued": rescued, "quarantined": ["a"] } });
        let items = vec![
            claim("77-1", Some(read(false))),
            claim("77-10", Some(read(true))),
            claim("77-2", Some(read(false))),
            claim("770-1", Some(read(false))),
        ];
        assert_eq!(
            newest_outcome(&items, 77),
            Some(IngestOutcome::Read {
                failed: 1,
                rescued: true,
                quarantined: vec!["a".into()]
            })
        );
        assert_eq!(
            newest_outcome(&[claim("5-1", Some(json!({ "repo": "acme/web" })))], 5),
            Some(IngestOutcome::Reading)
        );
        assert_eq!(
            newest_outcome(
                &[claim(
                    "5-1",
                    Some(json!({ "result": { "outcome": "no_tests" } }))
                )],
                5
            ),
            Some(IngestOutcome::NoTests)
        );
        assert_eq!(newest_outcome(&[], 5), None);
    }
}
