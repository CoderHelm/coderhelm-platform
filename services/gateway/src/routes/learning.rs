//! The Learning page: what CoderHelm learned from outcomes, and the admin
//! controls over it (see `common::rewards`).
//!
//! Everything is read and written under the caller's own team. Anyone on the
//! team may look; only admins pin a prompt variant, revert or restore a
//! behavior change, or tune the signal weights.

use axum::extract::{Query, State};
use axum::{http::StatusCode, Extension, Json};
use common::rewards::{bandit, event::AGENTS, health, store::Store, summary};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use crate::models::Claims;
use crate::AppState;

fn store(state: &AppState) -> Store {
    Store::new(&state.dynamo, &state.config.settings_table_name)
}

fn internal(e: String) -> StatusCode {
    error!(error = %e, "Learning store error");
    StatusCode::INTERNAL_SERVER_ERROR
}

#[derive(Deserialize)]
pub struct LearningQuery {
    days: Option<u32>,
    repo: Option<String>,
}

/// The repository filter: empty, or a valid `owner/name`.
pub fn repo_filter(raw: Option<&str>) -> Result<String, StatusCode> {
    let r = raw.unwrap_or("").trim();
    if r.is_empty() {
        return Ok(String::new());
    }
    if common::review_lessons::valid_scope(r) && r != common::review_lessons::TEAM_SCOPE {
        Ok(r.to_string())
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}

pub fn days_of(raw: Option<u32>) -> u32 {
    raw.unwrap_or(30).clamp(1, 180)
}

/// GET /api/learning — rewards, calibration, changes, examples and variants.
pub async fn get_learning(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<LearningQuery>,
) -> Result<Json<Value>, StatusCode> {
    let repo = repo_filter(q.repo.as_deref())?;
    let days = days_of(q.days);
    let team = claims.team_id.as_str();
    let st = store(&state);
    let since = (chrono::Utc::now() - chrono::Duration::days(days as i64)).to_rfc3339();
    let weights = st.weights(team).await;
    let events = st.events_since(team, &since).await.map_err(internal)?;
    let categories = st.all_categories(team).await.map_err(internal)?;
    let changes = st.changes(team).await.map_err(internal)?;
    let exemplars = st
        .exemplars(team, (!repo.is_empty()).then_some(repo.as_str()))
        .await
        .map_err(internal)?;
    let mut arms = vec![];
    for agent in AGENTS {
        if bandit::arms_for(agent).is_empty() {
            continue;
        }
        let stats = st
            .arm_stats(team, agent, &weights)
            .await
            .map_err(internal)?;
        arms.push((agent.to_string(), st.pin(team, agent).await, stats));
    }
    // Custom roles included: whoever holds the "learning" capability.
    let can_manage = claims.holder().holds("learning");
    Ok(Json(summary::build(&summary::Inputs {
        team_id: team,
        days,
        repo: &repo,
        can_manage,
        weights: &weights,
        events: &events,
        categories: &categories,
        changes: &changes,
        exemplars: &exemplars,
        arms: &arms,
    })))
}

/// Pages read per record kind for the health page (keys and a few fields
/// only, never an example's embedding or a decision's picture).
const HEALTH_PAGES: usize = 10;

type Item = std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>;

fn item_s(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn item_n(i: &Item, k: &str) -> f64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}

/// Pure: the CoderHelm pull requests that closed, from run items
/// (`pr_url`, `merged_at` or `pr_closed_at`).
pub fn closed_prs_of(runs: &[Item]) -> Vec<health::ClosedPr> {
    runs.iter()
        .filter_map(|r| {
            let (owner, name, pr) = common::pr_outcome::parse_pr_url(&item_s(r, "pr_url"))?;
            let closed_at = match item_s(r, "merged_at") {
                m if !m.is_empty() => m,
                _ => item_s(r, "pr_closed_at"),
            };
            // The coder arm its run picked; a run from before the coder had
            // variants has none, and its PR can't be credited to one.
            let variant = match item_s(r, "prompt_variant") {
                v if bandit::arm(common::rewards::event::AGENT_CODER, &v).is_some() => v,
                _ => String::new(),
            };
            (!closed_at.is_empty()).then(|| health::ClosedPr {
                repo: format!("{owner}/{name}"),
                pr,
                closed_at,
                variant,
            })
        })
        .collect()
}

/// The team's runs whose PR merged or closed since `since`.
async fn closed_runs(state: &AppState, team: &str, since: &str) -> Vec<Item> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..HEALTH_PAGES {
        let page = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .key_condition_expression("team_id = :t")
            .filter_expression("merged_at >= :s OR pr_closed_at >= :s")
            .projection_expression("pr_url, merged_at, pr_closed_at, prompt_variant")
            .expression_attribute_values(
                ":t",
                aws_sdk_dynamodb::types::AttributeValue::S(team.into()),
            )
            .expression_attribute_values(
                ":s",
                aws_sdk_dynamodb::types::AttributeValue::S(since.into()),
            )
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(page) = page else {
            break;
        };
        out.extend(page.items().iter().cloned());
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out
}

/// GET /api/learning/health — per learning loop: its last signal, its count
/// over the last 7 days, what should have fed it, and `quiet` (the page red)
/// when a loop that should have fired stayed silent.
pub async fn get_health(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let team = claims.team_id.as_str();
    let st = store(&state);
    let now = chrono::Utc::now();
    let since = (now - chrono::Duration::days(health::WINDOW_DAYS)).to_rfc3339();
    let fields = |prefix: &'static str, f: &'static [&'static str]| {
        let st = st.clone();
        let team = team.to_string();
        async move {
            st.query_projected(&team, prefix, f, HEALTH_PAGES)
                .await
                .unwrap_or_default()
        }
    };
    let events = st.events_since(team, &since).await.map_err(internal)?;
    let runs = closed_runs(&state, team, &since).await;
    // Each review's pull request comes from its key (`REVIEW#{repo}#{pr}#{at}`),
    // so the review loops count pull requests on both sides.
    let reviews: Vec<health::Review> = fields("REVIEW#", &["sk"])
        .await
        .iter()
        .filter_map(|i| health::Review::of_sk(&item_s(i, "sk")))
        .filter(|r| r.created_at.as_str() >= since.as_str())
        .collect();
    let exemplar_last_used: Vec<String> =
        fields(common::rewards::exemplars::PREFIX, &["last_used_at"])
            .await
            .iter()
            .map(|i| item_s(i, "last_used_at"))
            .collect();
    let arms: Vec<health::ArmPull> = fields(
        common::rewards::store::ARM_PREFIX,
        &["agent", "pulls", "last_pull_at"],
    )
    .await
    .iter()
    .map(|i| health::ArmPull {
        agent: item_s(i, "agent"),
        pulls: item_n(i, "pulls").max(0.0) as u64,
        last_pull_at: item_s(i, "last_pull_at"),
    })
    .collect();
    let categories_updated: Vec<String> =
        fields(common::rewards::store::CAT_PREFIX, &["updated_at"])
            .await
            .iter()
            .map(|i| item_s(i, "updated_at"))
            .collect();
    let calibration_changes: Vec<(String, String)> = st
        .changes(team)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|c| c.kind == common::rewards::changes::KIND_CALIBRATION)
        .map(|c| (c.at, c.to))
        .collect();
    let coder_lessons_seen: Vec<String> = st
        .lessons(team, None)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|l| l.agent == common::rewards::event::AGENT_CODER)
        .map(|l| l.last_seen_at)
        .collect();
    let pending: Vec<health::Pending> = fields(
        common::review_lessons::PENDING_SK_PREFIX,
        &["status", "last_event_at", "judged_at", "learned"],
    )
    .await
    .iter()
    .map(|i| health::Pending {
        status: item_s(i, "status"),
        last_event_at_ms: item_n(i, "last_event_at") as i64,
        judged_at: item_s(i, "judged_at"),
        learned: item_n(i, "learned").max(0.0) as u64,
    })
    .collect();
    let review_lessons_seen: Vec<String> =
        fields(common::review_lessons::LESSON_SK_PREFIX, &["last_seen_at"])
            .await
            .iter()
            .map(|i| item_s(i, "last_seen_at"))
            .collect();
    let decisions = common::decision_log::since_light(
        &state.dynamo,
        &state.config.settings_table_name,
        team,
        &since,
        HEALTH_PAGES,
    )
    .await;
    let closed = closed_prs_of(&runs);
    Ok(Json(health::build(
        &health::Inputs {
            events: &events,
            closed_prs: &closed,
            reviews: &reviews,
            exemplar_last_used: &exemplar_last_used,
            arms: &arms,
            categories_updated: &categories_updated,
            calibration_changes: &calibration_changes,
            coder_lessons_seen: &coder_lessons_seen,
            pending: &pending,
            review_lessons_seen: &review_lessons_seen,
            decisions: &decisions,
        },
        now,
    )))
}

#[derive(Deserialize)]
pub struct PinBody {
    agent: String,
    arm: Option<String>,
}

/// Checks a pin request: a known agent with variants, and one of its arms.
pub fn check_pin(agent: &str, arm: Option<&str>) -> Result<(), StatusCode> {
    if bandit::arms_for(agent).is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    match arm {
        Some(a) if bandit::arm(agent, a).is_none() => Err(StatusCode::BAD_REQUEST),
        _ => Ok(()),
    }
}

/// POST /api/learning/pin — pin (or with `arm: null` unpin) a prompt variant.
pub async fn pin_variant(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<PinBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let arm = body.arm.as_deref().filter(|a| !a.is_empty());
    check_pin(&body.agent, arm)?;
    store(&state)
        .set_pin(&claims.team_id, &body.agent, arm, &claims.display_name())
        .await
        .map_err(internal)?;
    info!(team_id = %claims.team_id, agent = %body.agent, arm = ?arm, "Prompt variant pinned");
    Ok(Json(json!({"ok": true})))
}

#[derive(Deserialize)]
pub struct RevertBody {
    id: Option<String>,
    repo: Option<String>,
}

/// A change id as the log writes them (a ULID).
pub fn valid_change_id(id: &str) -> bool {
    id.len() == 26 && id.chars().all(|c| c.is_ascii_alphanumeric())
}

/// POST /api/learning/revert — turn one change off, or every active change
/// of a repository (`{"repo": "owner/name"}`).
pub async fn revert_change(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<RevertBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let st = store(&state);
    let by = claims.display_name();
    match (body.id.as_deref(), body.repo.as_deref()) {
        (Some(id), None) => {
            if !valid_change_id(id) {
                return Err(StatusCode::BAD_REQUEST);
            }
            if !st
                .revert(&claims.team_id, id, &by)
                .await
                .map_err(internal)?
            {
                return Err(StatusCode::NOT_FOUND);
            }
            Ok(Json(json!({"reverted": 1})))
        }
        (None, Some(repo)) => {
            let repo = repo_filter(Some(repo))?;
            if repo.is_empty() {
                return Err(StatusCode::BAD_REQUEST);
            }
            let n = st
                .revert_repo(&claims.team_id, &repo, &by)
                .await
                .map_err(internal)?;
            info!(team_id = %claims.team_id, repo = %repo, n, "Learning changes reverted for a repository");
            Ok(Json(json!({"reverted": n})))
        }
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

#[derive(Deserialize)]
pub struct RestoreBody {
    id: String,
}

/// POST /api/learning/restore — undo a revert.
pub async fn restore_change(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<RestoreBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    if !valid_change_id(&body.id) {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !store(&state)
        .restore(&claims.team_id, &body.id, &claims.display_name())
        .await
        .map_err(internal)?
    {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(json!({"ok": true})))
}

#[derive(Deserialize)]
pub struct WeightsBody {
    weights: serde_json::Map<String, Value>,
}

/// PUT /api/learning/weights — the team's signal weights (`{}` resets).
pub async fn put_weights(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<WeightsBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let w = common::rewards::score::Weights::validate(&body.weights)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    store(&state)
        .put_weights(&claims.team_id, &w, &claims.display_name())
        .await
        .map_err(internal)?;
    Ok(Json(json!({"weights": w.effective()})))
}

// ── The scorecard, outcome lessons and reviewer asks ──────────────────

#[derive(Deserialize)]
pub struct ScorecardQuery {
    months: Option<u32>,
    repo: Option<String>,
}

/// The window in months; 0 is all time.
pub fn months_of(raw: Option<u32>) -> u32 {
    raw.unwrap_or(3).min(24)
}

/// GET /api/learning/scorecard — what became of the pull requests
/// CoderHelm opened, per team and per repository, from the counters the
/// webhooks keep (`common::scorecard`). `months=0` is all time.
pub async fn get_scorecard(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<ScorecardQuery>,
) -> Result<Json<Value>, StatusCode> {
    use common::scorecard::{self, Store as _};
    let repo = repo_filter(q.repo.as_deref())?;
    let months = months_of(q.months);
    let store = scorecard::Dynamo::new(&state.dynamo, &state.config.settings_table_name);
    let items = store
        .counters(&claims.team_id, scorecard::AGG_PREFIX)
        .await
        .map_err(internal)?;
    Ok(Json(scorecard::view(
        &items,
        months,
        &repo,
        chrono::Utc::now(),
    )))
}

/// Pure: the coder's lessons as the Learning page lists them, heaviest
/// first: the rule, how to check it, the evidence behind it, and where it
/// stands (recalled into runs, in the guide file, declined, turned off).
pub fn lessons_view(
    lessons: &[common::rewards::lessons::Lesson],
    guides: &[common::guide::Record],
    repo: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Value {
    use common::rewards::lessons as l;
    let mut rows: Vec<(f64, Value)> = lessons
        .iter()
        .filter(|x| x.agent == common::rewards::event::AGENT_CODER)
        .filter(|x| repo.is_empty() || x.repo == repo)
        .map(|x| {
            let weight = l::weight_of(x, now);
            let status = if x.reverted {
                "off"
            } else if x.in_guide {
                "in_guide"
            } else if weight < l::MIN_WEIGHT {
                if x.contradicted > 0 {
                    "contradicted"
                } else {
                    "faded"
                }
            } else {
                "recalled"
            };
            (
                weight,
                json!({
                    "id": x.id,
                    "repo": x.repo,
                    "text": x.text,
                    "check": x.check,
                    "applies_to": x.applies_to,
                    "source": x.source,
                    "seen": x.seen,
                    "contradicted": x.contradicted,
                    "prs": x.prs,
                    "evidence": x.evidence,
                    "created_at": x.created_at,
                    "last_seen_at": x.last_seen_at,
                    "status": status,
                    "guide_declined": x.guide_declined,
                    "guide_ready": l::guide_ready(x, now),
                }),
            )
        })
        .collect();
    rows.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let guides: Vec<Value> = guides
        .iter()
        .filter(|g| repo.is_empty() || g.repo == repo)
        .filter(|g| g.pr > 0)
        .map(|g| {
            json!({
                "repo": g.repo,
                "pr": g.pr,
                "url": format!("https://github.com/{}/pull/{}", g.repo, g.pr),
                "state": g.state,
                "path": g.path,
                "lessons": g.lesson_ids.len(),
                "at": g.at,
            })
        })
        .collect();
    json!({
        "lessons": rows.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
        "guide_prs": guides,
        "guide_min_seen": l::GUIDE_MIN_SEEN,
    })
}

/// GET /api/learning/lessons — the coder's lessons with their evidence,
/// and each repository's guide pull request.
pub async fn get_outcome_lessons(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<LearningQuery>,
) -> Result<Json<Value>, StatusCode> {
    let repo = repo_filter(q.repo.as_deref())?;
    let st = store(&state);
    let team = claims.team_id.as_str();
    let lessons = st
        .lessons(team, (!repo.is_empty()).then_some(repo.as_str()))
        .await
        .map_err(internal)?;
    let guides: Vec<common::guide::Record> = st
        .query_prefix(team, common::guide::RECORD_PREFIX, None)
        .await
        .map_err(internal)?
        .iter()
        .filter_map(common::guide::Record::from_item)
        .collect();
    Ok(Json(lessons_view(
        &lessons,
        &guides,
        &repo,
        chrono::Utc::now(),
    )))
}

/// GET /api/learning/reviewers — what each reviewer repeatedly asks for on
/// CoderHelm's pull requests (`common::reviewer_prefs`): the ask, its
/// category and the pull requests it was seen on. Nothing else is kept.
pub async fn get_reviewer_asks(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let prefs =
        common::reviewer_prefs::Store::new(&state.dynamo, &state.config.settings_table_name)
            .prefs(&claims.team_id, None)
            .await
            .map_err(internal)?;
    let mut v = common::reviewer_prefs::view(&prefs, chrono::Utc::now());
    v["can_manage"] = json!(claims.holder().holds("learning"));
    Ok(Json(v))
}

#[derive(Deserialize)]
pub struct ForgetBody {
    login: String,
    /// One ask; without it, everything kept about the reviewer.
    id: Option<String>,
}

/// What a forget request names: a plain login, and optionally one ask's id.
pub fn check_forget(login: &str, id: Option<&str>) -> Result<String, StatusCode> {
    let login = common::reviewer_prefs::login_key(login).ok_or(StatusCode::BAD_REQUEST)?;
    match id {
        Some(i) if !valid_change_id(i) => Err(StatusCode::BAD_REQUEST),
        _ => Ok(login),
    }
}

/// POST /api/learning/reviewers/forget — forget one ask, or everything
/// kept about a reviewer.
pub async fn forget_reviewer(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<ForgetBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let login = check_forget(&body.login, body.id.as_deref())?;
    let st = common::reviewer_prefs::Store::new(&state.dynamo, &state.config.settings_table_name);
    let forgotten = match body.id.as_deref() {
        Some(id) => {
            st.forget_ask(&claims.team_id, &login, id)
                .await
                .map_err(internal)?;
            1
        }
        None => st.forget(&claims.team_id, &login).await.map_err(internal)?,
    };
    info!(team_id = %claims.team_id, forgotten, "Reviewer asks forgotten");
    Ok(Json(json!({"forgotten": forgotten})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_scorecard_window_is_bounded_and_zero_is_all_time() {
        assert_eq!(months_of(None), 3);
        assert_eq!(months_of(Some(0)), 0);
        assert_eq!(months_of(Some(999)), 24);
    }

    #[test]
    fn a_forget_request_names_a_plain_login_and_a_real_id() {
        assert_eq!(check_forget("Pat-Lee", None), Ok("pat-lee".into()));
        assert_eq!(
            check_forget("pat", Some("01JBALERT3K8P2N6Q9R1S4T7V0")),
            Ok("pat".into())
        );
        assert_eq!(
            check_forget("acme/team", None),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(check_forget("pat#x", None), Err(StatusCode::BAD_REQUEST));
        assert_eq!(
            check_forget("pat", Some("../x")),
            Err(StatusCode::BAD_REQUEST)
        );
    }

    #[test]
    fn lessons_are_listed_with_their_evidence_and_where_they_stand() {
        use chrono::TimeZone;
        use common::rewards::lessons::Lesson;
        let now = chrono::Utc.with_ymd_and_hms(2026, 10, 20, 0, 0, 0).unwrap();
        let base = Lesson {
            repo: "acme/web".into(),
            agent: "coder".into(),
            check: "No `fetch(` in the diff.".into(),
            seen: 2,
            prs: vec!["acme/web#12".into(), "acme/web#15".into()],
            last_seen_at: "2026-10-19T00:00:00Z".into(),
            evidence: vec!["PR #12 in people's words: \"Use the shared client\"".into()],
            ..Default::default()
        };
        let lessons = vec![
            Lesson {
                id: "ready".into(),
                text: "Use `useApi`.".into(),
                ..base.clone()
            },
            Lesson {
                id: "guide".into(),
                in_guide: true,
                seen: 5,
                ..base.clone()
            },
            Lesson {
                id: "against".into(),
                contradicted: 2,
                ..base.clone()
            },
            Lesson {
                id: "off".into(),
                reverted: true,
                seen: 9,
                ..base.clone()
            },
            Lesson {
                id: "faded".into(),
                last_seen_at: "2025-10-01T00:00:00Z".into(),
                ..base.clone()
            },
            Lesson {
                id: "reviewers".into(),
                agent: "reviewer".into(),
                ..base.clone()
            },
            Lesson {
                id: "other".into(),
                repo: "acme/api".into(),
                ..base.clone()
            },
        ];
        let guides = vec![common::guide::Record {
            repo: "acme/web".into(),
            state: "open".into(),
            pr: 40,
            path: "AGENTS.md".into(),
            lesson_ids: vec!["ready".into()],
            ..Default::default()
        }];
        let v = lessons_view(&lessons, &guides, "acme/web", now);
        let status: std::collections::BTreeMap<String, String> = v["lessons"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| {
                (
                    l["id"].as_str().unwrap().into(),
                    l["status"].as_str().unwrap().into(),
                )
            })
            .collect();
        assert_eq!(
            status.len(),
            5,
            "the reviewer's and the other repository's are not listed"
        );
        assert_eq!(status["ready"], "recalled");
        assert_eq!(status["guide"], "in_guide");
        assert_eq!(status["against"], "contradicted");
        assert_eq!(status["off"], "off");
        assert_eq!(status["faded"], "faded");
        let ready = v["lessons"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["id"] == "ready")
            .unwrap();
        assert_eq!(ready["guide_ready"], true);
        assert_eq!(
            ready["evidence"][0],
            "PR #12 in people's words: \"Use the shared client\""
        );
        assert_eq!(
            v["guide_prs"][0]["url"],
            "https://github.com/acme/web/pull/40"
        );
        // All repositories.
        assert_eq!(
            lessons_view(&lessons, &guides, "", now)["lessons"]
                .as_array()
                .unwrap()
                .len(),
            6
        );
    }

    #[test]
    fn repo_filters_are_owner_and_name_only() {
        assert_eq!(repo_filter(None), Ok(String::new()));
        assert_eq!(repo_filter(Some(" ")), Ok(String::new()));
        assert_eq!(repo_filter(Some("acme/web")), Ok("acme/web".into()));
        assert_eq!(repo_filter(Some("*")), Err(StatusCode::BAD_REQUEST));
        assert_eq!(repo_filter(Some("acme")), Err(StatusCode::BAD_REQUEST));
        assert_eq!(repo_filter(Some("a/b/c")), Err(StatusCode::BAD_REQUEST));
        assert_eq!(repo_filter(Some("../x")), Err(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn days_are_bounded() {
        assert_eq!(days_of(None), 30);
        assert_eq!(days_of(Some(0)), 1);
        assert_eq!(days_of(Some(9999)), 180);
    }

    #[test]
    fn pins_name_a_real_agent_and_arm() {
        assert_eq!(check_pin("reviewer", Some("focused")), Ok(()));
        assert_eq!(check_pin("reviewer", None), Ok(()));
        assert_eq!(
            check_pin("reviewer", Some("bogus")),
            Err(StatusCode::BAD_REQUEST)
        );
        // The coder has variants now, so they can be pinned too.
        assert_eq!(check_pin("coder", None), Ok(()));
        assert_eq!(check_pin("coder", Some("read_first")), Ok(()));
        assert_eq!(check_pin("planner", None), Err(StatusCode::BAD_REQUEST));
        assert_eq!(check_pin("nope", Some("x")), Err(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn closed_prs_are_read_from_runs() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let run = |url: &str, merged: &str, closed: &str| {
            let mut i = Item::new();
            i.insert("pr_url".into(), A::S(url.into()));
            if !merged.is_empty() {
                i.insert("merged_at".into(), A::S(merged.into()));
            }
            if !closed.is_empty() {
                i.insert("pr_closed_at".into(), A::S(closed.into()));
            }
            i
        };
        let got = closed_prs_of(&[
            run(
                "https://github.com/acme/web/pull/3",
                "2026-11-05T00:00:00Z",
                "",
            ),
            run(
                "https://github.com/acme/web/pull/4",
                "",
                "2026-11-04T00:00:00Z",
            ),
            run("https://github.com/acme/web/pull/5", "", ""),
            run("not a url", "2026-11-05T00:00:00Z", ""),
        ]);
        assert_eq!(got.len(), 2);
        assert_eq!((got[0].repo.as_str(), got[0].pr), ("acme/web", 3));
        assert_eq!(got[1].closed_at, "2026-11-04T00:00:00Z");
        assert_eq!(got[0].variant, "");

        // The coder arm its run picked comes along; a reviewer arm or a
        // made-up one does not.
        let with = |arm: &str| {
            let mut i = run(
                "https://github.com/acme/web/pull/6",
                "2026-11-05T00:00:00Z",
                "",
            );
            i.insert("prompt_variant".into(), A::S(arm.into()));
            i
        };
        let got = closed_prs_of(&[with("read_first"), with("focused"), with("nope")]);
        let arms: Vec<&str> = got.iter().map(|p| p.variant.as_str()).collect();
        assert_eq!(arms, vec!["read_first", "", ""]);
    }

    #[test]
    fn change_ids_are_ulids() {
        assert!(valid_change_id(&ulid::Ulid::new().to_string()));
        assert!(!valid_change_id("../../x"));
        assert!(!valid_change_id(""));
    }
}
