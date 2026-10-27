//! Answering a run's questions from the dashboard (or the API).
//!
//! A run that stops as `needs_input` keeps the questions it asked on its
//! record (`common::run_questions`). Answering them here:
//!
//! 1. checks the person may start runs, the run is still waiting, and the
//!    answers are for its questions (at least one written);
//! 2. claims the run in one conditional write: `needs_input` becomes
//!    `retried`, with the answers and the id of the run that continues, so a
//!    second answer or a Retry racing this one is refused, never doubled;
//! 3. starts the ticket again with the answers on the message: the new
//!    run's triage and planner read them word for word;
//! 4. posts the answers on the ticket or issue, under CoderHelm's question,
//!    as "<name> answered in CoderHelm:".
//!
//! Retry stays for a ticket edited directly.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{extract::State, http::StatusCode, Extension, Json};
use common::run_questions::{self, Answer};
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::{Claims, TicketSource, WorkerMessage};
use crate::AppState;

type Item = HashMap<String, AttributeValue>;
type Refusal = (StatusCode, Json<Value>);

/// Answers to a run's questions.
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct AnswerRun {
    /// One entry per question answered. Questions left out (or blank) stay
    /// open; at least one needs an answer.
    pub answers: Vec<GivenAnswer>,
}

/// The answer to one question.
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct GivenAnswer {
    /// The question's `id` from the run's `questions`.
    #[schema(example = "q1")]
    pub question_id: String,
    /// The answer, in plain words. At most 4000 characters.
    #[schema(example = "Production only. Staging keeps the old limit.")]
    pub answer: String,
}

/// What answering did.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AnswerRunResult {
    /// `continuing`: the ticket is queued again with the answers.
    #[schema(example = "continuing")]
    pub status: String,
    /// The id of the run that continues the work. It shows in
    /// `GET /api/runs/{run_id}` once it starts, within seconds.
    #[schema(example = "01JB8Z3N5Q7S9V1X3Z5B7D9F1H")]
    pub run_id: String,
    /// Whether the answers were posted on the ticket or issue.
    #[schema(example = true)]
    pub posted: bool,
    /// The reply on the ticket or issue, when its address is known.
    #[schema(example = "https://github.com/acme/web/issues/42#issuecomment-2211")]
    pub comment_url: Option<String>,
}

fn refuse(status: StatusCode, message: &str) -> Refusal {
    (
        status,
        Json(json!({ "error": message, "code": crate::openapi::default_code(status) })),
    )
}

fn s(item: &Item, key: &str) -> String {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// Why a run can't take answers now, in words for a person; `None` when it
/// can. Pure, so the rules are tested on their own.
pub fn not_answerable(status: &str, has_questions: bool) -> Option<String> {
    if status != common::run_status::NEEDS_INPUT {
        return Some(match status {
            "retried" => {
                "These questions were answered already, or the run was retried. Refresh to see \
                 the run that continued."
                    .to_string()
            }
            "" => "This run isn't waiting for an answer.".to_string(),
            other => format!(
                "This run isn't waiting for an answer any more (it is {}). Refresh to see where \
                 it is.",
                other.replace('_', " ")
            ),
        });
    }
    if !has_questions {
        return Some(
            "This run didn't leave questions to answer here. Add the detail to the ticket, then \
             retry."
                .to_string(),
        );
    }
    None
}

/// POST /api/runs/:run_id/answer — answer a run's questions and continue.
#[utoipa::path(
    post,
    path = "/api/runs/{run_id}/answer",
    tag = "Runs",
    operation_id = "answerRun",
    summary = "Answer a run's questions",
    description = "Answers the questions a `needs_input` run asked (its `questions`) and continues \
        the work: the answers are posted on the ticket or issue as a reply from you, kept on the \
        run, and the ticket starts again as a new run whose plan reads them word for word. The \
        old run becomes `retried` with `continued_run_id` set. A run that isn't `needs_input` \
        any more (answered, retried or closed meanwhile) is refused with `409`.",
    params(("run_id" = String, Path, description = "The run's id.", example = "01JB8X6F4M2C9QZ3T7V5W1K0HA")),
    request_body = AnswerRun,
    responses(
        (status = 200, description = "The work continues with the answers.", body = AnswerRunResult),
        (status = 400, description = "No answer written, an answer too long, or a question the run didn't ask.", body = crate::openapi::ErrorBody),
        (status = 404, description = "No run with this id in the team.", body = crate::openapi::ErrorBody),
        (status = 409, description = "The run isn't waiting for an answer, or asked nothing to answer here.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["runs"])),
)]
pub async fn answer_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
    Json(body): Json<AnswerRun>,
) -> Result<Json<AnswerRunResult>, Refusal> {
    // Members and up, as Retry (checked again in `answer`).
    claims.require_role(1).map_err(members_only)?;
    answer(&state, &claims, &run_id, body).await.map(Json)
}

fn members_only(code: StatusCode) -> Refusal {
    refuse(
        code,
        "Only members can answer a run's questions. Ask an admin for member access.",
    )
}

/// The handler's work, callable from tests.
pub async fn answer(
    state: &AppState,
    claims: &Claims,
    run_id: &str,
    body: AnswerRun,
) -> Result<AnswerRunResult, Refusal> {
    claims.require_role(1).map_err(members_only)?;
    if super::github_webhook::check_run_budget(state, &claims.team_id)
        .await
        .is_some()
    {
        return Err(refuse(
            StatusCode::TOO_MANY_REQUESTS,
            "The monthly token limit is reached, so no new work can start.",
        ));
    }

    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(claims.team_id.clone()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| {
            error!(run_id, error = %e, "answer: could not read the run");
            refuse(
                StatusCode::INTERNAL_SERVER_ERROR,
                "The run couldn't be read. Try again.",
            )
        })?
        .item
        .ok_or_else(|| refuse(StatusCode::NOT_FOUND, "No run with this id in the team."))?;

    let asked = super::api::run_asked(&item);
    let questions = asked
        .as_ref()
        .map(|a| a.questions.clone())
        .unwrap_or_default();
    if let Some(why) = not_answerable(&s(&item, "status"), !questions.is_empty()) {
        return Err(refuse(StatusCode::CONFLICT, &why));
    }

    let profile = super::run_close::profile_name(state, claims).await;
    let person = super::run_close::closer(claims, profile.as_deref());
    let now = chrono::Utc::now().to_rfc3339();
    let given: Vec<(String, String)> = body
        .answers
        .into_iter()
        .map(|a| (a.question_id, a.answer))
        .collect();
    let answers = run_questions::validate(&questions, &given, &person.public, &now)
        .map_err(|e| refuse(StatusCode::BAD_REQUEST, &e.message()))?;

    let mut ticket = super::api::retry_message(state, claims, &item)
        .await
        .map_err(|code| refuse(code, "The run's ticket couldn't be read. Try again."))?;
    let new_run_id = ulid::Ulid::new().to_string();
    // Answers from earlier stops of the ticket reach the run through its
    // history (the worker merges them in).
    ticket.answers = answers.clone();
    ticket.new_run_id = Some(new_run_id.clone());

    claim(
        state,
        &claims.team_id,
        run_id,
        &answers,
        &new_run_id,
        &person.email,
        &now,
    )
    .await?;

    let queued = match serde_json::to_string(&WorkerMessage::Ticket(ticket.clone())) {
        Ok(msg) => {
            super::api::delete_ticket_lock(state, &claims.team_id, &ticket.ticket_id).await;
            state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(msg)
                .send()
                .await
                .map_err(|e| e.to_string())
        }
        Err(e) => Err(e.to_string()),
    };
    if let Err(e) = queued {
        error!(run_id, error = %e, "answer: could not queue the ticket again");
        release(state, &claims.team_id, run_id).await;
        return Err(refuse(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Your answers couldn't be sent on. Nothing was posted; try again.",
        ));
    }

    let asked_at = asked.and_then(|a| a.comment_url);
    let comment_url = post_reply(state, &item, &person.public, &answers, asked_at.as_deref()).await;
    let posted = comment_url.is_some();
    if let Some(Some(url)) = &comment_url {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", AttributeValue::S(claims.team_id.clone()))
            .key("run_id", AttributeValue::S(run_id.to_string()))
            .update_expression("SET answers_url = :u")
            .expression_attribute_values(":u", AttributeValue::S(url.clone()))
            .send()
            .await;
    }
    info!(
        run_id,
        new_run_id,
        answered = answers.len(),
        posted,
        "Run's questions answered — continuing"
    );
    Ok(AnswerRunResult {
        status: "continuing".to_string(),
        run_id: new_run_id,
        posted,
        comment_url: comment_url.flatten(),
    })
}

/// Marks the run answered, only while it still waits: two answers or an
/// answer and a Retry at once leave exactly one run going.
async fn claim(
    state: &AppState,
    team_id: &str,
    run_id: &str,
    answers: &[Answer],
    new_run_id: &str,
    by: &str,
    now: &str,
) -> Result<(), Refusal> {
    let json = serde_json::to_string(answers).unwrap_or_else(|_| "[]".to_string());
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .update_expression(
            "SET #status = :s, status_run_id = :sri, updated_at = :t, #a = :a, \
             continued_run_id = :nr, answered_by = :by",
        )
        .condition_expression("#status = :waiting")
        .expression_attribute_names("#status", "status")
        .expression_attribute_names("#a", run_questions::FIELD_ANSWERS)
        .expression_attribute_values(":s", AttributeValue::S("retried".into()))
        .expression_attribute_values(":sri", AttributeValue::S(format!("retried#{run_id}")))
        .expression_attribute_values(":t", AttributeValue::S(now.to_string()))
        .expression_attribute_values(":a", AttributeValue::S(json))
        .expression_attribute_values(":nr", AttributeValue::S(new_run_id.to_string()))
        .expression_attribute_values(":by", AttributeValue::S(by.to_string()))
        .expression_attribute_values(
            ":waiting",
            AttributeValue::S(common::run_status::NEEDS_INPUT.into()),
        )
        .send()
        .await;
    match res {
        Ok(_) => Ok(()),
        Err(e)
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
        {
            Err(refuse(
                StatusCode::CONFLICT,
                "This run moved on while you were writing (answered, retried or closed). \
                 Refresh to see where it is.",
            ))
        }
        Err(e) => {
            error!(run_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "answer: could not mark the run answered");
            Err(refuse(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Your answers couldn't be saved. Try again.",
            ))
        }
    }
}

/// Puts the run back to waiting when the ticket couldn't be queued.
async fn release(state: &AppState, team_id: &str, run_id: &str) {
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .update_expression(
            "SET #status = :w, status_run_id = :sri REMOVE #a, continued_run_id, answered_by",
        )
        .condition_expression("#status = :retried")
        .expression_attribute_names("#status", "status")
        .expression_attribute_names("#a", run_questions::FIELD_ANSWERS)
        .expression_attribute_values(
            ":w",
            AttributeValue::S(common::run_status::NEEDS_INPUT.into()),
        )
        .expression_attribute_values(":sri", AttributeValue::S(format!("needs_input#{run_id}")))
        .expression_attribute_values(":retried", AttributeValue::S("retried".into()))
        .send()
        .await
    {
        warn!(run_id, error = %e, "answer: could not put the run back to waiting");
    }
}

/// Posts the answers on the run's ticket or issue. `None` when nothing was
/// posted; `Some(None)` when posted but its address isn't known.
async fn post_reply(
    state: &AppState,
    item: &Item,
    author: &str,
    answers: &[Answer],
    asked_at: Option<&str>,
) -> Option<Option<String>> {
    let run_id = s(item, "run_id");
    let source = match s(item, "ticket_source").as_str() {
        "jira" => TicketSource::Jira,
        "alert" => TicketSource::Alert,
        _ => TicketSource::Github,
    };
    match source {
        TicketSource::Jira => {
            let ticket = s(item, "ticket_id");
            let config = state
                .dynamo
                .get_item()
                .table_name(&state.config.jira_config_table_name)
                .key("pk", AttributeValue::S(s(item, "team_id")))
                .key("sk", AttributeValue::S("JIRA#config".into()))
                .send()
                .await
                .ok()
                .and_then(|o| o.item)?;
            let url = Some(s(&config, "add_comment_url")).filter(|u| !u.is_empty())?;
            let site = s(&config, "site_url");
            let payload = json!({
                "issueKey": ticket,
                "comment": run_questions::reply(author, answers, asked_at, false),
                "commentType": "answer",
                "title": "Answered in CoderHelm",
            });
            match state.http.post(&url).json(&payload).send().await {
                Ok(r) if r.status().is_success() => {
                    let id = r.json::<Value>().await.ok().and_then(|v| match &v["id"] {
                        Value::String(s) => Some(s.clone()),
                        Value::Number(n) => Some(n.to_string()),
                        _ => None,
                    });
                    Some(jira_link(&site, &ticket, id.as_deref()))
                }
                Ok(r) => {
                    warn!(
                        run_id,
                        status = r.status().as_u16(),
                        "answer: Jira refused the reply"
                    );
                    None
                }
                Err(e) => {
                    warn!(run_id, error = %e, "answer: Jira reply failed");
                    None
                }
            }
        }
        TicketSource::Github => {
            let issue = item
                .get("issue_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            let repo = s(item, "repo");
            let (owner, name) = repo.split_once('/')?;
            if issue == 0 {
                return None;
            }
            let installation = item
                .get("installation_id")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            let token = crate::auth::github_app::get_installation_token(state, installation)
                .await
                .map_err(|e| warn!(run_id, error = %e, "answer: no GitHub token for the reply"))
                .ok()?;
            let res = state
                .http
                .post(format!(
                    "https://api.github.com/repos/{owner}/{name}/issues/{issue}/comments"
                ))
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/vnd.github+json")
                .header("User-Agent", "Coderhelm-bot")
                .json(&json!({ "body": run_questions::reply(author, answers, asked_at, true) }))
                .send()
                .await;
            match res {
                Ok(r) if r.status().is_success() => Some(
                    r.json::<Value>()
                        .await
                        .ok()
                        .and_then(|v| v["html_url"].as_str().map(str::to_string)),
                ),
                Ok(r) => {
                    warn!(
                        run_id,
                        status = r.status().as_u16(),
                        "answer: GitHub refused the reply"
                    );
                    None
                }
                Err(e) => {
                    warn!(run_id, error = %e, "answer: GitHub reply failed");
                    None
                }
            }
        }
        // An alert has no thread to reply in.
        TicketSource::Alert => None,
    }
}

/// The address of a Jira comment, or of the ticket when the id is unknown.
fn jira_link(site: &str, ticket: &str, id: Option<&str>) -> Option<String> {
    let site = site.trim().trim_end_matches('/');
    if site.is_empty() || ticket.is_empty() {
        return None;
    }
    Some(match id.filter(|i| !i.is_empty()) {
        Some(id) => format!("{site}/browse/{ticket}?focusedCommentId={id}"),
        None => format!("{site}/browse/{ticket}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_waiting_run_with_questions_takes_answers() {
        assert_eq!(not_answerable("needs_input", true), None);
        assert!(not_answerable("needs_input", false)
            .unwrap()
            .contains("Add the detail to the ticket"));
        assert!(not_answerable("retried", true)
            .unwrap()
            .contains("answered already"));
        assert!(not_answerable("running", true)
            .unwrap()
            .contains("(it is running)"));
        assert!(not_answerable("needs_outside_change", true)
            .unwrap()
            .contains("needs outside change"));
    }

    #[test]
    fn jira_links_point_at_the_comment() {
        assert_eq!(
            jira_link("https://example.atlassian.net/", "ABC-1", Some("10001")).as_deref(),
            Some("https://example.atlassian.net/browse/ABC-1?focusedCommentId=10001")
        );
        assert_eq!(
            jira_link("https://example.atlassian.net", "ABC-1", None).as_deref(),
            Some("https://example.atlassian.net/browse/ABC-1")
        );
        assert_eq!(jira_link("", "ABC-1", Some("1")), None);
    }

    // ── The handler against a stand-in for DynamoDB, SQS and the Jira app ──

    use axum::body::Bytes;
    use axum::http::HeaderMap;
    use std::sync::Mutex;

    /// What the stand-in holds and what it was asked.
    #[derive(Default)]
    struct Fake {
        /// One item per table, in DynamoDB's JSON.
        items: HashMap<String, Value>,
        /// Conditional updates fail (the run moved on meanwhile).
        conditions_fail: bool,
        /// (operation, request body), in order.
        calls: Vec<(String, Value)>,
    }

    impl Fake {
        fn ops(&self, op: &str) -> Vec<Value> {
            self.calls
                .iter()
                .filter(|(o, _)| o == op)
                .map(|(_, b)| b.clone())
                .collect()
        }
    }

    async fn serve(fake: Arc<Mutex<Fake>>) -> String {
        async fn handle(
            axum::extract::State(fake): axum::extract::State<Arc<Mutex<Fake>>>,
            uri: axum::http::Uri,
            headers: HeaderMap,
            body: Bytes,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
            let op = headers
                .get("x-amz-target")
                .and_then(|v| v.to_str().ok())
                .map(|t| t.rsplit('.').next().unwrap_or("").to_string())
                .unwrap_or_else(|| uri.path().to_string());
            let mut f = fake.lock().unwrap();
            f.calls.push((op.clone(), body.clone()));
            let amz = |status: u16, v: Value| {
                (
                    StatusCode::from_u16(status).unwrap(),
                    [("content-type", "application/x-amz-json-1.0")],
                    v.to_string(),
                )
                    .into_response()
            };
            match op.as_str() {
                "GetItem" => {
                    let table = body["TableName"].as_str().unwrap_or("");
                    match f.items.get(table) {
                        Some(item) => amz(200, json!({ "Item": item })),
                        None => amz(200, json!({})),
                    }
                }
                "UpdateItem" if f.conditions_fail && body.get("ConditionExpression").is_some() => {
                    amz(
                        400,
                        json!({
                            "__type": "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
                            "message": "The conditional request failed"
                        }),
                    )
                }
                "UpdateItem" | "DeleteItem" | "PutItem" => amz(200, json!({})),
                "Query" => amz(200, json!({ "Items": [], "Count": 0, "ScannedCount": 0 })),
                "SendMessage" => amz(200, json!({ "MessageId": "m-1" })),
                "/jira-comment" => (
                    StatusCode::CREATED,
                    [("content-type", "application/json")],
                    json!({ "id": "10042" }).to_string(),
                )
                    .into_response(),
                _ => amz(400, json!({ "__type": "UnknownOperation" })),
            }
        }
        let app = axum::Router::new().fallback(handle).with_state(fake);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        url
    }

    fn state_for(url: &str) -> AppState {
        let creds = aws_sdk_dynamodb::config::SharedCredentialsProvider::new(
            aws_sdk_dynamodb::config::Credentials::new("test", "test", None, None, "test"),
        );
        let aws = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(url)
            .credentials_provider(creds)
            .build();
        let secrets: crate::models::Secrets =
            serde_json::from_value(json!({ "jwt_secret": "test" })).unwrap();
        AppState {
            dynamo: aws_sdk_dynamodb::Client::new(&aws),
            sqs: aws_sdk_sqs::Client::new(&aws),
            ses: aws_sdk_sesv2::Client::new(&aws),
            s3: aws_sdk_s3::Client::new(&aws),
            cognito: aws_sdk_cognitoidentityprovider::Client::new(&aws),
            lambda: aws_sdk_lambda::Client::new(&aws),
            kms: aws_sdk_kms::Client::new(&aws),
            sts: aws_sdk_sts::Client::new(&aws),
            bedrock: aws_sdk_bedrockruntime::Client::new(&aws),
            scheduler: common::plugins::schedule::aws_sdk_scheduler::Client::new(&aws),
            http: reqwest::Client::new(),
            secrets,
            config: crate::models::Config {
                runs_table_name: "runs".into(),
                jira_config_table_name: "jira".into(),
                users_table_name: "users".into(),
                teams_table_name: "teams".into(),
                settings_table_name: "settings".into(),
                analytics_table_name: "analytics".into(),
                ticket_queue_url: format!("{url}/queue/tickets"),
                ..Default::default()
            },
            cognito_client_secret: String::new(),
        }
    }

    fn claims(role: &str) -> Claims {
        serde_json::from_value(json!({
            "sub": "USER#u1",
            "team_id": "TEAM#t1",
            "email": "pat@example.com",
            "role": role,
            "exp": 4_000_000_000u64,
            "iat": 1u64,
        }))
        .unwrap()
    }

    /// A Jira run stopped on two questions, the team's Jira app, and the
    /// member's profile.
    async fn fixture(status: &str) -> (Arc<Mutex<Fake>>, AppState) {
        let fake = Arc::new(Mutex::new(Fake::default()));
        let url = serve(fake.clone()).await;
        let asked = json!({
            "questions": [
                {"id": "q1", "question": "Which environments get the new limit?",
                 "why": "Each environment has its own value.",
                 "suggestions": ["Production only", "Every environment"]},
                {"id": "q2", "question": "What is the new limit?"}
            ],
            "asked_at": "2026-10-27T10:00:00Z",
            "comment_url": "https://example.atlassian.net/browse/ABC-7?focusedCommentId=10001"
        });
        {
            let mut f = fake.lock().unwrap();
            f.items.insert(
                "runs".into(),
                json!({
                    "team_id": {"S": "TEAM#t1"},
                    "run_id": {"S": "01JB8X6F4M2C9QZ3T7V5W1K0HA"},
                    "status": {"S": status},
                    "ticket_source": {"S": "jira"},
                    "ticket_id": {"S": "ABC-7"},
                    "title": {"S": "Raise the sign-up rate limit"},
                    "repo": {"S": "acme/web"},
                    "installation_id": {"N": "7"},
                    "ticket_body": {"S": "Raise the limit."},
                    "questions": {"S": asked.to_string()}
                }),
            );
            f.items.insert(
                "jira".into(),
                json!({
                    "add_comment_url": {"S": format!("{url}/jira-comment")},
                    "site_url": {"S": "https://example.atlassian.net"}
                }),
            );
            f.items
                .insert("users".into(), json!({ "name": {"S": "Pat Doe"} }));
        }
        (fake, state_for(&url))
    }

    fn body(answers: &[(&str, &str)]) -> AnswerRun {
        AnswerRun {
            answers: answers
                .iter()
                .map(|(q, a)| GivenAnswer {
                    question_id: q.to_string(),
                    answer: a.to_string(),
                })
                .collect(),
        }
    }

    const RUN: &str = "01JB8X6F4M2C9QZ3T7V5W1K0HA";

    #[tokio::test]
    async fn an_answer_is_kept_posted_on_the_ticket_and_continues_the_run() {
        let (fake, state) = fixture("needs_input").await;
        let out = answer(
            &state,
            &claims("member"),
            RUN,
            body(&[
                ("q1", "  Production only.\nStaging keeps 100.  "),
                ("q2", ""),
            ]),
        )
        .await
        .unwrap();
        assert_eq!(out.status, "continuing");
        assert!(ulid::Ulid::from_string(&out.run_id).is_ok());
        assert!(out.posted);
        assert_eq!(
            out.comment_url.as_deref(),
            Some("https://example.atlassian.net/browse/ABC-7?focusedCommentId=10042")
        );

        let f = fake.lock().unwrap();
        // The run is claimed only while it still waits, with the answer.
        let updates = f.ops("UpdateItem");
        let claim = &updates[0];
        assert_eq!(claim["ConditionExpression"], "#status = :waiting");
        assert_eq!(
            claim["ExpressionAttributeValues"][":waiting"]["S"],
            "needs_input"
        );
        assert_eq!(claim["ExpressionAttributeValues"][":s"]["S"], "retried");
        assert_eq!(claim["ExpressionAttributeValues"][":nr"]["S"], out.run_id);
        let kept: Vec<Answer> = serde_json::from_str(
            claim["ExpressionAttributeValues"][":a"]["S"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(kept.len(), 1, "a blank answer leaves its question open");
        assert_eq!(kept[0].question, "Which environments get the new limit?");
        assert_eq!(kept[0].answer, "Production only.\nStaging keeps 100.");
        assert_eq!(kept[0].author, "Pat Doe");

        // The ticket goes back on the queue as a new run with the answer.
        let sent = f.ops("SendMessage");
        assert_eq!(sent.len(), 1);
        let msg: Value = serde_json::from_str(sent[0]["MessageBody"].as_str().unwrap()).unwrap();
        assert_eq!(msg["type"], "ticket");
        assert_eq!(msg["ticket_id"], "ABC-7");
        assert_eq!(msg["body"], "Raise the limit.");
        assert_eq!(msg["new_run_id"], out.run_id);
        assert_eq!(
            msg["answers"][0]["answer"],
            "Production only.\nStaging keeps 100."
        );

        // The reply is on the ticket, from the person, under the question.
        let posted = f.ops("/jira-comment");
        assert_eq!(posted.len(), 1);
        assert_eq!(posted[0]["issueKey"], "ABC-7");
        let text = posted[0]["comment"].as_str().unwrap();
        assert!(text.starts_with("Pat Doe answered in CoderHelm:"), "{text}");
        assert!(text.contains("Q: Which environments get the new limit?"));
        assert!(text.contains("A: Production only.\nStaging keeps 100."));
        assert!(text.contains("focusedCommentId=10001"));
        assert!(!text.contains("What is the new limit?"));
        // And its address is kept on the run.
        assert_eq!(
            updates.last().unwrap()["ExpressionAttributeValues"][":u"]["S"],
            "https://example.atlassian.net/browse/ABC-7?focusedCommentId=10042"
        );
    }

    #[tokio::test]
    async fn a_viewer_cannot_answer() {
        let (fake, state) = fixture("needs_input").await;
        let err = answer(
            &state,
            &claims("viewer"),
            RUN,
            body(&[("q1", "Production")]),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert!(
            fake.lock().unwrap().calls.is_empty(),
            "nothing read or written"
        );
    }

    #[tokio::test]
    async fn blank_or_unknown_answers_are_refused_before_anything_changes() {
        let (fake, state) = fixture("needs_input").await;
        let err = answer(&state, &claims("member"), RUN, body(&[("q1", "   ")]))
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1["error"].as_str().unwrap().contains("at least one"));
        let err = answer(&state, &claims("member"), RUN, body(&[("q9", "yes")]))
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        let f = fake.lock().unwrap();
        assert!(f.ops("UpdateItem").is_empty());
        assert!(f.ops("SendMessage").is_empty());
        assert!(f.ops("/jira-comment").is_empty());
    }

    #[tokio::test]
    async fn a_run_that_moved_on_is_refused() {
        // Already answered (or retried) when the page was opened.
        let (fake, state) = fixture("retried").await;
        let err = answer(
            &state,
            &claims("member"),
            RUN,
            body(&[("q1", "Production")]),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::CONFLICT);
        assert!(fake.lock().unwrap().ops("SendMessage").is_empty());

        // Moved on between the read and the write: the claim fails.
        let (fake, state) = fixture("needs_input").await;
        fake.lock().unwrap().conditions_fail = true;
        let err = answer(
            &state,
            &claims("member"),
            RUN,
            body(&[("q1", "Production")]),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::CONFLICT);
        assert!(err.1["error"].as_str().unwrap().contains("moved on"));
        let f = fake.lock().unwrap();
        assert!(f.ops("SendMessage").is_empty(), "no second run");
        assert!(f.ops("/jira-comment").is_empty(), "nothing posted");
    }

    #[tokio::test]
    async fn a_run_without_questions_takes_no_answers() {
        let (fake, state) = fixture("needs_input").await;
        if let Some(run) = fake.lock().unwrap().items.get_mut("runs") {
            run.as_object_mut().unwrap().remove("questions");
        }
        let err = answer(
            &state,
            &claims("member"),
            RUN,
            body(&[("q1", "Production")]),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::CONFLICT);
    }

    #[test]
    fn answering_is_a_runs_action_for_members() {
        use crate::permissions::{resolve_access, Access, Holder};
        assert!(matches!(
            resolve_access(&Holder::builtin("member"), Some("run_answer::answer_run")),
            Access::Allow { .. }
        ));
        match resolve_access(&Holder::builtin("viewer"), Some("run_answer::answer_run")) {
            Access::Deny { capability } => assert_eq!(capability.key, "runs"),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            crate::permissions::routed_handler("POST", "/api/runs/:run_id/answer"),
            Some("run_answer::answer_run")
        );
    }
}
