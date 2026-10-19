//! Airflow on Amazon MWAA, through the team's log reader role and MWAA's
//! InvokeRestApi.
//!
//! GET only, enforced twice: the trait below has no way to send anything
//! else, and every path is checked against a short list (DAGs, DAG runs,
//! task instances, task logs, import errors, health) before it is sent.
//! Connections, variables and config are never read. The role's own policy
//! only allows Airflow's Viewer role, which can't trigger or change anything.

use serde_json::{Map, Value};

use super::aws::Session;
use super::redact::{hidden_terms, redact_all, redact_text};
use super::StepError;
use crate::live::store::default_hidden_fields;

const MAX_LIMIT: u64 = 100;

fn refused(message: impl Into<String>) -> StepError {
    StepError::new(
        "read_only",
        message,
        "Only DAGs, DAG runs, task instances, task logs, import errors and health can be read.",
    )
}

/// An id Airflow uses in a path (a DAG id, run id or task id), or `~` for
/// every DAG where Airflow allows it.
fn check_id(what: &str, s: &str) -> Result<String, StepError> {
    let s = s.trim();
    let ok = !s.is_empty()
        && s.len() <= 250
        && s != "."
        && s != ".."
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "._-:+~".contains(c));
    if ok {
        Ok(s.to_string())
    } else {
        Err(refused(format!("That {what} isn't valid.")))
    }
}

/// Whether `path` is one this module may read. Paths are built by the
/// functions below; this is the last check before anything is sent.
pub fn allowed_path(path: &str) -> bool {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let id = |s: &str| check_id("id", s).is_ok();
    match parts.as_slice() {
        ["health"] | ["version"] | ["importErrors"] | ["dags"] => true,
        ["dags", d] | ["dags", d, "details"] | ["dags", d, "tasks"] => id(d) && *d != "~",
        ["dags", d, "dagRuns"] => id(d),
        ["dags", d, "dagRuns", r] => id(d) && id(r) && *d != "~",
        ["dags", d, "dagRuns", r, "taskInstances"] => id(d) && id(r),
        ["dags", d, "dagRuns", r, "taskInstances", t] => id(d) && id(r) && id(t) && *d != "~",
        ["dags", d, "dagRuns", r, "taskInstances", t, "logs", n] => {
            id(d) && id(r) && id(t) && *d != "~" && n.parse::<u32>().is_ok_and(|n| n > 0 && n < 100)
        }
        _ => false,
    }
}

/// A checked request: environment, path and query parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct Get {
    pub environment: String,
    pub path: String,
    pub query: Map<String, Value>,
}

impl Get {
    pub fn describe(&self) -> String {
        let q: Vec<String> = self
            .query
            .iter()
            .map(|(k, v)| {
                format!(
                    "{k}={}",
                    v.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| v.to_string())
                )
            })
            .collect();
        format!(
            "GET {} {}{}{}",
            self.environment,
            self.path,
            if q.is_empty() { "" } else { "?" },
            q.join("&")
        )
    }
}

/// The environment to read: the one asked for if the team shared it, or the
/// only shared one.
pub fn pick_environment(asked: Option<&str>, shared: &[String]) -> Result<String, StepError> {
    match (asked.map(str::trim).filter(|a| !a.is_empty()), shared) {
        (Some(a), _) if shared.iter().any(|s| s == a) => Ok(a.to_string()),
        (Some(a), _) => Err(refused(format!(
            "{a} isn't an Airflow environment the team shared ({}).",
            shared.join(", ")
        ))),
        (None, [one]) => Ok(one.clone()),
        (None, []) => Err(refused("No Airflow environment is shared.")),
        (None, _) => Err(refused(format!(
            "Say which Airflow environment: {}.",
            shared.join(", ")
        ))),
    }
}

fn limit(v: &Value) -> u64 {
    v.as_u64().unwrap_or(25).clamp(1, MAX_LIMIT)
}

fn text_param(v: &Value, what: &str) -> Result<Option<String>, StepError> {
    match v.as_str().map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(None),
        Some(s) if s.len() <= 100 && s.chars().all(|c| !c.is_control()) => Ok(Some(s.to_string())),
        Some(_) => Err(refused(format!("That {what} isn't valid."))),
    }
}

const RUN_STATES: &[&str] = &["queued", "running", "success", "failed"];

/// Builds a read from a tool call. `what` is one of `dags`, `dag_runs`,
/// `task_instances`, `task_log`, `import_errors`, `health`.
pub fn build(what: &str, input: &Value, shared: &[String]) -> Result<Get, StepError> {
    let environment = pick_environment(input["environment"].as_str(), shared)?;
    let mut query = Map::new();
    let path = match what {
        "health" => "/health".to_string(),
        "import_errors" => {
            query.insert("limit".into(), Value::from(limit(&input["limit"])));
            "/importErrors".to_string()
        }
        "dags" => {
            query.insert("limit".into(), Value::from(limit(&input["limit"])));
            if let Some(p) = text_param(&input["search"], "search")? {
                query.insert("dag_id_pattern".into(), Value::from(p));
            }
            if let Some(only) = input["only_active"].as_bool() {
                query.insert("only_active".into(), Value::from(only));
            }
            match input["dag_id"]
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                Some(d) => {
                    query.clear();
                    format!("/dags/{}", check_id("DAG id", d)?)
                }
                None => "/dags".to_string(),
            }
        }
        "dag_runs" => {
            let dag = input["dag_id"].as_str().unwrap_or("~");
            query.insert("limit".into(), Value::from(limit(&input["limit"])));
            query.insert("order_by".into(), Value::from("-execution_date"));
            if let Some(st) = text_param(&input["state"], "state")? {
                if !RUN_STATES.contains(&st.as_str()) {
                    return Err(refused(format!(
                        "A run state is one of {}.",
                        RUN_STATES.join(", ")
                    )));
                }
                query.insert("state".into(), Value::from(st));
            }
            if let Some(since) = text_param(&input["since"], "start time")? {
                if chrono::DateTime::parse_from_rfc3339(&since).is_err() {
                    return Err(refused("`since` is a time like 2026-01-31T14:00:00Z."));
                }
                query.insert("execution_date_gte".into(), Value::from(since));
            }
            format!("/dags/{}/dagRuns", check_id("DAG id", dag)?)
        }
        "task_instances" => {
            let dag = check_id("DAG id", input["dag_id"].as_str().unwrap_or(""))?;
            let run = check_id("run id", input["run_id"].as_str().unwrap_or(""))?;
            query.insert("limit".into(), Value::from(limit(&input["limit"])));
            if let Some(st) = text_param(&input["state"], "state")? {
                query.insert("state".into(), Value::from(st));
            }
            format!("/dags/{dag}/dagRuns/{run}/taskInstances")
        }
        "task_log" => {
            let dag = check_id("DAG id", input["dag_id"].as_str().unwrap_or(""))?;
            let run = check_id("run id", input["run_id"].as_str().unwrap_or(""))?;
            let task = check_id("task id", input["task_id"].as_str().unwrap_or(""))?;
            let n = input["try_number"].as_u64().unwrap_or(1).clamp(1, 99);
            query.insert("full_content".into(), Value::from(false));
            format!("/dags/{dag}/dagRuns/{run}/taskInstances/{task}/logs/{n}")
        }
        other => return Err(refused(format!("{other} isn't something this tool reads."))),
    };
    if !allowed_path(&path) {
        return Err(refused(format!("{path} isn't a path Coderhelm reads.")));
    }
    Ok(Get {
        environment,
        path,
        query,
    })
}

/// GET-only access to MWAA's REST API. There is deliberately no method for
/// anything but a GET.
#[async_trait::async_trait]
pub trait AirflowApi: Send + Sync {
    /// Returns Airflow's status code and body.
    async fn get(&self, req: &Get) -> Result<(i32, Value), StepError>;
}

/// Runs a checked read and cleans what comes back.
pub async fn read(api: &dyn AirflowApi, req: &Get) -> Result<(String, usize), StepError> {
    if !allowed_path(&req.path) {
        return Err(refused(format!(
            "{} isn't a path Coderhelm reads.",
            req.path
        )));
    }
    let (status, mut body) = api.get(req).await?;
    if status == 404 {
        return Err(StepError::new(
            "read",
            "Airflow has nothing at that id.",
            "Check the DAG, run and task ids (list them first).",
        ));
    }
    if !(200..300).contains(&status) {
        return Err(StepError::new(
            "read",
            format!(
                "Airflow answered {status}: {}",
                crate::truncate_str(&body.to_string(), 300)
            ),
            "Check the ids and try again.",
        ));
    }
    let mut hidden = redact_all(&mut body, &hidden_terms(&default_hidden_fields()));
    let text = match &body {
        Value::String(s) => s.clone(),
        Value::Object(m) if m.get("content").is_some_and(Value::is_string) => {
            m["content"].as_str().unwrap_or("").to_string()
        }
        v => trim_runs(v),
    };
    let (text, k) = redact_text(&text);
    hidden += k;
    Ok((text, hidden))
}

/// Keeps the fields people need from lists of DAGs, runs and tasks.
fn trim_runs(v: &Value) -> String {
    const KEEP: &[&str] = &[
        "dag_id",
        "dag_run_id",
        "task_id",
        "state",
        "is_paused",
        "is_active",
        "schedule_interval",
        "next_dagrun",
        "last_parsed_time",
        "execution_date",
        "logical_date",
        "start_date",
        "end_date",
        "duration",
        "try_number",
        "max_tries",
        "operator",
        "external_trigger",
        "run_type",
        "note",
        "filename",
        "stack_trace",
        "timestamp",
        "import_error_id",
        "status",
        "metadatabase",
        "scheduler",
        "latest_scheduler_heartbeat",
        "total_entries",
        "tags",
        "owners",
        "description",
        "fileloc",
        "timetable_description",
        "has_import_errors",
    ];
    fn keep(v: &Value) -> Value {
        match v {
            Value::Object(m) => Value::Object(
                m.iter()
                    .filter(|(k, _)| {
                        KEEP.contains(&k.as_str()) || k.ends_with('s') && m[k.as_str()].is_array()
                    })
                    .map(|(k, v)| (k.clone(), keep(v)))
                    .collect(),
            ),
            Value::Array(a) => Value::Array(a.iter().map(keep).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string_pretty(&keep(v)).unwrap_or_default()
}

/// The real reads, on a session of the team's role.
pub struct Mwaa<'a> {
    pub session: &'a Session,
}

pub fn client(s: &Session) -> aws_sdk_mwaa::Client {
    use aws_sdk_mwaa::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    aws_sdk_mwaa::Client::from_conf(
        aws_sdk_mwaa::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(s.region.clone()))
            .credentials_provider(creds)
            .build(),
    )
}

fn to_doc(v: &Value) -> aws_smithy_types::Document {
    use aws_smithy_types::{Document, Number};
    match v {
        Value::Null => Document::Null,
        Value::Bool(b) => Document::Bool(*b),
        Value::Number(n) => Document::Number(match (n.as_u64(), n.as_i64()) {
            (Some(u), _) => Number::PosInt(u),
            (_, Some(i)) => Number::NegInt(i),
            _ => Number::Float(n.as_f64().unwrap_or(0.0)),
        }),
        Value::String(s) => Document::String(s.clone()),
        Value::Array(a) => Document::Array(a.iter().map(to_doc).collect()),
        Value::Object(m) => {
            Document::Object(m.iter().map(|(k, v)| (k.clone(), to_doc(v))).collect())
        }
    }
}

fn from_doc(d: &aws_smithy_types::Document) -> Value {
    use aws_smithy_types::{Document, Number};
    match d {
        Document::Null => Value::Null,
        Document::Bool(b) => Value::Bool(*b),
        Document::Number(Number::PosInt(u)) => Value::from(*u),
        Document::Number(Number::NegInt(i)) => Value::from(*i),
        Document::Number(Number::Float(f)) => Value::from(*f),
        Document::String(s) => Value::String(s.clone()),
        Document::Array(a) => Value::Array(a.iter().map(from_doc).collect()),
        Document::Object(m) => {
            Value::Object(m.iter().map(|(k, v)| (k.clone(), from_doc(v))).collect())
        }
    }
}

fn mwaa_error<E, R>(e: &aws_sdk_mwaa::error::SdkError<E, R>) -> StepError
where
    E: aws_sdk_mwaa::error::ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    use aws_sdk_mwaa::error::ProvideErrorMetadata as _;
    let code = e.code().unwrap_or("").to_string();
    let msg = e.message().unwrap_or("").to_string();
    if code.contains("AccessDenied") {
        return StepError::new(
            "sign_in",
            "The CoderHelm role in your AWS account can't read Airflow yet.",
            "Update the role from Settings → AWS (one click adds read access to Airflow).",
        );
    }
    StepError::new(
        "read",
        format!(
            "MWAA refused the read ({}).",
            super::error_detail(&code, &msg)
        ),
        "Check the environment name.",
    )
}

#[async_trait::async_trait]
impl AirflowApi for Mwaa<'_> {
    async fn get(&self, req: &Get) -> Result<(i32, Value), StepError> {
        let mut call = client(self.session)
            .invoke_rest_api()
            .name(&req.environment)
            .path(&req.path)
            .method(aws_sdk_mwaa::types::RestApiMethod::Get);
        if !req.query.is_empty() {
            call = call.query_parameters(to_doc(&Value::Object(req.query.clone())));
        }
        match call.send().await {
            Ok(out) => Ok((
                out.rest_api_status_code().unwrap_or(200),
                out.rest_api_response().map(from_doc).unwrap_or(Value::Null),
            )),
            Err(e) => {
                // Airflow's own errors come back as a client or server
                // exception carrying its status.
                if let Some(svc) = e.as_service_error() {
                    use aws_sdk_mwaa::operation::invoke_rest_api::InvokeRestApiError as X;
                    match svc {
                        X::RestApiClientException(c) => {
                            return Ok((
                                c.rest_api_status_code().unwrap_or(400),
                                c.rest_api_response().map(from_doc).unwrap_or(Value::Null),
                            ))
                        }
                        X::RestApiServerException(c) => {
                            return Ok((
                                c.rest_api_status_code().unwrap_or(500),
                                c.rest_api_response().map(from_doc).unwrap_or(Value::Null),
                            ))
                        }
                        _ => {}
                    }
                }
                Err(mwaa_error(&e))
            }
        }
    }
}

/// The MWAA environments the role can see, or `Ok(None)` when AWS says the
/// role isn't allowed to list them.
pub async fn list_environments(session: &Session) -> Result<Option<Vec<String>>, StepError> {
    match client(session).list_environments().send().await {
        Ok(out) => Ok(Some(out.environments().to_vec())),
        Err(e) => {
            let err = mwaa_error(&e);
            if err.step == "sign_in" {
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    fn shared() -> Vec<String> {
        vec!["prod-airflow".to_string()]
    }

    #[test]
    fn only_listed_paths_can_be_read() {
        for p in [
            "/dags",
            "/dags/etl_daily",
            "/dags/etl_daily/dagRuns",
            "/dags/~/dagRuns",
            "/dags/etl/dagRuns/manual__2026-01-01T00:00:00+00:00/taskInstances",
            "/dags/etl/dagRuns/r1/taskInstances/load/logs/2",
            "/importErrors",
            "/health",
        ] {
            assert!(allowed_path(p), "{p}");
        }
        for p in [
            "/connections",
            "/variables",
            "/config",
            "/users",
            "/dags/etl/clearTaskInstances",
            "/dags/etl/updateTaskInstancesState",
            "/dags/../variables",
            "/dags/etl/dagRuns/r1/taskInstances/t/logs/0",
            "/dags/~",
            "/pools",
            "",
        ] {
            assert!(!allowed_path(p), "{p}");
        }
    }

    #[test]
    fn requests_are_built_from_typed_inputs() {
        let g = build(
            "dag_runs",
            &json!({"dag_id":"etl","state":"failed","limit":500}),
            &shared(),
        )
        .unwrap();
        assert_eq!(g.environment, "prod-airflow");
        assert_eq!(g.path, "/dags/etl/dagRuns");
        assert_eq!(g.query["limit"], 100, "capped");
        assert_eq!(g.query["state"], "failed");
        assert!(build("dag_runs", &json!({"state":"deleted"}), &shared()).is_err());
        assert!(build("dag_runs", &json!({"dag_id":"a/b"}), &shared()).is_err());
        assert!(build(
            "task_log",
            &json!({"dag_id":"d","run_id":"r","task_id":"t","try_number":3}),
            &shared()
        )
        .unwrap()
        .path
        .ends_with("/logs/3"));
        assert!(build("variables", &json!({}), &shared()).is_err());
        assert!(build("dags", &json!({"environment":"other"}), &shared()).is_err());
        let two = vec!["a".to_string(), "b".to_string()];
        assert!(build("dags", &json!({}), &two).is_err(), "must choose");
        assert_eq!(
            build("dags", &json!({"environment":"b"}), &two)
                .unwrap()
                .environment,
            "b"
        );
    }

    struct Fake {
        seen: Mutex<Vec<Get>>,
        reply: (i32, Value),
    }

    #[async_trait::async_trait]
    impl AirflowApi for Fake {
        async fn get(&self, req: &Get) -> Result<(i32, Value), StepError> {
            self.seen.lock().unwrap().push(req.clone());
            Ok(self.reply.clone())
        }
    }

    #[tokio::test]
    async fn a_read_is_one_get_and_comes_back_cleaned() {
        let api = Fake {
            seen: Mutex::new(Vec::new()),
            reply: (
                200,
                json!({"dag_runs":[{"dag_id":"etl","state":"failed","conf":{"password":"p"},"note":"owner a@b.com"}],"total_entries":1}),
            ),
        };
        let g = build("dag_runs", &json!({"dag_id":"etl"}), &shared()).unwrap();
        let (text, hidden) = read(&api, &g).await.unwrap();
        assert!(text.contains("\"state\": \"failed\""), "{text}");
        assert!(!text.contains("a@b.com"));
        assert!(!text.contains("\"p\""));
        assert!(hidden >= 1);
        assert_eq!(api.seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_path_off_the_list_is_never_sent() {
        let api = Fake {
            seen: Mutex::new(Vec::new()),
            reply: (200, json!({})),
        };
        let bad = Get {
            environment: "prod-airflow".into(),
            path: "/variables".into(),
            query: Map::new(),
        };
        assert!(read(&api, &bad).await.unwrap_err().is_refusal());
        assert!(api.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn documents_round_trip() {
        let v = json!({"a":[1,-2,1.5,"x",true,null]});
        assert_eq!(from_doc(&to_doc(&v)), v);
    }
}
