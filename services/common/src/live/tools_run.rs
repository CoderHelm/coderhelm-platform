//! Runs one approved tool, the same way for a person previewing it in the
//! dashboard and for an agent mid-task. Every run re-validates the tool
//! against its source (lock 1), binds its inputs (lock 2) and goes through
//! the source's own read-only path (lock 3), with hidden values scrubbed
//! (lock 4).

use serde_json::Value;

use super::api::{run_lookup, ApiSource};
use super::aws;
use super::store::{Kind, Source};
use super::tools::{
    bind, http_get, http_url, render_logs_checked, validate, SourceInfo, ToolDef, ToolKind,
};
use super::{count, db, logs, redshift, StepError};

/// What one tool run sent and got back.
#[derive(Debug, Clone)]
pub struct ToolRun {
    /// The checked read that was sent (values travel separately).
    pub sent: String,
    pub result: String,
    pub hidden_count: usize,
    pub ms: u128,
    pub note: String,
}

/// What a tool needs to know about a connected source.
pub fn source_info(s: &Source) -> SourceInfo {
    SourceInfo {
        id: s.id.clone(),
        name: s.name.clone(),
        kind: Some(s.kind),
        engine: s.aws.as_ref().map(|a| a.engine.clone()).unwrap_or_default(),
        url: s.url.clone(),
        hidden: s.hidden_fields.clone(),
        aws: s.aws.clone(),
    }
}

/// Runs `def` with `inputs` against `source`. `key` is the source's opened
/// API key (empty for AWS sources); `actor` names the session in the team's
/// AWS records.
pub async fn run_tool(
    sts: &aws_sdk_sts::Client,
    def: &ToolDef,
    inputs: &Value,
    source: &Source,
    key: &str,
    actor: &str,
    max_chars: usize,
) -> Result<ToolRun, StepError> {
    let info = source_info(source);
    let prepared = validate(def, &info).map_err(|p| {
        StepError::new(
            "read_only",
            format!("This tool no longer passes its checks: {}", p.join(" ")),
            "Nothing was sent.",
        )
    })?;
    let params = bind(def, inputs)?;
    let hidden = &source.hidden_fields;
    match def.kind {
        ToolKind::Sql => {
            let t = source.aws.as_ref().ok_or_else(|| {
                StepError::new(
                    "reach",
                    "That database is missing its AWS details.",
                    "Connect it again.",
                )
            })?;
            let session = aws::assume(sts, t, actor).await?;
            let (rows, result, hidden_count, ms) = if t.engine == "redshift" {
                let database = redshift::pick_database(t, def.database.as_deref())?.to_string();
                redshift::verify_read_only_user(&session, t, &database).await?;
                redshift::run_tool_sql(
                    &session,
                    t,
                    &database,
                    &prepared.text,
                    &params,
                    hidden,
                    max_chars,
                )
                .await?
            } else {
                db::verify_read_only_user(&session, t).await?;
                db::run_tool_sql(&session, t, &prepared.text, &params, hidden, max_chars).await?
            };
            Ok(ToolRun {
                sent: prepared.text,
                result,
                hidden_count,
                ms,
                note: count(rows, "row"),
            })
        }
        ToolKind::Graphql => {
            let src = ApiSource {
                url: source.url.clone(),
                auth: source.auth,
                key: key.to_string(),
            };
            let vars: serde_json::Map<String, Value> = params
                .iter()
                .map(|p| (p.name.clone(), p.value.to_json()))
                .collect();
            let l = run_lookup(
                &src,
                &prepared.text,
                Some(&Value::Object(vars)),
                hidden,
                max_chars,
            )
            .await?;
            Ok(ToolRun {
                sent: l.query,
                result: l.result,
                hidden_count: l.hidden_count,
                ms: l.ms,
                note: String::new(),
            })
        }
        ToolKind::Logs => {
            let t = source.aws.as_ref().ok_or_else(|| {
                StepError::new(
                    "reach",
                    "That logs source is missing its AWS details.",
                    "Connect it again.",
                )
            })?;
            let search = render_logs_checked(def, inputs, t, chrono::Utc::now().timestamp())?;
            let session = aws::assume(sts, t, actor).await?;
            let r = logs::run_search(&session, &search, hidden, max_chars).await?;
            Ok(ToolRun {
                sent: search.query.clone(),
                result: r.result,
                hidden_count: r.hidden_count,
                ms: r.ms,
                note: count(r.rows, "line"),
            })
        }
        ToolKind::Http => {
            if source.kind != Kind::Api {
                return Err(StepError::new(
                    "read_only",
                    "HTTP tools read only a connected API.",
                    "Nothing was sent.",
                ));
            }
            let url = http_url(&source.url, def, inputs)?;
            let src = ApiSource {
                url: source.url.clone(),
                auth: source.auth,
                key: key.to_string(),
            };
            let (result, hidden_count, ms) = http_get(&src, &url, hidden, max_chars).await?;
            Ok(ToolRun {
                sent: format!("GET {url}"),
                result,
                hidden_count,
                ms,
                note: String::new(),
            })
        }
    }
}

/// The activity-log text for a tool run: its name and the inputs it got.
pub fn activity_text(def: &ToolDef, inputs: &Value) -> String {
    format!("{}({})", def.name, inputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::api::AuthStyle;
    use crate::live::tools::{bind, parse_def, BoundParam, ParamValue};
    use serde_json::json;

    /// Against a real database set up by the setup script. Set LIVE_SETUP_CODE
    /// and LIVE_EXTERNAL_ID, then run `cargo test -- --ignored real_tool`.
    /// Tries to write through a tool five ways; every one must fail, and a
    /// value shaped like SQL must come back as data.
    #[tokio::test]
    #[ignore = "needs a real AWS account"]
    async fn real_tool_reads_and_refuses_every_write() {
        let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} not set"));
        let conf = aws_config::load_from_env().await;
        let sts = aws_sdk_sts::Client::new(&conf);
        let mut t = aws::parse_setup_code(&env("LIVE_SETUP_CODE")).unwrap();
        t.external_id = env("LIVE_EXTERNAL_ID");
        let source = Source {
            id: "db".into(),
            kind: Kind::Database,
            name: "test database".into(),
            url: String::new(),
            auth: AuthStyle::XApiKey,
            key_sealed: String::new(),
            key_hint: String::new(),
            enabled: true,
            setup_complete: true,
            status: "ready".into(),
            status_message: String::new(),
            has_mutations: false,
            kinds: None,
            schema_available: false,
            samples: vec![],
            hidden_fields: crate::live::store::default_hidden_fields(),
            repos: vec![],
            last_checked_at: String::new(),
            created_at: String::new(),
            created_by: String::new(),
            aws: Some(t.clone()),
            header_name: String::new(),
            paths: Vec::new(),
            user: String::new(),
            indexes: Vec::new(),
            instructions: String::new(),
        };
        let tool = parse_def(
            "name: count_tables\nuse_when: test\nsource: db\nkind: sql\nread: SELECT count(*) AS n FROM information_schema.tables WHERE table_schema = :schema\ninputs:\n  schema: { type: string, pattern: \".{1,80}\", example: \"public\" }\nmax_rows: 5\n",
        )
        .unwrap();
        // A real read works.
        let r = run_tool(
            &sts,
            &tool,
            &json!({"schema": "public"}),
            &source,
            "",
            "integration-test",
            2000,
        )
        .await
        .unwrap();
        println!(
            "read: {} {}",
            r.note,
            r.result.split_whitespace().collect::<String>()
        );

        // 1. A tool that writes can't be saved or run.
        let mut writer = tool.clone();
        writer.read = "DELETE FROM pg_class WHERE relname = :schema".into();
        assert!(run_tool(
            &sts,
            &writer,
            &json!({"schema": "x"}),
            &source,
            "",
            "t",
            100
        )
        .await
        .unwrap_err()
        .is_refusal());
        // 2. An input outside its pattern is refused before anything is sent.
        let mut strict = tool.clone();
        strict.inputs.get_mut("schema").unwrap().pattern = Some("[a-z_]{1,30}".into());
        assert!(bind(&strict, &json!({"schema": "public'; DROP TABLE x; --"})).is_err());
        // 3. A value shaped like SQL is bound as data: the query runs, nothing else does.
        let r = run_tool(
            &sts,
            &tool,
            &json!({"schema": "x'); DROP TABLE pg_class; --"}),
            &source,
            "",
            "t",
            2000,
        )
        .await
        .unwrap();
        println!(
            "injection as data: {}",
            r.result.split_whitespace().collect::<String>()
        );
        assert!(r.result.contains("\"n\": 0") || r.result.contains("\"n\":0"));
        let session = aws::assume(&sts, &t, "integration-test").await.unwrap();
        // 4. Even unchecked text inside the tool path can't create anything.
        let e = db::run_tool_sql(
            &session,
            &t,
            "CREATE TABLE coderhelm_probe (x int)",
            &[],
            &[],
            100,
        )
        .await
        .unwrap_err();
        println!("create: {}", e.message);
        assert!(e.message.contains("read-only"));
        // 5. Nor delete, with a bound parameter.
        let p = [BoundParam {
            name: "n".into(),
            value: ParamValue::Text("zzz".into()),
        }];
        let e = db::run_tool_sql(
            &session,
            &t,
            "DELETE FROM pg_class WHERE relname = :n",
            &p,
            &[],
            100,
        )
        .await
        .unwrap_err();
        println!("delete: {}", e.message);
        assert!(e.message.contains("read-only") || e.message.contains("permission denied"));
    }
}
