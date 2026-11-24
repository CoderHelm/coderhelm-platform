//! Athena through the team's AWS reader role (`CoderHelmLogReader`), for a
//! skill run: the role is taken on the same way the run's other AWS reads
//! take it ([`crate::log_findings::session_for`]), every query runs in the
//! one workgroup the schedule names, and only reading statements reach it
//! (the engine checks that first; the role's policy denies every change).

use std::time::{Duration, Instant};

use aws_sdk_athena::error::ProvideErrorMetadata;
use aws_sdk_athena::types::{QueryExecutionContext, QueryExecutionState};

use super::engine::{Athena, AthenaResult, AwsError};
use super::AwsGrant;
use crate::live::aws::Session;

/// How long one query is waited for.
const WAIT: Duration = Duration::from_secs(10 * 60);
/// Rows one answer brings back at most.
const MAX_ROWS: usize = 5_000;

pub struct RoleAthena {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub sts: aws_sdk_sts::Client,
    /// The insights table, where each connected account's role is kept.
    pub insights_table: String,
}

fn client(s: &Session, region: &str) -> aws_sdk_athena::Client {
    use aws_sdk_athena::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    aws_sdk_athena::Client::from_conf(
        aws_sdk_athena::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .credentials_provider(creds)
            .build(),
    )
}

fn classify(code: Option<&str>, message: &str) -> AwsError {
    let text = format!("{}: {message}", code.unwrap_or("error"));
    if super::engine::is_access_error(&text)
        || matches!(
            code,
            Some("AccessDeniedException" | "UnauthorizedOperation")
        )
    {
        AwsError::Access(text)
    } else {
        AwsError::Other(text)
    }
}

impl RoleAthena {
    async fn session(&self, team: &str, g: &AwsGrant) -> Result<Session, AwsError> {
        match crate::log_findings::session_for(
            &self.dynamo,
            &self.sts,
            &self.insights_table,
            team,
            &g.account,
        )
        .await
        {
            Ok(Some(s)) => Ok(s.into_session()),
            Ok(None) => Err(AwsError::Access(format!(
                "AWS account {} isn't connected with a reader role on the Connections page",
                g.account
            ))),
            Err(e) => Err(AwsError::Access(format!(
                "the reader role in {} couldn't be taken on: {e}",
                g.account
            ))),
        }
    }
}

#[async_trait::async_trait]
impl Athena for RoleAthena {
    async fn check(&self, team: &str, g: &AwsGrant) -> Result<(), AwsError> {
        let s = self.session(team, g).await?;
        client(&s, &g.region)
            .get_work_group()
            .work_group(&g.workgroup)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                let mut err = classify(e.code(), e.message().unwrap_or("refused"));
                if let AwsError::Other(m) = &err {
                    err = AwsError::Access(format!("workgroup {}: {m}", g.workgroup));
                }
                err
            })
    }

    async fn query(
        &self,
        team: &str,
        g: &AwsGrant,
        sql: &str,
        database: &str,
    ) -> Result<AthenaResult, AwsError> {
        let s = self.session(team, g).await?;
        let athena = client(&s, &g.region);
        let mut req = athena
            .start_query_execution()
            .query_string(sql)
            .work_group(&g.workgroup);
        if !database.trim().is_empty() {
            req = req.query_execution_context(
                QueryExecutionContext::builder()
                    .database(database.trim())
                    .build(),
            );
        }
        let id = req
            .send()
            .await
            .map_err(|e| classify(e.code(), e.message().unwrap_or("refused")))?
            .query_execution_id()
            .unwrap_or_default()
            .to_string();
        let started = Instant::now();
        let scanned = loop {
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            let q = athena
                .get_query_execution()
                .query_execution_id(&id)
                .send()
                .await
                .map_err(|e| classify(e.code(), e.message().unwrap_or("refused")))?;
            let q = q.query_execution();
            let st = q.and_then(|q| q.status());
            let bytes = q
                .and_then(|q| q.statistics())
                .and_then(|s| s.data_scanned_in_bytes())
                .unwrap_or(0)
                .max(0) as u64;
            match st.and_then(|s| s.state()) {
                Some(QueryExecutionState::Succeeded) => break bytes,
                Some(QueryExecutionState::Failed | QueryExecutionState::Cancelled) => {
                    let why = st
                        .and_then(|s| s.state_change_reason())
                        .unwrap_or("no reason given");
                    return Err(classify(None, why));
                }
                _ if started.elapsed() > WAIT => {
                    let _ = athena
                        .stop_query_execution()
                        .query_execution_id(&id)
                        .send()
                        .await;
                    return Err(AwsError::Other(format!(
                        "The query took longer than {} minutes and was stopped.",
                        WAIT.as_secs() / 60
                    )));
                }
                _ => {}
            }
        };
        let mut lines = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let page = athena
                .get_query_results()
                .query_execution_id(&id)
                .set_next_token(token.clone())
                .send()
                .await
                .map_err(|e| classify(e.code(), e.message().unwrap_or("refused")))?;
            for r in page.result_set().map(|rs| rs.rows()).unwrap_or_default() {
                lines.push(
                    r.data()
                        .iter()
                        .map(|d| d.var_char_value().unwrap_or("").replace(['\t', '\n'], " "))
                        .collect::<Vec<_>>()
                        .join("\t"),
                );
            }
            token = page.next_token().map(str::to_string);
            if token.is_none() || lines.len() >= MAX_ROWS {
                break;
            }
        }
        let cut = lines.len() > MAX_ROWS;
        lines.truncate(MAX_ROWS);
        if cut {
            lines.push(format!("[more than {MAX_ROWS} rows; the rest left out]"));
        }
        Ok(AthenaResult {
            text: lines.join("\n"),
            scanned_bytes: scanned,
        })
    }
}
