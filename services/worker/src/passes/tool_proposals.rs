//! Tool definitions committed to `.coderhelm/tools/` on a repo's default
//! branch. Each becomes a proposed version: checked against its source, run
//! once with its examples so an approver sees the result, and saved waiting
//! for an admin's approval. Nothing here approves or switches on a tool.

use common::live::secret;
use common::live::store::{self, Activity, Kind};
use common::live::tools::{parse_def, validate};
use common::live::tools_run::{activity_text, run_tool, source_info};
use common::live::tools_store::ToolStore;
use serde_json::Value;
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::models::ToolProposalsMessage;
use crate::WorkerState;

const PREVIEW_CHARS: usize = 8_000;

pub async fn run(
    state: &WorkerState,
    msg: ToolProposalsMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let table = &state.config.settings_table_name;
    let st = ToolStore {
        dynamo: &state.dynamo,
        table,
        team: &msg.team_id,
    };
    let sources = store::list_sources(&state.dynamo, table, &msg.team_id).await?;
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let author = format!("repo:{repo}");
    for path in &msg.paths {
        let text = match github
            .read_file(&msg.repo_owner, &msg.repo_name, path, &msg.sha)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                warn!(path, error = %e, "Could not read a tool definition");
                continue;
            }
        };
        let def = match parse_def(&text) {
            Ok(d) => d,
            Err(e) => {
                warn!(path, error = %e, "Tool definition can't be read; not proposed");
                continue;
            }
        };
        let Some(source) = sources
            .iter()
            .find(|s| s.id == def.source || s.name.eq_ignore_ascii_case(&def.source))
        else {
            warn!(path, source = %def.source, "Tool definition names a source that isn't connected");
            continue;
        };
        if let Err(problems) = validate(&def, &source_info(source)) {
            warn!(path, problems = %problems.join(" | "), "Tool definition didn't pass its checks; not proposed");
            continue;
        }
        let key = if source.kind == Kind::Api && !source.key_sealed.is_empty() {
            match secret::open(&state.kms, &msg.team_id, &source.key_sealed).await {
                Ok(k) => k,
                Err(e) => {
                    warn!(error = %e, "Could not open a source key for a tool's example run");
                    continue;
                }
            }
        } else {
            String::new()
        };
        let inputs = Value::Object(
            def.inputs
                .iter()
                .map(|(k, s)| (k.clone(), s.example.clone()))
                .collect(),
        );
        let run = run_tool(
            &state.sts,
            &def,
            &inputs,
            source,
            &key,
            &author,
            PREVIEW_CHARS,
        )
        .await;
        let (label, detail, ms, example) = match &run {
            Ok(r) => (
                "ok",
                r.note.clone(),
                r.ms,
                serde_json::json!({"ok": true, "sent": r.sent, "result": r.result, "hidden_count": r.hidden_count, "note": r.note}),
            ),
            Err(e) => (
                if e.is_refusal() { "refused" } else { "failed" },
                e.message.clone(),
                0,
                serde_json::json!({"ok": false, "refused": e.is_refusal(), "message": e.message, "fix": e.fix}),
            ),
        };
        let _ = store::log_activity(
            &state.dynamo,
            table,
            &msg.team_id,
            &Activity {
                source_id: source.id.clone(),
                source_name: source.name.clone(),
                actor: author.clone(),
                repo: repo.clone(),
                pr: 0,
                query: activity_text(&def, &inputs),
                outcome: label,
                detail,
                ms,
            },
        )
        .await;
        let origin = format!("repo {repo}@{} {path}", common::truncate_str(&msg.sha, 12));
        match st
            .save_version(&def, &author, &origin, &example.to_string(), run.is_ok())
            .await
        {
            Ok((_, v)) => {
                info!(tool = %def.name, hash = %v.hash, path, "Tool proposed from repo; waiting for approval")
            }
            Err(e) => warn!(error = %e, "Could not save a proposed tool"),
        }
    }
    Ok(())
}
