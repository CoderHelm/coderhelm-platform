//! Daily: checks every connected AWS account's CoderHelmLogReader role
//! against the permissions this release uses
//! (`common::live::permissions`) and saves the result for the AWS page.
//!
//! It never sends a message. An account that needs new read-only
//! permissions is one quiet item on the AWS page, with its one-click update
//! stack; the install's own account on a one-stack install needs nothing at
//! all, because the release brings them there itself (the access stack's
//! reader update, lambda/native-reader). A push for this was the incident
//! `rq_reader_reads_arrive_with_the_release`.
//!
//! The same sweep moves data sources off the script-made
//! `CoderhelmReadOnly` role onto the account's role stack
//! (`common::role_move`): a source switches only when the stack's role
//! passed the source's own connection check end to end.
//!
//! The same sweep sends a re-check of every run waiting for the role's
//! reads (`role_wait`), so a run whose fast re-checks are over still goes
//! on the day the role stack is updated.

use std::collections::BTreeMap;

use common::live::aws_account::{self, AwsAccount};
use common::live::permissions;
use tracing::{info, warn};

use crate::WorkerState;

#[derive(Debug, Default)]
pub struct Report {
    pub accounts: usize,
    pub need_update: usize,
    /// Data sources moved onto their account's role stack.
    pub sources_moved: usize,
}

/// Every connected account, by team: each team's accounts read by key
/// (`pk = team`, every page), never by a scan of the whole AWS insights
/// table (it also holds every account's inventory reads).
pub(crate) async fn accounts_by_team(
    state: &WorkerState,
) -> Result<BTreeMap<String, Vec<AwsAccount>>, String> {
    let mut out: BTreeMap<String, Vec<AwsAccount>> = BTreeMap::new();
    for team in super::learning_wait::teams(state).await {
        let accounts =
            aws_account::list(&state.dynamo, &state.config.aws_insights_table_name, &team).await?;
        if !accounts.is_empty() {
            out.insert(team, accounts);
        }
    }
    Ok(out)
}

/// Moves each of the team's data sources that still reads through a
/// script-made role onto its account's role stack, when the stack's role
/// reads it end to end; the rest stay as they are and are tried again on
/// the next sweep. Returns how many moved.
async fn move_sources(state: &WorkerState, team_id: &str, accounts: &[AwsAccount]) -> usize {
    use common::live::store;
    use common::role_move;
    let table = &state.config.settings_table_name;
    let Ok(sources) = store::list_sources(&state.dynamo, table, team_id).await else {
        return 0;
    };
    let mut moved = 0;
    for src in &sources {
        let Some(t) = src.aws.as_ref() else { continue };
        let Some(account) = accounts.iter().find(|a| a.account == t.account()) else {
            continue;
        };
        let Some(candidate) = role_move::candidate(t, account) else {
            continue;
        };
        match role_move::try_move(&state.sts, src, &candidate).await {
            Ok(next) => {
                let mut s = src.clone();
                s.aws = Some(next);
                match store::put_source(&state.dynamo, table, team_id, &s).await {
                    Ok(()) => {
                        moved += 1;
                        info!(team_id, source = %src.id, account = %account.account, "Data source moved onto the account's role stack");
                    }
                    Err(e) => {
                        warn!(team_id, source = %src.id, error = %e, "Could not save the moved data source")
                    }
                }
            }
            Err(why) => {
                info!(team_id, source = %src.id, account = %account.account, why = %why, "Data source stays on the script-made role until the role stack reads it")
            }
        }
    }
    moved
}

pub async fn run(state: &WorkerState) -> Result<Report, String> {
    let teams = accounts_by_team(state).await?;
    let mut report = Report::default();
    let now = chrono::Utc::now().to_rfc3339();
    for (team_id, accounts) in &teams {
        for a in accounts {
            let c = permissions::check(&state.sts, &a.target(), "permissions-check", &now).await;
            if let Err(e) = aws_account::save_permissions(
                &state.dynamo,
                &state.config.aws_insights_table_name,
                team_id,
                &c,
            )
            .await
            {
                warn!(team_id, account = %a.account, error = %e, "Could not save the permissions check");
            }
            report.accounts += 1;
            // The install's own account, whose reader the release keeps
            // current, never needs a person.
            if c.status() == "update_needed" && !c.install_account {
                report.need_update += 1;
            }
        }
        report.sources_moved += move_sources(state, team_id, accounts).await;
        // Runs waiting for the role's reads (`awaiting_setup`) check the
        // role again now: the daily check after their fast re-checks.
        super::role_wait::wake(state, team_id, None).await;
    }
    info!(
        accounts = report.accounts,
        need_update = report.need_update,
        sources_moved = report.sources_moved,
        "AWS permissions sweep"
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    /// The sweep only checks and saves: no message of any kind, on any
    /// channel, for an account that needs new read-only permissions.
    #[test]
    fn the_sweep_sends_nothing() {
        let src = include_str!("aws_permissions.rs");
        let code = &src[..src.find("#[cfg(test)]").unwrap()];
        for gone in [
            "notify::",
            "deliver",
            "log_findings::channels",
            "aws_permissions_update",
        ] {
            assert!(!code.contains(gone), "{gone}");
        }
    }
}
