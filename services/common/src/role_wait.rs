//! A run whose missing data is only behind a read the team's AWS role
//! lacks waits for the install's setup item "AWS role reads", and goes on
//! by itself once the role has the read.
//!
//! An AWS WAF alarm run read the alarm's metrics, could not read the
//! requests the rule matched (the account's role stack predated the WAF
//! reads its template has), and ended `needs_outside_change` with "Needed
//! from you: Nothing" and a request that a person run an Athena query. The
//! read was the install's setup item, so no person had anything to do on
//! the run, and nothing would ever take the run up again once the role
//! could read.
//!
//! Now a run that stops without a code change ends by [`ending`]:
//!
//! - a person has an item (a decision, a credential, a change only they can
//!   make): `needs_outside_change`, as before;
//! - nothing is left for a person and the run was refused reads by the
//!   team's role ([`refused_this_run`]): it waits, `awaiting_setup`. It
//!   says so in one paragraph ([`waiting_note`]), records the wait
//!   ([`Waiting`], one record per run), and the worker re-checks the role
//!   every [`POLL_SECS`] for [`FAST_POLLS`] checks, then once a day with
//!   the role check, and at once when a read that was refused works again
//!   in another run. When the role reads, the run starts again from the
//!   ticket, reads the data and opens its pull request;
//! - nothing is left for a person and nothing to wait for: the plan is
//!   CoderHelm's to make again, never a report that asks nobody for
//!   anything under a status that says a person must act.

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use serde::{Deserialize, Serialize};

use crate::aws_role_reads::{Gap, ITEM_LABEL};

/// One record per waiting run, in the settings table under the team.
pub const SK_PREFIX: &str = "ROLE_WAIT#";
/// Seconds between re-checks on the fast schedule (SQS's longest delay).
pub const POLL_SECS: i32 = 900;
/// Re-checks on the fast schedule (two days); then the daily role check.
pub const FAST_POLLS: u32 = 192;

/// How a run whose plan changes no code ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
    /// A person has an item: `needs_outside_change`.
    Person,
    /// Nothing for a person; the role lacks reads the data is behind:
    /// `awaiting_setup` until a re-check shows the role has them.
    AwaitRole(Vec<Gap>),
    /// Nothing for a person and nothing to wait for: plan again.
    PlanAgain,
    /// The run was started again because a re-check read these actions as
    /// the team's role, and the run was refused them anyway: CoderHelm's own
    /// failure (the run and the re-check disagreed). It never waits again;
    /// it goes on with what it has.
    OwnFailure(Vec<String>),
}

/// Pure: how the run ends. `person_items` are the items left for a person
/// after the final check (`person_gate::sink`); `refused` the reads the
/// team's role was refused during this run ([`refused_this_run`]).
pub fn ending(person_items: &[String], refused: Vec<Gap>) -> Ending {
    if !person_items.is_empty() {
        Ending::Person
    } else if !refused.is_empty() {
        Ending::AwaitRole(refused)
    } else {
        Ending::PlanAgain
    }
}

/// Pure: [`ending`] for a run [`went_on`] started after a re-check read
/// `went_on`'s actions: refused one of them again, it ends
/// [`Ending::OwnFailure`] instead of waiting, so a run restarts at most once
/// for the same refused read.
pub fn ending_after(
    person_items: &[String],
    refused: Vec<Gap>,
    went_on: Option<&Waiting>,
) -> Ending {
    match ending(person_items, refused) {
        Ending::AwaitRole(gaps) => {
            let again: Vec<String> = went_on
                .map(|w| {
                    let mut a: Vec<String> = Vec::new();
                    for g in &gaps {
                        for x in &g.actions {
                            if w.actions.contains(x) && !a.contains(x) {
                                a.push(x.clone());
                            }
                        }
                    }
                    a
                })
                .unwrap_or_default();
            if again.is_empty() {
                Ending::AwaitRole(gaps)
            } else {
                Ending::OwnFailure(again)
            }
        }
        e => e,
    }
}

/// Pure: the one line a run that ended [`Ending::OwnFailure`] says.
pub fn own_failure_line(actions: &[String]) -> String {
    format!(
        "CoderHelm's own failure: the re-check read {} as the team's role, and this run was \
         refused it anyway. It goes on with what it has instead of waiting again.",
        actions.join(", ")
    )
}

/// Pure: the install's recorded gaps this run met: refused at or after
/// `run_started` (RFC 3339). A gap is written again each time a read is
/// refused, so one older than the run was not this run's.
pub fn refused_this_run(gaps: &[Gap], run_started: &str) -> Vec<Gap> {
    let started = chrono::DateTime::parse_from_rfc3339(run_started).ok();
    gaps.iter()
        .filter(|g| !g.actions.is_empty())
        .filter(|g| {
            match (
                started,
                chrono::DateTime::parse_from_rfc3339(&g.seen_at).ok(),
            ) {
                (Some(s), Some(seen)) => seen >= s,
                _ => false,
            }
        })
        .cloned()
        .collect()
}

/// What a waiting run waits for.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Waiting {
    pub run_id: String,
    /// The accounts whose role lacks the reads.
    pub accounts: Vec<String>,
    /// The reads, once each.
    pub actions: Vec<String>,
    /// When the wait started (RFC 3339).
    pub since: String,
}

impl Waiting {
    pub fn of(run_id: &str, gaps: &[Gap], now: &str) -> Waiting {
        let mut w = Waiting {
            run_id: run_id.to_string(),
            since: now.to_string(),
            ..Default::default()
        };
        for g in gaps {
            if !w.accounts.contains(&g.account) {
                w.accounts.push(g.account.clone());
            }
            for a in &g.actions {
                if !w.actions.contains(a) {
                    w.actions.push(a.clone());
                }
            }
        }
        w
    }
}

/// Pure: a read the data is not only behind: Athena and the Glue catalog
/// are one way to query WAF logs, and the log objects read straight from
/// the bucket are the other (`crate::waf_logs`), so a run never waits on
/// them alone once the other reads work.
pub fn optional(action: &str) -> bool {
    action.starts_with("athena:") || action.starts_with("glue:")
}

/// Pure: whether a re-check lets the run go on: none of the reads it waits
/// for was refused again (`still_refused` are the reads the re-check was
/// refused, in any order). Athena and Glue reads count only for a run that
/// waits for nothing else.
pub fn ready(w: &Waiting, still_refused: &[String]) -> bool {
    let needed: Vec<&String> = w.actions.iter().filter(|a| !optional(a)).collect();
    let needed = if needed.is_empty() {
        w.actions.iter().collect()
    } else {
        needed
    };
    !needed.iter().any(|a| still_refused.contains(a))
}

/// Pure: the paragraph a waiting run's report ends with: what it waits for,
/// that it asks nobody, and what it does once the role reads.
pub fn waiting_note(w: &Waiting) -> String {
    format!(
        "**Waiting for the install's setup item \"{ITEM_LABEL}\".** The data this needs is \
         behind reads that the role stack's template has and the role in account {} lacks: {}. \
         CoderHelm read what the role allows and asks nobody on this run: the item's one-click \
         update of the role stack, which an AWS admin of the account creates, is listed once \
         for the whole install. CoderHelm checks the role every {} minutes, then daily, and \
         goes on by itself as soon as it reads: it reads the data, makes the change and opens \
         the pull request.",
        w.accounts.join(", "),
        w.actions.join(", "),
        POLL_SECS / 60
    )
}

fn attr(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// Records the wait (best effort).
pub async fn record(dynamo: &DynamoClient, table: &str, team_id: &str, w: &Waiting) {
    let Ok(json) = serde_json::to_string(w) else {
        return;
    };
    let res = dynamo
        .put_item()
        .table_name(table)
        .item("pk", attr(team_id))
        .item("sk", attr(&format!("{SK_PREFIX}{}", w.run_id)))
        .item("waiting", attr(&json))
        .send()
        .await;
    if let Err(e) = res {
        tracing::warn!(team_id, run_id = %w.run_id, error = %e, "Could not record the run's wait for the AWS role's reads");
    }
}

/// The run's wait, when it has one.
pub async fn get(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    run_id: &str,
) -> Option<Waiting> {
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", attr(team_id))
        .key("sk", attr(&format!("{SK_PREFIX}{run_id}")))
        .send()
        .await
        .ok()?;
    let json = out.item()?.get("waiting")?.as_s().ok()?.clone();
    serde_json::from_str(&json).ok()
}

/// Every run of the team waiting for the role's reads.
pub async fn list(dynamo: &DynamoClient, table: &str, team_id: &str) -> Vec<Waiting> {
    let out = dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :p AND begins_with(sk, :s)")
        .expression_attribute_values(":p", attr(team_id))
        .expression_attribute_values(":s", attr(SK_PREFIX))
        .send()
        .await;
    let Ok(out) = out else {
        return Vec::new();
    };
    out.items()
        .iter()
        .filter_map(|i| i.get("waiting")?.as_s().ok())
        .filter_map(|j| serde_json::from_str(j).ok())
        .collect()
}

/// The run a re-check started again (`new_run`), and what that re-check
/// read: [`ending_after`] never lets it wait for the same reads twice.
pub const WENT_ON_PREFIX: &str = "ROLE_WENT_ON#";

/// Records that `new_run` was started because the role read `w`'s actions
/// (best effort).
pub async fn record_went_on(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    new_run: &str,
    w: &Waiting,
) {
    let Ok(json) = serde_json::to_string(w) else {
        return;
    };
    let res = dynamo
        .put_item()
        .table_name(table)
        .item("pk", attr(team_id))
        .item("sk", attr(&format!("{WENT_ON_PREFIX}{new_run}")))
        .item("waiting", attr(&json))
        .send()
        .await;
    if let Err(e) = res {
        tracing::warn!(team_id, new_run, error = %e, "Could not record the run a role re-check started");
    }
}

/// What the re-check that started `run_id` read, when one did.
pub async fn went_on(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    run_id: &str,
) -> Option<Waiting> {
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", attr(team_id))
        .key("sk", attr(&format!("{WENT_ON_PREFIX}{run_id}")))
        .send()
        .await
        .ok()?;
    let json = out.item()?.get("waiting")?.as_s().ok()?.clone();
    serde_json::from_str(&json).ok()
}

/// The wait is over (the run went on, or ended another way).
pub async fn remove(dynamo: &DynamoClient, table: &str, team_id: &str, run_id: &str) {
    let _ = dynamo
        .delete_item()
        .table_name(table)
        .key("pk", attr(team_id))
        .key("sk", attr(&format!("{SK_PREFIX}{run_id}")))
        .send()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gap(seen_at: &str) -> Gap {
        Gap {
            account: "111122223333".into(),
            role_arn: "arn:aws:iam::111122223333:role/CoderHelmLogReader".into(),
            actions: vec![
                "wafv2:ListWebACLs".into(),
                "wafv2:GetSampledRequests".into(),
            ],
            needed_for: "an AWS WAF alarm's evidence".into(),
            seen_at: seen_at.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_run_with_nothing_for_a_person_waits_for_the_role_or_plans_again() {
        let refused = vec![gap("2026-11-18T12:57:36Z")];
        assert_eq!(
            ending(
                &["Choose which page the banner goes on".into()],
                refused.clone()
            ),
            Ending::Person
        );
        assert_eq!(ending(&[], refused.clone()), Ending::AwaitRole(refused));
        assert_eq!(ending(&[], vec![]), Ending::PlanAgain);
    }

    #[test]
    fn only_refusals_this_run_met_count() {
        let gaps = vec![gap("2026-11-18T12:57:36Z"), gap("2026-11-01T00:00:00Z")];
        let this = refused_this_run(&gaps, "2026-11-18T12:57:29Z");
        assert_eq!(this.len(), 1);
        assert!(refused_this_run(&gaps, "2026-11-19T00:00:00Z").is_empty());
        assert!(refused_this_run(&gaps, "not a time").is_empty());
    }

    #[test]
    fn the_wait_names_its_reads_and_goes_on_once_none_is_refused() {
        let w = Waiting::of("run-1", &[gap("t"), gap("t")], "2026-11-18T13:04:33Z");
        assert_eq!(w.accounts, vec!["111122223333"]);
        assert_eq!(
            w.actions,
            vec!["wafv2:ListWebACLs", "wafv2:GetSampledRequests"]
        );
        assert!(!ready(&w, &["wafv2:GetSampledRequests".into()]));
        assert!(ready(&w, &["athena:StartQueryExecution".into()]));
        assert!(ready(&w, &[]));
        // Athena stays optional: the log objects are read without it.
        let mut logs = w.clone();
        logs.actions.push("athena:StartQueryExecution".into());
        assert!(ready(&logs, &["athena:StartQueryExecution".into()]));
        let athena_only = Waiting {
            actions: vec!["athena:StartQueryExecution".into()],
            ..w.clone()
        };
        assert!(!ready(&athena_only, &["athena:StartQueryExecution".into()]));
        let note = waiting_note(&w);
        assert!(note.contains("\"AWS role reads\""), "{note}");
        assert!(note.contains("asks nobody"), "{note}");
        assert!(note.contains("opens the pull request"), "{note}");
        // Never an ask that a person act on the run.
        assert!(!crate::person_gate::grant_backed(&note));
        let back: Waiting = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
        assert_eq!(back, w);
    }
}
