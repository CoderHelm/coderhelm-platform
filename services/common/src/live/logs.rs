//! Searches a team's CloudWatch logs with Logs Insights, under the rules in
//! [`super`].
//!
//! Logs Insights can only read. On top of that: a search may only touch the log
//! groups the team chose, may not pick its own groups from inside the query or
//! unmask masked values (also denied to the role itself), covers at most two
//! weeks, and every line is scrubbed of secrets, email addresses and card
//! numbers before anyone sees it.

use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Map, Value};

use super::aws::{logs_client, AwsTarget, Session};
use super::redact::{hidden_terms, redact_all};
use super::StepError;

const MAX_QUERY_CHARS: usize = 4_000;
pub const MAX_ROWS: i32 = 100;
const MAX_GROUPS_PER_SEARCH: usize = 10;
const MAX_RANGE: Duration = Duration::from_secs(14 * 24 * 3600);
const DEFAULT_RANGE: Duration = Duration::from_secs(3600);
const WAIT: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(800);

fn refused(message: impl Into<String>) -> StepError {
    StepError::new("read_only", message, "Nothing was sent.")
}

/// A search that passed every check, ready to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedSearch {
    pub query: String,
    pub groups: Vec<String>,
    pub start: i64,
    pub end: i64,
}

/// `15m`, `2h`, `3d` or `1w`.
pub fn parse_ago(s: &str) -> Option<Duration> {
    let s = s.trim().to_lowercase();
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit())?);
    let n: u64 = num.parse().ok()?;
    let secs = match unit.trim() {
        "m" | "min" | "mins" | "minutes" => 60,
        "h" | "hr" | "hrs" | "hours" => 3600,
        "d" | "day" | "days" => 86_400,
        "w" | "week" | "weeks" => 604_800,
        _ => return None,
    };
    Some(Duration::from_secs(n.checked_mul(secs)?))
}

fn parse_time(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s.trim())
        .ok()
        .map(|d| d.timestamp())
}

/// Checks a search against the team's chosen groups and the rules above.
/// `since` is how far back (`2h`) or a start time; `until` an end time.
pub fn check_search(
    t: &AwsTarget,
    groups: &[String],
    query: &str,
    since: Option<&str>,
    until: Option<&str>,
    now: i64,
) -> Result<CheckedSearch, StepError> {
    let query = query.trim();
    if query.is_empty() {
        return Err(refused("The search is empty."));
    }
    if query.len() > MAX_QUERY_CHARS {
        return Err(refused("That search is too long."));
    }
    let lower = query.to_lowercase();
    // Commands are separated by `|`; each starts with its name.
    for cmd in lower.split('|').map(str::trim) {
        let name = cmd
            .split(|c: char| c.is_whitespace() || c == '(')
            .next()
            .unwrap_or("");
        if name == "source" {
            return Err(refused(
                "A search can't choose its own log groups. Pass them as log_groups.",
            ));
        }
    }
    if lower.contains("unmask") {
        return Err(refused("Masked values stay masked; unmask isn't allowed."));
    }

    let groups: Vec<String> = if groups.is_empty() {
        // Only exact names can be searched; a shared prefix needs a name.
        t.log_groups
            .iter()
            .filter(|g| !g.ends_with('*'))
            .cloned()
            .collect()
    } else {
        groups.iter().map(|g| g.trim().to_string()).collect()
    };
    if groups.is_empty() {
        return Err(refused("Name the log groups to search."));
    }
    if groups.len() > MAX_GROUPS_PER_SEARCH {
        return Err(refused(format!(
            "Search at most {MAX_GROUPS_PER_SEARCH} log groups at once."
        )));
    }
    if let Some(g) = groups.iter().find(|g| !t.allows_log_group(g)) {
        return Err(refused(format!(
            "The log group {g} isn't one this team shared."
        )));
    }

    let end = match until.filter(|u| !u.trim().is_empty()) {
        Some(u) => parse_time(u).ok_or_else(|| {
            refused("The end time isn't a date and time like 2026-01-31T14:00:00Z.")
        })?,
        None => now,
    }
    .min(now);
    let start = match since.filter(|s| !s.trim().is_empty()) {
        Some(s) => match parse_ago(s) {
            Some(d) => end - d.as_secs() as i64,
            None => parse_time(s)
                .ok_or_else(|| refused("The start isn't like 2h, 3d or 2026-01-31T14:00:00Z."))?,
        },
        None => end - DEFAULT_RANGE.as_secs() as i64,
    };
    if start >= end {
        return Err(refused("The start must come before the end."));
    }
    if end - start > MAX_RANGE.as_secs() as i64 {
        return Err(refused("Search at most two weeks at a time."));
    }
    Ok(CheckedSearch {
        query: query.to_string(),
        groups,
        start,
        end,
    })
}

/// Whether the query sets its own row limit (`| limit 20`).
fn has_limit(query: &str) -> bool {
    query
        .to_lowercase()
        .split('|')
        .any(|cmd| cmd.split_whitespace().next() == Some("limit"))
}

/// The result of one search.
#[derive(Debug, Clone)]
pub struct LogsLookup {
    pub rows: usize,
    pub result: String,
    pub hidden_count: usize,
    pub scanned_mb: f64,
    pub ms: u128,
    pub partial: bool,
}

fn logs_error<E>(e: &aws_sdk_cloudwatchlogs::error::SdkError<E>) -> StepError
where
    E: aws_sdk_cloudwatchlogs::error::ProvideErrorMetadata + std::error::Error + 'static,
{
    let code = e.as_service_error().and_then(|s| s.code()).unwrap_or("");
    let msg = e.as_service_error().and_then(|s| s.message()).unwrap_or("");
    match code {
        "AccessDeniedException" => StepError::new(
            "sign_in",
            "The role in your AWS account isn't allowed to search these logs.",
            "Run the setup script again in AWS CloudShell.",
        ),
        "MalformedQueryException" | "InvalidParameterException" | "ResourceNotFoundException" => {
            StepError::new(
                "read",
                format!(
                    "Logs Insights couldn't run that: {}",
                    crate::truncate_str(msg, 300)
                ),
                "Check the search against the Logs Insights syntax, and the log group names.",
            )
        }
        "LimitExceededException" => StepError::new(
            "reach",
            "Too many log searches are running in that account.",
            "Try again in a minute.",
        ),
        _ => StepError::new("reach", "We couldn't reach CloudWatch Logs.", "Try again."),
    }
}

/// Runs one checked search and waits for it, up to a limit.
pub async fn run_search(
    session: &Session,
    search: &CheckedSearch,
    hidden: &[String],
    max_result_chars: usize,
) -> Result<LogsLookup, StepError> {
    let client = logs_client(session);
    let started = Instant::now();
    let q = client
        .start_query()
        .set_log_group_names(Some(search.groups.clone()))
        .query_string(&search.query)
        .start_time(search.start)
        .end_time(search.end)
        // The request's limit replaces the query's own, so it is only set when
        // the query has none; rows past the cap are dropped below either way.
        .set_limit((!has_limit(&search.query)).then_some(MAX_ROWS))
        .send()
        .await
        .map_err(|e| logs_error(&e))?;
    let id = q.query_id().unwrap_or_default().to_string();

    use aws_sdk_cloudwatchlogs::types::QueryStatus;
    let mut partial = false;
    let out = loop {
        tokio::time::sleep(POLL).await;
        let r = client
            .get_query_results()
            .query_id(&id)
            .send()
            .await
            .map_err(|e| logs_error(&e))?;
        match r.status() {
            Some(QueryStatus::Complete) => break r,
            Some(QueryStatus::Failed)
            | Some(QueryStatus::Cancelled)
            | Some(QueryStatus::Timeout) => {
                return Err(StepError::new(
                    "read",
                    "The log search didn't finish.",
                    "Search a shorter time range or fewer groups.",
                ));
            }
            _ if started.elapsed() > WAIT => {
                let _ = client.stop_query().query_id(&id).send().await;
                partial = true;
                break r;
            }
            _ => {}
        }
    };
    let ms = started.elapsed().as_millis();

    let rows: Vec<Value> = out
        .results()
        .iter()
        .take(MAX_ROWS as usize)
        .map(|fields| {
            let mut m = Map::new();
            for f in fields {
                let name = f.field().unwrap_or("");
                // A pointer to the raw event, meaningless outside the console.
                if name == "@ptr" {
                    continue;
                }
                m.insert(
                    name.to_string(),
                    Value::String(f.value().unwrap_or("").to_string()),
                );
            }
            Value::Object(m)
        })
        .collect();
    let count = rows.len();
    let mut value = Value::Array(rows);
    let hidden_count = redact_all(&mut value, &hidden_terms(hidden));
    let scanned_mb = out
        .statistics()
        .map(|s| s.bytes_scanned() / 1_000_000.0)
        .unwrap_or(0.0);
    let pretty = serde_json::to_string_pretty(&value).unwrap_or_default();
    Ok(LogsLookup {
        rows: count,
        result: crate::head_tail_str(&pretty, max_result_chars),
        hidden_count,
        scanned_mb,
        ms,
        partial,
    })
}

/// A log group in a team's account.
#[derive(Debug, Clone, Serialize)]
pub struct LogGroup {
    pub name: String,
    pub stored_mb: f64,
    pub retention_days: Option<i32>,
}

/// Every log group the role can see, up to 2,000, for choosing which to share.
pub async fn list_groups(session: &Session) -> Result<Vec<LogGroup>, StepError> {
    let client = logs_client(session);
    let mut groups = Vec::new();
    let mut next: Option<String> = None;
    loop {
        let r = client
            .describe_log_groups()
            .set_next_token(next.clone())
            .limit(50)
            .send()
            .await
            .map_err(|e| logs_error(&e))?;
        for g in r.log_groups() {
            groups.push(LogGroup {
                name: g.log_group_name().unwrap_or("").to_string(),
                stored_mb: g.stored_bytes().unwrap_or(0) as f64 / 1_000_000.0,
                retention_days: g.retention_in_days(),
            });
        }
        next = r.next_token().map(str::to_string);
        if next.is_none() || groups.len() >= 2_000 {
            break;
        }
    }
    Ok(groups)
}

/// What the agents are told a logs source offers.
pub fn describe(t: &AwsTarget) -> String {
    let mut s = String::from("Log groups you may search (pass the exact names as log_groups):\n");
    for g in &t.log_groups {
        if g.ends_with('*') {
            s.push_str(&format!(
                "- every group starting with {}\n",
                g.trim_end_matches('*')
            ));
        } else {
            s.push_str(&format!("- {g}\n"));
        }
    }
    s.push_str(
        "\nLogs Insights examples:\n\
         fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 50\n\
         fields @timestamp, @message | filter @message like /order 12345/ | sort @timestamp desc\n\
         stats count(*) by bin(1h) | filter @message like /timeout/\n\
         parse @message /status=(?<status>\\d+)/ | stats count(*) by status\n",
    );
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> AwsTarget {
        AwsTarget {
            role_arn: "arn:aws:iam::123456789012:role/CoderhelmReadOnly".into(),
            region: "us-east-1".into(),
            log_groups: vec![
                "/ecs/api".into(),
                "/ecs/worker".into(),
                "/aws/lambda/app-*".into(),
            ],
            ..Default::default()
        }
    }
    const NOW: i64 = 1_800_000_000;

    fn ok(groups: &[&str], q: &str, since: Option<&str>) -> CheckedSearch {
        let g: Vec<String> = groups.iter().map(|s| s.to_string()).collect();
        check_search(&target(), &g, q, since, None, NOW).unwrap_or_else(|e| panic!("{q}: {e}"))
    }
    fn no(groups: &[&str], q: &str, since: Option<&str>) -> String {
        let g: Vec<String> = groups.iter().map(|s| s.to_string()).collect();
        let e = check_search(&target(), &g, q, since, None, NOW).unwrap_err();
        assert!(e.is_refusal());
        e.message
    }

    #[test]
    fn searches_in_shared_groups_pass() {
        let c = ok(
            &["/ecs/api"],
            "fields @timestamp, @message | filter @message like /ERROR/",
            Some("2h"),
        );
        assert_eq!(c.groups, vec!["/ecs/api"]);
        assert_eq!(c.end - c.start, 7200);
        // no groups named: every exact shared group, never a prefix
        assert_eq!(
            ok(&[], "fields @message", None).groups,
            vec!["/ecs/api", "/ecs/worker"]
        );
        ok(
            &["/aws/lambda/app-orders"],
            "stats count(*) by bin(5m)",
            Some("3d"),
        );
        let c = ok(
            &["/ecs/api"],
            "fields @message",
            Some("2027-01-15T00:00:00Z"),
        );
        assert_eq!(c.start, 1_799_971_200);
    }

    #[test]
    fn groups_the_team_did_not_share_are_refused() {
        assert!(
            no(&["/ecs/billing"], "fields @message", None).contains("isn't one this team shared")
        );
        assert!(no(&["/ecs/api-worker"], "fields @message", None).contains("isn't one"));
        let many: Vec<String> = (0..11).map(|i| format!("/aws/lambda/app-{i}")).collect();
        let refs: Vec<&str> = many.iter().map(String::as_str).collect();
        assert!(no(&refs, "fields @message", None).contains("at most 10"));
    }

    #[test]
    fn a_search_cannot_pick_groups_or_unmask() {
        assert!(no(
            &["/ecs/api"],
            "SOURCE logGroups(namePrefix: ['/']) | fields @message",
            None
        )
        .contains("choose its own"));
        assert!(
            no(&["/ecs/api"], "fields @message | source logGroups()", None)
                .contains("choose its own")
        );
        assert!(no(&["/ecs/api"], "fields unmask(@message)", None).contains("unmask"));
        // the word inside a pattern is fine
        ok(
            &["/ecs/api"],
            "fields @message | filter @message like /source/",
            None,
        );
    }

    #[test]
    fn time_ranges_are_capped() {
        assert!(no(&["/ecs/api"], "fields @message", Some("15d")).contains("two weeks"));
        ok(&["/ecs/api"], "fields @message", Some("14d"));
        no(&["/ecs/api"], "fields @message", Some("yesterday"));
        assert_eq!(parse_ago("90m"), Some(Duration::from_secs(5400)));
        assert_eq!(parse_ago("1w"), Some(Duration::from_secs(604_800)));
        assert_eq!(parse_ago("h"), None);
        no(&["/ecs/api"], "", None);
    }

    #[test]
    fn a_query_limit_is_noticed() {
        assert!(has_limit(
            "fields @message | sort @timestamp desc | limit 5"
        ));
        assert!(has_limit("limit 5"));
        assert!(!has_limit("fields @message | filter @message like /limit/"));
    }

    #[test]
    fn the_description_names_groups_and_examples() {
        let d = describe(&target());
        assert!(d.contains("- /ecs/api\n"));
        assert!(d.contains("every group starting with /aws/lambda/app-"));
        assert!(d.contains("filter @message like /ERROR/"));
    }

    /// Against a real account. Set LIVE_ROLE_ARN, LIVE_EXTERNAL_ID and
    /// LIVE_LOG_GROUP, then run with `cargo test -- --ignored real_logs`.
    #[tokio::test]
    #[ignore = "needs a real AWS account"]
    async fn real_logs_can_be_listed_and_searched() {
        let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} not set"));
        let conf = aws_config::load_from_env().await;
        let sts = aws_sdk_sts::Client::new(&conf);
        let group = env("LIVE_LOG_GROUP");
        let t = AwsTarget {
            role_arn: env("LIVE_ROLE_ARN"),
            external_id: env("LIVE_EXTERNAL_ID"),
            region: "us-east-1".into(),
            log_groups: vec![group.clone()],
            ..Default::default()
        };
        assert!(
            !super::super::aws::opens_without_private_id(&sts, &t).await,
            "role must require the private id"
        );
        let session = super::super::aws::assume(&sts, &t, "integration-test")
            .await
            .unwrap();
        let groups = list_groups(&session).await.unwrap();
        assert!(groups.iter().any(|g| g.name == group));
        let search = check_search(
            &t,
            &[],
            "fields @timestamp, @message | sort @timestamp desc | limit 5",
            Some("24h"),
            None,
            chrono::Utc::now().timestamp(),
        )
        .unwrap();
        let r = run_search(
            &session,
            &search,
            &crate::live::store::default_hidden_fields(),
            4000,
        )
        .await
        .unwrap();
        println!(
            "rows={} hidden={} scanned={:.1}MB ms={} partial={}",
            r.rows, r.hidden_count, r.scanned_mb, r.ms, r.partial
        );
        assert!(r.rows <= 5);
        // a group the team didn't share is refused before anything is sent
        assert!(check_search(
            &t,
            &["/some/other".into()],
            "fields @message",
            None,
            None,
            chrono::Utc::now().timestamp()
        )
        .is_err());
    }
}
