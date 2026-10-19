//! New Relic through NerdGraph (its GraphQL API) with a user key.
//!
//! Every request is built here from a fixed query and checked as a single
//! read-only GraphQL query before it is sent: there is no way to send a
//! mutation. NRQL (New Relic's query language) only reads, and is also checked
//! to be one SELECT/FROM/SHOW statement. Only the team's chosen accounts are
//! queried, and results pass through the same redaction as every lookup.

use std::time::Duration;

use serde_json::{json, Value};

use super::graphql::check_read_only;
use super::http::{window, Method, Request, Transport};
use super::redact::{hidden_terms, redact_all};
use super::StepError;
use crate::live::store::default_hidden_fields;

pub const US_ENDPOINT: &str = "https://api.newrelic.com/graphql";
pub const EU_ENDPOINT: &str = "https://api.eu.newrelic.com/graphql";
const MAX_NRQL_CHARS: usize = 4_000;
/// The longest window a lookup may cover.
const MAX_WINDOW: Duration = Duration::from_secs(14 * 86_400);
const DEFAULT_WINDOW: Duration = Duration::from_secs(2 * 3600);

pub fn endpoint(region: &str) -> &'static str {
    if region.eq_ignore_ascii_case("eu") {
        EU_ENDPOINT
    } else {
        US_ENDPOINT
    }
}

fn refused(message: impl Into<String>, fix: &str) -> StepError {
    StepError::new("read_only", message, fix)
}

/// Checks NRQL: one statement that starts with SELECT, FROM or SHOW.
pub fn check_nrql(nrql: &str) -> Result<String, StepError> {
    let q = nrql.trim().trim_end_matches(';').trim();
    let fix = "Send one NRQL query, like SELECT count(*) FROM Transaction SINCE 1 hour ago.";
    if q.is_empty() {
        return Err(refused("The query is empty.", fix));
    }
    if q.len() > MAX_NRQL_CHARS {
        return Err(refused("That query is too long.", fix));
    }
    if q.contains(';') {
        return Err(refused("Only one query can be sent at a time.", fix));
    }
    let first = q
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    if !matches!(first.as_str(), "SELECT" | "FROM" | "SHOW") {
        return Err(refused("A query starts with SELECT, FROM or SHOW.", fix));
    }
    Ok(q.to_string())
}

/// A value placed inside single quotes in NRQL.
pub fn nrql_text(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// The account to query: the one asked for if the team allows it, or the
/// only allowed one.
pub fn pick_account(asked: Option<&str>, allowed: &[String]) -> Result<i64, StepError> {
    let asked = asked.map(str::trim).filter(|a| !a.is_empty());
    let id = match (asked, allowed) {
        (Some(a), _) => a.to_string(),
        (None, [one]) => one.clone(),
        (None, []) => {
            return Err(refused(
                "Say which New Relic account to query.",
                "Pass `account` (newrelic_accounts lists them).",
            ))
        }
        (None, _) => {
            return Err(refused(
                format!(
                    "Say which New Relic account to query: {}.",
                    allowed.join(", ")
                ),
                "Pass `account`.",
            ))
        }
    };
    if !allowed.is_empty() && !allowed.iter().any(|x| x == &id) {
        return Err(refused(
            format!("Account {id} isn't one the team shared."),
            "Use one of the accounts newrelic_accounts lists.",
        ));
    }
    id.parse::<i64>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| refused(format!("{id} isn't an account number."), "Pass the number."))
}

/// A New Relic connection for one call.
pub struct NewRelic<'a> {
    pub key: &'a str,
    pub region: &'a str,
    /// Accounts the team allows. Empty: any the key sees.
    pub accounts: &'a [String],
    pub http: &'a dyn Transport,
}

/// What came back, cleaned.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    /// What was asked, for the activity log.
    pub asked: String,
    pub data: Value,
    pub hidden: usize,
}

impl NewRelic<'_> {
    /// Sends one read-only GraphQL query built here.
    async fn query(&self, query: &str, variables: Value) -> Result<Value, StepError> {
        let read = check_read_only(query)?;
        let reply = self
            .http
            .send(Request {
                method: Method::Post,
                url: endpoint(self.region).to_string(),
                headers: vec![
                    ("API-Key".to_string(), self.key.to_string()),
                    ("Content-Type".to_string(), "application/json".to_string()),
                ],
                body: Some(json!({ "query": read.text, "variables": variables })),
            })
            .await?;
        if reply.status == 401 || reply.status == 403 {
            return Err(StepError::new(
                "sign_in",
                "New Relic didn't accept the key.",
                "Paste a new user key (it starts with NRAK-) in Data sources.",
            ));
        }
        let body = reply.json().ok_or_else(|| {
            StepError::new(
                "read",
                format!(
                    "New Relic answered {} with something that isn't JSON.",
                    reply.status
                ),
                "Try again in a minute.",
            )
        })?;
        if let Some(errs) = body["errors"].as_array().filter(|e| !e.is_empty()) {
            let msg = errs
                .iter()
                .filter_map(|e| e["message"].as_str())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(StepError::new(
                "read",
                format!(
                    "New Relic refused the lookup: {}",
                    crate::truncate_str(&msg, 300)
                ),
                "Check the account, the names and the NRQL.",
            ));
        }
        Ok(body["data"].clone())
    }

    fn clean(&self, asked: String, mut data: Value) -> Answer {
        let hidden = redact_all(&mut data, &hidden_terms(&default_hidden_fields()));
        Answer {
            asked,
            data,
            hidden,
        }
    }

    /// The accounts the key can see, limited to the ones the team allows.
    pub async fn accounts(&self) -> Result<Answer, StepError> {
        let data = self
            .query("{ actor { accounts { id name } } }", json!({}))
            .await?;
        let list: Vec<Value> = data["actor"]["accounts"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|a| {
                self.accounts.is_empty()
                    || self.accounts.iter().any(|x| {
                        a["id"].as_i64().is_some_and(|n| n.to_string() == *x)
                            || a["id"].as_str() == Some(x)
                    })
            })
            .collect();
        Ok(self.clean("list accounts".into(), json!(list)))
    }

    /// One NRQL query in one account.
    pub async fn nrql(&self, account: Option<&str>, nrql: &str) -> Result<Answer, StepError> {
        let q = check_nrql(nrql)?;
        let id = pick_account(account, self.accounts)?;
        let data = self
            .query(
                "query($id: Int!, $q: Nrql!) { actor { account(id: $id) { nrql(query: $q, timeout: 30) { results } } } }",
                json!({ "id": id, "q": q }),
            )
            .await?;
        let results = data["actor"]["account"]["nrql"]["results"].clone();
        Ok(self.clean(format!("[{id}] {q}"), results))
    }

    /// Entities (services, hosts, browser apps) whose name contains `name`.
    pub async fn entities(&self, name: &str, domain: Option<&str>) -> Result<Answer, StepError> {
        let name = name.trim();
        if name.is_empty() || name.len() > 200 {
            return Err(refused("Say part of the entity's name.", "Pass `name`."));
        }
        let mut search = format!("name LIKE '{}'", nrql_text(name));
        if let Some(d) = domain.map(str::trim).filter(|d| !d.is_empty()) {
            if !d.chars().all(|c| c.is_ascii_alphabetic()) || d.len() > 10 {
                return Err(refused(
                    "The domain is a word like APM, BROWSER, INFRA, MOBILE or SYNTH.",
                    "Pass a domain from that list or none.",
                ));
            }
            search.push_str(&format!(" AND domain = '{}'", d.to_ascii_uppercase()));
        }
        if !self.accounts.is_empty() {
            let ids: Vec<&str> = self
                .accounts
                .iter()
                .map(String::as_str)
                .filter(|a| a.chars().all(|c| c.is_ascii_digit()))
                .collect();
            if !ids.is_empty() {
                search.push_str(&format!(" AND accountId IN ({})", ids.join(", ")));
            }
        }
        let data = self
            .query(
                "query($q: String!) { actor { entitySearch(query: $q) { count results { entities { guid name entityType domain type accountId reporting alertSeverity } } } } }",
                json!({ "q": search }),
            )
            .await?;
        let found = data["actor"]["entitySearch"].clone();
        Ok(self.clean(format!("entities where {search}"), found))
    }

    /// Change events (deployment markers) in a window, optionally for one
    /// app or service by name.
    pub async fn changes(
        &self,
        account: Option<&str>,
        entity: Option<&str>,
        since: Option<&str>,
        until: Option<&str>,
        now: i64,
    ) -> Result<Answer, StepError> {
        let (start, end) = window(
            since,
            until,
            now,
            Duration::from_secs(24 * 3600),
            MAX_WINDOW,
        )?;
        let filter = entity
            .map(str::trim)
            .filter(|e| !e.is_empty())
            .map(|e| {
                let e = nrql_text(e);
                format!(" WHERE entity.name = '{e}' OR appName = '{e}'")
            })
            .unwrap_or_default();
        let q = format!(
            "SELECT * FROM Deployment{filter} SINCE {} UNTIL {} LIMIT 50",
            start * 1000,
            end * 1000
        );
        self.nrql(account, &q).await
    }

    /// Response time, error rate and throughput for one APM app before and
    /// after a moment (a deploy), over `minutes` on each side.
    pub async fn deployment_impact(
        &self,
        account: Option<&str>,
        app: &str,
        at: &str,
        minutes: u32,
        now: i64,
    ) -> Result<Answer, StepError> {
        let app = app.trim();
        if app.is_empty() || app.len() > 200 {
            return Err(refused("Say the app's name.", "Pass `app`."));
        }
        let at = chrono::DateTime::parse_from_rfc3339(at.trim())
            .map(|d| d.timestamp())
            .map_err(|_| {
                refused(
                    "`at` isn't a time.",
                    "Use a time like 2026-01-31T14:00:00Z.",
                )
            })?;
        let m = i64::from(minutes.clamp(5, 360));
        if at > now {
            return Err(refused(
                "That moment is in the future.",
                "Pass a past time.",
            ));
        }
        let after_end = (at + m * 60).min(now);
        let app = nrql_text(app);
        let select =
            "SELECT average(duration) AS 'response_time_s', percentile(duration, 95) AS 'p95_s', \
                      percentage(count(*), WHERE error IS true) AS 'error_rate_pct', \
                      rate(count(*), 1 minute) AS 'throughput_rpm' FROM Transaction";
        let before = format!(
            "{select} WHERE appName = '{app}' SINCE {} UNTIL {}",
            (at - m * 60) * 1000,
            at * 1000
        );
        let after = format!(
            "{select} WHERE appName = '{app}' SINCE {} UNTIL {}",
            at * 1000,
            after_end * 1000
        );
        let b = self.nrql(account, &before).await?;
        let a = self.nrql(account, &after).await?;
        Ok(Answer {
            asked: format!("deployment impact for {app} at {at} (±{m} min)"),
            data: json!({ "before": b.data, "after": a.data, "minutes_each_side": m }),
            hidden: a.hidden + b.hidden,
        })
    }

    /// Response time, error rate and throughput for one APM app over time.
    pub async fn golden(
        &self,
        account: Option<&str>,
        app: &str,
        since: Option<&str>,
        until: Option<&str>,
        now: i64,
    ) -> Result<Answer, StepError> {
        let app = app.trim();
        if app.is_empty() || app.len() > 200 {
            return Err(refused("Say the app's name.", "Pass `app`."));
        }
        let (start, end) = window(since, until, now, DEFAULT_WINDOW, MAX_WINDOW)?;
        let bucket = ((end - start) / 60 / 30).max(1);
        let q = format!(
            "SELECT average(duration) AS 'response_time_s', percentage(count(*), WHERE error IS true) AS 'error_rate_pct', \
             rate(count(*), 1 minute) AS 'throughput_rpm' FROM Transaction WHERE appName = '{}' \
             SINCE {} UNTIL {} TIMESERIES {bucket} minutes",
            nrql_text(app),
            start * 1000,
            end * 1000
        );
        self.nrql(account, &q).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::http::fake::Fake;

    fn nr<'a>(http: &'a Fake, accounts: &'a [String]) -> NewRelic<'a> {
        NewRelic {
            key: "NRAK-TEST",
            region: "us",
            accounts,
            http,
        }
    }

    #[test]
    fn nrql_is_one_reading_statement() {
        assert!(check_nrql("SELECT count(*) FROM Transaction").is_ok());
        assert!(check_nrql("FROM Transaction SELECT count(*);").is_ok());
        assert!(check_nrql("SHOW EVENT TYPES").is_ok());
        assert!(check_nrql("").is_err());
        assert!(check_nrql("SELECT 1; SELECT 2").is_err());
        assert!(check_nrql("DELETE FROM Transaction").is_err());
        assert!(check_nrql(&"SELECT ".repeat(1000)).is_err());
    }

    #[test]
    fn only_shared_accounts_are_queried() {
        let allowed = vec!["111".to_string(), "222".to_string()];
        assert_eq!(pick_account(Some("111"), &allowed).unwrap(), 111);
        assert!(pick_account(Some("333"), &allowed).is_err());
        assert!(pick_account(None, &allowed).is_err(), "two to choose from");
        assert_eq!(pick_account(None, &["9".to_string()]).unwrap(), 9);
        assert_eq!(pick_account(Some("42"), &[]).unwrap(), 42);
        assert!(pick_account(Some("abc"), &[]).is_err());
        assert!(pick_account(None, &[]).is_err());
    }

    #[test]
    fn quotes_in_names_cannot_end_the_string() {
        assert_eq!(nrql_text("a'b"), "a\\'b");
        assert_eq!(nrql_text("a\\'"), "a\\\\\\'");
    }

    #[tokio::test]
    async fn a_query_is_a_read_only_post_with_the_key_header() {
        let http = Fake::answering(vec![(
            200,
            r#"{"data":{"actor":{"account":{"nrql":{"results":[{"count":3,"email":"a@b.com"}]}}}}}"#,
        )]);
        let accounts = vec!["111".to_string()];
        let a = nr(&http, &accounts)
            .nrql(None, "SELECT count(*) FROM Transaction")
            .await
            .unwrap();
        assert_eq!(a.data[0]["count"], 3);
        assert_eq!(a.data[0]["email"], "[hidden]", "redacted");
        assert!(a.hidden >= 1);
        let sent = http.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].method, Method::Post);
        assert_eq!(sent[0].url, US_ENDPOINT);
        assert!(sent[0]
            .headers
            .iter()
            .any(|(k, v)| k == "API-Key" && v == "NRAK-TEST"));
        let q = sent[0].body.as_ref().unwrap()["query"].as_str().unwrap();
        assert!(q.trim_start().starts_with("query"), "{q}");
        assert!(!q.contains("mutation"));
        assert_eq!(sent[0].body.as_ref().unwrap()["variables"]["id"], 111);
    }

    #[tokio::test]
    async fn a_refused_query_is_never_sent() {
        let http = Fake::default();
        let accounts = vec!["111".to_string()];
        assert!(nr(&http, &accounts)
            .nrql(Some("999"), "SELECT 1 FROM Transaction")
            .await
            .is_err());
        assert!(nr(&http, &accounts).nrql(None, "DROP x").await.is_err());
        assert!(http.sent().is_empty());
    }

    #[tokio::test]
    async fn a_bad_key_says_so() {
        let http = Fake::answering(vec![(401, "{}")]);
        let e = nr(&http, &[]).accounts().await.unwrap_err();
        assert_eq!(e.step, "sign_in");
    }

    #[tokio::test]
    async fn deployment_impact_compares_both_sides() {
        let row =
            r#"{"data":{"actor":{"account":{"nrql":{"results":[{"response_time_s":0.2}]}}}}}"#;
        let http = Fake::answering(vec![(200, row), (200, row)]);
        let accounts = vec!["1".to_string()];
        let now = 1_800_000_000;
        let a = nr(&http, &accounts)
            .deployment_impact(None, "api's", "2027-01-15T08:00:00Z", 30, now)
            .await
            .unwrap();
        assert_eq!(a.data["minutes_each_side"], 30);
        let sent = http.sent();
        assert_eq!(sent.len(), 2);
        let q0 = sent[0].body.as_ref().unwrap()["variables"]["q"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(q0.contains("appName = 'api\\'s'"), "{q0}");
    }

    #[tokio::test]
    async fn the_eu_region_uses_the_eu_address() {
        let http = Fake::answering(vec![(200, r#"{"data":{"actor":{"accounts":[]}}}"#)]);
        let c = NewRelic {
            key: "k",
            region: "eu",
            accounts: &[],
            http: &http,
        };
        c.accounts().await.unwrap();
        assert_eq!(http.sent()[0].url, EU_ENDPOINT);
    }
}
