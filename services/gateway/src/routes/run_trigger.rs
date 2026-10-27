//! How a run was started and by whom, for the runs list and a run's page.
//!
//! Runs don't record "how they started" as one field: a run carries
//! `ticket_source` (`github`, `jira`, `alert` or `plugin`), a ticket id whose
//! prefix says which part of CoderHelm made it (`E2E-FIX-`, `LOG-`,
//! `ALERT-`), who asked (`requester_*`, or the free-text `sender` on older
//! runs) and, for runs of a plan's task, `plan_id`. [`kind`] reads those
//! fields alone, so the runs list can filter on it while it scans; [`trigger`]
//! adds the words and the link, using a few records looked up once per page
//! ([`Lookups`]): the alert or log finding behind a ticket, Jira user names,
//! and the team's Jira site.

use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes};
use common::requester::{self, Requester};
use tracing::warn;

use super::run_search::Item;
use crate::AppState;

/// Ticket id of a flaky test CoderHelm fixes (`E2E-FIX-<test id>`).
const E2E_FIX: &str = "E2E-FIX-";
/// Ticket id of a new test CoderHelm writes (`E2E-GEN-<suggestion id>`).
const E2E_GEN: &str = "E2E-GEN-";
/// Ticket id of an alert (`ALERT-<hash>`).
const ALERT: &str = "ALERT-";

/// The group a run's start belongs to, which the runs list filters on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum TriggerKind {
    /// A Jira ticket.
    Jira,
    /// A GitHub issue.
    Github,
    /// CoderHelm's own work: a flaky test, a new test, a log finding it
    /// picked up, a scheduled or release plugin run.
    Coderhelm,
    /// A monitoring alert.
    Alert,
    /// A task of a plan.
    Plan,
    /// A person in the dashboard: a log finding they sent, a plugin they ran.
    Dashboard,
    /// A message in Slack or Microsoft Teams.
    Chat,
}

impl TriggerKind {
    /// From a `started_by` filter value; unknown words are `None`.
    pub fn parse(s: &str) -> Option<TriggerKind> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "jira" => TriggerKind::Jira,
            "github" => TriggerKind::Github,
            "coderhelm" => TriggerKind::Coderhelm,
            "alert" | "alerts" => TriggerKind::Alert,
            "plan" | "plans" => TriggerKind::Plan,
            "dashboard" => TriggerKind::Dashboard,
            "chat" | "slack" | "teams" => TriggerKind::Chat,
            _ => return None,
        })
    }
}

/// How a run was started, in words, with a link to what started it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct RunTrigger {
    /// The group: `jira`, `github`, `coderhelm`, `alert`, `plan`,
    /// `dashboard` or `chat`. Treat a value you don't know as `coderhelm`.
    #[schema(example = "jira")]
    pub kind: TriggerKind,
    /// What started it, to show as is: `Jira`, `GitHub issue`, `Alert`,
    /// `Plan`, `Started by Coderhelm · flaky test`, `Dashboard · log finding`.
    #[schema(example = "Jira")]
    pub label: String,
    /// The thing that started it, when it has a name people know: a ticket
    /// key, `#42`, a plan's or an alert's title, a log group.
    #[schema(example = "WEB-42")]
    pub r#ref: Option<String>,
    /// Where that thing is: a full URL (GitHub, Jira) or a dashboard path
    /// starting with `/` (a test, a log finding, an alert, a plan).
    #[schema(example = "https://acme.atlassian.net/browse/WEB-42")]
    pub ref_url: Option<String>,
    /// The person who started it, when a person did: a name, GitHub login
    /// or email. Null for CoderHelm's own work.
    #[schema(example = "Dana Reyes")]
    pub by: Option<String>,
}

fn s<'a>(item: &'a Item, key: &str) -> &'a str {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("")
}

fn n(item: &Item, key: &str) -> u64 {
    item.get(key)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

/// Senders that name a part of CoderHelm, not a person.
fn is_system_sender(sender: &str) -> bool {
    let s = sender.trim().to_ascii_lowercase();
    matches!(
        s.as_str(),
        "" | "e2e"
            | "alert"
            | "scheduled"
            | "schedule"
            | "release"
            | "system"
            | "log finding"
            | "jira"
    )
}

/// Who asked, as the run recorded it.
fn requester_of(item: &Item) -> Option<Requester> {
    Requester::from_run(
        s(item, "requester_key"),
        s(item, "requester_name"),
        s(item, "requester_email"),
        s(item, "sender"),
    )
}

/// A log finding's run that CoderHelm started on its own (automatic fixes).
fn log_fix_is_automatic(item: &Item) -> bool {
    match s(item, "requester_key") {
        "" => is_system_sender(s(item, "sender")),
        key => key.starts_with(requester::CODERHELM),
    }
}

/// The group a run's start belongs to, from the run's own fields.
pub fn kind(item: &Item) -> TriggerKind {
    let source = s(item, "ticket_source");
    let ticket = s(item, "ticket_id");
    let key = s(item, "requester_key");
    if !s(item, "plan_id").is_empty() {
        return TriggerKind::Plan;
    }
    if source == "plugin" {
        return if s(item, "sender").contains('@') {
            TriggerKind::Dashboard
        } else {
            TriggerKind::Coderhelm
        };
    }
    if ticket.starts_with(E2E_FIX) || ticket.starts_with(E2E_GEN) {
        return TriggerKind::Coderhelm;
    }
    if common::log_findings::is_log_ticket(ticket) {
        return if log_fix_is_automatic(item) {
            TriggerKind::Coderhelm
        } else {
            TriggerKind::Dashboard
        };
    }
    if key.starts_with("slack:") || key.starts_with("teams:") {
        return TriggerKind::Chat;
    }
    if key == format!("{}{}", requester::CODERHELM, requester::SCHEDULED)
        || s(item, "sender").eq_ignore_ascii_case(requester::SCHEDULED)
    {
        return TriggerKind::Coderhelm;
    }
    if source == "alert" || ticket.starts_with(ALERT) {
        return TriggerKind::Alert;
    }
    if source == "jira" {
        return TriggerKind::Jira;
    }
    TriggerKind::Github
}

/// Records looked up for one page of runs.
#[derive(Debug, Default)]
pub struct Lookups {
    /// The team's Jira site, `https://<site>.atlassian.net`.
    pub jira_site: Option<String>,
    /// Settings-table items by sort key: `LOG_TICKET#`, `ALERT_TICKET#`,
    /// `JIRAUSER#`.
    pub records: HashMap<String, Item>,
}

impl Lookups {
    fn record(&self, sk: &str) -> Option<&Item> {
        self.records.get(sk)
    }
}

fn log_ticket_sk(ticket: &str) -> String {
    format!("LOG_TICKET#{ticket}")
}

fn alert_ticket_sk(ticket: &str) -> String {
    format!("ALERT_TICKET#{ticket}")
}

/// The Jira account id behind a run's requester, when that is all it has.
fn jira_account(item: &Item) -> Option<String> {
    let r = requester_of(item)?;
    if !r.name.is_empty() {
        return None;
    }
    r.jira_id().map(str::to_string)
}

/// The settings-table records [`trigger`] reads for these runs.
pub fn lookup_keys<'a>(items: impl IntoIterator<Item = &'a Item>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for item in items {
        let ticket = s(item, "ticket_id");
        let mut keys = Vec::new();
        if common::log_findings::is_log_ticket(ticket) {
            keys.push(log_ticket_sk(ticket));
        } else if ticket.starts_with(ALERT) {
            keys.push(alert_ticket_sk(ticket));
        }
        if let Some(id) = jira_account(item) {
            keys.push(requester::jira_user_sk(&id));
        }
        for k in keys {
            if seen.insert(k.clone()) {
                out.push(k);
            }
        }
    }
    out
}

/// Reads the records [`trigger`] needs for these runs: one batch read of the
/// settings table (100 keys a call) and, when a Jira run is among them, the
/// team's Jira site. A record that can't be read leaves its link out.
pub async fn load_lookups<'a>(
    state: &AppState,
    team_id: &str,
    items: impl IntoIterator<Item = &'a Item> + Clone,
) -> Lookups {
    let mut out = Lookups::default();
    let keys = lookup_keys(items.clone());
    for chunk in keys.chunks(100) {
        let mut ka = KeysAndAttributes::builder();
        for sk in chunk {
            ka = ka.keys(HashMap::from([
                ("pk".to_string(), AttributeValue::S(team_id.to_string())),
                ("sk".to_string(), AttributeValue::S(sk.clone())),
            ]));
        }
        let Ok(ka) = ka.build() else { continue };
        let mut request = HashMap::from([(state.config.settings_table_name.clone(), ka)]);
        // Unprocessed keys come back under load: one more try, then without.
        for _ in 0..2 {
            match state
                .dynamo
                .batch_get_item()
                .set_request_items(Some(request.clone()))
                .send()
                .await
            {
                Ok(res) => {
                    for item in res
                        .responses()
                        .and_then(|r| r.get(&state.config.settings_table_name))
                        .into_iter()
                        .flatten()
                    {
                        let sk = s(item, "sk").to_string();
                        out.records.insert(sk, item.clone());
                    }
                    match res.unprocessed_keys().filter(|u| !u.is_empty()) {
                        Some(left) => request = left.clone(),
                        None => break,
                    }
                }
                Err(e) => {
                    warn!(team_id, error = %e, "Could not read what started the runs");
                    break;
                }
            }
        }
    }
    if items.into_iter().any(|i| s(i, "ticket_source") == "jira") {
        out.jira_site = state
            .dynamo
            .get_item()
            .table_name(&state.config.jira_config_table_name)
            .key("pk", AttributeValue::S(team_id.to_string()))
            .key("sk", AttributeValue::S("JIRA#config".into()))
            .projection_expression("site_url")
            .send()
            .await
            .ok()
            .and_then(|r| r.item)
            .map(|i| s(&i, "site_url").trim_end_matches('/').to_string())
            .filter(|u| u.starts_with("https://"));
    }
    out
}

/// The person who started the run, in words, or `None` for CoderHelm's own
/// work and runs that recorded no one.
pub fn person(item: &Item, lookups: &Lookups) -> Option<String> {
    let r = requester_of(item)?;
    if r.is_coderhelm() {
        return None;
    }
    if s(item, "requester_key").is_empty() && is_system_sender(s(item, "sender")) {
        return None;
    }
    if !r.name.is_empty() && !requester::is_jira_account_id(&r.name) {
        return Some(r.name);
    }
    if let Some(id) = r.jira_id() {
        return lookups
            .record(&requester::jira_user_sk(id))
            .map(|u| s(u, "name").trim().to_string())
            .filter(|n| !n.is_empty());
    }
    if let Some(login) = r.key.strip_prefix("github:") {
        return Some(login.to_string());
    }
    if !r.email.is_empty() {
        return Some(r.email);
    }
    r.key
        .strip_prefix("email:")
        .or_else(|| r.key.strip_prefix("name:"))
        .map(str::to_string)
}

/// The issue number of a GitHub run: `issue_number`, or the number in
/// `GH-42` / `owner/name#42`.
fn issue_number(item: &Item) -> Option<u64> {
    let n = n(item, "issue_number");
    if n > 0 {
        return Some(n);
    }
    let ticket = s(item, "ticket_id");
    ticket
        .rsplit_once(['-', '#'])
        .and_then(|(_, d)| d.parse::<u64>().ok())
        .filter(|n| *n > 0)
}

fn q(v: &str) -> String {
    urlencoding::encode(v).into_owned()
}

fn some(v: &str) -> Option<String> {
    let v = v.trim();
    (!v.is_empty()).then(|| v.to_string())
}

/// How a run was started, in words and with a link.
pub fn trigger(item: &Item, lookups: &Lookups) -> RunTrigger {
    let kind = kind(item);
    let ticket = s(item, "ticket_id");
    let by = person(item, lookups);
    let t = |label: &str, r: Option<String>, url: Option<String>, by: Option<String>| RunTrigger {
        kind,
        label: label.to_string(),
        r#ref: r,
        ref_url: url,
        by,
    };
    match kind {
        TriggerKind::Plan => t(
            "Plan",
            some(s(item, "plan_title")),
            Some(format!("/plans/detail?id={}", q(s(item, "plan_id")))),
            some(s(item, "plan_by")).or(by),
        ),
        TriggerKind::Coderhelm | TriggerKind::Dashboard if s(item, "ticket_source") == "plugin" => {
            let plugin = some(s(item, "plugin_id"))
                .or_else(|| ticket.strip_prefix("plugin:").and_then(some));
            let url = plugin
                .as_deref()
                .map(|p| format!("/plugins/plugin?id={}", q(p)));
            if kind == TriggerKind::Coderhelm {
                t("Started by Coderhelm · plugin", plugin, url, None)
            } else {
                t("Dashboard · plugin", plugin, url, by)
            }
        }
        _ if ticket.starts_with(E2E_FIX) => t(
            "Started by Coderhelm · flaky test",
            None,
            Some(format!("/tests/test?id={}", q(&ticket[E2E_FIX.len()..]))),
            None,
        ),
        _ if ticket.starts_with(E2E_GEN) => t(
            "Started by Coderhelm · new test",
            None,
            Some("/tests".to_string()),
            None,
        ),
        _ if common::log_findings::is_log_ticket(ticket) => {
            let rec = lookups.record(&log_ticket_sk(ticket));
            let url = rec
                .map(|r| s(r, "rec_id"))
                .filter(|id| !id.is_empty())
                .map(|id| format!("/logs?finding={}", q(id)))
                .unwrap_or_else(|| "/logs".to_string());
            let group = rec.and_then(|r| some(s(r, "log_group")));
            if kind == TriggerKind::Coderhelm {
                t("Started by Coderhelm · log finding", group, Some(url), None)
            } else {
                t("Dashboard · log finding", group, Some(url), by)
            }
        }
        TriggerKind::Coderhelm => t("Started by Coderhelm · scheduled", None, None, None),
        TriggerKind::Alert => {
            let rec = lookups.record(&alert_ticket_sk(ticket));
            let url = rec
                .map(|r| s(r, "alert_id"))
                .filter(|id| !id.is_empty())
                .map(|id| format!("/alerts/detail?id={}", q(id)))
                .unwrap_or_else(|| "/alerts".to_string());
            let name = rec
                .and_then(|r| some(s(r, "title")))
                .or_else(|| some(s(item, "title")));
            t("Alert", name, Some(url), None)
        }
        TriggerKind::Jira => t(
            "Jira",
            some(ticket),
            lookups
                .jira_site
                .as_deref()
                .filter(|_| !ticket.is_empty())
                .map(|site| format!("{site}/browse/{}", q(ticket))),
            by,
        ),
        TriggerKind::Chat => t(
            if s(item, "requester_key").starts_with("teams:") {
                "Teams"
            } else {
                "Slack"
            },
            None,
            None,
            by,
        ),
        TriggerKind::Dashboard => t("Dashboard", None, None, by),
        TriggerKind::Github => {
            let repo = s(item, "repo");
            match issue_number(item) {
                Some(num) => t(
                    "GitHub issue",
                    Some(format!("#{num}")),
                    (repo.split_once('/').is_some())
                        .then(|| format!("https://github.com/{repo}/issues/{num}")),
                    by,
                ),
                None => t("GitHub", some(ticket), None, by),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(fields: &[(&str, &str)]) -> Item {
        fields
            .iter()
            .map(|(k, v)| {
                let v = if *k == "issue_number" {
                    AttributeValue::N((*v).into())
                } else {
                    AttributeValue::S((*v).into())
                };
                (k.to_string(), v)
            })
            .collect()
    }

    fn rec(fields: &[(&str, &str)]) -> Item {
        run(fields)
    }

    #[test]
    fn jira_ticket_with_a_named_person_and_the_site() {
        let item = run(&[
            ("ticket_source", "jira"),
            ("ticket_id", "WEB-42"),
            ("requester_key", "jira:5b10ac8d82e05b22cc7d4ef5"),
            ("requester_name", "Dana Reyes"),
        ]);
        let l = Lookups {
            jira_site: Some("https://acme.atlassian.net".into()),
            ..Default::default()
        };
        let tr = trigger(&item, &l);
        assert_eq!(tr.kind, TriggerKind::Jira);
        assert_eq!(tr.label, "Jira");
        assert_eq!(tr.r#ref.as_deref(), Some("WEB-42"));
        assert_eq!(
            tr.ref_url.as_deref(),
            Some("https://acme.atlassian.net/browse/WEB-42")
        );
        assert_eq!(tr.by.as_deref(), Some("Dana Reyes"));
    }

    #[test]
    fn jira_account_id_only_reads_the_name_it_was_seen_with() {
        let item = run(&[
            ("ticket_source", "jira"),
            ("ticket_id", "WEB-7"),
            ("sender", "5b10ac8d82e05b22cc7d4ef5"),
        ]);
        assert_eq!(
            lookup_keys([&item]),
            vec!["JIRAUSER#5b10ac8d82e05b22cc7d4ef5".to_string()]
        );
        let mut l = Lookups::default();
        assert_eq!(trigger(&item, &l).by, None);
        l.records.insert(
            "JIRAUSER#5b10ac8d82e05b22cc7d4ef5".into(),
            rec(&[("name", "Sam Ito")]),
        );
        let tr = trigger(&item, &l);
        assert_eq!(tr.by.as_deref(), Some("Sam Ito"));
        // No site known: no link, the key still shows.
        assert_eq!(tr.ref_url, None);
        assert_eq!(tr.r#ref.as_deref(), Some("WEB-7"));
    }

    #[test]
    fn github_issue_links_to_the_issue() {
        let item = run(&[
            ("ticket_source", "github"),
            ("ticket_id", "GH-42"),
            ("repo", "acme/web"),
            ("issue_number", "42"),
            ("sender", "octocat"),
            ("requester_key", "github:octocat"),
        ]);
        let tr = trigger(&item, &Lookups::default());
        assert_eq!(tr.kind, TriggerKind::Github);
        assert_eq!(tr.label, "GitHub issue");
        assert_eq!(tr.r#ref.as_deref(), Some("#42"));
        assert_eq!(
            tr.ref_url.as_deref(),
            Some("https://github.com/acme/web/issues/42")
        );
        assert_eq!(tr.by.as_deref(), Some("octocat"));
    }

    #[test]
    fn github_number_from_the_ticket_id_on_older_runs() {
        let item = run(&[("ticket_id", "acme/web#318"), ("repo", "acme/web")]);
        let tr = trigger(&item, &Lookups::default());
        assert_eq!(tr.kind, TriggerKind::Github);
        assert_eq!(tr.r#ref.as_deref(), Some("#318"));
    }

    #[test]
    fn flaky_test_fix_never_shows_its_internal_id() {
        let item = run(&[
            ("ticket_source", "alert"),
            ("ticket_id", "E2E-FIX-a7f51433483adf820849"),
            ("sender", "e2e"),
            ("requester_key", "coderhelm:e2e"),
            ("title", "Fix flaky test: checkout works"),
        ]);
        let tr = trigger(&item, &Lookups::default());
        assert_eq!(tr.kind, TriggerKind::Coderhelm);
        assert_eq!(tr.label, "Started by Coderhelm · flaky test");
        assert_eq!(tr.r#ref, None);
        assert_eq!(
            tr.ref_url.as_deref(),
            Some("/tests/test?id=a7f51433483adf820849")
        );
        assert_eq!(tr.by, None);
        // The alert source alone doesn't make it an alert.
        assert_ne!(kind(&item), TriggerKind::Alert);
    }

    #[test]
    fn new_test_is_coderhelm() {
        let item = run(&[("ticket_source", "alert"), ("ticket_id", "E2E-GEN-01ABC")]);
        let tr = trigger(&item, &Lookups::default());
        assert_eq!(tr.label, "Started by Coderhelm · new test");
        assert_eq!(tr.ref_url.as_deref(), Some("/tests"));
    }

    #[test]
    fn log_finding_automatic_or_by_a_person() {
        let auto = run(&[
            ("ticket_source", "alert"),
            ("ticket_id", "LOG-1A2B3C4D"),
            ("sender", "log finding"),
            ("requester_key", "coderhelm:log_finding"),
        ]);
        let mut l = Lookups::default();
        l.records.insert(
            "LOG_TICKET#LOG-1A2B3C4D".into(),
            rec(&[("rec_id", "rec-9"), ("log_group", "/aws/lambda/api")]),
        );
        let tr = trigger(&auto, &l);
        assert_eq!(tr.kind, TriggerKind::Coderhelm);
        assert_eq!(tr.label, "Started by Coderhelm · log finding");
        assert_eq!(tr.r#ref.as_deref(), Some("/aws/lambda/api"));
        assert_eq!(tr.ref_url.as_deref(), Some("/logs?finding=rec-9"));

        let person = run(&[
            ("ticket_source", "alert"),
            ("ticket_id", "LOG-1A2B3C4D"),
            ("sender", "dev@acme.test"),
            ("requester_key", "email:dev@acme.test"),
            ("requester_email", "dev@acme.test"),
        ]);
        let tr = trigger(&person, &Lookups::default());
        assert_eq!(tr.kind, TriggerKind::Dashboard);
        assert_eq!(tr.label, "Dashboard · log finding");
        assert_eq!(tr.ref_url.as_deref(), Some("/logs"));
        assert_eq!(tr.by.as_deref(), Some("dev@acme.test"));
        assert_eq!(lookup_keys([&person]), vec!["LOG_TICKET#LOG-1A2B3C4D"]);
    }

    #[test]
    fn alert_names_the_alarm_and_links_the_event() {
        let item = run(&[
            ("ticket_source", "alert"),
            ("ticket_id", "ALERT-3f9c2a71"),
            ("sender", "alert"),
            ("requester_key", "coderhelm:alert"),
            ("title", "ALARM: api-5xx"),
        ]);
        let mut l = Lookups::default();
        assert_eq!(
            trigger(&item, &l).ref_url.as_deref(),
            Some("/alerts"),
            "an expired record still links somewhere"
        );
        l.records.insert(
            "ALERT_TICKET#ALERT-3f9c2a71".into(),
            rec(&[("alert_id", "ev 1"), ("title", "api-5xx")]),
        );
        let tr = trigger(&item, &l);
        assert_eq!(tr.kind, TriggerKind::Alert);
        assert_eq!(tr.label, "Alert");
        assert_eq!(tr.r#ref.as_deref(), Some("api-5xx"));
        assert_eq!(tr.ref_url.as_deref(), Some("/alerts/detail?id=ev%201"));
        assert_eq!(tr.by, None);
    }

    #[test]
    fn plan_task_names_the_plan_and_who_ran_it() {
        let item = run(&[
            ("ticket_source", "github"),
            ("ticket_id", "GH-9"),
            ("repo", "acme/web"),
            ("sender", "coderhelm-app[bot]"),
            ("plan_id", "p1"),
            ("plan_title", "Rate limits"),
            ("plan_by", "dana@acme.test"),
        ]);
        let tr = trigger(&item, &Lookups::default());
        assert_eq!(tr.kind, TriggerKind::Plan);
        assert_eq!(tr.label, "Plan");
        assert_eq!(tr.r#ref.as_deref(), Some("Rate limits"));
        assert_eq!(tr.ref_url.as_deref(), Some("/plans/detail?id=p1"));
        assert_eq!(tr.by.as_deref(), Some("dana@acme.test"));
    }

    #[test]
    fn plugin_by_a_person_or_by_a_release() {
        let manual = run(&[
            ("ticket_source", "plugin"),
            ("ticket_id", "plugin:release-notes"),
            ("plugin_id", "release-notes"),
            ("sender", "dev@acme.test"),
        ]);
        let tr = trigger(&manual, &Lookups::default());
        assert_eq!(tr.kind, TriggerKind::Dashboard);
        assert_eq!(tr.label, "Dashboard · plugin");
        assert_eq!(tr.r#ref.as_deref(), Some("release-notes"));
        assert_eq!(
            tr.ref_url.as_deref(),
            Some("/plugins/plugin?id=release-notes")
        );
        assert_eq!(tr.by.as_deref(), Some("dev@acme.test"));

        let release = run(&[
            ("ticket_source", "plugin"),
            ("ticket_id", "plugin:release-notes"),
            ("sender", "release"),
        ]);
        let tr = trigger(&release, &Lookups::default());
        assert_eq!(tr.kind, TriggerKind::Coderhelm);
        assert_eq!(tr.label, "Started by Coderhelm · plugin");
        assert_eq!(tr.by, None);
    }

    #[test]
    fn scheduled_and_chat() {
        let sched = run(&[("ticket_source", "github"), ("sender", "scheduled")]);
        assert_eq!(kind(&sched), TriggerKind::Coderhelm);
        let slack = run(&[
            ("ticket_source", "github"),
            ("requester_key", "slack:u123"),
            ("requester_name", "Ana"),
        ]);
        let tr = trigger(&slack, &Lookups::default());
        assert_eq!(tr.kind, TriggerKind::Chat);
        assert_eq!(tr.label, "Slack");
        assert_eq!(tr.by.as_deref(), Some("Ana"));
    }

    #[test]
    fn parse_filter_words() {
        assert_eq!(TriggerKind::parse("Alerts"), Some(TriggerKind::Alert));
        assert_eq!(TriggerKind::parse("plans"), Some(TriggerKind::Plan));
        assert_eq!(TriggerKind::parse("nope"), None);
        for k in [
            TriggerKind::Jira,
            TriggerKind::Github,
            TriggerKind::Coderhelm,
            TriggerKind::Alert,
            TriggerKind::Plan,
            TriggerKind::Dashboard,
            TriggerKind::Chat,
        ] {
            let word = serde_json::to_value(k).unwrap();
            assert_eq!(TriggerKind::parse(word.as_str().unwrap()), Some(k));
        }
    }
}
