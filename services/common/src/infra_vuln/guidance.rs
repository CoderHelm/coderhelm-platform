//! What AWS and each infrastructure tool say about applying an update,
//! kept as data: one entry per fact, per service, per kind of update and
//! per tool, each with the page it came from and the day it was last read
//! (`guidance/*.json`).
//!
//! Both the pull request ticket ([`super::records`]) and the runbook
//! ([`super::runbook`]) are written from these entries; neither holds
//! service facts of its own. The replacement guard ([`super::guard`])
//! reads its lists of replacing attributes from here too.
//!
//! - An entry with no source or no review date fails the tests.
//! - An entry not re-read for [`MAX_AGE_DAYS`] is stale: it is still
//!   shown, flagged with its date, so a reader knows to check the page.
//! - Version numbers, dates and thresholds that move are not kept here:
//!   they come from the account's own reads and the daily feeds.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

/// An entry not re-read for this long is flagged.
pub const MAX_AGE_DAYS: i64 = 180;

/// The services with entries.
pub const SERVICES: &[&str] = &[
    "rds",
    "aurora",
    "elasticache",
    "opensearch",
    "eks_nodegroup",
    "ami",
    "lambda_runtime",
    "any",
];

/// The kinds of update an entry speaks of.
pub const ACTIONS: &[&str] = &[
    "os_update",
    "engine_patch",
    "engine_upgrade",
    "service_update",
    "service_software",
    "nodegroup_release",
    "ami_replace",
    "runtime_change",
    "any",
];

/// The tools an entry speaks of (`any`: true whatever defines the resource).
pub const TOOLS: &[&str] = &["terraform", "cloudformation", "cdk", "any"];

/// What an entry says.
pub const SECTIONS: &[&str] = &[
    "what",
    "in_code",
    "cve",
    "attribute",
    "takes_effect",
    "interruption",
    "replacement",
    "option",
    "trap",
    "permissions",
    "precheck",
    "backup",
    "console",
    "apply",
    "validation",
    "rollback",
    "if_nothing",
    "order",
    "plan",
    "pipeline",
];

/// One fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub id: String,
    pub service: String,
    pub action: String,
    pub tool: String,
    pub section: String,
    /// The fact, with `{placeholders}` for the resource's own identifiers.
    pub text: String,
    /// A command that goes with it, with the same placeholders.
    #[serde(default)]
    pub cli: String,
    /// Facts about the resource that must not contradict (`major`: `true`).
    #[serde(default)]
    pub when: BTreeMap<String, String>,
    /// `attribute`: the attribute the change edits. `replacement`: the
    /// attributes whose change replaces the resource.
    #[serde(default)]
    pub attributes: Vec<String>,
    /// `replacement`: what must be present for the change to be in place.
    #[serde(default)]
    pub requires: String,
    /// The page the fact was read from.
    pub source: String,
    /// The day it was last read (YYYY-MM-DD).
    pub reviewed_on: String,
}

const FILES: &[&str] = &[
    include_str!("guidance/rds.json"),
    include_str!("guidance/aurora.json"),
    include_str!("guidance/elasticache.json"),
    include_str!("guidance/opensearch.json"),
    include_str!("guidance/compute.json"),
];

/// Reads one guidance file. A file that does not parse is an error, never
/// an empty store.
pub fn parse(json: &str) -> Result<Vec<Entry>, String> {
    serde_json::from_str(json).map_err(|e| e.to_string())
}

/// Every entry.
pub fn store() -> &'static [Entry] {
    static STORE: OnceLock<Vec<Entry>> = OnceLock::new();
    STORE.get_or_init(|| {
        FILES
            .iter()
            .flat_map(|f| parse(f).expect("the guidance files parse (a test keeps them so)"))
            .collect()
    })
}

/// What is wrong with an entry, empty when nothing is.
pub fn problems(e: &Entry) -> Vec<String> {
    let mut out = Vec::new();
    if e.id.trim().is_empty() {
        out.push("no id".to_string());
    }
    if !SERVICES.contains(&e.service.as_str()) {
        out.push(format!("unknown service `{}`", e.service));
    }
    if !ACTIONS.contains(&e.action.as_str()) {
        out.push(format!("unknown action `{}`", e.action));
    }
    if !TOOLS.contains(&e.tool.as_str()) {
        out.push(format!("unknown tool `{}`", e.tool));
    }
    if !SECTIONS.contains(&e.section.as_str()) {
        out.push(format!("unknown section `{}`", e.section));
    }
    if e.text.trim().len() < 20 {
        out.push("no text".to_string());
    }
    if !e.source.starts_with("https://") || e.source.len() < 20 {
        out.push("no source link".to_string());
    }
    if NaiveDate::parse_from_str(&e.reviewed_on, "%Y-%m-%d").is_err() {
        out.push("no review date (YYYY-MM-DD)".to_string());
    }
    out
}

/// Whether an entry was last read more than [`MAX_AGE_DAYS`] before
/// `today` (YYYY-MM-DD). An unreadable date counts as stale.
pub fn is_stale(e: &Entry, today: &str) -> bool {
    let (Ok(read), Ok(now)) = (
        NaiveDate::parse_from_str(&e.reviewed_on, "%Y-%m-%d"),
        NaiveDate::parse_from_str(today, "%Y-%m-%d"),
    ) else {
        return true;
    };
    (now - read).num_days() > MAX_AGE_DAYS
}

/// The stale entries of a store.
pub fn stale<'a>(entries: &'a [Entry], today: &str) -> Vec<&'a Entry> {
    entries.iter().filter(|e| is_stale(e, today)).collect()
}

/// What is asked of the store.
#[derive(Debug, Clone, Default)]
pub struct Query<'a> {
    pub service: &'a str,
    pub action: &'a str,
    /// `terraform`, `cloudformation`, `cdk`, or empty for a runbook.
    pub tool: &'a str,
    /// What is known of the resource (`major`, `scope`).
    pub facts: BTreeMap<String, String>,
}

impl Query<'_> {
    fn takes(&self, e: &Entry) -> bool {
        (e.service == self.service || e.service == "any")
            && (e.action == self.action || e.action == "any")
            && (e.tool == "any" || (!self.tool.is_empty() && e.tool == self.tool))
            && e.when
                .iter()
                .all(|(k, v)| self.facts.get(k).is_none_or(|have| have == v))
    }
}

/// The entries of one section for a query, in file order, from `entries`.
pub fn section_in<'a>(entries: &'a [Entry], q: &Query, section: &str) -> Vec<&'a Entry> {
    entries
        .iter()
        .filter(|e| e.section == section && q.takes(e))
        .collect()
}

/// The entries of one section for a query, from the shipped store.
pub fn section(q: &Query, section: &str) -> Vec<&'static Entry> {
    section_in(store(), q, section)
}

/// The tool key guidance uses for a location's tool: SAM and Serverless
/// Framework deploy through CloudFormation and share its entries; OpenTofu
/// shares Terraform's.
pub fn tool_key(tool: &str) -> &'static str {
    match tool {
        "terraform" | "opentofu" => "terraform",
        "cloudformation" | "sam" | "serverless" => "cloudformation",
        "cdk" => "cdk",
        _ => "",
    }
}

/// Fills an entry's placeholders (`{arn}`, `{region}`) from `vars`. A
/// placeholder with no value is left as written, so a gap is seen and not
/// silently emptied.
pub fn fill(template: &str, vars: &BTreeMap<&str, String>) -> String {
    let mut out = template.to_string();
    for (k, v) in vars {
        if !v.is_empty() {
            out = out.replace(&format!("{{{k}}}"), v);
        }
    }
    out
}

/// A source as a card or pull request shows it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cited {
    pub url: String,
    pub reviewed_on: String,
    /// Not re-read within [`MAX_AGE_DAYS`].
    #[serde(default)]
    pub stale: bool,
}

/// The distinct sources of some entries, in order.
pub fn cited(entries: &[&Entry], today: &str) -> Vec<Cited> {
    let mut out: Vec<Cited> = Vec::new();
    for e in entries {
        if let Some(c) = out.iter_mut().find(|c| c.url == e.source) {
            c.stale |= is_stale(e, today);
            if e.reviewed_on < c.reviewed_on {
                c.reviewed_on = e.reviewed_on.clone();
            }
            continue;
        }
        out.push(Cited {
            url: e.source.clone(),
            reviewed_on: e.reviewed_on.clone(),
            stale: is_stale(e, today),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_entry_has_a_source_and_a_review_date() {
        let mut ids = std::collections::BTreeSet::new();
        for e in store() {
            assert_eq!(problems(e), Vec::<String>::new(), "{}", e.id);
            assert!(ids.insert(e.id.clone()), "{} is listed twice", e.id);
        }
        assert!(store().len() > 100, "{}", store().len());
    }

    #[test]
    fn an_entry_without_a_source_or_a_date_is_refused() {
        let ok = store()[0].clone();
        let mut no_source = ok.clone();
        no_source.source = String::new();
        assert!(problems(&no_source).contains(&"no source link".to_string()));
        let mut not_a_link = ok.clone();
        not_a_link.source = "the RDS user guide".into();
        assert!(problems(&not_a_link).contains(&"no source link".to_string()));
        let mut no_date = ok.clone();
        no_date.reviewed_on = "last week".into();
        assert!(problems(&no_date)
            .iter()
            .any(|p| p.starts_with("no review date")));
        let mut odd = ok;
        odd.section = "musings".into();
        assert!(problems(&odd).iter().any(|p| p.contains("unknown section")));
        // A file with a missing field does not load at all.
        assert!(parse(r#"[{"id":"x","service":"rds"}]"#).is_err());
    }

    #[test]
    fn an_entry_not_re_read_in_time_is_flagged() {
        let e = &store()[0];
        assert!(!is_stale(e, &e.reviewed_on));
        let read = NaiveDate::parse_from_str(&e.reviewed_on, "%Y-%m-%d").unwrap();
        let edge = (read + chrono::Duration::days(MAX_AGE_DAYS))
            .format("%Y-%m-%d")
            .to_string();
        let late = (read + chrono::Duration::days(MAX_AGE_DAYS + 1))
            .format("%Y-%m-%d")
            .to_string();
        assert!(!is_stale(e, &edge));
        assert!(is_stale(e, &late));
        assert!(is_stale(e, "not a day"));
        assert_eq!(stale(store(), &e.reviewed_on).len(), 0);
        assert_eq!(stale(store(), "2099-01-01").len(), store().len());
        let c = cited(&[e], &late);
        assert!(c[0].stale && c[0].url == e.source);
    }

    fn q<'a>(service: &'a str, action: &'a str, tool: &'a str) -> Query<'a> {
        Query {
            service,
            action,
            tool,
            facts: BTreeMap::new(),
        }
    }

    /// RDS and Aurora are covered in full: every part of a runbook for
    /// each update a person applies, and every part of a pull request for
    /// each tool.
    #[test]
    fn rds_and_aurora_cover_every_part_of_a_runbook_and_a_pull_request() {
        let runbook = [
            "what",
            "in_code",
            "cve",
            "interruption",
            "permissions",
            "precheck",
            "backup",
            "console",
            "apply",
            "validation",
            "rollback",
            "if_nothing",
        ];
        for (service, action) in [
            ("rds", "os_update"),
            ("rds", "engine_upgrade"),
            ("aurora", "os_update"),
            ("aurora", "engine_patch"),
            ("aurora", "engine_upgrade"),
        ] {
            for s in runbook {
                assert!(
                    !section(&q(service, action, ""), s).is_empty(),
                    "{service} {action} has no `{s}`"
                );
            }
        }
        for service in ["rds", "aurora"] {
            for tool in ["terraform", "cloudformation", "cdk"] {
                for s in ["attribute", "takes_effect", "replacement", "plan"] {
                    assert!(
                        !section(&q(service, "engine_upgrade", tool), s).is_empty(),
                        "{service} engine_upgrade with {tool} has no `{s}`"
                    );
                }
            }
            assert!(!section(&q(service, "engine_upgrade", "terraform"), "trap").is_empty());
        }
    }

    #[test]
    fn the_other_services_have_what_a_person_or_a_pull_request_needs() {
        for (service, action) in [
            ("elasticache", "service_update"),
            ("opensearch", "service_software"),
        ] {
            for s in [
                "what",
                "in_code",
                "interruption",
                "apply",
                "validation",
                "rollback",
            ] {
                assert!(
                    !section(&q(service, action, ""), s).is_empty(),
                    "{service} {action} has no `{s}`"
                );
            }
        }
        for (service, action) in [
            ("elasticache", "engine_upgrade"),
            ("opensearch", "engine_upgrade"),
            ("eks_nodegroup", "nodegroup_release"),
            ("ami", "ami_replace"),
            ("lambda_runtime", "runtime_change"),
        ] {
            assert!(
                !section(&q(service, action, "terraform"), "attribute").is_empty(),
                "{service}"
            );
            assert!(
                !section(&q(service, action, ""), "rollback").is_empty(),
                "{service}"
            );
        }
        // The OpenSearch trap: a version change replaces the domain
        // unless the update policy is there.
        let r = section(
            &q("opensearch", "engine_upgrade", "cloudformation"),
            "replacement",
        );
        assert_eq!(r[0].requires, "EnableVersionUpgrade");
    }

    #[test]
    fn a_query_takes_its_tool_and_what_holds_for_any() {
        let tf = section(&q("aurora", "engine_upgrade", "terraform"), "attribute");
        assert_eq!(tf.len(), 1);
        assert!(tf[0]
            .text
            .contains("Never raise it on an `aws_rds_cluster_instance`"));
        // A runbook asks with no tool: tool entries stay out.
        assert!(section(&q("aurora", "engine_upgrade", ""), "attribute").is_empty());
        // `when` leaves an entry out only when a known fact contradicts it.
        let mut minor = q("rds", "engine_upgrade", "");
        let all = section(&minor, "precheck").len();
        minor.facts.insert("major".into(), "false".into());
        assert_eq!(section(&minor, "precheck").len(), all - 1);
        assert_eq!(tool_key("sam"), "cloudformation");
        assert_eq!(tool_key("opentofu"), "terraform");
        assert_eq!(tool_key("pulumi"), "");
    }

    #[test]
    fn placeholders_take_the_resources_own_identifiers() {
        let mut v = BTreeMap::new();
        v.insert(
            "arn",
            "arn:aws:rds:us-east-1:111122223333:db:orders-1".to_string(),
        );
        v.insert("region", "us-east-1".to_string());
        v.insert("action", "system-update".to_string());
        v.insert("target", String::new());
        let e = section(&q("rds", "os_update", ""), "apply")[0];
        let c = fill(&e.cli, &v);
        assert_eq!(c, "aws rds apply-pending-maintenance-action --resource-identifier arn:aws:rds:us-east-1:111122223333:db:orders-1 --apply-action system-update --opt-in-type next-maintenance --region us-east-1");
        // A value that is not known stays visible.
        assert_eq!(fill("to {target}", &v), "to {target}");
    }
}
