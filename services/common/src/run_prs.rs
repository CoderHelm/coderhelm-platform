//! The pull requests a run opened, each with its own repository.
//!
//! A run that spans several repositories opens one pull request in each. Its
//! record used to keep `repo` (the run's) next to `pr_number` and `branch`
//! from the first pull request, which could be in another repository, and
//! lists of numbers and branches with no repository at all: a lookup by
//! number matched a pull request of the same number in an unrelated
//! repository, and a link built from `repo` and `pr_number` pointed at the
//! wrong pull request. Each pull request is now kept whole ([`RunPr`], the
//! record's `prs`), `repo`/`pr_number`/`branch`/`pr_url` always come from
//! one of them ([`primary`]), and lookups use keys that name the repository
//! ([`pr_key`], [`branch_key`]).

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

/// The run record field with every pull request ([`RunPr`]).
pub const FIELD: &str = "prs";
/// The run record field with each pull request's [`pr_key`].
pub const KEYS_FIELD: &str = "pr_keys";
/// The run record field with each pull request's [`branch_key`].
pub const BRANCH_KEYS_FIELD: &str = "branch_keys";

/// One pull request a run opened.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct RunPr {
    /// The repository, `owner/name`.
    pub repo: String,
    pub pr_number: u64,
    pub branch: String,
    pub url: String,
    /// `merged` or `closed` once the pull request ended; empty while open.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub outcome: String,
}

/// The key that finds a run by one of its pull requests: `owner/name#123`.
pub fn pr_key(repo: &str, pr_number: u64) -> String {
    format!("{}#{pr_number}", repo.to_ascii_lowercase())
}

/// The key that finds a run by one of its branches: `owner/name:branch`.
pub fn branch_key(repo: &str, branch: &str) -> String {
    format!("{}:{branch}", repo.to_ascii_lowercase())
}

/// The pull request the record's `repo`, `pr_number`, `branch` and `pr_url`
/// describe: the one in the run's own repository when there is one, else the
/// first. `None` when there are none.
pub fn primary<'a>(prs: &'a [RunPr], run_repo: &str) -> Option<&'a RunPr> {
    prs.iter()
        .find(|p| p.repo.eq_ignore_ascii_case(run_repo))
        .or_else(|| prs.first())
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

impl RunPr {
    pub fn to_attr(&self) -> AttributeValue {
        let mut m = HashMap::from([
            ("repo".to_string(), s(&self.repo)),
            (
                "pr_number".to_string(),
                AttributeValue::N(self.pr_number.to_string()),
            ),
            ("branch".to_string(), s(&self.branch)),
            ("url".to_string(), s(&self.url)),
        ]);
        if !self.outcome.is_empty() {
            m.insert("outcome".to_string(), s(&self.outcome));
        }
        AttributeValue::M(m)
    }

    fn from_attr(v: &AttributeValue) -> Option<RunPr> {
        let m = v.as_m().ok()?;
        let st = |k: &str| {
            m.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        let pr_number = m
            .get("pr_number")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let repo = st("repo");
        (pr_number > 0 && !repo.is_empty()).then(|| RunPr {
            repo,
            pr_number,
            branch: st("branch"),
            url: st("url"),
            outcome: st("outcome"),
        })
    }
}

/// The values a run record write sets for its pull requests: `prs`,
/// `pr_keys`, `branch_keys`, and the lists older readers use.
pub fn attrs(prs: &[RunPr]) -> Vec<(&'static str, AttributeValue)> {
    let list =
        |f: &dyn Fn(&RunPr) -> AttributeValue| AttributeValue::L(prs.iter().map(f).collect());
    vec![
        (FIELD, list(&|p| p.to_attr())),
        (KEYS_FIELD, list(&|p| s(&pr_key(&p.repo, p.pr_number)))),
        (
            BRANCH_KEYS_FIELD,
            list(&|p| s(&branch_key(&p.repo, &p.branch))),
        ),
        ("pr_urls", list(&|p| s(&p.url))),
        (
            "pr_numbers",
            list(&|p| AttributeValue::N(p.pr_number.to_string())),
        ),
        ("branches", list(&|p| s(&p.branch))),
    ]
}

/// The same values as an update's SET clause (`prs = :rp0, ...`) and the
/// values it names.
pub fn set_clause(prs: &[RunPr]) -> (String, Vec<(String, AttributeValue)>) {
    let mut clause = Vec::new();
    let mut values = Vec::new();
    for (i, (field, value)) in attrs(prs).into_iter().enumerate() {
        clause.push(format!("{field} = :rp{i}"));
        values.push((format!(":rp{i}"), value));
    }
    (clause.join(", "), values)
}

/// Every pull request on a run record. Older records have no `prs`: their
/// pull requests are read from `pr_urls` (each URL names its repository),
/// paired with `branches` by position, else the one `pr_url`.
pub fn from_item(item: &HashMap<String, AttributeValue>) -> Vec<RunPr> {
    if let Some(list) = item.get(FIELD).and_then(|v| v.as_l().ok()) {
        let prs: Vec<RunPr> = list.iter().filter_map(RunPr::from_attr).collect();
        if !prs.is_empty() {
            return prs;
        }
    }
    let st = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let strings = |k: &str| -> Vec<String> {
        item.get(k)
            .and_then(|v| v.as_l().ok())
            .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
            .unwrap_or_default()
    };
    let urls = strings("pr_urls");
    let branches = strings("branches");
    let from_urls: Vec<RunPr> = urls
        .iter()
        .enumerate()
        .filter_map(|(i, url)| {
            let (owner, name, n) = crate::pr_outcome::parse_pr_url(url)?;
            Some(RunPr {
                repo: format!("{owner}/{name}"),
                pr_number: n,
                branch: branches.get(i).cloned().unwrap_or_default(),
                url: url.clone(),
                outcome: String::new(),
            })
        })
        .collect();
    if !from_urls.is_empty() {
        return from_urls;
    }
    let url = st("pr_url");
    match crate::pr_outcome::parse_pr_url(&url) {
        Some((owner, name, n)) => vec![RunPr {
            repo: format!("{owner}/{name}"),
            pr_number: n,
            branch: st("branch"),
            url,
            outcome: String::new(),
        }],
        None => Vec::new(),
    }
}

/// Where in the record's `prs` the pull request `repo#pr_number` is.
pub fn position(prs: &[RunPr], repo: &str, pr_number: u64) -> Option<usize> {
    prs.iter()
        .position(|p| p.pr_number == pr_number && p.repo.eq_ignore_ascii_case(repo))
}

/// Whether `repo#pr_number` is the pull request the record's own `repo` and
/// `pr_number` describe.
pub fn is_primary(item: &HashMap<String, AttributeValue>, repo: &str, pr_number: u64) -> bool {
    let rec_repo = item
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .unwrap_or("");
    let rec_pr = item
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    rec_pr == pr_number && rec_repo.eq_ignore_ascii_case(repo)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pr(repo: &str, n: u64, branch: &str) -> RunPr {
        RunPr {
            repo: repo.into(),
            pr_number: n,
            branch: branch.into(),
            url: format!("https://github.com/{repo}/pull/{n}"),
            outcome: String::new(),
        }
    }

    /// The record's repo and number always name the same pull request.
    #[test]
    fn the_primary_pull_request_is_in_the_runs_repo_when_there_is_one() {
        let prs = vec![pr("acme/api", 7, "a"), pr("acme/web", 12, "b")];
        assert_eq!(primary(&prs, "acme/web").unwrap().pr_number, 12);
        assert_eq!(primary(&prs, "ACME/Web").unwrap().repo, "acme/web");
        // The run's repo opened none: the first, whole.
        let p = primary(&prs, "acme/docs").unwrap();
        assert_eq!((p.repo.as_str(), p.pr_number), ("acme/api", 7));
        assert!(primary(&[], "acme/web").is_none());
    }

    /// The same number in two repositories is two different pull requests.
    #[test]
    fn keys_name_the_repository() {
        assert_ne!(pr_key("acme/api", 12), pr_key("acme/web", 12));
        assert_eq!(pr_key("Acme/Web", 12), "acme/web#12");
        assert_eq!(
            branch_key("acme/web", "coderhelm/x"),
            "acme/web:coderhelm/x"
        );
    }

    #[test]
    fn prs_round_trip_and_older_records_are_read_from_their_urls() {
        let prs = vec![pr("acme/api", 7, "a"), pr("acme/web", 12, "b")];
        let item: HashMap<String, AttributeValue> = attrs(&prs)
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        assert_eq!(from_item(&item), prs);
        let (clause, values) = set_clause(&prs);
        assert!(clause.starts_with("prs = :rp0, pr_keys = :rp1, branch_keys = :rp2"));
        assert_eq!(values.len(), 6);
        let keys = item[KEYS_FIELD].as_l().unwrap();
        assert_eq!(keys[1].as_s().unwrap(), "acme/web#12");

        // Before `prs`: the URLs carry each repository, branches by position.
        let mut old = item.clone();
        old.remove(FIELD);
        assert_eq!(from_item(&old), prs);

        // A single-PR run.
        let single: HashMap<String, AttributeValue> = HashMap::from([
            (
                "pr_url".to_string(),
                s("https://github.com/acme/web/pull/3"),
            ),
            ("branch".to_string(), s("c")),
        ]);
        assert_eq!(from_item(&single), vec![pr("acme/web", 3, "c")]);
        assert!(from_item(&HashMap::new()).is_empty());
    }

    #[test]
    fn a_pull_request_is_found_by_repo_and_number() {
        let prs = vec![pr("acme/api", 12, "a"), pr("acme/web", 12, "b")];
        assert_eq!(position(&prs, "acme/web", 12), Some(1));
        assert_eq!(position(&prs, "acme/docs", 12), None);
        let item = HashMap::from([
            ("repo".to_string(), s("acme/web")),
            ("pr_number".to_string(), AttributeValue::N("12".into())),
        ]);
        assert!(is_primary(&item, "acme/web", 12));
        assert!(!is_primary(&item, "acme/api", 12));
    }
}
