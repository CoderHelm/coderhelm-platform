//! Who asked for a run, as one stable identity per person.
//!
//! The gateway records a [`Requester`] on every ticket it sends (the worker
//! stores it on the run as `requester_key`, `requester_name` and
//! `requester_email`), and analytics groups runs with [`group`]: the same
//! Jira account, email, GitHub login, or a name a known person goes by, is
//! one row. Runs from before requesters were recorded only have the free-text
//! `sender`; [`Requester::legacy`] reads it the same way.
//!
//! Jira users the gateway has seen are kept on the settings table (`pk` =
//! team, `sk` = [`jira_user_sk`]) so an account id from an older run can be
//! shown with the person's name.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Key prefix for CoderHelm's own work (tests it fixes, alerts it handles).
pub const CODERHELM: &str = "coderhelm:";
/// CoderHelm's E2E work: fixing flaky tests and adding new ones.
pub const E2E: &str = "e2e";
/// A monitoring alert routed to a repo.
pub const ALERT: &str = "alert";
/// Scheduled work.
pub const SCHEDULED: &str = "scheduled";

/// Settings-table sort key prefix of a Jira user CoderHelm has seen.
pub const JIRA_USER_PREFIX: &str = "JIRAUSER#";

/// The settings-table sort key of a Jira user.
pub fn jira_user_sk(account_id: &str) -> String {
    format!("{JIRA_USER_PREFIX}{account_id}")
}

/// The person (or part of CoderHelm) a run was started for.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct Requester {
    /// `jira:<accountId>`, `github:<login>`, `email:<address>`, `name:<name>`
    /// when only a name was sent, or `coderhelm:<what>`. Lowercase.
    pub key: String,
    /// The name to show, when known.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub email: String,
}

/// A Jira Cloud account id: `<digits>:<uuid>`, or 24 hex characters.
pub fn is_jira_account_id(s: &str) -> bool {
    let hex = |p: &str| p.chars().all(|c| c.is_ascii_hexdigit() || c == '-');
    match s.split_once(':') {
        Some((site, id)) => {
            !site.is_empty() && site.chars().all(|c| c.is_ascii_digit()) && id.len() >= 8 && hex(id)
        }
        None => s.len() == 24 && s.chars().all(|c| c.is_ascii_hexdigit()),
    }
}

/// A name lowercased with its spaces collapsed, for comparing.
fn norm_name(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// What the old Forge app sent when it had no user.
fn is_placeholder(s: &str) -> bool {
    s.trim().eq_ignore_ascii_case("jira")
}

impl Requester {
    /// From a Jira user object (`accountId`, `displayName`, `emailAddress`),
    /// as Jira webhooks and the CoderHelm Jira app send it. Older app
    /// versions sent the account id as `displayName` when the name was hidden.
    pub fn jira_user(user: &serde_json::Value) -> Option<Requester> {
        let field = |k: &str| user.get(k).and_then(|v| v.as_str()).unwrap_or("").trim();
        let (account, shown, email) = (
            field("accountId"),
            field("displayName"),
            field("emailAddress"),
        );
        let shown = if shown.is_empty() {
            field("name")
        } else {
            shown
        };
        let shown = if is_placeholder(shown) { "" } else { shown };
        let (key, name) = if !account.is_empty() {
            (format!("jira:{}", account.to_lowercase()), shown)
        } else if is_jira_account_id(shown) {
            (format!("jira:{}", shown.to_lowercase()), "")
        } else if !email.is_empty() {
            (format!("email:{}", email.to_lowercase()), shown)
        } else if !shown.is_empty() {
            (format!("name:{}", norm_name(shown)), shown)
        } else {
            return None;
        };
        let name = if is_jira_account_id(name) { "" } else { name };
        Some(Requester {
            key,
            name: name.to_string(),
            email: email.to_string(),
        })
    }

    /// A GitHub user, by login.
    pub fn github(login: &str) -> Option<Requester> {
        let login = login.trim();
        (!login.is_empty()).then(|| Requester {
            key: format!("github:{}", login.to_lowercase()),
            ..Default::default()
        })
    }

    /// A signed-in dashboard user: their GitHub login when linked, and email.
    pub fn member(github_login: Option<&str>, email: &str) -> Option<Requester> {
        let email = email.trim();
        let mut r = match github_login.map(str::trim).filter(|l| !l.is_empty()) {
            Some(login) => Requester::github(login)?,
            None if !email.is_empty() => Requester {
                key: format!("email:{}", email.to_lowercase()),
                ..Default::default()
            },
            None => return None,
        };
        r.email = email.to_string();
        Some(r)
    }

    /// CoderHelm's own work, by what it was ([`E2E`], [`ALERT`], ...).
    pub fn coderhelm(what: &str) -> Requester {
        Requester {
            key: format!("{CODERHELM}{what}"),
            ..Default::default()
        }
    }

    /// From the free-text `sender` older runs recorded: a Jira display name
    /// or account id, a GitHub login, a dashboard user's email, or the name of
    /// a part of CoderHelm. `None` when nothing was recorded.
    pub fn legacy(sender: &str) -> Option<Requester> {
        let s = sender.trim();
        if s.is_empty() || is_placeholder(s) {
            return None;
        }
        if [E2E, ALERT, SCHEDULED]
            .iter()
            .any(|w| s.eq_ignore_ascii_case(w))
        {
            return Some(Requester::coderhelm(&s.to_lowercase()));
        }
        if is_jira_account_id(s) {
            return Some(Requester {
                key: format!("jira:{}", s.to_lowercase()),
                ..Default::default()
            });
        }
        if s.contains('@') && !s.contains(char::is_whitespace) {
            return Some(Requester {
                key: format!("email:{}", s.to_lowercase()),
                email: s.to_string(),
                ..Default::default()
            });
        }
        if s.contains(char::is_whitespace) {
            return Some(Requester {
                key: format!("name:{}", norm_name(s)),
                name: s.to_string(),
                ..Default::default()
            });
        }
        Requester::github(s)
    }

    /// From a run record's fields, falling back to its `sender`.
    pub fn from_run(key: &str, name: &str, email: &str, sender: &str) -> Option<Requester> {
        if key.is_empty() {
            return Requester::legacy(sender);
        }
        Some(Requester {
            key: key.to_string(),
            name: name.to_string(),
            email: email.to_string(),
        })
    }

    /// The Jira account id, for a Jira user.
    pub fn jira_id(&self) -> Option<&str> {
        self.key.strip_prefix("jira:")
    }

    pub fn is_coderhelm(&self) -> bool {
        self.key.starts_with(CODERHELM)
    }
}

/// How CoderHelm's own work is labelled.
fn coderhelm_label(what: &str) -> String {
    match what {
        E2E => "CoderHelm (E2E tests)".to_string(),
        ALERT => "CoderHelm (alerts)".to_string(),
        SCHEDULED => "CoderHelm (scheduled)".to_string(),
        other => format!("CoderHelm ({other})"),
    }
}

/// Someone the team already knows: a team member, or a Jira user CoderHelm
/// has seen on a webhook. Empty fields are unknown.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Known {
    pub name: String,
    pub email: String,
    pub github_login: String,
    pub jira_id: String,
    /// On the team: their name wins over any other.
    pub member: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Person,
    Coderhelm,
    /// The run recorded nobody (runs from before requesters were saved).
    NotRecorded,
}

/// One row of "who asks for work".
#[derive(Debug, Clone, PartialEq)]
pub struct Group {
    /// Empty for [`Kind::NotRecorded`].
    pub label: String,
    pub kind: Kind,
}

/// Union-find over identity tokens (`jira:`, `email:`, `github:`, `name:`).
#[derive(Default)]
struct Links {
    ids: HashMap<String, usize>,
    parent: Vec<usize>,
}

impl Links {
    fn id(&mut self, token: &str) -> usize {
        if let Some(&i) = self.ids.get(token) {
            return i;
        }
        let i = self.parent.len();
        self.parent.push(i);
        self.ids.insert(token.to_string(), i);
        i
    }
    fn root(&mut self, mut i: usize) -> usize {
        while self.parent[i] != i {
            self.parent[i] = self.parent[self.parent[i]];
            i = self.parent[i];
        }
        i
    }
    /// Links all `tokens` together and returns their root.
    fn join(&mut self, tokens: &[String]) -> Option<usize> {
        let first = self.id(tokens.first()?);
        for t in &tokens[1..] {
            let (a, b) = (self.id(t), self.root(first));
            let a = self.root(a);
            if a != b {
                self.parent[a] = b;
            }
        }
        Some(self.root(first))
    }
}

fn strong_tokens(k: &Known) -> Vec<String> {
    let mut t = Vec::new();
    if !k.jira_id.is_empty() {
        t.push(format!("jira:{}", k.jira_id.to_lowercase()));
    }
    if !k.email.is_empty() {
        t.push(format!("email:{}", k.email.trim().to_lowercase()));
    }
    if !k.github_login.is_empty() {
        t.push(format!("github:{}", k.github_login.trim().to_lowercase()));
    }
    t
}

/// Everything seen about one group, to pick its label from.
#[derive(Default)]
struct Seen {
    member_name: Option<String>,
    known_name: Option<String>,
    names: Vec<String>,
    email: Option<String>,
    login: Option<String>,
    jira_id: Option<String>,
}

/// Groups one requester per run (`None` when the run recorded nobody) into
/// one group per person, using `known` people to merge an email, a GitHub
/// login, a Jira account and a name that belong to the same person. Returns
/// the groups and, for each input, the index of its group.
///
/// A name only merges when one known person goes by it: two people with the
/// same name stay apart.
pub fn group(requesters: &[Option<Requester>], known: &[Known]) -> (Vec<Group>, Vec<usize>) {
    let mut links = Links::default();
    for k in known {
        links.join(&strong_tokens(k));
    }
    // Names that more than one known person goes by never link. Two records
    // are different people when they hold different ids of the same kind (two
    // emails, two Jira accounts); a member and a Jira user whose email Jira
    // hides are not.
    let mut ids: HashMap<usize, Vec<String>> = HashMap::new();
    let mut by_name: HashMap<String, Vec<usize>> = HashMap::new();
    for k in known {
        let t = strong_tokens(k);
        let Some(root) = links.join(&t) else { continue };
        ids.entry(root).or_default().extend(t);
        if !k.name.trim().is_empty() {
            by_name.entry(norm_name(&k.name)).or_default().push(root);
        }
    }
    let differ = |a: usize, b: usize| {
        let (a, b) = (&ids[&a], &ids[&b]);
        a.iter().any(|x| {
            let kind = x.split(':').next();
            b.iter().any(|y| y.split(':').next() == kind && y != x) && !b.contains(x)
        })
    };
    let ambiguous: std::collections::HashSet<String> = by_name
        .iter()
        .filter(|(_, roots)| {
            roots
                .iter()
                .enumerate()
                .any(|(i, &a)| roots[i + 1..].iter().any(|&b| a != b && differ(a, b)))
        })
        .map(|(n, _)| n.clone())
        .collect();
    let name_token = |n: &str| -> Option<String> {
        let n = norm_name(n);
        (!n.is_empty() && !ambiguous.contains(&n)).then(|| format!("name:{n}"))
    };
    for k in known {
        let mut t = strong_tokens(k);
        t.extend(name_token(&k.name));
        links.join(&t);
    }

    let tokens_of = |r: &Requester| -> Vec<String> {
        if r.is_coderhelm() {
            return vec![r.key.clone()];
        }
        let mut t = Vec::new();
        match r.key.strip_prefix("name:") {
            // An ambiguous name groups only with the same name.
            Some(n) => t.push(name_token(n).unwrap_or_else(|| format!("name-only:{n}"))),
            None => t.push(r.key.clone()),
        }
        if !r.email.is_empty() {
            t.push(format!("email:{}", r.email.trim().to_lowercase()));
        }
        if !r.name.is_empty() && !r.key.starts_with("name:") {
            t.extend(name_token(&r.name));
        }
        t
    };
    let tokens: Vec<Vec<String>> = requesters
        .iter()
        .map(|r| r.as_ref().map(tokens_of).unwrap_or_default())
        .collect();
    for t in &tokens {
        links.join(t);
    }
    // Roots only after every link: a later run can join two earlier groups.
    let roots: Vec<Option<usize>> = tokens.iter().map(|t| links.join(t)).collect();

    let mut seen: HashMap<usize, Seen> = HashMap::new();
    for k in known {
        let Some(root) = links.join(&strong_tokens(k)).or_else(|| {
            name_token(&k.name).map(|t| {
                let i = links.id(&t);
                links.root(i)
            })
        }) else {
            continue;
        };
        let s = seen.entry(root).or_default();
        let name = k.name.trim();
        if !name.is_empty() && !is_jira_account_id(name) {
            let slot = if k.member {
                &mut s.member_name
            } else {
                &mut s.known_name
            };
            slot.get_or_insert_with(|| name.to_string());
        }
        if !k.email.is_empty() {
            s.email.get_or_insert_with(|| k.email.trim().to_string());
        }
        if !k.github_login.is_empty() {
            s.login
                .get_or_insert_with(|| k.github_login.trim().to_string());
        }
    }
    for (r, root) in requesters.iter().zip(&roots) {
        let (Some(r), Some(root)) = (r, root) else {
            continue;
        };
        let s = seen.entry(*root).or_default();
        if !r.name.trim().is_empty() {
            s.names.push(r.name.trim().to_string());
        }
        if !r.email.is_empty() {
            s.email.get_or_insert_with(|| r.email.trim().to_string());
        }
        if let Some(login) = r.key.strip_prefix("github:") {
            s.login.get_or_insert_with(|| login.to_string());
        }
        if let Some(id) = r.jira_id() {
            s.jira_id.get_or_insert_with(|| id.to_string());
        }
        if let Some(email) = r.key.strip_prefix("email:") {
            s.email.get_or_insert_with(|| email.to_string());
        }
    }

    let label = |r: &Requester, s: &Seen| -> String {
        if let Some(what) = r.key.strip_prefix(CODERHELM) {
            return coderhelm_label(what);
        }
        let mut counts: Vec<(&String, usize)> = Vec::new();
        for n in &s.names {
            match counts
                .iter_mut()
                .find(|(m, _)| norm_name(m) == norm_name(n))
            {
                Some(c) => c.1 += 1,
                None => counts.push((n, 1)),
            }
        }
        let common_name = counts
            .iter()
            .max_by_key(|(_, c)| *c)
            .map(|(n, _)| (*n).clone());
        s.member_name
            .clone()
            .or_else(|| s.known_name.clone())
            .or(common_name)
            .or_else(|| s.email.clone())
            .or_else(|| s.login.clone())
            .or_else(|| {
                s.jira_id.as_ref().map(|id| {
                    let tail: String = id
                        .chars()
                        .skip(id.chars().count().saturating_sub(4))
                        .collect();
                    format!("Jira user …{tail}")
                })
            })
            .unwrap_or_else(|| r.key.clone())
    };

    let mut groups: Vec<Group> = Vec::new();
    let mut index: HashMap<Option<usize>, usize> = HashMap::new();
    let mut out = Vec::with_capacity(requesters.len());
    for (r, root) in requesters.iter().zip(&roots) {
        let at = *index.entry(*root).or_insert_with(|| {
            groups.push(match (r, root) {
                (Some(r), Some(root)) => Group {
                    label: label(r, &seen[root]),
                    kind: if r.is_coderhelm() {
                        Kind::Coderhelm
                    } else {
                        Kind::Person
                    },
                },
                _ => Group {
                    label: String::new(),
                    kind: Kind::NotRecorded,
                },
            });
            groups.len() - 1
        });
        out.push(at);
    }
    (groups, out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ADA_ID: &str = "557058:0a1b2c3d-4e5f-6789-abcd-ef0123456789";

    fn ada_member() -> Known {
        Known {
            name: "Ada Lovelace".into(),
            email: "ada@example.com".into(),
            member: true,
            ..Default::default()
        }
    }

    fn labels(requesters: &[Option<Requester>], known: &[Known]) -> Vec<(String, usize)> {
        let (groups, at) = group(requesters, known);
        let mut counts = vec![0usize; groups.len()];
        for i in at {
            counts[i] += 1;
        }
        groups.into_iter().map(|g| g.label).zip(counts).collect()
    }

    #[test]
    fn jira_account_ids_are_told_apart_from_names() {
        assert!(is_jira_account_id(ADA_ID));
        assert!(is_jira_account_id("5b10ac8d82e05b22cc7d4ef5"));
        assert!(!is_jira_account_id("Ada Lovelace"));
        assert!(!is_jira_account_id("ada-l"));
        assert!(!is_jira_account_id("12:ab"));
        assert!(!is_jira_account_id("ada:0a1b2c3d4e5f"));
    }

    #[test]
    fn jira_users_key_on_the_account_id() {
        let r = Requester::jira_user(&json!({
            "accountId": ADA_ID, "displayName": "Ada Lovelace", "emailAddress": "ada@example.com"
        }))
        .unwrap();
        assert_eq!(r.key, format!("jira:{ADA_ID}"));
        assert_eq!(r.name, "Ada Lovelace");
        assert_eq!(r.email, "ada@example.com");
        // Older app versions sent the id as the name when it was hidden.
        let r = Requester::jira_user(&json!({ "displayName": ADA_ID })).unwrap();
        assert_eq!(
            (r.key.as_str(), r.name.as_str()),
            (format!("jira:{ADA_ID}").as_str(), "")
        );
        assert_eq!(
            Requester::jira_user(&json!({ "displayName": "jira" })),
            None
        );
        assert_eq!(Requester::jira_user(&json!({})), None);
    }

    #[test]
    fn legacy_senders_are_read_by_shape() {
        let key = |s: &str| Requester::legacy(s).map(|r| r.key);
        assert_eq!(key(""), None);
        assert_eq!(key("jira"), None);
        assert_eq!(key("e2e").as_deref(), Some("coderhelm:e2e"));
        assert_eq!(key("Alert").as_deref(), Some("coderhelm:alert"));
        assert_eq!(key(ADA_ID), Some(format!("jira:{ADA_ID}")));
        assert_eq!(
            key("Ada@Example.com").as_deref(),
            Some("email:ada@example.com")
        );
        assert_eq!(key("Ada  Lovelace").as_deref(), Some("name:ada lovelace"));
        assert_eq!(key("ada-l").as_deref(), Some("github:ada-l"));
    }

    #[test]
    fn a_dashboard_user_keys_on_their_login_and_keeps_the_email() {
        let r = Requester::member(Some("Ada-L"), "ada@example.com").unwrap();
        assert_eq!(
            (r.key.as_str(), r.email.as_str()),
            ("github:ada-l", "ada@example.com")
        );
        let r = Requester::member(None, "ada@example.com").unwrap();
        assert_eq!(r.key, "email:ada@example.com");
        assert_eq!(Requester::member(Some(" "), ""), None);
    }

    #[test]
    fn email_name_login_and_jira_id_of_one_member_are_one_row() {
        let mut ada = ada_member();
        ada.github_login = "ada-l".into();
        let jira_ada = Known {
            name: "Ada Lovelace".into(),
            jira_id: ADA_ID.into(),
            ..Default::default()
        };
        let runs = vec![
            Requester::legacy("ada@example.com"),
            Requester::legacy("Ada Lovelace"),
            Requester::legacy("ada-l"),
            Requester::legacy(ADA_ID),
            Requester::jira_user(&json!({"accountId": ADA_ID, "displayName": "Ada L."})),
        ];
        assert_eq!(
            labels(&runs, &[ada, jira_ada]),
            vec![("Ada Lovelace".to_string(), 5)]
        );
    }

    #[test]
    fn a_jira_id_resolves_through_seen_jira_users() {
        let seen = Known {
            name: "Grace Hopper".into(),
            jira_id: ADA_ID.into(),
            ..Default::default()
        };
        assert_eq!(
            labels(&[Requester::legacy(ADA_ID)], &[seen]),
            vec![("Grace Hopper".to_string(), 1)]
        );
        // Not seen yet: a short, plain label instead of the raw id.
        assert_eq!(
            labels(&[Requester::legacy(ADA_ID)], &[]),
            vec![("Jira user …6789".to_string(), 1)]
        );
    }

    #[test]
    fn a_recorded_name_merges_runs_by_the_same_account() {
        let named =
            Requester::jira_user(&json!({"accountId": ADA_ID, "displayName": "Ada Lovelace"}));
        let bare = Requester::legacy(ADA_ID);
        let by_name = Requester::legacy("Ada Lovelace");
        assert_eq!(
            labels(&[bare, named, by_name], &[]),
            vec![("Ada Lovelace".to_string(), 3)]
        );
    }

    #[test]
    fn coderhelm_work_is_grouped_and_labelled_and_never_merged_with_people() {
        let runs = vec![
            Requester::legacy("e2e"),
            Some(Requester::coderhelm(E2E)),
            Some(Requester::coderhelm(ALERT)),
            Requester::legacy("ada@example.com"),
        ];
        let (groups, at) = group(&runs, &[ada_member()]);
        assert_eq!(at, vec![0, 0, 1, 2]);
        assert_eq!(groups[0].label, "CoderHelm (E2E tests)");
        assert_eq!(groups[0].kind, Kind::Coderhelm);
        assert_eq!(groups[1].label, "CoderHelm (alerts)");
        assert_eq!(groups[2].label, "Ada Lovelace");
        assert_eq!(groups[2].kind, Kind::Person);
    }

    #[test]
    fn runs_with_nobody_recorded_are_one_group() {
        let (groups, at) = group(&[None, Requester::legacy(""), None], &[]);
        assert_eq!(at, vec![0, 0, 0]);
        assert_eq!(groups[0].kind, Kind::NotRecorded);
        assert_eq!(groups[0].label, "");
    }

    #[test]
    fn two_people_with_one_name_stay_apart() {
        let a = Known {
            name: "Sam Lee".into(),
            email: "sam.a@example.com".into(),
            member: true,
            ..Default::default()
        };
        let b = Known {
            name: "Sam Lee".into(),
            email: "sam.b@example.com".into(),
            member: true,
            ..Default::default()
        };
        let runs = vec![
            Requester::legacy("sam.a@example.com"),
            Requester::legacy("sam.b@example.com"),
            Requester::legacy("Sam Lee"),
        ];
        let (groups, at) = group(&runs, &[a, b]);
        assert_eq!(groups.len(), 3);
        assert_eq!(at, vec![0, 1, 2]);
    }

    #[test]
    fn requesters_survive_a_queue_round_trip() {
        let r = Requester::jira_user(&json!({"accountId": ADA_ID, "displayName": "Ada Lovelace"}))
            .unwrap();
        let back: Requester = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back, r);
        assert_eq!(
            Requester::from_run(&r.key, &r.name, "", "ignored"),
            Some(r.clone())
        );
        assert_eq!(
            Requester::from_run("", "", "", "ada@example.com").map(|r| r.key),
            Some("email:ada@example.com".to_string())
        );
    }
}
