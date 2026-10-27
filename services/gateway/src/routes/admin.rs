//! The platform admin page: every team grouped under the organisation it
//! belongs to, and the beta sign-ups.
//!
//! Why grouping: one company shows up as many teams. Before Oct 18 every
//! person who signed in with GitHub got a fresh team that also claimed the
//! org's installation, and every email sign-up still gets a personal team.
//! Listed flat, one customer reads as a dozen. Like Stripe's customers list
//! or PostHog's organisations, the admin page shows one row per organisation
//! with its teams under it, and hides the ones with no activity behind a
//! filter that says how many there are.
//!
//! Teams are joined by the strongest link they share, in this order:
//! 1. the same GitHub installation or GitHub org,
//! 2. the same Jira site,
//! 3. a member they share (the same user id or email),
//! 4. the same company email domain (never a public one like gmail.com).
//!
//! A team with its own installation or Jira site is never folded into another
//! organisation by a weaker link: it is a different account on GitHub or Jira.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{extract::State, http::StatusCode, Extension, Json};
use serde::Serialize;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use super::api::is_platform_admin;
use super::auth::normalize_email;
use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;

/// Most teams the overview reads. Far above today's count; a cap so one page
/// load can never fan out without bound.
const MAX_TEAMS: usize = 2000;

/// Free mail providers: sharing one of these says nothing about a company.
const PUBLIC_DOMAINS: &[&str] = &[
    "gmail.com",
    "googlemail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "msn.com",
    "yahoo.com",
    "ymail.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
    "pm.me",
    "gmx.com",
    "gmx.net",
    "zoho.com",
    "fastmail.com",
    "hey.com",
    "yandex.com",
    "mail.com",
    "qq.com",
    "163.com",
    "users.noreply.github.com",
];

/// A company domain from an email, or `None` for a public mail provider.
pub(crate) fn company_domain(email: &str) -> Option<String> {
    let domain = email.trim().rsplit_once('@')?.1.to_ascii_lowercase();
    if domain.is_empty() || !domain.contains('.') {
        return None;
    }
    let public = PUBLIC_DOMAINS
        .iter()
        .any(|p| domain == *p || domain.ends_with(&format!(".{p}")));
    (!public).then_some(domain)
}

/// One person on a team, as the overview needs them.
#[derive(Clone, Debug, Default)]
pub struct MemberFact {
    pub user_id: String,
    /// Normalised (lower case, no plus alias); empty for GitHub-only members.
    pub email: String,
    /// Later of last sign-in and last use, RFC 3339; empty when unknown.
    pub last_active_at: String,
}

/// Everything the overview knows about one team.
#[derive(Clone, Debug, Default)]
pub struct TeamFacts {
    pub team_id: String,
    pub name: String,
    pub status: String,
    pub plan: String,
    pub created_at: String,
    pub installation_id: String,
    pub github_org: String,
    pub jira_site: String,
    pub members: Vec<MemberFact>,
    pub invites: u64,
    pub repos: Vec<String>,
    pub runs_total: u64,
    pub runs_month: u64,
    pub reviews_total: u64,
    pub reviews_month: u64,
    pub cost_month_usd: f64,
    pub tokens_month: u64,
}

impl TeamFacts {
    fn installation_key(&self) -> Option<String> {
        let id = self.installation_id.trim();
        (!id.is_empty() && id != "0").then(|| format!("inst:{id}"))
    }

    fn org_key(&self) -> Option<String> {
        let org = self.github_org.trim().to_ascii_lowercase();
        (!org.is_empty() && org != "unknown" && self.installation_key().is_some())
            .then(|| format!("org:{org}"))
    }

    fn jira_key(&self) -> Option<String> {
        site_host(&self.jira_site).map(|h| format!("jira:{h}"))
    }

    /// Keys that make two teams one organisation outright.
    fn strong_keys(&self) -> Vec<String> {
        [self.installation_key(), self.org_key(), self.jira_key()]
            .into_iter()
            .flatten()
            .collect()
    }

    /// How the people on the team are known: user ids and emails.
    fn person_keys(&self) -> Vec<String> {
        let mut out = Vec::new();
        for m in &self.members {
            if !m.user_id.is_empty() {
                out.push(format!("id:{}", m.user_id));
            }
            if !m.email.is_empty() {
                out.push(format!("email:{}", m.email));
            }
        }
        out
    }

    fn domains(&self) -> BTreeMap<String, usize> {
        let mut out = BTreeMap::new();
        for m in &self.members {
            if let Some(d) = company_domain(&m.email) {
                *out.entry(d).or_insert(0) += 1;
            }
        }
        out
    }

    fn last_active_at(&self) -> String {
        self.members
            .iter()
            .map(|m| m.last_active_at.as_str())
            .max()
            .unwrap_or("")
            .to_string()
    }

    /// How much real use the team has had, to pick an organisation's main team.
    fn weight(&self) -> (u64, usize, std::cmp::Reverse<String>) {
        (
            self.runs_total + self.reviews_total,
            self.members.len(),
            std::cmp::Reverse(self.created_at.clone()),
        )
    }
}

/// `https://acme.atlassian.net/` → `acme.atlassian.net`.
fn site_host(url: &str) -> Option<String> {
    let rest = url.trim();
    let rest = rest
        .strip_prefix("https://")
        .or_else(|| rest.strip_prefix("http://"))
        .unwrap_or(rest);
    let host = rest.split(['/', '?', '#']).next()?.to_ascii_lowercase();
    (!host.is_empty()).then_some(host)
}

/// What ties a team to its organisation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Link {
    /// The organisation's main team, or a team on its own.
    Main,
    Github,
    Jira,
    Member,
    Domain,
}

/// One organisation: the teams that belong together, main team first.
#[derive(Clone, Debug, PartialEq)]
pub struct OrgGroup {
    pub teams: Vec<(usize, Link)>,
}

/// Union-find over team indices.
struct Sets(Vec<usize>);

impl Sets {
    fn new(n: usize) -> Self {
        Sets((0..n).collect())
    }
    fn find(&mut self, i: usize) -> usize {
        let mut r = i;
        while self.0[r] != r {
            r = self.0[r];
        }
        let mut i = i;
        while self.0[i] != r {
            let next = self.0[i];
            self.0[i] = r;
            i = next;
        }
        r
    }
    fn union(&mut self, a: usize, b: usize) {
        let (a, b) = (self.find(a), self.find(b));
        if a != b {
            self.0[b.max(a)] = a.min(b);
        }
    }
}

/// Join every team in `keys_of` that shares a key.
fn union_by_keys(sets: &mut Sets, members: &[usize], keys_of: impl Fn(usize) -> Vec<String>) {
    let mut first: HashMap<String, usize> = HashMap::new();
    for &i in members {
        for k in keys_of(i) {
            match first.get(&k) {
                Some(&j) => sets.union(i, j),
                None => {
                    first.insert(k, i);
                }
            }
        }
    }
}

/// Group teams into organisations (see the module notes for the order).
pub fn group_teams(teams: &[TeamFacts]) -> Vec<OrgGroup> {
    let n = teams.len();
    let mut sets = Sets::new(n);
    let strong: Vec<usize> = (0..n)
        .filter(|&i| !teams[i].strong_keys().is_empty())
        .collect();
    let loose: Vec<usize> = (0..n)
        .filter(|&i| teams[i].strong_keys().is_empty())
        .collect();
    union_by_keys(&mut sets, &strong, |i| teams[i].strong_keys());

    // What each strong organisation is known by, to attach loose teams.
    let mut root_people: HashMap<String, HashSet<usize>> = HashMap::new();
    let mut root_domains: HashMap<String, HashMap<usize, usize>> = HashMap::new();
    for &i in &strong {
        let root = sets.find(i);
        for k in teams[i].person_keys() {
            root_people.entry(k).or_default().insert(root);
        }
        for (d, count) in teams[i].domains() {
            *root_domains.entry(d).or_default().entry(root).or_insert(0) += count;
        }
    }
    // A strong organisation's size, to break ties toward the busier one.
    let mut root_weight: HashMap<usize, u64> = HashMap::new();
    for &i in &strong {
        let root = sets.find(i);
        *root_weight.entry(root).or_insert(0) += teams[i].runs_total + teams[i].reviews_total;
    }

    let mut link: Vec<Link> = vec![Link::Main; n];
    for &i in &strong {
        let t = &teams[i];
        link[i] = if t.installation_key().is_some() {
            Link::Github
        } else {
            Link::Jira
        };
    }

    // Loose teams: a shared member first, then a company domain.
    let mut unattached = Vec::new();
    for &i in &loose {
        let by_member: HashSet<usize> = teams[i]
            .person_keys()
            .iter()
            .filter_map(|k| root_people.get(k))
            .flatten()
            .copied()
            .collect();
        let pick = |cands: Vec<(usize, usize)>| {
            cands
                .into_iter()
                .max_by_key(|&(root, score)| {
                    (
                        score,
                        root_weight.get(&root).copied().unwrap_or(0),
                        std::cmp::Reverse(root),
                    )
                })
                .map(|(root, _)| root)
        };
        if let Some(root) = pick(by_member.into_iter().map(|r| (r, 0)).collect()) {
            sets.union(root, i);
            link[i] = Link::Member;
            continue;
        }
        let mut by_domain: HashMap<usize, usize> = HashMap::new();
        for d in teams[i].domains().keys() {
            for (&root, &count) in root_domains.get(d).into_iter().flatten() {
                *by_domain.entry(root).or_insert(0) += count;
            }
        }
        if let Some(root) = pick(by_domain.into_iter().collect()) {
            sets.union(root, i);
            link[i] = Link::Domain;
            continue;
        }
        unattached.push(i);
    }

    // Loose teams with nothing strong to join: join each other, by a shared
    // member and then by a company domain.
    union_by_keys(&mut sets, &unattached, |i| teams[i].person_keys());
    let by_member: Vec<usize> = unattached.iter().map(|&i| sets.find(i)).collect();
    union_by_keys(&mut sets, &unattached, |i| {
        teams[i]
            .domains()
            .into_keys()
            .map(|d| format!("domain:{d}"))
            .collect()
    });
    let mut size: HashMap<usize, usize> = HashMap::new();
    for &i in &unattached {
        *size.entry(sets.find(i)).or_insert(0) += 1;
    }
    for (k, &i) in unattached.iter().enumerate() {
        if size[&sets.find(i)] > 1 {
            let shares_member = by_member
                .iter()
                .enumerate()
                .any(|(j, &root)| j != k && root == by_member[k]);
            link[i] = if shares_member {
                Link::Member
            } else {
                Link::Domain
            };
        }
    }

    // Collect, main team first; organisations in first-seen order.
    let mut groups: Vec<(usize, Vec<usize>)> = Vec::new();
    let mut at: HashMap<usize, usize> = HashMap::new();
    for i in 0..n {
        let root = sets.find(i);
        let slot = *at.entry(root).or_insert_with(|| {
            groups.push((root, Vec::new()));
            groups.len() - 1
        });
        groups[slot].1.push(i);
    }
    groups
        .into_iter()
        .map(|(_, mut idx)| {
            idx.sort_by(|&a, &b| {
                teams[b]
                    .weight()
                    .cmp(&teams[a].weight())
                    .then(teams[a].team_id.cmp(&teams[b].team_id))
            });
            let main = idx[0];
            OrgGroup {
                teams: idx
                    .into_iter()
                    .map(|i| (i, if i == main { Link::Main } else { link[i] }))
                    .collect(),
            }
        })
        .collect()
}

/// A team of one whose only member also belongs to another team: their own
/// workspace, not an organisation.
pub fn is_personal(teams: &[TeamFacts], i: usize) -> bool {
    let t = &teams[i];
    if t.members.len() != 1 {
        return false;
    }
    let mine: HashSet<String> = t.person_keys().into_iter().collect();
    teams
        .iter()
        .enumerate()
        .any(|(j, o)| j != i && o.person_keys().iter().any(|k| mine.contains(k)))
}

/// A team counts as active when it has run or reviewed anything, or (for a
/// team that is not someone's personal workspace) when it has repos or a
/// GitHub installation. A personal workspace that only carries copies of the
/// org's repos has done nothing yet.
pub fn is_active(teams: &[TeamFacts], i: usize) -> bool {
    let t = &teams[i];
    if t.runs_total > 0 || t.reviews_total > 0 {
        return true;
    }
    !is_personal(teams, i) && (!t.repos.is_empty() || t.installation_key().is_some())
}

fn team_json(teams: &[TeamFacts], i: usize, link: Link) -> Value {
    let t = &teams[i];
    json!({
        "team_id": t.team_id,
        "name": t.name,
        "status": if t.status.is_empty() { "active" } else { t.status.as_str() },
        "plan": t.plan,
        "created_at": t.created_at,
        "members": t.members.len(),
        "invites": t.invites,
        "repos": t.repos.len(),
        "runs_total": t.runs_total,
        "runs_month": t.runs_month,
        "reviews_total": t.reviews_total,
        "reviews_month": t.reviews_month,
        "cost_month_usd": (t.cost_month_usd * 100.0).round() / 100.0,
        "tokens_month": t.tokens_month,
        "last_active_at": t.last_active_at(),
        "github_org": t.github_org,
        "has_installation": t.installation_key().is_some(),
        "jira_site": site_host(&t.jira_site).unwrap_or_default(),
        "personal": is_personal(teams, i),
        "active": is_active(teams, i),
        "link": link,
    })
}

/// The organisation rows the admin page shows, busiest first.
pub fn orgs_json(teams: &[TeamFacts]) -> Vec<Value> {
    let mut orgs: Vec<Value> = group_teams(teams)
        .into_iter()
        .map(|g| {
            let main = &teams[g.teams[0].0];
            let idx: Vec<usize> = g.teams.iter().map(|&(i, _)| i).collect();
            let rows: Vec<Value> = g
                .teams
                .iter()
                .map(|&(i, l)| team_json(teams, i, l))
                .collect();
            let mut people: HashSet<String> = HashSet::new();
            let mut repos: HashSet<&str> = HashSet::new();
            for &i in &idx {
                for m in &teams[i].members {
                    people.insert(if m.email.is_empty() {
                        format!("id:{}", m.user_id)
                    } else {
                        format!("email:{}", m.email)
                    });
                }
                repos.extend(teams[i].repos.iter().map(|r| r.as_str()));
            }
            let sum = |f: fn(&TeamFacts) -> u64| idx.iter().map(|&i| f(&teams[i])).sum::<u64>();
            let cost: f64 = idx.iter().map(|&i| teams[i].cost_month_usd).sum();
            let last = idx
                .iter()
                .map(|&i| teams[i].last_active_at())
                .max()
                .unwrap_or_default();
            let created = idx
                .iter()
                .map(|&i| teams[i].created_at.as_str())
                .filter(|c| !c.is_empty())
                .min()
                .unwrap_or("")
                .to_string();
            let domain = main
                .domains()
                .into_iter()
                .max_by_key(|&(_, c)| c)
                .map(|(d, _)| d);
            let (label, via) =
                if !main.github_org.trim().is_empty() && main.installation_key().is_some() {
                    (main.github_org.clone(), "github")
                } else if let Some(h) = site_host(&main.jira_site) {
                    (h, "jira")
                } else if !main.name.trim().is_empty() {
                    (main.name.clone(), "name")
                } else if let Some(d) = domain.clone() {
                    (d, "domain")
                } else {
                    (String::new(), "none")
                };
            let active = idx.iter().any(|&i| is_active(teams, i));
            json!({
                "key": main.team_id,
                "label": label,
                "label_from": via,
                "domain": domain.unwrap_or_default(),
                "active": active,
                "team_count": idx.len(),
                "personal_count": idx.iter().filter(|&&i| is_personal(teams, i)).count(),
                "members": people.len(),
                "repos": repos.len(),
                "runs_total": sum(|t| t.runs_total),
                "runs_month": sum(|t| t.runs_month),
                "reviews_total": sum(|t| t.reviews_total),
                "reviews_month": sum(|t| t.reviews_month),
                "cost_month_usd": (cost * 100.0).round() / 100.0,
                "last_active_at": last,
                "created_at": created,
                "plan": main.plan,
                "teams": rows,
            })
        })
        .collect();
    orgs.sort_by(|a, b| {
        let k = |v: &Value| {
            (
                v["active"].as_bool().unwrap_or(false),
                v["runs_month"].as_u64().unwrap_or(0) + v["reviews_month"].as_u64().unwrap_or(0),
                v["runs_total"].as_u64().unwrap_or(0) + v["reviews_total"].as_u64().unwrap_or(0),
            )
        };
        k(b).cmp(&k(a))
    });
    orgs
}

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn text(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok().cloned().or_else(|| v.as_n().ok().cloned()))
        .unwrap_or_default()
}

fn num(i: &Option<Item>, k: &str) -> f64 {
    i.as_ref()
        .and_then(|i| i.get(k))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<f64>().ok())
        .unwrap_or(0.0)
}

/// Every item a scan returns, up to `cap`.
async fn scan_all(
    state: &AppState,
    table: &str,
    filter: Option<(&str, &str, &str)>,
    projection: &str,
    cap: usize,
) -> Result<Vec<Item>, StatusCode> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let mut req = state
            .dynamo
            .scan()
            .table_name(table)
            .set_exclusive_start_key(start);
        if let Some((expr, name, value)) = filter {
            req = req
                .filter_expression(expr)
                .expression_attribute_values(name, attr_s(value));
        }
        if !projection.is_empty() {
            req = req.projection_expression(projection);
        }
        let page = req.send().await.map_err(|e| {
            error!(table, "Admin scan failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        out.extend(page.items().iter().cloned());
        start = page.last_evaluated_key().cloned();
        if start.is_none() || out.len() >= cap {
            break;
        }
    }
    Ok(out)
}

/// Every membership record (members and invites), grouped by team.
async fn memberships(state: &AppState) -> Result<HashMap<String, Vec<Item>>, StatusCode> {
    let items = scan_all(state, &state.config.users_table_name, None, "", 50_000).await?;
    let mut by_team: HashMap<String, Vec<Item>> = HashMap::new();
    for i in items {
        if text(&i, "sk").starts_with("USER#") {
            by_team.entry(text(&i, "pk")).or_default().push(i);
        }
    }
    Ok(by_team)
}

fn is_invite(i: &Item) -> bool {
    text(i, "status") == "invited"
}

/// One team's facts from its records.
async fn team_facts(
    state: &AppState,
    team_id: &str,
    teams_meta: Option<&Item>,
    members: &[Item],
    month: &str,
) -> TeamFacts {
    let get = |table: String, key: [(&'static str, String); 2]| {
        let dynamo = state.dynamo.clone();
        async move {
            let mut req = dynamo.get_item().table_name(table);
            for (k, v) in key {
                req = req.key(k, AttributeValue::S(v));
            }
            req.send().await.ok().and_then(|r| r.item)
        }
    };
    let repos = async {
        let mut names = Vec::new();
        let mut start: Option<Item> = None;
        loop {
            let Ok(page) = state
                .dynamo
                .query()
                .table_name(&state.config.repos_table_name)
                .key_condition_expression("pk = :pk AND begins_with(sk, :r)")
                .expression_attribute_values(":pk", attr_s(team_id))
                .expression_attribute_values(":r", attr_s("REPO#"))
                .projection_expression("sk")
                .set_exclusive_start_key(start)
                .send()
                .await
            else {
                break;
            };
            names.extend(page.items().iter().map(|i| {
                text(i, "sk")
                    .trim_start_matches("REPO#")
                    .to_ascii_lowercase()
            }));
            start = page.last_evaluated_key().cloned();
            if start.is_none() || names.len() >= 5000 {
                break;
            }
        }
        names
    };
    let (meta, all, this_month, jira, repos) = tokio::join!(
        get(
            state.config.table_name.clone(),
            [("pk", team_id.to_string()), ("sk", "META".to_string())]
        ),
        get(
            state.config.analytics_table_name.clone(),
            [
                ("team_id", team_id.to_string()),
                ("period", "ALL_TIME".to_string())
            ]
        ),
        get(
            state.config.analytics_table_name.clone(),
            [
                ("team_id", team_id.to_string()),
                ("period", month.to_string())
            ]
        ),
        get(
            state.config.jira_config_table_name.clone(),
            [
                ("pk", team_id.to_string()),
                ("sk", "JIRA#config".to_string())
            ]
        ),
        repos,
    );
    let m = |k: &str| meta.as_ref().map(|i| text(i, k)).unwrap_or_default();
    let tm = |k: &str| teams_meta.map(|i| text(i, k)).unwrap_or_default();
    let or = |a: String, b: String| if a.is_empty() { b } else { a };
    TeamFacts {
        team_id: team_id.to_string(),
        name: or(m("github_org"), tm("github_org")),
        status: m("status"),
        plan: m("plan"),
        created_at: or(tm("created_at"), m("created_at")),
        installation_id: or(tm("github_installation_id"), m("github_install_id")),
        github_org: or(tm("github_org"), m("github_org")),
        jira_site: jira
            .as_ref()
            .map(|i| text(i, "site_url"))
            .unwrap_or_default(),
        members: members
            .iter()
            .filter(|i| !is_invite(i))
            .map(|i| MemberFact {
                user_id: text(i, "sk").trim_start_matches("USER#").to_string(),
                email: {
                    let e = text(i, "email");
                    if e.contains('@') {
                        normalize_email(&e)
                    } else {
                        String::new()
                    }
                },
                last_active_at: super::users::last_active(i).to_string(),
            })
            .collect(),
        invites: members.iter().filter(|i| is_invite(i)).count() as u64,
        repos,
        runs_total: num(&all, "total_runs") as u64,
        runs_month: num(&this_month, "total_runs") as u64,
        reviews_total: num(&all, "reviews") as u64,
        reviews_month: num(&this_month, "reviews") as u64,
        cost_month_usd: num(&this_month, "total_cost_usd"),
        tokens_month: num(&this_month, "total_tokens") as u64,
    }
}

/// GET /api/admin/teams — every team, grouped by organisation. Platform admin
/// only. `teams` is the same teams flat, for older pages.
pub async fn admin_teams(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    if !is_platform_admin(&claims) {
        warn!(email = %claims.email, "Teams overview refused: not a platform admin");
        return Err(StatusCode::FORBIDDEN);
    }
    // A team is in the teams table (email and GitHub sign-ups, installs) or
    // only has members (Google sign-ups): read both.
    let meta_items = scan_all(
        &state,
        &state.config.teams_table_name,
        Some(("sk = :meta", ":meta", "META")),
        "",
        MAX_TEAMS,
    )
    .await?;
    let by_team = memberships(&state).await?;
    let mut ids: Vec<String> = meta_items.iter().map(|i| text(i, "team_id")).collect();
    let known: HashSet<String> = ids.iter().cloned().collect();
    let mut extra: Vec<String> = by_team
        .keys()
        .filter(|t| t.starts_with("TEAM#") && !known.contains(*t))
        .cloned()
        .collect();
    extra.sort();
    ids.extend(extra);
    ids.retain(|t| !t.is_empty());
    ids.truncate(MAX_TEAMS);
    let meta_by_id: HashMap<String, &Item> =
        meta_items.iter().map(|i| (text(i, "team_id"), i)).collect();

    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let empty: Vec<Item> = Vec::new();
    let teams: Vec<TeamFacts> = futures::future::join_all(ids.iter().map(|id| {
        team_facts(
            &state,
            id,
            meta_by_id.get(id).copied(),
            by_team.get(id).unwrap_or(&empty),
            &month,
        )
    }))
    .await;

    let mut orgs = orgs_json(&teams);
    // Each team's plugin flag, for the switch on the admin page.
    let kv = common::plugins::kv::Dynamo {
        client: state.dynamo.clone(),
        table: state.config.settings_table_name.clone(),
    };
    for o in orgs.iter_mut() {
        if let Some(ts) = o["teams"].as_array_mut() {
            for t in ts.iter_mut() {
                let id = t["team_id"].as_str().unwrap_or("").to_string();
                let on = !id.is_empty() && common::plugins::flag::enabled(&kv, &id).await;
                t["flags"] = json!({ "plugins": on });
            }
        }
    }
    // Installations several teams carry: who receives their events, and
    // which links are stale (a warning on the page, with a way to release).
    let links: Vec<(String, u64)> = teams
        .iter()
        .filter_map(|t| {
            t.installation_id
                .trim()
                .parse::<u64>()
                .ok()
                .map(|iid| (t.team_id.clone(), iid))
        })
        .collect();
    let mut shared_installations: Vec<Value> = Vec::new();
    for (iid, team_ids) in super::installation_owner::shared(&links) {
        let owner = super::github_webhook::resolve_team_by_installation(&state, iid).await;
        shared_installations.push(json!({
            "installation_id": iid,
            "owner_team_id": owner,
            "stale_team_ids": super::installation_owner::stale_teams(&team_ids, owner.as_deref()),
        }));
    }
    shared_installations.sort_by_key(|v| v["installation_id"].as_u64());
    let flat: Vec<Value> = orgs
        .iter()
        .flat_map(|o| o["teams"].as_array().cloned().unwrap_or_default())
        .collect();
    let active = orgs.iter().filter(|o| o["active"] == true).count();
    Ok(Json(json!({
        "month": month,
        "orgs": orgs,
        "teams": flat,
        "shared_installations": shared_installations,
        "counts": {
            "teams": teams.len(),
            "orgs": orgs.len(),
            "active_orgs": active,
            "inactive_orgs": orgs.len() - active,
        },
    })))
}

#[derive(serde::Deserialize, Default)]
pub struct ReleaseStaleRequest {
    /// Say what would be released without changing anything (the default).
    #[serde(default = "yes")]
    pub dry_run: bool,
}

fn yes() -> bool {
    true
}

/// POST /api/admin/installations/:installation_id/release-stale { dry_run }
/// — removes the installation's link from every team that carries it but
/// doesn't own it (platform admins only). The owner keeps its link. A dry
/// run (the default) only lists them.
pub async fn release_stale_installation_links(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(installation_id): axum::extract::Path<u64>,
    body: Option<Json<ReleaseStaleRequest>>,
) -> Result<Json<Value>, StatusCode> {
    if !is_platform_admin(&claims) {
        warn!(email = %claims.email, "Installation link release refused: not a platform admin");
        return Err(StatusCode::FORBIDDEN);
    }
    let dry_run = body.map(|Json(b)| b.dry_run).unwrap_or(true);
    let teams =
        super::github_webhook::resolve_all_teams_by_installation(&state, installation_id).await;
    let owner = super::github_webhook::resolve_team_by_installation(&state, installation_id).await;
    let stale = super::installation_owner::stale_teams(&teams, owner.as_deref());
    let mut released: Vec<String> = Vec::new();
    if !dry_run && owner.is_some() {
        for team in &stale {
            match super::installation_owner::release_stale(&state, team, installation_id).await {
                Ok(true) => released.push(team.clone()),
                Ok(false) => {}
                Err(e) => {
                    warn!(team, installation_id, error = %e, "Could not release a stale link")
                }
            }
        }
        info!(installation_id, released = ?released, by = %claims.email, "Stale installation links released");
    }
    Ok(Json(json!({
        "installation_id": installation_id,
        "owner_team_id": owner,
        "stale_team_ids": stale,
        "dry_run": dry_run,
        "released": released,
    })))
}

/// PUT /api/admin/teams/:team_id/flags { plugins } — turns a team's
/// feature flags on or off (platform admins only).
pub async fn set_team_flags(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(team_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if !is_platform_admin(&claims) {
        warn!(email = %claims.email, "Team flags refused: not a platform admin");
        return Err(StatusCode::FORBIDDEN);
    }
    let Some(on) = body["plugins"].as_bool() else {
        return Err(StatusCode::BAD_REQUEST);
    };
    if team_id.is_empty() || team_id.len() > 128 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let kv = common::plugins::kv::Dynamo {
        client: state.dynamo.clone(),
        table: state.config.settings_table_name.clone(),
    };
    let flags = common::plugins::flag::set(&kv, &team_id, on)
        .await
        .map_err(|e| {
            error!(error = %e, "Couldn't set team flags");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    common::plugins::audit::record(
        &kv,
        &team_id,
        common::plugins::audit::Event::new(
            &claims.email,
            common::plugins::audit::ActorKind::Person,
            common::plugins::audit::action::FLAG_SET,
            "ok",
        )
        .detail(format!("plugins {}", if on { "on" } else { "off" })),
    )
    .await;
    info!(team_id, plugins = on, by = %claims.email, "Team plugin flag set");
    Ok(Json(json!({ "team_id": team_id, "flags": flags })))
}

// ── Beta sign-ups ────────────────────────────────────────────────────

/// Where a sign-up stands. `Joined` wins: the person is on a team, whatever
/// else was recorded.
pub fn signup_status(
    email: &str,
    recorded: &str,
    member_emails: &HashSet<String>,
    allowlist: &HashSet<String>,
) -> &'static str {
    let email = normalize_email(email);
    if member_emails.contains(&email) {
        "joined"
    } else if recorded == "invited" || allowlist.contains(&email) {
        "invited"
    } else {
        "new"
    }
}

async fn allowlisted(state: &AppState) -> Result<HashSet<String>, StatusCode> {
    let mut out = HashSet::new();
    let mut start: Option<Item> = None;
    loop {
        let page = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s("ALLOWLIST"))
            .expression_attribute_values(":prefix", attr_s("EMAIL#"))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| {
                error!("Beta sign-ups: allowlist read failed: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        out.extend(
            page.items()
                .iter()
                .map(|i| normalize_email(&text(i, "email"))),
        );
        start = page.last_evaluated_key().cloned();
        if start.is_none() {
            break;
        }
    }
    Ok(out)
}

/// GET /api/admin/beta-signups — everyone on the beta waitlist and where
/// they stand. Platform admin only.
pub async fn beta_signups(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    if !is_platform_admin(&claims) {
        warn!(email = %claims.email, "Beta sign-ups refused: not a platform admin");
        return Err(StatusCode::FORBIDDEN);
    }
    let (signups, by_team, allow) = tokio::join!(
        scan_all(&state, &state.config.waitlist_table_name, None, "", 20_000),
        memberships(&state),
        allowlisted(&state),
    );
    let (signups, by_team, allow) = (signups?, by_team?, allow?);
    let member_emails: HashSet<String> = by_team
        .values()
        .flatten()
        .filter(|i| !is_invite(i))
        .map(|i| text(i, "email"))
        .filter(|e| e.contains('@'))
        .map(|e| normalize_email(&e))
        .collect();
    let mut rows: Vec<Value> = signups
        .iter()
        .map(|i| {
            let email = text(i, "email");
            let status = signup_status(&email, &text(i, "status"), &member_emails, &allow);
            json!({
                "email": email,
                "original_email": text(i, "original_email"),
                "company": text(i, "company"),
                "domain": company_domain(&email).unwrap_or_default(),
                "reason": text(i, "reason"),
                "created_at": text(i, "created_at"),
                "status": status,
                "invited_at": text(i, "invited_at"),
            })
        })
        .collect();
    rows.sort_by(|a, b| {
        b["created_at"]
            .as_str()
            .unwrap_or("")
            .cmp(a["created_at"].as_str().unwrap_or(""))
    });
    let count = |s: &str| rows.iter().filter(|r| r["status"] == s).count();
    let counts = json!({
        "new": count("new"),
        "invited": count("invited"),
        "joined": count("joined"),
        "total": rows.len(),
    });
    Ok(Json(json!({ "signups": rows, "counts": counts })))
}

/// POST /api/admin/beta-signups/invite — mark a sign-up invited and add the
/// email to the sign-up allowlist, so the person can create an account. No
/// email is sent. Platform admin only.
pub async fn invite_beta_signup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if !is_platform_admin(&claims) {
        warn!(email = %claims.email, "Beta invite refused: not a platform admin");
        return Err(StatusCode::FORBIDDEN);
    }
    let email = body["email"]
        .as_str()
        .map(normalize_email)
        .filter(|e| e.contains('@') && e.len() <= 320)
        .ok_or(StatusCode::BAD_REQUEST)?;
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.waitlist_table_name)
        .key("email", attr_s(&email))
        .update_expression("SET #st = :invited, invited_at = :now, invited_by = :by")
        .condition_expression("attribute_exists(email)")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":invited", attr_s("invited"))
        .expression_attribute_values(":now", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.email))
        .send()
        .await
        .map_err(|e| {
            let e = aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string();
            if e.contains("ConditionalCheckFailed") {
                StatusCode::NOT_FOUND
            } else {
                error!("Beta invite: waitlist update failed: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        })?;
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s("ALLOWLIST"))
        .item("sk", attr_s(&format!("EMAIL#{email}")))
        .item("email", attr_s(&email))
        .item("added_by", attr_s(&claims.email))
        .item("created_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Beta invite: allowlist add failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    info!(by = %claims.email, "Beta sign-up invited and allowlisted");
    Ok(Json(
        json!({ "status": "invited", "email": email, "invited_at": now }),
    ))
}

/// GET /api/admin/dlq — the shared dead-letter queue: every team's failed
/// messages, the global jobs', and the ones nobody can read. Platform admin
/// only; teams see only their own on the health page.
pub async fn admin_dlq(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    if !is_platform_admin(&claims) {
        warn!(email = %claims.email, "Shared queue refused: not a platform admin");
        return Err(StatusCode::FORBIDDEN);
    }
    super::dlq::admin_list(&state).await.map(Json)
}

/// GET /api/admin/usage-check — the daily check of recorded model usage
/// against Anthropic's own report (`usage_reconcile` in the worker): whether
/// it is on, and its last result. Platform admin only.
pub async fn admin_usage_check(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    if !is_platform_admin(&claims) {
        return Err(StatusCode::FORBIDDEN);
    }
    let (pk, sk) = common::usage::usage_check_key();
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key("team_id", AttributeValue::S(pk))
        .key("period", AttributeValue::S(sk))
        .send()
        .await
        .map_err(|e| {
            warn!(error = %e, "Couldn't read the usage check status");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(common::usage::usage_check_json(item.item())))
}

/// POST /api/admin/dlq/delete — delete any one message from the shared queue
/// by `message_id`. Platform admin only; logged with the admin's email.
pub async fn admin_dlq_delete(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    if !is_platform_admin(&claims) {
        warn!(email = %claims.email, "Shared queue delete refused: not a platform admin");
        return Err(StatusCode::FORBIDDEN);
    }
    super::dlq::admin_delete(&state, &claims.email, &body).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, email: &str) -> MemberFact {
        MemberFact {
            user_id: id.into(),
            email: email.into(),
            last_active_at: String::new(),
        }
    }

    fn team(id: &str, members: Vec<MemberFact>) -> TeamFacts {
        TeamFacts {
            team_id: id.into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            members,
            ..Default::default()
        }
    }

    fn installed(mut t: TeamFacts, inst: &str, org: &str) -> TeamFacts {
        t.installation_id = inst.into();
        t.github_org = org.into();
        t
    }

    fn ids(teams: &[TeamFacts], g: &OrgGroup) -> Vec<(String, Link)> {
        g.teams
            .iter()
            .map(|&(i, l)| (teams[i].team_id.clone(), l))
            .collect()
    }

    /// The shape found in production: one org's busy team, the per-person
    /// teams GitHub sign-ins used to mint on the same installation, and
    /// email sign-ups' personal teams whose owners joined the org team.
    #[test]
    fn one_company_reads_as_one_organisation() {
        let mut main = installed(
            team(
                "TEAM#main",
                vec![
                    member("101", ""),
                    member("102", ""),
                    member("u-ana", "ana@acme.test"),
                    member("u-ben", "ben@acme.test"),
                ],
            ),
            "77",
            "acme",
        );
        main.runs_total = 500;
        let gh_dupe = installed(team("TEAM#gh1", vec![member("101", "")]), "77", "acme");
        let gh_dupe2 = installed(team("TEAM#gh2", vec![member("102", "")]), "77", "acme");
        let personal = team("TEAM#p-ana", vec![member("u-ana", "ana@acme.test")]);
        // Never joined the org team, but works at the same company.
        let colleague = team("TEAM#p-cy", vec![member("u-cy", "cy@acme.test")]);
        // Someone else entirely, on a public mail domain.
        let stranger = team("TEAM#p-zed", vec![member("u-zed", "zed@gmail.com")]);
        let teams = vec![gh_dupe, personal, main, colleague, stranger, gh_dupe2];

        let groups = group_teams(&teams);
        assert_eq!(groups.len(), 2, "{groups:?}");
        assert_eq!(
            ids(&teams, &groups[0]),
            vec![
                ("TEAM#main".into(), Link::Main),
                ("TEAM#gh1".into(), Link::Github),
                ("TEAM#gh2".into(), Link::Github),
                ("TEAM#p-ana".into(), Link::Member),
                ("TEAM#p-cy".into(), Link::Domain),
            ]
        );
        assert_eq!(
            ids(&teams, &groups[1]),
            vec![("TEAM#p-zed".into(), Link::Main)]
        );
    }

    #[test]
    fn a_team_with_its_own_installation_is_never_folded_in_by_a_weaker_link() {
        let mut org = installed(
            team("TEAM#org", vec![member("u-ana", "ana@acme.test")]),
            "1",
            "acme",
        );
        org.runs_total = 10;
        // The same person installed the app on their own GitHub account.
        let own = installed(
            team("TEAM#own", vec![member("u-ana2", "ana@acme.test")]),
            "2",
            "ana-personal",
        );
        let groups = group_teams(&[org, own]);
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn the_same_jira_site_joins_two_installations() {
        let mut a = installed(team("TEAM#a", vec![]), "1", "acme-web");
        a.jira_site = "https://acme.atlassian.net/".into();
        let mut b = installed(team("TEAM#b", vec![]), "2", "acme-mobile");
        b.jira_site = "acme.atlassian.net".into();
        let c = installed(team("TEAM#c", vec![]), "3", "other");
        let groups = group_teams(&[a, b, c]);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].teams.len(), 2);
    }

    #[test]
    fn the_same_org_name_on_two_installations_is_one_organisation() {
        let a = installed(team("TEAM#a", vec![]), "1", "Acme");
        let b = installed(team("TEAM#b", vec![]), "2", "acme");
        assert_eq!(group_teams(&[a, b]).len(), 1);
    }

    #[test]
    fn loose_teams_join_each_other_by_member_then_domain_but_not_public_mail() {
        let a = team("TEAM#a", vec![member("u-1", "dee@beta.test")]);
        // The same person with a second identity (e.g. Google and email).
        let b = team("TEAM#b", vec![member("u-2", "dee@beta.test")]);
        let c = team("TEAM#c", vec![member("u-3", "eli@beta.test")]);
        let d = team("TEAM#d", vec![member("u-4", "fay@gmail.com")]);
        let e = team("TEAM#e", vec![member("u-5", "gus@gmail.com")]);
        let teams = [a, b, c, d, e];
        let groups = group_teams(&teams);
        assert_eq!(groups.len(), 3, "{groups:?}");
        let first: HashMap<String, Link> = ids(&teams, &groups[0]).into_iter().collect();
        assert_eq!(first.len(), 3);
        assert_eq!(first["TEAM#b"], Link::Member);
        assert_eq!(first["TEAM#c"], Link::Domain);
    }

    #[test]
    fn the_main_team_is_the_busiest() {
        let a = installed(team("TEAM#a", vec![member("1", "")]), "9", "acme");
        let mut b = installed(team("TEAM#b", vec![member("2", "")]), "9", "acme");
        b.reviews_total = 3;
        let teams = [a, b];
        let groups = group_teams(&teams);
        assert_eq!(ids(&teams, &groups[0])[0], ("TEAM#b".into(), Link::Main));
    }

    #[test]
    fn personal_workspaces_with_copied_repos_are_not_activity() {
        let mut main = installed(
            team("TEAM#main", vec![member("101", ""), member("102", "")]),
            "7",
            "acme",
        );
        main.runs_total = 5;
        let mut dupe = installed(team("TEAM#dupe", vec![member("101", "")]), "7", "acme");
        dupe.repos = vec!["acme/web".into()];
        let mut fresh = installed(team("TEAM#fresh", vec![member("201", "")]), "8", "new-co");
        fresh.repos = vec!["new-co/app".into()];
        let empty = team("TEAM#empty", vec![member("301", "x@empty.test")]);
        let teams = [main, dupe, fresh, empty];
        assert!(is_active(&teams, 0));
        assert!(is_personal(&teams, 1) && !is_active(&teams, 1));
        // Set up and waiting for its first run: active.
        assert!(!is_personal(&teams, 2) && is_active(&teams, 2));
        assert!(!is_personal(&teams, 3) && !is_active(&teams, 3));
    }

    #[test]
    fn org_rows_count_people_and_repos_once() {
        let mut main = installed(
            team(
                "TEAM#main",
                vec![member("101", ""), member("u-a", "a@acme.test")],
            ),
            "7",
            "acme",
        );
        main.runs_total = 5;
        main.runs_month = 2;
        main.repos = vec!["acme/web".into(), "acme/api".into()];
        let mut dupe = installed(team("TEAM#dupe", vec![member("101", "")]), "7", "acme");
        dupe.repos = vec!["acme/web".into()];
        let lone = team("TEAM#lone", vec![member("u-z", "z@zeta.test")]);
        let orgs = orgs_json(&[dupe, lone, main]);
        assert_eq!(orgs.len(), 2);
        let acme = &orgs[0];
        assert_eq!(acme["label"], "acme");
        assert_eq!(acme["active"], true);
        assert_eq!(acme["team_count"], 2);
        assert_eq!(acme["personal_count"], 1);
        assert_eq!(acme["members"], 2);
        assert_eq!(acme["repos"], 2);
        assert_eq!(acme["runs_total"], 5);
        assert_eq!(acme["teams"][0]["team_id"], "TEAM#main");
        assert_eq!(acme["teams"][1]["link"], "github");
        assert_eq!(orgs[1]["active"], false);
        assert_eq!(orgs[1]["label"], "zeta.test");
    }

    #[test]
    fn company_domains_skip_public_mail() {
        assert_eq!(company_domain("a@Acme.test").as_deref(), Some("acme.test"));
        assert_eq!(company_domain("a@gmail.com"), None);
        assert_eq!(company_domain("1+x@users.noreply.github.com"), None);
        assert_eq!(company_domain("no-at-sign"), None);
        assert_eq!(company_domain("a@localhost"), None);
    }

    #[test]
    fn a_signup_on_a_team_has_joined_whatever_else_was_recorded() {
        let members: HashSet<String> = ["dee@beta.test".to_string()].into();
        let allow: HashSet<String> = ["eli@beta.test".to_string()].into();
        assert_eq!(
            signup_status("Dee+beta@beta.test", "invited", &members, &allow),
            "joined"
        );
        assert_eq!(
            signup_status("eli@beta.test", "", &members, &allow),
            "invited"
        );
        assert_eq!(
            signup_status("fay@beta.test", "invited", &members, &allow),
            "invited"
        );
        assert_eq!(signup_status("gus@beta.test", "", &members, &allow), "new");
    }

    /// Every admin handler starts with the platform admin check.
    #[test]
    fn every_admin_handler_checks_for_the_platform_admin_first() {
        let src = include_str!("admin.rs");
        let mut checked = 0;
        for chunk in src.split("\npub async fn ").skip(1) {
            let name: String = chunk
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            let body = &chunk[chunk.find("{\n").expect("body")..];
            let first = body
                .lines()
                .map(str::trim)
                .find(|l| !l.is_empty() && *l != "{")
                .unwrap_or("");
            assert_eq!(
                first, "if !is_platform_admin(&claims) {",
                "{name} must refuse anyone but the platform admin before doing anything"
            );
            checked += 1;
        }
        assert_eq!(checked, 8);
    }
}
