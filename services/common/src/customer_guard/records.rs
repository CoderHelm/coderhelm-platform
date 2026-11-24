//! Reads the guard's terms from CoderHelm's own records, on the server, for
//! one install: every team on it and, on the hosted side, every linked
//! self-hosted install. Read only, kept only in memory by the caller, and
//! never logged (errors name the table, never a value).

use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;

use super::{InstallRecords, TeamRecords};
use crate::knowledge::sources::{Kind as SourceKind, Source};

type Item = HashMap<String, AttributeValue>;

/// The tables the records are read from. An empty name is skipped.
#[derive(Debug, Clone, Default)]
pub struct Tables {
    /// The main table (`pk` team, `sk` `META`).
    pub main: String,
    /// The teams table (`team_id`, `sk`): `META` and ticket locks.
    pub teams: String,
    pub users: String,
    pub repos: String,
    pub jira_config: String,
    pub settings: String,
    pub aws_insights: String,
}

fn s(item: &Item, key: &str) -> String {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn push(v: &mut Vec<String>, value: String) {
    let t = value.trim().to_string();
    if !t.is_empty() && !v.contains(&t) {
        v.push(t);
    }
}

/// Every item of a scan, with only `attrs` read.
async fn scan(dynamo: &Client, table: &str, attrs: &[&str]) -> Result<Vec<Item>, String> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    let names: HashMap<String, String> = attrs
        .iter()
        .enumerate()
        .map(|(i, a)| (format!("#a{i}"), a.to_string()))
        .collect();
    let projection = (0..attrs.len())
        .map(|i| format!("#a{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    loop {
        let r = dynamo
            .scan()
            .table_name(table)
            .projection_expression(&projection)
            .set_expression_attribute_names(Some(names.clone()))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|_| format!("customer guard: scan of {table} failed"))?;
        out.extend(r.items().iter().cloned());
        match r.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

/// Every item under one partition, optionally under a sort-key prefix.
async fn query(
    dynamo: &Client,
    table: &str,
    pk_name: &str,
    pk: &str,
    prefix: Option<&str>,
) -> Result<Vec<Item>, String> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let mut q = dynamo
            .query()
            .table_name(table)
            .expression_attribute_names("#pk", pk_name)
            .expression_attribute_values(":pk", AttributeValue::S(pk.to_string()))
            .set_exclusive_start_key(start.take());
        q = match prefix {
            Some(p) => q
                .key_condition_expression("#pk = :pk AND begins_with(sk, :p)")
                .expression_attribute_values(":p", AttributeValue::S(p.to_string())),
            None => q.key_condition_expression("#pk = :pk"),
        };
        let r = q
            .send()
            .await
            .map_err(|_| format!("customer guard: read of {table} failed"))?;
        out.extend(r.items().iter().cloned());
        match r.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

/// Ticket prefixes CoderHelm makes up itself (GitHub issues, security
/// fixes, alerts, log findings and the like): never a customer's project.
pub const OWN_TICKET_PREFIXES: &[&str] = &[
    "GH", "GHSA", "CVE", "SEC", "ALERT", "LOG", "START", "INC", "MAL", "CGA", "PLAN", "TEST",
];

/// Pure: the project key of a ticket id (`KEY` of `KEY-123`), unless it is
/// one of CoderHelm's own prefixes.
pub fn project_key_of(ticket: &str) -> Option<String> {
    let (key, num) = ticket.split_once('-')?;
    let key_ok = (2..=10).contains(&key.len())
        && key.chars().next().is_some_and(|c| c.is_ascii_uppercase())
        && key
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
    let num_ok = !num.is_empty() && num.chars().all(|c| c.is_ascii_digit());
    (key_ok && num_ok && !OWN_TICKET_PREFIXES.contains(&key)).then(|| key.to_string())
}

/// Pure: every AWS account id in an ARN in the text.
pub fn arn_accounts(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for part in text.split("arn:aws").skip(1) {
        // ":service:region:account:..."
        if let Some(acct) = part.split(':').nth(3) {
            if acct.len() == 12 && acct.chars().all(|c| c.is_ascii_digit()) {
                push(&mut out, acct.to_string());
            }
        }
    }
    out
}

/// Pure: the Teams tenant of an incoming webhook's host
/// (`<tenant>.webhook.office.com`), from the stored hint or address.
pub fn teams_tenant(hint_or_url: &str) -> Option<String> {
    let host = hint_or_url
        .trim()
        .trim_start_matches("https://")
        .split(['/', ' ', '…'])
        .next()?
        .to_ascii_lowercase();
    let tenant = host.strip_suffix(".webhook.office.com")?;
    (!tenant.is_empty() && !tenant.contains('.')).then(|| tenant.to_string())
}

/// Pure: what a knowledge source adds (Confluence: its site and spaces).
pub fn source_records(src: &Source, r: &mut TeamRecords) {
    if src.kind != Some(SourceKind::Confluence) {
        return;
    }
    if let Some(site) = src.options["site"].as_str() {
        push(&mut r.wiki_hosts, site.to_string());
    }
    if let Some(u) = &src.used {
        if u.account.contains('.') {
            push(&mut r.wiki_hosts, u.account.clone());
        }
    }
    for sel in &src.selection {
        if sel.kind == "space" {
            push(&mut r.wiki_spaces, sel.title.clone());
        }
        if let Some(space) = sel.path.first() {
            push(&mut r.wiki_spaces, space.clone());
        }
    }
}

/// One team's records, read by its id.
async fn team(dynamo: &Client, t: &Tables, team_id: &str, r: &mut TeamRecords) {
    // Failed reads leave that part out; the caller reads again next time.
    if !t.repos.is_empty() {
        for i in query(dynamo, &t.repos, "pk", team_id, None)
            .await
            .unwrap_or_default()
        {
            push(&mut r.repos, s(&i, "repo_name"));
        }
    }
    if !t.users.is_empty() {
        for i in query(dynamo, &t.users, "pk", team_id, Some("USER#"))
            .await
            .unwrap_or_default()
        {
            push(&mut r.member_emails, s(&i, "email"));
        }
    }
    if !t.jira_config.is_empty() {
        for i in query(dynamo, &t.jira_config, "pk", team_id, Some("JIRA#"))
            .await
            .unwrap_or_default()
        {
            push(&mut r.jira_hosts, s(&i, "site_url"));
            let key = s(&i, "default_project");
            if project_key_of(&format!("{key}-1")).is_some() {
                push(&mut r.project_keys, key);
            }
        }
    }
    if !t.aws_insights.is_empty() {
        for i in query(dynamo, &t.aws_insights, "pk", team_id, Some("AWS_CONN#"))
            .await
            .unwrap_or_default()
        {
            push(&mut r.aws_accounts, s(&i, "account_id"));
            for a in arn_accounts(&s(&i, "role_arn")) {
                push(&mut r.aws_accounts, a);
            }
        }
    }
    if !t.settings.is_empty() {
        for i in query(dynamo, &t.settings, "pk", team_id, Some("KBSRC#"))
            .await
            .unwrap_or_default()
        {
            if let Some(src) = crate::knowledge::read_data::<Source>(&i) {
                source_records(&src, r);
            }
        }
        for i in query(dynamo, &t.settings, "pk", team_id, Some("DATASOURCE#"))
            .await
            .unwrap_or_default()
        {
            for v in i.values().filter_map(|v| v.as_s().ok()) {
                for a in arn_accounts(v) {
                    push(&mut r.aws_accounts, a);
                }
            }
        }
        for (_, i) in crate::notify::route::load(dynamo, &t.settings, team_id).await {
            for k in ["teams_webhook_hint", "teams_webhook_url"] {
                if let Some(tenant) = teams_tenant(&s(&i, k)) {
                    push(&mut r.workspaces, tenant);
                }
            }
        }
    }
}

/// The hosted side's linked self-hosted installs: each one's name, its
/// linked GitHub accounts and repositories, and its custom domain.
async fn hybrid_installs(dynamo: &Client, settings: &str) -> Vec<TeamRecords> {
    let mut out = Vec::new();
    let Ok(list) = query(dynamo, settings, "pk", "HYBRIDINSTALLS", None).await else {
        return out;
    };
    for inst in list {
        let id = s(&inst, "sk");
        let mut r = TeamRecords::default();
        let name = s(&inst, "name");
        if !name.is_empty() {
            push(&mut r.custom_domains, format!("{name}.coderhelm.com"));
            push(&mut r.install_names, name);
        }
        let pk = crate::hybrid::relay::install_pk(&id);
        for i in query(dynamo, settings, "pk", &pk, None)
            .await
            .unwrap_or_default()
        {
            let sk = s(&i, "sk");
            if sk.starts_with("GHLINK#") {
                if let Some(link) =
                    crate::knowledge::read_data::<crate::hybrid::broker::GithubLink>(&i)
                {
                    push(&mut r.orgs, link.account.clone());
                    for repo in link
                        .repositories
                        .iter()
                        .chain(link.ceiling.iter().flatten())
                    {
                        push(&mut r.repos, repo.clone());
                    }
                }
            } else if sk == crate::mail::relay::DOMAIN_SK {
                push(&mut r.custom_domains, s(&i, "hostname"));
            }
        }
        out.push(r);
    }
    out
}

/// The install's own names: its organization name (the email sender's) and
/// the hosts it is served on.
async fn install_names(dynamo: &Client, settings: &str) -> TeamRecords {
    let mut r = TeamRecords::default();
    if !settings.is_empty() {
        if let Ok(Some(n)) = crate::mail::sender_name::load(dynamo, settings).await {
            push(&mut r.names, n.organization);
        }
    }
    if let Some(org) = crate::mail::naming::org_from_env() {
        push(&mut r.names, org);
    }
    for var in ["APP_URL", "API_URL", "SITE_URL"] {
        if let Ok(v) = std::env::var(var) {
            push(&mut r.custom_domains, v);
        }
    }
    r
}

/// Every team's records on the install. A read that fails leaves that
/// part out (the error says which table, never a value); a failure to list
/// the teams at all is an error.
pub async fn load(dynamo: &Client, t: &Tables) -> Result<InstallRecords, String> {
    let mut teams: BTreeMap<String, TeamRecords> = BTreeMap::new();
    if !t.teams.is_empty() {
        for i in scan(dynamo, &t.teams, &["team_id", "sk", "github_org"]).await? {
            let id = s(&i, "team_id");
            if id.is_empty() {
                continue;
            }
            let r = teams.entry(id).or_default();
            let sk = s(&i, "sk");
            if sk == "META" {
                push(&mut r.orgs, s(&i, "github_org"));
            } else if let Some(ticket) = sk
                .strip_prefix("TICKET_LOCK#")
                .or_else(|| sk.strip_prefix("TICKET_RUN#"))
            {
                if let Some(key) = project_key_of(ticket) {
                    push(&mut r.project_keys, key);
                }
            }
        }
    }
    if !t.main.is_empty() {
        for i in scan(dynamo, &t.main, &["pk", "sk", "github_org"]).await? {
            if s(&i, "sk") != "META" {
                continue;
            }
            let id = s(&i, "pk");
            if !id.is_empty() {
                push(&mut teams.entry(id).or_default().orgs, s(&i, "github_org"));
            }
        }
    }
    let ids: Vec<String> = teams.keys().cloned().collect();
    for id in ids {
        if let Some(r) = teams.get_mut(&id) {
            team(dynamo, t, &id, r).await;
        }
    }
    let mut out: Vec<TeamRecords> = teams.into_values().collect();
    if !t.settings.is_empty() {
        out.extend(hybrid_installs(dynamo, &t.settings).await);
    }
    Ok(InstallRecords {
        teams: out,
        install: install_names(dynamo, &t.settings).await,
    })
}
