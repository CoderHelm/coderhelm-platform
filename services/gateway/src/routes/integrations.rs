//! The Integrations page: every outside system Coderhelm connects to, on one
//! page, each with its status, what Coderhelm does with it, when it was last
//! checked and the one thing to do next.
//!
//! Members see it read-only; admins act on it through each integration's own
//! routes (GitHub, Jira, AWS, New Relic and Confluence, MCP servers, Teams, Slack).
//!
//! A connected AWS account is one integration with abilities (logs, metrics,
//! Airflow). What the account's role can do is found by a read-only check,
//! run when the page loads (if the last one is older than a few minutes) and
//! on "Check again". Loading the page also folds old separate metrics and
//! Airflow connections into their AWS account (see
//! `common::live::aws_account`).

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::knowledge::sources::Kind as SourceKind;
use common::live::aws_account::{self, AwsAccount};
use common::live::connectors::{self, ConnKind, Connector};
use common::live::identity;
use common::mcp_remote::store::{Owner, ServerRecord, Sharing, Status, TokenRecord};
use common::problems::{Check, Kind, Outcome};
use serde_json::{json, Value};
use tracing::{error, info, warn};

use super::integration_status::{product_state, Action, Need, Path as Way, ProductState, Use};
use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;
type Item = HashMap<String, AttributeValue>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

fn internal(context: &str, e: String) -> (StatusCode, Json<Value>) {
    error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn ob(item: &Item, k: &str) -> Option<bool> {
    item.get(k).and_then(|v| v.as_bool().ok()).copied()
}

/// Where each integration sits on the page.
pub fn section_of(id: &str) -> &'static str {
    // A named REST API (`rest:<name>`) sits with the others.
    let id = id.split_once(':').map(|(k, _)| k).unwrap_or(id);
    match id {
        "github" | "jira" | "atlassian" => "code",
        "aws" | "opensearch" | "rest" => "cloud",
        "newrelic" | "sentry" => "monitoring",
        "confluence" | "notion" | "figma" | "sanity" | "google_drive" => "docs",
        "teams" | "slack" => "chat",
        // Remote MCP servers an admin added by address (`mcp:<id>`).
        "mcp" => "mcp",
        _ => "other",
    }
}

/// A card's state, worked out from what is saved. `status` is one of
/// `connected`, `needs_credentials`, `needs_reconnect`, `not_connected`,
/// `error`; `action` is the card's one primary action: `connect`,
/// `add_credentials`, `reconnect`, `check_again` or `manage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardState {
    pub status: &'static str,
    pub reason: String,
    pub action: &'static str,
}

fn cs(status: &'static str, reason: impl Into<String>, action: &'static str) -> CardState {
    CardState {
        status,
        reason: reason.into(),
        action,
    }
}

/// An MCP server from the catalog. Switched on without credentials is the
/// state that matters most: agents don't get it, and nothing else says so.
pub fn plugin_state(
    enabled: Option<bool>,
    has_credentials: bool,
    last_test_ok: Option<bool>,
    last_test_message: &str,
    tested_after_credentials: bool,
) -> CardState {
    match enabled {
        None | Some(false) => cs("not_connected", "", "connect"),
        Some(true) if !has_credentials => cs(
            "needs_credentials",
            "Switched on, but no credentials are saved, so agents don't get it.",
            "add_credentials",
        ),
        Some(true) => match last_test_ok.filter(|_| tested_after_credentials) {
            Some(false) => cs(
                "error",
                if last_test_message.is_empty() {
                    "The last test failed.".to_string()
                } else {
                    format!("The last test failed: {last_test_message}")
                },
                "check_again",
            ),
            _ => cs("connected", "", "manage"),
        },
    }
}

/// A catalog server with the team's own app saved (`own_app` is its card
/// JSON): connected through it, it needs signing in again, or (not signed
/// in yet) the key's state stands, saying a sign-in is waiting.
pub fn own_app_state(own_app: &Value, key: &CardState) -> CardState {
    let reason = own_app["status_reason"].as_str().unwrap_or("").to_string();
    let coderhelm = own_app["source"] == "coderhelm";
    match own_app["status"].as_str().unwrap_or("") {
        "connected" if coderhelm => cs("connected", "Signed in with Coderhelm's app.", "manage"),
        "connected" => cs("connected", "Signed in with your own app.", "manage"),
        "needs_reconnect" => cs(
            "needs_reconnect",
            if !reason.is_empty() {
                reason
            } else if coderhelm {
                "The sign-in has to be renewed.".to_string()
            } else {
                "Your own app's sign-in has to be renewed.".to_string()
            },
            "reconnect",
        ),
        "failed" => cs("error", reason, "check_again"),
        _ if key.status == "connected" => key.clone(),
        _ if coderhelm => cs("not_connected", "", "connect"),
        _ => cs(
            "not_connected",
            "Your own app is saved. Connect it to sign in.",
            "connect",
        ),
    }
}

/// A remote MCP server, as seen by one person: their own connection on a
/// personal server, the team's otherwise.
pub fn remote_state(
    s: &common::mcp_remote::store::ServerRecord,
    mine: Option<&common::mcp_remote::store::TokenRecord>,
) -> CardState {
    use common::mcp_remote::store::{AuthKind, Sharing, Status};
    let (status, reason) = if s.auth == AuthKind::OAuth && s.sharing == Sharing::Personal {
        match mine {
            None => (
                Status::NotConnected,
                "Each person connects their own account. You haven't yet.".to_string(),
            ),
            Some(t) => (t.status, t.status_reason.clone()),
        }
    } else {
        (s.status, s.status_reason.clone())
    };
    let waiting = common::mcp_remote::tools::awaiting_approval(s).len();
    match status {
        Status::Connected if s.allowed.is_empty() => cs(
            "connected",
            "No tools are allowed yet, so agents don't get it.",
            "manage",
        ),
        Status::Connected if waiting > 0 => cs(
            "connected",
            format!(
                "{} waiting for another admin to approve.",
                common::live::count(waiting, "tool")
            ),
            "manage",
        ),
        Status::Connected => cs("connected", reason, "manage"),
        Status::NeedsReconnect => cs(
            "needs_reconnect",
            if reason.is_empty() {
                "The sign-in has to be renewed.".to_string()
            } else {
                reason
            },
            "reconnect",
        ),
        Status::Failed => cs("error", reason, "check_again"),
        Status::NotConnected => cs("not_connected", reason, "connect"),
    }
}

/// New Relic or Confluence.
pub fn connector_state(c: Option<&Connector>) -> CardState {
    match c {
        None => cs("not_connected", "", "connect"),
        Some(c) if c.key_sealed.is_empty() && !c.via_jira_app() && !c.via_sign_in() => {
            cs("needs_credentials", "No key is saved.", "add_credentials")
        }
        Some(c) if c.status != "ready" => cs(
            "error",
            if c.status_message.is_empty() {
                "The last test failed.".to_string()
            } else {
                c.status_message.clone()
            },
            "check_again",
        ),
        Some(c) if !c.enabled => cs(
            "connected",
            if c.via_jira_app() {
                "Connected through the Jira app but paused: agents don't use it."
            } else if c.via_sign_in() {
                "Reads with the team's Atlassian sign-in but paused: agents don't use it."
            } else {
                "Connected but paused: agents don't use it."
            },
            "manage",
        ),
        Some(c) if c.via_jira_app() => cs("connected", "Connected through the Jira app.", "manage"),
        Some(c) if c.via_sign_in() => cs(
            "connected",
            "Agents read pages with the team's Atlassian sign-in.",
            "manage",
        ),
        Some(_) => cs("connected", "", "manage"),
    }
}

/// A connected AWS account.
pub fn aws_state(a: &AwsAccount) -> CardState {
    if !a.abilities.check_error.is_empty() {
        return cs(
            "error",
            format!(
                "Coderhelm couldn't take on the role: {}",
                a.abilities.check_error
            ),
            "check_again",
        );
    }
    if a.status == "error" {
        return cs("error", "The last test of the role failed.", "check_again");
    }
    if let Some(p) = a
        .permissions
        .as_ref()
        .filter(|p| p.status() == "update_needed")
    {
        let n = p.missing.len();
        let s_ = if n == 1 { "" } else { "s" };
        // The install's own account gets them with the next version update:
        // nothing to do in AWS.
        if install_account(a) {
            return cs(
                "connected",
                format!("{n} read-only permission{s_} arrive with the next Coderhelm update. Nothing is needed in AWS."),
                "manage",
            );
        }
        return cs(
            "connected",
            format!("Update needed: {n} new read-only permission{s_}."),
            "manage",
        );
    }
    // Switched on, but the role lacks the reads: a warning that names it.
    let infra = a.infra_missing();
    if !infra.is_empty() {
        let n = infra.len();
        return cs(
            "connected",
            format!(
                "Infrastructure vulnerabilities is on, but the role lacks {n} read-only permission{} it needs. Update the role.",
                if n == 1 { "" } else { "s" }
            ),
            "manage",
        );
    }
    cs("connected", "", "manage")
}

/// What the account can't do yet that the add-on stack would give it.
pub fn aws_missing(a: &AwsAccount) -> Vec<&'static str> {
    let mut out = Vec::new();
    if a.abilities.can_metrics == Some(false) {
        out.push("metrics");
    }
    if a.abilities.can_airflow == Some(false) {
        out.push("airflow");
    }
    if !a.infra_missing().is_empty() {
        out.push("infra");
    }
    out
}

/// The account is the self-hosted install's own. Its read-only permissions
/// arrive with the install's version updates, so no update stack is offered.
pub fn install_account(a: &AwsAccount) -> bool {
    a.permissions.as_ref().is_some_and(|p| p.install_account)
        || common::live::aws::in_install_account(
            &a.role_arn,
            common::live::aws::install_read_role().as_deref(),
        )
}

/// The one-click update stack's link, when the role lacks something and a
/// person can add it (never in the install's own account).
fn update_stack_link(a: &AwsAccount) -> Option<String> {
    (!install_account(a) && (!aws_missing(a).is_empty() || needs_update(a)))
        .then(|| super::connectors::update_stack_url(&a.region, &a.chosen()))
        .flatten()
}

/// The last live check found permissions of this release missing.
pub fn needs_update(a: &AwsAccount) -> bool {
    a.permissions
        .as_ref()
        .is_some_and(|p| p.status() == "update_needed")
}

fn card(
    id: &str,
    name: &str,
    does: &str,
    st: CardState,
    last_check: &str,
    manage: &str,
    detail: Value,
) -> Value {
    json!({
        "id": id,
        "section": section_of(id),
        "name": name,
        "does": does,
        "status": st.status,
        "reason": st.reason,
        "action": st.action,
        "last_check": last_check,
        "manage_href": manage,
        "detail": detail,
        // What it is used for, how it is connected and by whom: filled in
        // for a product reached in more than one way ([`product_card`]).
        "uses": [],
        "how": "",
        "connected_by": "",
        "connected_at": "",
    })
}

/// A card whose state comes from every way the team reaches the product
/// (`integration_status`).
fn product_card(
    id: &str,
    name: &str,
    does: &str,
    p: &ProductState,
    last_check: &str,
    manage: &str,
    detail: Value,
) -> Value {
    let mut v = card(id, name, does, p.card.clone(), last_check, manage, detail);
    if let Some(fields) = p.fields().as_object() {
        for (k, val) in fields {
            v[k.as_str()] = val.clone();
        }
    }
    v
}

/// The sign-in a team (or, for Sanity, this person) made so Knowledge can
/// sync a product: its record and its connection, when someone signed in.
#[derive(Debug, Clone)]
pub struct KnowledgeSignIn {
    pub kind: SourceKind,
    pub server: ServerRecord,
    pub token: TokenRecord,
    /// How many of the team's Knowledge sources are of this kind.
    pub sources: usize,
}

impl KnowledgeSignIn {
    fn personal(&self) -> bool {
        self.server.sharing == Sharing::Personal
    }

    fn works(&self) -> bool {
        self.token.status == Status::Connected
    }

    /// Whose sign-in it is, in a card's words.
    pub fn how(&self) -> String {
        match (self.kind, self.personal()) {
            (_, true) => format!("your {} sign-in", self.kind.label()),
            (SourceKind::Confluence, _) => "the team's Atlassian sign-in".into(),
            (SourceKind::GoogleDrive, _) => "the team's Google sign-in".into(),
            (k, _) => format!("the team's {} sign-in", k.label()),
        }
    }

    /// The sign-in's own state. A sign-in that stopped working is renewed
    /// by signing in again, whatever stopped it.
    pub fn state(&self) -> CardState {
        let why = self.token.status_reason.clone();
        match self.token.status {
            Status::Connected => cs("connected", "", "manage"),
            Status::Failed => cs("error", why, "reconnect"),
            Status::NotConnected => cs("not_connected", why, "connect"),
            Status::NeedsReconnect => cs(
                "needs_reconnect",
                if why.is_empty() {
                    "The sign-in has to be renewed.".to_string()
                } else {
                    why
                },
                "reconnect",
            ),
        }
    }

    /// The way Knowledge reaches the product.
    pub fn way(&self) -> Way {
        Way::new(&self.how(), self.state(), &[Use::Knowledge])
            .by(&self.token.connected_by, &self.token.connected_at)
    }

    fn json(&self) -> Value {
        let st = self.state();
        json!({
            "kind": self.kind.key(),
            "label": self.kind.label(),
            "server_id": self.server.id,
            "personal": self.personal(),
            "status": st.status,
            "reason": st.reason,
            "connected_by": self.token.connected_by,
            "connected_at": self.token.connected_at,
            "sources": self.sources,
        })
    }
}

/// The Knowledge source kind a card's product is also synced as.
pub fn knowledge_kind(card_id: &str) -> Option<SourceKind> {
    match card_id {
        "confluence" => Some(SourceKind::Confluence),
        "notion" => Some(SourceKind::Notion),
        "sanity" => Some(SourceKind::Sanity),
        "google_drive" => Some(SourceKind::GoogleDrive),
        _ => None,
    }
}

const KNOWLEDGE_HREF: &str = "/knowledge/";

/// Confluence's card, from the connection agents read with (an API token,
/// the Jira app, or the team's Atlassian sign-in switched on for runs) and
/// the sign-in made for Knowledge.
pub fn confluence_product(
    conn: Option<&Connector>,
    knowledge: Option<&KnowledgeSignIn>,
    sources: usize,
) -> ProductState {
    let mut ways = Vec::new();
    if let Some(k) = knowledge {
        // The same sign-in reads pages during runs once an admin turns
        // that on: its scopes cover it, so nobody signs in twice.
        ways.push(k.way().extend(
            Use::Runs,
            "The same sign-in can read pages during runs and reviews. Turn it on: no second sign-in.",
            Some(Action::new("use_in_runs", "Use in runs too")),
        ));
    }
    if let Some(c) = conn {
        let mut st = connector_state(Some(c));
        let how = if c.via_jira_app() {
            "the Jira app"
        } else if c.via_sign_in() {
            "the team's Atlassian sign-in"
        } else {
            "an API token"
        };
        // Reading with the sign-in stops when the sign-in does.
        if c.via_sign_in() && st.status == "connected" && !knowledge.is_some_and(|k| k.works()) {
            st = match knowledge {
                Some(k) => k.state(),
                None => cs(
                    "needs_reconnect",
                    "The team isn't signed in to Confluence any more.",
                    "reconnect",
                ),
            };
        }
        let token = !c.via_jira_app() && !c.via_sign_in();
        // A saved token also syncs Knowledge's sources when nobody signed in.
        let serves: &[Use] = if token && knowledge.is_none() && sources > 0 {
            &[Use::Runs, Use::Knowledge]
        } else {
            &[Use::Runs]
        };
        let mut way = Way::new(how, st, serves).by(&c.updated_by, &c.updated_at);
        if token {
            way = way.extend(
                Use::Knowledge,
                "Knowledge can sync with the same token: add a source there.",
                Some(Action::link("Add a source in Knowledge", KNOWLEDGE_HREF)),
            );
        }
        ways.push(way);
    }
    let needs = [Need {
        for_use: Use::Knowledge,
        detail: "Syncing pages into Knowledge needs an Atlassian sign-in or an API token: the Jira app only reads pages during runs.".into(),
        action: Some(Action::link("Connect in Knowledge", KNOWLEDGE_HREF)),
    }];
    product_state(&[Use::Runs, Use::Knowledge], &ways, &needs)
}

/// The ways a team reaches a catalog product, as found in its records.
#[derive(Debug, Default)]
pub struct CatalogWays<'a> {
    /// The app sign-in's card JSON (`mcp_servers::own_app_json`).
    pub own_app: Option<&'a Value>,
    /// The vendor's remote server and this person's (or the team's)
    /// connection to it.
    pub remote: Option<(&'a ServerRecord, Option<&'a TokenRecord>)>,
    /// The key for the vendor's package, when the switch was ever set.
    pub key: Option<CardState>,
    pub key_at: &'a str,
    pub knowledge: Option<&'a KnowledgeSignIn>,
    pub sources: usize,
}

/// A catalog product's card (Notion, Sanity, Figma, Sentry and the rest):
/// one status from its app sign-in, the vendor's remote server, its key and
/// its Knowledge sign-in.
pub fn catalog_product(id: &str, name: &str, w: &CatalogWays) -> ProductState {
    let kind = knowledge_kind(id);
    let mut ways = Vec::new();
    if let Some(k) = w.knowledge {
        ways.push(k.way());
    }
    if let Some(o) = w.own_app {
        let how = if o["source"] == "coderhelm" {
            format!("Coderhelm's {name} app")
        } else {
            format!("your own {name} app")
        };
        let st = own_app_state(o, &cs("not_connected", "", "connect"));
        ways.push(Way::new(&how, st, &[Use::Runs]).by(
            o["connected_by"].as_str().unwrap_or(""),
            o["connected_at"].as_str().unwrap_or(""),
        ));
    }
    if let Some((r, token)) = w.remote {
        let mut way = Way::new(
            &format!("{name}'s own server"),
            remote_state(r, token),
            &[Use::Runs],
        );
        if let Some(t) = token {
            way = way.by(&t.connected_by, &t.connected_at);
        }
        // A member's sign-in on Sanity's server is the one Knowledge syncs with.
        if kind == Some(SourceKind::Sanity) {
            way = way.extend(
                Use::Knowledge,
                "Knowledge can sync with this sign-in: add a source there.",
                Some(Action::link("Add a source in Knowledge", KNOWLEDGE_HREF)),
            );
        }
        ways.push(way);
    }
    if let Some(st) = &w.key {
        // Notion's and Sanity's saved keys also sync Knowledge's sources.
        let syncs = matches!(kind, Some(SourceKind::Notion | SourceKind::Sanity));
        let serves: &[Use] = if syncs && w.knowledge.is_none() && w.sources > 0 {
            &[Use::Runs, Use::Knowledge]
        } else {
            &[Use::Runs]
        };
        let mut way = Way::new("a key", st.clone(), serves).by("", w.key_at);
        if syncs {
            way = way.extend(
                Use::Knowledge,
                "Knowledge can sync with the same key: add a source there.",
                Some(Action::link("Add a source in Knowledge", KNOWLEDGE_HREF)),
            );
        }
        ways.push(way);
    }
    let needs: Vec<Need> = match kind {
        Some(SourceKind::Notion) => vec![
            Need {
                for_use: Use::Runs,
                detail: "Using Notion in runs needs Notion's own sign-in for agents, or a key: Notion grants that separately from Knowledge.".into(),
                action: Some(Action::new("connect", "Connect for runs")),
            },
            Need {
                for_use: Use::Knowledge,
                detail: "Syncing Notion into Knowledge needs the Notion sign-in Knowledge uses, or a key: Notion grants that separately.".into(),
                action: Some(Action::link("Connect in Knowledge", KNOWLEDGE_HREF)),
            },
        ],
        Some(SourceKind::Sanity) => vec![Need {
            for_use: Use::Runs,
            detail: "Using Sanity in runs needs Sanity connected here, so an admin can pick its tools.".into(),
            action: Some(Action::new("connect", "Connect for runs")),
        }],
        _ => Vec::new(),
    };
    let uses: &[Use] = if kind.is_some() {
        &[Use::Runs, Use::Knowledge]
    } else {
        &[Use::Runs]
    };
    product_state(uses, &ways, &needs)
}

/// The sign-ins the team made for Knowledge, by kind.
async fn knowledge_sign_ins(
    rt: &common::mcp_remote::runtime::Runtime,
    email: &str,
    sources: &[common::knowledge::sources::Source],
) -> Vec<KnowledgeSignIn> {
    let mut out = Vec::new();
    for kind in [
        SourceKind::Confluence,
        SourceKind::Notion,
        SourceKind::Sanity,
        SourceKind::GoogleDrive,
    ] {
        let id = common::knowledge::sources::oauth::connection_id(kind);
        let Ok(Some(server)) = rt.store.get_server(&rt.team, &id).await else {
            continue;
        };
        let owner = match server.sharing {
            Sharing::Personal => Owner::user(email),
            Sharing::Shared => Owner::Team,
        };
        // A record nobody finished signing in to is no connection.
        let Ok(Some(token)) = rt.store.get_token(&rt.team, &id, &owner).await else {
            continue;
        };
        out.push(KnowledgeSignIn {
            kind,
            server,
            token,
            sources: sources_of(sources, kind),
        });
    }
    out
}

fn sources_of(sources: &[common::knowledge::sources::Source], kind: SourceKind) -> usize {
    sources.iter().filter(|s| s.kind == Some(kind)).count()
}

/// Checks the account's role (read-only) and saves what it can do.
pub(crate) async fn check_and_save(
    state: &AppState,
    team_id: &str,
    a: &mut AwsAccount,
    actor: &str,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let target = a.target();
    let (c, perms) = tokio::join!(
        identity::check(&state.sts, &target, Vec::new(), actor, true),
        common::live::permissions::check(&state.sts, &target, actor, &now),
    );
    a.apply_check(&c, &now);
    // The role check is the same check as Test connection: its pass clears
    // what a failed test left ("The last test of the role failed").
    let table = &state.config.aws_insights_table_name;
    let sk = format!("{}{}", aws_account::PREFIX, a.account);
    if c.works {
        if a.status == "error" {
            let cleared = state
                .dynamo
                .update_item()
                .table_name(table)
                .key("pk", AttributeValue::S(team_id.into()))
                .key("sk", AttributeValue::S(sk.clone()))
                .condition_expression("attribute_exists(pk)")
                .update_expression("SET #s = :s REMOVE last_error, last_error_at")
                .expression_attribute_names("#s", "status")
                .expression_attribute_values(":s", AttributeValue::S("active".into()))
                .send()
                .await;
            if cleared.is_ok() {
                a.status = "active".into();
            }
        }
        common::problems::record_on(
            &state.dynamo,
            table,
            team_id,
            &sk,
            Check::AwsRole,
            Outcome::Pass,
            &now,
        )
        .await;
    } else {
        let why = format!("Coderhelm couldn't take on the role: {}", c.error);
        common::problems::record_on(
            &state.dynamo,
            table,
            team_id,
            &sk,
            Check::AwsRole,
            Outcome::Fail(Kind::AwsRoleRefused, why),
            &now,
        )
        .await;
    }
    if let Err(e) = aws_account::save_permissions(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team_id,
        &perms,
    )
    .await
    {
        warn!(error = %e, account = %a.account, "Could not save the permissions check");
    }
    a.permissions = Some(perms);
    if let Err(e) = aws_account::save_abilities(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team_id,
        a,
    )
    .await
    {
        warn!(error = %e, account = %a.account, "Could not save what the AWS role can do");
    }
}

/// Folds old separate metrics and Airflow connections into their AWS
/// accounts and removes them. Returns how many were folded.
async fn migrate_legacy(state: &AppState, team_id: &str, accounts: &mut [AwsAccount]) -> usize {
    let table = &state.config.settings_table_name;
    let legacy: Vec<Connector> = match connectors::list(&state.dynamo, table, team_id).await {
        Ok(l) => l.into_iter().filter(|c| c.kind().uses_aws()).collect(),
        Err(e) => {
            warn!(error = %e, "Could not read old connections to fold in");
            return 0;
        }
    };
    if legacy.is_empty() {
        return 0;
    }
    let now = chrono::Utc::now().to_rfc3339();
    let mut folded_kinds: Vec<(ConnKind, String)> = Vec::new();
    for a in accounts.iter_mut() {
        if a.fold_legacy(&legacy, &now) {
            if let Err(e) = aws_account::save_abilities(
                &state.dynamo,
                &state.config.aws_insights_table_name,
                team_id,
                a,
            )
            .await
            {
                warn!(error = %e, "Could not save folded abilities; keeping the old connections");
                continue;
            }
            for c in &legacy {
                if c.aws.as_ref().is_some_and(|t| t.account() == a.account) {
                    folded_kinds.push((c.kind(), a.account.clone()));
                }
            }
        }
    }
    let mut removed = 0;
    for (kind, account) in &folded_kinds {
        match connectors::delete(&state.dynamo, table, team_id, *kind, "").await {
            Ok(()) => {
                removed += 1;
                info!(
                    team_id,
                    kind = kind.as_str(),
                    account,
                    "Folded an old connection into its AWS account"
                );
            }
            Err(e) => warn!(error = %e, "Could not remove a folded connection"),
        }
    }
    removed
}

async fn get_item(state: &AppState, table: &str, key: &[(&str, &str)]) -> Option<Item> {
    let mut req = state.dynamo.get_item().table_name(table);
    for (k, v) in key {
        req = req.key(*k, attr_s(v));
    }
    req.send().await.ok().and_then(|o| o.item().cloned())
}

/// GET /api/integrations — every integration with its status. Checks each
/// AWS account's role when the last check is old, and folds old metrics and
/// Airflow connections into their accounts.
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    if let Err(code) = claims.require_role(1) {
        return Err(fail(code, "Ask a member of the team to open this page."));
    }
    let team = claims.team_id.clone();
    let can_edit = claims.holder().holds("integrations");

    // AWS accounts: fold old connections in, then check stale ones.
    let mut accounts =
        aws_account::list(&state.dynamo, &state.config.aws_insights_table_name, &team)
            .await
            .map_err(|e| internal("list aws accounts", e))?;
    let sources = with_knowledge_bases(&state, &team, &mut accounts).await;
    // A support view only reads: no migration, no checks into the team's
    // AWS accounts (they would run as the support admin).
    let support_view = claims.support_view.is_some();
    let migrated = if support_view {
        Default::default()
    } else {
        migrate_legacy(&state, &team, &mut accounts).await
    };
    let now = chrono::Utc::now();
    let actor = claims.email.clone();
    let checks = accounts
        .iter_mut()
        .filter(|a| !support_view && a.check_is_stale(now))
        .map(|a| check_and_save(&state, &team, a, &actor));
    futures::future::join_all(checks).await;

    // Everything else, read in parallel.
    let settings = &state.config.settings_table_name;
    // REST APIs and OpenSearch connected here before live in Data sources.
    if support_view {
    } else if let Err(e) =
        common::live::store::adopt_connections(&state.dynamo, settings, &team).await
    {
        warn!(error = %e, "Could not move connections to data sources");
    }
    let team_key = [("team_id", team.as_str()), ("sk", "META")];
    let jira_key = [("pk", team.as_str()), ("sk", "JIRA#config")];
    let (saved, github, jira_secret, jira_config, teams, plugins) = tokio::join!(
        connectors::list(&state.dynamo, settings, &team),
        get_item(&state, &state.config.teams_table_name, &team_key),
        super::api::load_jira_secret(&state, &team),
        get_item(&state, &state.config.jira_config_table_name, &jira_key),
        super::alerts::team_notify_settings(&state, &team),
        state
            .dynamo
            .query()
            .table_name(super::plugins::mcp_table(&state))
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", attr_s(&team))
            .expression_attribute_values(":p", attr_s("PLUGIN#"))
            .send(),
    );
    // Confluence through the team's Jira app shows as connected on its own.
    let saved = common::live::jira_app::with_jira_app(
        saved.map_err(|e| internal("list connectors", e))?,
        jira_config
            .as_ref()
            .and_then(common::live::jira_app::JiraApp::from_item)
            .as_ref(),
    );
    let plugin_items: Vec<Item> = plugins
        .map(|o| o.items().to_vec())
        .map_err(|e| internal("list plugins", e.to_string()))?;
    // Remote MCP servers, and this person's own connection to each.
    let rt = super::mcp_servers::runtime(&state, &team);
    let remotes = rt
        .store
        .list_servers(&team)
        .await
        .map_err(|e| internal("list mcp servers", e))?;
    let mut my_tokens = HashMap::new();
    for r in &remotes {
        let owner = r
            .owner_for(Some(&claims.email))
            .unwrap_or(common::mcp_remote::store::Owner::Team);
        if let Ok(Some(t)) = rt.store.get_token(&team, &r.id, &owner).await {
            my_tokens.insert(r.id.clone(), t);
        }
    }
    // The sign-ins made for Knowledge (kept apart from MCP servers): a
    // product signed in to there is connected here too.
    let knowledge = knowledge_sign_ins(&rt, &claims.email, &sources).await;
    let knowledge_of = |kind: SourceKind| knowledge.iter().find(|k| k.kind == kind);
    // Sign-ins nobody finished are removed once they expire, whether or
    // not the table expires records on its own.
    if !support_view {
        let swept = rt.sweep_pending().await;
        if swept > 0 {
            info!(team_id = %team, swept, "Expired sign-ins in progress removed");
        }
    }

    let mut items = Vec::new();

    // GitHub.
    let org = github
        .as_ref()
        .map(|i| s(i, "github_org"))
        .unwrap_or_default();
    let installed = github
        .as_ref()
        .and_then(|i| i.get("github_installation_id"))
        .is_some();
    items.push(card(
        "github",
        "GitHub",
        "Reads your code, opens pull requests, reviews them and follows CI.",
        if installed && !org.is_empty() {
            cs("connected", format!("Installed on {org}."), "manage")
        } else {
            cs("not_connected", "", "connect")
        },
        "",
        "/settings/integrations/github",
        json!({ "org": org }),
    ));

    // Jira.
    let forge = jira_config
        .as_ref()
        .is_some_and(|i| !s(i, "add_comment_url").is_empty());
    let jira_on = forge || jira_secret.is_some();
    items.push(card(
        "jira",
        "Jira",
        "Starts work from Jira tickets and keeps their status in step with the pull request.",
        if jira_on {
            cs("connected", "", "manage")
        } else {
            cs("not_connected", "", "connect")
        },
        "",
        "/settings/integrations/jira",
        json!({ "app": forge }),
    ));

    // AWS accounts: one card each (or one to connect the first).
    if accounts.is_empty() {
        items.push(card(
            "aws",
            "AWS",
            "Reads CloudWatch logs, CloudWatch metrics and Airflow (MWAA) through a read-only role in your account.",
            cs("not_connected", "", "connect"),
            "",
            "/settings/integrations/aws",
            json!({ "accounts": [] }),
        ));
    } else {
        for a in &accounts {
            let mut detail = a.public_json();
            detail["missing"] = json!(aws_missing(a));
            detail["update_url"] = json!(update_stack_link(a));
            detail["install_account"] = json!(install_account(a));
            detail["stack_name"] = json!(common::log_reader::update_stack_name_for(&a.chosen()));
            items.push(card(
                &format!("aws-{}", a.account),
                &format!("AWS account {}", a.account),
                "Reads CloudWatch logs, CloudWatch metrics and Airflow (MWAA) through a read-only role in your account.",
                aws_state(a),
                &a.abilities.checked_at,
                "/settings/integrations/aws",
                detail,
            ));
        }
    }

    // New Relic and Confluence. REST APIs and OpenSearch are data sources
    // (the Data sources page), with every other system agents read.
    for (kind, does) in [
        (
            ConnKind::NewRelic,
            "NRQL queries, entity search, deployment markers and change events, and response time, error rate and throughput before and after a deploy.",
        ),
        (
            ConnKind::Confluence,
            "Searches and reads Confluence pages. Optionally adds text to the end of pages you list.",
        ),
    ] {
        let mine: Vec<&Connector> = saved.iter().filter(|c| c.kind() == kind).collect();
        let mut shown: Vec<Option<&Connector>> = mine.iter().map(|c| Some(*c)).collect();
        if mine.is_empty() || connectors::nameable(kind) {
            shown.push(None);
        }
        for c in shown {
            let needs_name = kind == ConnKind::Rest || !mine.is_empty();
            let detail = c.map(|c| c.public_json()).unwrap_or_else(
                || json!({ "kind": kind.as_str(), "connected": false, "needs_name": needs_name }),
            );
            let (id, name) = match c {
                Some(c) => (c.id(), c.label()),
                // `kind:` so it never shares an id with a saved unnamed one.
                None if !mine.is_empty() => {
                    (format!("{}:", kind.as_str()), format!("Another {}", kind.label()))
                }
                None => (kind.as_str().to_string(), kind.label().to_string()),
            };
            if kind == ConnKind::Confluence {
                // One status from every way the team reaches Confluence:
                // the connection agents read with and the Knowledge sign-in.
                let k = knowledge_of(SourceKind::Confluence);
                let p = confluence_product(c, k, sources_of(&sources, SourceKind::Confluence));
                let mut detail = detail;
                detail["knowledge"] = json!(k.map(KnowledgeSignIn::json));
                items.push(product_card(
                    &id,
                    &name,
                    does,
                    &p,
                    c.map(|c| c.updated_at.as_str()).unwrap_or(""),
                    "",
                    detail,
                ));
                continue;
            }
            items.push(card(
                &id,
                &name,
                does,
                connector_state(c),
                c.map(|c| c.updated_at.as_str()).unwrap_or(""),
                "",
                detail,
            ));
        }
    }

    // Chat channels: the team's alert channel, per channel.
    {
        use common::notify::{store, ChannelKind};
        for (kind, does) in [
            (
                ChannelKind::Teams,
                "Posts alert cards, pull request links and recoveries to a Teams channel.",
            ),
            (
                ChannelKind::Slack,
                "Posts alerts, pull request links and recoveries to a Slack channel.",
            ),
        ] {
            let saved = store::stored(&teams.item, kind).is_set();
            items.push(card(
                kind.key(),
                kind.label(),
                does,
                match (saved, teams.enabled) {
                    (false, _) => cs("not_connected", "", "connect"),
                    (true, false) => cs(
                        "connected",
                        "A channel is saved but posting is off.",
                        "manage",
                    ),
                    (true, true) => cs("connected", "", "manage"),
                },
                "",
                "",
                json!({ "enabled": teams.enabled && saved, "has_webhook": saved }),
            ));
        }
    }

    // MCP servers.
    for p in super::plugins::catalog_json() {
        let id = p["id"].as_str().unwrap_or("").to_string();
        let rec = plugin_items
            .iter()
            .find(|i| s(i, "sk").strip_prefix("PLUGIN#") == Some(id.as_str()));
        let enabled = rec.and_then(|r| ob(r, "enabled"));
        let has_creds = rec.and_then(|r| ob(r, "has_credentials")).unwrap_or(false);
        let tested_at = rec.map(|r| s(r, "last_test_at")).unwrap_or_default();
        let creds_at = rec
            .map(|r| s(r, "credentials_updated_at"))
            .unwrap_or_default();
        let st = plugin_state(
            enabled,
            has_creds,
            rec.and_then(|r| ob(r, "last_test_ok")),
            &rec.map(|r| s(r, "last_test_message")).unwrap_or_default(),
            !tested_at.is_empty() && tested_at >= creds_at,
        );
        let custom_prompt = rec.map(|r| s(r, "custom_prompt")).unwrap_or_default();
        // The vendor's own remote server, when the team set it up.
        let remote = remotes.iter().find(|r| r.id == id && r.catalog_id == id);
        // A record for a vendor's server the catalog no longer offers
        // (Figma's takes only apps Figma approved) that was never
        // connected: kept, said on the card as unused, and not a second
        // sign-in beside the one that works.
        let offered = !p["remote_url"].as_str().unwrap_or("").is_empty();
        let (remote, unused) = match remote {
            Some(r) if !offered && remote_state(r, my_tokens.get(&r.id)).status != "connected" => (
                None,
                vec![json!({
                    "id": r.id,
                    "name": r.name,
                    "kind": "remote",
                    "note": format!(
                        "A sign-in through {}'s own server was started and never finished. Coderhelm doesn't use it.",
                        r.name
                    ),
                })],
            ),
            r => (r, Vec::new()),
        };
        let tested_at = match remote {
            Some(r) => r.tools_listed_at.clone(),
            None => tested_at,
        };
        // The sign-in with an app (CoderHelm's or the team's own): what
        // agents use first when it is there.
        let own = match common::mcp_remote::own_app::load(&rt, &id).await {
            Ok(o) => o,
            Err(e) => {
                warn!(error = %e, plugin = %id, "Could not read own app");
                None
            }
        };
        let own_json = match &own {
            Some(o) => Some(super::mcp_servers::own_app_json(&state, &rt, o).await),
            None => None,
        };
        // One status from every way the team reaches it: the app sign-in
        // agents use first, the vendor's server, the key, and the sign-in
        // made for Knowledge.
        let kind = knowledge_kind(&id);
        let k = kind.and_then(&knowledge_of);
        let product = catalog_product(
            &id,
            p["name"].as_str().unwrap_or(&id),
            &CatalogWays {
                own_app: own_json.as_ref(),
                remote: remote.map(|r| (r, my_tokens.get(&r.id))),
                key: enabled.is_some().then_some(st),
                key_at: &creds_at,
                knowledge: k,
                sources: kind.map_or(0, |kind| sources_of(&sources, kind)),
            },
        );
        // Where the team's own app sends people back, for its setup steps.
        let own_redirect = common::mcp_catalog::own_app(&id)
            .map(|_| super::mcp_servers::redirect_uri(&state.config.stage));
        // The one-click sign-in with CoderHelm's app, where it works here.
        let coderhelm_app =
            own_redirect.is_some() && super::mcp_servers::coderhelm_app_ready(&state, &id).await;
        let detail = json!({
            "own_app": own_json,
            "own_app_redirect_uri": own_redirect,
            "coderhelm_app": coderhelm_app,
            "remote": remote.map(|r| r.public_json(my_tokens.get(&r.id), can_edit)),
            "plugin": p,
            "enabled": enabled.unwrap_or(false),
            "has_credentials": has_creds,
            "custom_prompt": if can_edit { custom_prompt } else { String::new() },
            "last_test_ok": rec.and_then(|r| ob(r, "last_test_ok")),
            "last_test_message": rec.map(|r| s(r, "last_test_message")).unwrap_or_default(),
            "knowledge": k.map(KnowledgeSignIn::json),
            "unused": unused,
        });
        items.push(product_card(
            &id,
            p["name"].as_str().unwrap_or(&id),
            p["description"].as_str().unwrap_or(""),
            &product,
            &tested_at,
            "",
            detail,
        ));
    }

    // Google Drive is only ever synced into Knowledge: it has a card here
    // once the team signed in, so the sign-in is seen where sign-ins are.
    if let Some(k) = knowledge_of(SourceKind::GoogleDrive) {
        let p = product_state(&[Use::Knowledge], &[k.way()], &[]);
        items.push(product_card(
            "google_drive",
            "Google Drive",
            "Syncs the folders, Docs, Sheets, Slides and PDFs you pick into Knowledge. It only reads.",
            &p,
            "",
            KNOWLEDGE_HREF,
            json!({ "knowledge": k.json() }),
        ));
    }

    // Servers added by address.
    for r in remotes.iter().filter(|r| r.catalog_id.is_empty()) {
        let host = reqwest::Url::parse(&r.url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_string))
            .unwrap_or_default();
        items.push(card(
            &format!("mcp:{}", r.id),
            &r.name,
            &format!("A remote MCP server at {host}. Agents get only the tools you allow."),
            remote_state(r, my_tokens.get(&r.id)),
            &r.tools_listed_at,
            "",
            json!({ "remote": r.public_json(my_tokens.get(&r.id), can_edit) }),
        ));
    }

    Ok(Json(json!({
        "can_edit": can_edit,
        "items": items,
        "migrated": migrated,
    })))
}

fn account_of(raw: &str) -> Result<&str, (StatusCode, Json<Value>)> {
    if raw.len() == 12 && raw.chars().all(|c| c.is_ascii_digit()) {
        Ok(raw)
    } else {
        Err(fail(StatusCode::NOT_FOUND, "No such AWS account."))
    }
}

async fn load_account(
    state: &AppState,
    team_id: &str,
    account: &str,
) -> Result<AwsAccount, (StatusCode, Json<Value>)> {
    let kbs = common::knowledge::remote::arns_in(&team_bases(state, team_id).await, account);
    let buckets =
        common::knowledge::sources::s3::buckets_in(&team_sources(state, team_id).await, account);
    aws_account::list(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team_id,
    )
    .await
    .map_err(|e| internal("list aws accounts", e))?
    .into_iter()
    .find(|a| a.account == account)
    .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That AWS account isn't connected."))
    .map(|mut a| {
        a.knowledge_bases = kbs;
        a.import_buckets = buckets;
        a
    })
}

/// Each account's connected knowledge bases and import buckets, so its
/// update link keeps the role able to search and read them.
async fn with_knowledge_bases(
    state: &AppState,
    team_id: &str,
    accounts: &mut [AwsAccount],
) -> Vec<common::knowledge::sources::Source> {
    let bases = team_bases(state, team_id).await;
    let sources = team_sources(state, team_id).await;
    for a in accounts {
        a.knowledge_bases = common::knowledge::remote::arns_in(&bases, &a.account);
        a.import_buckets = common::knowledge::sources::s3::buckets_in(&sources, &a.account);
    }
    sources
}

/// Every knowledge source of the team (the buckets it imports from).
async fn team_sources(state: &AppState, team_id: &str) -> Vec<common::knowledge::sources::Source> {
    common::knowledge::sources::store::team_sources(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await
    .unwrap_or_default()
}

async fn team_bases(state: &AppState, team_id: &str) -> Vec<common::knowledge::Base> {
    use common::knowledge::search::Store;
    super::knowledge::store(state)
        .bases(team_id)
        .await
        .unwrap_or_default()
}

fn account_reply(a: &AwsAccount) -> Value {
    let mut detail = a.public_json();
    detail["missing"] = json!(aws_missing(a));
    detail["update_url"] = json!(update_stack_link(a));
    detail["install_account"] = json!(install_account(a));
    detail["stack_name"] = json!(common::log_reader::update_stack_name_for(&a.chosen()));
    let st = aws_state(a);
    json!({ "account": detail, "status": st.status, "reason": st.reason, "last_check": a.abilities.checked_at })
}

/// POST /api/aws-connections/:id/abilities/check — checks now what the role
/// can do (read-only) and saves it.
pub async fn check_aws(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    if let Err(code) = claims.require_role(3) {
        return Err(fail(code, "Only admins can check integrations."));
    }
    let account = account_of(&id)?;
    let mut a = load_account(&state, &claims.team_id, account).await?;
    check_and_save(&state, &claims.team_id, &mut a, &claims.email).await;
    Ok(Json(account_reply(&a)))
}

/// POST /api/aws-connections/:id/permissions/check — checks the role
/// against this release's permissions, live, and saves the result. With
/// `{"pressed": true}` (someone pressed Update), a missing or failed update
/// stack comes back as the problem to show.
pub async fn check_aws_permissions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    body: Option<Json<Value>>,
) -> Reply {
    if let Err(code) = claims.require_role(3) {
        return Err(fail(code, "Only admins can check integrations."));
    }
    let pressed = body
        .as_ref()
        .and_then(|b| b.get("pressed"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let account = account_of(&id)?;
    let mut a = load_account(&state, &claims.team_id, account).await?;
    check_and_save(&state, &claims.team_id, &mut a, &claims.email).await;
    let mut reply = account_reply(&a);
    reply["permissions"] = a
        .permissions
        .as_ref()
        // With the team's choices, like every other read of the account: the
        // update link keeps their knowledge bases, buckets and reads.
        .map(|p| {
            p.view_with(
                pressed,
                &common::public_urls::log_reader_template_url(),
                &a.chosen(),
            )
        })
        .unwrap_or(Value::Null);
    Ok(Json(reply))
}

/// What an admin may change about an account's abilities.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AbilityChange {
    pub metrics: Option<bool>,
    pub airflow: Option<bool>,
    /// Infrastructure vulnerabilities: may be switched on before the role
    /// has the reads (the one-click update then asks for them).
    pub infrastructure: Option<bool>,
    /// Environments agents may read; the rest of what was found is skipped.
    pub airflow_environments: Option<Vec<String>>,
}

impl AbilityChange {
    pub fn parse(body: &Value) -> Result<AbilityChange, String> {
        let flag = |k: &str| -> Result<Option<bool>, String> {
            match body.get(k) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::Bool(b)) => Ok(Some(*b)),
                Some(_) => Err(format!("{k} must be true or false.")),
            }
        };
        let envs = match body.get("airflow_environments") {
            None | Some(Value::Null) => None,
            Some(Value::Array(l)) if l.len() <= 50 => Some(
                l.iter()
                    .map(|v| {
                        v.as_str()
                            .map(str::to_string)
                            .ok_or("Environment names are text.")
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some(_) => return Err("airflow_environments must be a list.".into()),
        };
        Ok(AbilityChange {
            metrics: flag("metrics")?,
            airflow: flag("airflow")?,
            infrastructure: flag("infrastructure")?,
            airflow_environments: envs,
        })
    }

    /// Applies the change. Environments must be ones the check found.
    pub fn apply(&self, a: &mut AwsAccount) -> Result<(), String> {
        if let Some(m) = self.metrics {
            if m && a.abilities.can_metrics != Some(true) {
                return Err("The role can't read CloudWatch metrics yet. Add the permissions with the stack link, then check again.".into());
            }
            a.abilities.metrics_off = !m;
        }
        if let Some(on) = self.airflow {
            if on && a.abilities.can_airflow != Some(true) {
                return Err("The role can't read Airflow yet. Add the permissions with the stack link, then check again.".into());
            }
            a.abilities.airflow_off = !on;
        }
        if let Some(on) = self.infrastructure {
            a.abilities.infra_on = on;
        }
        if let Some(keep) = &self.airflow_environments {
            if let Some(bad) = keep.iter().find(|e| !a.abilities.airflow_found.contains(e)) {
                return Err(format!(
                    "{bad} isn't an Airflow environment the role can see."
                ));
            }
            a.abilities.airflow_skip = a
                .abilities
                .airflow_found
                .iter()
                .filter(|e| !keep.contains(e))
                .cloned()
                .collect();
        }
        Ok(())
    }
}

/// PUT /api/aws-connections/:id/abilities — switches metrics or Airflow on or
/// off for the account, and picks which found Airflow environments agents
/// may read.
pub async fn set_aws_abilities(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    if let Err(code) = claims.require_role(3) {
        return Err(fail(code, "Only admins can change integrations."));
    }
    let account = account_of(&id)?;
    let change = AbilityChange::parse(&body).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let mut a = load_account(&state, &claims.team_id, account).await?;
    change
        .apply(&mut a)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    aws_account::save_abilities(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        &claims.team_id,
        &a,
    )
    .await
    .map_err(|e| internal("save aws abilities", e))?;
    info!(team_id = %claims.team_id, account, metrics = a.metrics_on(), airflow = ?a.airflow_environments(), "AWS abilities changed");
    Ok(Json(account_reply(&a)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_own_app_sign_in_is_the_cards_state() {
        let key_off = plugin_state(None, false, None, "", false);
        let key_on = plugin_state(Some(true), true, Some(true), "", true);
        let own = |status: &str, reason: &str| json!({ "status": status, "status_reason": reason });
        let st = own_app_state(&own("connected", ""), &key_off);
        assert_eq!((st.status, st.action), ("connected", "manage"));
        let st = own_app_state(&own("needs_reconnect", "Figma refused it."), &key_on);
        assert_eq!(
            (st.status, st.reason.as_str()),
            ("needs_reconnect", "Figma refused it.")
        );
        // Saved but not signed in: a working key still stands.
        assert_eq!(own_app_state(&own("not_connected", ""), &key_on), key_on);
        let st = own_app_state(&own("not_connected", ""), &key_off);
        assert_eq!((st.status, st.action), ("not_connected", "connect"));
    }

    #[test]
    fn a_sign_in_with_coderhelms_app_says_so() {
        let key_off = plugin_state(None, false, None, "", false);
        let ours =
            |status: &str| json!({ "status": status, "status_reason": "", "source": "coderhelm" });
        let st = own_app_state(&ours("connected"), &key_off);
        assert_eq!(st.reason, "Signed in with Coderhelm's app.");
        let st = own_app_state(&ours("needs_reconnect"), &key_off);
        assert_eq!((st.status, st.action), ("needs_reconnect", "reconnect"));
        assert!(!st.reason.contains("own app"));
        // Started but not finished: nothing of "your own app" on the card.
        let st = own_app_state(&ours("not_connected"), &key_off);
        assert_eq!((st.status, st.reason.as_str()), ("not_connected", ""));
    }
    use common::live::identity::RoleCheck;

    fn account() -> AwsAccount {
        let mut a = AwsAccount {
            account: "123456789012".into(),
            role_arn: "arn:aws:iam::123456789012:role/CoderHelmLogReader".into(),
            external_id: "x".into(),
            region: "us-east-1".into(),
            status: "active".into(),
            ..Default::default()
        };
        a.apply_check(
            &RoleCheck {
                works: true,
                can_read_logs: Some(true),
                can_read_metrics: Some(true),
                can_read_airflow: Some(true),
                airflow_environments: vec!["prod".into(), "dev".into()],
                ..Default::default()
            },
            "2026-10-19T10:00:00Z",
        );
        a
    }

    #[test]
    fn sections_follow_the_page() {
        assert_eq!(section_of("github"), "code");
        assert_eq!(section_of("jira"), "code");
        assert_eq!(section_of("aws"), "cloud");
        assert_eq!(section_of("newrelic"), "monitoring");
        assert_eq!(section_of("sentry"), "monitoring");
        for d in ["confluence", "notion", "figma", "sanity"] {
            assert_eq!(section_of(d), "docs");
        }
        assert_eq!(section_of("teams"), "chat");
        assert_eq!(section_of("slack"), "chat");
        assert_eq!(section_of("linear"), "other");
        assert_eq!(section_of("mcp:acme-wiki"), "mcp");
        assert_eq!(section_of("atlassian"), "code");
    }

    #[test]
    fn a_plugin_switched_on_without_credentials_needs_them() {
        let st = plugin_state(Some(true), false, None, "", false);
        assert_eq!(st.status, "needs_credentials");
        assert_eq!(st.action, "add_credentials");
        assert_eq!(
            plugin_state(None, false, None, "", false).status,
            "not_connected"
        );
        assert_eq!(
            plugin_state(Some(false), true, None, "", false).action,
            "connect"
        );
        assert_eq!(
            plugin_state(Some(true), true, None, "", false).status,
            "connected"
        );
        let bad = plugin_state(Some(true), true, Some(false), "401 Unauthorized", true);
        assert_eq!(bad.status, "error");
        assert!(bad.reason.contains("401"));
        assert_eq!(bad.action, "check_again");
        // New credentials since the failed test: not an error any more.
        assert_eq!(
            plugin_state(Some(true), true, Some(false), "401", false).status,
            "connected"
        );
    }

    #[test]
    fn connector_states() {
        assert_eq!(connector_state(None).status, "not_connected");
        let mut c = Connector::new(ConnKind::NewRelic);
        assert_eq!(connector_state(Some(&c)).status, "needs_credentials");
        c.key_sealed = "sealed".into();
        c.status = "error".into();
        c.status_message = "Bad key.".into();
        let st = connector_state(Some(&c));
        assert_eq!(
            (st.status, st.reason.as_str(), st.action),
            ("error", "Bad key.", "check_again")
        );
        c.status = "ready".into();
        c.enabled = true;
        assert_eq!(connector_state(Some(&c)), cs("connected", "", "manage"));
    }

    #[test]
    fn confluence_through_the_jira_app_is_connected_without_a_key() {
        let app = common::live::jira_app::JiraApp {
            site: "https://yourcompany.atlassian.net".into(),
            confluence_url: "https://example.invalid/x1/confluence".into(),
            secret: "s".into(),
        };
        let c = app.default_confluence();
        let st = connector_state(Some(&c));
        assert_eq!(
            (st.status, st.reason.as_str()),
            ("connected", "Connected through the Jira app.")
        );
        let paused = Connector {
            enabled: false,
            ..c
        };
        assert!(connector_state(Some(&paused)).reason.contains("paused"));
    }

    #[test]
    fn an_aws_account_that_cannot_be_reached_says_why() {
        let mut a = account();
        assert_eq!(aws_state(&a).status, "connected");
        a.abilities.check_error = "AccessDenied".into();
        let st = aws_state(&a);
        assert_eq!(st.status, "error");
        assert!(st.reason.contains("AccessDenied"));
    }

    #[test]
    fn missing_abilities_offer_the_stack() {
        let mut a = account();
        assert!(aws_missing(&a).is_empty());
        a.abilities.can_metrics = Some(false);
        a.abilities.can_airflow = Some(false);
        assert_eq!(aws_missing(&a), vec!["metrics", "airflow"]);
        let r = account_reply(&a);
        assert!(r["account"]["update_url"]
            .as_str()
            .unwrap()
            .contains("CoderHelmLogReaderUpdate-"));
        assert!(account_reply(&account())["account"]["update_url"].is_null());
    }

    #[test]
    fn admins_switch_abilities_and_pick_environments() {
        let mut a = account();
        AbilityChange::parse(&json!({ "metrics": false, "airflow_environments": ["prod"] }))
            .unwrap()
            .apply(&mut a)
            .unwrap();
        assert!(!a.metrics_on());
        assert_eq!(a.airflow_environments(), vec!["prod"]);
        assert_eq!(a.abilities.airflow_skip, vec!["dev"]);
        AbilityChange::parse(&json!({ "metrics": true, "airflow": false }))
            .unwrap()
            .apply(&mut a)
            .unwrap();
        assert!(a.metrics_on());
        assert!(!a.airflow_on());
    }

    #[test]
    fn abilities_the_role_lacks_cannot_be_switched_on() {
        let mut a = account();
        a.abilities.can_metrics = Some(false);
        let e = AbilityChange::parse(&json!({ "metrics": true }))
            .unwrap()
            .apply(&mut a)
            .unwrap_err();
        assert!(e.contains("can't read CloudWatch metrics"));
        let e = AbilityChange::parse(&json!({ "airflow_environments": ["typed-name"] }))
            .unwrap()
            .apply(&mut account())
            .unwrap_err();
        assert!(e.contains("typed-name"), "only discovered environments");
        assert!(AbilityChange::parse(&json!({ "metrics": "yes" })).is_err());
        assert!(AbilityChange::parse(&json!({ "airflow_environments": "prod" })).is_err());
    }

    // ── One status per product, from every record the team has ──────────

    fn token(status: Status, reason: &str) -> TokenRecord {
        TokenRecord {
            owner: Owner::Team,
            sealed: String::new(),
            expires_at: None,
            scope: String::new(),
            version: 1,
            status,
            status_reason: reason.into(),
            step_up: vec![],
            connected_by: "ada@example.com".into(),
            connected_at: "2026-11-11T10:00:00Z".into(),
        }
    }

    /// The sign-in Knowledge made for `kind` (`MCPSERVER#ks-<kind>` and its
    /// `MCPTOKEN`).
    fn signed_in(kind: SourceKind, status: Status) -> KnowledgeSignIn {
        let id = common::knowledge::sources::oauth::connection_id(kind);
        let mut server = ServerRecord::new(
            &id,
            &format!("{} (knowledge)", kind.label()),
            "https://auth.example.com",
        );
        server.catalog_id = common::knowledge::sources::oauth::catalog_id(kind);
        server.auth = common::mcp_remote::store::AuthKind::OAuth;
        if kind == SourceKind::Sanity {
            server.sharing = Sharing::Personal;
        }
        let reason = if status == Status::Connected {
            ""
        } else {
            "The provider ended the sign-in."
        };
        KnowledgeSignIn {
            kind,
            server,
            token: token(status, reason),
            sources: 0,
        }
    }

    fn confluence_token() -> Connector {
        Connector {
            enabled: true,
            status: "ready".into(),
            key_sealed: "sealed".into(),
            site: "https://example.atlassian.net".into(),
            email: "bot@example.com".into(),
            updated_by: "grace@example.com".into(),
            updated_at: "2026-11-01T09:00:00Z".into(),
            ..Connector::new(ConnKind::Confluence)
        }
    }

    fn jira_app_confluence() -> Connector {
        common::live::jira_app::JiraApp {
            site: "https://example.atlassian.net".into(),
            confluence_url: "https://example.invalid/x1/confluence".into(),
            secret: "s".into(),
        }
        .default_confluence()
    }

    fn use_state(p: &ProductState, u: Use) -> &str {
        p.uses.iter().find(|s| s.of == u).unwrap().state
    }

    /// The report: a connected-app record for Confluence as a knowledge
    /// source and its team sign-in, no saved connector, and a Jira app that
    /// can't reach Confluence. The card read "Not connected".
    #[test]
    fn confluence_signed_in_for_knowledge_only_is_connected() {
        let k = signed_in(SourceKind::Confluence, Status::Connected);
        let p = confluence_product(None, Some(&k), 0);
        assert_eq!((p.card.status, p.card.action), ("connected", "manage"));
        assert_eq!(
            p.card.reason,
            "Connected for Knowledge. The same sign-in can read pages during runs and reviews. Turn it on: no second sign-in."
        );
        assert_eq!(use_state(&p, Use::Knowledge), "on");
        assert_eq!(use_state(&p, Use::Runs), "ready");
        assert_eq!(p.how, "the team's Atlassian sign-in");
        assert_eq!(p.connected_by, "ada@example.com");
        assert_eq!(p.connected_at, "2026-11-11T10:00:00Z");
    }

    #[test]
    fn confluence_in_every_combination_of_its_records() {
        let k_ok = signed_in(SourceKind::Confluence, Status::Connected);
        let k_renew = signed_in(SourceKind::Confluence, Status::NeedsReconnect);
        let token = confluence_token();
        let app = jira_app_confluence();
        let signed = Connector {
            enabled: true,
            status: "ready".into(),
            via: common::live::connectors::VIA_SIGN_IN.into(),
            site: "https://example.atlassian.net".into(),
            ..Connector::new(ConnKind::Confluence)
        };

        // Nothing at all.
        let p = confluence_product(None, None, 0);
        assert_eq!((p.card.status, p.card.action), ("not_connected", "connect"));

        // An API token only: runs work, Knowledge can use the same token.
        let p = confluence_product(Some(&token), None, 0);
        assert_eq!(p.card.status, "connected");
        assert_eq!(
            p.card.reason,
            "Connected for runs and reviews. Knowledge can sync with the same token: add a source there."
        );
        assert_eq!(use_state(&p, Use::Knowledge), "ready");
        assert_eq!(p.connected_by, "grace@example.com");
        // Once a source syncs with that token, both uses are on.
        let p = confluence_product(Some(&token), None, 2);
        assert_eq!(p.card.reason, "Used in runs and reviews, and in Knowledge.");

        // The Jira app only: Knowledge needs a different grant, said exactly.
        let p = confluence_product(Some(&app), None, 0);
        assert_eq!(p.card.status, "connected");
        assert!(
            p.card.reason.starts_with("Connected for runs and reviews."),
            "{}",
            p.card.reason
        );
        assert!(
            p.card.reason.contains(
                "Syncing pages into Knowledge needs an Atlassian sign-in or an API token"
            ),
            "{}",
            p.card.reason
        );
        assert_eq!(use_state(&p, Use::Knowledge), "needs");

        // Token and Knowledge sign-in, the Jira app and Knowledge sign-in.
        for c in [&token, &app] {
            let p = confluence_product(Some(c), Some(&k_ok), 1);
            assert_eq!(p.card.status, "connected");
            assert!(
                p.card
                    .reason
                    .starts_with("Used in runs and reviews, and in Knowledge."),
                "{}",
                p.card.reason
            );
        }

        // Runs switched on with the Knowledge sign-in: one sign-in, both uses.
        let p = confluence_product(Some(&signed), Some(&k_ok), 1);
        assert_eq!(p.card.status, "connected");
        assert_eq!(
            p.card.reason,
            "Used in runs and reviews, and in Knowledge. Agents read pages with the team's Atlassian sign-in."
        );
        // That sign-in ends: both uses need it renewed, with one action.
        let p = confluence_product(Some(&signed), Some(&k_renew), 1);
        assert_eq!(
            (p.card.status, p.card.action),
            ("needs_reconnect", "reconnect")
        );
        assert_eq!(p.card.reason, "The provider ended the sign-in.");
        // ... or was removed from under the connection.
        let p = confluence_product(Some(&signed), None, 0);
        assert_eq!(p.card.status, "needs_reconnect");
        assert!(p.card.reason.contains("isn't signed in to Confluence"));

        // The Knowledge sign-in needs renewing while a token still works:
        // attention, never "not connected".
        let p = confluence_product(Some(&token), Some(&k_renew), 1);
        assert_eq!(
            (p.card.status, p.card.action),
            ("needs_reconnect", "reconnect")
        );
        assert_eq!(
            p.card.reason,
            "Runs and reviews still work. Knowledge: The provider ended the sign-in."
        );

        // Only a Knowledge sign-in that needs renewing.
        let p = confluence_product(None, Some(&k_renew), 0);
        assert_eq!(p.card.status, "needs_reconnect");
        assert_eq!(use_state(&p, Use::Runs), "off");

        // A token Confluence refuses beside a working Knowledge sign-in.
        let bad = Connector {
            status: "error".into(),
            status_message: "Confluence didn't accept the email and token.".into(),
            ..confluence_token()
        };
        let p = confluence_product(Some(&bad), Some(&k_ok), 1);
        assert_eq!((p.card.status, p.card.action), ("error", "check_again"));
        assert!(
            p.card.reason.starts_with("Knowledge still works."),
            "{}",
            p.card.reason
        );
    }

    fn remote(id: &str, name: &str, status: Status) -> ServerRecord {
        let mut r = ServerRecord::new(id, name, &format!("https://mcp.{id}.example/mcp"));
        r.catalog_id = id.into();
        r.auth = common::mcp_remote::store::AuthKind::OAuth;
        r.status = status;
        r.allowed = ["search".to_string()].into();
        r.approved = r.allowed.clone();
        r
    }

    fn key_on() -> CardState {
        plugin_state(Some(true), true, Some(true), "", true)
    }

    #[test]
    fn notion_in_every_combination_of_its_records() {
        let k = signed_in(SourceKind::Notion, Status::Connected);
        let r = remote("notion", "Notion", Status::Connected);
        let r_off = remote("notion", "Notion", Status::NotConnected);
        let ways = |own: Option<(&ServerRecord, Option<&TokenRecord>)>,
                    key: Option<CardState>,
                    knowledge: Option<&KnowledgeSignIn>,
                    sources: usize| {
            let w = CatalogWays {
                remote: own,
                key,
                knowledge,
                sources,
                ..Default::default()
            };
            catalog_product("notion", "Notion", &w)
        };

        assert_eq!(ways(None, None, None, 0).card.status, "not_connected");

        // Signed in for Knowledge only: connected, and runs really do need
        // another grant (Notion's sign-in for agents is a different one).
        let p = ways(None, None, Some(&k), 1);
        assert_eq!(p.card.status, "connected");
        assert_eq!(
            p.card.reason,
            "Connected for Knowledge. Using Notion in runs needs Notion's own sign-in for agents, or a key: Notion grants that separately from Knowledge."
        );
        assert_eq!(use_state(&p, Use::Runs), "needs");
        assert_eq!(p.how, "the team's Notion sign-in");
        // A sign-in for agents that was started and never finished changes nothing.
        let p2 = ways(Some((&r_off, None)), None, Some(&k), 1);
        assert_eq!(p2.card, p.card);

        // Notion's own server only: runs work, Knowledge needs its own grant.
        let p = ways(Some((&r, None)), None, None, 0);
        assert_eq!(p.card.status, "connected");
        assert!(p.card.reason.starts_with("Connected for runs and reviews."));
        assert!(p
            .card
            .reason
            .contains("Syncing Notion into Knowledge needs"));
        assert_eq!(use_state(&p, Use::Knowledge), "needs");

        // A key only: it serves runs and Knowledge can use it too.
        let p = ways(None, Some(key_on()), None, 0);
        assert_eq!(use_state(&p, Use::Knowledge), "ready");
        assert!(p
            .card
            .reason
            .contains("Knowledge can sync with the same key"));
        let p = ways(None, Some(key_on()), None, 3);
        assert_eq!(p.card.reason, "Used in runs and reviews, and in Knowledge.");

        // Everything: one card, both uses on.
        let p = ways(Some((&r, None)), Some(key_on()), Some(&k), 1);
        assert_eq!(p.card.status, "connected");
        assert_eq!(p.card.reason, "Used in runs and reviews, and in Knowledge.");
        assert_eq!(p.how, "Notion's own server");

        // A sign-in started and never finished doesn't hide a key that works.
        let p = ways(Some((&r_off, None)), Some(key_on()), None, 0);
        assert_eq!(p.card.status, "connected");
        assert_eq!(p.how, "a key");
    }

    #[test]
    fn sanity_in_every_combination_of_its_records() {
        let mine = signed_in(SourceKind::Sanity, Status::Connected);
        let mut r = remote("sanity", "Sanity", Status::Connected);
        r.sharing = Sharing::Personal;
        let my_token = TokenRecord {
            owner: Owner::user("ada@example.com"),
            ..token(Status::Connected, "")
        };
        let product = |w: CatalogWays| catalog_product("sanity", "Sanity", &w);

        // A member's sign-in for Knowledge only.
        let p = product(CatalogWays {
            knowledge: Some(&mine),
            ..Default::default()
        });
        assert_eq!(p.card.status, "connected");
        assert_eq!(p.how, "your Sanity sign-in");
        assert!(p.card.reason.starts_with("Connected for Knowledge."));
        assert!(p
            .card
            .reason
            .contains("Using Sanity in runs needs Sanity connected here"));

        // The connected app only, signed in to by this person: runs work
        // and the same sign-in syncs Knowledge.
        let p = product(CatalogWays {
            remote: Some((&r, Some(&my_token))),
            ..Default::default()
        });
        assert_eq!(p.card.status, "connected");
        assert_eq!(use_state(&p, Use::Knowledge), "ready");
        assert!(p
            .card
            .reason
            .contains("Knowledge can sync with this sign-in"));

        // The connected app that this person hasn't signed in to yet, and
        // their Knowledge sign-in: connected, never "not connected".
        let p = product(CatalogWays {
            remote: Some((&r, None)),
            knowledge: Some(&mine),
            ..Default::default()
        });
        assert_eq!(p.card.status, "connected");
        assert_eq!(use_state(&p, Use::Knowledge), "on");

        // A key and a Knowledge sign-in that needs renewing.
        let renew = signed_in(SourceKind::Sanity, Status::NeedsReconnect);
        let p = product(CatalogWays {
            key: Some(key_on()),
            knowledge: Some(&renew),
            ..Default::default()
        });
        assert_eq!(p.card.status, "needs_reconnect");
        assert!(p.card.reason.starts_with("Runs and reviews still work."));
    }

    /// Figma: a sign-in through Coderhelm's app (`MCPSERVER#app-figma`), an
    /// older record for Figma's own server (`MCPSERVER#figma`) and the old
    /// switch (`PLUGIN#figma`). One card.
    #[test]
    fn figma_with_several_records_is_one_card() {
        let ours = |status: &str| {
            json!({ "status": status, "status_reason": "", "source": "coderhelm",
                    "connected_by": "ada@example.com", "connected_at": "2026-11-12T08:00:00Z" })
        };
        let product = |w: CatalogWays| catalog_product("figma", "Figma", &w);
        let stale = remote("figma", "Figma", Status::NotConnected);
        let old_switch = plugin_state(Some(false), false, None, "", false);

        // Signed in with Coderhelm's app; the other records change nothing.
        let app = ours("connected");
        let p = product(CatalogWays {
            own_app: Some(&app),
            remote: Some((&stale, None)),
            key: Some(old_switch.clone()),
            ..Default::default()
        });
        assert_eq!(
            p.card,
            cs("connected", "Signed in with Coderhelm's app.", "manage")
        );
        assert_eq!(p.how, "Coderhelm's Figma app");
        assert_eq!(p.connected_by, "ada@example.com");
        assert_eq!(p.uses.len(), 1, "Figma has one use");

        // A sign-in with the app started and never finished, beside the
        // other unfinished records: not connected, one Connect.
        let pending = ours("not_connected");
        let p = product(CatalogWays {
            own_app: Some(&pending),
            remote: Some((&stale, None)),
            key: Some(old_switch),
            ..Default::default()
        });
        assert_eq!(p.card, cs("not_connected", "", "connect"));

        // The app sign-in has to be renewed: that is what agents use, so it
        // shows even with a key saved.
        let renew = ours("needs_reconnect");
        let p = product(CatalogWays {
            own_app: Some(&renew),
            key: Some(key_on()),
            ..Default::default()
        });
        assert_eq!(
            (p.card.status, p.card.action),
            ("needs_reconnect", "reconnect")
        );

        // The team's own app, saved but not signed in to, and a key that works.
        let own = json!({ "status": "not_connected", "status_reason": "", "source": "own" });
        let p = product(CatalogWays {
            own_app: Some(&own),
            key: Some(key_on()),
            ..Default::default()
        });
        assert_eq!(p.card.status, "connected");
        assert_eq!(p.how, "a key");
        let p = product(CatalogWays {
            own_app: Some(&own),
            ..Default::default()
        });
        assert_eq!(
            p.card.reason,
            "Your own app is saved. Connect it to sign in."
        );
    }

    /// Sentry: the vendor's server and a key.
    #[test]
    fn sentry_with_a_sign_in_and_a_key() {
        let product = |w: CatalogWays| catalog_product("sentry", "Sentry", &w);
        let r = remote("sentry", "Sentry", Status::Connected);
        let failed = remote("sentry", "Sentry", Status::Failed);
        let p = product(CatalogWays {
            remote: Some((&r, None)),
            key: Some(plugin_state(Some(true), false, None, "", false)),
            ..Default::default()
        });
        assert_eq!(p.card, cs("connected", "", "manage"));
        assert_eq!(p.uses.len(), 1);
        // The sign-in agents would use stopped working: it shows.
        let p = product(CatalogWays {
            remote: Some((&failed, None)),
            key: Some(key_on()),
            ..Default::default()
        });
        assert_eq!((p.card.status, p.card.action), ("error", "check_again"));
        // Switched on with no credentials and no sign-in.
        let p = product(CatalogWays {
            key: Some(plugin_state(Some(true), false, None, "", false)),
            ..Default::default()
        });
        assert_eq!(
            (p.card.status, p.card.action),
            ("needs_credentials", "add_credentials")
        );
        // No tools allowed yet is still said on the card.
        let mut bare = remote("sentry", "Sentry", Status::Connected);
        bare.allowed.clear();
        let p = product(CatalogWays {
            remote: Some((&bare, None)),
            ..Default::default()
        });
        assert_eq!(
            p.card.reason,
            "No tools are allowed yet, so agents don't get it."
        );
    }

    #[test]
    fn google_drive_signed_in_for_knowledge_has_a_card() {
        let k = signed_in(SourceKind::GoogleDrive, Status::Connected);
        let p = product_state(&[Use::Knowledge], &[k.way()], &[]);
        let v = product_card(
            "google_drive",
            "Google Drive",
            "",
            &p,
            "",
            KNOWLEDGE_HREF,
            json!({}),
        );
        assert_eq!(v["status"], "connected");
        assert_eq!(v["section"], "docs");
        assert_eq!(v["how"], "the team's Google sign-in");
        assert_eq!(v["connected_by"], "ada@example.com");
        assert_eq!(v["uses"][0]["id"], "knowledge");
        assert_eq!(v["uses"][0]["state"], "on");
        let renew = signed_in(SourceKind::GoogleDrive, Status::NeedsReconnect);
        let p = product_state(&[Use::Knowledge], &[renew.way()], &[]);
        assert_eq!(
            (p.card.status, p.card.action),
            ("needs_reconnect", "reconnect")
        );
        // Every card carries the same fields, filled in or not.
        let plain = card(
            "github",
            "GitHub",
            "",
            cs("connected", "", "manage"),
            "",
            "",
            json!({}),
        );
        assert_eq!(plain["uses"], json!([]));
        assert_eq!(plain["connected_by"], "");
    }

    #[test]
    fn a_knowledge_sign_in_says_whose_it_is() {
        let json = signed_in(SourceKind::Confluence, Status::Connected).json();
        assert_eq!(json["server_id"], "ks-confluence");
        assert_eq!(json["kind"], "confluence");
        assert_eq!(json["status"], "connected");
        assert_eq!(json["personal"], false);
        assert_eq!(
            signed_in(SourceKind::Sanity, Status::Connected).json()["personal"],
            true
        );
        for id in ["confluence", "notion", "sanity", "google_drive"] {
            assert!(knowledge_kind(id).is_some(), "{id}");
        }
        assert!(knowledge_kind("figma").is_none());
        assert!(knowledge_kind("sentry").is_none());
    }

    #[test]
    fn remote_servers_show_the_viewers_own_connection() {
        use common::mcp_remote::store::{
            AuthKind, Owner, ServerRecord, Sharing, Status, TokenRecord,
        };
        let mut r = ServerRecord::new("wiki", "Wiki", "https://mcp.example.com/mcp");
        r.auth = AuthKind::OAuth;
        r.sharing = Sharing::Personal;
        r.status = Status::Connected;
        r.allowed = ["search".to_string()].into();
        r.approved = r.allowed.clone();
        assert_eq!(remote_state(&r, None).status, "not_connected");
        let t = TokenRecord {
            owner: Owner::user("a@x.com"),
            sealed: String::new(),
            expires_at: None,
            scope: String::new(),
            version: 1,
            status: Status::NeedsReconnect,
            status_reason: "refresh token revoked".into(),
            step_up: vec![],
            connected_by: String::new(),
            connected_at: String::new(),
        };
        let st = remote_state(&r, Some(&t));
        assert_eq!((st.status, st.action), ("needs_reconnect", "reconnect"));
        assert!(st.reason.contains("revoked"));
        // A shared server shows the team's connection.
        r.sharing = Sharing::Shared;
        assert_eq!(remote_state(&r, None).status, "connected");
        // Tools waiting for a second admin are said on the card.
        r.approved.clear();
        assert!(remote_state(&r, None).reason.contains("waiting"));
        r.allowed.clear();
        assert!(remote_state(&r, None).reason.contains("No tools"));
        r.status = Status::Failed;
        assert_eq!(remote_state(&r, None).status, "error");
    }
}
