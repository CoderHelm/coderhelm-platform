//! One-time claim links: how a member who never linked the team's identity
//! provider gets in once "Require single sign-on" is on.
//!
//! There is no linking by email, ever: an Okta or Microsoft account whose
//! email matches a member is not that member until someone with the right
//! to say so says it. Here that is an admin, for one member at a time:
//!
//! - **Issue.** An admin (never for the owner) makes a link for one active
//!   member who is not linked to the provider the rule names. It is a row
//!   `pk = SSO_CLAIM#<team>`, `sk = <member id>` holding a hash of the
//!   link's secret (never the secret), the member's email and role, the
//!   provider, a 72-hour expiry and a used flag. The link is shown once, to
//!   the admin, who sends it to the member. It is in the audit log and every
//!   owner is emailed. An unused link can be revoked; a new one replaces an
//!   unused one.
//! - **Start.** `GET /auth/sso/claim?token=...` (public) checks the link is
//!   live and begins the provider's sign-in, carrying the member and the
//!   secret's hash in the signed flow cookie.
//! - **Use.** The provider's callback, when the identity it vouched for is
//!   linked to nobody on the team, accepts the claim in one transaction:
//!   the member is still unlinked, with the same email and role; the
//!   identity's uniqueness row doesn't exist; the claim row is unused,
//!   unexpired and has the same hash, email and role; on hosted the email's
//!   domain proof is this team's and not failing. Before it, the provider's
//!   email must be verified (Okta's `email_verified`, Microsoft's
//!   `xms_edov`) and be exactly the member's (trimmed, lowercase). It is in
//!   the audit log and every owner is emailed. A sign-in without a claim
//!   still gets "ask an admin".

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem, Update};
use axum::{
    extract::{Extension, Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64URL, Engine};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

use super::require_sso::{self, Link, SsoProvider};
use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;
type ApiError = (StatusCode, Json<Value>);

pub const PREFIX: &str = "SSO_CLAIM#";
/// How long a claim link works.
pub const CLAIM_HOURS: i64 = 72;

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn s<'a>(item: &'a Item, key: &str) -> &'a str {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .map_or("", String::as_str)
}

fn fail(code: StatusCode, msg: &str, key: &str) -> ApiError {
    (code, Json(json!({ "error": msg, "code": key })))
}

fn internal(context: &str, e: impl std::fmt::Display) -> ApiError {
    tracing::error!("{context}: {e}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "Something went wrong on our side. Try again." })),
    )
}

fn admin(claims: &Claims) -> Result<(), ApiError> {
    claims.require_role(3).map_err(|_| {
        fail(
            StatusCode::FORBIDDEN,
            "Only admins and owners can do that.",
            "admin_only",
        )
    })
}

pub fn claim_pk(team: &str) -> String {
    format!("{PREFIX}{team}")
}

/// SHA-256 of a link's secret, hex. Only this is stored.
pub fn token_hash(secret: &str) -> String {
    Sha256::digest(secret.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The link's token: the team, the member and the secret.
pub fn make_token(team: &str, member: &str, secret: &str) -> String {
    format!(
        "{}.{}.{secret}",
        B64URL.encode(team.as_bytes()),
        B64URL.encode(member.as_bytes())
    )
}

/// The team, member and secret of a token, if it reads as one.
pub fn parse_token(t: &str) -> Option<(String, String, String)> {
    let mut parts = t.trim().splitn(3, '.');
    let team = String::from_utf8(B64URL.decode(parts.next()?).ok()?).ok()?;
    let member = String::from_utf8(B64URL.decode(parts.next()?).ok()?).ok()?;
    let secret = parts.next()?.to_string();
    (team.starts_with("TEAM#") && member.starts_with("USER#") && secret.len() >= 32)
        .then_some((team, member, secret))
}

/// A stored claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub team: String,
    pub member: String,
    pub email: String,
    pub role: String,
    pub provider: Option<SsoProvider>,
    pub token_hash: String,
    /// Unix seconds.
    pub expires_at: i64,
    pub used: bool,
    pub used_at: Option<String>,
    pub issued_by: String,
    pub issued_at: String,
}

impl Claim {
    pub fn from_item(i: &Item) -> Option<Claim> {
        let team = s(i, "pk").strip_prefix(PREFIX)?.to_string();
        Some(Claim {
            team,
            member: s(i, "sk").to_string(),
            email: s(i, "email").to_string(),
            role: s(i, "role").to_string(),
            provider: SsoProvider::parse(s(i, "provider")),
            token_hash: s(i, "token_hash").to_string(),
            expires_at: i
                .get("expires_at")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0),
            used: i
                .get("used")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true),
            used_at: Some(s(i, "used_at").to_string()).filter(|v| !v.is_empty()),
            issued_by: s(i, "issued_by").to_string(),
            issued_at: s(i, "issued_at").to_string(),
        })
    }

    pub fn view(&self) -> Value {
        json!({
            "user_id": self.member.trim_start_matches("USER#"),
            "email": self.email,
            "role": self.role,
            "provider": self.provider.map(SsoProvider::slug),
            "expires_at": chrono::DateTime::from_timestamp(self.expires_at, 0).map(|t| t.to_rfc3339()),
            "used": self.used,
            "used_at": self.used_at,
            "issued_by": self.issued_by,
            "issued_at": self.issued_at,
        })
    }
}

/// Why a claim can't be used by a sign-in whose provider vouched for
/// `email`, or `Ok`. Pure: the member record and the claim as read now.
pub fn redeemable(
    claim: &Claim,
    member: Option<&Item>,
    provider: SsoProvider,
    hash: &str,
    email: &str,
    now: i64,
) -> Result<(), &'static str> {
    if claim.used {
        return Err("used");
    }
    if claim.expires_at <= now {
        return Err("expired");
    }
    if claim.token_hash.is_empty() || claim.token_hash != hash {
        return Err("not_this_link");
    }
    if claim.provider != Some(provider) {
        return Err("other_provider");
    }
    let Some(m) = member else {
        return Err("no_member");
    };
    if s(m, "status") == "invited" || s(m, "role") == "owner" || claim.role == "owner" {
        return Err("not_for_this_member");
    }
    if s(m, "role") != claim.role || s(m, "email") != claim.email {
        return Err("member_changed");
    }
    if Link::is_linked(m, provider) {
        return Err("already_linked");
    }
    let want = require_sso::exact_address(&claim.email);
    if want.is_empty() || require_sso::exact_address(email) != want {
        return Err("other_email");
    }
    Ok(())
}

/// The claims of a team, for the Sign-in rules page.
pub async fn list(state: &AppState, team: &str) -> Result<Vec<Claim>, String> {
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", attr_s(&claim_pk(team)))
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(out.items().iter().filter_map(Claim::from_item).collect())
}

async fn read(state: &AppState, team: &str, member: &str) -> Result<Option<Claim>, String> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claim_pk(team)))
        .key("sk", attr_s(member))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(out.item().and_then(Claim::from_item))
}

async fn member_item(state: &AppState, team: &str, member: &str) -> Result<Option<Item>, String> {
    Ok(state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team))
        .key("sk", attr_s(member))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{e}"))?
        .item)
}

#[derive(Deserialize)]
pub struct IssueBody {
    pub user_id: String,
}

/// POST /api/sign-in/claims: a one-time link for one unlinked member, while
/// the team requires single sign-on. Admins and owners; never for an owner.
/// The link is in the answer only.
pub async fn issue(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<IssueBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    if claims.support_view.is_some() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Not from a support view.",
            "support_view",
        ));
    }
    let team = require_sso::rule_team(&state, &claims)
        .await
        .map_err(|e| internal("Organization not read", e))?
        .ok_or_else(|| {
            fail(
                StatusCode::NOT_FOUND,
                "This team isn't the organization.",
                "not_org",
            )
        })?;
    let rule = super::sign_in::team_rule(&state, &team)
        .await
        .map_err(|e| internal("Sign-in rules not read", e))?;
    let Some(provider) = rule.require_sso else {
        return Err(fail(
            StatusCode::CONFLICT,
            "Claim links are for a team that requires single sign-on. Members link their account themselves in Settings, Security until then.",
            "sso_not_required",
        ));
    };
    let member_id = if body.user_id.starts_with("USER#") {
        body.user_id.clone()
    } else {
        format!("USER#{}", body.user_id.trim())
    };
    let member = member_item(&state, &team, &member_id)
        .await
        .map_err(|e| internal("Member not read", e))?
        .filter(|m| s(m, "status") != "invited")
        .ok_or_else(|| {
            fail(
                StatusCode::NOT_FOUND,
                "They aren't on this team.",
                "not_member",
            )
        })?;
    if s(&member, "role") == "owner" {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "There is no claim link for an owner: an owner links their account themselves before the rule is turned on.",
            "owner",
        ));
    }
    if Link::is_linked(&member, provider) {
        return Err(fail(
            StatusCode::CONFLICT,
            &format!("They're already linked to {}.", provider.name()),
            "already_linked",
        ));
    }
    let email = s(&member, "email").to_string();
    if require_sso::exact_address(&email).is_empty() || !email.contains('@') {
        return Err(fail(
            StatusCode::CONFLICT,
            "They have no email on the team, so a claim link can't be checked against one.",
            "no_email",
        ));
    }
    let secret = super::oidc_signin::random_token();
    let now = chrono::Utc::now();
    let expires = now + chrono::Duration::hours(CLAIM_HOURS);
    let at = now.to_rfc3339();
    // A new link replaces one not used yet; a used one stays as the record.
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claim_pk(&team)))
        .item("sk", attr_s(&member_id))
        .item("token_hash", attr_s(&token_hash(&secret)))
        .item("email", attr_s(&email))
        .item("role", attr_s(s(&member, "role")))
        .item("provider", attr_s(provider.slug()))
        .item(
            "expires_at",
            AttributeValue::N(expires.timestamp().to_string()),
        )
        .item("used", AttributeValue::Bool(false))
        .item("issued_by", attr_s(&claims.display_name()))
        .item("issued_at", attr_s(&at))
        .item(
            "ttl",
            AttributeValue::N(
                (expires + chrono::Duration::days(30))
                    .timestamp()
                    .to_string(),
            ),
        )
        .condition_expression("attribute_not_exists(pk) OR used = :f")
        .expression_attribute_values(":f", AttributeValue::Bool(false))
        .send()
        .await
        .map_err(|e| {
            if format!("{e:?}").contains("ConditionalCheckFailed") {
                fail(
                    StatusCode::CONFLICT,
                    "Their claim link was used already.",
                    "claim_used",
                )
            } else {
                internal("Claim link not saved", e)
            }
        })?;
    let who = if email.is_empty() {
        member_id.clone()
    } else {
        email.clone()
    };
    require_sso::audit(
        &state,
        &claims,
        &team,
        format!(
            "Single sign-on claim link issued for {who} ({}), good for {CLAIM_HOURS} hours, for {}",
            s(&member, "role"),
            provider.name()
        ),
    )
    .await;
    let by = claims.display_name();
    require_sso::tell_owners(
        &state,
        &team,
        "sso_claim_issued",
        "a single sign-on claim link was issued",
        &format!(
            "{by} made a one-time link for a member of your team to link their {} account and sign in. It works once, for 72 hours. The audit log says who it is for. If you didn't expect this, revoke it in Settings, Sign-in rules.",
            provider.name()
        ),
        &[("actor", by.as_str()), ("provider", provider.name())],
    )
    .await;
    info!(team_id = %team, "Single sign-on claim link issued");
    let url = format!(
        "{}/auth/sso/claim?token={}",
        common::public_urls::api_url(&state.config.stage).trim_end_matches('/'),
        make_token(&team, &member_id, &secret)
    );
    Ok(Json(json!({
        "url": url,
        "expires_at": expires.to_rfc3339(),
        "claim": read(&state, &team, &member_id).await.ok().flatten().map(|c| c.view()),
    })))
}

/// DELETE /api/sign-in/claims/:user_id: revoke a member's unused link.
pub async fn revoke(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    if claims.support_view.is_some() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Not from a support view.",
            "support_view",
        ));
    }
    let team = require_sso::rule_team(&state, &claims)
        .await
        .map_err(|e| internal("Organization not read", e))?
        .ok_or_else(|| {
            fail(
                StatusCode::NOT_FOUND,
                "This team isn't the organization.",
                "not_org",
            )
        })?;
    let member_id = format!("USER#{}", user_id.trim().trim_start_matches("USER#"));
    let res = state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claim_pk(&team)))
        .key("sk", attr_s(&member_id))
        .condition_expression("used = :f")
        .expression_attribute_values(":f", AttributeValue::Bool(false))
        .send()
        .await;
    if let Err(e) = res {
        return Err(if format!("{e:?}").contains("ConditionalCheckFailed") {
            fail(
                StatusCode::CONFLICT,
                "There's no unused claim link for them.",
                "no_unused_claim",
            )
        } else {
            internal("Claim link not revoked", e)
        });
    }
    require_sso::audit(
        &state,
        &claims,
        &team,
        format!(
            "Single sign-on claim link revoked for member {}",
            member_id.trim_start_matches("USER#")
        ),
    )
    .await;
    Ok(Json(json!({ "revoked": true })))
}

#[derive(Deserialize)]
pub struct StartQuery {
    #[serde(default)]
    pub token: String,
}

/// Where a claim link that can't be used lands.
pub fn claim_failed_url(app_url: &str, why: &str) -> String {
    format!(
        "{}/?signin_error=sso_claim_invalid&reason={}",
        app_url.trim_end_matches('/'),
        urlencoding::encode(why)
    )
}

fn to_failed(state: &AppState, why: &str) -> Response {
    warn!(why, "Claim link not started");
    (
        StatusCode::FOUND,
        [(
            header::LOCATION,
            claim_failed_url(common::public_urls::app_url(&state.config.stage), why),
        )],
    )
        .into_response()
}

/// The claim a flow cookie carries: the member and the secret's hash.
pub fn flow_value(member: &str, hash: &str) -> String {
    format!("{member}|{hash}")
}

pub fn read_flow_value(v: &str) -> Option<(&str, &str)> {
    let (m, h) = v.split_once('|')?;
    (!m.is_empty() && !h.is_empty()).then_some((m, h))
}

/// GET /auth/sso/claim?token=...: a live claim link begins the sign-in with
/// the team's provider, carrying the claim in the signed flow cookie.
pub async fn start(State(state): State<Arc<AppState>>, Query(q): Query<StartQuery>) -> Response {
    let Some((team, member, secret)) = parse_token(&q.token) else {
        return to_failed(&state, "not_a_link");
    };
    let hash = token_hash(&secret);
    let claim = match read(&state, &team, &member).await {
        Ok(Some(c)) => c,
        Ok(None) => return to_failed(&state, "not_a_link"),
        Err(_) => return to_failed(&state, "server"),
    };
    if claim.token_hash != hash {
        return to_failed(&state, "not_a_link");
    }
    if claim.used {
        return to_failed(&state, "used");
    }
    if claim.expires_at <= chrono::Utc::now().timestamp() {
        return to_failed(&state, "expired");
    }
    let rule = match super::sign_in::team_rule(&state, &team).await {
        Ok(r) => r,
        Err(_) => return to_failed(&state, "server"),
    };
    let Some(provider) = rule.require_sso.filter(|p| Some(*p) == claim.provider) else {
        return to_failed(&state, "rule_off");
    };
    let carried = flow_value(&member, &hash);
    let started = match provider {
        SsoProvider::Okta => super::oidc_signin::start_claim(&state, &team, &carried).await,
        SsoProvider::Microsoft => super::entra_sso::start_claim(&state, &team, &carried).await,
    };
    started.unwrap_or_else(|why| to_failed(&state, why))
}

/// Accepts a claim for the identity `link` the provider just vouched for
/// (with verified `email`), in one transaction (see the module docs). The
/// member record as it is after, for the session.
pub async fn redeem(
    state: &AppState,
    team: &str,
    carried: &str,
    provider: SsoProvider,
    link: &Link,
    email: &str,
) -> Result<Item, &'static str> {
    let (member_id, hash) = read_flow_value(carried).ok_or("not_a_link")?;
    let claim = read(state, team, member_id)
        .await
        .map_err(|_| "server")?
        .ok_or("not_a_link")?;
    let member = member_item(state, team, member_id)
        .await
        .map_err(|_| "server")?;
    let now = chrono::Utc::now();
    redeemable(
        &claim,
        member.as_ref(),
        provider,
        hash,
        email,
        now.timestamp(),
    )?;
    let member = member.ok_or("no_member")?;
    let mut items: Vec<TransactWriteItem> = Vec::new();
    // Hosted: the email's domain is this team's proven, non-failing domain,
    // checked now and held in the same transaction.
    if !super::sign_in::applies() {
        let domain = super::sign_in::email_domain(email).ok_or("other_email")?;
        if !super::oidc_signin::domain_usable(state, team, &domain)
            .await
            .map_err(|_| "server")?
        {
            return Err("domain");
        }
        items.push(
            super::oidc_signin::domain_still_usable(state, team, &domain).map_err(|_| "server")?,
        );
    }
    let at = now.to_rfc3339();
    let b = |e: aws_sdk_dynamodb::error::BuildError| {
        warn!(error = %e, "Claim transaction not built");
        "server"
    };
    let (row_pk, row_sk) = link.row(team);
    let (set, mut values) = link.member_set(&at);
    values.insert(":inv".into(), attr_s("invited"));
    values.insert(":em".into(), attr_s(&claim.email));
    values.insert(":role".into(), attr_s(&claim.role));
    let unlinked = link.unlinked_condition();
    items.push(
        TransactWriteItem::builder()
            .update(
                Update::builder()
                    .table_name(&state.config.users_table_name)
                    .key("pk", attr_s(team))
                    .key("sk", attr_s(member_id))
                    .update_expression(format!("SET {set}"))
                    .condition_expression(format!(
                        "attribute_exists(pk) AND #st <> :inv AND email = :em AND #r = :role AND {unlinked}"
                    ))
                    .expression_attribute_names("#st", "status")
                    .expression_attribute_names("#r", "role")
                    .set_expression_attribute_values(Some(values))
                    .build()
                    .map_err(b)?,
            )
            .build(),
    );
    items.push(
        TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name(&state.config.settings_table_name)
                    .item("pk", attr_s(&row_pk))
                    .item("sk", attr_s(&row_sk))
                    .item("member", attr_s(member_id))
                    .condition_expression("attribute_not_exists(pk)")
                    .build()
                    .map_err(b)?,
            )
            .build(),
    );
    items.push(
        TransactWriteItem::builder()
            .update(
                Update::builder()
                    .table_name(&state.config.settings_table_name)
                    .key("pk", attr_s(&claim_pk(team)))
                    .key("sk", attr_s(member_id))
                    .update_expression("SET used = :t, used_at = :at")
                    .condition_expression(
                        "token_hash = :h AND used = :f AND expires_at > :now AND email = :em AND #r = :role AND provider = :p",
                    )
                    .expression_attribute_names("#r", "role")
                    .expression_attribute_values(":t", AttributeValue::Bool(true))
                    .expression_attribute_values(":f", AttributeValue::Bool(false))
                    .expression_attribute_values(":at", attr_s(&at))
                    .expression_attribute_values(":h", attr_s(hash))
                    .expression_attribute_values(":now", AttributeValue::N(now.timestamp().to_string()))
                    .expression_attribute_values(":em", attr_s(&claim.email))
                    .expression_attribute_values(":role", attr_s(&claim.role))
                    .expression_attribute_values(":p", attr_s(provider.slug()))
                    .build()
                    .map_err(b)?,
            )
            .build(),
    );
    if let Err(e) = state
        .dynamo
        .transact_write_items()
        .set_transact_items(Some(items))
        .send()
        .await
    {
        warn!(error = ?e, "Claim not accepted; nothing changed");
        return Err("changed");
    }
    let who = claim.email.clone();
    require_sso::audit_system(
        state,
        team,
        format!(
            "Single sign-on claim link used: {who} linked their {} account and signed in",
            provider.name()
        ),
    )
    .await;
    require_sso::tell_owners(
        state,
        team,
        "sso_claim_used",
        "a single sign-on claim link was used",
        &format!(
            "A member of your team used their one-time link: their {} account is now linked and they signed in. The audit log says who. If you didn't expect this, check Settings, Team.",
            provider.name()
        ),
        &[("provider", provider.name())],
    )
    .await;
    info!(team_id = %team, "Single sign-on claim accepted");
    let _ = member;
    member_item(state, team, member_id)
        .await
        .map_err(|_| "server")?
        .ok_or("no_member")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), attr_s(v)))
            .collect()
    }

    fn claim(now: i64) -> Claim {
        Claim {
            team: "TEAM#a".into(),
            member: "USER#m".into(),
            email: "Pat@Example.com".into(),
            role: "member".into(),
            provider: Some(SsoProvider::Okta),
            token_hash: token_hash("s"),
            expires_at: now + 10,
            used: false,
            used_at: None,
            issued_by: "Admin".into(),
            issued_at: String::new(),
        }
    }

    fn member() -> Item {
        item(&[
            ("pk", "TEAM#a"),
            ("sk", "USER#m"),
            ("email", "Pat@Example.com"),
            ("role", "member"),
            ("status", "active"),
        ])
    }

    #[test]
    fn a_token_reads_back_and_nothing_else_does() {
        let secret = super::super::oidc_signin::random_token();
        let t = make_token("TEAM#a", "USER#m", &secret);
        assert_eq!(
            parse_token(&t),
            Some(("TEAM#a".into(), "USER#m".into(), secret))
        );
        assert_eq!(parse_token("x.y.z"), None);
        assert_eq!(parse_token(""), None);
        assert_eq!(token_hash("s").len(), 64);
    }

    #[test]
    fn a_claim_is_used_once_by_exactly_its_member() {
        let now = 1_000;
        let h = token_hash("s");
        let p = SsoProvider::Okta;
        let m = member();
        let ok = |c: &Claim, m: Option<&Item>, e: &str| redeemable(c, m, p, &h, e, now);
        assert_eq!(ok(&claim(now), Some(&m), " pat@example.com "), Ok(()));
        // Not another mailbox, not a +tag, not a folded address.
        assert_eq!(
            ok(&claim(now), Some(&m), "pat+x@example.com"),
            Err("other_email")
        );
        assert_eq!(
            ok(&claim(now), Some(&m), "other@example.com"),
            Err("other_email")
        );
        let mut used = claim(now);
        used.used = true;
        assert_eq!(ok(&used, Some(&m), "pat@example.com"), Err("used"));
        let mut old = claim(now);
        old.expires_at = now;
        assert_eq!(ok(&old, Some(&m), "pat@example.com"), Err("expired"));
        assert_eq!(
            redeemable(
                &claim(now),
                Some(&m),
                p,
                &token_hash("t"),
                "pat@example.com",
                now
            ),
            Err("not_this_link")
        );
        assert_eq!(
            redeemable(
                &claim(now),
                Some(&m),
                SsoProvider::Microsoft,
                &h,
                "pat@example.com",
                now
            ),
            Err("other_provider")
        );
        assert_eq!(ok(&claim(now), None, "pat@example.com"), Err("no_member"));
        // The member changed since: another role or email, an owner, linked.
        let mut admin = m.clone();
        admin.insert("role".into(), attr_s("admin"));
        assert_eq!(
            ok(&claim(now), Some(&admin), "pat@example.com"),
            Err("member_changed")
        );
        let mut owner = m.clone();
        owner.insert("role".into(), attr_s("owner"));
        assert_eq!(
            ok(&claim(now), Some(&owner), "pat@example.com"),
            Err("not_for_this_member")
        );
        let mut linked = m.clone();
        linked.insert("okta_sub".into(), attr_s("x"));
        assert_eq!(
            ok(&claim(now), Some(&linked), "pat@example.com"),
            Err("already_linked")
        );
    }
}
