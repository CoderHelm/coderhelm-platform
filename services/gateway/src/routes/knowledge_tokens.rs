//! Connecting a knowledge source without the provider's sign-in app, and
//! what an operator sees about those apps.
//!
//! - `POST /api/knowledge/connectors/:kind/token` connects Notion or
//!   Confluence with a token pasted in the Knowledge add flow. The token is
//!   checked with the provider first and saved exactly as the Integrations
//!   page saves it (sealed per team by KMS, admins only), so the add flow
//!   goes straight on to the page picker. Google Drive has no token path:
//!   it connects only through Google sign-in, and the answer says so.
//! - `GET /api/admin/sign-in-apps` tells a platform admin which providers'
//!   OAuth apps this install has (read from its secret), the redirect URI to
//!   register and which keys hold them. It never returns a client secret.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::knowledge::sources::{oauth, token, Kind};
use common::mcp_credentials::Credentials;
use serde_json::{json, Value};
use tracing::{info, warn};

use super::knowledge_sources::app;
use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

fn internal(context: &str, e: String) -> (StatusCode, Json<Value>) {
    tracing::error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side: try again.",
    )
}

/// What the add flow sends. Confluence needs all three; Notion the token.
#[derive(serde::Deserialize, Default)]
#[serde(default)]
pub struct TokenBody {
    pub token: String,
    pub site: String,
    pub email: String,
}

/// POST /api/knowledge/connectors/:kind/token — check a pasted token with
/// the provider and save it the way Integrations does. Answers
/// `{ connected, shared, warning }`, or 400 with one sentence and its fix.
pub async fn save_token(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kind): Path<String>,
    Json(body): Json<TokenBody>,
) -> Reply {
    claims.require_role(3).map_err(|_| {
        fail(
            StatusCode::FORBIDDEN,
            "Only admins and owners save tokens: ask one of them to connect this.",
        )
    })?;
    let kind =
        Kind::parse(&kind).ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such kind of source."))?;
    match kind {
        Kind::Notion => save_notion(&state, &claims, &body.token).await,
        Kind::Confluence => save_confluence(state, claims, body).await,
        Kind::GoogleDrive => Err(fail(
            StatusCode::BAD_REQUEST,
            "Google Drive connects only through Google sign-in: ask whoever runs Coderhelm to set it up.",
        )),
        _ => Err(fail(
            StatusCode::BAD_REQUEST,
            "This kind of source doesn't connect with a token here.",
        )),
    }
}

async fn save_notion(state: &AppState, claims: &Claims, raw: &str) -> Reply {
    let t = token::clean_token(raw, "Notion").map_err(|e| fail(StatusCode::BAD_REQUEST, &e.0))?;
    // Checked before anything is saved: a refused token never replaces one
    // that works.
    let http = common::mcp_remote::http::GuardedHttps;
    let seen = token::check_notion(&http, &t).await.map_err(|e| {
        info!(team = %claims.team_id, "Notion token refused in the add flow");
        fail(StatusCode::BAD_REQUEST, &e.0)
    })?;
    super::plugins::enable_if_missing(state, &claims.team_id, "notion")
        .await
        .map_err(|e| internal("enable notion", e))?;
    let creds: Credentials = [("api_key".to_string(), t)].into_iter().collect();
    super::plugins::store_credentials(state, &claims.team_id, "notion", &creds)
        .await
        .map_err(|e| match e {
            super::plugins::StoreError::TooLong => fail(
                StatusCode::BAD_REQUEST,
                "That token is too long: paste only the token.",
            ),
            super::plugins::StoreError::Failed(e) => internal("save notion token", e),
        })?;
    info!(team = %claims.team_id, shared = seen.shared, "Notion token saved from Knowledge");
    Ok(Json(json!({
        "connected": true,
        "shared": seen.shared,
        "warning": if seen.shared == 0 { token::NOTION_NOTHING_SHARED } else { "" },
    })))
}

async fn save_confluence(state: Arc<AppState>, claims: Claims, body: TokenBody) -> Reply {
    let t = token::clean_token(&body.token, "Confluence API")
        .map_err(|e| fail(StatusCode::BAD_REQUEST, &e.0))?;
    if body.site.trim().is_empty() || !body.email.contains('@') {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Enter your Confluence address and the email of the Atlassian account the token belongs to.",
        ));
    }
    // The Integrations page's own save: it checks the site, seals the token,
    // signs in once and records whether that worked.
    let Json(out) = super::connectors::save(
        State(state),
        Extension(claims),
        Path("confluence".to_string()),
        Json(json!({
            "site": body.site.trim(),
            "email": body.email.trim(),
            "key": t,
            "via": "token",
            "enabled": true,
        })),
    )
    .await?;
    let c = &out["connector"];
    if c["status"] == "ready" {
        Ok(Json(
            json!({ "connected": true, "shared": Value::Null, "warning": "", "note": c["status_message"] }),
        ))
    } else {
        let why = c["status_message"].as_str().unwrap_or("").trim();
        Err(fail(
            StatusCode::BAD_REQUEST,
            if why.is_empty() {
                "Confluence didn't accept that token: create a new API token and paste it again."
            } else {
                why
            },
        ))
    }
}

// ── Sign-in apps (operator view) ────────────────────────────────────────────

/// Where each provider's app comes from in the install's secret: only ever
/// its own id key, and only when its app is set (Drive never counts the
/// Google sign-in client).
fn app_source(s: &crate::models::Secrets, kind: Kind) -> &'static str {
    match oauth::provider(kind) {
        Some(p) if super::knowledge_sources::app_from(s, kind).is_set() => p.id_key,
        _ => "",
    }
}

/// One provider's row: configured or not, and what to register. Built from
/// the provider table and whether the app is set, never from its secret.
pub fn app_row(kind: Kind, configured: bool, source: &str, redirect_uri: &str) -> Value {
    let p = oauth::provider(kind);
    let (name, kind_of_app) = match kind {
        Kind::Notion => ("Notion", "Notion public connection (OAuth)"),
        Kind::Confluence => ("Atlassian (Confluence)", "Atlassian OAuth 2.0 (3LO) app"),
        Kind::GoogleDrive => ("Google Drive", "Google OAuth client (Web application)"),
        _ => (kind.label(), ""),
    };
    json!({
        "kind": kind.key(),
        "name": name,
        "app": kind_of_app,
        "configured": configured,
        "source": source,
        "id_key": p.as_ref().map(|p| p.id_key).unwrap_or(""),
        "secret_key": p.as_ref().map(|p| p.secret_key).unwrap_or(""),
        "console": p.as_ref().map(|p| p.console).unwrap_or(""),
        "scopes": p.as_ref().map(|p| p.scopes.to_vec()).unwrap_or_default(),
        "redirect_uri": redirect_uri,
    })
}

/// GET /api/admin/sign-in-apps — which sign-in apps this install has.
/// Platform admins only; read-only.
pub async fn sign_in_apps(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    if !super::api::is_platform_admin(&claims) {
        warn!(email = %claims.email, "Sign-in apps refused: not a platform admin");
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only platform admins see this.",
        ));
    }
    let redirect = super::mcp_servers::redirect_uri(&state.config.stage);
    let apps: Vec<Value> = [Kind::Notion, Kind::Confluence, Kind::GoogleDrive]
        .into_iter()
        .map(|k| {
            // A hybrid install may sign in through Coderhelm's own app instead.
            let brokered = super::hybrid_install::brokered(k).is_some();
            let source = if app(&state, k).is_set() || !brokered {
                app_source(&state.secrets, k)
            } else {
                "Coderhelm's app (hybrid)"
            };
            app_row(k, app(&state, k).is_set() || brokered, source, &redirect)
        })
        .collect();
    let secret = std::env::var("SECRETS_NAME").unwrap_or_else(|_| "coderhelm/prod/secrets".into());
    Ok(Json(json!({ "apps": apps, "secret_name": secret })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_names_what_to_register_and_never_a_secret() {
        let r = app_row(
            Kind::Notion,
            false,
            "",
            "https://app.coderhelm.com/settings/integrations/mcp-callback/",
        );
        assert_eq!(r["configured"], false);
        assert_eq!(r["id_key"], "notion_client_id");
        assert_eq!(r["secret_key"], "notion_client_secret");
        assert_eq!(
            r["redirect_uri"],
            "https://app.coderhelm.com/settings/integrations/mcp-callback/"
        );
        let a = app_row(Kind::Confluence, true, "atlassian_client_id", "x");
        assert!(a["scopes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s == "offline_access"));
        let g = app_row(Kind::GoogleDrive, true, "google_drive_client_id", "x");
        assert_eq!(
            g["scopes"][0],
            "https://www.googleapis.com/auth/drive.readonly"
        );
        // Only names and addresses: no field could hold a client secret.
        let fields: Vec<String> = g.as_object().unwrap().keys().cloned().collect();
        assert!(!fields
            .iter()
            .any(|f| f == "client_secret" || f == "client_id"));
    }

    fn secrets(extra: Value) -> crate::models::Secrets {
        let mut v = json!({ "jwt_secret": "x" });
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn drive_is_not_set_up_with_only_the_google_sign_in_client() {
        // The hosted secret as it was: Cognito's Google client, no Drive keys.
        let s = secrets(json!({
            "google_client_id": "signin.apps.googleusercontent.com",
            "google_client_secret": "signin-secret",
        }));
        assert!(!super::super::knowledge_sources::app_from(&s, Kind::GoogleDrive).is_set());
        assert_eq!(app_source(&s, Kind::GoogleDrive), "");
        let row = app_row(
            Kind::GoogleDrive,
            false,
            app_source(&s, Kind::GoogleDrive),
            "https://app.coderhelm.com/settings/integrations/mcp-callback/",
        );
        assert_eq!(row["configured"], false);
        assert_eq!(row["id_key"], "google_drive_client_id");
        assert_eq!(row["secret_key"], "google_drive_client_secret");
    }

    #[test]
    fn drive_is_set_up_from_its_own_keys() {
        let s = secrets(json!({
            "google_client_id": "signin.apps.googleusercontent.com",
            "google_client_secret": "signin-secret",
            "google_drive_client_id": "drive.apps.googleusercontent.com",
            "google_drive_client_secret": "drive-secret",
        }));
        let a = super::super::knowledge_sources::app_from(&s, Kind::GoogleDrive);
        assert_eq!(a.client_id, "drive.apps.googleusercontent.com");
        assert_eq!(app_source(&s, Kind::GoogleDrive), "google_drive_client_id");
        assert_eq!(app_source(&s, Kind::Notion), "");
    }
}
