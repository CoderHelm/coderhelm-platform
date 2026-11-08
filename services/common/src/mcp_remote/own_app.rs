//! A team's own OAuth app for a catalog server whose package reads the
//! vendor's API with an OAuth token as well as a key (Figma).
//!
//! Some vendors' remote MCP servers sign in only apps the vendor approved,
//! while their REST API takes a token from any OAuth app the team registers
//! itself. For those, the team pastes its app's client id and secret, an
//! admin signs in once, and the catalog package gets that token in place of
//! a key ([`token_env`]), renewed before it expires.
//!
//! The sign-in is a [`ServerRecord`] like any other (PKCE, a one-time
//! `state`, sealed tokens, early refresh, "needs reconnect" when the grant
//! is gone), whose `catalog_id` is `app:<id>` and which has no tools: it is
//! never listed or used as an MCP server ([`super::store::is_token_only`]).
//! The token goes only to the vendor's API, through the vendor's package,
//! exactly where a key for it would go.

use super::http::HttpRequest;
use super::oauth::ClientAuth;
use super::runtime::{ConnError, Runtime};
use super::store::{AuthKind, OAuthSettings, Owner, ServerRecord, Sharing, Status};
use crate::mcp_catalog::OwnApp;

/// The prefix of an own-app sign-in's `catalog_id`.
const PREFIX: &str = "app:";

/// The id of the record holding a catalog server's own-app sign-in.
pub fn connection_id(catalog_id: &str) -> String {
    format!("app-{catalog_id}")
}

/// The `catalog_id` of that record.
pub fn tag(catalog_id: &str) -> String {
    format!("{PREFIX}{catalog_id}")
}

/// Is this record a catalog server's own-app sign-in?
pub fn is_app_connection(catalog_id: &str) -> bool {
    catalog_id.starts_with(PREFIX)
}

/// The catalog server an own-app record signs in for.
pub fn catalog_of(tag: &str) -> Option<&str> {
    tag.strip_prefix(PREFIX).filter(|id| !id.is_empty())
}

/// The record for a team's own app with `app`, its secret already sealed
/// for this record. The team owns the connection: agents run on schedules
/// and webhooks, not in anyone's session.
pub fn record(
    catalog_id: &str,
    name: &str,
    app: &OwnApp,
    client_id: &str,
    sealed_secret: &str,
) -> ServerRecord {
    let issuer = origin(app.authorize);
    let mut s = ServerRecord::new(&connection_id(catalog_id), name, &issuer);
    s.catalog_id = tag(catalog_id);
    s.auth = AuthKind::OAuth;
    s.sharing = Sharing::Shared;
    s.oauth = Some(OAuthSettings {
        issuer,
        authorization_endpoint: app.authorize.into(),
        token_endpoint: app.token.into(),
        refresh_endpoint: app.refresh.into(),
        token_auth_methods: vec![ClientAuth::ClientSecretBasic.as_str().into()],
        scopes: app.scopes.iter().map(|s| s.to_string()).collect(),
        client_id: client_id.into(),
        client_auth: ClientAuth::ClientSecretBasic,
        client_secret_sealed: sealed_secret.into(),
        registration: super::oauth::Registration::Manual.as_str().into(),
        ..Default::default()
    });
    s
}

fn origin(u: &str) -> String {
    url::Url::parse(u)
        .map(|p| p.origin().ascii_serialization())
        .unwrap_or_else(|_| u.to_string())
}

/// Why an own app wasn't saved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaveError {
    /// Not a catalog server with an own-app sign-in.
    NoOwnApp,
    /// Something the person pasted isn't usable; the message says what.
    Invalid(&'static str),
    Internal(String),
}

/// Saves (or replaces) the team's own app for `catalog_id`. A different
/// client id ends the old sign-in (its tokens belong to the other app); the
/// same client id with a new secret keeps it.
pub async fn save(
    rt: &Runtime,
    catalog_id: &str,
    client_id: &str,
    client_secret: &str,
    by: &str,
    at: &str,
) -> Result<ServerRecord, SaveError> {
    let app = crate::mcp_catalog::own_app(catalog_id).ok_or(SaveError::NoOwnApp)?;
    let def = crate::mcp_catalog::find(catalog_id).ok_or(SaveError::NoOwnApp)?;
    let (client_id, client_secret) = (client_id.trim(), client_secret.trim());
    if client_id.is_empty() {
        return Err(SaveError::Invalid("Paste the app's client id."));
    }
    if client_id.len() > 512 || client_secret.len() > 1024 {
        return Err(SaveError::Invalid("That client id or secret is too long."));
    }
    let id = connection_id(catalog_id);
    let old = rt
        .store
        .get_server(&rt.team, &id)
        .await
        .map_err(SaveError::Internal)?;
    let old_oauth = old.as_ref().and_then(|o| o.oauth.clone());
    let sealed = match (client_secret.is_empty(), &old_oauth) {
        // Same app, secret left blank: keep the saved one.
        (true, Some(o)) if o.client_id == client_id && !o.client_secret_sealed.is_empty() => {
            o.client_secret_sealed.clone()
        }
        (true, _) => return Err(SaveError::Invalid("Paste the app's client secret.")),
        (false, _) => rt
            .seal_client_secret(&id, client_secret)
            .await
            .map_err(SaveError::Internal)?,
    };
    let mut rec = record(catalog_id, def.name, &app, client_id, &sealed);
    rec.added_by = by.to_ascii_lowercase();
    rec.added_at = at.to_string();
    rec.status = Status::NotConnected;
    if let Some(old) = &old {
        let same_app = old_oauth.as_ref().is_some_and(|o| o.client_id == client_id);
        if same_app {
            rec.status = old.status;
            rec.status_reason = old.status_reason.clone();
        } else if let Err(e) = rt.disconnect(old, &Owner::Team).await {
            return Err(SaveError::Internal(e));
        }
        rec.added_by = old.added_by.clone();
        rec.added_at = old.added_at.clone();
        rec.custom_prompt = old.custom_prompt.clone();
    }
    rt.store
        .put_server(&rt.team, &rec)
        .await
        .map_err(SaveError::Internal)?;
    Ok(rec)
}

/// The team's own-app record for `catalog_id`, when one is saved.
pub async fn load(rt: &Runtime, catalog_id: &str) -> Result<Option<ServerRecord>, String> {
    if crate::mcp_catalog::own_app(catalog_id).is_none() {
        return Ok(None);
    }
    Ok(rt
        .store
        .get_server(&rt.team, &connection_id(catalog_id))
        .await?
        .filter(|s| s.catalog_id == tag(catalog_id)))
}

/// Is the team signed in with its own app for `catalog_id` right now?
pub async fn connected(rt: &Runtime, catalog_id: &str) -> bool {
    let Ok(Some(s)) = load(rt, catalog_id).await else {
        return false;
    };
    matches!(
        rt.store.get_token(&rt.team, &s.id, &Owner::Team).await,
        Ok(Some(t)) if t.status == Status::Connected
    )
}

/// The environment variable and token the catalog package for `catalog_id`
/// runs with when the team signed in with its own app: `Ok(None)` when it
/// hasn't (the package uses the saved key, if any). The token is renewed
/// first when it is about to expire.
pub async fn token_env(
    rt: &Runtime,
    catalog_id: &str,
) -> Result<Option<(&'static str, String)>, ConnError> {
    let Some(app) = crate::mcp_catalog::own_app(catalog_id) else {
        return Ok(None);
    };
    let Some(s) = load(rt, catalog_id).await.map_err(ConnError::Temporary)? else {
        return Ok(None);
    };
    match rt.auth_header(&s, &Owner::Team, false).await {
        Ok(Some(h)) => Ok(Some((app.env_var, bearer_value(&h.1).to_string()))),
        Ok(None) | Err(ConnError::NotConnected) => Ok(None),
        Err(e) => Err(e),
    }
}

fn bearer_value(header: &str) -> &str {
    header.strip_prefix("Bearer ").unwrap_or(header)
}

/// Asks the vendor's API who the token belongs to, right after a sign-in,
/// and records the answer on the connection: a token the API refuses is
/// marked so the card says so instead of agents finding out.
pub async fn check(rt: &Runtime, s: &ServerRecord) -> Result<(), String> {
    let catalog_id = catalog_of(&s.catalog_id).ok_or("This isn't an own-app sign-in.")?;
    let app =
        crate::mcp_catalog::own_app(catalog_id).ok_or("This server has no own-app sign-in.")?;
    let header = rt
        .auth_header(s, &Owner::Team, false)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("Not signed in.")?;
    let resp = rt
        .http
        .send(HttpRequest::get(app.check).header(&header.0, &header.1))
        .await
        .map_err(|e| format!("Couldn't reach {}: {e}", origin(app.check)))?;
    if (200..300).contains(&resp.status) {
        rt.mark(s, &Owner::Team, Status::Connected, "", &[]).await;
        return Ok(());
    }
    let why = match resp.status {
        401 | 403 => format!(
            "{} refused the new sign-in (HTTP {}). Check the app's scopes and that it is published for your team, then connect again.",
            s.name, resp.status
        ),
        n => format!("{} answered HTTP {n} when the sign-in was checked. Try again.", s.name),
    };
    let status = if matches!(resp.status, 401 | 403) {
        Status::NeedsReconnect
    } else {
        Status::Failed
    };
    rt.mark(s, &Owner::Team, status, &why, &[]).await;
    Err(why)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_remote::http::fake::{json as fjson, Fake};
    use crate::mcp_remote::http::Method;
    use crate::mcp_remote::runtime::testing::{sign_in, FakeKms};
    use crate::mcp_remote::store::fake::MemStore;
    use serde_json::json;
    use std::sync::Arc;

    const REDIRECT: &str = "https://coderhelm.example.com/settings/integrations/mcp-callback/";

    fn rt_with(http: Arc<Fake>, now: i64) -> Runtime {
        Runtime {
            store: Arc::new(MemStore::default()),
            cipher: Arc::new(FakeKms),
            http,
            team: "TEAM#a".into(),
            clock: Arc::new(move || now),
        }
    }

    fn query(url: &str) -> std::collections::HashMap<String, String> {
        url::Url::parse(url)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect()
    }

    #[tokio::test]
    async fn figma_signs_in_with_the_teams_own_app() {
        let http = Arc::new(Fake::default());
        let rt = rt_with(http.clone(), 1_000_000);
        let mut s = save(
            &rt,
            "figma",
            " client-1 ",
            "secret-1",
            "Ada@Example.com",
            "t0",
        )
        .await
        .unwrap();
        assert_eq!(s.id, "app-figma");
        assert_eq!(s.catalog_id, "app:figma");
        assert_eq!(s.added_by, "ada@example.com");
        // Never an MCP server: not listed, never usable by agents.
        assert!(rt.store.list_servers(&rt.team).await.unwrap().is_empty());
        assert!(rt
            .store
            .get_server(&rt.team, "app-figma")
            .await
            .unwrap()
            .is_some());

        let url = rt
            .start_sign_in(
                &mut s,
                &Owner::Team,
                "sub-1",
                "ada@example.com",
                REDIRECT,
                Some("https://coderhelm.example.com/integrations/mcp-client.json"),
            )
            .await
            .unwrap();
        assert!(url.starts_with("https://www.figma.com/oauth?"), "{url}");
        let q = query(&url);
        assert_eq!(q["client_id"], "client-1");
        assert_eq!(q["redirect_uri"], REDIRECT);
        assert_eq!(q["scope"], "current_user:read file_content:read");
        assert_eq!(q["response_type"], "code");
        assert_eq!(q["code_challenge_method"], "S256");
        assert!(!q.contains_key("resource"));

        http.on(
            Method::Post,
            "https://api.figma.com/v1/oauth/token",
            fjson(200, json!({"user_id_string": "1", "access_token": "at-1", "token_type": "bearer", "expires_in": 7_776_000, "refresh_token": "rt-1"})),
        );
        rt.finish_sign_in(
            &q["state"],
            Some("code-1"),
            None,
            None,
            "sub-1",
            "ada@example.com",
        )
        .await
        .unwrap();
        let sent = http.sent_to("https://api.figma.com/v1/oauth/token");
        assert_eq!(sent.len(), 1);
        // Figma wants the client in a Basic header, never in the body.
        let basic = sent[0].header_value("Authorization").unwrap();
        assert!(basic.starts_with("Basic "), "{basic}");
        let body = sent[0].body_text();
        assert!(body.contains("grant_type=authorization_code") && body.contains("code=code-1"));
        assert!(!body.contains("client_secret"));

        assert!(connected(&rt, "figma").await);
        assert_eq!(
            token_env(&rt, "figma").await.unwrap(),
            Some(("FIGMA_OAUTH_TOKEN", "at-1".to_string()))
        );
    }

    #[tokio::test]
    async fn an_expiring_token_is_renewed_at_figmas_refresh_address() {
        let http = Arc::new(Fake::default());
        let rt = rt_with(http.clone(), 2_000_000);
        save(
            &rt,
            "figma",
            "client-1",
            "secret-1",
            "ada@example.com",
            "t0",
        )
        .await
        .unwrap();
        sign_in(
            &rt,
            "app-figma",
            &Owner::Team,
            "at-old",
            Some("rt-1"),
            Some(2_000_010),
            "ada@example.com",
        )
        .await;
        http.on(
            Method::Post,
            "https://api.figma.com/v1/oauth/refresh",
            fjson(
                200,
                json!({"access_token": "at-new", "token_type": "bearer", "expires_in": 7_776_000}),
            ),
        );
        let got = token_env(&rt, "figma").await.unwrap();
        assert_eq!(got, Some(("FIGMA_OAUTH_TOKEN", "at-new".to_string())));
        let sent = http.sent_to("https://api.figma.com/v1/oauth/refresh");
        assert_eq!(sent.len(), 1);
        assert!(sent[0]
            .header_value("Authorization")
            .unwrap()
            .starts_with("Basic "));
        assert!(sent[0].body_text().contains("refresh_token=rt-1"));
        assert!(http
            .sent_to("https://api.figma.com/v1/oauth/token")
            .is_empty());
        // Figma doesn't rotate refresh tokens: the old one is kept for next time.
        http.on(
            Method::Post,
            "https://api.figma.com/v1/oauth/refresh",
            fjson(200, json!({"access_token": "at-3", "expires_in": 1})),
        );
        let s = load(&rt, "figma").await.unwrap().unwrap();
        rt.auth_header(&s, &Owner::Team, true).await.unwrap();
        let sent = http.sent_to("https://api.figma.com/v1/oauth/refresh");
        assert!(sent[1].body_text().contains("refresh_token=rt-1"));
    }

    #[tokio::test]
    async fn without_a_sign_in_the_package_keeps_its_key() {
        let rt = rt_with(Arc::new(Fake::default()), 1);
        assert_eq!(token_env(&rt, "figma").await.unwrap(), None);
        save(&rt, "figma", "client-1", "secret-1", "a@example.com", "t0")
            .await
            .unwrap();
        assert_eq!(token_env(&rt, "figma").await.unwrap(), None);
        assert!(!connected(&rt, "figma").await);
        // A server without an own app never has one.
        assert_eq!(token_env(&rt, "sentry").await.unwrap(), None);
        assert_eq!(
            save(&rt, "sentry", "c", "s", "a@example.com", "t0")
                .await
                .unwrap_err(),
            SaveError::NoOwnApp
        );
    }

    #[tokio::test]
    async fn another_app_ends_the_old_sign_in_and_a_new_secret_keeps_it() {
        let rt = rt_with(Arc::new(Fake::default()), 1);
        assert_eq!(
            save(&rt, "figma", "client-1", "", "a@example.com", "t0")
                .await
                .unwrap_err(),
            SaveError::Invalid("Paste the app's client secret.")
        );
        save(&rt, "figma", "client-1", "secret-1", "a@example.com", "t0")
            .await
            .unwrap();
        sign_in(
            &rt,
            "app-figma",
            &Owner::Team,
            "at",
            Some("rt"),
            None,
            "a@example.com",
        )
        .await;
        rt.store
            .set_status(&rt.team, "app-figma", Status::Connected, "")
            .await
            .unwrap();
        // Same app, secret left blank: the saved secret and sign-in stay.
        let s = save(&rt, "figma", "client-1", "", "b@example.com", "t1")
            .await
            .unwrap();
        assert_eq!(s.status, Status::Connected);
        assert_eq!(s.added_by, "a@example.com");
        assert!(connected(&rt, "figma").await);
        // Another app: the old tokens are gone.
        let s = save(&rt, "figma", "client-2", "secret-2", "b@example.com", "t2")
            .await
            .unwrap();
        assert_eq!(s.status, Status::NotConnected);
        assert!(!connected(&rt, "figma").await);
        assert_eq!(s.oauth.unwrap().client_id, "client-2");
    }

    #[tokio::test]
    async fn the_check_marks_a_refused_token() {
        let http = Arc::new(Fake::default());
        let rt = rt_with(http.clone(), 1);
        let s = save(&rt, "figma", "client-1", "secret-1", "a@example.com", "t0")
            .await
            .unwrap();
        sign_in(
            &rt,
            "app-figma",
            &Owner::Team,
            "at",
            None,
            None,
            "a@example.com",
        )
        .await;
        http.on(
            Method::Get,
            "https://api.figma.com/v1/me",
            fjson(200, json!({"id": "1"})),
        );
        check(&rt, &s).await.unwrap();
        assert_eq!(
            http.sent_to("https://api.figma.com/v1/me")[0].header_value("Authorization"),
            Some("Bearer at")
        );

        let http = Arc::new(Fake::default());
        let rt = rt_with(http.clone(), 1);
        let s = save(&rt, "figma", "client-1", "secret-1", "a@example.com", "t0")
            .await
            .unwrap();
        sign_in(
            &rt,
            "app-figma",
            &Owner::Team,
            "at",
            None,
            None,
            "a@example.com",
        )
        .await;
        http.on(
            Method::Get,
            "https://api.figma.com/v1/me",
            fjson(403, json!({"status": 403, "err": "Invalid scope"})),
        );
        let e = check(&rt, &s).await.unwrap_err();
        assert!(e.contains("published for your team"), "{e}");
        assert!(!connected(&rt, "figma").await);
    }
}
