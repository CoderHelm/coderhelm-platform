//! One-click sign-in for the connectors that use OAuth (Notion, Confluence
//! through Atlassian, Google Drive).
//!
//! These reuse the remote MCP sign-in machinery (`mcp_remote::runtime`)
//! whole: PKCE, a one-time `state` bound to the person and team, the
//! dashboard's existing callback page, tokens sealed by KMS for one team,
//! one connection and one owner, early refresh that never loses a rotated
//! refresh token, "needs reconnect" when a grant is revoked, and revocation
//! on disconnect. A connection is a [`ServerRecord`] whose `catalog_id` is
//! `source:<kind>`, with no tools, so agents never get it as an MCP server
//! (the runtime only offers servers with allowed tools).
//!
//! What differs per provider is fixed here ([`provider`]); the OAuth app
//! (client id and secret) is the install's own and comes from configuration
//! ([`App`]). Without one, the connector's card says which settings are
//! missing and offers the team's saved key instead where there is one.
//!
//! An app is read from the provider's own two keys and nothing else
//! ([`App::from_keys`]). Google Drive once fell back to the Google sign-in
//! client (`google_client_id`), which has neither the Knowledge redirect
//! address nor the Drive scope registered, so Google answered every sign-in
//! with `redirect_uri_mismatch`. A provider without its own app is "not set
//! up", and [`start_sign_in`] never builds an authorize address for it.

use crate::mcp_remote::oauth::ClientAuth;
use crate::mcp_remote::runtime::Runtime;
use crate::mcp_remote::store::{AuthKind, OAuthSettings, Owner, ServerRecord, Sharing};

use super::Kind;

/// What a provider's sign-in needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provider {
    pub issuer: &'static str,
    pub authorize: &'static str,
    pub token: &'static str,
    pub revoke: Option<&'static str>,
    pub scopes: &'static [&'static str],
    pub extra: &'static [(&'static str, &'static str)],
    pub client_auth: ClientAuth,
    pub json_body: bool,
    /// The configuration keys that hold the app's client id and secret.
    pub id_key: &'static str,
    pub secret_key: &'static str,
    /// Where an operator registers the app.
    pub console: &'static str,
}

/// The OAuth provider of a kind, when it signs in with OAuth.
pub fn provider(kind: Kind) -> Option<Provider> {
    match kind {
        Kind::Notion => Some(Provider {
            issuer: "https://api.notion.com",
            authorize: "https://api.notion.com/v1/oauth/authorize",
            token: "https://api.notion.com/v1/oauth/token",
            revoke: None,
            scopes: &[],
            extra: &[("owner", "user")],
            client_auth: ClientAuth::ClientSecretBasic,
            json_body: true,
            id_key: "notion_client_id",
            secret_key: "notion_client_secret",
            console: "https://www.notion.so/profile/integrations",
        }),
        Kind::Confluence => Some(Provider {
            issuer: "https://auth.atlassian.com",
            authorize: "https://auth.atlassian.com/authorize",
            token: "https://auth.atlassian.com/oauth/token",
            revoke: None,
            scopes: &[
                "read:page:confluence",
                "read:space:confluence",
                "read:hierarchical-content:confluence",
                "read:confluence-content.summary",
                "search:confluence",
                "offline_access",
            ],
            extra: &[("audience", "api.atlassian.com"), ("prompt", "consent")],
            client_auth: ClientAuth::ClientSecretPost,
            json_body: true,
            id_key: "atlassian_client_id",
            secret_key: "atlassian_client_secret",
            console: "https://developer.atlassian.com/console/myapps/",
        }),
        Kind::GoogleDrive => Some(Provider {
            issuer: "https://accounts.google.com",
            authorize: "https://accounts.google.com/o/oauth2/v2/auth",
            token: "https://oauth2.googleapis.com/token",
            revoke: Some("https://oauth2.googleapis.com/revoke"),
            scopes: &["https://www.googleapis.com/auth/drive.readonly"],
            extra: &[
                ("access_type", "offline"),
                ("prompt", "consent"),
                ("include_granted_scopes", "true"),
            ],
            client_auth: ClientAuth::ClientSecretPost,
            json_body: false,
            id_key: "google_drive_client_id",
            secret_key: "google_drive_client_secret",
            console: "https://console.cloud.google.com/apis/credentials",
        }),
        _ => None,
    }
}

/// The install's OAuth app for a provider.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct App {
    pub client_id: String,
    pub client_secret: String,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

impl App {
    pub fn is_set(&self) -> bool {
        !self.client_id.trim().is_empty() && !self.client_secret.trim().is_empty()
    }

    /// The app for `kind`, read from the provider's own keys
    /// ([`Provider::id_key`], [`Provider::secret_key`]) through `get`, which
    /// looks a key up in the install's secret. No other key is ever read: an
    /// app registered for something else (the Google sign-in client) lacks
    /// this provider's redirect address and scopes.
    pub fn from_keys(kind: Kind, get: impl Fn(&str) -> Option<String>) -> App {
        let Some(p) = provider(kind) else {
            return App::default();
        };
        let read = |key: &str| {
            get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .unwrap_or_default()
        };
        App {
            client_id: read(p.id_key),
            client_secret: read(p.secret_key),
        }
    }
}

/// How a sign-in reaches the provider.
#[derive(Debug, Clone, Copy)]
pub enum Via<'a> {
    /// The install's own app for the provider.
    App(&'a App),
    /// CoderHelm's broker, for a hybrid install that chose CoderHelm's app:
    /// the install id is the client id, its registration secret the secret.
    Broker {
        hosted: &'a str,
        install_id: &'a str,
        secret: &'a str,
    },
}

/// Who starts a sign-in, and the callback it returns to.
#[derive(Debug, Clone)]
pub struct Starter<'a> {
    pub user_sub: &'a str,
    pub user_email: &'a str,
    /// When a new connection record was added (the starter is who added it).
    pub added_at: &'a str,
    /// The callback page, exactly as registered with the provider's app.
    pub redirect_uri: &'a str,
}

/// Why a sign-in didn't start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartError {
    /// The install has no app for this provider (and no broker).
    NotSetUp,
    /// This kind doesn't sign in with OAuth.
    NotOAuth,
    /// Sealing or saving failed.
    Internal(String),
    /// The sign-in itself was refused.
    Refused(String),
}

/// Starts the team's one-click sign-in to `kind`: saves its connection
/// record (keeping who first added it and its status) and returns where to
/// send the browser. Refuses before building anything when the app isn't
/// set up, so an address with somebody else's client id never reaches the
/// provider.
pub async fn start_sign_in(
    rt: &Runtime,
    kind: Kind,
    via: Via<'_>,
    who: &Starter<'_>,
) -> Result<String, StartError> {
    let p = provider(kind).ok_or(StartError::NotOAuth)?;
    let (client_id, secret) = match via {
        Via::App(a) if a.is_set() => (a.client_id.trim(), a.client_secret.trim()),
        Via::Broker {
            install_id, secret, ..
        } if !install_id.trim().is_empty() && !secret.trim().is_empty() => {
            (install_id.trim(), secret.trim())
        }
        _ => return Err(StartError::NotSetUp),
    };
    let id = connection_id(kind);
    let sealed = rt
        .seal_client_secret(&id, secret)
        .await
        .map_err(StartError::Internal)?;
    let mut rec = match via {
        Via::Broker { hosted, .. } => brokered_connection_record(kind, hosted, client_id, &sealed),
        Via::App(_) => connection_record(kind, &p, client_id, &sealed),
    };
    rec.added_by = who.user_email.to_string();
    rec.added_at = who.added_at.to_string();
    if let Ok(Some(old)) = rt.store.get_server(&rt.team, &id).await {
        rec.status = old.status;
        rec.status_reason = old.status_reason;
        rec.added_by = old.added_by;
        rec.added_at = old.added_at;
    }
    rt.store
        .put_server(&rt.team, &rec)
        .await
        .map_err(StartError::Internal)?;
    rt.start_sign_in(
        &mut rec,
        &Owner::Team,
        who.user_sub,
        who.user_email,
        who.redirect_uri,
        None,
    )
    .await
    .map_err(StartError::Refused)
}

/// The id of a kind's connection record.
pub fn connection_id(kind: Kind) -> String {
    format!("ks-{}", kind.key().replace('_', "-"))
}

/// The `catalog_id` that marks a connection record as a knowledge source's.
pub fn catalog_id(kind: Kind) -> String {
    format!("source:{}", kind.key())
}

/// Is this MCP server record a knowledge source's connection (never shown
/// or used as an MCP server)?
pub fn is_source_connection(catalog_id: &str) -> bool {
    catalog_id.starts_with("source:")
}

/// The connection record for `kind`, signed in with `app` (its secret
/// already sealed for this record). The team owns the connection: a sync
/// runs on a schedule, not in anyone's session.
pub fn connection_record(
    kind: Kind,
    p: &Provider,
    client_id: &str,
    sealed_secret: &str,
) -> ServerRecord {
    let mut s = ServerRecord::new(
        &connection_id(kind),
        &format!("{} (knowledge)", kind.label()),
        p.issuer,
    );
    s.catalog_id = catalog_id(kind);
    s.auth = AuthKind::OAuth;
    s.sharing = Sharing::Shared;
    s.oauth = Some(OAuthSettings {
        issuer: p.issuer.into(),
        authorization_endpoint: p.authorize.into(),
        token_endpoint: p.token.into(),
        registration_endpoint: None,
        revocation_endpoint: p.revoke.map(str::to_string),
        iss_supported: false,
        cimd_supported: false,
        token_auth_methods: vec![p.client_auth.as_str().into()],
        resource: String::new(),
        scopes: p.scopes.iter().map(|s| s.to_string()).collect(),
        client_id: client_id.into(),
        client_auth: p.client_auth,
        client_secret_sealed: sealed_secret.into(),
        registration: "manual".into(),
        legacy: false,
        extra_authorize: p
            .extra
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        token_body_json: p.json_body,
    });
    s
}

/// The connection record for `kind` signed in through CoderHelm's broker
/// (a hybrid install that chose CoderHelm's app): the hosted side is the
/// authorization server, the install id is the client id and the
/// registration secret is the client secret. The provider's own scopes and
/// app live on the hosted side; refresh, sealing and reconnect work as for
/// any other connection.
pub fn brokered_connection_record(
    kind: Kind,
    hosted: &str,
    install_id: &str,
    sealed_secret: &str,
) -> ServerRecord {
    let hosted = hosted.trim_end_matches('/');
    let mut s = ServerRecord::new(
        &connection_id(kind),
        &format!("{} (knowledge)", kind.label()),
        hosted,
    );
    s.catalog_id = catalog_id(kind);
    s.auth = AuthKind::OAuth;
    s.sharing = Sharing::Shared;
    s.oauth = Some(OAuthSettings {
        issuer: hosted.into(),
        authorization_endpoint: format!("{hosted}/hybrid/oauth/{}/authorize", kind.key()),
        token_endpoint: format!("{hosted}/hybrid/oauth/token"),
        registration_endpoint: None,
        revocation_endpoint: Some(format!("{hosted}/hybrid/oauth/revoke")),
        iss_supported: false,
        cimd_supported: false,
        token_auth_methods: vec![ClientAuth::ClientSecretPost.as_str().into()],
        resource: String::new(),
        scopes: vec![],
        client_id: install_id.into(),
        client_auth: ClientAuth::ClientSecretPost,
        client_secret_sealed: sealed_secret.into(),
        registration: "manual".into(),
        legacy: false,
        extra_authorize: vec![],
        token_body_json: false,
    });
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_remote::http::fake::Fake;
    use crate::mcp_remote::runtime::testing::FakeKms;
    use crate::mcp_remote::store::fake::MemStore;
    use std::collections::HashMap;
    use std::sync::Arc;

    const REDIRECT: &str = "https://app.coderhelm.com/settings/integrations/mcp-callback/";

    fn rt() -> Runtime {
        Runtime {
            store: Arc::new(MemStore::default()),
            cipher: Arc::new(FakeKms),
            http: Arc::new(Fake::default()),
            team: "TEAM#a".into(),
            clock: Arc::new(|| 1_000_000),
        }
    }

    fn who() -> Starter<'static> {
        Starter {
            user_sub: "sub-1",
            user_email: "ada@example.com",
            added_at: "2026-10-26T00:00:00Z",
            redirect_uri: REDIRECT,
        }
    }

    fn secret(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The hosted secret as it was: the Google sign-in client only.
    fn sign_in_client_only() -> HashMap<String, String> {
        secret(&[
            ("google_client_id", "signin.apps.googleusercontent.com"),
            ("google_client_secret", "signin-secret"),
        ])
    }

    #[test]
    fn drive_never_borrows_the_google_sign_in_client() {
        let s = sign_in_client_only();
        let a = App::from_keys(Kind::GoogleDrive, |k| s.get(k).cloned());
        assert!(!a.is_set());
        assert_eq!(a, App::default());
        // Half an app is no app.
        let half = secret(&[("google_drive_client_id", "drive.apps.googleusercontent.com")]);
        assert!(!App::from_keys(Kind::GoogleDrive, |k| half.get(k).cloned()).is_set());
        let blank = secret(&[
            ("google_drive_client_id", "  "),
            ("google_drive_client_secret", "x"),
        ]);
        assert!(!App::from_keys(Kind::GoogleDrive, |k| blank.get(k).cloned()).is_set());
    }

    #[test]
    fn each_provider_reads_only_its_own_keys() {
        let s = secret(&[
            ("notion_client_id", " n-id "),
            ("notion_client_secret", "n-secret"),
            ("atlassian_client_id", "a-id"),
            ("atlassian_client_secret", "a-secret"),
            ("google_drive_client_id", "d-id"),
            ("google_drive_client_secret", "d-secret"),
            ("google_client_id", "signin"),
            ("google_client_secret", "signin-secret"),
        ]);
        let get = |k: &str| s.get(k).cloned();
        assert_eq!(App::from_keys(Kind::Notion, get).client_id, "n-id");
        assert_eq!(App::from_keys(Kind::Confluence, get).client_id, "a-id");
        let d = App::from_keys(Kind::GoogleDrive, get);
        assert_eq!(
            (d.client_id.as_str(), d.client_secret.as_str()),
            ("d-id", "d-secret")
        );
        assert!(!App::from_keys(Kind::Sanity, get).is_set());
    }

    #[tokio::test]
    async fn without_a_drive_app_no_authorize_address_is_built() {
        let s = sign_in_client_only();
        let a = App::from_keys(Kind::GoogleDrive, |k| s.get(k).cloned());
        let rt = rt();
        let r = start_sign_in(&rt, Kind::GoogleDrive, Via::App(&a), &who()).await;
        assert_eq!(r, Err(StartError::NotSetUp));
        // Nothing was saved: no record, so no sign-in can follow from it.
        assert!(rt
            .store
            .get_server(&rt.team, &connection_id(Kind::GoogleDrive))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn a_drive_app_signs_in_with_exactly_the_registered_redirect() {
        let s = secret(&[
            ("google_drive_client_id", "drive.apps.googleusercontent.com"),
            ("google_drive_client_secret", "drive-secret"),
            ("google_client_id", "signin.apps.googleusercontent.com"),
            ("google_client_secret", "signin-secret"),
        ]);
        let a = App::from_keys(Kind::GoogleDrive, |k| s.get(k).cloned());
        let url = start_sign_in(&rt(), Kind::GoogleDrive, Via::App(&a), &who())
            .await
            .unwrap();
        let u = reqwest::Url::parse(&url).unwrap();
        assert_eq!(u.host_str(), Some("accounts.google.com"));
        assert_eq!(u.path(), "/o/oauth2/v2/auth");
        let q: HashMap<String, String> = u.query_pairs().into_owned().collect();
        assert_eq!(q["redirect_uri"], REDIRECT);
        assert_eq!(q["client_id"], "drive.apps.googleusercontent.com");
        assert_eq!(q["scope"], "https://www.googleapis.com/auth/drive.readonly");
        assert_eq!(q["access_type"], "offline");
        assert_eq!(q["code_challenge_method"], "S256");
        assert!(!url.contains("signin.apps"));
        assert_eq!(
            u.query_pairs().filter(|(k, _)| k == "redirect_uri").count(),
            1
        );
    }

    #[tokio::test]
    async fn a_broker_without_credentials_is_not_set_up_either() {
        let r = start_sign_in(
            &rt(),
            Kind::GoogleDrive,
            Via::Broker {
                hosted: "https://api.coderhelm.com",
                install_id: "",
                secret: "",
            },
            &who(),
        )
        .await;
        assert_eq!(r, Err(StartError::NotSetUp));
        assert_eq!(
            start_sign_in(&rt(), Kind::Web, Via::App(&App::default()), &who()).await,
            Err(StartError::NotOAuth)
        );
    }

    #[test]
    fn brokered_records_point_at_the_hosted_broker() {
        let r = brokered_connection_record(
            Kind::Notion,
            "https://api.coderhelm.com/",
            "ins_abcdefghijklmnopqrstuvwx",
            "sealed",
        );
        let o = r.oauth.unwrap();
        assert_eq!(
            o.authorization_endpoint,
            "https://api.coderhelm.com/hybrid/oauth/notion/authorize"
        );
        assert_eq!(
            o.token_endpoint,
            "https://api.coderhelm.com/hybrid/oauth/token"
        );
        assert_eq!(o.client_id, "ins_abcdefghijklmnopqrstuvwx");
        assert_eq!(o.client_auth, ClientAuth::ClientSecretPost);
        assert!(is_source_connection(&r.catalog_id));
        assert!(r.tools.is_empty());
    }

    #[test]
    fn oauth_kinds_have_providers_and_records() {
        for k in [Kind::Notion, Kind::Confluence, Kind::GoogleDrive] {
            let p = provider(k).unwrap();
            assert!(p.authorize.starts_with("https://") && p.token.starts_with("https://"));
            let r = connection_record(k, &p, "cid", "sealed");
            assert!(is_source_connection(&r.catalog_id));
            assert_eq!(r.auth, AuthKind::OAuth);
            assert!(r.tools.is_empty(), "never an MCP server for agents");
            let o = r.oauth.unwrap();
            assert_eq!(o.registration, "manual");
            assert!(o.resource.is_empty());
        }
        for k in [Kind::Sanity, Kind::Github, Kind::Web] {
            assert!(provider(k).is_none());
        }
        assert_eq!(connection_id(Kind::GoogleDrive), "ks-google-drive");
        assert!(!format!(
            "{:?}",
            App {
                client_id: "a".into(),
                client_secret: "s3cret".into()
            }
        )
        .contains("s3cret"));
    }

    #[test]
    fn google_asks_for_offline_access_and_read_only_drive() {
        let p = provider(Kind::GoogleDrive).unwrap();
        assert!(p.extra.contains(&("access_type", "offline")));
        assert_eq!(p.scopes, ["https://www.googleapis.com/auth/drive.readonly"]);
    }
}
