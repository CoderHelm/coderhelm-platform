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

use crate::mcp_remote::oauth::ClientAuth;
use crate::mcp_remote::store::{AuthKind, OAuthSettings, ServerRecord, Sharing};

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

#[cfg(test)]
mod tests {
    use super::*;

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
