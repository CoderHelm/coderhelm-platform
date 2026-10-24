//! Finding out how a remote MCP server wants to be signed in to.
//!
//! Follows the MCP authorization spec (revision 2026-07-28):
//!
//! 1. The server answers an unauthenticated request with `401` and a
//!    `WWW-Authenticate: Bearer resource_metadata="..."` header. That URL is
//!    used first; without it, the RFC 9728 well-known addresses are tried,
//!    the path form before the root.
//! 2. The protected resource metadata names the authorization server. Its
//!    RFC 8414 metadata is fetched (OpenID Connect discovery as a fallback,
//!    with path insertion), and the `issuer` inside must be exactly the one
//!    used to build the address.
//! 3. An authorization server that does not list `S256` in
//!    `code_challenge_methods_supported` is refused: no PKCE, no connection.
//!
//! A server with no protected resource metadata at all (some vendors still
//! follow the 2025-03-26 revision) is tried once more with the server's own
//! origin as the authorization server, so those servers still connect.

use serde_json::Value;

use super::http::{check_url, Http, HttpError, HttpRequest};

/// The parameters of a `Bearer` challenge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Challenge {
    pub resource_metadata: Option<String>,
    pub scope: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}

/// Parses a `WWW-Authenticate` value. Only the `Bearer` challenge is read;
/// other schemes in the same header are skipped.
pub fn parse_challenge(header: &str) -> Option<Challenge> {
    let lower = header.to_ascii_lowercase();
    let at = find_scheme(&lower, "bearer")?;
    let mut rest = &header[at + "bearer".len()..];
    let mut c = Challenge::default();
    loop {
        rest = rest.trim_start_matches([' ', ',', '\t']);
        if rest.is_empty() {
            break;
        }
        let Some(eq) = rest.find('=') else { break };
        let key = rest[..eq].trim().to_ascii_lowercase();
        // A bare word with no `=` before the next space starts another scheme.
        if key.contains(' ') {
            break;
        }
        rest = &rest[eq + 1..];
        let (value, after) = if let Some(stripped) = rest.strip_prefix('"') {
            let mut out = String::new();
            let mut chars = stripped.char_indices();
            let mut end = stripped.len();
            while let Some((i, ch)) = chars.next() {
                match ch {
                    '\\' => {
                        if let Some((_, n)) = chars.next() {
                            out.push(n);
                        }
                    }
                    '"' => {
                        end = i + 1;
                        break;
                    }
                    _ => out.push(ch),
                }
            }
            (out, &stripped[end.min(stripped.len())..])
        } else {
            let end = rest.find([',', ' ']).unwrap_or(rest.len());
            (rest[..end].to_string(), &rest[end..])
        };
        rest = after;
        match key.as_str() {
            "resource_metadata" => c.resource_metadata = Some(value),
            "scope" => c.scope = Some(value),
            "error" => c.error = Some(value),
            "error_description" => c.error_description = Some(value),
            _ => {}
        }
    }
    Some(c)
}

fn find_scheme(lower: &str, scheme: &str) -> Option<usize> {
    let mut from = 0;
    while let Some(i) = lower[from..].find(scheme) {
        let at = from + i;
        let before_ok = at == 0 || matches!(lower.as_bytes()[at - 1], b' ' | b',');
        let after = lower.as_bytes().get(at + scheme.len()).copied();
        if before_ok && matches!(after, None | Some(b' ')) {
            return Some(at);
        }
        from = at + scheme.len();
    }
    None
}

/// The server's address in the form used as the token audience when its
/// metadata names none: lowercase scheme and host, no trailing slash, no
/// query or fragment.
pub fn canonical_resource(url: &reqwest::Url) -> String {
    let mut s = format!(
        "{}://{}",
        url.scheme(),
        url.host_str().unwrap_or("").to_ascii_lowercase()
    );
    if let Some(p) = url.port() {
        s.push_str(&format!(":{p}"));
    }
    let path = url.path().trim_end_matches('/');
    s.push_str(path);
    s
}

/// The RFC 9728 well-known addresses for a server, path form first.
pub fn resource_metadata_urls(server: &reqwest::Url) -> Vec<String> {
    let origin = origin_of(server);
    let path = server.path().trim_end_matches('/');
    let mut out = Vec::new();
    if !path.is_empty() {
        out.push(format!(
            "{origin}/.well-known/oauth-protected-resource{path}"
        ));
    }
    out.push(format!("{origin}/.well-known/oauth-protected-resource"));
    out
}

fn origin_of(u: &reqwest::Url) -> String {
    let mut s = format!("{}://{}", u.scheme(), u.host_str().unwrap_or(""));
    if let Some(p) = u.port() {
        s.push_str(&format!(":{p}"));
    }
    s
}

/// The authorization server metadata addresses for an issuer, in the order
/// the spec gives.
pub fn auth_server_metadata_urls(issuer: &str) -> Vec<String> {
    let Ok(u) = reqwest::Url::parse(issuer) else {
        return Vec::new();
    };
    let origin = origin_of(&u);
    let path = u.path().trim_end_matches('/');
    if path.is_empty() {
        vec![
            format!("{origin}/.well-known/oauth-authorization-server"),
            format!("{origin}/.well-known/openid-configuration"),
        ]
    } else {
        vec![
            format!("{origin}/.well-known/oauth-authorization-server{path}"),
            format!("{origin}/.well-known/openid-configuration{path}"),
            format!("{origin}{path}/.well-known/openid-configuration"),
        ]
    }
}

/// Protected resource metadata, as much of it as is used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceMetadata {
    pub resource: String,
    pub authorization_servers: Vec<String>,
    pub scopes_supported: Vec<String>,
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Reads protected resource metadata and checks it names this server: the
/// `resource` must be on the same origin as the server and its path must be
/// the server's path or a parent of it. A document naming some other
/// resource would have us ask for tokens meant for it, so it is refused.
pub fn parse_resource_metadata(
    doc: &Value,
    server: &reqwest::Url,
) -> Result<ResourceMetadata, String> {
    let resource = doc["resource"].as_str().unwrap_or("").trim().to_string();
    let servers = strings(&doc["authorization_servers"]);
    if servers.is_empty() {
        return Err("The server's metadata names no sign-in server.".into());
    }
    if !resource.is_empty() {
        let r = reqwest::Url::parse(&resource)
            .map_err(|_| "The server's metadata names an unreadable resource.".to_string())?;
        let same_origin = r.scheme() == server.scheme()
            && r.host_str().map(str::to_ascii_lowercase)
                == server.host_str().map(str::to_ascii_lowercase)
            && r.port_or_known_default() == server.port_or_known_default();
        let rp = r.path().trim_end_matches('/');
        let sp = server.path().trim_end_matches('/');
        let parent = sp == rp || sp.starts_with(&format!("{rp}/")) || rp.is_empty();
        if !same_origin || !parent {
            return Err(format!(
                "The server's metadata is for {resource}, not this address."
            ));
        }
    }
    Ok(ResourceMetadata {
        resource: if resource.is_empty() {
            canonical_resource(server)
        } else {
            resource
        },
        authorization_servers: servers,
        scopes_supported: strings(&doc["scopes_supported"]),
    })
}

/// Authorization server metadata, as much of it as is used.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuthServer {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub registration_endpoint: Option<String>,
    pub revocation_endpoint: Option<String>,
    pub scopes_supported: Vec<String>,
    pub token_auth_methods: Vec<String>,
    pub cimd_supported: bool,
    pub iss_supported: bool,
}

/// Reads authorization server metadata fetched for `expected_issuer`.
pub fn parse_auth_server(doc: &Value, expected_issuer: &str) -> Result<AuthServer, String> {
    let issuer = doc["issuer"].as_str().unwrap_or("");
    if issuer != expected_issuer {
        return Err(format!(
            "The sign-in server's metadata names issuer {issuer:?}, not {expected_issuer:?}."
        ));
    }
    let https = |k: &str| -> Result<String, String> {
        let v = doc[k].as_str().unwrap_or("").to_string();
        match reqwest::Url::parse(&v) {
            Ok(u) if u.scheme() == "https" => Ok(v),
            _ => Err(format!(
                "The sign-in server's {k} is missing or not an https address."
            )),
        }
    };
    let optional_https = |k: &str| -> Option<String> {
        doc[k]
            .as_str()
            .filter(|v| reqwest::Url::parse(v).is_ok_and(|u| u.scheme() == "https"))
            .map(str::to_string)
    };
    let methods = strings(&doc["code_challenge_methods_supported"]);
    if !methods.iter().any(|m| m == "S256") {
        return Err(
            "The sign-in server doesn't support PKCE (S256), so Coderhelm won't connect to it."
                .into(),
        );
    }
    Ok(AuthServer {
        issuer: issuer.to_string(),
        authorization_endpoint: https("authorization_endpoint")?,
        token_endpoint: https("token_endpoint")?,
        registration_endpoint: optional_https("registration_endpoint"),
        revocation_endpoint: optional_https("revocation_endpoint"),
        scopes_supported: strings(&doc["scopes_supported"]),
        token_auth_methods: strings(&doc["token_endpoint_auth_methods_supported"]),
        cimd_supported: doc["client_id_metadata_document_supported"].as_bool() == Some(true),
        iss_supported: doc["authorization_response_iss_parameter_supported"].as_bool()
            == Some(true),
    })
}

/// How Coderhelm signs in to a server, worked out from its metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthPlan {
    /// The token audience (RFC 8707 `resource`), exactly as the server's
    /// metadata gives it.
    pub resource: String,
    /// The scopes to ask for: the challenge's, else everything the server
    /// lists, else none (the `scope` parameter is then left out).
    pub scopes: Vec<String>,
    pub auth_server: AuthServer,
    /// The server publishes no protected resource metadata (pre-2025-06-18
    /// behavior); its origin was used as the authorization server.
    pub legacy: bool,
}

async fn fetch_json(http: &dyn Http, url: &str) -> Result<Option<Value>, HttpError> {
    let r = http.send(HttpRequest::get(url)).await?;
    if r.status == 200 {
        Ok(r.json())
    } else {
        Ok(None)
    }
}

/// Finds the authorization server for a server that answered `401`.
pub async fn discover(
    http: &dyn Http,
    server_url: &str,
    challenge: Option<&Challenge>,
) -> Result<Option<OAuthPlan>, String> {
    let server = check_url(server_url).map_err(|e| e.to_string())?.url;
    let mut candidates = Vec::new();
    if let Some(u) = challenge.and_then(|c| c.resource_metadata.clone()) {
        candidates.push(u);
    }
    candidates.extend(resource_metadata_urls(&server));
    let mut prm = None;
    for url in candidates {
        // A metadata address that our guard refuses (private, not https) is
        // skipped like a missing one.
        match fetch_json(http, &url).await {
            Ok(Some(doc)) => {
                prm = Some(parse_resource_metadata(&doc, &server)?);
                break;
            }
            Ok(None) | Err(HttpError::Refused(_)) => {}
            Err(HttpError::Unreachable(m)) => return Err(m),
        }
    }
    let (resource, issuers, scopes_supported, legacy) = match prm {
        Some(p) => (
            p.resource,
            p.authorization_servers,
            p.scopes_supported,
            false,
        ),
        None => (
            canonical_resource(&server),
            vec![origin_of(&server)],
            Vec::new(),
            true,
        ),
    };
    let mut last_err = None;
    for issuer in issuers.iter().take(3) {
        if check_url(issuer).is_err() {
            last_err = Some(format!(
                "The sign-in server {issuer} can't be reached from here."
            ));
            continue;
        }
        for url in auth_server_metadata_urls(issuer) {
            match fetch_json(http, &url).await {
                Ok(Some(doc)) => {
                    let auth_server = parse_auth_server(&doc, issuer)?;
                    let scopes = match challenge.and_then(|c| c.scope.as_deref()) {
                        Some(s) if !s.trim().is_empty() => {
                            s.split_whitespace().map(str::to_string).collect()
                        }
                        _ => scopes_supported.clone(),
                    };
                    return Ok(Some(OAuthPlan {
                        resource,
                        scopes,
                        auth_server,
                        legacy,
                    }));
                }
                Ok(None) | Err(HttpError::Refused(_)) => {}
                Err(HttpError::Unreachable(m)) => last_err = Some(m),
            }
        }
    }
    if legacy {
        // No metadata anywhere: not an OAuth server.
        return Ok(None);
    }
    Err(last_err.unwrap_or_else(|| "The sign-in server's metadata couldn't be found.".into()))
}

#[cfg(test)]
mod tests {
    use super::super::http::fake::{json, Fake};
    use super::super::http::Method;
    use super::*;
    use serde_json::json as j;

    fn url(s: &str) -> reqwest::Url {
        reqwest::Url::parse(s).unwrap()
    }

    #[test]
    fn challenges_are_parsed() {
        let c = parse_challenge(
            r#"Bearer resource_metadata="https://mcp.example.com/.well-known/oauth-protected-resource/mcp", scope="read write", error="invalid_token""#,
        )
        .unwrap();
        assert_eq!(
            c.resource_metadata.as_deref(),
            Some("https://mcp.example.com/.well-known/oauth-protected-resource/mcp")
        );
        assert_eq!(c.scope.as_deref(), Some("read write"));
        assert_eq!(c.error.as_deref(), Some("invalid_token"));
        // Other schemes first, escapes, unquoted values.
        let c = parse_challenge(r#"Basic realm="x", Bearer error=insufficient_scope, error_description="needs \"files\"""#).unwrap();
        assert_eq!(c.error.as_deref(), Some("insufficient_scope"));
        assert_eq!(c.error_description.as_deref(), Some(r#"needs "files""#));
        assert!(parse_challenge(r#"Basic realm="x""#).is_none());
        assert!(parse_challenge("Bearer")
            .unwrap()
            .resource_metadata
            .is_none());
    }

    #[test]
    fn well_known_addresses_follow_the_spec() {
        assert_eq!(
            resource_metadata_urls(&url("https://mcp.example.com/v1/mcp/")),
            vec![
                "https://mcp.example.com/.well-known/oauth-protected-resource/v1/mcp",
                "https://mcp.example.com/.well-known/oauth-protected-resource",
            ]
        );
        assert_eq!(
            resource_metadata_urls(&url("https://mcp.example.com")),
            vec!["https://mcp.example.com/.well-known/oauth-protected-resource"]
        );
        assert_eq!(
            auth_server_metadata_urls("https://auth.example.com/tenant1"),
            vec![
                "https://auth.example.com/.well-known/oauth-authorization-server/tenant1",
                "https://auth.example.com/.well-known/openid-configuration/tenant1",
                "https://auth.example.com/tenant1/.well-known/openid-configuration",
            ]
        );
        assert_eq!(
            auth_server_metadata_urls("https://auth.example.com"),
            vec![
                "https://auth.example.com/.well-known/oauth-authorization-server",
                "https://auth.example.com/.well-known/openid-configuration",
            ]
        );
    }

    #[test]
    fn canonical_resource_drops_the_trailing_slash_and_case() {
        assert_eq!(
            canonical_resource(&url("HTTPS://MCP.Example.com/mcp/")),
            "https://mcp.example.com/mcp"
        );
        assert_eq!(
            canonical_resource(&url("https://mcp.example.com:8443")),
            "https://mcp.example.com:8443"
        );
    }

    #[test]
    fn resource_metadata_must_name_this_server() {
        let server = url("https://mcp.example.com/mcp");
        let ok = parse_resource_metadata(
            &j!({"resource": "https://mcp.example.com", "authorization_servers": ["https://auth.example.com"], "scopes_supported": ["read"]}),
            &server,
        )
        .unwrap();
        assert_eq!(ok.resource, "https://mcp.example.com");
        assert_eq!(ok.scopes_supported, vec!["read"]);
        // The exact path, or with a trailing slash, is fine.
        assert!(parse_resource_metadata(&j!({"resource": "https://mcp.example.com/mcp/", "authorization_servers": ["https://a.example.com"]}), &server).is_ok());
        for bad in [
            "https://evil.example.net/mcp",
            "https://mcp.example.com/other",
            "https://mcp.example.com/mcpx",
            "http://mcp.example.com/mcp",
        ] {
            assert!(
                parse_resource_metadata(
                    &j!({"resource": bad, "authorization_servers": ["https://a.example.com"]}),
                    &server
                )
                .is_err(),
                "{bad}"
            );
        }
        assert!(
            parse_resource_metadata(&j!({"resource": "https://mcp.example.com"}), &server).is_err()
        );
    }

    fn as_doc(issuer: &str) -> Value {
        j!({
            "issuer": issuer,
            "authorization_endpoint": format!("{issuer}/authorize"),
            "token_endpoint": format!("{issuer}/token"),
            "registration_endpoint": format!("{issuer}/register"),
            "revocation_endpoint": format!("{issuer}/revoke"),
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none", "client_secret_post"],
            "client_id_metadata_document_supported": true,
            "authorization_response_iss_parameter_supported": true
        })
    }

    #[test]
    fn auth_server_metadata_is_checked() {
        let a = parse_auth_server(
            &as_doc("https://auth.example.com"),
            "https://auth.example.com",
        )
        .unwrap();
        assert_eq!(a.token_endpoint, "https://auth.example.com/token");
        assert!(a.cimd_supported && a.iss_supported);
        assert_eq!(
            a.registration_endpoint.as_deref(),
            Some("https://auth.example.com/register")
        );
        // Issuer mismatch (mix-up defence).
        assert!(parse_auth_server(
            &as_doc("https://evil.example.net"),
            "https://auth.example.com"
        )
        .is_err());
        // No PKCE: refuse.
        let mut d = as_doc("https://auth.example.com");
        d["code_challenge_methods_supported"] = j!(["plain"]);
        assert!(parse_auth_server(&d, "https://auth.example.com")
            .unwrap_err()
            .contains("PKCE"));
        d.as_object_mut()
            .unwrap()
            .remove("code_challenge_methods_supported");
        assert!(parse_auth_server(&d, "https://auth.example.com").is_err());
        // Plain-http endpoints are refused.
        let mut d = as_doc("https://auth.example.com");
        d["token_endpoint"] = j!("http://auth.example.com/token");
        assert!(parse_auth_server(&d, "https://auth.example.com").is_err());
    }

    #[tokio::test]
    async fn discovery_follows_the_challenge_then_well_known() {
        let f = Fake::default();
        f.on(
            Method::Get,
            "https://mcp.example.com/.well-known/oauth-protected-resource/mcp",
            json(200, j!({"resource": "https://mcp.example.com/mcp", "authorization_servers": ["https://auth.example.com/t1"], "scopes_supported": ["a", "b"]})),
        );
        f.on(
            Method::Get,
            "https://auth.example.com/.well-known/oauth-authorization-server/t1",
            json(200, as_doc("https://auth.example.com/t1")),
        );
        let plan = discover(&f, "https://mcp.example.com/mcp", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(plan.resource, "https://mcp.example.com/mcp");
        assert_eq!(plan.scopes, vec!["a", "b"]);
        assert_eq!(plan.auth_server.issuer, "https://auth.example.com/t1");
        assert!(!plan.legacy);
        // The challenge's scope wins over scopes_supported.
        let c = Challenge {
            scope: Some("files:read".into()),
            ..Default::default()
        };
        let plan = discover(&f, "https://mcp.example.com/mcp", Some(&c))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(plan.scopes, vec!["files:read"]);
    }

    #[tokio::test]
    async fn a_private_resource_metadata_address_is_never_fetched() {
        let f = Fake::default();
        f.on(
            Method::Get,
            "https://mcp.example.com/.well-known/oauth-protected-resource",
            json(200, j!({"resource": "https://mcp.example.com", "authorization_servers": ["https://auth.example.com"]})),
        );
        f.on(
            Method::Get,
            "https://auth.example.com/.well-known/oauth-authorization-server",
            json(200, as_doc("https://auth.example.com")),
        );
        let c = Challenge {
            resource_metadata: Some("https://169.254.169.254/latest/meta-data".into()),
            ..Default::default()
        };
        let plan = discover(&f, "https://mcp.example.com/mcp", Some(&c))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(plan.auth_server.issuer, "https://auth.example.com");
        assert!(f.sent().iter().all(|r| !r.url.contains("169.254")));
    }

    #[tokio::test]
    async fn no_resource_metadata_falls_back_to_the_origin() {
        let f = Fake::default();
        f.on(
            Method::Get,
            "https://mcp.example.com/.well-known/oauth-authorization-server",
            json(200, as_doc("https://mcp.example.com")),
        );
        let plan = discover(&f, "https://mcp.example.com/v1/mcp", None)
            .await
            .unwrap()
            .unwrap();
        assert!(plan.legacy);
        assert_eq!(plan.resource, "https://mcp.example.com/v1/mcp");
        // Nothing at all: not OAuth.
        let empty = Fake::default();
        assert!(discover(&empty, "https://mcp.example.com/v1/mcp", None)
            .await
            .unwrap()
            .is_none());
    }
}
