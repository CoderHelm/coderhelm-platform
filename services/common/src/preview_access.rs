//! A repository's preview access: optional, per repository, one Settings
//! item (Settings, Repositories, Preview access).
//!
//! Most previews are found and opened with nothing set (`preview_find`
//! reads deployments, statuses, checks and comments). Two things only a
//! team can give, and they live here:
//!
//! - the preview's address as a template (`https://pr-{pr}.preview.example`),
//!   for a host that publishes it nowhere CoderHelm can read
//!   (`preview_find::expand`);
//! - how an automated browser gets through a protected preview, with one
//!   secret the team stores:
//!   - [`Kind::Header`]: one to four `Name: value` lines sent with every
//!     request to the preview's host only (a protection bypass header, a
//!     sign-in wall's service token pair);
//!   - [`Kind::Query`]: `name=value` added to each page's address (a bypass
//!     parameter that sets a cookie);
//!   - [`Kind::Basic`]: `user:password` answered to the preview host's
//!     password prompt.
//!
//! The secret is sealed by KMS bound to the team, the repository and this
//! purpose, never returned by the API, handed to the sandbox's capture
//! through its one-time secret object (never in the script, its environment
//! or its log) and only ever sent to the preview's own host.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};

/// The settings-table sort key for a repository's preview access.
pub fn sk(repo: &str) -> String {
    format!("PREVIEW_ACCESS#REPO#{repo}")
}

/// Where the sealed secret is kept on the record.
pub const SECRET_ATTR: &str = "secret_sealed";
const PURPOSE: &str = "preview-access";
/// The longest secret kept.
pub const MAX_SECRET: usize = 2000;
/// Header lines a header secret may hold.
pub const MAX_HEADERS: usize = 4;

/// How the preview is protected.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Open, or only the address is set.
    #[default]
    None,
    Header,
    Query,
    Basic,
}

impl Kind {
    pub fn parse(s: &str) -> Option<Kind> {
        match s.trim() {
            "" | "none" => Some(Kind::None),
            "header" => Some(Kind::Header),
            "query" => Some(Kind::Query),
            "basic" => Some(Kind::Basic),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::None => "none",
            Kind::Header => "header",
            Kind::Query => "query",
            Kind::Basic => "basic",
        }
    }
}

/// A repository's preview access, without its secret.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PreviewAccess {
    /// `owner/name`.
    pub repo: String,
    pub kind: Kind,
    /// The preview's address template, "" when the preview publishes its
    /// own address.
    pub address: String,
}

/// Headers a secret may not set: the browser owns them.
const OWNED_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "connection",
    "transfer-encoding",
    "upgrade",
    "te",
    "trailer",
    "keep-alive",
];

fn token_chars(s: &str, extra: &[char]) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || extra.contains(&c))
}

/// Checks a secret's shape for its kind; the error is one sentence a person
/// can act on and never repeats the secret.
pub fn check_secret(kind: Kind, secret: &str) -> Result<(), String> {
    if secret.len() > MAX_SECRET {
        return Err("The secret is too long.".into());
    }
    if secret
        .chars()
        .any(|c| c.is_control() && c != '\n' && c != '\r')
    {
        return Err("The secret can't hold control characters.".into());
    }
    match kind {
        Kind::None => Ok(()),
        Kind::Header => {
            let lines: Vec<&str> = secret
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .collect();
            if lines.is_empty() || lines.len() > MAX_HEADERS {
                return Err(format!(
                    "Enter one to {MAX_HEADERS} lines, each `Header-Name: value`."
                ));
            }
            for l in lines {
                let Some((name, value)) = l.split_once(':') else {
                    return Err("Each line is `Header-Name: value`.".into());
                };
                let name = name.trim();
                if !token_chars(name, &['-', '_']) {
                    return Err(format!("`{name}` isn't a header name."));
                }
                if OWNED_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
                    return Err(format!("`{name}` is set by the browser itself."));
                }
                if value.trim().is_empty() {
                    return Err(format!("The `{name}` header has no value."));
                }
            }
            Ok(())
        }
        Kind::Query => {
            let s = secret.trim();
            if s.contains(char::is_whitespace) || s.starts_with('?') || s.contains('#') {
                return Err("Enter the parameters as `name=value` (join several with &).".into());
            }
            for pair in s.split('&') {
                let Some((name, value)) = pair.split_once('=') else {
                    return Err(
                        "Enter the parameters as `name=value` (join several with &).".into(),
                    );
                };
                if !token_chars(name, &['-', '_', '.']) || value.is_empty() {
                    return Err(
                        "Enter the parameters as `name=value` (join several with &).".into(),
                    );
                }
            }
            Ok(())
        }
        Kind::Basic => match secret.trim().split_once(':') {
            Some((user, pass)) if !user.is_empty() && !pass.is_empty() && !user.contains('\n') => {
                Ok(())
            }
            _ => Err("Enter the user name and password as `user:password`.".into()),
        },
    }
}

impl PreviewAccess {
    /// Checks the access as a team entered it.
    pub fn validate(&self) -> Result<(), String> {
        if !self.repo.contains('/') || self.repo.len() > 200 {
            return Err("Pick the repository this is for.".into());
        }
        crate::preview_find::check_template(&self.address)?;
        if self.kind == Kind::None && self.address.trim().is_empty() {
            return Err(
                "Set the preview's address, how it is protected, or both. With neither, remove it."
                    .into(),
            );
        }
        Ok(())
    }

    /// The record, without the secret.
    pub fn item(&self, team_id: &str) -> HashMap<String, AttributeValue> {
        let s = |v: &str| AttributeValue::S(v.to_string());
        HashMap::from([
            ("pk".to_string(), s(team_id)),
            ("sk".to_string(), s(&sk(&self.repo))),
            ("repo".to_string(), s(&self.repo)),
            ("kind".to_string(), s(self.kind.as_str())),
            ("address".to_string(), s(&self.address)),
        ])
    }

    /// Reads a record. `None` when it isn't one.
    pub fn from_item(item: &HashMap<String, AttributeValue>) -> Option<PreviewAccess> {
        let s = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        let a = PreviewAccess {
            repo: s("repo"),
            kind: Kind::parse(&s("kind")).unwrap_or_default(),
            address: s("address"),
        };
        (!a.repo.is_empty()).then_some(a)
    }

    /// The API's view: never the secret, only whether one is set.
    pub fn view(&self, secret_set: bool) -> serde_json::Value {
        serde_json::json!({
            "configured": true,
            "repo": self.repo,
            "kind": self.kind.as_str(),
            "address": self.address,
            "secret_set": secret_set,
        })
    }
}

/// The encryption context: the team, the repository and the purpose.
pub fn context(team_id: &str, repo: &str) -> HashMap<String, String> {
    HashMap::from([
        ("team".to_string(), team_id.to_string()),
        ("repo".to_string(), repo.to_string()),
        ("purpose".to_string(), PURPOSE.to_string()),
    ])
}

/// Seals a secret for one team's repository. Returns text safe to store.
pub async fn seal(
    cipher: &dyn crate::mcp_credentials::Cipher,
    team_id: &str,
    repo: &str,
    secret: &str,
) -> Result<String, String> {
    use base64::Engine;
    let blob = cipher
        .encrypt(context(team_id, repo), secret.as_bytes())
        .await?;
    Ok(base64::engine::general_purpose::STANDARD.encode(blob))
}

/// Opens a secret sealed for one team's repository.
pub async fn open(
    cipher: &dyn crate::mcp_credentials::Cipher,
    team_id: &str,
    repo: &str,
    sealed: &str,
) -> Result<String, String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(sealed)
        .map_err(|_| "stored secret is not valid".to_string())?;
    let plain = cipher.decrypt(context(team_id, repo), &bytes).await?;
    String::from_utf8(plain).map_err(|_| "stored secret is not text".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_checked_for_their_kind_without_repeating_them() {
        assert!(check_secret(Kind::Header, "X-Bypass-Token: s3cr3t").is_ok());
        assert!(check_secret(
            Kind::Header,
            "X-Client-Id: abc.access\nX-Client-Secret: def\n"
        )
        .is_ok());
        let e = check_secret(Kind::Header, "no-colon-s3cr3t").unwrap_err();
        assert!(!e.contains("s3cr3t"), "{e}");
        assert!(check_secret(Kind::Header, "Host: evil.example").is_err());
        assert!(check_secret(Kind::Header, "A: 1\nB: 2\nC: 3\nD: 4\nE: 5").is_err());
        assert!(check_secret(Kind::Query, "bypass=abc123").is_ok());
        assert!(check_secret(Kind::Query, "bypass=abc&set-cookie=true").is_ok());
        assert!(check_secret(Kind::Query, "?bypass=abc").is_err());
        assert!(check_secret(Kind::Query, "abc").is_err());
        assert!(check_secret(Kind::Basic, "preview:hunter2").is_ok());
        assert!(check_secret(Kind::Basic, "hunter2").is_err());
        assert!(check_secret(Kind::Basic, &"x".repeat(MAX_SECRET + 1)).is_err());
    }

    #[test]
    fn an_access_needs_an_address_or_a_protection_and_a_valid_template() {
        let a = |kind, address: &str| PreviewAccess {
            repo: "acme/web".into(),
            kind,
            address: address.into(),
        };
        assert!(a(Kind::None, "").validate().is_err());
        assert!(a(Kind::Header, "").validate().is_ok());
        assert!(a(Kind::None, "https://pr-{pr}.preview.example.com")
            .validate()
            .is_ok());
        assert!(a(Kind::None, "https://staging.example.com")
            .validate()
            .is_err());
    }

    #[test]
    fn the_record_round_trips_without_its_secret() {
        let a = PreviewAccess {
            repo: "acme/web".into(),
            kind: Kind::Query,
            address: "https://pr-{pr}.preview.example.com".into(),
        };
        let item = a.item("team-1");
        assert!(!item.contains_key(SECRET_ATTR));
        assert_eq!(PreviewAccess::from_item(&item), Some(a.clone()));
        let v = a.view(true);
        assert_eq!(v["kind"], "query");
        assert_eq!(v["secret_set"], true);
        assert!(v.get("secret").is_none());
    }

    #[test]
    fn the_seal_is_bound_to_the_team_repository_and_purpose() {
        let c = context("team-1", "acme/web");
        assert_eq!(c["purpose"], "preview-access");
        assert_eq!(c["repo"], "acme/web");
    }
}
