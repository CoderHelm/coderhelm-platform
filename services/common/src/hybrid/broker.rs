//! The hosted token broker's rules.
//!
//! A hybrid install uses CoderHelm's GitHub App and, where it chose to,
//! CoderHelm's OAuth apps. It never holds those apps' private keys or client
//! secrets: it asks the hosted broker for a short-lived token, signed with its
//! registration secret, and the broker mints one only within what the install
//! was linked to. These functions decide that, so the gateway's handlers and
//! the tests share one answer.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// What an install asks for: an installation token, optionally narrowed to
/// some repositories and fewer permissions.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct TokenAsk {
    pub installation_id: u64,
    /// Repository names (without the owner). Empty: every repository the
    /// link allows.
    #[serde(default)]
    pub repositories: Vec<String>,
    /// Permission name to `read` or `write`. Empty: the link's permissions.
    #[serde(default)]
    pub permissions: std::collections::BTreeMap<String, String>,
}

/// One GitHub installation linked to an install, as the broker sees it.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct GithubLink {
    pub installation_id: u64,
    /// The account (organization or user) the installation is on.
    pub account: String,
    /// The repositories the customer chose for this install. Empty: every
    /// repository the link allows ([`GithubLink::ceiling`]).
    #[serde(default)]
    pub repositories: Vec<String>,
    /// The repositories the person who linked the installation may push to
    /// on GitHub, read at link time ([`writable_repositories`]): no token is
    /// ever wider. None only for links made before the ceiling existed: those
    /// get no token until the installation is linked again
    /// ([`BrokerRefusal::RelinkNeeded`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ceiling: Option<Vec<String>>,
}

/// GitHub caps a token at this many named repositories, so a ceiling is
/// never longer.
pub const MAX_CEILING: usize = 500;

/// The repositories in a page of GitHub's `GET /user/installations/{id}/repositories`
/// (asked with the person's own token) they may push to: their own
/// permissions, not the installation's.
pub fn writable_repositories(page: &serde_json::Value) -> Vec<String> {
    page["repositories"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or_default()
        .iter()
        .filter(|r| {
            let p = &r["permissions"];
            p["push"].as_bool() == Some(true) || p["admin"].as_bool() == Some(true)
        })
        .filter_map(|r| r["name"].as_str())
        .map(|n| n.to_ascii_lowercase())
        .collect()
}

fn norm_repo(r: &str) -> String {
    r.trim()
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
}

/// Whether every repository in `asked` is inside the link's ceiling. A link
/// without one allows nothing.
pub fn ceiling_allows(link: &GithubLink, asked: &[String]) -> bool {
    match &link.ceiling {
        None => false,
        Some(c) => {
            let c: Vec<String> = c.iter().map(|r| norm_repo(r)).collect();
            asked.iter().all(|r| c.contains(&norm_repo(r)))
        }
    }
}

/// The permissions a brokered token may ever carry: what CoderHelm's app
/// asks for, no more (the same list the guide shows).
pub const MAX_PERMISSIONS: &[(&str, &str)] = &[
    ("contents", "write"),
    ("pull_requests", "write"),
    ("issues", "write"),
    ("checks", "read"),
    ("actions", "read"),
    ("statuses", "read"),
    ("workflows", "write"),
    ("metadata", "read"),
];

/// Why the broker refused, as a code and one plain sentence with the fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerRefusal {
    NotLinked,
    /// Linked before CoderHelm recorded which repositories the person who
    /// linked it may push to, so there is no limit to hold a token to.
    RelinkNeeded,
    RepositoryNotAllowed,
    PermissionNotAllowed,
    Revoked,
}

impl BrokerRefusal {
    pub fn code(self) -> &'static str {
        match self {
            Self::NotLinked => "broker_not_linked",
            Self::RelinkNeeded => "broker_relink_needed",
            Self::RepositoryNotAllowed => "broker_repository_not_allowed",
            Self::PermissionNotAllowed => "broker_permission_not_allowed",
            Self::Revoked => "broker_revoked",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::NotLinked => "That GitHub installation isn't linked to this install.",
            Self::RelinkNeeded => "That GitHub installation was linked before CoderHelm recorded which repositories it may reach, so no token is issued for it.",
            Self::RepositoryNotAllowed => "That repository isn't one this install was given.",
            Self::PermissionNotAllowed => {
                "A permission was asked for that CoderHelm's app doesn't grant."
            }
            Self::Revoked => "This install's access to CoderHelm's apps was turned off.",
        }
    }

    pub fn fix(self) -> &'static str {
        match self {
            Self::NotLinked | Self::RelinkNeeded => "Link the installation again in Settings, System, Hybrid.",
            Self::RepositoryNotAllowed => "Add the repository on GitHub (the app's Configure page) and to this install in Settings, System, Hybrid.",
            Self::PermissionNotAllowed => "Update CoderHelm: a newer release may ask for less.",
            Self::Revoked => "Contact CoderHelm support, or switch to your own GitHub App in Settings, System, Hybrid.",
        }
    }
}

/// The body to send GitHub's `POST /app/installations/{id}/access_tokens`.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct MintBody {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub repositories: Vec<String>,
    pub permissions: std::collections::BTreeMap<String, String>,
}

fn level(p: &str) -> u8 {
    match p {
        "write" => 2,
        "read" => 1,
        _ => 0,
    }
}

/// Decides one token request: which installation, narrowed to which
/// repositories and permissions. Never wider than the link or the app.
pub fn plan_token(links: &[GithubLink], ask: &TokenAsk) -> Result<MintBody, BrokerRefusal> {
    let link = links
        .iter()
        .find(|l| l.installation_id == ask.installation_id)
        .ok_or(BrokerRefusal::NotLinked)?;
    let norm = norm_repo;
    let chosen: Vec<String> = link.repositories.iter().map(|r| norm(r)).collect();
    let allowed: Vec<String> = match &link.ceiling {
        // Never wider than what the linker could push to: without that
        // limit, no token.
        None => return Err(BrokerRefusal::RelinkNeeded),
        Some(c) => {
            let c: Vec<String> = c.iter().map(|r| norm(r)).take(MAX_CEILING).collect();
            if c.is_empty() {
                return Err(BrokerRefusal::RepositoryNotAllowed);
            }
            if chosen.is_empty() {
                c
            } else {
                // A chosen list never reaches past the ceiling.
                let inside: Vec<String> = chosen.into_iter().filter(|r| c.contains(r)).collect();
                if inside.is_empty() {
                    return Err(BrokerRefusal::RepositoryNotAllowed);
                }
                inside
            }
        }
    };
    let mut repositories: Vec<String> = Vec::new();
    for r in &ask.repositories {
        let n = norm(r);
        if n.is_empty() {
            continue;
        }
        if !allowed.is_empty() && !allowed.contains(&n) {
            return Err(BrokerRefusal::RepositoryNotAllowed);
        }
        if !repositories.contains(&n) {
            repositories.push(n);
        }
    }
    // Asked for nothing in particular: the link's list (GitHub caps a token
    // at 500 named repositories; past that the installation's own choice holds).
    if repositories.is_empty() && allowed.len() <= 500 {
        repositories = allowed;
    }
    let mut permissions = std::collections::BTreeMap::new();
    if ask.permissions.is_empty() {
        for (k, v) in MAX_PERMISSIONS {
            permissions.insert((*k).to_string(), (*v).to_string());
        }
    } else {
        for (k, v) in &ask.permissions {
            let max = MAX_PERMISSIONS
                .iter()
                .find(|(n, _)| n == k)
                .map(|(_, l)| *l);
            match max {
                Some(m) if level(v) >= 1 && level(v) <= level(m) => {
                    permissions.insert(k.clone(), v.clone());
                }
                _ => return Err(BrokerRefusal::PermissionNotAllowed),
            }
        }
    }
    Ok(MintBody {
        repositories,
        permissions,
    })
}

/// PKCE (S256): does the verifier match the challenge the install sent first?
pub fn pkce_matches(challenge: &str, verifier: &str) -> bool {
    use base64::Engine;
    if verifier.len() < 43 || verifier.len() > 128 {
        return false;
    }
    let got = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(Sha256::digest(verifier.as_bytes()));
    let a = got.as_bytes();
    let b = challenge.trim().as_bytes();
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The store key for a grant handle: only its hash is kept.
pub fn grant_key(handle: &str) -> String {
    format!(
        "HYBRIDGRANT#{}",
        hex::encode(Sha256::digest(handle.as_bytes()))
    )
}

/// A new opaque handle (`hr_` and 40 random characters).
pub fn new_handle(prefix: &str) -> String {
    use rand::Rng;
    const A: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let tail: String = (0..40)
        .map(|_| A[rng.random_range(0..A.len())] as char)
        .collect();
    format!("{prefix}_{tail}")
}

/// Whether a redirect address belongs to an install: one of its own origins,
/// on the dashboard's sign-in callback page and nothing else.
pub fn redirect_allowed(redirect_uri: &str, origins: &[String]) -> bool {
    let Ok(u) = url::Url::parse(redirect_uri) else {
        return false;
    };
    if u.scheme() != "https"
        || u.query().is_some()
        || u.fragment().is_some()
        || !u.username().is_empty()
    {
        return false;
    }
    let origin = format!(
        "https://{}",
        u.host_str().unwrap_or("").to_ascii_lowercase()
    );
    if u.port().is_some() {
        return false;
    }
    let path_ok = u.path() == "/settings/integrations/mcp-callback/";
    path_ok
        && origins
            .iter()
            .any(|o| o.trim_end_matches('/').eq_ignore_ascii_case(&origin))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn links() -> Vec<GithubLink> {
        vec![
            GithubLink {
                installation_id: 11,
                account: "example-org".into(),
                repositories: vec!["web".into(), "api".into()],
                ceiling: Some(vec!["web".into(), "api".into(), "docs".into()]),
            },
            GithubLink {
                installation_id: 22,
                account: "example-user".into(),
                repositories: vec![],
                ceiling: Some(vec!["x".into(), "y".into()]),
            },
        ]
    }

    /// A link holds what the person who linked it may push to on GitHub
    /// (its ceiling): someone with read access to one repository can't link
    /// the organization's installation and mint write tokens for every
    /// repository in it. The ceiling goes to GitHub as the token's
    /// `repositories`, so GitHub enforces it too.
    #[test]
    fn a_link_never_reaches_past_what_its_linker_can_push_to() {
        let link = GithubLink {
            installation_id: 44,
            account: "example-org".into(),
            repositories: vec![],
            ceiling: Some(vec!["web".into(), "docs-site".into()]),
        };
        let ask = |repos: &[&str]| TokenAsk {
            installation_id: 44,
            repositories: repos.iter().map(|r| r.to_string()).collect(),
            ..Default::default()
        };
        let all = plan_token(std::slice::from_ref(&link), &ask(&[])).unwrap();
        assert_eq!(all.repositories, vec!["web", "docs-site"]);
        assert_eq!(
            plan_token(std::slice::from_ref(&link), &ask(&["payments"])),
            Err(BrokerRefusal::RepositoryNotAllowed)
        );
        assert!(ceiling_allows(&link, &["web".into()]));
        assert!(!ceiling_allows(&link, &["web".into(), "payments".into()]));
        // A chosen list outside the ceiling (stored before) is cut back to it.
        let mut wide = link.clone();
        wide.repositories = vec!["web".into(), "payments".into()];
        let t = plan_token(std::slice::from_ref(&wide), &ask(&[])).unwrap();
        assert_eq!(t.repositories, vec!["web"]);
        // Nothing the linker can push to: nothing at all, never "everything".
        let mut none = link.clone();
        none.ceiling = Some(vec![]);
        assert_eq!(
            plan_token(&[none], &ask(&[])),
            Err(BrokerRefusal::RepositoryNotAllowed)
        );
    }

    /// A link made before the ceiling existed gets no token and no new
    /// repository list: the install is asked to link it again.
    #[test]
    fn a_link_without_a_ceiling_must_be_made_again() {
        let old = GithubLink {
            installation_id: 55,
            account: "example-org".into(),
            repositories: vec![],
            ceiling: None,
        };
        let ask = TokenAsk {
            installation_id: 55,
            ..Default::default()
        };
        assert_eq!(
            plan_token(std::slice::from_ref(&old), &ask),
            Err(BrokerRefusal::RelinkNeeded)
        );
        let mut chosen = old.clone();
        chosen.repositories = vec!["web".into()];
        assert_eq!(
            plan_token(&[chosen], &ask),
            Err(BrokerRefusal::RelinkNeeded)
        );
        assert!(!ceiling_allows(&old, &[]));
        assert!(!ceiling_allows(&old, &["web".into()]));
        let r = BrokerRefusal::RelinkNeeded;
        assert!(r.message().ends_with('.') && r.fix().contains("Link the installation again"));
    }

    #[test]
    fn writable_repositories_come_from_the_persons_own_permissions() {
        let page = serde_json::json!({"repositories": [
            {"name": "web", "permissions": {"admin": false, "push": true, "pull": true}},
            {"name": "Docs", "permissions": {"admin": true, "push": true, "pull": true}},
            {"name": "readonly", "permissions": {"admin": false, "push": false, "pull": true}},
            {"name": "noperms"}
        ]});
        assert_eq!(writable_repositories(&page), vec!["web", "docs"]);
    }

    #[test]
    fn only_linked_installations_get_tokens() {
        let ask = TokenAsk {
            installation_id: 33,
            ..Default::default()
        };
        assert_eq!(plan_token(&links(), &ask), Err(BrokerRefusal::NotLinked));
        assert!(plan_token(
            &[],
            &TokenAsk {
                installation_id: 11,
                ..Default::default()
            }
        )
        .is_err());
    }

    #[test]
    fn a_token_is_narrowed_to_the_chosen_repositories() {
        let all = plan_token(
            &links(),
            &TokenAsk {
                installation_id: 11,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(all.repositories, vec!["web", "api"]);
        let one = plan_token(
            &links(),
            &TokenAsk {
                installation_id: 11,
                repositories: vec!["example-org/Web".into(), "web".into()],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(one.repositories, vec!["web"]);
        let other = TokenAsk {
            installation_id: 11,
            repositories: vec!["secret-repo".into()],
            ..Default::default()
        };
        assert_eq!(
            plan_token(&links(), &other),
            Err(BrokerRefusal::RepositoryNotAllowed)
        );
        // A link with no list: anything inside its ceiling.
        let any = plan_token(
            &links(),
            &TokenAsk {
                installation_id: 22,
                repositories: vec!["x".into()],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(any.repositories, vec!["x"]);
    }

    #[test]
    fn permissions_never_exceed_the_app() {
        let full = plan_token(
            &links(),
            &TokenAsk {
                installation_id: 22,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            full.permissions.get("contents").map(String::as_str),
            Some("write")
        );
        assert!(!full.permissions.contains_key("administration"));
        let mut p = std::collections::BTreeMap::new();
        p.insert("contents".to_string(), "read".to_string());
        let narrow = plan_token(
            &links(),
            &TokenAsk {
                installation_id: 22,
                permissions: p,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(narrow.permissions.len(), 1);
        for (k, v) in [
            ("administration", "write"),
            ("checks", "write"),
            ("contents", "admin"),
        ] {
            let mut p = std::collections::BTreeMap::new();
            p.insert(k.to_string(), v.to_string());
            let r = plan_token(
                &links(),
                &TokenAsk {
                    installation_id: 22,
                    permissions: p,
                    ..Default::default()
                },
            );
            assert_eq!(r, Err(BrokerRefusal::PermissionNotAllowed), "{k}:{v}");
        }
    }

    #[test]
    fn the_mint_body_is_what_github_expects() {
        let b = plan_token(
            &links(),
            &TokenAsk {
                installation_id: 11,
                ..Default::default()
            },
        )
        .unwrap();
        let j = serde_json::to_value(&b).unwrap();
        assert_eq!(j["repositories"][0], "web");
        assert_eq!(j["permissions"]["metadata"], "read");
        let none = serde_json::to_value(MintBody::default()).unwrap();
        assert!(none.get("repositories").is_none());
    }

    #[test]
    fn pkce_s256_is_checked() {
        // RFC 7636, appendix B.
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
        assert!(pkce_matches(challenge, verifier));
        assert!(!pkce_matches(
            challenge,
            "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXX"
        ));
        assert!(!pkce_matches(challenge, "short"));
    }

    #[test]
    fn handles_are_random_and_only_their_hash_is_a_key() {
        let h = new_handle("hr");
        assert!(h.starts_with("hr_") && h.len() == 43);
        assert_ne!(h, new_handle("hr"));
        let k = grant_key(&h);
        assert!(k.starts_with("HYBRIDGRANT#") && !k.contains(&h));
    }

    #[test]
    fn redirects_go_only_to_the_install_callback_page() {
        let origins = vec![
            "https://d1abcdef234567.cloudfront.net".to_string(),
            "https://example.coderhelm.com".to_string(),
        ];
        assert!(redirect_allowed(
            "https://example.coderhelm.com/settings/integrations/mcp-callback/",
            &origins
        ));
        assert!(redirect_allowed(
            "https://D1ABCDEF234567.cloudfront.net/settings/integrations/mcp-callback/",
            &origins
        ));
        for bad in [
            "http://example.coderhelm.com/settings/integrations/mcp-callback/",
            "https://evil.example.org/settings/integrations/mcp-callback/",
            "https://example.coderhelm.com/other/",
            "https://example.coderhelm.com/settings/integrations/mcp-callback/?x=1",
            "https://example.coderhelm.com:8443/settings/integrations/mcp-callback/",
            "https://user@example.coderhelm.com/settings/integrations/mcp-callback/",
            "https://example.coderhelm.com.evil.org/settings/integrations/mcp-callback/",
            "not a url",
        ] {
            assert!(!redirect_allowed(bad, &origins), "{bad}");
        }
    }

    #[test]
    fn every_refusal_has_words_and_a_fix() {
        for r in [
            BrokerRefusal::NotLinked,
            BrokerRefusal::RepositoryNotAllowed,
            BrokerRefusal::PermissionNotAllowed,
            BrokerRefusal::Revoked,
        ] {
            assert!(
                r.message().ends_with('.')
                    && r.fix().ends_with('.')
                    && r.code().starts_with("broker_")
            );
        }
    }
}
