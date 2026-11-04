//! GitHub sign-in on a hybrid install through CoderHelm's GitHub App.
//!
//! A hybrid install has no GitHub OAuth client of its own, and GitHub only
//! sends people back to the callback addresses registered on CoderHelm's
//! app. So the hosted side carries the GitHub trip, and the install makes
//! every decision:
//!
//! 1. The install makes a state, a PKCE verifier and its challenge, and asks
//!    the hosted side (signed with its registration secret) to start a flow
//!    for its own callback address. The hosted side refuses any address that
//!    isn't one of the install's verified hosts.
//! 2. GitHub comes back to the hosted callback with a one-time code. The
//!    hosted side sends the browser on to the install's callback with that
//!    code, the install's state and the flow id. It never exchanges the code
//!    on its own.
//! 3. The install checks the state and the flow against its sealed cookie,
//!    then asks the hosted side (signed) to exchange the code with the PKCE
//!    verifier only it holds. GitHub checks the verifier against the
//!    challenge from step 1, so a code from another flow can't be used.
//! 4. The install reads the person's GitHub id and login from GitHub itself
//!    with that token and applies its own sign-in rules (team, invites,
//!    two-factor). Then it has the hosted side revoke the token at GitHub
//!    (only CoderHelm's client can), so it lives for seconds. The hosted
//!    side can't mint a session on the install and doesn't choose who signs in.
//!
//! Everything here is pure (no AWS, no HTTP) so each check has a test.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// The install's callback for this flow (on its own API address).
pub const CALLBACK_PATH: &str = "/auth/github/hybrid-callback";
/// The hosted callback GitHub returns to: the one already registered on
/// CoderHelm's GitHub App for hybrid linking.
pub const HOSTED_CALLBACK_PATH: &str = "/auth/github/callback/hybrid";
/// The signed calls the install makes.
pub const START_PATH: &str = "/hybrid/github/sign-in/start";
pub const EXCHANGE_PATH: &str = "/hybrid/github/sign-in/exchange";
pub const REVOKE_PATH: &str = "/hybrid/github/sign-in/revoke";
/// A flow id's prefix (`hsi_` and 40 random characters).
pub const FLOW_PREFIX: &str = "hsi";
/// How long a started flow waits for GitHub, and a returned code for the
/// install's exchange.
pub const START_TTL_SECS: i64 = 600;
pub const RETURN_TTL_SECS: i64 = 300;
/// How long the install's sealed cookie is good for.
pub const COOKIE_TTL_SECS: i64 = 600;

const ALNUM: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

fn random(n: usize) -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    (0..n)
        .map(|_| ALNUM[rng.random_range(0..ALNUM.len())] as char)
        .collect()
}

fn alnum(s: &str) -> bool {
    s.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// A flow id the hosted side made ([`FLOW_PREFIX`]).
pub fn is_flow(s: &str) -> bool {
    s.strip_prefix("hsi_")
        .is_some_and(|t| t.len() == 40 && alnum(t))
}

/// The install's state: 43 random characters.
pub fn new_state() -> String {
    random(43)
}

/// An install state the hosted side accepts and carries back.
pub fn is_state(s: &str) -> bool {
    (32..=128).contains(&s.len()) && alnum(s)
}

/// A PKCE verifier (RFC 7636): 64 characters from the unreserved set.
pub fn new_verifier() -> String {
    random(64)
}

/// The S256 challenge for a verifier.
pub fn challenge(verifier: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// An S256 challenge's shape: 43 base64url characters.
pub fn is_challenge(c: &str) -> bool {
    c.len() == 43
        && c.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// What the hosted side keeps of a returned code: its hash, never the code.
pub fn code_hash(code: &str) -> String {
    hex::encode(Sha256::digest(code.as_bytes()))
}

/// Constant-time string equality.
pub fn same(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Whether `redirect_uri` is the install's sign-in callback on one of its
/// verified hosts: https, no port, no user, no query or fragment, exactly
/// [`CALLBACK_PATH`], host compared whole (never by prefix).
pub fn redirect_allowed(redirect_uri: &str, origins: &[String]) -> bool {
    let Ok(u) = url::Url::parse(redirect_uri) else {
        return false;
    };
    if u.scheme() != "https"
        || u.port().is_some()
        || !u.username().is_empty()
        || u.password().is_some()
        || u.query().is_some()
        || u.fragment().is_some()
        || u.path() != CALLBACK_PATH
    {
        return false;
    }
    let origin = format!(
        "https://{}",
        u.host_str().unwrap_or("").to_ascii_lowercase()
    );
    origins
        .iter()
        .any(|o| o.trim_end_matches('/').eq_ignore_ascii_case(&origin))
}

/// GitHub's authorize page for a flow: CoderHelm's client, the hosted
/// callback, the flow id as GitHub's state, and the install's challenge.
pub fn authorize_url(client_id: &str, hosted_api: &str, flow: &str, challenge: &str) -> String {
    let redirect = format!("{}{HOSTED_CALLBACK_PATH}", hosted_api.trim_end_matches('/'));
    format!(
        "https://github.com/login/oauth/authorize?client_id={}&redirect_uri={}&state={}&code_challenge={}&code_challenge_method=S256&allow_signup=false",
        urlencoding::encode(client_id),
        urlencoding::encode(&redirect),
        urlencoding::encode(flow),
        urlencoding::encode(challenge),
    )
}

/// The install's check of the authorize address the hosted side answered:
/// GitHub's page, for this flow, with this install's own challenge. Anything
/// else is refused before the browser goes anywhere.
pub fn authorize_url_ok(url: &str, flow: &str, challenge: &str) -> bool {
    let Ok(u) = url::Url::parse(url) else {
        return false;
    };
    if u.scheme() != "https"
        || u.host_str() != Some("github.com")
        || u.port().is_some()
        || u.path() != "/login/oauth/authorize"
    {
        return false;
    }
    let q = |k: &str| {
        u.query_pairs()
            .filter(|(n, _)| n == k)
            .map(|(_, v)| v.into_owned())
            .collect::<Vec<_>>()
    };
    q("state") == [flow]
        && q("code_challenge") == [challenge]
        && q("code_challenge_method") == ["S256"]
}

/// Where the hosted side sends the browser back to: the install's callback
/// with GitHub's code (or error), the install's state and the flow id.
pub fn return_url(
    redirect: &str,
    outcome: Result<&str, &str>,
    install_state: &str,
    flow: &str,
) -> String {
    let first = match outcome {
        Ok(code) => format!("code={}", urlencoding::encode(code)),
        Err(e) => {
            // GitHub's error names are short fixed tokens; keep only those.
            let e: String = e
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                .take(64)
                .collect();
            format!(
                "error={}",
                if e.is_empty() {
                    "access_denied".into()
                } else {
                    e
                }
            )
        }
    };
    format!(
        "{redirect}?{first}&state={}&flow={}",
        urlencoding::encode(install_state),
        urlencoding::encode(flow)
    )
}

/// What the install keeps in its cookie between the start and the return.
#[derive(Clone, PartialEq, Eq)]
pub struct Pending {
    pub state: String,
    pub flow: String,
    pub verifier: String,
}

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending")
            .field("flow", &self.flow)
            .finish_non_exhaustive()
    }
}

fn mac(key: &str, body: &str) -> String {
    let mut m = Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("any key length");
    m.update(b"coderhelm-hybrid-github-sign-in\n");
    m.update(body.as_bytes());
    hex::encode(m.finalize().into_bytes())
}

/// The cookie value: the pending flow and its expiry, with the install's own
/// MAC (a key only the install has), so the browser can't change any part.
pub fn seal(key: &str, p: &Pending, now: i64) -> String {
    let body = format!(
        "{}.{}.{}.{}",
        p.state,
        p.flow,
        p.verifier,
        now + COOKIE_TTL_SECS
    );
    format!("{body}.{}", mac(key, &body))
}

/// Opens a cookie [`seal`] made: None when altered, malformed or expired.
pub fn open(key: &str, value: &str, now: i64) -> Option<Pending> {
    let (body, tag) = value.rsplit_once('.')?;
    if !same(&mac(key, body), tag) {
        return None;
    }
    let mut it = body.split('.');
    let (state, flow, verifier, exp) = (it.next()?, it.next()?, it.next()?, it.next()?);
    if it.next().is_some() || exp.parse::<i64>().ok()? < now {
        return None;
    }
    (is_state(state) && is_flow(flow) && verifier.len() == 64 && alnum(verifier)).then(|| Pending {
        state: state.into(),
        flow: flow.into(),
        verifier: verifier.into(),
    })
}

/// The return belongs to the flow this browser started: the same state and
/// the same flow id (no mix-up with another flow or install).
pub fn return_matches(p: &Pending, state: &str, flow: &str) -> bool {
    same(&p.state, state) && same(&p.flow, flow)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origins() -> Vec<String> {
        vec![
            "https://d1234.cloudfront.net".into(),
            "https://acme.coderhelm.com".into(),
        ]
    }

    fn flow() -> String {
        format!("hsi_{}", "a".repeat(40))
    }

    #[test]
    fn the_redirect_is_only_the_install_callback_on_its_own_hosts() {
        let o = origins();
        assert!(redirect_allowed(
            "https://d1234.cloudfront.net/auth/github/hybrid-callback",
            &o
        ));
        assert!(redirect_allowed(
            "https://ACME.coderhelm.com/auth/github/hybrid-callback",
            &o
        ));
        for bad in [
            "http://d1234.cloudfront.net/auth/github/hybrid-callback",
            "https://d1234.cloudfront.net:8443/auth/github/hybrid-callback",
            "https://d1234.cloudfront.net.evil.example/auth/github/hybrid-callback",
            "https://evil.example/auth/github/hybrid-callback",
            "https://user@d1234.cloudfront.net/auth/github/hybrid-callback",
            "https://d1234.cloudfront.net/auth/github/hybrid-callback?x=1",
            "https://d1234.cloudfront.net/auth/github/hybrid-callback#x",
            "https://d1234.cloudfront.net/auth/github/callback",
            "https://d1234.cloudfront.net/auth/github/hybrid-callback/",
            "https://other.coderhelm.com/auth/github/hybrid-callback",
            "not a url",
            "",
        ] {
            assert!(!redirect_allowed(bad, &o), "{bad}");
        }
    }

    #[test]
    fn handles_and_secrets_have_their_shapes() {
        assert!(is_flow(&flow()));
        assert!(!is_flow("hyb_aaaa"));
        assert!(!is_flow(&format!("hsi_{}", "a".repeat(39))));
        assert!(!is_flow(&format!("hsi_{}.", "a".repeat(39))));
        let s = new_state();
        assert!(is_state(&s) && s != new_state());
        assert!(!is_state("short") && !is_state(&format!("{}.", "a".repeat(40))));
        let v = new_verifier();
        let c = challenge(&v);
        assert!(is_challenge(&c));
        assert!(crate::hybrid::broker::pkce_matches(&c, &v));
        assert!(!crate::hybrid::broker::pkce_matches(&c, &new_verifier()));
        assert_eq!(code_hash("abc"), code_hash("abc"));
        assert_ne!(code_hash("abc"), code_hash("abd"));
        assert!(!code_hash("abc").contains("abc"));
    }

    #[test]
    fn the_authorize_url_carries_the_flow_and_the_install_challenge() {
        let c = challenge(&new_verifier());
        let u = authorize_url("Iv1.coderhelm", "https://api.coderhelm.com/", &flow(), &c);
        assert!(u.starts_with("https://github.com/login/oauth/authorize?client_id=Iv1.coderhelm&"));
        assert!(u.contains(&format!(
            "redirect_uri={}",
            urlencoding::encode("https://api.coderhelm.com/auth/github/callback/hybrid")
        )));
        assert!(authorize_url_ok(&u, &flow(), &c));
        // Another flow, another challenge, or somewhere other than GitHub.
        assert!(!authorize_url_ok(
            &u,
            &format!("hsi_{}", "b".repeat(40)),
            &c
        ));
        assert!(!authorize_url_ok(&u, &flow(), &challenge(&new_verifier())));
        let elsewhere = u.replace("https://github.com/", "https://github.com.evil.example/");
        assert!(!authorize_url_ok(&elsewhere, &flow(), &c));
        let plain = u.replace("code_challenge_method=S256", "code_challenge_method=plain");
        assert!(!authorize_url_ok(&plain, &flow(), &c));
        let twice = format!("{u}&state=hsi_{}", "b".repeat(40));
        assert!(!authorize_url_ok(&twice, &flow(), &c));
    }

    #[test]
    fn the_return_url_carries_the_code_or_a_clean_error() {
        let r = "https://acme.coderhelm.com/auth/github/hybrid-callback";
        let u = return_url(r, Ok("c0de"), "S1", &flow());
        assert_eq!(u, format!("{r}?code=c0de&state=S1&flow={}", flow()));
        let u = return_url(r, Err("access_denied"), "S1", &flow());
        assert!(u.starts_with(&format!("{r}?error=access_denied&state=S1")));
        let u = return_url(r, Err("<script>&x=1"), "S1", &flow());
        assert!(
            u.starts_with(&format!("{r}?error=scriptx1&state=S1")),
            "{u}"
        );
    }

    #[test]
    fn the_cookie_opens_only_unaltered_unexpired_and_with_the_install_key() {
        let p = Pending {
            state: new_state(),
            flow: flow(),
            verifier: new_verifier(),
        };
        let v = seal("install-key", &p, 1_000);
        assert_eq!(open("install-key", &v, 1_000), Some(p.clone()));
        assert_eq!(
            open("install-key", &v, 1_000 + COOKIE_TTL_SECS),
            Some(p.clone())
        );
        // Expired.
        assert_eq!(open("install-key", &v, 1_001 + COOKIE_TTL_SECS), None);
        // Another key (another install, or anyone without the install's).
        assert_eq!(open("other-key", &v, 1_000), None);
        // Any byte changed.
        let mut forged = v.clone().into_bytes();
        forged[0] = if forged[0] == b'a' { b'b' } else { b'a' };
        assert_eq!(
            open("install-key", &String::from_utf8(forged).unwrap(), 1_000),
            None
        );
        // A longer expiry written by hand.
        let (body, _) = v.rsplit_once('.').unwrap();
        let later = body.replace(&(1_000 + COOKIE_TTL_SECS).to_string(), "9999999999");
        assert_eq!(
            open(
                "install-key",
                &format!("{later}.{}", mac("x", &later)),
                1_000
            ),
            None
        );
        assert_eq!(open("install-key", "", 1_000), None);
        assert!(!format!("{p:?}").contains(&p.verifier));
        assert!(!format!("{p:?}").contains(&p.state));
    }

    #[test]
    fn a_return_matches_only_its_own_state_and_flow() {
        let p = Pending {
            state: "S".repeat(43),
            flow: flow(),
            verifier: new_verifier(),
        };
        assert!(return_matches(&p, &"S".repeat(43), &flow()));
        assert!(!return_matches(&p, &"T".repeat(43), &flow()));
        assert!(!return_matches(
            &p,
            &"S".repeat(43),
            &format!("hsi_{}", "b".repeat(40))
        ));
        assert!(!return_matches(&p, "", ""));
    }
}
