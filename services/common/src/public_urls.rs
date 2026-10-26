//! Where this deployment's dashboard and API are, for links in comments,
//! emails, redirects and webhooks.
//!
//! The deploy config sets them (infra/lib/service-env.ts):
//! - `APP_URL` and `API_URL` when they are known at deploy time (a custom
//!   domain, or the hosted deployment's own hostnames);
//! - otherwise `PUBLIC_URL_PARAMETER` names an SSM parameter holding the one
//!   URL CloudFront gave the deployment (the dashboard and the API share it).
//!   It only exists once the deploy finishes, so a read that fails is tried
//!   again (at most every 30 seconds) on later requests.
//!
//! With neither (local runs, tests) the stage decides, as it always did, on
//! the hosted deployment only. A self-hosted install never falls back to the
//! hosted addresses: until its own address is known its links are relative
//! (`/runs/...`), it logs an error at start, and Setup checks fail
//! ([`problem`]). Links that belong to an install (runs, reviews, settings,
//! approvals, unsubscribing) must come from here; `tests/no_hosted_urls.rs`
//! refuses hardcoded hosted addresses anywhere else.

use std::cell::RefCell;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

pub use aws_sdk_ssm as ssm;

/// The dashboard's and the API's base URLs, without a trailing slash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Urls {
    pub app: String,
    pub api: String,
}

static URLS: OnceLock<Urls> = OnceLock::new();
static SSM: OnceLock<ssm::Client> = OnceLock::new();
static LAST_TRY: Mutex<Option<Instant>> = Mutex::new(None);
const RETRY_AFTER: Duration = Duration::from_secs(30);

/// The marketing site the hosted deployment links to when SITE_URL is unset.
const HOSTED_SITE: &str = "https://coderhelm.com";
/// The hosted deployment's support inbox and platform admin (defaults only
/// there; a self-hosted install uses its own).
const HOSTED_SUPPORT: &str = "support@coderhelm.com";
const HOSTED_ADMIN: &str = "admin@coderhelm.com";

fn clean(u: &str) -> String {
    u.trim().trim_end_matches('/').to_string()
}

/// APP_URL and API_URL, when both are set and not empty.
pub fn from_vars(app: Option<String>, api: Option<String>) -> Option<Urls> {
    match (app, api) {
        (Some(a), Some(b)) if !a.trim().is_empty() && !b.trim().is_empty() => Some(Urls {
            app: clean(&a),
            api: clean(&b),
        }),
        _ => None,
    }
}

/// One URL for both (single origin), when it is a usable https or http URL.
pub fn from_single(url: &str) -> Option<Urls> {
    let u = clean(url);
    if u.starts_with("https://") || u.starts_with("http://") {
        Some(Urls {
            app: u.clone(),
            api: u,
        })
    } else {
        None
    }
}

/// What a test sets for its own thread instead of the environment: the
/// install's URLs, its marketing site, and whether it is self-hosted.
#[derive(Debug, Clone)]
pub struct Override {
    pub urls: Option<Urls>,
    pub site: Option<String>,
    pub self_hosted: bool,
}

/// A thread's override as stored: the URLs, the site, self-hosted or not.
type Stored = (Option<&'static Urls>, Option<&'static str>, bool);

thread_local! {
    static OVERRIDE: RefCell<Option<Stored>> = const { RefCell::new(None) };
}

/// Sets (or with `None` clears) this thread's [`Override`]. For tests that
/// render messages as a given deployment would; the services never call it.
/// Each call leaks its two short strings, which only tests can afford.
#[doc(hidden)]
pub fn override_for_this_thread(o: Option<Override>) {
    let v = o.map(|o| {
        let urls: Option<&'static Urls> = o.urls.map(|u| &*Box::leak(Box::new(u)));
        let site: Option<&'static str> = o.site.map(|s| &*Box::leak(s.into_boxed_str()));
        (urls, site, o.self_hosted)
    });
    OVERRIDE.with(|c| *c.borrow_mut() = v);
}

/// The address tests render a self-hosted install's messages with.
#[doc(hidden)]
pub const EXAMPLE_SELF_HOST: &str = "https://coderhelm.example.com";

/// Makes this thread a self-hosted install at [`EXAMPLE_SELF_HOST`] with no
/// marketing site (tests only; see [`override_for_this_thread`]).
#[doc(hidden)]
pub fn pretend_self_hosted() {
    override_for_this_thread(Some(Override {
        urls: from_single(EXAMPLE_SELF_HOST),
        site: None,
        self_hosted: true,
    }));
}

/// The public resources a self-hosted install may still link to: CoderHelm's
/// docs. Everything else on coderhelm.com is the hosted service.
pub const PUBLIC_HOSTS: &[&str] = &["docs.coderhelm.com"];

/// Every coderhelm.com host (or address at it) in `text` other than
/// [`PUBLIC_HOSTS`]: what a self-hosted install's messages must not contain.
pub fn hosted_mentions(text: &str) -> Vec<String> {
    let lower = text.to_ascii_lowercase();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(i) = lower[from..].find("coderhelm.com") {
        let at = from + i;
        let end = at + "coderhelm.com".len();
        let start = lower[..at]
            .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '-'))
            .map_or(0, |j| j + 1);
        let host = &lower[start..end];
        let next = lower[end..].chars().next();
        let whole = !next.is_some_and(|c| c.is_ascii_alphanumeric() || c == '-');
        if whole && !PUBLIC_HOSTS.contains(&host) {
            let ctx_start = lower[..start]
                .char_indices()
                .rev()
                .nth(30)
                .map_or(0, |(j, _)| j);
            out.push(text[ctx_start..end].to_string());
        }
        from = end;
    }
    out
}

fn overridden() -> Option<Stored> {
    OVERRIDE.with(|c| *c.borrow())
}

fn current() -> Option<&'static Urls> {
    if let Some((urls, _, _)) = overridden() {
        return urls;
    }
    if let Some(u) = URLS.get() {
        return Some(u);
    }
    from_vars(std::env::var("APP_URL").ok(), std::env::var("API_URL").ok())
        .map(|u| URLS.get_or_init(|| u))
}

/// Call once at start: keeps the SSM client and reads the URL if needed.
pub async fn init(client: ssm::Client) {
    let _ = SSM.set(client);
    ensure().await;
    if let Some(p) = problem() {
        tracing::error!(problem = %p, "This install's address is unknown: links in emails, chat and GitHub are relative until it is set");
    }
}

/// Whether this deployment is a self-hosted install. The deploy sets
/// `SELF_HOSTED`; installs deployed before it are told apart by what only a
/// self-hosted install carries (its setup endpoints, the SSM parameter for
/// its CloudFront address, its release stamp).
pub fn self_hosted() -> bool {
    if let Some((_, _, s)) = overridden() {
        return s;
    }
    self_hosted_from(|k| std::env::var(k).ok())
}

/// [`self_hosted`]'s rule, on a given environment.
pub fn self_hosted_from(get: impl Fn(&str) -> Option<String>) -> bool {
    let on = |k: &str| get(k).is_some_and(|v| matches!(v.trim(), "1" | "true"));
    let set = |k: &str| get(k).is_some_and(|v| !v.trim().is_empty());
    on("SELF_HOSTED")
        || on("SETUP_ENDPOINTS")
        || set("PUBLIC_URL_PARAMETER")
        || set("CODERHELM_CONFIG_FINGERPRINT")
}

/// Why a self-hosted install's links can't be built yet, or `None` when they
/// can (or on the hosted deployment, which has its own defaults).
pub fn problem() -> Option<String> {
    if current().is_some() || !self_hosted() {
        return None;
    }
    Some(problem_text(std::env::var("PUBLIC_URL_PARAMETER").ok()))
}

/// [`problem`]'s words, given the SSM parameter's name (if any).
pub fn problem_text(parameter: Option<String>) -> String {
    match parameter.filter(|p| !p.trim().is_empty()) {
        Some(p) => format!(
            "The install's address isn't readable from the SSM parameter {p} yet. Links in emails, chat and GitHub stay relative until it is. The web stack writes it at the end of a deploy: finish the deploy, or run the doctor."
        ),
        None => "APP_URL and API_URL aren't set, and there is no PUBLIC_URL_PARAMETER, so this install doesn't know its own address. Links in emails, chat and GitHub stay relative until it does. Deploy again with the installer, which sets them.".to_string(),
    }
}

/// Reads the URL from SSM if it isn't known yet (cheap once it is).
pub async fn ensure() {
    if current().is_some() {
        return;
    }
    let Ok(name) = std::env::var("PUBLIC_URL_PARAMETER") else {
        return;
    };
    let Some(client) = SSM.get() else {
        return;
    };
    {
        let mut last = LAST_TRY.lock().unwrap_or_else(|p| p.into_inner());
        if last.is_some_and(|t| t.elapsed() < RETRY_AFTER) {
            return;
        }
        *last = Some(Instant::now());
    }
    match client.get_parameter().name(&name).send().await {
        Ok(out) => match out
            .parameter()
            .and_then(|p| p.value())
            .and_then(from_single)
        {
            Some(u) => {
                tracing::info!(url = %u.app, "Public URL read from SSM");
                let _ = URLS.set(u);
            }
            None => tracing::warn!(parameter = %name, "Public URL parameter is empty or not a URL"),
        },
        Err(e) => {
            tracing::warn!(parameter = %name, error = %e, "Could not read the public URL; links use the stage default until it can be read")
        }
    }
}

/// The stage's default dashboard URL (before deployments had a config).
pub fn stage_app_url(stage: &str) -> &'static str {
    if stage == "prod" {
        "https://app.coderhelm.com"
    } else {
        "http://localhost:3000"
    }
}

/// The stage's default API URL (before deployments had a config).
pub fn stage_api_url(stage: &str) -> &'static str {
    if stage == "prod" {
        "https://api.coderhelm.com"
    } else {
        "http://localhost:3001"
    }
}

/// The dashboard's base URL, e.g. `https://app.example.com` (no trailing
/// slash). On a self-hosted install whose address isn't known yet, "" (links
/// stay relative), never the hosted dashboard.
pub fn app_url(stage: &str) -> &'static str {
    match current() {
        Some(u) => u.app.as_str(),
        None if self_hosted() => "",
        None => stage_app_url(stage),
    }
}

/// The API's base URL (webhooks, OAuth callbacks, unsubscribe links). On a
/// self-hosted install whose address isn't known yet, "", never the hosted
/// API.
pub fn api_url(stage: &str) -> &'static str {
    match current() {
        Some(u) => u.api.as_str(),
        None if self_hosted() => "",
        None => stage_api_url(stage),
    }
}

/// The stage as the services read it (prod when unset, like their configs).
fn env_stage() -> String {
    std::env::var("STAGE").unwrap_or_else(|_| "prod".to_string())
}

/// The dashboard's URL, for code that has no config at hand.
pub fn app_url_from_env() -> &'static str {
    app_url(&env_stage())
}

/// The API's URL, for code that has no config at hand.
pub fn api_url_from_env() -> &'static str {
    api_url(&env_stage())
}

/// The AWS account teams' read-only roles trust: this deployment's own
/// (CODERHELM_ACCOUNT_ID, set by the deploy), else the hosted one.
pub fn platform_account_id() -> String {
    std::env::var("CODERHELM_ACCOUNT_ID")
        .ok()
        .filter(|a| a.len() == 12 && a.chars().all(|c| c.is_ascii_digit()))
        .unwrap_or_else(|| "654654210434".to_string())
}

/// The CloudFormation template teams create that role from.
pub fn log_reader_template_url() -> String {
    std::env::var("LOG_READER_TEMPLATE_URL")
        .ok()
        .filter(|u| u.starts_with("https://"))
        .unwrap_or_else(|| {
            "https://coderhelm-public.s3.amazonaws.com/cfn/coderhelm-log-reader.yaml".to_string()
        })
}

/// The marketing site linked from email footers: SITE_URL, none when it is
/// empty, and the hosted site when it is unset.
pub fn site_url() -> Option<&'static str> {
    if let Some((_, site, _)) = overridden() {
        return site;
    }
    static SITE: OnceLock<Option<String>> = OnceLock::new();
    SITE.get_or_init(|| site_from(std::env::var("SITE_URL").ok()))
        .as_deref()
}

/// [`site_url`]'s rule, on a given value: unset means the hosted site, on
/// the hosted deployment only (a self-hosted install links to no site).
pub fn site_from(v: Option<String>) -> Option<String> {
    match v {
        None if self_hosted() => None,
        None => Some(HOSTED_SITE.to_string()),
        Some(s) if s.trim().is_empty() => None,
        Some(s) => Some(clean(&s)),
    }
}

/// Where support questions (and unsubscribe requests by mail) go:
/// SUPPORT_INBOX; else, on a self-hosted install, the address its email is
/// sent from (its own admins, never CoderHelm support); else the hosted
/// support inbox.
pub fn support_inbox() -> String {
    support_inbox_from(
        std::env::var("SUPPORT_INBOX").ok(),
        std::env::var("SES_FROM_ADDRESS").ok(),
        self_hosted(),
    )
}

/// [`support_inbox`]'s rule, on given values ("" when a self-hosted install
/// has neither).
pub fn support_inbox_from(
    inbox: Option<String>,
    from: Option<String>,
    self_hosted: bool,
) -> String {
    let set = |v: Option<String>| v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    match (set(inbox), self_hosted) {
        (Some(i), _) => i,
        (None, true) => set(from).unwrap_or_default(),
        (None, false) => HOSTED_SUPPORT.to_string(),
    }
}

/// Who may administer the whole install when PLATFORM_ADMIN_EMAILS is unset:
/// the hosted admin on the hosted deployment, nobody on a self-hosted one.
pub fn default_platform_admins() -> &'static str {
    if self_hosted() {
        ""
    } else {
        HOSTED_ADMIN
    }
}

/// The host part of a URL (`app.example.com`), for display.
pub fn host_of(url: &str) -> &str {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    rest.split('/').next().unwrap_or(rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_vars_are_needed_and_trailing_slashes_go() {
        assert_eq!(
            from_vars(
                Some("https://a.example.com/".into()),
                Some(" https://b.example.com ".into())
            ),
            Some(Urls {
                app: "https://a.example.com".into(),
                api: "https://b.example.com".into()
            })
        );
        assert_eq!(from_vars(Some("https://a".into()), None), None);
        assert_eq!(from_vars(Some("".into()), Some("https://b".into())), None);
    }

    #[test]
    fn a_single_origin_url_serves_both() {
        let u = from_single("https://d111.cloudfront.net/").unwrap();
        assert_eq!(u.app, "https://d111.cloudfront.net");
        assert_eq!(u.api, u.app);
        assert_eq!(from_single("d111.cloudfront.net"), None);
        assert_eq!(from_single(""), None);
    }

    #[test]
    fn stage_defaults_are_unchanged() {
        assert_eq!(stage_app_url("prod"), "https://app.coderhelm.com");
        assert_eq!(stage_api_url("prod"), "https://api.coderhelm.com");
        assert_eq!(stage_app_url("dev"), "http://localhost:3000");
        assert_eq!(stage_api_url("dev"), "http://localhost:3001");
    }

    #[test]
    fn site_url_rule() {
        assert_eq!(site_from(None).as_deref(), Some("https://coderhelm.com"));
        assert_eq!(site_from(Some("".into())), None);
        assert_eq!(
            site_from(Some("https://example.com/".into())).as_deref(),
            Some("https://example.com")
        );
    }

    #[test]
    fn self_hosted_is_told_apart() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert!(!self_hosted_from(env(&[])));
        assert!(!self_hosted_from(env(&[(
            "APP_URL",
            "https://app.coderhelm.com"
        )])));
        assert!(self_hosted_from(env(&[("SELF_HOSTED", "1")])));
        assert!(self_hosted_from(env(&[("SETUP_ENDPOINTS", "1")])));
        assert!(self_hosted_from(env(&[(
            "PUBLIC_URL_PARAMETER",
            "/coderhelm/prod/public-url"
        )])));
        assert!(self_hosted_from(env(&[(
            "CODERHELM_CONFIG_FINGERPRINT",
            "abc"
        )])));
        assert!(!self_hosted_from(env(&[
            ("SELF_HOSTED", "0"),
            ("PUBLIC_URL_PARAMETER", "")
        ])));
    }

    #[test]
    fn a_self_hosted_install_never_falls_back_to_the_hosted_addresses() {
        override_for_this_thread(Some(Override {
            urls: None,
            site: None,
            self_hosted: true,
        }));
        assert_eq!(app_url("prod"), "");
        assert_eq!(api_url("prod"), "");
        assert_eq!(app_url_from_env(), "");
        assert!(problem().is_some());
        assert_eq!(site_from(None), None);
        override_for_this_thread(Some(Override {
            urls: from_single("https://coderhelm.example.com/"),
            site: None,
            self_hosted: true,
        }));
        assert_eq!(app_url("prod"), "https://coderhelm.example.com");
        assert_eq!(api_url("prod"), "https://coderhelm.example.com");
        assert!(problem().is_none());
        override_for_this_thread(None);
    }

    #[test]
    fn the_problem_names_what_is_missing() {
        assert!(problem_text(None).contains("APP_URL and API_URL"));
        assert!(problem_text(Some("/p".into())).contains("SSM parameter /p"));
    }

    #[test]
    fn support_goes_to_the_install_on_self_host() {
        let s = |v: &str| Some(v.to_string());
        assert_eq!(
            support_inbox_from(s("help@example.org"), s("noreply@example.org"), true),
            "help@example.org"
        );
        assert_eq!(
            support_inbox_from(None, s("noreply@example.org"), true),
            "noreply@example.org"
        );
        assert_eq!(support_inbox_from(s(" "), None, true), "");
        assert_eq!(
            support_inbox_from(None, s("noreply@example.org"), false),
            "support@coderhelm.com"
        );
    }

    #[test]
    fn hosted_mentions_spare_the_docs_only() {
        assert!(hosted_mentions(
            "see https://docs.coderhelm.com/api and https://coderhelm.example.com/runs"
        )
        .is_empty());
        assert_eq!(
            hosted_mentions("go to https://app.coderhelm.com/x").len(),
            1
        );
        assert_eq!(hosted_mentions("mail support@coderhelm.com").len(), 1);
        assert_eq!(
            hosted_mentions("<a href=\"https://coderhelm.com\">").len(),
            1
        );
        assert_eq!(hosted_mentions("API.CODERHELM.COM").len(), 1);
        assert!(hosted_mentions("coderhelm.company").is_empty());
    }

    #[test]
    fn host_for_display() {
        assert_eq!(host_of("https://app.example.com/x"), "app.example.com");
        assert_eq!(host_of("app.example.com"), "app.example.com");
    }
}
