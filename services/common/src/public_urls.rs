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
//! With neither (local runs, tests) the stage decides, as it always did.

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

fn current() -> Option<&'static Urls> {
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

/// The dashboard's base URL, e.g. `https://app.example.com` (no trailing slash).
pub fn app_url(stage: &str) -> &'static str {
    current()
        .map(|u| u.app.as_str())
        .unwrap_or_else(|| stage_app_url(stage))
}

/// The API's base URL (webhooks, OAuth callbacks, unsubscribe links).
pub fn api_url(stage: &str) -> &'static str {
    current()
        .map(|u| u.api.as_str())
        .unwrap_or_else(|| stage_api_url(stage))
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
    static SITE: OnceLock<Option<String>> = OnceLock::new();
    SITE.get_or_init(|| site_from(std::env::var("SITE_URL").ok()))
        .as_deref()
}

/// [`site_url`]'s rule, on a given value.
pub fn site_from(v: Option<String>) -> Option<String> {
    match v {
        None => Some(HOSTED_SITE.to_string()),
        Some(s) if s.trim().is_empty() => None,
        Some(s) => Some(clean(&s)),
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
    fn host_for_display() {
        assert_eq!(host_of("https://app.example.com/x"), "app.example.com");
        assert_eq!(host_of("app.example.com"), "app.example.com");
    }
}
