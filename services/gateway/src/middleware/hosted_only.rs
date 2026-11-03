//! The hosted service's platform pages (every team, beta sign-ups, the
//! support queue and the support view, the shared failed-message queue, the
//! usage check, the hybrid installs) answer 404 on a self-hosted install. Its
//! admins manage their team in Settings, and its tickets go to CoderHelm
//! support. Sign-in apps stay: install admins set up connector sign-in there.

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Whether a path under `/api` belongs only to the hosted service.
pub fn hosted_only(path: &str) -> bool {
    let path = path.trim_end_matches('/');
    let admin = path == "/admin" || path.starts_with("/admin/");
    let kept = path == "/admin/sign-in-apps" || path.starts_with("/admin/sign-in-apps/");
    admin && !kept
}

/// Answers 404 for the hosted-only paths on a self-hosted install.
pub async fn hosted_only_on_self_host(req: Request, next: Next) -> Response {
    if crate::routes::setup_checks::self_hosted() && hosted_only(req.uri().path()) {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": "Not found" }))).into_response();
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::hosted_only;

    #[test]
    fn platform_pages_are_hosted_only_and_sign_in_apps_stay() {
        for p in [
            "/admin/teams",
            "/admin/teams/TEAM%23x/flags",
            "/admin/beta-signups",
            "/admin/beta-signups/invite",
            "/admin/support/tickets",
            "/admin/support/tickets/01ABC/reply",
            "/admin/support/view",
            "/admin/dlq",
            "/admin/dlq/delete",
            "/admin/usage-check",
            "/admin/hybrid/summary",
            "/admin/installations/1/release-stale",
            "/admin/",
        ] {
            assert!(hosted_only(p), "{p}");
        }
        for p in [
            "/admin/sign-in-apps",
            "/admin/sign-in-apps/",
            "/me",
            "/support/tickets",
            "/settings/sign-in",
            "/administrators",
        ] {
            assert!(!hosted_only(p), "{p}");
        }
    }
}
