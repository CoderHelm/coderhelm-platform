//! Every relayed email, written as an install's people get it, kept in
//! `golden/<case>.txt` (From, subject and text) and `golden/<case>.html`.
//! A change to a template shows up as a diff of these files.
//!
//! `UPDATE_GOLDEN=1 cargo test -p common golden` rewrites them;
//! `EMAIL_PREVIEW_DIR=<dir>` also writes each email as a page to open in a
//! browser.

use super::naming;
use super::templates::{check, escape, render, TEMPLATES};
use serde_json::{json, Value};

const APP: &str = "https://helm.example.org";
const HOST: &str = "helm.example.org";
const RELAY_FROM: &str = "notifications@notify.coderhelm.com";

/// One email: a case name, its template and parameters, and the
/// organization the install names.
fn cases() -> Vec<(&'static str, &'static str, Value, Option<&'static str>)> {
    let sys = format!("{APP}/settings/system/");
    let notes = format!("{APP}/settings/system/#version");
    let ready = format!("{APP}/settings/system/#readiness");
    vec![
        ("test_email", "test_email", json!({}), Some("Acme")),
        (
            "team_invite",
            "team_invite",
            json!({"team": "Platform", "inviter": "Dana Lee", "role": "member", "link": format!("{APP}/invite/?code=abc")}),
            Some("Acme"),
        ),
        (
            "welcome",
            "welcome",
            json!({"link": format!("{APP}/")}),
            Some("Acme"),
        ),
        (
            "plugin_approval",
            "plugin_approval",
            json!({
                "plugin": "Release notes", "title": "Weekly release notes for web",
                "excerpt": "Three fixes and one new page this week.", "version": 4,
                "approve_link": format!("{APP}/plugins/approve/?t=SIGNED"),
                "review_link": format!("{APP}/plugins/draft/?id=d1"),
            }),
            Some("Acme"),
        ),
        (
            "knowledge_signin",
            "knowledge_signin",
            json!({"source": "Engineering wiki", "problem": "The sign-in expired on 2026-10-30.", "link": format!("{APP}/settings/knowledge/")}),
            Some("Acme"),
        ),
        (
            "token_warning",
            "token_warning",
            json!({"limit_name": "new tokens", "month": "October 2026", "used": "8.1M", "limit": "10M", "pct": 81, "link": format!("{APP}/settings/usage/")}),
            Some("Acme"),
        ),
        (
            "token_limit_reached",
            "token_limit_reached",
            json!({"limit_name": "new tokens", "month": "October 2026", "used": "10.0M", "limit": "10M", "link": format!("{APP}/settings/usage/")}),
            Some("Acme"),
        ),
        (
            "support_update",
            "support_update",
            json!({"ticket": "Runs stop after the build step", "note": "Fixed in v0.8.1: update when you can.", "link": format!("{APP}/support/ticket/?id=7")}),
            Some("Acme"),
        ),
        (
            "weekly_summary",
            "weekly_summary",
            json!({
                "team": "Platform", "period": "Oct 19 to Oct 25", "merged": 14, "runs": 22,
                "reviews": 31, "waiting": 2, "highlights": "Billing page shipped.\nFlaky test in api fixed.",
                "link": format!("{APP}/"), "unsubscribe": format!("{APP}/unsubscribe/?t=SIGNED"),
            }),
            Some("Acme"),
        ),
        (
            "webhook_disabled",
            "webhook_disabled",
            json!({"days": 3, "link": format!("{APP}/settings/webhooks/")}),
            Some("Acme"),
        ),
        (
            "two_factor_lockout",
            "two_factor_lockout",
            json!({"minutes": 15, "link": format!("{APP}/settings/sign-in/")}),
            Some("Acme"),
        ),
        (
            "two_factor_required",
            "two_factor_required",
            json!({"link": format!("{APP}/settings/security/")}),
            Some("Acme"),
        ),
        (
            "two_factor_reset",
            "two_factor_reset",
            json!({"link": format!("{APP}/settings/security/")}),
            Some("Acme"),
        ),
        (
            "update_available",
            "update_notice",
            json!({
                "what": "available", "version": "v0.8.0", "current": "v0.7.4", "link": sys,
                "checked_at": "2026-10-31 14:05 UTC", "waiting": "automatic updates are off",
                "notes": "Faster reviews on large pull requests.\nA guided uninstall.", "notes_link": notes,
            }),
            Some("Acme"),
        ),
        (
            "update_available_major",
            "update_notice",
            json!({
                "what": "available", "version": "v1.0.0", "current": "v0.8.3", "link": sys, "major": "yes",
                "checked_at": "2026-10-31 14:05 UTC", "waiting": "a major release waits for an admin",
                "notes_link": notes,
            }),
            None,
        ),
        (
            "update_blocked",
            "update_notice",
            json!({
                "what": "blocked", "version": "v0.8.0", "current": "v0.7.4", "link": ready,
                "checked_at": "2026-10-31 14:05 UTC",
                "reason": "- claude-opus-5-5: Bedrock refused the call: the model's terms aren't accepted in this account.",
                "action": "In the AWS console, open Amazon Bedrock, then Model access, and request access to Claude Opus 5.5. A person has to do this; CoderHelm only checks.",
                "notes_link": notes,
            }),
            Some("Acme"),
        ),
        (
            "update_succeeded",
            "update_notice",
            json!({"what": "succeeded", "version": "v0.8.0", "current": "v0.7.4", "link": sys, "checked_at": "2026-10-31 14:22 UTC", "notes_link": notes}),
            Some("Acme"),
        ),
        (
            "update_failed",
            "update_notice",
            json!({
                "what": "failed", "version": "v0.8.0", "current": "v0.7.4", "link": sys,
                "checked_at": "2026-10-31 14:22 UTC",
                "reason": "The deploy stopped: the api stack failed to update (resource limit reached).",
                "action": "Open the update's log under Update history, fix what it names, then press Update now.",
                "notes_link": notes,
            }),
            Some("Acme"),
        ),
        (
            "update_rolled_back",
            "update_notice",
            json!({
                "what": "rolled_back", "version": "v0.8.0", "current": "v0.7.4", "link": sys,
                "checked_at": "2026-10-31 14:30 UTC", "reason": "The health check failed after the deploy: the gateway didn't answer.",
                "notes_link": notes,
            }),
            Some("Acme"),
        ),
        (
            "domain_live",
            "update_notice",
            json!({"what": "domain_live", "link": format!("{APP}/settings/system/#domain"), "checked_at": "2026-10-31 15:00 UTC"}),
            Some("Acme"),
        ),
        (
            "domain_failed",
            "update_notice",
            json!({"what": "domain_failed", "link": format!("{APP}/settings/system/#domain"), "checked_at": "2026-10-31 15:00 UTC", "reason": "The certificate is still waiting for its DNS record."}),
            Some("Acme"),
        ),
        (
            "domain_removed",
            "update_notice",
            json!({"what": "domain_removed", "link": format!("{APP}/settings/system/#domain"), "checked_at": "2026-10-31 15:00 UTC"}),
            Some("Acme"),
        ),
        // What an install older than these templates still sends.
        (
            "update_blocked_legacy",
            "update_notice",
            json!({"what": "blocked", "version": "v0.8.0", "link": sys}),
            None,
        ),
        (
            "relay_paused",
            "relay_paused",
            json!({"reason": "bounces"}),
            None,
        ),
    ]
}

fn golden_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/mail/golden")
}

/// The From line, subject, text and HTML of one case, as the relay writes
/// them for an install on `helm.example.org`.
fn written(
    template: &str,
    params: &Value,
    org: Option<&str>,
) -> (String, super::templates::Rendered) {
    let t = super::templates::get(template).unwrap();
    let hosts = vec![HOST.to_string()];
    let (t, v) = check(
        t.id,
        params.as_object().unwrap(),
        &hosts,
        t.install_may_send,
    )
    .unwrap_or_else(|e| panic!("{template}: {e:?}"));
    // As the relay names it: the organization, else the install's domain.
    let label = naming::label(org, Some(HOST), None, "coderhelm.com");
    let from = if t.install_may_send {
        naming::from_header(&naming::display_name(label.as_deref()), RELAY_FROM)
    } else {
        naming::from_header("CoderHelm mail service", RELAY_FROM)
    };
    let r = render(t, &v, HOST, Some("admin@example.org"), label.as_deref());
    (from, r)
}

#[test]
fn every_relayed_email_matches_its_golden_file() {
    let dir = golden_dir();
    let update = std::env::var("UPDATE_GOLDEN").is_ok_and(|v| v == "1");
    let preview = std::env::var("EMAIL_PREVIEW_DIR").ok();
    let mut seen = std::collections::BTreeSet::new();
    for (name, template, params, org) in cases() {
        seen.insert(template);
        let (from, r) = written(template, &params, org);
        let txt = format!("From: {from}\nSubject: {}\n\n{}\n", r.subject, r.text);
        let html = format!("{}\n", r.html);
        let (tp, hp) = (
            dir.join(format!("{name}.txt")),
            dir.join(format!("{name}.html")),
        );
        if update {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(&tp, &txt).unwrap();
            std::fs::write(&hp, &html).unwrap();
        }
        if let Some(p) = &preview {
            let page = format!(
                "<!-- {name} -->\n<div style=\"font:13px/1.5 Menlo,monospace;background:#f4f4f4;padding:12px 24px;border-bottom:1px solid #ddd\">From: {}<br>Subject: {}</div>\n{}",
                escape(&from),
                escape(&r.subject),
                r.html
            );
            std::fs::write(std::path::Path::new(p).join(format!("{name}.html")), page).unwrap();
        }
        let want = std::fs::read_to_string(&tp)
            .unwrap_or_else(|_| panic!("{name}: no golden file; run with UPDATE_GOLDEN=1"));
        assert_eq!(
            txt, want,
            "{name}: text changed (UPDATE_GOLDEN=1 to accept)"
        );
        let want = std::fs::read_to_string(&hp).unwrap();
        assert_eq!(
            html, want,
            "{name}: HTML changed (UPDATE_GOLDEN=1 to accept)"
        );
    }
    for t in TEMPLATES {
        assert!(seen.contains(t.id), "{} has no golden case", t.id);
    }
}

#[test]
fn every_email_states_whose_install_what_happened_and_where_to_go() {
    for (name, template, params, org) in cases() {
        let (from, r) = written(template, &params, org);
        assert!(!from.contains("cloudfront"), "{name}: {from}");
        assert!(r.text.contains(HOST), "{name}: names the install");
        // A plain subject: never the old one-word kind.
        assert!(
            !r.subject.to_lowercase().ends_with("update notice"),
            "{name}"
        );
        // Every email but the relay's own pause notice has a working link.
        if !matches!(template, "relay_paused" | "test_email") {
            assert!(r.html.contains(&format!("href=\"{APP}/")), "{name}: a link");
        }
    }
}

#[test]
fn the_update_notice_says_which_versions_why_and_what_to_do() {
    let (_, r) = written(
        "update_notice",
        &cases()
            .into_iter()
            .find(|c| c.0 == "update_blocked")
            .unwrap()
            .2,
        Some("Acme"),
    );
    assert_eq!(r.subject, "Acme CoderHelm: update to v0.8.0 needs your OK");
    for want in [
        "Install: helm.example.org",
        "Running now: v0.7.4",
        "Waiting to install: v0.8.0",
        "Checked: 2026-10-31 14:05 UTC",
        "Why it's blocked:",
        "terms aren't accepted",
        "What to do:",
        "Update readiness",
        "Check again",
        "npx -y github:CoderHelm/coderhelm-platform update --to v0.8.0",
        "Read the release notes: https://helm.example.org/settings/system/#version",
    ] {
        assert!(r.text.contains(want), "missing {want:?} in\n{}", r.text);
    }
}

#[test]
fn a_cloudfront_install_still_gets_a_human_sender_name() {
    // No organization, no domain, no CoderHelm name: plain CoderHelm, never
    // the CloudFront host.
    let label = super::relay::sender_label(None, None, None, 0);
    let from = naming::from_header(&naming::display_name(label.as_deref()), RELAY_FROM);
    assert_eq!(from, "\"CoderHelm\" <notifications@notify.coderhelm.com>");
}
