//! sg. A Confluence knowledge source on a hybrid install stopped syncing
//! for fourteen and a half hours and told its team to reconnect, though
//! nothing about its sign-in was wrong. One scheduled sync asked the
//! hosted broker for a token while Atlassian's sign-in server answered
//! `503`. The broker's runtime called that passing, as it is, and the
//! broker then wrote the passing reason as a debug dump under
//! `invalid_grant` with HTTP 400: "invalid_grant: Temporary(\"The sign-in
//! server refused (503).\")". The install read `invalid_grant` as a revoked
//! grant, marked its connection as needing a reconnect, and every
//! scheduled sync after it stopped in no time at all on that stored mark,
//! 38 in a row, each one "Stopped" with "Reconnect Confluence to sign in
//! again." A person signed in again and the next check worked at once.
//!
//! Guarantee: only a definitive refusal (HTTP 400 or 401 with
//! `invalid_grant` or the provider's revoked or expired refresh signal)
//! asks a person to reconnect. No answer, a timeout, a `429` or a `5xx` is
//! passing whatever words it carries: the refresh is asked again within the
//! run, then left for the next sync, and nothing is marked or wiped. The
//! broker answers a passing failure as `temporarily_unavailable` with HTTP
//! 503, never `invalid_grant`. A sync that ends on a passing cause is
//! recorded as "retrying", keeps the source as it was, and says once how
//! many syncs in a row and since when; one that finishes clears it. Only
//! one refresher at a time sends a refresh token, so a scheduled sync and a
//! check by hand can't spend Atlassian's rotating refresh token twice.

use common::knowledge::sources::connect::passing_sign_in;
use common::knowledge::sources::runs::{stop_result, stopped};
use common::knowledge::sources::{note_finished, note_stop, ConnectorError, Kind, Source, Status};
use common::mcp_remote::http::HttpResponse;
use common::mcp_remote::oauth::{
    parse_token_response, sign_in_provider, sign_in_unavailable, TokenError,
};
use common::mcp_remote::runtime::ConnError;

fn reply(status: u16, body: &str) -> HttpResponse {
    HttpResponse {
        status,
        headers: vec![("content-type".into(), "application/json".into())],
        body: body.as_bytes().to_vec(),
    }
}

fn refusal(status: u16, body: &str) -> TokenError {
    parse_token_response(&reply(status, body), 0, None).unwrap_err()
}

fn confluence() -> Source {
    Source {
        id: "s1".into(),
        kb_id: "kb1".into(),
        kind: Some(Kind::Confluence),
        name: "Engineering wiki".into(),
        status: Status::Ready,
        last_success_at: "2026-11-23T13:18:54+00:00".into(),
        ..Default::default()
    }
}

#[test]
fn atlassians_503_is_passing_and_only_a_definitive_refusal_asks_for_a_reconnect() {
    // Atlassian's own answer at the broker.
    let atlassian = refusal(503, "Service Unavailable");
    assert!(atlassian.is_passing() && !atlassian.needs_reconnect());
    // Even a server error that says invalid_grant is a server error.
    assert!(refusal(503, r#"{"error":"invalid_grant"}"#).is_passing());
    // What the fixed broker sends the install for it.
    let broker = refusal(
        503,
        r#"{"error":"temporarily_unavailable","error_description":"Atlassian's sign-in server was briefly unavailable (HTTP 503); retrying."}"#,
    );
    assert!(broker.is_passing() && !broker.needs_reconnect());
    // A revoked grant still asks for a reconnect.
    assert!(refusal(
        400,
        r#"{"error":"invalid_grant","error_description":"revoked"}"#
    )
    .needs_reconnect());
    assert!(refusal(401, r#"{"error":"invalid_client"}"#).needs_reconnect());
}

#[test]
fn the_words_say_retrying_and_name_atlassian_never_reconnect() {
    let who = sign_in_provider(
        "https://broker.example.com/hybrid/oauth/token",
        "source:confluence",
        "Confluence",
    );
    let why = sign_in_unavailable(&who, 503, false);
    assert_eq!(
        why,
        "Atlassian's sign-in server was briefly unavailable (HTTP 503); retrying."
    );
    let e = passing_sign_in(ConnError::Busy {
        why: why.clone(),
        retry_after: None,
        rate_limited: false,
    });
    assert!(e.is_busy());
    let shown = e.for_source(Kind::Confluence);
    assert_eq!(shown.action, "retry");
    for text in [&shown.message, &shown.fix] {
        assert!(!text.contains("Reconnect"), "{text}");
        assert!(!text.contains("invalid_grant"), "{text}");
        assert!(!text.contains("Temporary("), "{text}");
    }
}

#[test]
fn stops_in_a_row_are_retrying_said_once_and_cleared_by_a_sync_that_works() {
    let mut s = confluence();
    let passing = passing_sign_in(ConnError::Busy {
        why: sign_in_unavailable("Atlassian", 503, false),
        retry_after: None,
        rate_limited: false,
    })
    .for_source(Kind::Confluence);
    // 14:33 UTC, then every half hour: the incident's 38 stops.
    for i in 0..38 {
        let run = stopped(&s, "schedule", 1_795_444_434 + i * 1_800, passing.clone());
        assert_eq!(run.result, "retrying");
        note_stop(&mut s, passing.clone(), 1_795_444_434 + i * 1_800);
    }
    assert_eq!(s.status, Status::Ready, "nothing for a person to fix");
    assert!(s.error.is_none());
    assert_eq!(s.stops, 38);
    assert_eq!(s.stopped_since, "2026-11-23T14:33:54+00:00");
    assert_eq!(stop_result(s.retrying.as_ref().unwrap()), "retrying");
    note_finished(&mut s);
    assert_eq!((s.stops, s.retrying.is_none()), (0, true));
    // A cause a person must fix still stops and asks for it.
    let gone = ConnectorError::Reconnect("revoked".into()).for_source(Kind::Confluence);
    assert_eq!(stopped(&s, "schedule", 0, gone.clone()).result, "stopped");
    note_stop(&mut s, gone, 0);
    assert_eq!(s.status, Status::Error);
}

#[test]
fn the_broker_and_the_runtime_keep_the_classification_where_it_runs() {
    let hybrid = include_str!("../../../../gateway/src/routes/hybrid.rs");
    let handler = &hybrid[hybrid.find("pub async fn oauth_token(").unwrap()
        ..hybrid.find("pub async fn oauth_revoke(").unwrap()];
    assert!(handler.contains("broker_refusal(&e)"));
    assert!(!handler.contains(concat!("{e", ":?}")));
    assert!(hybrid.contains("StatusCode::SERVICE_UNAVAILABLE"));
    let connect = include_str!("../../../../common/src/knowledge/sources/connect.rs");
    assert!(!connect.contains(concat!("Temporary(format!(\"{e", ":?}\"))")));
    assert!(connect.contains("Err(e) => Err(passing_sign_in(e)),"));
    let runtime = include_str!("../../../../common/src/mcp_remote/runtime.rs");
    let bearer = &runtime[runtime.find("async fn bearer(").unwrap()
        ..runtime.find("async fn newer_tokens(").unwrap()];
    // One refresher at a time, passing refusals asked again, and the
    // mark only on a definitive refusal.
    assert!(bearer.contains(".claim_refresh("));
    assert!(bearer.contains("e.is_passing()"));
    assert!(bearer.contains("Err(e) if e.needs_reconnect() =>"));
    assert!(bearer.contains("key_service_passing(&e)"));
}
