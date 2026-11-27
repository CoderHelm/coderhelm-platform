//! su. Chat stopped working on a self-hosted install right after the
//! Require single sign-on release. Every question in Chat, and every Ask on
//! the App map, answered "Your session ended. Sign in again." (code
//! `session_ended`), and signing in again changed nothing. The streaming
//! function logged `Member record not read; request refused`
//! (`error="service error"`).
//!
//! The release made each chat stream check the session its token was minted
//! from (`require_sso::check_scoped`), which reads the member's record from
//! the users table (`sign_in::check_member_session`). The chat streams run
//! in their own Lambda, whose role grants table by table and had no grant on
//! the users table, so every read was refused. The main gateway, which can
//! read every table, passed every test. The brain stream then reported the
//! failed read as an ended session.
//!
//! Guarantee: the streaming role reads the users table (and the teams table
//! a refusal names), and a list of every DynamoDB call the stream routes
//! make, recorded by driving them against the AWS stand-in, is held against
//! the synthesized role by the infra tests. A member record that can't be
//! read is a server problem ("Chat couldn't check your sign-in right now.
//! Try again."), never `session_ended`.

use std::path::{Path, PathBuf};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(repo().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

/// The body of `fn name` in `src` (to the next top-level item).
fn body<'a>(src: &'a str, name: &str) -> &'a str {
    let at = src.find(name).unwrap_or_else(|| panic!("no {name}"));
    let rest = &src[at..];
    let end = rest[1..]
        .find("\npub ")
        .or_else(|| rest[1..].find("\nfn "))
        .map_or(rest.len(), |e| e + 1);
    &rest[..end]
}

#[test]
fn the_streaming_role_reads_the_member_record_its_check_needs() {
    let listed: serde_json::Value =
        serde_json::from_str(&read("infra/streaming-dynamodb.json")).expect("valid JSON");
    let users = listed["dynamodb"]["users"]
        .as_array()
        .expect("the users table is listed");
    assert!(users.iter().any(|a| a == "dynamodb:GetItem"));

    let stack = read("infra/lib/streaming-stack.ts");
    assert!(
        stack.contains("table/${prefix}-users`"),
        "the streaming role names the users table"
    );
    // The infra tests hold the role to the list, and the gateway's tests
    // hold the stream routes' DynamoDB calls to it.
    let infra_test = read("infra/test/streaming-access.test.ts");
    assert!(infra_test.contains("streaming-dynamodb.json"));
    let gateway_test = read("services/gateway/src/routes/stream_access.rs");
    assert!(gateway_test.contains("streaming-dynamodb.json"));
    assert!(gateway_test.contains("dynamo_calls()"));
    assert!(gateway_test.contains("\"/api/brain/stream\""));
    assert!(gateway_test.contains("\"/api/plans/chat/stream\""));
}

#[test]
fn a_record_that_cant_be_read_is_never_an_ended_session() {
    let brain = read("services/gateway/src/routes/brain.rs");
    let refusal = body(&brain, "pub fn scoped_refusal(");
    assert!(refusal.contains("is_server_error()"));
    assert!(refusal.contains("Chat couldn't check your sign-in right now. Try again."));
    let stream = body(&brain, "pub async fn brain_stream(");
    let check = stream
        .find("require_sso::check_scoped(")
        .expect("the stream checks its session");
    let after = &stream[check..];
    assert!(
        after[..after.find("brain_ask").unwrap_or(after.len())].contains("scoped_refusal("),
        "the brain stream answers a failed check through scoped_refusal"
    );
    // The member check itself refuses a failed read with a server error.
    let sign_in = read("services/gateway/src/routes/sign_in.rs");
    let member = body(&sign_in, "pub async fn check_member_session(");
    let read_failed = &member[member.find("Err(e) =>").expect("a failed read")..];
    assert!(read_failed[..200].contains("SERVICE_UNAVAILABLE"));
}
