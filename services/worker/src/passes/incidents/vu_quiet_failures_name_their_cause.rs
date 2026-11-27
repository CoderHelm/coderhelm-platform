//! vu. Three failures on one install that each logged every day and that
//! nobody could act on:
//!
//! - The agent's memory-read counter failed on every run, logged only as
//!   "service error". Its update named the attribute `reads`, a DynamoDB
//!   reserved word, so DynamoDB refused it every time.
//! - A data source in the install's own account was refused
//!   `rds-data:BeginTransaction` on every daily check. The role's grant is
//!   in its template, but only for the databases its stack names, and the
//!   stack the release keeps current there named none; nobody is ever asked
//!   to update that role, so nothing could name it. The access stack that
//!   runs that update was weeks behind, and its card said "the deploy role
//!   may not make  yet", naming nothing.
//! - Gateway requests hit the Lambda's 30 second limit with nothing logged
//!   between START and END.
//!
//! Guarantees: an AWS SDK error on these paths logs its kind (the service's
//! code, or how the request failed); no expression the services send names a
//! reserved word bare (`common::dynamo_names`, which reads every one); every
//! Data API call the code makes is granted by both role templates
//! (`common::log_reader` tests); the release's reader names the databases an
//! admin made a read-only user for; the access card always says what is
//! missing and that only an AWS admin can apply it, and an access stack
//! running an older reader update reads as pending; a gateway request that
//! runs out of time logs its route and where the time went, and answers 503
//! (`gateway::middleware::deadline`).

use common::dynamo_names::bare_reserved;

const RECALL: &str = include_str!("../../memory/recall.rs");
const READER: &str = include_str!("../../../../../lambda/native-reader/handler.py");
const UPDATER: &str = include_str!("../../../../../lambda/updater/handler.py");
const DEADLINE: &str = include_str!("../../../../gateway/src/middleware/deadline.rs");
const GATEWAY: &str = include_str!("../../../../gateway/src/main.rs");

#[test]
fn the_memory_read_counter_names_no_reserved_word_and_logs_its_error_kind() {
    let update = RECALL
        .lines()
        .find(|l| l.starts_with("const AGENT_USE_UPDATE"))
        .expect("the counter's update is one named expression");
    assert!(bare_reserved(update).is_empty(), "{update}");
    assert!(bare_reserved("ADD reads :one, memories :n").contains(&"reads".to_string()));
    let counter = &RECALL[RECALL
        .find("Could not count the agent's memory reads")
        .unwrap()
        - 300..];
    assert!(counter.contains("error_kind = %common::aws_error::kind(&e)"));
}

#[test]
fn the_release_reader_names_the_databases_an_admin_chose() {
    assert!(READER.contains("def databases(sm, stack_id):"));
    assert!(READER.contains("values[\"DatabaseClusters\"] = \",\".join(clusters)"));
    assert!(READER.contains("SECRET_PREFIX = \"coderhelm-readonly-\""));
    assert_eq!(common::live::aws::SECRET_PREFIX, "coderhelm-readonly-");
}

#[test]
fn the_access_card_always_says_what_is_missing_and_who_applies_it() {
    assert!(UPDATER.contains("line = access_needed_line(need, out)"));
    assert!(!UPDATER.contains("the deploy role may not make \"\n            + \", \".join"));
    assert!(UPDATER.contains("reader_parts(access) != existing.get(\"reader_parts\")"));
}

#[test]
fn a_gateway_request_out_of_time_logs_its_route_and_answers() {
    assert!(DEADLINE.contains("\"Request ran out of time\""));
    assert!(DEADLINE.contains("route = %self.route"));
    assert!(DEADLINE.contains("waiting_on = %self.spent.waiting.join(\",\")"));
    assert!(DEADLINE.contains("StatusCode::SERVICE_UNAVAILABLE"));
    assert!(GATEWAY.contains(".layer(axum_middleware::from_fn(middleware::deadline::guard))"));
}
