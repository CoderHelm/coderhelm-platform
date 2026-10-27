//! E2E test intelligence: reading test reports from CI, telling flaky tests
//! from broken ones, and keeping what was learned.
//!
//! - [`parse`] reads Playwright JSON, JUnit XML and Cucumber JSON reports,
//!   keeping every attempt.
//! - [`signature`] groups failures by what went wrong.
//! - [`detect`] decides healthy, flaky, broken or fixed; pure and tested.
//! - [`coverage`] maps code to the areas tests cover.
//! - [`store`] keeps runs, tests, failure groups and recommendations.
//!
//! Fixing a failing or flaky test from a ticket or pull request:
//!
//! - [`report_source`] is where a team keeps each repository's reports.
//! - [`failure`] reads the real error and the page state at the failure.
//! - [`flake`] counts passes and failures across the reports kept.
//! - [`fix_guard`] refuses waits without evidence and helpers that walk
//!   every row.
//! - [`test_package`] checks test code in its own package and repeats the
//!   affected spec.
//! - [`check_gate`] keeps the pull request a draft until the checks that
//!   test it are green on its head.
//! - [`fix_ticket`] reads the ticket and shapes the description.
//! - [`integrity`] refuses fixes that ask less of a test.
//! - [`attribution`] decides whether a failure on Coderhelm's own pull
//!   request comes from the change, from the page and the code it runs.
//! - [`bug_rules`] tells a bug from a flake and holds a bug fix to the app,
//!   with a unit test, keeping only the changes that matter.

pub mod attribution;
pub mod bug_rules;
pub mod check_gate;
pub mod ci_setup;
pub mod coverage;
pub mod detect;
pub mod failure;
pub mod fix_guard;
pub mod fix_ticket;
pub mod flake;
pub mod integrity;
pub mod messages;
pub mod model;
pub mod owners;
pub mod parse;
pub mod report_source;
pub mod signature;
pub mod stats;
pub mod store;
pub mod test_package;
pub mod triage;
