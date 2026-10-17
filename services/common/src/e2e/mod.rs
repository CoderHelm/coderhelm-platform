//! E2E test intelligence: reading test reports from CI, telling flaky tests
//! from broken ones, and keeping what was learned.
//!
//! - [`parse`] reads Playwright JSON, JUnit XML and Cucumber JSON reports,
//!   keeping every attempt.
//! - [`signature`] groups failures by what went wrong.
//! - [`detect`] decides healthy, flaky, broken or fixed; pure and tested.
//! - [`coverage`] maps code to the areas tests cover.
//! - [`store`] keeps runs, tests, failure groups and recommendations.

pub mod coverage;
pub mod detect;
pub mod messages;
pub mod model;
pub mod owners;
pub mod parse;
pub mod signature;
pub mod stats;
pub mod store;
pub mod triage;
