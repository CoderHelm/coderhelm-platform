//! Read-only lookups against a team's own systems.
//!
//! Everything that can reach a customer system goes through this module, so the
//! rules hold no matter who asks (a person testing in the dashboard, or the
//! reviewer mid-review):
//!
//! - **Reads only.** A request is parsed and refused unless it is a single
//!   GraphQL query ([`graphql::check_read_only`]), a single SQL SELECT
//!   ([`sql::check_read_only_sql`]) or a Logs Insights search over the log
//!   groups a team shared ([`logs::check_search`]). What is sent is built here
//!   from the checked text, so nothing else can ride along. SQL also runs in a
//!   rolled-back read-only transaction as a user that can only read ([`db`]).
//! - **One address.** Requests go to the registered HTTPS address and nowhere
//!   else: redirects are not followed and addresses that resolve inside a
//!   private network are refused ([`net`]).
//! - **Hidden fields stay hidden.** A lookup that asks for a field a team has
//!   hidden is refused, and anything hidden that still comes back is replaced
//!   before the result is shown or handed to a model ([`redact`]).
//! - **Keys are sealed.** A team's API key is encrypted for that team alone
//!   before it is stored ([`secret`]), and every lookup is logged ([`store`]).
//! - **AWS through a read-only role.** Logs and databases are reached through a
//!   role the team creates in its own account, usable only with that team's
//!   private ID and only able to read ([`aws`]).

pub mod api;
pub mod aws;
pub mod db;
pub mod graphql;
pub mod logs;
pub mod net;
pub mod redact;
pub mod redshift;
pub mod secret;
pub mod sql;
pub mod store;
pub mod tools;
pub mod tools_run;
pub mod tools_store;

use serde::Serialize;

/// `1 row`, `3 rows`: a count with its noun, for notes people read.
pub fn count(n: usize, noun: &str) -> String {
    if n == 1 {
        format!("1 {noun}")
    } else {
        format!("{n} {noun}s")
    }
}

/// A failure a person can act on: which step failed, what happened, what to do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StepError {
    /// `reach` | `sign_in` | `read` | `read_only`
    pub step: &'static str,
    pub message: String,
    pub fix: String,
}

impl StepError {
    pub fn new(step: &'static str, message: impl Into<String>, fix: impl Into<String>) -> Self {
        Self {
            step,
            message: message.into(),
            fix: fix.into(),
        }
    }

    /// True when the request was refused by our own read-only rule (it was
    /// never sent).
    pub fn is_refusal(&self) -> bool {
        self.step == "read_only"
    }
}

impl std::fmt::Display for StepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.message, self.fix)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn counts_read_naturally() {
        assert_eq!(super::count(1, "row"), "1 row");
        assert_eq!(super::count(0, "row"), "0 rows");
        assert_eq!(super::count(40, "line"), "40 lines");
    }
}
