//! Read-only lookups against a team's own systems.
//!
//! Everything that can reach a customer system goes through this module, so the
//! rules hold no matter who asks (a person testing in the dashboard, or the
//! reviewer mid-review):
//!
//! - **Reads only.** A request is parsed and refused unless it is a single
//!   GraphQL query ([`graphql::check_read_only`]). The request body is built
//!   here from the checked text, so nothing else can ride along.
//! - **One address.** Requests go to the registered HTTPS address and nowhere
//!   else: redirects are not followed and addresses that resolve inside a
//!   private network are refused ([`net`]).
//! - **Hidden fields stay hidden.** Values of sensitive-looking fields are
//!   replaced before the result is shown or handed to a model ([`redact`]).
//! - **Keys are sealed.** A team's API key is encrypted for that team alone
//!   before it is stored ([`secret`]), and every lookup is logged ([`store`]).

pub mod api;
pub mod graphql;
pub mod net;
pub mod redact;
pub mod secret;
pub mod store;

use serde::Serialize;

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
