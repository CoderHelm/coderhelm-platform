//! When a run may stop, and how it says why, for every team.
//!
//! A run stops for a person only when a person is what it needs. Four rules,
//! each enforced here and called where a run would stop:
//!
//! 1. [`tooling`]: the run's own tools failing (a missing dependency, a
//!    command that isn't installed, an install that failed, the sandbox) is
//!    never a question. The run installs the package's dependencies with the
//!    package's own manager and tries again; when that still fails it ends as
//!    a tooling failure that names the exact command and error.
//! 2. [`environment`]: a test-related run reads where the tests run (base
//!    address and its overrides, data sources, fixtures) before it judges
//!    data, and records it. A difference between environments (production
//!    and staging content) is never a contradiction and never a stop reason.
//! 3. [`screen`]: when the ticket states the root cause, a fix plan and
//!    acceptance criteria, the plan is followed. A question stops the run
//!    only when it cites evidence (`file:line` or a lookup) that contradicts
//!    the ticket; others are dropped and the run goes on.
//! 4. [`existing_work`]: an open pull request (by anyone) that already fixes
//!    the same failing test or ticket ends the run as covered by it, linked
//!    in the run summary and on the ticket, unless the ticket asks for a
//!    separate change.

pub mod environment;
pub mod existing_work;
pub mod screen;
pub mod tooling;

/// Lower-cased with runs of whitespace as one space, for quote checks.
pub(crate) fn flat(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// Whether `quote` appears in `text`, ignoring case, whitespace runs and
/// surrounding quote marks. Short quotes (under 8 characters) never count.
pub(crate) fn quoted_in(quote: &str, text: &str) -> bool {
    let q = flat(
        quote
            .trim()
            .trim_matches(|c| c == '"' || c == '\'' || c == '`'),
    );
    q.chars().count() >= 8 && flat(text).contains(&q)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_are_found_whatever_the_spacing_and_case() {
        let text = "Root cause:\n  the test   reads the WRONG card.";
        assert!(quoted_in("the test reads the wrong card", text));
        assert!(quoted_in("\"the test reads the wrong card\"", text));
        assert!(!quoted_in("the app is wrong", text));
        assert!(!quoted_in("card", text));
    }
}
