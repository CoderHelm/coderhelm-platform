//! Model ids a team can have saved, and what a retired one runs as now.
//!
//! Anthropic retires model ids; a saved id that no longer exists makes every
//! call fail with `not_found_error`. A retired id is read as its current
//! replacement wherever a team's setting is loaded, so old settings keep
//! working, and the settings page says what changed.

/// A retired id and the id that replaces it.
const RETIRED: &[(&str, &str)] = &[
    ("claude-sonnet-4-20250514", "claude-sonnet-5-5"),
    ("claude-opus-4-20250514", "claude-opus-5-5"),
    ("claude-haiku-4-5-20250514", "claude-haiku-4-5"),
];

/// The id to call for a saved id: its replacement when it is retired.
pub fn current(id: &str) -> &str {
    RETIRED
        .iter()
        .find(|(old, _)| *old == id)
        .map(|(_, new)| *new)
        .unwrap_or(id)
}

/// Whether a saved id is retired.
pub fn is_retired(id: &str) -> bool {
    RETIRED.iter().any(|(old, _)| *old == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retired_id_runs_as_its_replacement() {
        assert_eq!(current("claude-sonnet-4-20250514"), "claude-sonnet-5-5");
        assert_eq!(current("claude-opus-4-20250514"), "claude-opus-5-5");
        assert!(is_retired("claude-sonnet-4-20250514"));
    }

    #[test]
    fn a_current_id_is_left_alone() {
        assert_eq!(current("claude-opus-4-8"), "claude-opus-4-8");
        assert!(!is_retired("claude-sonnet-5-5"));
    }
}
