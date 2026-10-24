//! Reviewer calibration: per repository and finding category, how often the
//! team acts on what the reviewer says.
//!
//! A category this repository keeps dismissing is downgraded (raised only as
//! a non-blocking note) or suppressed (left out unless it is a verified
//! defect); one it keeps acting on is raised (the reviewer is told to look
//! for it). Nothing changes before [`MIN_SAMPLES`] outcomes, and security and
//! correctness categories ([`is_exempt`]) are never downgraded or suppressed,
//! however often they are dismissed.

use serde::Serialize;

/// Outcomes needed in one (repo, category) before it changes behavior.
pub const MIN_SAMPLES: u64 = 8;
/// Accept rate at or above which a category is raised.
pub const RAISE_AT: f64 = 0.7;
/// Accept rate at or below which a category is downgraded.
pub const DOWNGRADE_AT: f64 = 0.35;
/// Accept rate at or below which a category is suppressed.
pub const SUPPRESS_AT: f64 = 0.15;
/// A comment left open at close counts as half a dismissal.
pub const IGNORED_WEIGHT: f64 = 0.5;

/// Categories that are never downgraded or suppressed.
const EXEMPT: &[&str] = &[
    "security",
    "correctness",
    "bug",
    "defect",
    "data-loss",
    "data loss",
    "privacy",
    "auth",
    "authentication",
    "authorization",
    "safety",
    "secrets",
    "injection",
    "concurrency",
    "source-data-override",
];

/// Is `category` one the reviewer must keep raising whatever the team does?
pub fn is_exempt(category: &str) -> bool {
    let c = category.trim().to_ascii_lowercase().replace('_', "-");
    if c.is_empty() {
        // Unknown means we cannot prove it is safe to quiet.
        return true;
    }
    EXEMPT.iter().any(|e| c == *e)
        || c.contains("secur")
        || c.contains("vuln")
        || c.contains("correct")
        || c.contains("data-loss")
}

/// What became of one (repo, category)'s findings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct Counts {
    pub accepted: u64,
    pub dismissed: u64,
    pub ignored: u64,
}

impl Counts {
    pub fn samples(&self) -> u64 {
        self.accepted + self.dismissed + self.ignored
    }

    /// accepted / (accepted + dismissed + ½·ignored); None with no outcomes.
    pub fn accept_rate(&self) -> Option<f64> {
        let denom =
            self.accepted as f64 + self.dismissed as f64 + IGNORED_WEIGHT * self.ignored as f64;
        (denom > 0.0).then(|| self.accepted as f64 / denom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Adjustment {
    None,
    Raise,
    Downgrade,
    Suppress,
}

impl Adjustment {
    pub fn key(self) -> &'static str {
        match self {
            Adjustment::None => "none",
            Adjustment::Raise => "raise",
            Adjustment::Downgrade => "downgrade",
            Adjustment::Suppress => "suppress",
        }
    }

    pub fn from_key(k: &str) -> Adjustment {
        match k {
            "raise" => Adjustment::Raise,
            "downgrade" => Adjustment::Downgrade,
            "suppress" => Adjustment::Suppress,
            _ => Adjustment::None,
        }
    }
}

/// The adjustment the evidence supports.
pub fn decide(category: &str, c: &Counts, min_samples: u64) -> Adjustment {
    if c.samples() < min_samples.max(1) {
        return Adjustment::None;
    }
    let Some(rate) = c.accept_rate() else {
        return Adjustment::None;
    };
    if rate >= RAISE_AT {
        return Adjustment::Raise;
    }
    if is_exempt(category) {
        return Adjustment::None;
    }
    if rate <= SUPPRESS_AT {
        Adjustment::Suppress
    } else if rate <= DOWNGRADE_AT {
        Adjustment::Downgrade
    } else {
        Adjustment::None
    }
}

/// Why, in words people read on the dashboard and in the change log.
pub fn reason(category: &str, c: &Counts, adj: Adjustment) -> String {
    let rate = c.accept_rate().unwrap_or(0.0);
    let pct = (rate * 100.0).round() as u64;
    let seen = format!(
        "{} acted on, {} dismissed, {} left open (accept rate {pct}%)",
        c.accepted, c.dismissed, c.ignored
    );
    match adj {
        Adjustment::Raise => format!("`{category}` comments are acted on here: {seen}"),
        Adjustment::Downgrade => {
            format!("`{category}` comments are mostly not acted on here: {seen}. Raised only as notes")
        }
        Adjustment::Suppress => format!(
            "`{category}` comments are almost never acted on here: {seen}. Left out unless a verified defect"
        ),
        Adjustment::None if is_exempt(category) && rate <= DOWNGRADE_AT => format!(
            "`{category}` is a security or correctness category, so it is always raised: {seen}"
        ),
        Adjustment::None => format!("No change: {seen}"),
    }
}

/// One category's state, as the reviewer and the dashboard see it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CategoryState {
    pub category: String,
    pub counts: Counts,
    pub adjustment: Adjustment,
    pub exempt: bool,
    /// An admin turned this change off for the repository.
    pub reverted: bool,
}

impl CategoryState {
    /// The adjustment that applies: none when reverted.
    pub fn effective(&self) -> Adjustment {
        if self.reverted {
            Adjustment::None
        } else {
            self.adjustment
        }
    }
}

/// The block the reviewer and its critic are given. Empty when nothing applies.
pub fn prompt_block(states: &[CategoryState]) -> String {
    let mut raise = vec![];
    let mut quiet = vec![];
    for s in states {
        let rate = (s.counts.accept_rate().unwrap_or(0.0) * 100.0).round() as u64;
        match s.effective() {
            Adjustment::Raise => raise.push(format!(
                "- `{}`: the team acted on {} of {} ({rate}%). Look for these carefully.",
                s.category,
                s.counts.accepted,
                s.counts.samples()
            )),
            Adjustment::Downgrade => quiet.push(format!(
                "- `{}`: acted on {} of {} ({rate}%). Raise one only as a non-blocking note, and only when it clearly matters.",
                s.category,
                s.counts.accepted,
                s.counts.samples()
            )),
            Adjustment::Suppress => quiet.push(format!(
                "- `{}`: acted on {} of {} ({rate}%). Leave these out unless it is a verified defect.",
                s.category,
                s.counts.accepted,
                s.counts.samples()
            )),
            Adjustment::None => {}
        }
    }
    if raise.is_empty() && quiet.is_empty() {
        return String::new();
    }
    let mut out = String::from(
        "\n\n## What this team acts on in this repository\nLearned from what happened to earlier review comments here.",
    );
    if !raise.is_empty() {
        out.push_str("\nUsually acted on:\n");
        out.push_str(&raise.join("\n"));
    }
    if !quiet.is_empty() {
        out.push_str("\nUsually dismissed or left open:\n");
        out.push_str(&quiet.join("\n"));
    }
    out.push_str("\nSecurity and correctness defects are always raised, whatever this list says.");
    out
}

/// What to do with one finding of `category`, given the repo's states.
/// `verified_defect` findings are never dropped; exempt categories are never
/// touched (their state can only be Raise or None anyway).
pub fn finding_action(
    states: &[CategoryState],
    category: &str,
    verified_defect: bool,
) -> Adjustment {
    if is_exempt(category) {
        return Adjustment::None;
    }
    let found = states
        .iter()
        .find(|s| s.category.eq_ignore_ascii_case(category.trim()));
    match found.map(CategoryState::effective) {
        Some(Adjustment::Suppress) if verified_defect => Adjustment::Downgrade,
        Some(a @ (Adjustment::Suppress | Adjustment::Downgrade)) => a,
        _ => Adjustment::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(a: u64, d: u64, i: u64) -> Counts {
        Counts {
            accepted: a,
            dismissed: d,
            ignored: i,
        }
    }

    #[test]
    fn nothing_changes_before_the_minimum_sample() {
        assert_eq!(decide("style", &c(0, 7, 0), MIN_SAMPLES), Adjustment::None);
        assert_eq!(
            decide("style", &c(0, 8, 0), MIN_SAMPLES),
            Adjustment::Suppress
        );
        assert_eq!(decide("tests", &c(7, 0, 0), MIN_SAMPLES), Adjustment::None);
        assert_eq!(decide("tests", &c(8, 0, 0), MIN_SAMPLES), Adjustment::Raise);
    }

    #[test]
    fn rates_pick_the_adjustment() {
        assert_eq!(decide("style", &c(1, 9, 0), 8), Adjustment::Suppress);
        assert_eq!(decide("style", &c(3, 7, 0), 8), Adjustment::Downgrade);
        assert_eq!(decide("style", &c(5, 5, 0), 8), Adjustment::None);
        assert_eq!(decide("style", &c(8, 2, 0), 8), Adjustment::Raise);
        // Left-open comments count half.
        assert_eq!(c(2, 0, 4).accept_rate(), Some(0.5));
        assert_eq!(c(0, 0, 0).accept_rate(), None);
    }

    #[test]
    fn security_and_correctness_are_never_quieted() {
        for cat in [
            "security",
            "Security",
            "correctness",
            "data_loss",
            "sql-injection-vuln",
            "auth",
            "",
        ] {
            assert!(is_exempt(cat), "{cat}");
            assert_eq!(decide(cat, &c(0, 50, 50), 8), Adjustment::None, "{cat}");
            let states = vec![CategoryState {
                category: cat.into(),
                counts: c(0, 50, 0),
                adjustment: Adjustment::Suppress,
                exempt: false,
                reverted: false,
            }];
            assert_eq!(finding_action(&states, cat, false), Adjustment::None);
        }
        // They can still be raised.
        assert_eq!(decide("security", &c(9, 1, 0), 8), Adjustment::Raise);
        assert!(reason("security", &c(0, 9, 0), Adjustment::None).contains("always raised"));
        assert!(!is_exempt("style"));
    }

    #[test]
    fn findings_follow_the_state_unless_reverted_or_verified() {
        let mut states = vec![
            CategoryState {
                category: "style".into(),
                counts: c(0, 9, 0),
                adjustment: Adjustment::Suppress,
                exempt: false,
                reverted: false,
            },
            CategoryState {
                category: "naming".into(),
                counts: c(3, 7, 0),
                adjustment: Adjustment::Downgrade,
                exempt: false,
                reverted: false,
            },
        ];
        assert_eq!(
            finding_action(&states, "style", false),
            Adjustment::Suppress
        );
        assert_eq!(
            finding_action(&states, "Style", true),
            Adjustment::Downgrade
        );
        assert_eq!(
            finding_action(&states, "naming", false),
            Adjustment::Downgrade
        );
        assert_eq!(finding_action(&states, "tests", false), Adjustment::None);
        states[0].reverted = true;
        assert_eq!(finding_action(&states, "style", false), Adjustment::None);
    }

    #[test]
    fn the_prompt_block_explains_and_keeps_safety() {
        assert_eq!(prompt_block(&[]), "");
        let b = prompt_block(&[
            CategoryState {
                category: "tests".into(),
                counts: c(9, 1, 0),
                adjustment: Adjustment::Raise,
                exempt: false,
                reverted: false,
            },
            CategoryState {
                category: "style".into(),
                counts: c(0, 9, 0),
                adjustment: Adjustment::Suppress,
                exempt: false,
                reverted: false,
            },
        ]);
        assert!(b.contains("`tests`: the team acted on 9 of 10 (90%)"));
        assert!(b.contains("`style`: acted on 0 of 9 (0%)"));
        assert!(b.contains("Security and correctness defects are always raised"));
        assert!(Adjustment::from_key("suppress") == Adjustment::Suppress);
        assert!(Adjustment::from_key("x") == Adjustment::None);
    }
}
