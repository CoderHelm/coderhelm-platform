//! What the person who asked said not to do ("don't comment on the PR",
//! "don't merge", "one push"), read from the ticket and the review thread
//! by the model into structured constraints the passes obey. A word match
//! would miss "please keep the PR quiet" and fire on "we don't merge on
//! Fridays, but go ahead"; the model reads the sentence, and every
//! constraint carries the words it came from, which must be in the text.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The constraints CoderHelm's passes know how to obey.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Constraints {
    /// Post nothing on the pull request (no replies, no comments).
    #[serde(default)]
    pub no_pr_comments: bool,
    /// Never merge the pull request.
    #[serde(default)]
    pub no_merge: bool,
    /// Push once for the request (no extra merge or fix-up pushes).
    #[serde(default)]
    pub single_push: bool,
    /// The words each constraint came from.
    #[serde(default)]
    pub quotes: Vec<String>,
}

impl Constraints {
    pub fn is_empty(&self) -> bool {
        !self.no_pr_comments && !self.no_merge && !self.single_push
    }

    /// Both together: a constraint once said holds for the rest of the run.
    pub fn merged(&self, later: &Constraints) -> Constraints {
        let mut quotes = self.quotes.clone();
        for q in &later.quotes {
            if !quotes.contains(q) {
                quotes.push(q.clone());
            }
        }
        Constraints {
            no_pr_comments: self.no_pr_comments || later.no_pr_comments,
            no_merge: self.no_merge || later.no_merge,
            single_push: self.single_push || later.single_push,
            quotes,
        }
    }

    /// Whether the pass may post on the pull request.
    pub fn may_comment(&self) -> bool {
        !self.no_pr_comments
    }

    /// Whether anything may merge the pull request.
    pub fn may_merge(&self) -> bool {
        !self.no_merge
    }

    /// Whether a push beyond the fix itself (a conflict merge) may go out.
    pub fn may_push_extra(&self) -> bool {
        !self.single_push
    }

    /// Whether the start-of-round update (merging or rebasing the base into
    /// the branch) may push. `explicit` is a person asking for exactly that
    /// update: then it IS the one push, so "one push" never blocks it.
    pub fn may_update_branch(&self, explicit: bool) -> bool {
        explicit || self.may_push_extra()
    }

    /// One line per constraint, for the run page and the evidence.
    pub fn lines(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.no_pr_comments {
            out.push("No comments on the pull request".to_string());
        }
        if self.no_merge {
            out.push("Never merge".to_string());
        }
        if self.single_push {
            out.push("One push".to_string());
        }
        out
    }
}

/// What the extraction is told.
pub const SYSTEM: &str = "You read what a person asked of CoderHelm, an AI that changes code \
and opens pull requests, and list the explicit constraints they put on HOW it works. Only these \
kinds: \"no_pr_comments\" (they said not to comment, reply or post on the pull request), \
\"no_merge\" (they said not to merge it, or that they or someone else will merge), \
\"single_push\" (they said to push once, one commit, or not to push repeatedly). Only what the \
person says to CoderHelm about this work, in their own words: not a description of the code's \
behaviour (\"the job must not merge duplicate rows\" is about the code, not a constraint), not a \
general team policy that doesn't apply now, not something they allow (\"feel free to merge\"). \
For each constraint copy the exact words it comes from into `quote`. No constraint: an empty \
list.";

pub fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "constraints": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "enum": ["no_pr_comments", "no_merge", "single_push"]},
                        "quote": {"type": "string"}
                    },
                    "required": ["kind", "quote"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["constraints"],
        "additionalProperties": false
    })
}

/// The extraction's prompt: each source (where it came from, its text).
pub fn prompt(sources: &[(String, String)]) -> String {
    let mut out = String::new();
    for (from, text) in sources {
        if text.trim().is_empty() {
            continue;
        }
        out.push_str(&format!(
            "## {from}\n{}\n\n",
            common::head_tail_str(text.trim(), 6_000)
        ));
    }
    out
}

fn squash(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
        .replace(['’', '‘'], "'")
}

/// The answer read into constraints. A constraint whose quote is not in
/// the sources is dropped: the person never said it.
pub fn parse(answer: &Value, sources: &[(String, String)]) -> Constraints {
    let all: String = squash(
        &sources
            .iter()
            .map(|(_, t)| t.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let mut c = Constraints::default();
    for item in answer
        .get("constraints")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
    {
        let kind = item.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        let quote = item
            .get("quote")
            .and_then(|q| q.as_str())
            .unwrap_or("")
            .trim();
        if quote.is_empty() || !all.contains(&squash(quote)) {
            continue;
        }
        match kind {
            "no_pr_comments" => c.no_pr_comments = true,
            "no_merge" => c.no_merge = true,
            "single_push" => c.single_push = true,
            _ => continue,
        }
        if !c.quotes.iter().any(|q| q == quote) {
            c.quotes.push(quote.to_string());
        }
    }
    c
}

/// The run record attribute that keeps them.
pub const RUN_ATTR: &str = "requester_constraints";

/// The settings-table sort key that keeps them for one pull request, read
/// by the merge gate (which knows the PR, not the run).
pub fn pr_sk(owner: &str, repo: &str, pr: u64) -> String {
    format!(
        "PRCONSTRAINTS#{}/{}#{pr}",
        owner.to_ascii_lowercase(),
        repo.to_ascii_lowercase()
    )
}

/// Reads them back from their stored JSON.
pub fn from_json(s: &str) -> Constraints {
    serde_json::from_str(s).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(text: &str) -> Vec<(String, String)> {
        vec![("Ticket".to_string(), text.to_string())]
    }

    #[test]
    fn quoted_constraints_are_kept_and_unquoted_ones_dropped() {
        let sources = src("Fix the log level. Please don't comment on the PR and don't merge it; I'll merge after review. One push only.");
        let answer = json!({"constraints": [
            {"kind": "no_pr_comments", "quote": "don't comment on the PR"},
            {"kind": "no_merge", "quote": "don't merge it"},
            {"kind": "single_push", "quote": "One push only."},
            {"kind": "no_merge", "quote": "never merge on Fridays"}
        ]});
        let c = parse(&answer, &sources);
        assert!(c.no_pr_comments && c.no_merge && c.single_push);
        assert_eq!(c.quotes.len(), 3);
        assert!(!c.may_comment() && !c.may_merge() && !c.may_push_extra());
        // An explicit request to update the branch is the one push.
        assert!(!c.may_update_branch(false));
        assert!(c.may_update_branch(true));
        assert!(Constraints::default().may_update_branch(false));
    }

    #[test]
    fn a_made_up_quote_or_unknown_kind_is_no_constraint() {
        let c = parse(
            &json!({"constraints": [
                {"kind": "no_merge", "quote": "do not merge"},
                {"kind": "no_deploy", "quote": "Fix the log level"}
            ]}),
            &src("Fix the log level."),
        );
        assert!(c.is_empty());
        assert!(c.may_comment() && c.may_merge());
        assert!(parse(&json!({}), &src("x")).is_empty());
    }

    #[test]
    fn quotes_match_across_spacing_case_and_curly_apostrophes() {
        let c = parse(
            &json!({"constraints": [{"kind": "no_pr_comments", "quote": "Don't   post on the PR"}]}),
            &src("please don’t post\non the PR"),
        );
        assert!(c.no_pr_comments);
    }

    #[test]
    fn constraints_once_said_hold_and_round_trip_through_storage() {
        let a = Constraints {
            no_merge: true,
            quotes: vec!["don't merge".into()],
            ..Default::default()
        };
        let b = Constraints {
            no_pr_comments: true,
            quotes: vec!["no PR comments".into(), "don't merge".into()],
            ..Default::default()
        };
        let m = a.merged(&b);
        assert!(m.no_merge && m.no_pr_comments && !m.single_push);
        assert_eq!(m.quotes.len(), 2);
        let back = from_json(&serde_json::to_string(&m).unwrap());
        assert_eq!(back, m);
        assert_eq!(from_json("not json"), Constraints::default());
        assert_eq!(
            m.lines(),
            vec!["No comments on the pull request", "Never merge"]
        );
    }

    #[test]
    fn the_prompt_skips_empty_sources_and_the_key_is_per_pr() {
        let p = prompt(&[
            ("Ticket".into(), "Do X".into()),
            ("Review".into(), "  ".into()),
        ]);
        assert!(p.contains("## Ticket\nDo X"));
        assert!(!p.contains("## Review"));
        assert_eq!(pr_sk("Org", "Repo", 7), "PRCONSTRAINTS#org/repo#7");
    }
}
