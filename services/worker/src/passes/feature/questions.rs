//! Rule 8: product questions the ticket doesn't settle are decided with
//! their default, never left open in the pull request.
//!
//! A question that blocks the change stops the run (the plan's
//! `CLARIFICATION_NEEDED:`). A product decision the ticket doesn't settle,
//! that the change can go ahead without (who sees a new block, which
//! audience a rule applies to), is decided at plan time with its default
//! (today's behaviour, or the smallest choice the ticket allows) and listed
//! in design.md under `## Out of scope` (`common::scope_ledger`). A plan
//! that still writes [`HEADING`], or a coder note starting [`NOTE_MARK`],
//! is read the same way: each becomes an out-of-scope entry decided with
//! its default, and the pull request says what was decided, never asks.
//! A pull request once carried three such questions after its plan had
//! already answered each with "meanwhile", and a later round built one of
//! them anyway.

pub const HEADING: &str = "## Open product questions";
/// A coder's note line that raises one.
pub const NOTE_MARK: &str = "Open product question:";

/// What the planner is told.
pub const PLANNER: &str = "If you find a product decision the ticket doesn't settle and the \
     change can go ahead without it (for example, which audience a rule applies to), decide it \
     with its default (keep today's behaviour, or the smallest choice the ticket allows) and list \
     it in design.md under `## Out of scope` as `- <item>: decided with the default: <default>`. \
     Never write open questions for the team; the pull request states the decision. Only a \
     question that stops the change belongs in CLARIFICATION_NEEDED.";

fn clean(s: &str) -> String {
    s.trim()
        .trim_start_matches(['-', '*'])
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// The questions from the plan's design and the coder's notes, deduped.
pub fn collect(design: &str, notes: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |q: String| {
        let none = q.is_empty() || {
            let l = q.to_lowercase();
            l == "none" || l.starts_with("none.") || l.starts_with("no open")
        };
        if !none && !out.iter().any(|x| x.eq_ignore_ascii_case(&q)) {
            out.push(q);
        }
    };
    if let Some(s) = super::reuse::section(design, HEADING) {
        for l in s.lines() {
            let t = l.trim_start();
            if t.starts_with("- ") || t.starts_with("* ") {
                push(clean(t));
            }
        }
    }
    for l in notes.lines() {
        if let Some(q) = l
            .trim_start()
            .trim_start_matches(['-', '*', ' '])
            .strip_prefix(NOTE_MARK)
        {
            push(clean(q));
        }
    }
    out
}

/// The description's section: each question decided with its default,
/// under "Out of scope". Empty without questions.
pub fn markdown(qs: &[String]) -> String {
    if qs.is_empty() {
        return String::new();
    }
    let mut s = String::from("### Out of scope\n");
    for q in qs {
        let e = common::scope_ledger::default_of(q);
        s.push_str(&format!("- {}: {}\n", e.item, e.why));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collects_questions_from_the_plan_and_the_coder_once_each() {
        let design = "## Approach\nx\n\n## Open product questions\n- Should members see the block, or everyone? Meanwhile: everyone, as today.\n- none\n\n## Reuse\n- a/b.ts\n";
        let notes = "Done.\nOpen product question: Should members see the block, or everyone? Meanwhile: everyone, as today.\n- Open product question: Does the filter remember the last marina?\n";
        let q = collect(design, notes);
        assert_eq!(q.len(), 2);
        assert!(q[1].starts_with("Does the filter"));
        let md = markdown(&q);
        // Decided with the default, never asked.
        assert!(md.starts_with("### Out of scope"), "{md}");
        assert!(
            md.contains(
                "Members see the block, or everyone: decided with the default: everyone, as today"
            ),
            "{md}"
        );
        assert!(md.contains("The filter remember the last marina: decided with the default: today's behaviour stays"), "{md}");
        assert!(!md.contains('?'));
        assert!(!md.to_lowercase().contains("please answer"));
        assert!(markdown(&[]).is_empty());
    }
}
