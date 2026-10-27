//! Rule 8: open product questions, raised and never decided.
//!
//! A question that blocks the change stops the run (the plan's
//! `CLARIFICATION_NEEDED:`). A product decision the ticket doesn't settle,
//! that the change can go ahead without (who sees a new block, which
//! audience a rule applies to), is different: the plan lists it in
//! design.md under [`HEADING`], the coder may add one with a line starting
//! [`NOTE_MARK`], and the pull request shows them all under the same
//! heading as questions for the team, with the change keeping the current
//! behaviour rather than deciding them.

pub const HEADING: &str = "## Open product questions";
/// A coder's note line that raises one.
pub const NOTE_MARK: &str = "Open product question:";

/// What the planner is told.
pub const PLANNER: &str = "If you find a product decision the ticket doesn't settle and the \
     change can go ahead without it (for example, which audience a rule applies to), do NOT \
     decide it: list it in design.md under `## Open product questions`, one `- ` line each, \
     saying what the change does meanwhile (keep current behaviour). Only a question that stops \
     the change belongs in CLARIFICATION_NEEDED.";

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

/// The description's section. Empty without questions.
pub fn markdown(qs: &[String]) -> String {
    if qs.is_empty() {
        return String::new();
    }
    let mut s = format!(
        "{HEADING}\nThe ticket doesn't settle these; this pull request doesn't decide them and \
         keeps the current behaviour. Please answer on the pull request:\n"
    );
    for q in qs {
        s.push_str(&format!("- {q}\n"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collects_questions_from_the_plan_and_the_coder_once_each() {
        let design = "## Approach\nx\n\n## Open product questions\n- Should members see the block, or everyone? Meanwhile: everyone, as today.\n- none\n\n## Reuse\n- a/b.ts\n";
        let notes = "Done.\nOpen product question: Should members see the block, or everyone? Meanwhile: everyone, as today.\n- Open product question: Does the filter remember the last venue?\n";
        let q = collect(design, notes);
        assert_eq!(q.len(), 2);
        assert!(q[1].starts_with("Does the filter"));
        let md = markdown(&q);
        assert!(md.starts_with(HEADING));
        assert!(md.contains("doesn't decide them"));
        assert!(markdown(&[]).is_empty());
    }
}
