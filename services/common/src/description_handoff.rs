//! A round never hands the pull request's description to a person.
//!
//! A fix round corrected the code a review asked about, then ended its
//! account with "I can't edit the PR description from here" and listed the
//! corrections for a person to make. The description stayed wrong.
//!
//! The description is CoderHelm's: after every push it is rebuilt from the
//! branch (`worker::passes::description`). A round has no tool that writes
//! it and needs none, so the round is told that ([`ROUND_NOTE`]) and its
//! account is checked ([`handoffs`]): a sentence that says the description
//! can't be edited, or leaves an edit of it to a person, is taken out
//! ([`scrub`]) and the rebuild runs, whether or not the round pushed code.
//!
//! The check is narrow on purpose. It reads one sentence at a time and needs
//! both the description as the thing edited and either an inability or a
//! person asked to do it; a sentence that only mentions the description
//! passes.

use std::sync::OnceLock;

use regex::Regex;

/// What every fix round is told about the description.
pub const ROUND_NOTE: &str = "The pull request's description is kept by CoderHelm: right after \
    this round it is rebuilt from the branch as it stands (the versions the lockfile resolves, \
    the counts, the changes, the verification and the open items). You have no tool that writes \
    it and need none. When a finding says the description is wrong, fix what is wrong in the \
    code if anything is, and say in your account which statement was wrong; never write that \
    you cannot edit the description, and never leave an edit of it to a person.";

/// What replaces a handed-off edit in a round's account.
pub const REBUILT: &str =
    "CoderHelm rebuilds the pull request's description from the branch after this round.";

/// The description, as the object of an edit: `the PR description`, `the
/// table in the description`.
const OBJECT: &str = r"(?:(?:the|this|that|its|their|my|our|pr|pr's|prs|pull|request|request's|current|existing)\s+){0,5}(?:description|pr body|pull request body)\b";
const IN_OBJECT: &str = r"[^.;!?]{0,60}?\b(?:in|of|from) (?:(?:the|this|that|its|pr|pr's|pull|request|request's)\s+){0,4}(?:description|pr body|pull request body)\b";
const EDIT: &str = r"(?:edit|update|change|modify|rewrite|fix|correct|amend|touch|adjust|revise)";
const EDITED: &str =
    r"(?:edited|updated|changed|modified|rewritten|fixed|corrected|amended|adjusted|revised)";
const CANNOT: &str = r"(?:can't|cannot|can not|couldn't|could not|won't be able to|am unable to|was unable to|unable to|am not able to|not able to|have no way to|no way to|don't have (?:a |the |any )?(?:way|tool|tools|access|permission|permissions|ability) to|do not have (?:a |the |any )?(?:way|tool|tools|access|permission|permissions|ability) to|have no (?:tool|tools|access|permission|ability) to|lack (?:a |the )?(?:tool|tools|access|permission|ability) to)";
const PERSON: &str = r"(?:you|someone|somebody|a person|a human|a maintainer|the maintainer|the author|a reviewer|the reviewer|the team|a teammate)";

fn patterns() -> &'static [Regex] {
    static P: OnceLock<Vec<Regex>> = OnceLock::new();
    P.get_or_init(|| {
        [
            // "I can't edit the PR description from here."
            format!(r"(?i)\b{CANNOT}\s+(?:\w+\s+){{0,2}}?{EDIT}\s+(?:{OBJECT}|{IN_OBJECT})"),
            // "The description can't be changed from this round."
            format!(r"(?i)\b(?:description|pr body|pull request body)\b[^.;!?]{{0,40}}?\b(?:can't|cannot|can not|could not|couldn't) be {EDITED}"),
            // "You will need to update the description."
            format!(r"(?i)\b{PERSON}(?:'ll| will| would| should| must| may| can)?(?: need to| needs to| have to| has to| want to)?\s+(?:please\s+)?(?:manually\s+)?{EDIT}\s+(?:{OBJECT}|{IN_OBJECT})"),
            // "Please fix the count in the description."
            format!(r"(?i)\bplease\s+(?:manually\s+)?{EDIT}\s+(?:{OBJECT}|{IN_OBJECT})"),
            // "The description needs a manual update."
            r"(?i)\b(?:description|pr body|pull request body)\b[^.;!?]{0,60}?\b(?:needs?|requires?|will need|still needs?) (?:a |to be )?(?:manual|manually|hand|human)".to_string(),
            // "The description must be updated by hand."
            format!(r"(?i)\b(?:description|pr body|pull request body)\b[^.;!?]{{0,60}}?\b{EDITED} (?:by hand|manually|by a person|by someone|by a maintainer|by the author)"),
            // "I left the description corrections for you."
            format!(r"(?i)\b(?:left|leave|leaving)\b[^.;!?]{{0,50}}?\b(?:description|pr body|pull request body)\b[^.;!?]{{0,50}}?\b(?:for|to) {PERSON}"),
        ]
        .iter()
        .map(|p| Regex::new(p).expect("a fixed pattern"))
        .collect()
    })
}

fn hands_off(sentence: &str) -> bool {
    let s = sentence.replace('\u{2019}', "'");
    // An instruction not to ("never write that you cannot edit the
    // description") is the rule itself, not a handoff.
    let lower = s.to_ascii_lowercase();
    if lower.contains("never ") {
        return false;
    }
    patterns().iter().any(|p| p.is_match(&s))
}

/// Each line's sentences, as (line index, byte range in the line).
fn spans(text: &str) -> Vec<(usize, usize, usize)> {
    let mut out = vec![];
    for (i, line) in text.lines().enumerate() {
        for (a, b) in crate::prose::sentence_spans(line) {
            out.push((i, a, b));
        }
    }
    out
}

/// The sentences of a round's account that say the description can't be
/// edited or leave an edit of it to a person.
pub fn handoffs(text: &str) -> Vec<String> {
    let lines: Vec<&str> = text.lines().collect();
    spans(text)
        .into_iter()
        .map(|(i, a, b)| lines[i][a..b].trim())
        .filter(|s| hands_off(s))
        .map(String::from)
        .collect()
}

/// `text` without its [`handoffs`], [`REBUILT`] in place of the first.
/// `None` when it has none, so a caller rebuilds only when there is cause.
pub fn scrub(text: &str) -> Option<String> {
    let mut found = false;
    let mut out: Vec<String> = vec![];
    for line in text.lines() {
        let mut kept = String::new();
        let mut cut = false;
        for (a, b) in crate::prose::sentence_spans(line) {
            let piece = &line[a..b];
            if !hands_off(piece.trim()) {
                kept.push_str(piece);
                continue;
            }
            cut = true;
            if !found {
                // Keep the sentence's lead (indent, bullet) with the text
                // that replaces it.
                let lead: String = piece
                    .chars()
                    .take_while(|c| c.is_whitespace() || matches!(c, '-' | '*' | '>'))
                    .collect();
                kept.push_str(&lead);
                kept.push_str(REBUILT);
                found = true;
            }
        }
        if !cut {
            out.push(line.to_string());
        } else if !kept.trim().trim_matches(['-', '*', '>']).trim().is_empty() {
            out.push(kept);
        }
    }
    found.then(|| out.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_round_that_hands_the_description_to_a_person_is_caught() {
        for s in [
            "I can't edit the PR description from here.",
            "I cannot update the pull request description in this round, so it still lists the old versions.",
            "I don't have a tool to change the description.",
            "I was unable to fix the table in the PR description.",
            "The description can't be changed from this round.",
            "You will need to update the PR description: the table still says 4.0.4.",
            "Please correct the count in the description.",
            "The PR description needs a manual update.",
            "The description should be corrected by hand.",
            "I left the description corrections for the author.",
            "Someone should fix the headline in the PR body.",
            "I can\u{2019}t edit the PR description from here.",
        ] {
            assert_eq!(handoffs(s), vec![s.to_string()], "{s}");
        }
    }

    #[test]
    fn a_sentence_that_only_mentions_the_description_passes() {
        for s in [
            "Updated the description as asked.",
            "The description said five packages cross a major version; the manifest keeps each on its line.",
            "I can't reproduce the failure the description mentions.",
            "You should update the tests the ticket's description lists.",
            "The table in the description was wrong about glob-walk.",
            "Please review the override for path-match.",
            "I could not run the build in the sandbox.",
            "The description of the `retry` option in the README is unchanged.",
            "CoderHelm rebuilds the pull request's description from the branch after this round.",
            "No change to the meta description tag was needed.",
        ] {
            assert!(handoffs(s).is_empty(), "{s}: {:?}", handoffs(s));
        }
    }

    #[test]
    fn the_handoff_is_replaced_by_the_rebuild_once() {
        let account = "Kept glob-walk on the 2 line with a scoped override.\n\
                       - I can't edit the PR description from here. Please update the table in the description.\n\
                       - The lockfile was written by the sandbox.";
        let out = scrub(account).unwrap();
        assert_eq!(
            out,
            format!(
                "Kept glob-walk on the 2 line with a scoped override.\n- {REBUILT}\n- The lockfile was written by the sandbox."
            )
        );
        assert!(handoffs(&out).is_empty());
        assert_eq!(scrub("Kept glob-walk on the 2 line."), None);
        // The round's note itself is never read as a handoff.
        assert!(
            handoffs(ROUND_NOTE).is_empty(),
            "{:?}",
            handoffs(ROUND_NOTE)
        );
    }
}
