//! CoderHelm finishes the work.
//!
//! CoderHelm is a coding agent, not an agent that does its best and hands
//! the code back. Every model that writes or fixes code is told so in the
//! same words ([`CODER`], [`PLANNER`]), and every sentence CoderHelm shows a
//! person says what it did, what it deferred and why, and what it does next.
//! A person is asked for something only when only a person has it: the
//! human-only list in `common::handback`.
//!
//! The test at the bottom reads the worker's sources and fails when a prompt
//! or a message shown to people hands work back with one of
//! [`HAND_OFF_PHRASES`], outside the few human-only places on its allowlist.

/// The human-only reasons, in the words the prompts use. A model may stop
/// for a person only for one of these.
pub const HUMAN_ONLY_WORDS: &str = "a credential or secret only a person can provide, access \
     CoderHelm doesn't have, a product decision the ticket leaves open, a ticket that reads two \
     ways that lead to different code, or an approval the team requires";

/// The stance of every model that writes or fixes code: the implementer, the
/// CI and test fixers, the review and feedback fixers, the log, alert and
/// security fixes. Goes at the top of their system prompt.
pub const CODER: &str = "You are the engineer who owns this change until it is done. CoderHelm \
     is a coding agent: the work is finished here, not handed back.\n\
     - Finish the task: every part of it, as working code.\n\
     - Verify it: run the checks you have (run_checks when it is offered, the build, the \
     tests) on what you changed and read their output.\n\
     - When a check fails, fix it. When you are stuck, change the approach: read more of the \
     code, trace the failure to its cause, try another way, make the change smaller.\n\
     - Defer only an item that is not core to the task, and name it with the reason.\n\
     - Never ask a person to do work you can do, and never end with \"please review\", \
     \"complete manually\", \"re-trigger\" or \"a person should\".\n\
     - Stop for a person only for what only a person has: ";

/// The planner's stance: plan the whole job so it can be finished.
pub const PLANNER: &str = "You plan work that CoderHelm, a coding agent, finishes itself: \
     plan every part of the job, including how it is verified, never a partial plan for a \
     person to complete. Ask a person only for what only a person has: ";

/// [`CODER`] with the human-only list, as it goes in a system prompt.
pub fn coder_stance() -> String {
    format!("{CODER}{HUMAN_ONLY_WORDS}. Then ask one specific question about it.\n\n")
}

/// [`PLANNER`] with the human-only list, as it goes in a system prompt.
pub fn planner_stance() -> String {
    format!("{PLANNER}{HUMAN_ONLY_WORDS}.\n\n")
}

/// Phrases that hand work back to a person. None may appear in a prompt or
/// a message CoderHelm writes, outside [`ALLOWED`].
pub const HAND_OFF_PHRASES: &[&str] = &[
    "please review",
    "please complete",
    "complete manually",
    "manually complete",
    "complete the remaining",
    "finish it manually",
    "finish manually",
    "finish the work by hand",
    "finish it by hand",
    "do it manually",
    "re-trigger",
    "retrigger",
    "human review",
    "for a human",
    "a person should",
    "needs a human",
    "someone should",
    "handed off",
    "handing it off",
    "hand it off",
    "over to you",
    "take it from here",
    "take over from here",
    "do your best",
    "best effort attempt",
    "if you can't, say so and stop",
    "if you cannot, say so and stop",
];

/// How a model's reply gives the work up, besides [`HAND_OFF_PHRASES`].
const GIVE_UP_PHRASES: &[&str] = &[
    "couldn't finish",
    "could not finish",
    "couldn't complete",
    "could not complete",
    "unable to finish",
    "unable to complete",
    "manually",
    "for you to finish",
    "for you to complete",
    "left for you",
    "you'll need to finish",
    "you will need to finish",
    "a person needs to",
    "someone needs to",
];

/// Whether a model's reply hands the work back to a person for a reason
/// that is not on the human-only list (`common::handback`). A reply that
/// names a credential, missing access, a product decision, an ambiguous
/// ticket or an approval is a stop the stop judge weighs; anything else is
/// no progress, and the work goes on.
pub fn hands_back_without_reason(reply: &str) -> bool {
    let t = reply.to_lowercase();
    let gives_up = HAND_OFF_PHRASES
        .iter()
        .chain(GIVE_UP_PHRASES)
        .any(|p| t.contains(p));
    gives_up && common::handback::human_only_in(reply).is_none()
}

/// What the coder is told when its reply hands the work back without a
/// human-only reason: change approach and keep going. `None` for any other
/// reply.
pub fn escalation_note(reply: &str) -> Option<String> {
    hands_back_without_reason(reply).then(|| {
        format!(
            "[SYSTEM NOTE] Your reply hands the work back to a person, and none of the reasons \
             that allows applies ({HUMAN_ONLY_WORDS}). CoderHelm finishes the work. Keep going \
             with a different approach: read the failing part and the code around it again, \
             trace the error to its cause, try another way, or make the change smaller, and \
             call run_checks when it is offered. Defer only an item that is not core to the \
             task, naming it with the reason. End with what you did and what you checked."
        )
    })
}

/// The places a phrase may stay, as (file under `src/`, text around the
/// phrase). Each is a human-only case or not a hand-off at all.
#[cfg(test)]
pub const ALLOWED: &[(&str, &str)] = &[
    // The list itself, and the stance that forbids the phrases.
    ("passes/ownership.rs", ""),
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                // The incident and replay suites quote old failure messages
                // on purpose, to prove they never come back.
                let name = p.file_name().unwrap().to_string_lossy().to_string();
                if name != "incidents" && name != "replay" {
                    sources(&p, out);
                }
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }

    /// The code of a file without its comments or test module, with Rust's
    /// string line continuations joined, lowercased: what a prompt or a
    /// message would say.
    pub(super) fn said(src: &str) -> String {
        let mut code = String::new();
        let lines: Vec<&str> = src.lines().collect();
        let mut i = 0;
        while i < lines.len() {
            let line = lines[i];
            let t = line.trim_start();
            // A test module, to its closing brace at the same indent
            // (rustfmt's layout): tests quote phrases to prove they're gone.
            if t == "#[cfg(test)]" {
                if let Some(next) = lines.get(i + 1) {
                    let n = next.trim_start();
                    if n.starts_with("mod ") && n.ends_with('{') {
                        let close = format!("{}}}", &next[..next.len() - n.len()]);
                        i += 2;
                        while i < lines.len() && lines[i] != close {
                            i += 1;
                        }
                        i += 1;
                        continue;
                    }
                }
            }
            i += 1;
            if t.starts_with("//") {
                continue;
            }
            code.push_str(line);
            code.push('\n');
        }
        let mut out = String::new();
        let mut chars = code.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' && chars.peek() == Some(&'\n') {
                chars.next();
                while chars.peek().is_some_and(|c| c.is_whitespace()) {
                    chars.next();
                }
                continue;
            }
            out.push(c);
        }
        out.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    }

    fn allowed(file: &str, around: &str) -> bool {
        ALLOWED
            .iter()
            .any(|(f, ctx)| *f == file && around.contains(&ctx.to_lowercase()))
    }

    /// Every hand-off phrase in the worker's prompts and messages, as
    /// `file: text around it`.
    fn hand_offs() -> Vec<String> {
        // The worker's sources, and the shared crate's (some prompts and
        // messages are built there), as `common/...`.
        let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let common = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../common/src");
        let mut files = Vec::new();
        sources(&worker, &mut files);
        let n_worker = files.len();
        sources(&common, &mut files);
        let mut found = Vec::new();
        for (i, f) in files.into_iter().enumerate() {
            let rel = if i < n_worker {
                f.strip_prefix(&worker)
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            } else {
                format!(
                    "common/{}",
                    f.strip_prefix(&common).unwrap().to_string_lossy()
                )
            }
            .replace('\\', "/");
            let text = said(&std::fs::read_to_string(&f).unwrap());
            for phrase in HAND_OFF_PHRASES {
                let mut from = 0;
                while let Some(i) = text[from..].find(phrase) {
                    let at = from + i;
                    let lo = text[..at]
                        .char_indices()
                        .rev()
                        .nth(60)
                        .map_or(0, |(i, _)| i);
                    let hi = text[at..]
                        .char_indices()
                        .nth(phrase.len() + 60)
                        .map_or(text.len(), |(i, _)| at + i);
                    let around = &text[lo..hi];
                    if !allowed(&rel, around) {
                        found.push(format!("{rel}: …{around}…"));
                    }
                    from = at + phrase.len();
                }
            }
        }
        found
    }

    #[test]
    fn no_prompt_or_message_hands_work_back() {
        let found = hand_offs();
        assert!(
            found.is_empty(),
            "CoderHelm finishes the work: say what was done, what was deferred and why, and \
             what happens next, or ask one question for a human-only reason \
             (`common::handback`). Hand-off phrases found:\n{}",
            found.join("\n")
        );
    }

    #[test]
    fn the_scan_sees_through_continuations_and_skips_comments() {
        let src = "// a person should look\nconst A: &str = \"CI is red, handed \\\n    off for human \\\n    review\";\n#[cfg(test)]\nmod tests {\n    const B: &str = \"over to you\";\n}\nconst C: &str = \"please complete it\";\n";
        let s = said(src);
        assert!(s.contains("handed off for human review"));
        assert!(!s.contains("a person should"));
        assert!(!s.contains("over to you"));
        // Code after an early test module is still read.
        assert!(s.contains("please complete it"));
    }

    #[test]
    fn every_allowed_place_still_exists() {
        // An allowlist entry for text that is gone would hide the next one.
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        for (file, ctx) in ALLOWED {
            let text = said(&std::fs::read_to_string(root.join(file)).unwrap());
            assert!(
                text.contains(&ctx.to_lowercase()),
                "stale allowlist entry: {file}: {ctx}"
            );
        }
    }

    #[test]
    fn a_reply_that_gives_up_without_a_listed_reason_keeps_going() {
        for reply in [
            "I couldn't finish, please complete manually.",
            "Tasks 3 and 4 are left for you to finish.",
            "The migration needs to be applied manually.",
            "I'm handing it off: a person should look at the failing test.",
        ] {
            assert!(hands_back_without_reason(reply), "{reply}");
            assert!(escalation_note(reply)
                .unwrap()
                .contains("different approach"));
        }
        // A human-only reason is the stop judge's to weigh, not a give-up.
        assert!(!hands_back_without_reason(
            "I couldn't finish: it needs the API key for the payments sandbox."
        ));
        // A finished reply is not a hand-back.
        assert!(escalation_note("Updated `src/a.rs`; run_checks passes.").is_none());
    }

    #[test]
    fn the_stance_names_the_human_only_reasons_and_no_escape_hatch() {
        let s = coder_stance();
        for w in [
            "credential",
            "access",
            "product decision",
            "reads two ways",
            "approval",
        ] {
            assert!(s.contains(w), "{w}");
        }
        assert!(s.contains("change the approach"));
        assert!(s.contains("run_checks"));
        assert!(planner_stance().contains("product decision"));
    }
}
