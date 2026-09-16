//! Parse what a person asked CoderHelm to do in a GitHub comment.
//!
//! One parser for every comment surface (PR conversation, inline review
//! threads, edits), so "@CoderHelm re review" means the same thing everywhere.

/// What a comment asks CoderHelm to do.
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    /// Re-run the review against the PR's current head.
    Rereview,
    /// Anything else addressed to CoderHelm (a question or an instruction),
    /// with the address token removed.
    Ask(String),
}

/// Parse a comment. None when the comment does not address CoderHelm.
///
/// Quoted lines (`> …`), fenced code blocks and inline code are ignored, so
/// quoting CoderHelm's own footer ("Reply @coderhelm re-review …") or pasting
/// a log that mentions it is not a command. The mention is case-insensitive
/// and also accepts `@coderhelm[bot]`; a line starting with `/coderhelm` is a
/// slash command.
pub fn parse(body: &str) -> Option<Command> {
    let text = strip_quotes_and_code(body);
    let lower = text.to_lowercase();
    let addressed = find_mentions(&lower).next().is_some()
        || lower.lines().any(|l| is_slash_command(l.trim_start()));
    if !addressed {
        return None;
    }
    let cleaned = remove_address_tokens(&text);
    // The command is what follows the address; text before it is context.
    let command_text = text_after_address(&text).unwrap_or_else(|| cleaned.clone());
    Some(match classify(&command_text) {
        Command::Rereview => Command::Rereview,
        Command::Ask(_) => Command::Ask(cleaned.trim().to_string()),
    })
}

/// The text after the first address token (mention or slash command), with
/// any further address tokens removed. None when nothing follows it or the
/// offsets cannot be mapped (non-ASCII case folding).
fn text_after_address(text: &str) -> Option<String> {
    let lower = text.to_lowercase();
    if lower.len() != text.len() {
        return None;
    }
    let mention = find_mentions(&lower).next().map(|(i, len)| i + len);
    let slash = {
        let mut offset = 0;
        let mut found = None;
        for line in lower.split_inclusive('\n') {
            let lead = line.len() - line.trim_start().len();
            if is_slash_command(&line[lead..]) {
                found = Some(offset + lead + "/coderhelm".len());
                break;
            }
            offset += line.len();
        }
        found
    };
    let start = match (mention, slash) {
        (Some(a), Some(b)) => a.min(b),
        (a, b) => a.or(b)?,
    };
    let after = remove_address_tokens(&text[start..]);
    (!after.trim().is_empty()).then_some(after)
}

/// Pure: a re-review request, or something else addressed to CoderHelm.
fn classify(cleaned: &str) -> Command {
    let norm = cleaned
        .trim()
        .trim_matches(|c: char| c.is_ascii_punctuation() && c != '?' || c.is_whitespace())
        .to_lowercase();
    if norm.is_empty() {
        return Command::Rereview;
    }
    // Politeness before the verb doesn't change the ask.
    let mut rest = norm.as_str();
    for lead in ["please ", "pls ", "can you ", "could you ", "kindly "] {
        if let Some(r) = rest.strip_prefix(lead) {
            rest = r.trim_start();
        }
    }
    const VERBS: [&str; 6] = [
        "re-review",
        "rereview",
        "re review",
        "review again",
        "re-check",
        "review",
    ];
    let is_verb = VERBS.iter().any(|v| {
        rest.strip_prefix(v).is_some_and(|after| {
            // Whole word, and not a question ("review why X is flagged?").
            (after.is_empty() || after.starts_with(|c: char| !c.is_alphanumeric()))
                && !after.contains('?')
                && after.split_whitespace().count() <= 4
        })
    });
    if is_verb {
        Command::Rereview
    } else {
        Command::Ask(cleaned.trim().to_string())
    }
}

fn is_slash_command(line: &str) -> bool {
    line.strip_prefix("/coderhelm")
        .is_some_and(|after| after.is_empty() || !after.starts_with(is_handle_char))
}

fn is_handle_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

/// Byte offsets and lengths of `@coderhelm` / `@coderhelm[bot]` mentions in an
/// already-lowercased string, whole-token only.
fn find_mentions(lower: &str) -> impl Iterator<Item = (usize, usize)> + '_ {
    lower.match_indices("@coderhelm").filter_map(move |(i, m)| {
        let before_ok = lower[..i]
            .chars()
            .next_back()
            .is_none_or(|c| !is_handle_char(c) && c != '@');
        let after = &lower[i + m.len()..];
        let len = if after.starts_with("[bot]") {
            m.len() + "[bot]".len()
        } else {
            m.len()
        };
        let after_ok = lower[i + len..]
            .chars()
            .next()
            .is_none_or(|c| !is_handle_char(c));
        (before_ok && after_ok).then_some((i, len))
    })
}

/// Remove the address tokens, keeping the person's actual words.
fn remove_address_tokens(text: &str) -> String {
    let lower = text.to_lowercase();
    // Lowercasing ASCII keeps byte offsets; bail out to the plain text if a
    // non-ASCII character changed length.
    if lower.len() != text.len() {
        return text.replace("@coderhelm", " ").replace("/coderhelm", " ");
    }
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    for (i, len) in find_mentions(&lower).collect::<Vec<_>>() {
        out.push_str(&text[last..i]);
        out.push(' ');
        last = i + len;
    }
    out.push_str(&text[last..]);
    out.lines()
        .map(|l| {
            let t = l.trim_start();
            if is_slash_command(&t.to_lowercase()) {
                t["/coderhelm".len()..].to_string()
            } else {
                l.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Drop quoted lines, fenced code blocks and inline code spans.
fn strip_quotes_and_code(body: &str) -> String {
    let mut out = Vec::new();
    let mut in_fence = false;
    for line in body.lines() {
        let t = line.trim_start();
        if t.starts_with("```") || t.starts_with("~~~") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence || t.starts_with('>') {
            continue;
        }
        // Inline code: drop everything between paired backticks.
        let mut kept = String::with_capacity(line.len());
        let mut in_code = false;
        for c in line.chars() {
            if c == '`' {
                in_code = !in_code;
            } else if !in_code {
                kept.push(c);
            }
        }
        out.push(kept);
    }
    out.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(s: &str) -> Option<Command> {
        Some(Command::Ask(s.to_string()))
    }

    #[test]
    fn rereview_spellings_and_casing() {
        for body in [
            "@coderhelm re-review",
            "@coderhelm rereview",
            "@coderhelm re review",
            "@CoderHelm re review",
            "@coderhelm[bot] review again",
            "@coderhelm review",
            "@coderhelm please re-review this",
            "@coderhelm",
            "/coderhelm re-review",
            "Pushed the fix. @CoderHelm re-review!",
        ] {
            assert_eq!(parse(body), Some(Command::Rereview), "{body}");
        }
    }

    #[test]
    fn questions_and_instructions_are_asks() {
        assert_eq!(
            parse("@coderhelm why is the retry flagged?"),
            ask("why is the retry flagged?")
        );
        assert_eq!(
            parse("@coderhelm review why X is flagged?"),
            ask("review why X is flagged?")
        );
        assert_eq!(
            parse("@coderhelm please record this as a standing constraint on this repo"),
            ask("please record this as a standing constraint on this repo")
        );
    }

    #[test]
    fn not_addressed() {
        assert_eq!(parse("LGTM, merging"), None);
        assert_eq!(parse("ping @coderhelmfan about this"), None);
        assert_eq!(parse("email me at x@coderhelm.com"), None);
        assert_eq!(parse("run /coderhelmish later"), None);
    }

    #[test]
    fn quoted_footer_and_code_are_not_commands() {
        let quoted = "> Reply **@coderhelm re-review** to re-run\n\nthanks, looks good";
        assert_eq!(parse(quoted), None);
        let quoted_with_question =
            "> Reply **@coderhelm re-review** to re-run\n\n@coderhelm why did you flag line 4?";
        assert_eq!(parse(quoted_with_question), ask("why did you flag line 4?"));
        assert_eq!(parse("the log said `@coderhelm re-review` twice"), None);
        assert_eq!(parse("```\n@coderhelm re-review\n```"), None);
    }
}
