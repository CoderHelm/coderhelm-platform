//! Text a person reads on a pull request is never cut mid-sentence: every cap
//! on posted text ends on a sentence boundary, and a sentence too long for a
//! cap is kept whole or dropped with a note, never split.

/// Words whose full stop doesn't end a sentence.
const ABBREVIATIONS: &[&str] = &[
    "e.g", "i.e", "etc", "vs", "cf", "no", "approx", "mr", "ms", "dr",
];

/// The byte offsets just past each sentence end in `text`. A terminator ends
/// a sentence only when whitespace or the end of the text follows it, so the
/// dot in `src/lib.rs`, `v1.2.3` or `example.com` doesn't.
pub fn sentence_ends(text: &str) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if !matches!(c, '.' | '!' | '?') {
            continue;
        }
        // Closing marks stay with their sentence: `foo.rs.`, (done.), "yes."
        let mut end = i + c.len_utf8();
        while let Some(&(j, n)) = chars.peek() {
            if matches!(
                n,
                '.' | '!' | '?' | ')' | ']' | '"' | '\'' | '`' | '*' | '_'
            ) {
                end = j + n.len_utf8();
                chars.next();
            } else {
                break;
            }
        }
        let at_break = match chars.peek() {
            None => true,
            Some(&(_, n)) => n.is_whitespace(),
        };
        if !at_break {
            continue;
        }
        if c == '.' {
            let word = text[..i]
                .rsplit(|ch: char| ch.is_whitespace() || ch == '(')
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            if ABBREVIATIONS.contains(&word.as_str()) {
                continue;
            }
        }
        ends.push(end);
    }
    ends
}

/// The sentences of `text`, each with its byte range: every stretch up to a
/// sentence end ([`sentence_ends`]), and what is left after the last one.
/// Leading whitespace belongs to the sentence it precedes.
pub fn sentence_spans(text: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0;
    for end in sentence_ends(text) {
        if !text[start..end].trim().is_empty() {
            out.push((start, end));
        }
        start = end;
    }
    if !text[start..].trim().is_empty() {
        out.push((start, text.len()));
    }
    out
}

/// At most `max_sentences` whole sentences and about `max_bytes`. A first
/// sentence longer than `max_bytes` is kept whole: the cap never splits it.
/// Text with no sentence end is returned as it is.
pub fn first_sentences(text: &str, max_sentences: usize, max_bytes: usize) -> &str {
    let ends = sentence_ends(text);
    let mut cut = None;
    for (n, &end) in ends.iter().enumerate() {
        if n >= max_sentences || (end > max_bytes && cut.is_some()) {
            break;
        }
        cut = Some(end);
    }
    match cut {
        Some(end) => text[..end].trim_end(),
        None => text.trim_end(),
    }
}

/// `text` within `max_bytes`, cut at the last sentence or paragraph end that
/// fits. `None` when it already fits. Empty when nothing whole fits.
fn cut_point(text: &str, max_bytes: usize) -> Option<usize> {
    if text.len() <= max_bytes {
        return None;
    }
    let sentence = sentence_ends(text)
        .into_iter()
        .take_while(|&e| e <= max_bytes)
        .last();
    let paragraph = text
        .match_indices("\n\n")
        .map(|(i, _)| i)
        .take_while(|&i| i <= max_bytes)
        .last();
    Some(sentence.max(paragraph).unwrap_or(0))
}

/// `text` within `max_bytes` on a sentence or paragraph boundary, followed by
/// `note` (on its own paragraph) when anything was left out.
pub fn cap_on_sentence(text: &str, max_bytes: usize, note: &str) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let end = cut_point(text, max_bytes.saturating_sub(note.len() + 2)).unwrap_or(0);
    let kept = text[..end].trim_end();
    if kept.is_empty() {
        note.to_string()
    } else {
        format!("{kept}\n\n{note}")
    }
}

/// A model's answer that stopped before it finished (its token limit) ends on
/// its last whole sentence. Only a last line of prose is judged: a list item,
/// a table row, a heading or a code block may end without a full stop.
pub fn end_on_sentence(text: &str) -> &str {
    let t = text.trim_end();
    let last = t.lines().last().unwrap_or("").trim_start();
    let structured = last.starts_with(['-', '*', '|', '#', '>', '`'])
        || last
            .split('.')
            .next()
            .is_some_and(|n| !n.is_empty() && n.len() < 4 && n.bytes().all(|b| b.is_ascii_digit()));
    if structured || t.ends_with("```") {
        return t;
    }
    let ends = sentence_ends(t);
    match ends.last() {
        Some(&e) if e == t.len() => t,
        Some(&e) => t[..e].trim_end(),
        None => t,
    }
}

/// The first whole sentence of a model's free text on one line, for showing
/// it on its own; `None` when it holds no finished sentence.
pub fn one_whole_sentence(text: &str) -> Option<String> {
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let end = *sentence_ends(&line).first()?;
    Some(line[..end].trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_dot_in_a_path_is_not_the_end_of_a_sentence() {
        let s = "The job reads services/worker/src/main.rs and lambda/handler.py from the source \
                 paths. It is fine.";
        assert_eq!(sentence_ends(s).len(), 2);
        assert_eq!(first_sentences(s, 1, 500), &s[..s.find(" It is").unwrap()]);
        assert_eq!(
            sentence_ends("Bumped to v1.2.3 on example.com now").len(),
            0
        );
        assert_eq!(sentence_ends("Use e.g. this one. Done.").len(), 2);
        assert_eq!(sentence_ends("See `a.rs`. Then (done.) Next!").len(), 3);
    }

    #[test]
    fn a_long_first_sentence_is_kept_whole() {
        let long = format!("{} end.", "word ".repeat(300));
        assert_eq!(first_sentences(&long, 4, 100), long);
        let two = format!("Short one. {long}");
        assert_eq!(first_sentences(&two, 4, 100), "Short one.");
        assert_eq!(first_sentences("no end here", 4, 5), "no end here");
    }

    #[test]
    fn a_cap_ends_on_a_boundary_and_says_so() {
        let text = "First sentence here. Second sentence here. Third sentence goes on and on.";
        let out = cap_on_sentence(text, 40, "(more)");
        assert_eq!(out, "First sentence here.\n\n(more)");
        assert_eq!(cap_on_sentence("short.", 60, "(more)"), "short.");
        let para = "para without an end\n\nmore without an end that is long enough to matter here";
        assert_eq!(
            cap_on_sentence(para, 40, "(more)"),
            "para without an end\n\n(more)"
        );
        assert_eq!(cap_on_sentence(&"x".repeat(100), 20, "(more)"), "(more)");
    }

    #[test]
    fn an_answer_cut_by_a_token_limit_ends_on_its_last_sentence() {
        assert_eq!(end_on_sentence("All good. The source paths"), "All good.");
        assert_eq!(end_on_sentence("All good.\n"), "All good.");
        assert_eq!(
            end_on_sentence("Items:\n- one\n- two"),
            "Items:\n- one\n- two"
        );
        assert_eq!(
            end_on_sentence("Steps:\n1. one\n2. two"),
            "Steps:\n1. one\n2. two"
        );
        assert_eq!(end_on_sentence("```\ncode\n```"), "```\ncode\n```");
        assert_eq!(end_on_sentence("no sentence at all"), "no sentence at all");
    }

    #[test]
    fn only_a_finished_sentence_is_shown_on_its_own() {
        assert_eq!(
            one_whole_sentence("The fact\nchanged. And more."),
            Some("The fact changed.".to_string())
        );
        assert_eq!(one_whole_sentence("because it"), None);
        assert_eq!(one_whole_sentence("  "), None);
    }
}
