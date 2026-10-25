//! Splitting a document's text into passages for search.
//!
//! A passage never crosses a section (a PDF page, a heading's part, a
//! sheet), so its citation can name where it is. Inside a section, whole
//! paragraphs are packed up to [`TARGET_CHARS`]; a paragraph that is too long
//! on its own is split by lines, then by words. Each passage after the first
//! repeats the tail of the one before ([`OVERLAP_CHARS`]) so a sentence cut
//! at a boundary is still found whole. A table's header row is repeated on
//! every passage so each one reads on its own.

/// One part of a document: where it is and its text.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Section {
    /// Where it is ("Page 3", "Sheet: Prices", "Setup > Install"), or empty.
    pub loc: String,
    pub text: String,
    /// A table's header row, repeated at the top of each passage.
    pub header: Option<String>,
}

/// One passage.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    /// Its position in the document (0-based).
    pub ord: u32,
    pub loc: String,
    pub text: String,
}

/// Target passage size in characters (about 400 tokens).
pub const TARGET_CHARS: usize = 1_600;
/// Characters repeated from the end of the previous passage.
pub const OVERLAP_CHARS: usize = 200;

/// The passages of `sections`, in order.
pub fn chunk(sections: &[Section]) -> Vec<Chunk> {
    chunk_with(sections, TARGET_CHARS, OVERLAP_CHARS)
}

/// [`chunk`] with explicit sizes (tests).
pub fn chunk_with(sections: &[Section], target: usize, overlap: usize) -> Vec<Chunk> {
    let target = target.max(50);
    let overlap = overlap.min(target / 2);
    let mut out: Vec<Chunk> = Vec::new();
    for s in sections {
        let header = s.header.as_deref().map(str::trim).filter(|h| !h.is_empty());
        let room = target.saturating_sub(header.map_or(0, |h| h.chars().count() + 1));
        let room = room.max(target / 2);
        let mut pieces: Vec<String> = Vec::new();
        for para in paragraphs(&s.text) {
            if char_len(&para) <= room {
                pieces.push(para);
            } else {
                pieces.extend(split_long(&para, room));
            }
        }
        let mut current = String::new();
        let flush = |current: &mut String, out: &mut Vec<Chunk>| {
            let body = current.trim();
            if body.is_empty() {
                current.clear();
                return;
            }
            let text = match header {
                Some(h) if !body.starts_with(h) => format!("{h}\n{body}"),
                _ => body.to_string(),
            };
            out.push(Chunk {
                ord: out.len() as u32,
                loc: s.loc.clone(),
                text,
            });
            current.clear();
        };
        for p in pieces {
            let sep = if current.is_empty() { 0 } else { 2 };
            if !current.is_empty() && char_len(&current) + sep + char_len(&p) > room {
                let tail = tail_chars(&current, overlap);
                flush(&mut current, &mut out);
                if !tail.is_empty() && char_len(&tail) + 2 + char_len(&p) <= room {
                    current.push_str(&tail);
                }
            }
            if !current.is_empty() {
                current.push_str("\n\n");
            }
            current.push_str(&p);
        }
        flush(&mut current, &mut out);
    }
    out
}

fn char_len(s: &str) -> usize {
    s.chars().count()
}

/// Paragraphs: text between blank lines, each trimmed, empty ones dropped.
fn paragraphs(text: &str) -> Vec<String> {
    let text = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut out = Vec::new();
    let mut cur: Vec<&str> = Vec::new();
    for line in text.split('\n') {
        if line.trim().is_empty() {
            if !cur.is_empty() {
                out.push(cur.join("\n").trim().to_string());
                cur.clear();
            }
        } else {
            cur.push(line.trim_end());
        }
    }
    if !cur.is_empty() {
        out.push(cur.join("\n").trim().to_string());
    }
    out.retain(|p| !p.is_empty());
    out
}

/// A paragraph longer than `room`, as pieces no longer than `room`: whole
/// lines packed together, a too-long line split at word boundaries, and a
/// too-long word split at character boundaries.
fn split_long(para: &str, room: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let push_piece = |cur: &mut String, piece: &str, out: &mut Vec<String>| {
        let sep = if cur.is_empty() { 0 } else { 1 };
        if !cur.is_empty() && char_len(cur) + sep + char_len(piece) > room {
            out.push(std::mem::take(cur));
        }
        if !cur.is_empty() {
            cur.push('\n');
        }
        cur.push_str(piece);
    };
    for line in para.split('\n') {
        if char_len(line) <= room {
            push_piece(&mut cur, line, &mut out);
            continue;
        }
        // Words, packed to `room`.
        let mut words = String::new();
        for word in line.split_whitespace() {
            let pieces: Vec<String> = if char_len(word) > room {
                word.chars()
                    .collect::<Vec<_>>()
                    .chunks(room)
                    .map(|c| c.iter().collect())
                    .collect()
            } else {
                vec![word.to_string()]
            };
            for w in pieces {
                let sep = if words.is_empty() { 0 } else { 1 };
                if !words.is_empty() && char_len(&words) + sep + char_len(&w) > room {
                    push_piece(&mut cur, &std::mem::take(&mut words), &mut out);
                }
                if !words.is_empty() {
                    words.push(' ');
                }
                words.push_str(&w);
            }
        }
        if !words.is_empty() {
            push_piece(&mut cur, &words, &mut out);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// The last `n` characters of `s`, starting at a word boundary.
fn tail_chars(s: &str, n: usize) -> String {
    if n == 0 {
        return String::new();
    }
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= n {
        return String::new();
    }
    let start = chars.len() - n;
    let tail: String = chars[start..].iter().collect();
    match tail.find(char::is_whitespace) {
        Some(i) => tail[i..].trim().to_string(),
        None => tail.trim().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sec(loc: &str, text: &str) -> Section {
        Section {
            loc: loc.into(),
            text: text.into(),
            header: None,
        }
    }

    #[test]
    fn short_text_is_one_passage_with_its_place() {
        let c = chunk(&[sec("Page 1", "Refunds take five days.\n\nAsk support.")]);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].loc, "Page 1");
        assert_eq!(c[0].ord, 0);
        assert!(c[0].text.contains("five days") && c[0].text.contains("Ask support"));
    }

    #[test]
    fn empty_text_has_no_passages() {
        assert!(chunk(&[sec("Page 1", "  \n\n \t ")]).is_empty());
        assert!(chunk(&[]).is_empty());
    }

    #[test]
    fn passages_stay_under_the_target_and_never_cross_sections() {
        let para = "The deploy pipeline runs tests, builds images and rolls out. ".repeat(8);
        let text = (0..20)
            .map(|_| para.clone())
            .collect::<Vec<_>>()
            .join("\n\n");
        let c = chunk_with(
            &[sec("Page 1", &text), sec("Page 2", "Short page.")],
            600,
            100,
        );
        assert!(c.len() > 5);
        for p in &c {
            assert!(
                p.text.chars().count() <= 600,
                "{} chars",
                p.text.chars().count()
            );
        }
        assert_eq!(c.last().unwrap().loc, "Page 2");
        assert_eq!(c.last().unwrap().text, "Short page.");
        // Positions run in order.
        assert!(c.iter().enumerate().all(|(i, p)| p.ord as usize == i));
    }

    #[test]
    fn consecutive_passages_overlap() {
        let paras: Vec<String> = (0..12)
            .map(|i| {
                format!(
                    "Paragraph {i} explains step {i} of the release in plain words for readers."
                )
            })
            .collect();
        let c = chunk_with(&[sec("", &paras.join("\n\n"))], 300, 80);
        assert!(c.len() >= 3);
        for w in c.windows(2) {
            let prev_tail: String = w[0]
                .text
                .chars()
                .rev()
                .take(30)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            let probe = prev_tail.split_whitespace().last().unwrap();
            assert!(
                w[1].text.contains(probe),
                "no overlap between {:?} and {:?}",
                w[0].text,
                w[1].text
            );
        }
    }

    #[test]
    fn a_huge_line_or_word_is_split_safely() {
        let word = "é".repeat(2_500);
        let c = chunk_with(&[sec("", &word)], 400, 50);
        assert!(c.len() >= 6);
        assert!(c.iter().all(|p| p.text.chars().count() <= 400));
        let total: usize = c.iter().map(|p| p.text.chars().count()).sum();
        assert!(total >= 2_500);
    }

    #[test]
    fn table_headers_repeat_on_every_passage() {
        let rows: Vec<String> = (0..200)
            .map(|i| format!("sku-{i} | widget {i} | {i}.00"))
            .collect();
        let s = Section {
            loc: "Sheet: Prices".into(),
            text: rows.join("\n"),
            header: Some("sku | name | price".into()),
        };
        let c = chunk_with(&[s], 500, 0);
        assert!(c.len() > 3);
        for p in &c {
            assert!(p.text.starts_with("sku | name | price\n"), "{}", p.text);
            assert!(p.text.chars().count() <= 500);
            assert_eq!(p.loc, "Sheet: Prices");
        }
    }
}
