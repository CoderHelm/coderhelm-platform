//! Line hunks of one file's change, and the file with only some of them
//! applied. The scope check names hunks by id; the ones a review thread did
//! not ask for are taken back out by rebuilding the file from its original
//! with only the kept hunks, so nothing else in the file moves.

/// Above this many line pairs the middle of a change is one hunk (the
/// quadratic table would be too large). Files a review fix touches are far
/// smaller.
const MAX_TABLE: usize = 4_000_000;

/// One contiguous change: old lines `old_start..old_end` (0-based, end
/// exclusive) become `new_lines`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hunk {
    pub path: String,
    pub old_start: usize,
    pub old_end: usize,
    pub new_start: usize,
    /// The removed lines, each with its line ending.
    pub removed: Vec<String>,
    /// The added lines, each with its line ending.
    pub added: Vec<String>,
    /// Up to three unchanged lines before and after, for the reader.
    pub before: Vec<String>,
    pub after: Vec<String>,
}

impl Hunk {
    /// The hunk as a unified diff block, for the model and the evidence.
    pub fn render(&self) -> String {
        let mut out = format!(
            "@@ {} -{},{} +{},{} @@\n",
            self.path,
            self.old_start + 1,
            self.removed.len(),
            self.new_start + 1,
            self.added.len()
        );
        let line = |out: &mut String, mark: char, l: &str| {
            out.push(mark);
            out.push_str(l.trim_end_matches(['\n', '\r']));
            out.push('\n');
        };
        for l in &self.before {
            line(&mut out, ' ', l);
        }
        for l in &self.removed {
            line(&mut out, '-', l);
        }
        for l in &self.added {
            line(&mut out, '+', l);
        }
        for l in &self.after {
            line(&mut out, ' ', l);
        }
        out
    }

    /// The added lines without their endings.
    pub fn added_text(&self) -> Vec<&str> {
        self.added
            .iter()
            .map(|l| l.trim_end_matches(['\n', '\r']))
            .collect()
    }

    /// The removed lines without their endings.
    pub fn removed_text(&self) -> Vec<&str> {
        self.removed
            .iter()
            .map(|l| l.trim_end_matches(['\n', '\r']))
            .collect()
    }
}

/// Lines of `text`, each with its own line ending (the last may have none).
fn lines(text: &str) -> Vec<&str> {
    text.split_inclusive('\n').collect()
}

/// The hunks that turn `old` into `new`. A created file (`old` None) or a
/// removed one (`new` None) is one hunk.
pub fn file_hunks(path: &str, old: Option<&str>, new: Option<&str>) -> Vec<Hunk> {
    let a = lines(old.unwrap_or(""));
    let b = lines(new.unwrap_or(""));
    if old.is_none() || new.is_none() {
        if a.is_empty() && b.is_empty() {
            return Vec::new();
        }
        return vec![Hunk {
            path: path.to_string(),
            old_start: 0,
            old_end: a.len(),
            new_start: 0,
            removed: a.iter().map(|s| s.to_string()).collect(),
            added: b.iter().map(|s| s.to_string()).collect(),
            before: Vec::new(),
            after: Vec::new(),
        }];
    }
    // Common prefix and suffix first: the table only covers the middle.
    let mut pre = 0;
    while pre < a.len() && pre < b.len() && a[pre] == b[pre] {
        pre += 1;
    }
    let mut suf = 0;
    while suf < a.len() - pre && suf < b.len() - pre && a[a.len() - 1 - suf] == b[b.len() - 1 - suf]
    {
        suf += 1;
    }
    let am = &a[pre..a.len() - suf];
    let bm = &b[pre..b.len() - suf];
    // (old index, new index) pairs of the middle that stay.
    let mut keep: Vec<(usize, usize)> = Vec::new();
    if !am.is_empty() && !bm.is_empty() && am.len().saturating_mul(bm.len()) <= MAX_TABLE {
        let (n, m) = (am.len(), bm.len());
        let mut lcs = vec![vec![0u32; m + 1]; n + 1];
        for i in (0..n).rev() {
            for j in (0..m).rev() {
                lcs[i][j] = if am[i] == bm[j] {
                    lcs[i + 1][j + 1] + 1
                } else {
                    lcs[i + 1][j].max(lcs[i][j + 1])
                };
            }
        }
        let (mut i, mut j) = (0, 0);
        while i < n && j < m {
            if am[i] == bm[j] {
                keep.push((pre + i, pre + j));
                i += 1;
                j += 1;
            } else if lcs[i + 1][j] >= lcs[i][j + 1] {
                i += 1;
            } else {
                j += 1;
            }
        }
    }
    // Walk the kept pairs; every gap between them is one hunk.
    let mut hunks = Vec::new();
    let (mut i, mut j) = (pre, pre);
    let ends = keep
        .into_iter()
        .chain(std::iter::once((a.len() - suf, b.len() - suf)));
    for (ki, kj) in ends {
        if ki > i || kj > j {
            hunks.push(Hunk {
                path: path.to_string(),
                old_start: i,
                old_end: ki,
                new_start: j,
                removed: a[i..ki].iter().map(|s| s.to_string()).collect(),
                added: b[j..kj].iter().map(|s| s.to_string()).collect(),
                before: a[i.saturating_sub(3)..i]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                after: a[ki..(ki + 3).min(a.len())]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            });
        }
        i = ki + 1;
        j = kj + 1;
    }
    hunks
}

/// `old` with only `kept` of its hunks applied (hunks from [`file_hunks`]
/// of the same old text). All kept gives the new text back; none gives the
/// old text.
pub fn apply(old: &str, kept: &[&Hunk]) -> String {
    let a = lines(old);
    let mut sorted: Vec<&&Hunk> = kept.iter().collect();
    sorted.sort_by_key(|h| h.old_start);
    let mut out = String::with_capacity(old.len());
    let mut at = 0;
    for h in sorted {
        if h.old_start < at {
            continue;
        }
        for l in &a[at..h.old_start.min(a.len())] {
            out.push_str(l);
        }
        for l in &h.added {
            out.push_str(l);
        }
        at = h.old_end;
    }
    for l in a.iter().skip(at) {
        out.push_str(l);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_separate_changes_are_two_hunks_and_each_can_be_taken_back() {
        let old = "a\nb\nc\nd\ne\nf\ng\nh\n";
        let new = "a\nB\nc\nd\ne\nf\nG\nh\n";
        let h = file_hunks("f.py", Some(old), Some(new));
        assert_eq!(h.len(), 2);
        assert_eq!(h[0].removed_text(), vec!["b"]);
        assert_eq!(h[0].added_text(), vec!["B"]);
        assert_eq!(h[1].added_text(), vec!["G"]);
        // Everything kept is the new file; nothing kept is the old one.
        assert_eq!(apply(old, &[&h[0], &h[1]]), new);
        assert_eq!(apply(old, &[]), old);
        // Only the first: the neighbour stays as it was.
        assert_eq!(apply(old, &[&h[0]]), "a\nB\nc\nd\ne\nf\ng\nh\n");
        assert_eq!(apply(old, &[&h[1]]), "a\nb\nc\nd\ne\nf\nG\nh\n");
    }

    #[test]
    fn insertions_deletions_and_a_missing_final_newline_round_trip() {
        let old = "one\ntwo\nthree";
        let new = "zero\none\nthree\nfour";
        let h = file_hunks("x.rs", Some(old), Some(new));
        let all: Vec<&Hunk> = h.iter().collect();
        assert_eq!(apply(old, &all), new);
        assert_eq!(apply(old, &[]), old);
        for one in &h {
            // Any single hunk applies cleanly on its own.
            let _ = apply(old, &[one]);
        }
    }

    #[test]
    fn a_created_or_removed_file_is_one_hunk() {
        let h = file_hunks("new.txt", None, Some("x\ny\n"));
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].added.len(), 2);
        let h = file_hunks("gone.txt", Some("x\n"), None);
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].removed.len(), 1);
        assert!(file_hunks("same", Some("a\n"), Some("a\n")).is_empty());
    }

    #[test]
    fn render_shows_context_and_marks() {
        let h = file_hunks("f", Some("a\nb\nc\n"), Some("a\nX\nc\n"));
        let r = h[0].render();
        assert!(r.starts_with("@@ f -2,1 +2,1 @@"));
        assert!(r.contains(" a\n-b\n+X\n c\n"));
    }
}
