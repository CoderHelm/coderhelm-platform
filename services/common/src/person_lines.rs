//! A person's lines on a CoderHelm branch stay theirs.
//!
//! The failure this was written for: a person pushed a fixed end-to-end
//! spec to CoderHelm's pull request (30 of 30 passing on staging). The next
//! fix round read the spec, looked at an older version of it, and wrote the
//! assertions the person had taken out back in, in two commits. Nine of ten
//! tests broke. Nothing stopped it: the branch guards stop resets and force
//! pushes, and the snapshot check stops writes over commits the pass never
//! read, but this pass had read the person's commit. It simply disagreed
//! with it and wrote over it.
//!
//! Now every write CoderHelm commits to a branch is held against the
//! commits people made on it ([`PersonEdit`], read from each commit's
//! patch). A change that takes out a line a person added, or puts back a
//! line a person removed, is not committed ([`guard`]): that part of the
//! file keeps the person's version, the rest of the write goes through,
//! and the pull request gets one short note that says what CoderHelm
//! disagrees with and asks the person ([`note`]). The person's work wins
//! until they answer; a round a person asked for is their answer.
//!
//! Lines that carry no meaning on their own (`}`, `});`, blank lines) are
//! never counted: they appear everywhere and would block every edit.

use std::collections::HashSet;

/// A commit CoderHelm (or another app) made: its GitHub author is a bot
/// (an app's `name[bot]`), or, with no linked account, its author email is
/// a bot's noreply address. Every other commit is a person's.
pub fn is_bot_commit(c: &serde_json::Value) -> bool {
    let a = &c["author"];
    if a["type"].as_str() == Some("Bot")
        || a["login"].as_str().is_some_and(|l| l.ends_with("[bot]"))
    {
        return true;
    }
    a.is_null()
        && c["commit"]["author"]["email"]
            .as_str()
            .is_some_and(|e| e.ends_with("[bot]@users.noreply.github.com"))
}

/// The marker of the one note on a pull request about a person's lines
/// CoderHelm left alone; the note is edited in place.
pub const NOTE_MARKER: &str = "<!-- coderhelm:person-lines -->";

/// What one commit a person made did to one file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersonEdit {
    /// The commit.
    pub sha: String,
    /// Who made it (their login, else their name).
    pub author: String,
    pub path: String,
    /// The lines the commit added (without the `+`).
    pub added: Vec<String>,
    /// The lines the commit removed (without the `-`).
    pub removed: Vec<String>,
}

impl PersonEdit {
    /// The edit a commit's file entry describes, from its unified `patch`.
    pub fn from_patch(sha: &str, author: &str, path: &str, patch: &str) -> PersonEdit {
        let (added, removed) = patch_lines(patch);
        PersonEdit {
            sha: sha.to_string(),
            author: author.to_string(),
            path: path.to_string(),
            added,
            removed,
        }
    }
}

/// The added and removed lines of a unified diff hunk list (GitHub's
/// `files[].patch`): no file headers, no "no newline" markers.
pub fn patch_lines(patch: &str) -> (Vec<String>, Vec<String>) {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    for line in patch.lines() {
        if line.starts_with("+++") || line.starts_with("---") {
            continue;
        }
        if let Some(l) = line.strip_prefix('+') {
            added.push(l.to_string());
        } else if let Some(l) = line.strip_prefix('-') {
            removed.push(l.to_string());
        }
    }
    (added, removed)
}

/// Whether a line means something on its own: at least three letters or
/// digits once trimmed. Braces, blank lines and lone punctuation are in
/// every file and never mark whose a change is.
pub fn significant(line: &str) -> bool {
    line.trim().chars().filter(|c| c.is_alphanumeric()).count() >= 3
}

fn key(line: &str) -> String {
    line.trim().to_string()
}

/// What a write would have done to a person's line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Undo {
    /// Taken out a line the person added.
    RemovesTheirLine,
    /// Put back a line the person removed.
    RestoresWhatTheyRemoved,
    /// Deleted a file the person changed.
    DeletesTheirFile,
}

/// One part of a write that was not committed because it would undo a
/// person's change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kept {
    pub path: String,
    pub sha: String,
    pub author: String,
    pub undo: Undo,
    /// The person's line in question (trimmed).
    pub line: String,
}

impl Kept {
    /// One line on what was kept and why.
    pub fn sentence(&self) -> String {
        let short: String = self.sha.chars().take(7).collect();
        let line = crate::truncate_str(&self.line, 90);
        match self.undo {
            Undo::RemovesTheirLine => format!(
                "`{}`: CoderHelm's change would take out `{line}`, which {} added in {short}",
                self.path, self.author
            ),
            Undo::RestoresWhatTheyRemoved => format!(
                "`{}`: CoderHelm's change would put back `{line}`, which {} removed in {short}",
                self.path, self.author
            ),
            Undo::DeletesTheirFile => format!(
                "`{}`: CoderHelm's change would delete the file {} changed in {short}",
                self.path, self.author
            ),
        }
    }
}

/// A write held against the people's edits of its file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Guarded {
    /// What may be committed: the proposed content with every part that
    /// would undo a person's change put back as it is now.
    pub content: String,
    /// What was kept as the person had it.
    pub kept: Vec<Kept>,
}

/// One step of a line diff from `a` to `b`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Same(usize),
    Del(usize),
    Ins(usize),
}

/// Cells of the longest-common-subsequence table above which the middle of
/// a file is treated as one replaced block (a whole-file rewrite).
const MAX_CELLS: usize = 4_000_000;

/// The line diff from `a` to `b`: a shared start and end, and the longest
/// common subsequence of what lies between.
fn diff(a: &[&str], b: &[&str]) -> Vec<Op> {
    let mut pre = 0;
    while pre < a.len() && pre < b.len() && a[pre] == b[pre] {
        pre += 1;
    }
    let mut suf = 0;
    while suf < a.len() - pre && suf < b.len() - pre && a[a.len() - 1 - suf] == b[b.len() - 1 - suf]
    {
        suf += 1;
    }
    let (am, bm) = (&a[pre..a.len() - suf], &b[pre..b.len() - suf]);
    let mut ops: Vec<Op> = (0..pre).map(Op::Same).collect();
    let (n, m) = (am.len(), bm.len());
    if n.saturating_mul(m) > MAX_CELLS {
        ops.extend((0..n).map(|i| Op::Del(pre + i)));
        ops.extend((0..m).map(|j| Op::Ins(pre + j)));
    } else {
        // lcs[i][j]: the longest common subsequence of am[i..] and bm[j..].
        let w = m + 1;
        let mut lcs = vec![0u32; (n + 1) * w];
        for i in (0..n).rev() {
            for j in (0..m).rev() {
                lcs[i * w + j] = if am[i] == bm[j] {
                    lcs[(i + 1) * w + j + 1] + 1
                } else {
                    lcs[(i + 1) * w + j].max(lcs[i * w + j + 1])
                };
            }
        }
        let (mut i, mut j) = (0, 0);
        while i < n && j < m {
            if am[i] == bm[j] {
                ops.push(Op::Same(pre + i));
                i += 1;
                j += 1;
            } else if lcs[(i + 1) * w + j] >= lcs[i * w + j + 1] {
                ops.push(Op::Del(pre + i));
                i += 1;
            } else {
                ops.push(Op::Ins(pre + j));
                j += 1;
            }
        }
        ops.extend((i..n).map(|i| Op::Del(pre + i)));
        ops.extend((j..m).map(|j| Op::Ins(pre + j)));
    }
    ops.extend((0..suf).map(|k| Op::Same(a.len() - suf + k)));
    ops
}

/// The person's lines of `path`: what they added that is still meant to be
/// there, and what they removed.
struct Theirs<'a> {
    added: Vec<(String, &'a PersonEdit)>,
    removed: Vec<(String, &'a PersonEdit)>,
}

fn theirs<'a>(path: &str, edits: &'a [PersonEdit]) -> Theirs<'a> {
    let mut t = Theirs {
        added: Vec::new(),
        removed: Vec::new(),
    };
    for e in edits.iter().filter(|e| e.path == path) {
        let added: HashSet<String> = e.added.iter().map(|l| key(l)).collect();
        let removed: HashSet<String> = e.removed.iter().map(|l| key(l)).collect();
        for l in e.added.iter().filter(|l| significant(l)) {
            // A line moved within the commit is neither added nor removed.
            if !removed.contains(&key(l)) {
                t.added.push((key(l), e));
            }
        }
        for l in e.removed.iter().filter(|l| significant(l)) {
            if !added.contains(&key(l)) {
                t.removed.push((key(l), e));
            }
        }
    }
    t
}

/// Holds a write of `proposed` over `current` (what the branch has now;
/// `None` when the file doesn't exist) against the people's `edits`.
/// A line a person added that the change would take out stays, and a line
/// a person removed that the change would put back stays out; every other
/// line is the proposed content. A write over a file no person touched is unchanged.
pub fn guard(path: &str, current: Option<&str>, proposed: &str, edits: &[PersonEdit]) -> Guarded {
    let t = theirs(path, edits);
    if t.added.is_empty() && t.removed.is_empty() {
        return Guarded {
            content: proposed.to_string(),
            kept: Vec::new(),
        };
    }
    let cur = current.unwrap_or("");
    let a: Vec<&str> = cur.split_inclusive('\n').collect();
    let b: Vec<&str> = proposed.split_inclusive('\n').collect();
    // Compared without line endings and indentation, so a re-indent or a
    // missing final newline is not a different line.
    let ak: Vec<String> = a.iter().map(|l| key(l)).collect();
    let bk: Vec<String> = b.iter().map(|l| key(l)).collect();
    let ar: Vec<&str> = ak.iter().map(String::as_str).collect();
    let br: Vec<&str> = bk.iter().map(String::as_str).collect();
    let ops = diff(&ar, &br);

    let mut out = String::with_capacity(proposed.len());
    let mut kept: Vec<Kept> = Vec::new();
    let mut k = 0;
    while k < ops.len() {
        if let Op::Same(i) = ops[k] {
            // Same once trimmed: the proposed line keeps its new indentation
            // only when nothing around it was kept back; taking the
            // current one is always safe and changes no meaning.
            out.push_str(a[i]);
            k += 1;
            continue;
        }
        // One block of changes: every delete and insert up to the next
        // shared line, decided line by line in their order.
        let start = k;
        while k < ops.len() && !matches!(ops[k], Op::Same(_)) {
            k += 1;
        }
        let block = &ops[start..k];
        let inserted: HashSet<&str> = block
            .iter()
            .filter_map(|o| match o {
                Op::Ins(j) => Some(br[*j]),
                _ => None,
            })
            .collect();
        let deleted: HashSet<&str> = block
            .iter()
            .filter_map(|o| match o {
                Op::Del(i) => Some(ar[*i]),
                _ => None,
            })
            .collect();
        for op in block {
            match *op {
                Op::Del(i) => {
                    let line = ar[i];
                    let theirs = (!inserted.contains(line))
                        .then(|| t.added.iter().find(|(l, _)| l == line))
                        .flatten();
                    // Their line stays where it is.
                    if let Some((_, e)) = theirs {
                        out.push_str(a[i]);
                        kept.push(Kept {
                            path: path.to_string(),
                            sha: e.sha.clone(),
                            author: e.author.clone(),
                            undo: Undo::RemovesTheirLine,
                            line: line.to_string(),
                        });
                    }
                }
                Op::Ins(j) => {
                    let line = br[j];
                    let theirs = (!deleted.contains(line))
                        .then(|| t.removed.iter().find(|(l, _)| l == line))
                        .flatten();
                    match theirs {
                        // What they removed stays out.
                        Some((_, e)) => kept.push(Kept {
                            path: path.to_string(),
                            sha: e.sha.clone(),
                            author: e.author.clone(),
                            undo: Undo::RestoresWhatTheyRemoved,
                            line: line.to_string(),
                        }),
                        None => out.push_str(b[j]),
                    }
                }
                Op::Same(_) => {}
            }
        }
    }
    if kept.is_empty() {
        // Nothing kept back: the write is exactly what was proposed
        // (indentation and line endings included).
        return Guarded {
            content: proposed.to_string(),
            kept,
        };
    }
    Guarded { content: out, kept }
}

/// Holds deleting `path` (content `current`) against the people's edits:
/// the file stays when a line a person added is still in it.
pub fn guard_delete(path: &str, current: &str, edits: &[PersonEdit]) -> Option<Kept> {
    let t = theirs(path, edits);
    let now: HashSet<String> = current.lines().map(key).collect();
    t.added
        .iter()
        .find(|(l, _)| now.contains(l))
        .map(|(_, e)| Kept {
            path: path.to_string(),
            sha: e.sha.clone(),
            author: e.author.clone(),
            undo: Undo::DeletesTheirFile,
            line: String::new(),
        })
}

/// The most kept parts a note or refusal lists.
const MAX_LISTED: usize = 4;

fn listed(kept: &[Kept]) -> Vec<String> {
    let mut seen = HashSet::new();
    kept.iter()
        .filter(|k| seen.insert((k.path.clone(), k.sha.clone(), k.undo)))
        .take(MAX_LISTED)
        .map(Kept::sentence)
        .collect()
}

/// The pull request's note about the parts CoderHelm left as a person had
/// them: short, says what it disagrees with, and asks.
pub fn note(kept: &[Kept]) -> String {
    let mut who: Vec<&str> = kept.iter().map(|k| k.author.as_str()).collect();
    who.sort();
    who.dedup();
    let mut out = format!(
        "{NOTE_MARKER}\nCoderHelm kept {}'s version and committed none of these:\n",
        who.join(", ")
    );
    for l in listed(kept) {
        out.push_str(&format!("- {l}\n"));
    }
    out.push_str(
        "Reply here if CoderHelm's version should replace yours; until then your lines stay.",
    );
    out
}

/// What the coder reads when every part of its write was kept back: what
/// it would have undone, and what to do instead.
pub fn refusal(kept: &[Kept]) -> String {
    format!(
        "Not committed: this change would undo a person's commit on this branch. {}. \
         Their version stays. Make your change around their lines; if you think their change \
         is wrong, say why in your Evidence, and the pull request asks them.",
        listed(kept).join("; ")
    )
}

/// The parts of `kept` of one note, merged with what an earlier note
/// listed (`earlier`), newest last, without repeats.
pub fn merge(earlier: &[Kept], new: &[Kept]) -> Vec<Kept> {
    let mut out: Vec<Kept> = earlier.to_vec();
    for k in new {
        if !out.contains(k) {
            out.push(k.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edit(path: &str, patch: &str) -> PersonEdit {
        PersonEdit::from_patch("abc1234def", "dana", path, patch)
    }

    #[test]
    fn patch_lines_skip_headers() {
        let (a, r) = patch_lines("--- a/x\n+++ b/x\n@@ -1,2 +1,2 @@\n ctx\n-old line\n+new line\n");
        assert_eq!(a, vec!["new line"]);
        assert_eq!(r, vec!["old line"]);
    }

    #[test]
    fn a_file_no_person_touched_is_written_as_proposed() {
        let g = guard("a.ts", Some("one\ntwo\n"), "one\nthree\n", &[]);
        assert_eq!(g.content, "one\nthree\n");
        assert!(g.kept.is_empty());
    }

    #[test]
    fn putting_back_a_removed_assertion_is_kept_out_and_the_rest_goes_through() {
        // The person took a failing assertion out and fixed a selector.
        let person = edit(
            "e2e/tests/signup.spec.ts",
            "@@ -3,4 +3,3 @@\n   await page.goto('/signup');\n-  await expect(page.getByRole('heading', { name: 'Price' })).toBeVisible();\n-  await page.getByText('Old label').click();\n+  await page.getByText('New label').click();\n",
        );
        let current = "test('signup', async ({ page }) => {\n  await page.goto('/signup');\n  await page.getByText('New label').click();\n});\n\nconst timeout = 1000;\n";
        // CoderHelm's round writes the assertion back and also changes an
        // unrelated constant.
        let proposed = "test('signup', async ({ page }) => {\n  await page.goto('/signup');\n  await expect(page.getByRole('heading', { name: 'Price' })).toBeVisible();\n  await page.getByText('New label').click();\n});\n\nconst timeout = 5000;\n";
        let g = guard(
            "e2e/tests/signup.spec.ts",
            Some(current),
            proposed,
            &[person],
        );
        assert!(!g.content.contains("name: 'Price'"), "{}", g.content);
        assert!(g.content.contains("const timeout = 5000;"));
        assert_eq!(g.kept.len(), 1);
        assert_eq!(g.kept[0].undo, Undo::RestoresWhatTheyRemoved);
    }

    #[test]
    fn taking_out_a_line_the_person_added_is_kept_out() {
        let person = edit(
            "src/a.ts",
            "@@ -1 +1,2 @@\n export const a = 1;\n+export const fixedByPerson = true;\n",
        );
        let current = "export const a = 1;\nexport const fixedByPerson = true;\n";
        let proposed = "export const a = 2;\n";
        let g = guard("src/a.ts", Some(current), proposed, &[person]);
        assert!(g.content.contains("fixedByPerson"));
        assert_eq!(g.kept[0].undo, Undo::RemovesTheirLine);
    }

    #[test]
    fn braces_and_blank_lines_never_count() {
        let person = edit("src/a.ts", "@@ -1 +1,3 @@\n+}\n+\n+});\n");
        let g = guard("src/a.ts", Some("x\n}\n\n});\n"), "x\n", &[person]);
        assert!(g.kept.is_empty());
        assert_eq!(g.content, "x\n");
    }

    #[test]
    fn a_reindent_of_their_line_is_not_an_undo() {
        let person = edit("src/a.ts", "@@ -1 +1 @@\n+  callThePersonsFix();\n");
        let g = guard(
            "src/a.ts",
            Some("  callThePersonsFix();\n"),
            "    callThePersonsFix();\n",
            &[person],
        );
        assert!(g.kept.is_empty());
    }

    #[test]
    fn deleting_a_file_with_their_lines_is_kept_out() {
        let person = edit("src/a.ts", "@@ -0,0 +1 @@\n+export const keepMe = 1;\n");
        assert!(guard_delete(
            "src/a.ts",
            "export const keepMe = 1;\n",
            std::slice::from_ref(&person)
        )
        .is_some());
        assert!(guard_delete("src/a.ts", "something else\n", &[person]).is_none());
    }

    #[test]
    fn the_note_is_short_and_asks() {
        let k = Kept {
            path: "e2e/x.spec.ts".into(),
            sha: "abcdef1234".into(),
            author: "dana".into(),
            undo: Undo::RestoresWhatTheyRemoved,
            line: "await expect(a).toBeVisible();".into(),
        };
        let n = note(&[k.clone(), k]);
        assert!(n.starts_with(NOTE_MARKER));
        assert_eq!(n.lines().count(), 4, "{n}");
        assert!(n.contains("abcdef1"));
        assert!(n.contains("Reply here"));
    }
}
