//! The files a review fix round changes, held until the round is checked.
//! Every write the agent makes lands here instead of on the branch, so the
//! round can be judged (scope, logs, the full test suite) and goes out as
//! one commit in one push, or not at all.

use std::collections::BTreeMap;
use std::sync::Mutex;

use super::hunks::{self, Hunk};

/// One staged file: what the branch held when the round first touched it
/// (None: it did not exist) and what it holds now (None: removed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub original: Option<String>,
    pub current: Option<String>,
}

impl Change {
    /// Whether the file differs from the branch.
    pub fn changed(&self) -> bool {
        self.original != self.current
    }
}

/// A hunk with its id across the whole round, as the scope check names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumberedHunk {
    pub id: usize,
    pub hunk: Hunk,
}

#[derive(Debug, Default)]
pub struct Staged {
    files: Mutex<BTreeMap<String, Change>>,
    /// The commit messages the agent gave, in order, for the one commit.
    messages: Mutex<Vec<String>>,
}

impl Staged {
    pub fn new() -> Self {
        Self::default()
    }

    /// The staged state of `path`: `Some(Some(text))` written,
    /// `Some(None)` removed, `None` untouched this round.
    pub fn read(&self, path: &str) -> Option<Option<String>> {
        self.files.lock().ok()?.get(path).map(|c| c.current.clone())
    }

    /// Stages `content` for `path`. `original` is what the branch holds,
    /// used only the first time the round touches the file.
    pub fn write(&self, path: &str, original: Option<String>, content: Option<String>) {
        if let Ok(mut f) = self.files.lock() {
            f.entry(path.to_string())
                .and_modify(|c| c.current = content.clone())
                .or_insert(Change {
                    original,
                    current: content,
                });
        }
    }

    /// Records a commit message the agent gave.
    pub fn note_message(&self, message: &str) {
        let m = message.trim();
        if m.is_empty() {
            return;
        }
        if let Ok(mut v) = self.messages.lock() {
            if !v.iter().any(|x| x == m) {
                v.push(m.to_string());
            }
        }
    }

    pub fn messages(&self) -> Vec<String> {
        self.messages.lock().map(|v| v.clone()).unwrap_or_default()
    }

    /// Every file that differs from the branch, by path.
    pub fn changes(&self) -> Vec<(String, Change)> {
        self.files
            .lock()
            .map(|f| {
                f.iter()
                    .filter(|(_, c)| c.changed())
                    .map(|(p, c)| (p.clone(), c.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.changes().is_empty()
    }

    /// Every hunk of the round, numbered from 1 in path order.
    pub fn hunks(&self) -> Vec<NumberedHunk> {
        let mut out = Vec::new();
        for (path, c) in self.changes() {
            for h in hunks::file_hunks(&path, c.original.as_deref(), c.current.as_deref()) {
                out.push(NumberedHunk {
                    id: out.len() + 1,
                    hunk: h,
                });
            }
        }
        out
    }

    /// Takes the hunks with these ids back out: each file is rebuilt from
    /// its original with only its other hunks. Returns the paths changed.
    pub fn revert(&self, ids: &[usize]) -> Vec<String> {
        if ids.is_empty() {
            return Vec::new();
        }
        let all = self.hunks();
        let mut touched = Vec::new();
        let Ok(mut files) = self.files.lock() else {
            return touched;
        };
        let paths: std::collections::BTreeSet<&str> = all
            .iter()
            .filter(|h| ids.contains(&h.id))
            .map(|h| h.hunk.path.as_str())
            .collect();
        for path in paths {
            let Some(c) = files.get_mut(path) else {
                continue;
            };
            let kept: Vec<&Hunk> = all
                .iter()
                .filter(|h| h.hunk.path == path && !ids.contains(&h.id))
                .map(|h| &h.hunk)
                .collect();
            c.current = match (&c.original, &c.current) {
                // A created or removed file is one hunk: back to how it was.
                (None, _) | (_, None) => {
                    if kept.is_empty() {
                        c.original.clone()
                    } else {
                        c.current.clone()
                    }
                }
                (Some(old), Some(_)) => Some(hunks::apply(old, &kept)),
            };
            touched.push(path.to_string());
        }
        touched
    }

    /// Drops every staged change to `path`.
    pub fn restore(&self, path: &str) {
        if let Ok(mut f) = self.files.lock() {
            if let Some(c) = f.get_mut(path) {
                c.current = c.original.clone();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_writes_read_back_and_keep_the_first_original() {
        let s = Staged::new();
        assert!(s.read("a.py").is_none());
        s.write("a.py", Some("x\n".into()), Some("y\n".into()));
        // A second write keeps the branch's original.
        s.write("a.py", Some("y\n".into()), Some("z\n".into()));
        assert_eq!(s.read("a.py"), Some(Some("z\n".into())));
        let c = s.changes();
        assert_eq!(c[0].1.original.as_deref(), Some("x\n"));
        // Writing the original back is no change at all.
        s.write("a.py", None, Some("x\n".into()));
        assert!(s.is_empty());
    }

    #[test]
    fn reverting_one_hunk_keeps_the_others_in_the_same_file() {
        let s = Staged::new();
        let old = "def a():\n    return 1\n\n\n\n\ndef b():\n    return 2\n";
        let new = "def a():\n    return 10\n\n\n\n\ndef b():\n    return 20\n";
        s.write("m.py", Some(old.into()), Some(new.into()));
        let h = s.hunks();
        assert_eq!(h.len(), 2);
        assert_eq!(s.revert(&[2]), vec!["m.py".to_string()]);
        assert_eq!(
            s.read("m.py"),
            Some(Some(
                "def a():\n    return 10\n\n\n\n\ndef b():\n    return 2\n".into()
            ))
        );
        assert_eq!(s.hunks().len(), 1);
    }

    #[test]
    fn reverting_a_created_file_drops_it_and_ops_follow_the_state() {
        let s = Staged::new();
        s.write("new.txt", None, Some("hi\n".into()));
        s.write("old.txt", Some("bye\n".into()), None);
        assert_eq!(s.changes().len(), 2);
        let ids: Vec<usize> = s
            .hunks()
            .iter()
            .filter(|h| h.hunk.path == "new.txt")
            .map(|h| h.id)
            .collect();
        s.revert(&ids);
        let left = s.changes();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].0, "old.txt");
        assert!(left[0].1.current.is_none());
        s.restore("old.txt");
        assert!(s.is_empty());
    }

    #[test]
    fn messages_are_kept_once_in_order() {
        let s = Staged::new();
        s.note_message("Lower the log level");
        s.note_message("  ");
        s.note_message("Lower the log level");
        s.note_message("Add a test");
        assert_eq!(s.messages(), vec!["Lower the log level", "Add a test"]);
    }
}
