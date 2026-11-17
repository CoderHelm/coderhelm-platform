//! The lines a pull request carries about its own CI, true for its head.
//!
//! A pull request's description kept "CI is failing" (with a lint warning
//! quoted as the error) and "Deferred: reverted 7 file(s)" after the work
//! had come back on a later head and the checks were running again. Both
//! were written once, by the path that stopped fixing, and nothing ever
//! took them out.
//!
//! The rules:
//!
//! - The "CI is failing" section names the head it was written on
//!   ([`failing_section`]). On every build of the description
//!   ([`reconcile`]) it goes when the head moved, or when the head's checks
//!   show no failure.
//! - A revert's "Deferred" line names the green head it went back to and
//!   the files it put back ([`revert_line`]). It goes once any of those
//!   files differs from that head again: the work came back, so "reverted"
//!   is no longer true.

/// The "CI is failing" section's markers.
pub const FAILING_START: &str = "<!-- coderhelm:ci-failing";
pub const FAILING_END: &str = "<!-- /coderhelm:ci-failing -->";

/// The marker on a revert's line.
pub const REVERT_MARK: &str = "<!-- coderhelm:revert ";

/// The "CI is failing" section for `head`, around `inner` (its heading and
/// lines).
pub fn failing_section(head: &str, inner: &str) -> String {
    let head: String = head.chars().take(12).collect();
    format!(
        "{FAILING_START} head={head} -->\n{}\n{FAILING_END}",
        inner.trim()
    )
}

/// The head a "CI is failing" section in `body` was written on: `Some("")`
/// for a section written before it named one, `None` with no section.
pub fn failing_head(body: &str) -> Option<String> {
    let i = body.find(FAILING_START)?;
    let rest = &body[i + FAILING_START.len()..];
    let end = rest.find("-->").unwrap_or(0);
    let attrs = &rest[..end];
    Some(
        attrs
            .split_whitespace()
            .find_map(|a| a.strip_prefix("head="))
            .unwrap_or("")
            .to_string(),
    )
}

/// `body` without its "CI is failing" section.
pub fn without_failing(body: &str) -> String {
    let (Some(a), Some(b)) = (body.find(FAILING_START), body.find(FAILING_END)) else {
        return body.to_string();
    };
    if b < a {
        return body.to_string();
    }
    let mut out = body.to_string();
    out.replace_range(a..b + FAILING_END.len(), "");
    squeeze(&out)
}

fn squeeze(s: &str) -> String {
    let mut out = s.to_string();
    while out.contains("\n\n\n") {
        out = out.replace("\n\n\n", "\n\n");
    }
    out.trim().to_string()
}

/// A revert's line under "Deferred": the note, and a hidden mark with the
/// green head it went back to and the files it put back.
pub fn revert_line(note: &str, to: &str, paths: &[String]) -> String {
    let to: String = to.chars().take(40).collect();
    let paths: Vec<String> = paths
        .iter()
        .filter(|p| !p.contains(char::is_whitespace) && !p.contains("--"))
        .cloned()
        .collect();
    format!(
        "- {} {REVERT_MARK}to={to} paths={} -->",
        note.trim(),
        paths.join(",")
    )
}

/// A revert named on a line of the description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevertMark {
    pub to: String,
    pub paths: Vec<String>,
}

/// The revert a line names, when it carries the mark.
pub fn revert_of(line: &str) -> Option<RevertMark> {
    let i = line.find(REVERT_MARK)?;
    let rest = &line[i + REVERT_MARK.len()..];
    let attrs = &rest[..rest.find("-->").unwrap_or(rest.len())];
    let mut to = String::new();
    let mut paths = Vec::new();
    for a in attrs.split_whitespace() {
        if let Some(t) = a.strip_prefix("to=") {
            to = t.to_string();
        } else if let Some(p) = a.strip_prefix("paths=") {
            paths = p
                .split(',')
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect();
        }
    }
    (!to.is_empty()).then_some(RevertMark { to, paths })
}

/// Every revert the description names.
pub fn reverts_in(body: &str) -> Vec<RevertMark> {
    body.lines().filter_map(revert_of).collect()
}

/// Where the head's checks stand, as the build read them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeadCi {
    pub head: String,
    /// The head's checks were read.
    pub read: bool,
    /// The checks failing on the head.
    pub failing: Vec<String>,
}

/// `body` with its CI lines true for the head: the "CI is failing" section
/// goes when it was written on another head or the head's checks show no
/// failure; a revert's line goes when `undone` says its files changed
/// again since the green head it went back to.
pub fn reconcile(body: &str, ci: &HeadCi, undone: &dyn Fn(&RevertMark) -> bool) -> String {
    let mut out = body.to_string();
    if let Some(at) = failing_head(&out) {
        let other_head = !at.is_empty()
            && !ci.head.is_empty()
            && !ci.head.starts_with(&at)
            && !at.starts_with(&ci.head);
        let no_failure = ci.read && ci.failing.is_empty();
        if other_head || no_failure {
            out = without_failing(&out);
        }
    }
    if out.contains(REVERT_MARK) {
        let kept: Vec<&str> = out
            .lines()
            .filter(|l| revert_of(l).is_none_or(|m| !undone(&m)))
            .collect();
        out = kept.join("\n");
        out = drop_empty_deferred(&out);
    }
    out
}

/// A "Deferred" heading left with nothing under it goes too.
fn drop_empty_deferred(body: &str) -> String {
    let lines: Vec<&str> = body.lines().collect();
    let mut out: Vec<&str> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let l = lines[i];
        if l.trim() == "## Deferred" {
            let next = lines[i + 1..]
                .iter()
                .find(|n| !n.trim().is_empty())
                .map(|n| n.trim());
            if next.is_none_or(|n| n.starts_with('#') || n.starts_with("<!--")) {
                i += 1;
                continue;
            }
        }
        out.push(l);
        i += 1;
    }
    squeeze(&out.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = "Source ticket: WEB-1\n\n<!-- coderhelm:ci-failing head=abc123456789 -->\n## CI is failing\n**CI is failing:** `Deploy Preview`.\n<!-- /coderhelm:ci-failing -->\n\nRemoves the link.\n\n## Deferred\n- CI stayed red: reverted 2 file(s) to b66dfd3. <!-- coderhelm:revert to=b66dfd3aaaa paths=a.ts,b.ts -->";

    #[test]
    fn the_failing_section_goes_with_its_head_or_a_green_head() {
        assert_eq!(failing_head(BODY).as_deref(), Some("abc123456789"));
        let same = HeadCi {
            head: "abc123456789ff".into(),
            read: true,
            failing: vec!["Deploy Preview".into()],
        };
        assert!(reconcile(BODY, &same, &|_| false).contains("## CI is failing"));
        let moved = HeadCi {
            head: "def000".into(),
            read: false,
            failing: vec![],
        };
        assert!(!reconcile(BODY, &moved, &|_| false).contains("CI is failing"));
        let green = HeadCi {
            head: "abc123456789".into(),
            read: true,
            failing: vec![],
        };
        let out = reconcile(BODY, &green, &|_| false);
        assert!(!out.contains("CI is failing"), "{out}");
        assert!(
            out.starts_with("Source ticket: WEB-1\n\nRemoves the link."),
            "{out}"
        );
    }

    #[test]
    fn a_revert_line_goes_once_its_work_came_back() {
        let marks = reverts_in(BODY);
        assert_eq!(
            marks,
            vec![RevertMark {
                to: "b66dfd3aaaa".into(),
                paths: vec!["a.ts".into(), "b.ts".into()]
            }]
        );
        let ci = HeadCi::default();
        assert!(reconcile(BODY, &ci, &|_| false).contains("reverted 2 file(s)"));
        let out = reconcile(BODY, &ci, &|m| m.paths.contains(&"a.ts".to_string()));
        assert!(!out.contains("reverted"), "{out}");
        assert!(!out.contains("## Deferred"), "{out}");
        let line = revert_line("Reverted.", "b66dfd3", &["x y.ts".into(), "c.ts".into()]);
        assert_eq!(revert_of(&line).unwrap().paths, vec!["c.ts".to_string()]);
    }
}
