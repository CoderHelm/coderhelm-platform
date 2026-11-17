//! The tickets and pull requests related to a run's ticket are part of its
//! work, and the pull request says which of their criteria it covers.
//!
//! The failure this was written for: a ticket's pull request was closed by
//! a person who expected another ticket's pull request to cover it (the
//! tickets were linked in Jira). That pull request never read the linked
//! ticket: the drawer's Register button still pointed to the old address
//! when it was called ready.
//!
//! Now:
//!
//! - The Jira app sends the ticket's issue links and the tickets its text
//!   names, each with its summary and description ([`Related`]); the
//!   gateway keeps them on the run's message.
//! - [`criteria`] reads a ticket's acceptance criteria from its text; the
//!   planner is given every related ticket's ([`plan_note`]) and the run
//!   keeps them ([`Criterion`]).
//! - A pull request closed in favor of this one (it links this pull
//!   request and was closed without merging) brings its own change:
//!   [`moved_links`] reads the addresses it replaced, and [`check_source`]
//!   says whether this pull request's head made the same move.
//! - Before the pull request is called ready, each criterion is checked
//!   where it can be: an address change on the head's files, a link on the
//!   preview page ([`link_expectation`], [`check_page`]). One that can't be
//!   checked is listed as not covered, with why. [`section`] is what the
//!   description shows.

use serde::{Deserialize, Serialize};

/// The marker of the description's related-tickets section.
pub const MARKER: &str = "<!-- coderhelm:related -->";

/// A ticket related to the run's ticket, as the Jira app sends it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Related {
    pub key: String,
    /// How it relates ("relates to", "blocks", "is cloned by", "named in
    /// the ticket").
    #[serde(default)]
    pub relation: String,
    #[serde(default)]
    pub summary: String,
    /// Its description as plain text.
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub status: String,
}

/// One acceptance criterion this pull request takes on from a related
/// ticket or a pull request closed in favor of it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Criterion {
    /// Where it comes from: a ticket key or `#123`.
    pub from: String,
    pub text: String,
}

/// What checking a criterion found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    /// Covered, with what showed it.
    Covered(String),
    /// Not covered, with why (what was found, or why it couldn't be
    /// checked).
    NotCovered(String),
}

/// The related tickets a Jira app event carries (`related`: key,
/// relation, summary, status, description as a document or text).
pub fn from_payload(payload: &serde_json::Value) -> Vec<Related> {
    payload["related"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| {
            let key = r["key"].as_str()?.trim().to_string();
            if key.is_empty() {
                return None;
            }
            let s = |k: &str| r[k].as_str().unwrap_or("").trim().to_string();
            Some(Related {
                key,
                relation: s("relation"),
                summary: s("summary"),
                body: crate::jira_text::body_text(&r["description"]),
                status: s("status"),
            })
        })
        .take(8)
        .collect()
}

/// Ticket keys a text names (`ABC-123`), without `own` and without
/// repeats, in the order they appear.
pub fn keys_in_text(text: &str, own: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let start_ok = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'-');
        if start_ok && b[i].is_ascii_uppercase() {
            let mut j = i;
            while j < b.len() && (b[j].is_ascii_uppercase() || b[j].is_ascii_digit()) {
                j += 1;
            }
            if j - i >= 2 && j < b.len() && b[j] == b'-' {
                let mut k = j + 1;
                while k < b.len() && b[k].is_ascii_digit() {
                    k += 1;
                }
                let end_ok = k == b.len() || !b[k].is_ascii_alphanumeric();
                if k > j + 1 && end_ok {
                    let key = &text[i..k];
                    if !key.eq_ignore_ascii_case(own) && !out.iter().any(|o| o == key) {
                        out.push(key.to_string());
                    }
                    i = k;
                    continue;
                }
            }
            i = j.max(i + 1);
            continue;
        }
        i += 1;
    }
    out
}

fn is_heading(line: &str) -> bool {
    let t = line.trim();
    t.starts_with('#')
        || (t.starts_with("**") && t.ends_with("**"))
        || (t.ends_with(':') && t.len() < 60 && !t.starts_with('-') && !t.starts_with('*'))
}

fn criteria_heading(line: &str) -> bool {
    let t = line.to_ascii_lowercase();
    t.contains("acceptance criteria")
        || t.contains("acceptance criterion")
        || t.contains("definition of done")
        || t.trim()
            .trim_start_matches('#')
            .trim()
            .trim_end_matches(':')
            == "ac"
}

fn item(line: &str) -> Option<&str> {
    let t = line.trim_start();
    for p in ["- [ ] ", "- [x] ", "- ", "* ", "+ ", "• "] {
        if let Some(r) = t.strip_prefix(p) {
            return Some(r.trim());
        }
    }
    let d = t.chars().take_while(|c| c.is_ascii_digit()).count();
    if d > 0 && (t[d..].starts_with(". ") || t[d..].starts_with(") ")) {
        return Some(t[d + 2..].trim());
    }
    None
}

/// A ticket's acceptance criteria: the items under its "Acceptance
/// criteria" (or "Definition of done") heading. With no such heading, the
/// items of its text when it is a list of what must be true, else nothing.
pub fn criteria(text: &str) -> Vec<String> {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = Vec::new();
    if let Some(at) = lines.iter().position(|l| criteria_heading(l)) {
        // "Acceptance criteria: the button links to /x" on one line.
        if let Some((_, rest)) = lines[at].split_once(':') {
            let rest = rest.trim();
            if rest.len() > 8 {
                out.push(rest.to_string());
            }
        }
        let rest = &lines[at + 1..];
        for (k, l) in rest.iter().enumerate() {
            if let Some(i) = item(l) {
                if !i.is_empty() {
                    out.push(i.to_string());
                }
            } else if l.trim().is_empty() {
                // A blank line ends the list unless more items follow it.
                let next = rest[k + 1..].iter().find(|n| !n.trim().is_empty());
                if !out.is_empty() && next.is_none_or(|n| item(n).is_none()) {
                    break;
                }
            } else if is_heading(l) || !out.is_empty() {
                break;
            }
        }
        return out;
    }
    lines
        .iter()
        .filter_map(|l| item(l))
        .filter(|i| {
            let i = i.to_ascii_lowercase();
            [
                " should ",
                " must ",
                "links to",
                "points to",
                "goes to",
                "opens ",
            ]
            .iter()
            .any(|w| i.contains(w))
        })
        .map(str::to_string)
        .collect()
}

/// Every related ticket's criteria; a ticket with none brings its summary
/// as the one thing it asks for.
pub fn fold(related: &[Related]) -> Vec<Criterion> {
    let mut out = Vec::new();
    for r in related {
        let mut c = criteria(&r.body);
        if c.is_empty() && !r.summary.trim().is_empty() {
            c.push(r.summary.trim().to_string());
        }
        for text in c {
            out.push(Criterion {
                from: r.key.clone(),
                text,
            });
        }
    }
    out
}

/// What the planner reads about the related tickets: their criteria are
/// part of this change unless the plan says why one isn't.
pub fn plan_note(related: &[Related]) -> String {
    if related.is_empty() {
        return String::new();
    }
    let mut s = String::from(
        "\n\n## Related tickets\nThese tickets are linked to this one. Their acceptance criteria are \
         part of this change: plan a task for each, or say in the plan why one is not this \
         change's.\n",
    );
    for r in related {
        s.push_str(&format!(
            "- {} ({}): {}\n",
            r.key,
            if r.relation.is_empty() {
                "related"
            } else {
                &r.relation
            },
            r.summary.trim()
        ));
        let mut c = criteria(&r.body);
        if c.is_empty() {
            c = r
                .body
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .take(6)
                .map(str::to_string)
                .collect();
        }
        for line in c.iter().take(12) {
            s.push_str(&format!("  - {}\n", crate::truncate_str(line, 300)));
        }
    }
    s
}

fn is_addr_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || "-._~:/?#[]@!$&'()*+,;=%".contains(c)
}

/// The addresses in a line: `https://` links and site paths (`/a/b`).
pub fn addresses(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(i) = rest.find("https://").or_else(|| rest.find("http://")) {
        let tail = &rest[i..];
        let end = tail.find(|c: char| !is_addr_char(c)).unwrap_or(tail.len());
        let a = tail[..end].trim_end_matches(['.', ',', ';', ')', '\'', '"']);
        out.push(a.to_string());
        rest = &tail[end..];
    }
    // Quoted site paths: "/venue/register", '/x', `/x`.
    for q in ['"', '\'', '`'] {
        let parts: Vec<&str> = line.split(q).collect();
        for (k, p) in parts.iter().enumerate() {
            if k % 2 == 1
                && p.starts_with('/')
                && !p.starts_with("//")
                && p.len() > 1
                && p.chars().all(is_addr_char)
                && !out.iter().any(|o| o == p)
            {
                out.push(p.to_string());
            }
        }
    }
    out
}

/// An address one change replaced with another in one file: the pull
/// request closed in favor of this one moved `from` to `to` in `path`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovedLink {
    pub path: String,
    pub from: String,
    pub to: String,
}

/// The addresses a patch replaced: a removed line's address whose line was
/// replaced by one with a different address, in the same hunk.
pub fn moved_links(path: &str, patch: &str) -> Vec<MovedLink> {
    let mut out = Vec::new();
    let mut removed: Vec<String> = Vec::new();
    let mut added: Vec<String> = Vec::new();
    let mut flush = |removed: &mut Vec<String>, added: &mut Vec<String>| {
        for (r, a) in removed.iter().zip(added.iter()) {
            if r != a {
                let m = MovedLink {
                    path: path.to_string(),
                    from: r.clone(),
                    to: a.clone(),
                };
                if !out.contains(&m) {
                    out.push(m);
                }
            }
        }
        removed.clear();
        added.clear();
    };
    for line in patch.lines() {
        if line.starts_with("+++") || line.starts_with("---") {
            continue;
        }
        if let Some(l) = line.strip_prefix('-') {
            if !added.is_empty() {
                flush(&mut removed, &mut added);
            }
            removed.extend(addresses(l));
        } else if let Some(l) = line.strip_prefix('+') {
            added.extend(addresses(l));
        } else {
            flush(&mut removed, &mut added);
        }
    }
    flush(&mut removed, &mut added);
    out
}

/// Whether this pull request's head made the move: `content` is the file
/// at the head (`None` when it doesn't exist there).
pub fn check_source(m: &MovedLink, content: Option<&str>) -> Verdict {
    match content {
        None => Verdict::NotCovered(format!(
            "`{}` is not on this branch; the closed pull request changed `{}` to `{}` there",
            m.path, m.from, m.to
        )),
        Some(c) if c.contains(&m.from) && !c.contains(&m.to) => Verdict::NotCovered(format!(
            "`{}` still uses `{}`; the closed pull request changed it to `{}`",
            m.path, m.from, m.to
        )),
        Some(c) if c.contains(&m.from) => Verdict::NotCovered(format!(
            "`{}` uses `{}` but still also `{}`",
            m.path, m.to, m.from
        )),
        Some(c) if c.contains(&m.to) => Verdict::Covered(format!("`{}` uses `{}`", m.path, m.to)),
        Some(_) => Verdict::NotCovered(format!(
            "`{}` has neither `{}` nor `{}`: the link moved somewhere this check doesn't read",
            m.path, m.from, m.to
        )),
    }
}

/// A criterion that names a control and where it goes: "the Register
/// button links to /camps/register".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkExpectation {
    /// The control's visible text.
    pub label: String,
    pub target: String,
}

/// The control and address a criterion names, when it names both.
pub fn link_expectation(text: &str) -> Option<LinkExpectation> {
    let target = addresses(text).into_iter().next().or_else(|| {
        text.split_whitespace()
            .map(|w| w.trim_matches(|c: char| ",.;:()".contains(c)))
            .find(|w| w.starts_with('/') && w.len() > 1 && w.chars().all(is_addr_char))
            .map(str::to_string)
    })?;
    // A quoted label first ("Register"), else the word before "button" or
    // "link".
    let quoted = ['"', '“', '\''].iter().find_map(|q| {
        let close = if *q == '“' { '”' } else { *q };
        let start = text.find(*q)? + q.len_utf8();
        let end = text[start..].find(close)? + start;
        let l = text[start..end].trim();
        (!l.is_empty() && !l.starts_with('/') && !l.starts_with("http") && l.len() < 40)
            .then(|| l.to_string())
    });
    let label = quoted.or_else(|| {
        let words: Vec<&str> = text.split_whitespace().collect();
        words.iter().enumerate().find_map(|(i, w)| {
            let w = w.to_ascii_lowercase();
            (i > 0 && (w.starts_with("button") || w.starts_with("link") || w.starts_with("cta")))
                .then(|| {
                    words[i - 1]
                        .trim_matches(|c: char| !c.is_alphanumeric())
                        .to_string()
                })
                .filter(|l| {
                    !l.is_empty() && !["the", "a", "an"].contains(&l.to_ascii_lowercase().as_str())
                })
        })
    })?;
    Some(LinkExpectation { label, target })
}

fn strip_tags(s: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn attr<'a>(tag: &'a str, name: &str) -> Option<&'a str> {
    let at = tag.find(&format!("{name}="))? + name.len() + 1;
    let rest = &tag[at..];
    let q = rest.chars().next()?;
    if q == '"' || q == '\'' {
        let end = rest[1..].find(q)? + 1;
        Some(&rest[1..end])
    } else {
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '>')
            .unwrap_or(rest.len());
        Some(&rest[..end])
    }
}

fn same_target(href: &str, target: &str) -> bool {
    let norm = |s: &str| s.trim_end_matches('/').to_ascii_lowercase();
    let (h, t) = (norm(href), norm(target));
    h == t || h.ends_with(&t) || (t.ends_with(&h) && !h.is_empty() && h.starts_with('/'))
}

/// What the preview page's HTML says about a link expectation: the links
/// whose text is the label, and where they go. `None` when no link with
/// that text is on the page (it is drawn later, or in a drawer the page
/// opens): the page alone can't say.
pub fn check_page(page: &str, html: &str, e: &LinkExpectation) -> Option<Verdict> {
    let lower = html.to_ascii_lowercase();
    let label = e.label.to_ascii_lowercase();
    let mut seen: Vec<String> = Vec::new();
    let mut from = 0;
    while let Some(i) = lower[from..].find("<a") {
        let start = from + i;
        let Some(tag_end) = lower[start..].find('>').map(|j| start + j) else {
            break;
        };
        let Some(close) = lower[tag_end..].find("</a>").map(|j| tag_end + j) else {
            break;
        };
        let text = strip_tags(&html[tag_end + 1..close]).to_ascii_lowercase();
        let tag = &html[start..=tag_end];
        if text.contains(&label)
            || attr(tag, "aria-label").is_some_and(|a| a.to_ascii_lowercase().contains(&label))
        {
            if let Some(href) = attr(tag, "href") {
                if same_target(href, &e.target) {
                    return Some(Verdict::Covered(format!(
                        "on {page}, the \"{}\" link goes to `{href}`",
                        e.label
                    )));
                }
                seen.push(href.to_string());
            }
        }
        from = close + 4;
    }
    seen.first().map(|h| {
        Verdict::NotCovered(format!(
            "on {page}, the \"{}\" link goes to `{h}`, not `{}`",
            e.label, e.target
        ))
    })
}

/// The description's related-tickets section: each criterion with what
/// checking it found.
pub fn section(results: &[(Criterion, Verdict)]) -> String {
    if results.is_empty() {
        return String::new();
    }
    let mut s = format!("{MARKER}\n### Related tickets\n");
    for (c, v) in results {
        let text = crate::truncate_str(c.text.trim(), 160);
        match v {
            Verdict::Covered(how) => {
                s.push_str(&format!("- {}: {text}: covered ({how})\n", c.from))
            }
            Verdict::NotCovered(why) => {
                s.push_str(&format!("- {}: {text}: **not covered**: {why}\n", c.from))
            }
        }
    }
    s
}

/// `body` with its related-tickets section replaced by `section` (added at
/// the end when it has none; taken out when `section` is empty).
pub fn with_section(body: &str, section: &str) -> String {
    let mut out: Vec<&str> = Vec::new();
    let mut skipping = false;
    for line in body.lines() {
        if line.trim() == MARKER {
            skipping = true;
            continue;
        }
        if skipping {
            if line.trim() == "### Related tickets" || line.starts_with("- ") {
                continue;
            }
            skipping = false;
        }
        out.push(line);
    }
    let kept = out.join("\n");
    if section.is_empty() {
        return kept;
    }
    format!("{}\n\n{}\n", kept.trim_end(), section.trim_end())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn related_tickets_come_from_the_app_event() {
        let p = serde_json::json!({"related": [
            {"key": "ABC-2", "relation": "relates to", "summary": "Drawer button", "status": "Closed",
             "description": "{\"type\":\"doc\",\"content\":[{\"type\":\"paragraph\",\"content\":[{\"type\":\"text\",\"text\":\"Links to /new\"}]}]}"},
            {"relation": "no key"}
        ]});
        let r = from_payload(&p);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].key, "ABC-2");
        assert!(r[0].body.contains("Links to /new"), "{}", r[0].body);
        assert!(from_payload(&serde_json::json!({})).is_empty());
    }

    #[test]
    fn keys_named_in_text() {
        assert_eq!(
            keys_in_text(
                "See ABC-12 and XY-3; not ABC-12 again, not abc-1, not OWN-1.",
                "OWN-1"
            ),
            vec!["ABC-12", "XY-3"]
        );
    }

    #[test]
    fn criteria_under_their_heading() {
        let t = "What & why\nChange the button.\n\nAcceptance criteria\n- The Register button links to /camps/register\n- Opens in the same tab\n\nNotes\n- not this\n";
        assert_eq!(
            criteria(t),
            vec![
                "The Register button links to /camps/register",
                "Opens in the same tab"
            ]
        );
    }

    #[test]
    fn a_moved_link_is_read_from_the_patch_and_checked_on_the_head() {
        let patch = "@@ -10,3 +10,3 @@\n <Drawer>\n-  <Button href=\"/old/register\">Register</Button>\n+  <Button href=\"/camps/register\">Register</Button>\n </Drawer>\n";
        let m = moved_links("src/drawer.tsx", patch);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].from, "/old/register");
        assert_eq!(m[0].to, "/camps/register");
        let old = "<Button href=\"/old/register\">Register</Button>";
        assert!(matches!(
            check_source(&m[0], Some(old)),
            Verdict::NotCovered(_)
        ));
        let new = "<Button href=\"/camps/register\">Register</Button>";
        assert!(matches!(
            check_source(&m[0], Some(new)),
            Verdict::Covered(_)
        ));
    }

    #[test]
    fn a_link_criterion_is_checked_on_the_page() {
        let e =
            link_expectation("The drawer's \"Register\" button links to /camps/register").unwrap();
        assert_eq!(e.label, "Register");
        assert_eq!(e.target, "/camps/register");
        let old = r#"<nav><a class="btn" href="/old/register"><span>Register</span></a></nav>"#;
        assert!(matches!(
            check_page("the preview", old, &e),
            Some(Verdict::NotCovered(_))
        ));
        let new = r#"<a href="https://preview.example.com/camps/register">Register</a>"#;
        assert!(matches!(
            check_page("the preview", new, &e),
            Some(Verdict::Covered(_))
        ));
        assert_eq!(check_page("the preview", "<p>nothing</p>", &e), None);
    }

    #[test]
    fn the_section_replaces_itself() {
        let c = Criterion {
            from: "ABC-2".into(),
            text: "Register links to /x".into(),
        };
        let s1 = section(&[(c.clone(), Verdict::NotCovered("not checked".into()))]);
        let b1 = with_section("## Summary\nx\n", &s1);
        let s2 = section(&[(c, Verdict::Covered("ok".into()))]);
        let b2 = with_section(&b1, &s2);
        assert_eq!(b2.matches(MARKER).count(), 1, "{b2}");
        assert!(
            b2.contains("covered (ok)") && !b2.contains("not checked"),
            "{b2}"
        );
        assert!(b2.starts_with("## Summary"));
    }
}
