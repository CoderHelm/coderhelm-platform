//! Every body CoderHelm sends GitHub fits GitHub's limit.
//!
//! GitHub refuses a pull request, issue or comment body over 65,536
//! characters (`422 Validation Failed: body is too long`). A bundled
//! security fix of 55 packages and 229 advisories wrote a description far
//! over that, and the run that had done all of its work ended without its
//! pull request.
//!
//! [`fit`] keeps what a reader and CoderHelm need and shrinks the long
//! parts. Kept as written: the text before the first `## ` heading (the
//! ticket line, the draft status, the "Not yet verified" block), every
//! section CoderHelm or a reviewer reads first (Summary, Not finished, CI
//! failing, Not yet verified, Verification, Open items, Proof, Risk), a
//! dependency fix run's account (its `### Dependency fix run` block under
//! What to check: the bumps applied, the ones deferred with why, the
//! sandbox's checks and the cost) and its `### Upgrade evidence` block (what
//! the run recorded for the head: the advisory count, the families, the
//! verification set, each major and its behaviour probe), every `## ` heading (so a section rewritten in place later is still found), and
//! every line holding an HTML comment (the markers CoderHelm parses later).
//! Shrunk, largest section first and a step at a time: nested detail under
//! a list item, then long lists (capped, with "and N more"), then a whole
//! section's text (a line saying where the full detail is). Only a body
//! whose kept parts alone are over the limit is cut further, and even then
//! its markers stay.

use std::fmt::Write as _;

/// GitHub's limit for a pull request, issue or comment body, in characters.
pub const GITHUB_BODY_MAX: usize = 65_536;

/// What [`fit`] aims for, in bytes: a byte count is never below the
/// character count GitHub measures, and the room left covers the notice.
pub const BODY_TARGET: usize = 64_000;

/// GitHub's limit for a pull request or issue title, in characters.
pub const GITHUB_TITLE_MAX: usize = 256;

/// The sections kept as written, by the start of their heading.
const KEPT: [&str; 12] = [
    "summary",
    "not finished",
    "ci failing",
    "not yet verified",
    "verification",
    "open items",
    "proof",
    "risk",
    "why it stopped",
    "what you can do",
    "dependency fix run",
    "upgrade evidence",
];

/// The notice a shortened body ends with.
pub const SHORTENED: &str = "Some lists here are shortened to fit GitHub's limit for a description";

/// True when GitHub takes `body` as it is.
pub fn fits(body: &str) -> bool {
    body.chars().count() <= GITHUB_BODY_MAX
}

/// `title` within GitHub's title limit.
pub fn fit_title(title: &str) -> String {
    if title.chars().count() <= GITHUB_TITLE_MAX {
        return title.to_string();
    }
    let mut out: String = title.chars().take(GITHUB_TITLE_MAX - 3).collect();
    out.push_str("...");
    out
}

/// `body` within GitHub's limit (as it is when it already fits).
pub fn fit(body: &str) -> String {
    fit_to(body, BODY_TARGET, None)
}

/// [`fit`], with `more` (a link) named where detail is left out.
pub fn fit_with_link(body: &str, more: &str) -> String {
    fit_to(
        body,
        BODY_TARGET,
        Some(more).filter(|m| !m.trim().is_empty()),
    )
}

/// `body` within `target` bytes, shrunk as the module says.
pub fn fit_to(body: &str, target: usize, more: Option<&str>) -> String {
    if body.len() <= target {
        return body.to_string();
    }
    let notice = match more {
        Some(m) => format!("\n\n_{SHORTENED}. Full detail: {m}_\n"),
        None => format!("\n\n_{SHORTENED}._\n"),
    };
    let budget = target.saturating_sub(notice.len()).max(1);
    let mut doc = Doc::parse(body);
    let and_more = |n: usize| match more {
        Some(m) => format!("- and {n} more (full list: {m})"),
        None => format!("- and {n} more"),
    };
    // Each step applies to one section at a time, largest first, until the
    // body fits: the kept sections only after every other one is a stub.
    for kept_too in [false, true] {
        for level in LEVELS {
            loop {
                if doc.len() <= budget {
                    return doc.render() + &notice;
                }
                let Some(i) = doc.largest_shrinkable(kept_too, level) else {
                    break;
                };
                let section = &mut doc.sections[i];
                section.lines = shrink(&section.lines, level, &and_more, more);
                section.levels_done = section.levels_done.max(level.rank + 1);
            }
        }
    }
    // Only the text before the first heading is left over the limit: cut it,
    // keeping every marker.
    let rendered = doc.render();
    cut_keeping_markers(&rendered, budget) + &notice
}

/// One step of shrinking.
#[derive(Clone, Copy)]
struct Level {
    rank: u8,
    /// Nested lines kept under a list item (and lines under a `###`).
    children: usize,
    /// Top-level items kept in a list (items, table rows, `###` blocks).
    items: usize,
    /// The section's text becomes one line.
    stub: bool,
}

const LEVELS: [Level; 6] = [
    Level {
        rank: 0,
        children: 6,
        items: 60,
        stub: false,
    },
    Level {
        rank: 1,
        children: 2,
        items: 30,
        stub: false,
    },
    Level {
        rank: 2,
        children: 0,
        items: 15,
        stub: false,
    },
    Level {
        rank: 3,
        children: 0,
        items: 6,
        stub: false,
    },
    Level {
        rank: 4,
        children: 0,
        items: 2,
        stub: false,
    },
    Level {
        rank: 5,
        children: 0,
        items: 0,
        stub: true,
    },
];

struct Section {
    /// The `## ` line, or `None` for the text before the first one.
    heading: Option<String>,
    lines: Vec<String>,
    kept: bool,
    levels_done: u8,
}

impl Section {
    fn len(&self) -> usize {
        self.heading.as_ref().map_or(0, |h| h.len() + 1)
            + self.lines.iter().map(|l| l.len() + 1).sum::<usize>()
    }
}

struct Doc {
    sections: Vec<Section>,
    trailing_newline: bool,
}

fn is_heading2(line: &str) -> bool {
    line.starts_with("## ")
}

fn is_marker(line: &str) -> bool {
    line.contains("<!--")
}

fn is_kept_heading(h: &str) -> bool {
    let t = h.trim_start_matches('#').trim().to_ascii_lowercase();
    KEPT.iter().any(|k| t.starts_with(k))
}

impl Doc {
    fn parse(body: &str) -> Doc {
        let mut sections = vec![Section {
            heading: None,
            lines: vec![],
            kept: true,
            levels_done: 0,
        }];
        let mut fence = false;
        for line in body.lines() {
            if line.trim_start().starts_with("```") {
                fence = !fence;
            }
            if !fence && is_heading2(line) {
                sections.push(Section {
                    heading: Some(line.to_string()),
                    lines: vec![],
                    kept: is_kept_heading(line),
                    levels_done: 0,
                });
                continue;
            }
            sections
                .last_mut()
                .expect("one section at least")
                .lines
                .push(line.to_string());
        }
        Doc {
            sections,
            trailing_newline: body.ends_with('\n'),
        }
    }

    fn len(&self) -> usize {
        self.sections.iter().map(Section::len).sum()
    }

    /// The largest section `level` has not been applied to yet.
    fn largest_shrinkable(&self, kept_too: bool, level: Level) -> Option<usize> {
        self.sections
            .iter()
            .enumerate()
            .filter(|(_, s)| s.heading.is_some())
            .filter(|(_, s)| kept_too || !s.kept)
            .filter(|(_, s)| s.levels_done <= level.rank)
            .filter(|(_, s)| {
                s.lines
                    .iter()
                    .any(|l| !l.trim().is_empty() && !is_marker(l))
            })
            .max_by_key(|(_, s)| s.len())
            .map(|(i, _)| i)
    }

    fn render(&self) -> String {
        let mut out = String::new();
        for s in &self.sections {
            if let Some(h) = &s.heading {
                out.push_str(h);
                out.push('\n');
            }
            for l in &s.lines {
                out.push_str(l);
                out.push('\n');
            }
        }
        if !self.trailing_newline && out.ends_with('\n') {
            out.pop();
        }
        out
    }
}

/// A piece of a section: a list item or table row with its nested lines, a
/// `###` block with its lines, a fenced block, or a line of text.
enum Unit {
    Item { head: String, children: Vec<String> },
    Block { head: String, children: Vec<String> },
    Text(Vec<String>),
}

fn is_item_line(line: &str) -> bool {
    let t = line;
    if t.starts_with(' ') || t.starts_with('\t') {
        return false;
    }
    if t.starts_with("- ") || t.starts_with("* ") || t.starts_with("+ ") {
        return true;
    }
    if t.starts_with('|') {
        return !is_table_rule(t);
    }
    let digits = t.chars().take_while(|c| c.is_ascii_digit()).count();
    digits > 0 && t[digits..].starts_with(". ")
}

fn is_table_rule(line: &str) -> bool {
    let t = line.trim();
    t.starts_with('|') && t.chars().all(|c| matches!(c, '|' | '-' | ':' | ' '))
}

fn units(lines: &[String]) -> Vec<Unit> {
    let mut out: Vec<Unit> = vec![];
    let mut i = 0;
    while i < lines.len() {
        let line = &lines[i];
        if line.trim_start().starts_with("```") {
            let mut block = vec![line.clone()];
            i += 1;
            while i < lines.len() {
                block.push(lines[i].clone());
                let closes = lines[i].trim_start().starts_with("```");
                i += 1;
                if closes {
                    break;
                }
            }
            out.push(Unit::Text(block));
            continue;
        }
        if line.starts_with("### ") || line.starts_with("#### ") {
            let mut children = vec![];
            i += 1;
            while i < lines.len() && !lines[i].starts_with("### ") && !lines[i].starts_with("#### ")
            {
                children.push(lines[i].clone());
                i += 1;
            }
            out.push(Unit::Block {
                head: line.clone(),
                children,
            });
            continue;
        }
        // A table's header row is the row before its rule: kept as text.
        let header = line.starts_with('|') && lines.get(i + 1).is_some_and(|n| is_table_rule(n));
        if is_item_line(line) && !header {
            let mut children = vec![];
            i += 1;
            while i < lines.len()
                && (lines[i].starts_with(' ') || lines[i].starts_with('\t'))
                && !lines[i].trim().is_empty()
            {
                children.push(lines[i].clone());
                i += 1;
            }
            out.push(Unit::Item {
                head: line.clone(),
                children,
            });
            continue;
        }
        out.push(Unit::Text(vec![line.clone()]));
        i += 1;
    }
    out
}

/// The marker lines among `lines`: kept whatever else goes.
fn markers(lines: &[String]) -> impl Iterator<Item = &String> {
    lines.iter().filter(|l| is_marker(l))
}

fn shrink(
    lines: &[String],
    level: Level,
    and_more: &dyn Fn(usize) -> String,
    more: Option<&str>,
) -> Vec<String> {
    if level.stub {
        let mut out = vec![String::new()];
        out.push(match more {
            Some(m) => {
                format!("Left out to fit GitHub's limit for a description. Full detail: {m}")
            }
            None => "Left out to fit GitHub's limit for a description.".to_string(),
        });
        out.push(String::new());
        // A kept block stays whole; the markers of the rest stay too.
        for unit in units(lines) {
            match unit {
                Unit::Block { head, children } if is_kept_heading(&head) => {
                    out.push(head);
                    out.extend(children);
                }
                Unit::Item { head, children } | Unit::Block { head, children } => {
                    if is_marker(&head) {
                        out.push(head);
                    }
                    out.extend(markers(&children).cloned());
                }
                Unit::Text(t) => out.extend(markers(&t).cloned()),
            }
        }
        if out.last().is_some_and(|l| !l.trim().is_empty()) {
            out.push(String::new());
        }
        return out;
    }
    let mut out: Vec<String> = vec![];
    let mut items_seen = 0usize;
    let mut dropped = 0usize;
    let mut held_markers: Vec<String> = vec![];
    let flush_dropped = |out: &mut Vec<String>, dropped: &mut usize, held: &mut Vec<String>| {
        if *dropped > 0 {
            out.push(and_more(*dropped));
            *dropped = 0;
        }
        out.append(held);
    };
    for unit in units(lines) {
        match unit {
            Unit::Text(t) => {
                // A list's end: say how many items it left out.
                if t.iter().any(|l| !l.trim().is_empty()) {
                    flush_dropped(&mut out, &mut dropped, &mut held_markers);
                    items_seen = 0;
                }
                out.extend(t);
            }
            // A kept block (a dependency fix run's account) stays as
            // written and is not one of the list's items.
            Unit::Block { head, children } if is_kept_heading(&head) => {
                flush_dropped(&mut out, &mut dropped, &mut held_markers);
                out.push(head);
                out.extend(children);
            }
            Unit::Item { head, children } | Unit::Block { head, children } => {
                items_seen += 1;
                if items_seen > level.items {
                    dropped += 1;
                    if is_marker(&head) {
                        held_markers.push(head.clone());
                    }
                    held_markers.extend(markers(&children).cloned());
                    continue;
                }
                out.push(head);
                let keep = level.children;
                let (shown, rest) = children.split_at(children.len().min(keep));
                out.extend(shown.iter().cloned());
                // Never leave a code block open: it would swallow the
                // headings after it.
                if shown
                    .iter()
                    .filter(|l| l.trim_start().starts_with("```"))
                    .count()
                    % 2
                    == 1
                {
                    out.push("```".to_string());
                }
                let left: Vec<&String> = rest
                    .iter()
                    .filter(|l| !l.trim().is_empty() && !is_marker(l))
                    .collect();
                if !left.is_empty() {
                    out.push(format!("  - and {} more lines", left.len()));
                }
                out.extend(markers(rest).cloned());
                if rest.last().is_some_and(|l| l.trim().is_empty()) {
                    out.push(String::new());
                }
            }
        }
    }
    flush_dropped(&mut out, &mut dropped, &mut held_markers);
    out
}

/// `body` cut to `budget` bytes, with every marker line it lost put back at
/// its end (pairs stay in order: a start kept, its end comes after it).
fn cut_keeping_markers(body: &str, budget: usize) -> String {
    let all: Vec<&str> = body.lines().filter(|l| is_marker(l)).collect();
    let tail: usize = all.iter().map(|l| l.len() + 1).sum();
    let room = budget.saturating_sub(tail + 32);
    let mut out = crate::truncate_str(body, room).to_string();
    // Never end inside a line: what's left of it goes.
    if let Some(i) = out.rfind('\n') {
        out.truncate(i + 1);
    }
    let mut kept_markers: Vec<String> = out
        .lines()
        .filter(|l| is_marker(l))
        .map(String::from)
        .collect();
    out.push_str("\n...\n");
    for m in all {
        if let Some(p) = kept_markers.iter().position(|k| *k == m) {
            kept_markers.remove(p);
            continue;
        }
        let _ = writeln!(out, "{m}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    const UNFINISHED_START: &str = "<!-- coderhelm:unfinished -->";
    const UNFINISHED_END: &str = "<!-- /coderhelm:unfinished -->";

    /// A body like the bundled security fix's: the ticket line, the
    /// draft status block, the kept sections, and long per-package detail.
    fn big_body(packages: usize, advisories: usize) -> String {
        let mut s = String::from("Source ticket: SEC-1\n\n<!-- coderhelm:unverified -->\n> [!WARNING]\n> **Not yet verified**\n> - **Waiting on CI:** build <!-- coderhelm:ci -->\n<!-- /coderhelm:unverified -->\n\n");
        s.push_str(&format!("{UNFINISHED_START}\n## Not finished\n- **Why it stopped:** a check errored.\n{UNFINISHED_END}\n\n"));
        s.push_str("## Summary\n\nFixes advisories in many packages.\n\n| Package | Version |\n| --- | --- |\n");
        for p in 0..packages {
            s.push_str(&format!("| `pkg-{p}` | 1.0.0 to 2.0.0 |\n"));
        }
        s.push_str("\n## Advisory\n\n");
        for a in 0..advisories {
            s.push_str(&format!("- GHSA-{a:04}-xxxx-yyyy, CVE-2026-{a:05} · CVSS 7.5 · CWE-400 · published 2026-01-01\n  - https://github.com/advisories/GHSA-{a:04}-xxxx-yyyy\n  - https://nvd.nist.gov/vuln/detail/CVE-2026-{a:05}\n"));
        }
        s.push_str("\n## Reachability\n\n");
        for a in 0..advisories {
            s.push_str(&format!("- GHSA-{a:04}: **Unknown**. The package is loaded by the server at start and its parser is called on request bodies.\n"));
            for l in 0..6 {
                s.push_str(&format!(
                    "  - `src/module_{a}/file_{l}.ts:{l}` const value = parse(input, options)\n"
                ));
            }
        }
        s.push_str("\n## What to check\n\n");
        for p in 0..packages {
            s.push_str(&format!("### `pkg-{p}` 1.0.0 to 2.0.0 (major version)\n\n- Release notes: read from GitHub releases (2.0.0).\n- Breaking changes found: 3\n  - removed `parse()`\n  - requires Node 20\n  - default timeout is 5s\n- Where the code uses what changed:\n  - `src/a.ts:10` (`parse`): not changed here: check it still works with the new version\n\n"));
        }
        s.push_str("**Tests**: CI runs on this pull request; this line is updated when it ends.\n");
        s.push_str(
            "\n## Proof\n\nCI runs on this pull request; it stays a draft until CI passes.\n",
        );
        s.push_str("\n## Verification\n\n- [ ] CI passes\n\n<!-- coderhelm:security -->\n");
        s
    }

    fn marker_lines(s: &str) -> Vec<String> {
        s.lines()
            .filter(|l| l.contains("<!--"))
            .map(String::from)
            .collect()
    }

    fn headings(s: &str) -> Vec<String> {
        s.lines()
            .filter(|l| l.starts_with("## "))
            .map(String::from)
            .collect()
    }

    #[test]
    fn a_body_three_times_over_fits_with_every_kept_part() {
        let mut body = big_body(55, 229);
        while body.len() < 3 * GITHUB_BODY_MAX {
            body = body.replace(
                "## Proof",
                "## Affected\n\n- `x` 1.0 in `package.json`, runtime, direct\n\n## Proof",
            );
            body.push_str(&"- filler line for the size of the body\n".repeat(200));
        }
        assert!(body.len() >= 3 * GITHUB_BODY_MAX, "{}", body.len());
        let out = fit_with_link(&body, "https://app.example.com/runs/detail?id=r1");
        assert!(fits(&out), "{} chars", out.chars().count());
        assert!(out.len() <= BODY_TARGET);
        assert!(out.starts_with("Source ticket: SEC-1\n"));
        // Every marker, in the same order, and every heading.
        assert_eq!(marker_lines(&out), marker_lines(&body));
        assert_eq!(headings(&out), headings(&body));
        for kept in [
            "## Not finished\n- **Why it stopped:** a check errored.",
            "## Proof\n\nCI runs on this pull request; it stays a draft until CI passes.",
            "## Verification\n\n- [ ] CI passes",
            "> **Not yet verified**",
            "Fixes advisories in many packages.",
        ] {
            assert!(out.contains(kept), "{kept}");
        }
        assert!(out.contains("and "), "says what it left out");
        assert!(out.contains("Full detail: https://app.example.com/runs/detail?id=r1"));
    }

    #[test]
    fn a_body_that_fits_is_sent_as_it_is() {
        let body = big_body(3, 4);
        assert_eq!(fit(&body), body);
    }

    #[test]
    fn a_section_rewritten_in_place_still_rounds_trip_on_the_fitted_body() {
        let body = big_body(55, 229);
        assert!(!fits(&body));
        let fitted = fit(&body);
        assert!(fits(&fitted));
        let fresh = "## What to check\n\nfresh check: CI fails on `build`.\n\n## Proof\n\nCI fails on this pull request (`build`).\n";
        let next = crate::upgrade_notes::replace_sections(
            &fitted,
            fresh,
            &["## What to check", "## Proof"],
        );
        assert!(next.contains("## What to check\n\nfresh check: CI fails on `build`."));
        assert!(next.contains("## Proof\n\nCI fails on this pull request (`build`)."));
        assert_eq!(marker_lines(&next), marker_lines(&body));
        // Fitting again changes nothing a later rewrite reads.
        let again = fit(&next);
        assert!(again.contains("fresh check: CI fails on `build`."));
        assert_eq!(headings(&again), headings(&body));
    }

    #[test]
    fn the_not_finished_markers_stay_paired() {
        let body = big_body(55, 229);
        let out = fit(&body);
        let a = out.find(UNFINISHED_START).unwrap();
        let b = out.find(UNFINISHED_END).unwrap();
        assert!(a < b);
    }

    #[test]
    fn a_body_over_the_limit_before_its_first_heading_is_cut_keeping_markers() {
        let mut body = String::from("Source ticket: T-1\n<!-- coderhelm:draft-status -->\n");
        body.push_str(&"plain text that never ends\n".repeat(6000));
        body.push_str("<!-- coderhelm:links -->\n## Summary\nx\n");
        let out = fit(&body);
        assert!(fits(&out));
        assert!(out.contains("<!-- coderhelm:draft-status -->"));
        assert!(out.contains("<!-- coderhelm:links -->"));
        assert!(out.starts_with("Source ticket: T-1"));
    }

    /// Random bodies of every size: the result always fits, keeps every
    /// marker and heading, and a body that fits is never changed.
    #[test]
    fn any_size_fits_and_keeps_its_markers() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        for round in 0..120 {
            let mut s = String::from("Closes #12\n\n<!-- coderhelm:draft-status -->\n> draft\n");
            let sections = rng.random_range(1..12);
            for k in 0..sections {
                let name = [
                    "Summary",
                    "Changes",
                    "Advisory",
                    "What to check",
                    "Verification",
                    "Open items",
                    "Files",
                ][rng.random_range(0..7)];
                s.push_str(&format!("\n## {name} {k}\n\n"));
                if rng.random_bool(0.2) {
                    s.push_str(&format!("<!-- coderhelm:mark-{round}-{k} -->\n"));
                }
                let big = rng.random_bool(0.3);
                let items = rng.random_range(0..(if big { 3000 } else { 40 }));
                for i in 0..items {
                    match rng.random_range(0..5) {
                        0 => s.push_str(&format!("### block {i}\n- a\n- b\n\n")),
                        1 => s.push_str(&format!("| r{i} | v |\n")),
                        2 => s.push_str(&format!(
                            "- item {i} {}\n  - child\n  - child two\n",
                            "w".repeat(rng.random_range(0..200))
                        )),
                        3 => s.push_str("```\ncode line\n```\n"),
                        _ => s.push_str(&format!("text {i}\n")),
                    }
                }
            }
            let out = fit(&s);
            assert!(fits(&out), "round {round}: {} chars", out.chars().count());
            if s.len() <= BODY_TARGET {
                assert_eq!(out, s, "round {round}");
            }
            let mut want = marker_lines(&s);
            let mut got = marker_lines(&out);
            want.sort();
            got.sort();
            assert_eq!(got, want, "round {round}");
            assert_eq!(headings(&out), headings(&s), "round {round}");
        }
    }

    /// A dependency fix run's account, as `dep_fix::DepRun::report_md`
    /// writes it.
    fn dep_report(applied: usize, deferred: usize) -> String {
        let mut s = String::from("### Dependency fix run\n\n");
        s.push_str(&format!("**Applied ({applied}):**\n"));
        for i in 0..applied {
            s.push_str(&format!(
                "- `pkg-{i}` 1.0.0 to 1.0.{i} (package.json; same version line)\n"
            ));
        }
        s.push_str(&format!(
            "\n**Deferred ({deferred}), the findings stay open:**\n"
        ));
        for i in 0..deferred {
            s.push_str(&format!(
                "- `big-{i}` 1.0.0 to 2.0.0: a major version for a finding that is not reachable\n"
            ));
        }
        s.push_str("\n**Checks run in the sandbox:**\n- test: passes with these bumps\n- lint: fails on the base branch too (not caused by this change; not fixed here)\n");
        s.push_str("\n**Cost:** 4 sandbox probe(s); model $0.42 by batch:\n- batch 1: 2 probe(s), 30 applied, 0 deferred, 1200 tokens, $0.20\n");
        s
    }

    #[test]
    fn a_dependency_fix_runs_account_stays_whole_in_a_fitted_body() {
        let report = dep_report(40, 15);
        let body = big_body(55, 229).replace(
            "## What to check\n\n",
            &format!("## What to check\n\n{report}\n"),
        );
        assert!(!fits(&body));
        for target in [BODY_TARGET, 20_000, 12_000] {
            let out = fit_to(&body, target, None);
            assert!(out.contains(report.trim_end()), "target {target}: {out}");
            assert_eq!(marker_lines(&out), marker_lines(&body), "target {target}");
            assert_eq!(headings(&out), headings(&body), "target {target}");
        }
        // The packages beside it are what shrinks.
        let out = fit_to(&body, 12_000, None);
        assert!(out.matches("### `pkg-").count() < 55);
    }

    #[test]
    fn titles_stay_within_githubs_limit() {
        assert_eq!(fit_title("fix(deps): x"), "fix(deps): x");
        let t = fit_title(&"é".repeat(400));
        assert_eq!(t.chars().count(), GITHUB_TITLE_MAX);
    }
}
