//! Where each part of a description goes: the visible part (Summary,
//! Needed from you, Review focus, Risk and Rollback, Verification) stays in
//! sight, everything else goes in one of three collapsed blocks with fixed
//! names, and a one-line footer ends it.
//!
//! Descriptions were disliked because many code paths each appended a
//! section and nothing read the result as a person would; the old layout
//! then gave every section its own fold, seven of them on one pull request,
//! one called "More". Now every writer's section lands in the first fold
//! ("Implementation notes", or "All N updates" for a dependency update),
//! the checks, commands and open records in "Full test evidence", and the
//! plan and run in "How CoderHelm built this" (`pr_body`).
//!
//! [`layout`] is pure text and is applied on every write
//! (`worker::passes::description::save`): it takes its own fold lines out
//! first and puts them back, so a writer that rebuilt a section in between
//! can never leave the folds unbalanced. A body with no lead (one never
//! built from facts) is left as it is.
//!
//! [`rubric`] reads a finished description the way its reader does: the
//! layout's checklist (`pr_body::defects`), raw machinery in sight, and
//! statements the facts refute. The quality eval asserts it for every case
//! (`worker::passes::description_eval`).

use crate::pr_facts::{Facts, LEAD_END, LEAD_START};

/// What marks a line as one of this module's fold lines.
const FOLD_MARK: &str = "<!-- fold -->";
const FOLD_OPEN: &str = "<details><!-- fold -->";
const FOLD_CLOSE: &str = "</details><!-- fold -->";

/// The blocks of CoderHelm's that stay in sight: each exists only when
/// something is wrong.
const VISIBLE_BLOCKS: &[&str] = &["draft-status", "objections", "open-findings", "unfinished"];

/// Blocks an earlier shape of the description wrote that the lead replaces.
const SUPERSEDED: &[(&str, &str)] = &[(
    "<!-- coderhelm:not-done:start -->",
    "<!-- coderhelm:not-done:end -->",
)];

fn is_fold_line(line: &str) -> bool {
    let t = line.trim();
    t.ends_with(FOLD_MARK)
        && (t.starts_with("<details>") || t.starts_with("</details>") || t.starts_with("<summary>"))
}

/// `body` without this module's fold lines.
pub fn unfold(body: &str) -> String {
    if !body.contains(FOLD_MARK) {
        return body.to_string();
    }
    let kept: Vec<&str> = body.lines().filter(|l| !is_fold_line(l)).collect();
    squeeze(&kept.join("\n"))
}

/// No run of more than one blank line, none at the ends.
fn squeeze(text: &str) -> String {
    let mut out: Vec<&str> = Vec::new();
    for l in text.lines() {
        if l.trim().is_empty() && out.last().is_none_or(|p| p.trim().is_empty()) {
            continue;
        }
        out.push(l.trim_end());
    }
    while out.last().is_some_and(|l| l.trim().is_empty()) {
        out.pop();
    }
    out.join("\n")
}

fn block_name(line: &str) -> Option<&str> {
    let t = line.trim();
    let name = t
        .strip_prefix("<!-- coderhelm:")?
        .strip_suffix("-->")?
        .trim();
    (!name.is_empty() && !name.contains(' ')).then_some(name)
}

fn is_ticket_line(line: &str) -> bool {
    let l = line.trim().to_lowercase();
    l.starts_with("source ticket:")
        || l.starts_with("closes #")
        || l.starts_with("fixes #")
        || l.starts_with("resolves #")
}

/// One part of the body below the lead.
#[derive(Debug)]
struct Part {
    lines: Vec<String>,
    /// It stays in sight.
    visible: bool,
}

impl Part {
    /// Whether it says anything: a heading and markers alone say nothing.
    fn has_content(&self) -> bool {
        self.lines.iter().any(|l| {
            let t = l.trim();
            !(t.is_empty()
                || t.starts_with('#')
                || (t.starts_with("<!--") && t.ends_with("-->"))
                || t.starts_with("<a name="))
        })
    }
}

fn heading_text(line: &str) -> Option<String> {
    let t = line.trim();
    if t.starts_with('#') {
        let h = t.trim_start_matches('#').trim().trim_matches('*').trim();
        return (!h.is_empty()).then(|| h.to_string());
    }
    if t.len() > 4 && t.starts_with("**") && t.ends_with("**") && t.matches("**").count() == 2 {
        return Some(t.trim_matches('*').trim().to_string());
    }
    None
}

/// The lead, the lines that stay at the very end (a `<sub>` credit, hidden
/// marks, the stamp), and what is between.
fn split(body: &str) -> Option<(String, Vec<String>, Vec<String>)> {
    let a = body.find(LEAD_START)?;
    let b = a + body[a..].find(LEAD_END)? + LEAD_END.len();
    let lead = body[a..b].to_string();
    let rest = format!("{}\n{}", &body[..a], &body[b..]);
    let mut lines: Vec<String> = rest.lines().map(str::to_string).collect();
    let mut footer: Vec<String> = Vec::new();
    // A credit line or the stamp a writer left above a section it appended
    // goes back to the end.
    let mut hoisted: Vec<String> = Vec::new();
    lines.retain(|l| {
        let t = l.trim();
        let is_tail = t.starts_with("<sub>") || t.starts_with("<!-- coderhelm:built-for ");
        if is_tail {
            hoisted.push(l.clone());
        }
        !is_tail
    });
    while let Some(l) = lines.last() {
        let t = l.trim();
        let is_footer = t.is_empty()
            || t.starts_with("<sub>")
            || (t.starts_with("<!--") && t.ends_with("-->") && !t.starts_with("<!-- /coderhelm:"));
        if !is_footer {
            break;
        }
        let l = lines.pop().unwrap_or_default();
        if !l.trim().is_empty() {
            footer.insert(0, l);
        }
    }
    // The stamp is the last line of all.
    hoisted.sort_by_key(|l| l.trim().starts_with("<!-- coderhelm:built-for "));
    let stamp_at = hoisted
        .iter()
        .position(|l| l.trim().starts_with("<!-- coderhelm:built-for "))
        .unwrap_or(hoisted.len());
    let stamps = hoisted.split_off(stamp_at);
    let mut all = hoisted;
    all.extend(footer);
    all.extend(stamps);
    Some((lead, lines, all))
}

/// The parts of the body below the lead: one per `##` heading and one per
/// block of CoderHelm's, in the order they stand.
fn parts(lines: &[String]) -> (Vec<String>, Vec<Part>) {
    let mut ticket: Vec<String> = Vec::new();
    let mut out: Vec<Part> = Vec::new();
    let mut in_code = false;
    let mut block: Option<String> = None;
    let mut dropping: Option<&str> = None;
    let mut cur = Part {
        lines: Vec::new(),
        visible: false,
    };
    let flush = |cur: &mut Part, out: &mut Vec<Part>, visible: bool| {
        let done = std::mem::replace(
            cur,
            Part {
                lines: Vec::new(),
                visible,
            },
        );
        if done.has_content() {
            out.push(done);
        }
    };
    for (i, l) in lines.iter().enumerate() {
        let t = l.trim();
        if let Some(end) = dropping {
            if t == end {
                dropping = None;
            }
            continue;
        }
        if t.starts_with("```") || t.starts_with("~~~") {
            in_code = !in_code;
            cur.lines.push(l.clone());
            continue;
        }
        if in_code {
            cur.lines.push(l.clone());
            continue;
        }
        if let Some(close) = &block {
            cur.lines.push(l.clone());
            if t == close {
                block = None;
                flush(&mut cur, &mut out, false);
            }
            continue;
        }
        if let Some((_, end)) = SUPERSEDED.iter().find(|(s, _)| t == *s) {
            if lines[i + 1..].iter().any(|x| x.trim() == *end) {
                dropping = Some(end);
                continue;
            }
        }
        if let Some(name) = block_name(l) {
            let close = format!("<!-- /coderhelm:{name} -->");
            if lines[i + 1..].iter().any(|x| x.trim() == close) {
                flush(&mut cur, &mut out, VISIBLE_BLOCKS.contains(&name));
                cur.lines.push(l.clone());
                block = Some(close);
                continue;
            }
        }
        if t.starts_with("## ") || t.starts_with("# ") {
            flush(&mut cur, &mut out, false);
            cur.lines.push(l.clone());
            continue;
        }
        if is_ticket_line(l) && !ticket.iter().any(|x| x.trim() == t) {
            ticket.push(t.to_string());
            continue;
        }
        cur.lines.push(l.clone());
    }
    flush(&mut cur, &mut out, false);
    (ticket, out)
}

/// Takes the block from `start` to `end` (both lines included) out of
/// `lines`, when both are there.
fn take_block(lines: &mut Vec<String>, start: &str, end: &str) -> Vec<String> {
    let Some(a) = lines.iter().position(|l| l.trim() == start) else {
        return Vec::new();
    };
    let Some(b) = lines[a..].iter().position(|l| l.trim() == end) else {
        return Vec::new();
    };
    lines.drain(a..=a + b).collect()
}

/// A closing keyword's line (`Closes #12`): GitHub reads it anywhere, and it
/// stays on the footer line.
fn is_closing(line: &str) -> bool {
    let l = line.trim().to_lowercase();
    ["closes #", "fixes #", "resolves #"]
        .iter()
        .any(|k| l.starts_with(k))
}

/// The closing keywords a footer line carries after the footer itself.
fn closing_of_footer(footer: &str) -> Vec<String> {
    footer
        .split(" · ")
        .filter(|p| is_closing(p))
        .map(|p| p.trim().to_string())
        .collect()
}

/// `body` without CoderHelm's generated blocks (the lead, the folds'
/// generated parts, the evidence and the footer): the written text, with
/// a closing keyword the footer carried kept as its own line.
pub fn strip_generated(body: &str) -> String {
    let mut lines: Vec<String> = unfold(body).lines().map(str::to_string).collect();
    let mut kept: Vec<String> = Vec::new();
    for (start, end) in crate::pr_body::GENERATED.iter().copied().chain([
        (LEAD_START, LEAD_END),
        (
            crate::pr_steps::EVIDENCE_START,
            crate::pr_steps::EVIDENCE_END,
        ),
    ]) {
        let taken = take_block(&mut lines, start, end);
        if start == crate::pr_body::FOOTER_START {
            for l in &taken {
                kept.extend(closing_of_footer(l));
            }
        }
    }
    let mut text = squeeze(&lines.join("\n"));
    for k in kept {
        if !text.lines().any(|l| l.trim() == k) {
            text.push_str(&format!("\n\n{k}"));
        }
    }
    text
}

fn fold(label: &str, parts: &[String]) -> String {
    let joined = parts
        .iter()
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    // A heading that only repeats the fold's name goes.
    let text = squeeze(
        &joined
            .lines()
            .filter(|l| {
                !(l.trim_start().starts_with('#')
                    && heading_text(l).is_some_and(|h| h.eq_ignore_ascii_case(label)))
            })
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let part = Part {
        lines: text.lines().map(str::to_string).collect(),
        visible: false,
    };
    if !part.has_content() {
        return String::new();
    }
    format!(
        "{FOLD_OPEN}\n<summary>{}</summary>{FOLD_MARK}\n\n{text}\n\n{FOLD_CLOSE}",
        label.replace('<', "&lt;")
    )
}

/// `body` in its order: the visible part (the lead), the blocks that exist
/// only when something is wrong, then at most three folds with fixed names
/// (every written section in the first, the checks and open records in
/// "Full test evidence", the plan and run in "How CoderHelm built this"),
/// then the footer line and the hidden marks. A section with nothing in it
/// is dropped, and a line that repeats the summary goes. A body with no
/// lead is returned as it is. Laying out twice changes nothing.
pub fn layout(body: &str) -> String {
    use crate::pr_body::{self as pb};
    let flat = unfold(body);
    let Some((lead, mut lines, footer)) = split(&flat) else {
        return body.to_string();
    };
    let label = lead
        .lines()
        .find_map(|l| {
            l.trim()
                .strip_prefix(pb::LABEL_START)
                .and_then(|r| r.strip_suffix("-->"))
        })
        .map(|l| l.trim().to_string())
        .unwrap_or_else(|| pb::NOTES.to_string());
    let ticket_key = lead
        .lines()
        .find_map(|l| l.trim().strip_prefix("<!-- coderhelm:body v1 ticket="))
        .and_then(|r| r.split_whitespace().next())
        .filter(|k| *k != "none")
        .unwrap_or("")
        .to_string();
    let first = lead
        .lines()
        .find(|l| l.contains(pb::SUMMARY_END))
        .map(|l| pb::summary_core(l, &ticket_key))
        .filter(|l| l.len() >= 20)
        .unwrap_or_default();
    let notes_top = take_block(&mut lines, pb::NOTES_TOP_START, pb::NOTES_TOP_END);
    let checks = take_block(&mut lines, pb::CHECKS_START, pb::CHECKS_END);
    let evidence = take_block(
        &mut lines,
        crate::pr_steps::EVIDENCE_START,
        crate::pr_steps::EVIDENCE_END,
    );
    let mut unverified = take_block(
        &mut lines,
        "<!-- coderhelm:unverified -->",
        "<!-- /coderhelm:unverified -->",
    );
    // A record of open items with none in it says nothing.
    if !unverified
        .iter()
        .any(|l| l.trim_start().starts_with("- ") || l.trim_start().starts_with("* "))
    {
        unverified.clear();
    }
    let built = take_block(&mut lines, pb::BUILT_START, pb::BUILT_END);
    let foot = take_block(&mut lines, pb::FOOTER_START, pb::FOOTER_END);
    let mut closing: Vec<String> = foot.iter().flat_map(|l| closing_of_footer(l)).collect();
    lines.retain(|l| {
        if is_closing(l) {
            let t = l.trim().to_string();
            if !closing.contains(&t) {
                closing.push(t);
            }
            false
        } else {
            true
        }
    });
    // Credit lines belong with how it was built.
    let (credits, footer): (Vec<String>, Vec<String>) = footer
        .into_iter()
        .partition(|l| l.trim().starts_with("<sub>"));
    let (ticket, parts) = parts(&lines);
    let mut out = lead;
    for p in parts.iter().filter(|p| p.visible) {
        out.push_str("\n\n");
        out.push_str(squeeze(&p.lines.join("\n")).trim());
    }
    let mut notes: Vec<String> = vec![notes_top.join("\n")];
    if !ticket.is_empty() {
        notes.push(ticket.join("\n"));
    }
    for p in parts.iter().filter(|p| !p.visible) {
        let kept: Vec<&str> = p
            .lines
            .iter()
            .map(String::as_str)
            .filter(|l| first.is_empty() || l.trim() != first)
            .collect();
        let text = squeeze(&kept.join("\n"));
        let check = Part {
            lines: text.lines().map(str::to_string).collect(),
            visible: false,
        };
        if check.has_content() {
            notes.push(text);
        }
    }
    for (name, parts) in [
        (label.as_str(), notes),
        (
            pb::EVIDENCE,
            vec![
                checks.join("\n"),
                evidence.join("\n"),
                unverified.join("\n"),
            ],
        ),
        (pb::BUILT, vec![built.join("\n"), credits.join("\n")]),
    ] {
        let f = fold(name, &parts);
        if !f.is_empty() {
            out.push_str("\n\n");
            out.push_str(&f);
        }
    }
    // The footer line, with the closing keywords on it.
    let mut line = foot
        .iter()
        .map(|l| l.trim())
        .find(|l| l.starts_with("Opened by CoderHelm"))
        .map(|l| {
            l.split(" · ")
                .filter(|p| !is_closing(p))
                .collect::<Vec<_>>()
                .join(" · ")
        })
        .unwrap_or_default();
    if line.is_empty() && !closing.is_empty() {
        line = "Opened by CoderHelm".to_string();
    }
    for c in &closing {
        line.push_str(&format!(" · {c}"));
    }
    if !line.is_empty() {
        out.push_str(&format!(
            "\n\n{}\n{line}\n{}",
            pb::FOOTER_START,
            pb::FOOTER_END
        ));
    }
    if !footer.is_empty() {
        out.push_str("\n\n");
        out.push_str(&footer.join("\n"));
    }
    out
}

/// The part of a description a reader sees without opening anything: what
/// is outside every collapsed block, without hidden comments.
pub fn visible(body: &str) -> String {
    let mut depth = 0usize;
    let mut out: Vec<&str> = Vec::new();
    for l in body.lines() {
        let t = l.trim();
        if t.starts_with("<details") {
            depth += 1;
            continue;
        }
        if t.starts_with("</details") {
            depth = depth.saturating_sub(1);
            continue;
        }
        if depth > 0 || t.is_empty() || (t.starts_with("<!--") && t.ends_with("-->")) {
            continue;
        }
        out.push(l);
    }
    out.join("\n")
}

/// One way a description fails its reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Flaw {
    /// A rule of the layout's checklist (`pr_body::defects`) broken.
    Layout(String),
    /// A raw error, query, id or internal name in the visible text.
    Machinery(String),
    /// A statement the recorded facts do not bear out.
    Contradiction(String),
}

/// What must never be in sight: raw machinery and the phrases of the old
/// outside-steps section.
const MACHINERY: &[&str] = &[
    "HTTP status",
    "Resource not accessible by integration",
    "documentation_url",
    "403 Forbidden",
    "404 Not Found",
    "401 Unauthorized",
    "{\"",
    "[{",
    "https://api.",
    "not done: not done",
    "owner: found by CoderHelm",
    "the owner of the last step",
    "after the pull request opens",
    "none, because",
];

/// The raw machinery in `text` (the visible part of a description, or a
/// ticket comment).
pub fn machinery(text: &str) -> Vec<String> {
    let mut out: Vec<String> = MACHINERY
        .iter()
        .filter(|m| text.contains(*m))
        .map(|m| m.to_string())
        .collect();
    for (name, _) in crate::pr_steps::INTERNAL_NAMES {
        if text.contains(name) {
            out.push(name.to_string());
        }
    }
    out
}

/// How a finished description fails its reader, if it does: the layout's
/// checklist, raw machinery in sight, and anything that contradicts a
/// recorded fact.
pub fn rubric(body: &str, facts: &Facts) -> Vec<Flaw> {
    let mut flaws: Vec<Flaw> = crate::pr_body::defects(body, facts.short_head())
        .into_iter()
        .map(Flaw::Layout)
        .collect();
    for m in machinery(&visible(body)) {
        flaws.push(Flaw::Machinery(m));
    }
    for c in crate::pr_claims::check(body, facts) {
        flaws.push(Flaw::Contradiction(c.said));
    }
    flaws
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pr_body::{FOOTER_END, FOOTER_START};

    const LEAD: &str = "<!-- coderhelm:lead -->\n<!-- coderhelm:body v1 ticket=OPS-41 head=abc1234 -->\nRetries the export poller three times before it gives up (OPS-41).<!-- ch:summary-end -->\n\n### Review focus\n\n- Approach: does this do what OPS-41 asks?\n\n**Risk:** Low. One module changes.\n\n**Rollback:** Revert this pull request.\n\n### Verification\n\nOn abc1234 (head of this PR):\n\n| Check | Result |\n|---|---|\n| lint (CI) | Passed |\n\n<!-- /coderhelm:lead -->";

    fn body() -> String {
        format!(
            "<!-- coderhelm:draft-status -->\n**Draft:** CI is still running.\n<!-- /coderhelm:draft-status -->\n\nSource ticket: OPS-41\n\n{LEAD}\n\n## Summary\n\nRetries the export poller three times before it gives up.\n\n## Changes\n\n* `src/export/poller.rs`: the retry loop.\n\n<!-- coderhelm:reviewer-guide -->\n**For the reviewer**\n* **Roll back:** revert this pull request.\n<!-- /coderhelm:reviewer-guide -->\n\n## Risk\n\n**Low**: one module.\n\n## Verification\n\n<!-- coderhelm:unverified -->\n## Open items\n- The staging secret is not set.\n<!-- /coderhelm:unverified -->\n\n{FOOTER_START}\nOpened by CoderHelm from OPS-41\n{FOOTER_END}\n\nCloses #12\n\n<sub>Consulted: the platform agent</sub>\n\n<!-- coderhelm:built-for abc1234 0f -->"
        )
    }

    #[test]
    fn the_lead_comes_first_and_the_rest_is_in_three_folds() {
        let out = layout(&body());
        assert!(out.starts_with(LEAD), "{out}");
        let seen = visible(&out);
        assert!(seen.contains("**Draft:** CI is still running."), "{seen}");
        for gone in ["## Changes", "Open items", "Source ticket", "Consulted"] {
            assert!(!seen.contains(gone), "{gone}: {seen}");
        }
        let labels: Vec<&str> = out
            .lines()
            .filter_map(|l| l.strip_prefix("<summary>"))
            .collect();
        assert_eq!(
            labels,
            vec![
                "Implementation notes</summary><!-- fold -->",
                "Full test evidence</summary><!-- fold -->",
                "How CoderHelm built this</summary><!-- fold -->",
            ],
            "{out}"
        );
        // The Summary only repeated the lead; Verification said nothing.
        assert!(!out.contains("\n## Summary"), "{out}");
        assert!(!out.contains("\n## Verification"), "{out}");
        assert_eq!(
            out.matches("Retries the export poller three times").count(),
            1
        );
        // The footer line carries the closing keyword, and the stamp stays
        // last.
        assert!(
            out.contains("Opened by CoderHelm from OPS-41 · Closes #12\n"),
            "{out}"
        );
        assert!(
            out.ends_with("<!-- coderhelm:built-for abc1234 0f -->"),
            "{out}"
        );
        assert_eq!(
            out.matches("<details>").count(),
            out.matches("</details>").count()
        );
        // A blank line after each summary, so what is inside is parsed.
        for (i, l) in out.lines().enumerate() {
            if l.starts_with("<summary>") {
                assert_eq!(out.lines().nth(i + 1), Some(""), "{out}");
            }
        }
    }

    #[test]
    fn laying_out_twice_changes_nothing_and_no_line_is_lost() {
        let once = layout(&body());
        assert_eq!(layout(&once), once);
        let flat = unfold(&once);
        for l in [
            "* `src/export/poller.rs`: the retry loop.",
            "* **Roll back:** revert this pull request.",
            "**Low**: one module.",
            "- The staging secret is not set.",
            "<!-- coderhelm:unverified -->",
            "<!-- /coderhelm:unverified -->",
            "Source ticket: OPS-41",
        ] {
            assert!(flat.lines().any(|x| x == l), "lost: {l}\n{flat}");
        }
        assert!(!flat.contains("<details>"), "{flat}");
    }

    #[test]
    fn a_section_rebuilt_between_two_writes_never_unbalances_the_folds() {
        let once = layout(&body());
        // A writer replaced the Changes section by its heading, taking the
        // fold lines after it along.
        let a = once.find("## Changes").unwrap();
        let b = once.find("## Risk").unwrap();
        let broken = format!(
            "{}## Changes\n\n* `src/export/poller.rs`: the retry loop and its limit.\n\n{}",
            &once[..a],
            &once[b..]
        );
        let again = layout(&broken);
        assert_eq!(
            again.matches("<details>").count(),
            again.matches("</details>").count()
        );
        assert!(again.contains("the retry loop and its limit."), "{again}");
        assert_eq!(layout(&again), again);
    }

    #[test]
    fn a_section_appended_after_the_stamp_goes_in_the_first_fold() {
        let once = layout(&body());
        let appended = format!(
            "{once}\n\n<!-- coderhelm:ci-notes -->\n**CI notes**\n- `e2e` was re-run once.\n<!-- /coderhelm:ci-notes -->"
        );
        let out = layout(&appended);
        assert!(
            out.ends_with("<!-- coderhelm:built-for abc1234 0f -->"),
            "{out}"
        );
        assert!(!visible(&out).contains("CI notes"), "{out}");
        assert!(out.contains("- `e2e` was re-run once."), "{out}");
        assert!(out.matches("<details>").count() <= 3, "{out}");
        assert_eq!(layout(&out), out);
    }

    #[test]
    fn a_body_never_built_from_facts_is_left_alone() {
        let plain = "## Summary\n\nRetries the poll.\n";
        assert_eq!(layout(plain), plain);
    }

    #[test]
    fn the_old_folds_and_not_done_block_give_way() {
        let b = format!(
            "<!-- coderhelm:not-done:start -->\n**Not done: this ticket still needs access CoderHelm doesn't have**\n\n- Deploy the staging Studio\n<!-- coderhelm:not-done:end -->\n\n{LEAD}\n\n<details><summary><a name=\"coderhelm-more\"></a>More</summary><!-- fold -->\n\n* one\n\n</details><!-- fold -->\n"
        );
        let out = layout(&b);
        assert!(!out.contains("Not done: this ticket"), "{out}");
        assert!(!out.contains(">More<"), "{out}");
        assert!(out.contains("* one"), "{out}");
    }

    #[test]
    fn generated_blocks_are_stripped_and_the_closing_keyword_kept() {
        let once = layout(&body());
        let written = strip_generated(&once);
        assert!(!written.contains("### Review focus"), "{written}");
        assert!(!written.contains("Opened by CoderHelm"), "{written}");
        assert!(written.contains("Closes #12"), "{written}");
        assert!(written.contains("* `src/export/poller.rs`"), "{written}");
    }

    #[test]
    fn the_rubric_reads_what_is_in_sight() {
        let facts = Facts {
            head: "abc1234".into(),
            ..Default::default()
        };
        let good = layout(&body());
        assert_eq!(rubric(&good, &facts), vec![]);
        // Raw machinery in sight, and the old labels.
        let bad = "Deploys the Studio.\n\nCoderHelm could not do it: HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/acme/site/actions/workflows/test.yml/dispatches): {\"message\":\"Resource not accessible by integration\"}\n\nHow: query_content on production with the confirm query. (owner: the owner of the last step)\n";
        let flaws = rubric(bad, &facts);
        for m in [
            "HTTP status",
            "403 Forbidden",
            "https://api.",
            "{\"",
            "query_content",
            "the owner of the last step",
        ] {
            assert!(
                flaws.contains(&Flaw::Machinery(m.to_string())),
                "{m}: {flaws:?}"
            );
        }
        assert!(
            flaws
                .iter()
                .any(|f| matches!(f, Flaw::Layout(d) if d.contains("no Review focus"))),
            "{flaws:?}"
        );
        // Folded away, the same machinery is evidence, not a flaw.
        let folded = format!(
            "{}\n\n<details>\n<summary>Full test evidence</summary>\n\nquery_content returned [{{\"n\":392}}]\n\n</details>\n\nOpened by CoderHelm",
            LEAD
        );
        assert_eq!(rubric(&folded, &facts), vec![]);
    }
}
