//! Where each part of a description goes: what a reader needs first stays
//! in sight, everything else is collapsed below it, and a section with
//! nothing in it is dropped.
//!
//! Descriptions were disliked because many code paths each appended a
//! section and nothing read the result as a person would. The visible part
//! is now a budget: the lead (`pr_facts::lead`: the change, how to check
//! it, the risk, what the reader has to do), and the few blocks that exist
//! only when something is wrong (why it is a draft, a reviewer's objection,
//! open findings, planned work not done). Every other section, whoever
//! wrote it, sits in a collapsed block with a stable anchor.
//!
//! [`layout`] is pure text and is applied on every write
//! (`worker::passes::description::save`): it takes its own fold lines out
//! first and puts them back, so a writer that rebuilt a section in between
//! can never leave the folds unbalanced. A body with no lead (one never
//! built from facts) is left as it is.
//!
//! [`rubric`] reads a finished description the way its reader does. It is
//! what the quality eval asserts for every case
//! (`worker::passes::description_eval`).

use crate::pr_facts::{Facts, LEAD_END, LEAD_START};

/// What marks a line as one of this module's fold lines.
const FOLD_MARK: &str = "<!-- fold -->";
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
    t.ends_with(FOLD_MARK) && (t.starts_with("<details>") || t.starts_with("</details>"))
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
    /// What its fold is called.
    title: String,
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

fn title_case(name: &str) -> String {
    let words = name.replace(['-', '_'], " ");
    let mut c = words.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

fn slug(title: &str) -> String {
    let mut out = String::new();
    for c in title.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('-') && !out.is_empty() {
            out.push('-');
        }
    }
    out.trim_matches('-').chars().take(40).collect()
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
        title: "Description".to_string(),
        lines: Vec::new(),
        visible: false,
    };
    let flush = |cur: &mut Part, out: &mut Vec<Part>, title: String, visible: bool| {
        let done = std::mem::replace(
            cur,
            Part {
                title,
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
                flush(&mut cur, &mut out, "More".to_string(), false);
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
                let title = lines[i + 1..]
                    .iter()
                    .take_while(|x| x.trim() != close)
                    .find_map(|x| heading_text(x))
                    .unwrap_or_else(|| title_case(name));
                flush(&mut cur, &mut out, title, VISIBLE_BLOCKS.contains(&name));
                cur.lines.push(l.clone());
                block = Some(close);
                continue;
            }
        }
        if t.starts_with("## ") || t.starts_with("# ") {
            let title = heading_text(l).unwrap_or_else(|| "More".to_string());
            flush(&mut cur, &mut out, title, false);
            cur.lines.push(l.clone());
            continue;
        }
        if is_ticket_line(l) && !ticket.iter().any(|x| x.trim() == t) {
            ticket.push(t.to_string());
            continue;
        }
        cur.lines.push(l.clone());
    }
    flush(&mut cur, &mut out, String::new(), false);
    (ticket, out)
}

/// `body` in its order: the lead, the blocks that exist only when something
/// is wrong, the ticket line, then every other part collapsed under its own
/// name with a stable anchor, then the footer. A part with nothing in it is
/// dropped, and a line that repeats the lead's first line goes. A body with
/// no lead is returned as it is. Laying out twice changes nothing.
pub fn layout(body: &str) -> String {
    let flat = unfold(body);
    let Some((lead, lines, footer)) = split(&flat) else {
        return body.to_string();
    };
    let first = lead
        .lines()
        .nth(1)
        .map(str::trim)
        .filter(|l| l.len() >= 20)
        .unwrap_or("")
        .to_string();
    let (ticket, parts) = parts(&lines);
    let mut out = lead;
    for p in parts.iter().filter(|p| p.visible) {
        out.push_str("\n\n");
        out.push_str(squeeze(&p.lines.join("\n")).trim());
    }
    if !ticket.is_empty() {
        out.push_str("\n\n");
        out.push_str(&ticket.join("\n"));
    }
    let mut used: Vec<String> = Vec::new();
    for p in parts.iter().filter(|p| !p.visible) {
        let kept: Vec<&str> = p
            .lines
            .iter()
            .map(String::as_str)
            .filter(|l| first.is_empty() || l.trim() != first)
            .collect();
        let text = squeeze(&kept.join("\n"));
        let text = text.trim();
        let check = Part {
            title: String::new(),
            lines: text.lines().map(str::to_string).collect(),
            visible: false,
        };
        if !check.has_content() {
            continue;
        }
        let mut anchor = slug(&p.title);
        if anchor.is_empty() {
            anchor = "more".to_string();
        }
        let mut n = 2;
        let base = anchor.clone();
        while used.contains(&anchor) {
            anchor = format!("{base}-{n}");
            n += 1;
        }
        used.push(anchor.clone());
        out.push_str(&format!(
            "\n\n<details><summary><a name=\"coderhelm-{anchor}\"></a>{}</summary>{FOLD_MARK}\n\n{text}\n\n{FOLD_CLOSE}",
            p.title.replace('<', "&lt;")
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
    /// What the reader has to do is not in the first five lines.
    ActionNotFirst,
    /// A raw error, query, id or internal name in the visible text.
    Machinery(String),
    /// A statement the recorded facts do not bear out.
    Contradiction(String),
    /// The visible part is longer than the change allows.
    TooLong { lines: usize, budget: usize },
    /// The same thing said twice in the visible part.
    Repeated(String),
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

/// How a finished description fails its reader, if it does: can they find
/// what they must do in the first five lines, is any raw machinery in
/// sight, does anything contradict a recorded fact, is the visible part
/// within the budget for the change, is anything said twice.
pub fn rubric(body: &str, facts: &Facts) -> Vec<Flaw> {
    let mut flaws = Vec::new();
    let seen = visible(body);
    let lines: Vec<&str> = seen.lines().filter(|l| !l.trim().is_empty()).collect();
    if !lines
        .iter()
        .take(5)
        .any(|l| l.trim_start().starts_with("**Needed from you:**"))
    {
        flaws.push(Flaw::ActionNotFirst);
    }
    for m in machinery(&seen) {
        flaws.push(Flaw::Machinery(m));
    }
    for c in crate::pr_claims::check(body, facts) {
        flaws.push(Flaw::Contradiction(c.said));
    }
    // The blocks that exist only when something is wrong are counted with
    // the budget: each is one more thing out of the ordinary.
    let extra = body
        .lines()
        .filter(|l| block_name(l).is_some_and(|n| VISIBLE_BLOCKS.contains(&n)))
        .count()
        * 4;
    let budget = crate::pr_facts::visible_budget(facts) + extra;
    if lines.len() > budget {
        flaws.push(Flaw::TooLong {
            lines: lines.len(),
            budget,
        });
    }
    let mut said: Vec<&str> = Vec::new();
    for l in &lines {
        let t = l.trim();
        if t.len() >= 30 && said.contains(&t) {
            flaws.push(Flaw::Repeated(t.to_string()));
        }
        said.push(t);
    }
    flaws
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEAD: &str = "<!-- coderhelm:lead -->\nRetries the export poller three times before it gives up.\n\n**Check:** 2 recorded checks passed on `abc1234`.\n**Needed from you:** Nothing.\n**Review:** decide whether this is right to merge. CoderHelm's conclusion: every recorded check passed on `abc1234`.\n<!-- /coderhelm:lead -->";

    fn body() -> String {
        format!(
            "<!-- coderhelm:draft-status -->\n**Draft:** CI is still running.\n<!-- /coderhelm:draft-status -->\n\nSource ticket: OPS-41\n\n{LEAD}\n\n## Summary\n\nRetries the export poller three times before it gives up.\n\n## Changes\n\n* `src/export/poller.rs`: the retry loop.\n\n<!-- coderhelm:reviewer-guide -->\n**For the reviewer**\n* **Roll back:** revert this pull request.\n<!-- /coderhelm:reviewer-guide -->\n\n## Risk\n\n**Low**: one module.\n\n## Verification\n\n<!-- coderhelm:unverified -->\n## Open items\n- The staging secret is not set.\n<!-- /coderhelm:unverified -->\n\n<sub>Consulted: the platform agent</sub>\n\n<!-- coderhelm:built-for abc1234 0f -->"
        )
    }

    #[test]
    fn the_lead_comes_first_and_the_rest_is_collapsed() {
        let out = layout(&body());
        assert!(out.starts_with(LEAD), "{out}");
        let seen = visible(&out);
        assert!(seen.contains("**Draft:** CI is still running."), "{seen}");
        assert!(seen.contains("Source ticket: OPS-41"), "{seen}");
        assert!(
            !seen.contains("## Changes") && !seen.contains("Open items"),
            "{seen}"
        );
        // Each part is under its own name, with an anchor that stays.
        for (anchor, title) in [
            ("coderhelm-changes", "Changes"),
            ("coderhelm-for-the-reviewer", "For the reviewer"),
            ("coderhelm-risk", "Risk"),
            ("coderhelm-open-items", "Open items"),
        ] {
            assert!(
                out.contains(&format!(
                    "<details><summary><a name=\"{anchor}\"></a>{title}</summary>"
                )),
                "{anchor}: {out}"
            );
        }
        // The Summary only repeated the lead; Verification said nothing.
        assert!(!out.contains("## Summary"), "{out}");
        assert!(!out.contains("## Verification"), "{out}");
        assert_eq!(
            out.matches("Retries the export poller three times").count(),
            1
        );
        // The footer and the stamp stay last, outside every fold.
        assert!(
            out.ends_with(
                "<sub>Consulted: the platform agent</sub>\n<!-- coderhelm:built-for abc1234 0f -->"
            ),
            "{out}"
        );
        assert_eq!(
            out.matches("<details>").count(),
            out.matches("</details>").count()
        );
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
    fn a_section_appended_after_the_stamp_goes_above_it() {
        let once = layout(&body());
        let appended = format!(
            "{once}\n\n<!-- coderhelm:ci-notes -->\n**CI notes**\n- `e2e` was re-run once.\n<!-- /coderhelm:ci-notes -->"
        );
        let out = layout(&appended);
        assert!(
            out.ends_with(
                "<sub>Consulted: the platform agent</sub>\n<!-- coderhelm:built-for abc1234 0f -->"
            ),
            "{out}"
        );
        assert!(
            out.contains("<a name=\"coderhelm-ci-notes\"></a>CI notes</summary>"),
            "{out}"
        );
        assert_eq!(layout(&out), out);
    }

    #[test]
    fn a_body_never_built_from_facts_is_left_alone() {
        let plain = "## Summary\n\nRetries the poll.\n";
        assert_eq!(layout(plain), plain);
    }

    #[test]
    fn the_old_not_done_block_gives_way_to_the_lead() {
        let b = format!(
            "<!-- coderhelm:not-done:start -->\n**Not done: this ticket still needs access CoderHelm doesn't have**\n\n- Deploy the staging Studio\n<!-- coderhelm:not-done:end -->\n\n{LEAD}\n\n## Changes\n\n* one\n"
        );
        let out = layout(&b);
        assert!(!out.contains("Not done: this ticket"), "{out}");
        assert!(out.contains("* one"), "{out}");
    }

    #[test]
    fn the_rubric_reads_what_is_in_sight() {
        let facts = Facts {
            head: "abc1234".into(),
            ..Default::default()
        };
        let good = layout(&body());
        assert_eq!(rubric(&good, &facts), vec![]);
        // Raw machinery in sight, the action buried, and a line twice.
        let bad = "## Summary\n\nDeploys the Studio.\n\nCoderHelm could not do it: HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/acme/site/actions/workflows/test.yml/dispatches): {\"message\":\"Resource not accessible by integration\"}\n\nHow: query_content on production with the confirm query. (owner: the owner of the last step)\nHow: query_content on production with the confirm query. (owner: the owner of the last step)\n";
        let flaws = rubric(bad, &facts);
        assert!(flaws.contains(&Flaw::ActionNotFirst), "{flaws:?}");
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
            flaws.iter().any(|f| matches!(f, Flaw::Repeated(_))),
            "{flaws:?}"
        );
        // Folded away, the same machinery is evidence, not a flaw.
        let folded = format!(
            "{LEAD}\n\n<details><summary>Evidence</summary>\n\nquery_content returned [{{\"n\":392}}]\n\n</details>"
        );
        assert_eq!(rubric(&folded, &facts), vec![]);
        let long = format!(
            "{LEAD}\n\n{}",
            (0..30)
                .map(|i| format!("line {i}"))
                .collect::<Vec<_>>()
                .join("\n\n")
        );
        assert!(rubric(&long, &facts)
            .iter()
            .any(|f| matches!(f, Flaw::TooLong { .. })));
    }
}
