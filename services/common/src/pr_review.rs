//! The last read of a pull request description before it is posted.
//!
//! A description CoderHelm posted showed six checks as "still running when
//! this was written" in its results table and never changed them; asked a
//! person to deploy staging when the repository starts that deploy from a
//! label CoderHelm adds itself; said the file to read first and the way
//! back twice each (its lead and an older "For the reviewer" block); closed
//! the review focus with "CoderHelm's conclusion: nothing is out of the
//! ordinary"; named a content page by its raw document id next to its
//! name; and listed checks still running beside checks that did not run as
//! one "Not verified" line. Each writer was right by itself; nothing read
//! the result as its reader would.
//!
//! [`review`] is that read. It runs on every description CoderHelm writes
//! or rewrites (`worker::passes::description::save`, the one write), is
//! pure and deterministic, and fixes what it finds rather than reporting
//! it:
//!
//! - a check still running is never a result: it reads "Running" and the
//!   description is rebuilt when it ends (the stamp says so);
//! - an item for a person the gate does not let through is taken out
//!   (`person_gate::sink`): what CoderHelm can do is its own work;
//! - a line said twice is said once;
//! - a sentence that carries no fact is dropped;
//! - a raw id next to the name of what it identifies goes;
//! - a "Not verified" line keeps what is still running apart from what did
//!   not run.
//!
//! [`template_missing`] checks a description laid into the repository's
//! template (`pr_template`) has each of its headings.

/// What the review found and fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A check still running shown as a result.
    RunningAsResult,
    /// An item for a person CoderHelm can do itself.
    PersonItem,
    /// A line said again.
    Duplicate,
    /// A sentence that says nothing.
    Filler,
    /// A raw id where a name exists.
    RawId,
    /// Still running and did not run on one line.
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub kind: Kind,
    pub said: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reviewed {
    pub body: String,
    pub findings: Vec<Finding>,
}

/// What a frozen running row said.
const FROZEN: &str = "still running when this was written";

/// Sentences that carry no fact, as CoderHelm's conclusion or alone.
const FILLER: &[&str] = &[
    "nothing is out of the ordinary",
    "everything looks good",
    "nothing to note",
    "no concerns",
];

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn is_hex(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// A raw id: a UUID, or a long token of letters and digits with no word in
/// it (a document or record id).
pub fn is_raw_id(token: &str) -> bool {
    let t = token.trim();
    let parts: Vec<&str> = t.split('-').collect();
    if parts.len() == 5
        && [8, 4, 4, 4, 12]
            .iter()
            .zip(&parts)
            .all(|(n, p)| p.len() == *n && is_hex(p))
    {
        return true;
    }
    t.len() >= 20
        && t.chars().all(|c| c.is_ascii_alphanumeric())
        && t.chars().any(|c| c.is_ascii_digit())
        && t.chars().any(|c| c.is_ascii_alphabetic())
}

/// `line` without the parenthesised raw ids that follow a name: "Slots
/// page (0f3c2a1b-...)" is "Slots page". An id in code, in a link, or
/// with no name before it stays.
pub fn without_raw_ids(line: &str) -> String {
    let mut out = line.to_string();
    let mut from = 0;
    while let Some(rel) = out[from..].find('(') {
        let a = from + rel;
        let Some(b_rel) = out[a..].find(')') else {
            break;
        };
        let b = a + b_rel;
        let inner = out[a + 1..b].trim();
        let id = inner
            .strip_prefix("id ")
            .or_else(|| inner.strip_prefix("id: "))
            .or_else(|| inner.strip_prefix("ID "))
            .unwrap_or(inner);
        let before = out[..a].trim_end();
        let named = before
            .chars()
            .last()
            .is_some_and(|c| c.is_alphanumeric() || c == '\'' || c == '"');
        let link = out[..a].ends_with(']');
        let ticks = out[..a].matches('`').count() % 2 == 1;
        if named && !link && !ticks && is_raw_id(id) {
            let start = before.len();
            out.replace_range(start..b + 1, "");
            from = start;
        } else {
            from = a + 1;
        }
    }
    out
}

/// A line as compared for a repeat: no list mark, no emphasis, the way
/// back named one way.
fn norm(line: &str) -> String {
    let t = line
        .trim()
        .trim_start_matches(['-', '*', '>'])
        .trim()
        .replace("**", "")
        .replace('`', "")
        .to_lowercase();
    let t = t
        .replace("roll back:", "rollback:")
        .replace("look first:", "read first:");
    one_line(&t).trim_end_matches('.').to_string()
}

/// Whether a line is prose a reader reads (not a table, code fence,
/// heading, comment or fold line).
fn is_prose(line: &str) -> bool {
    let t = line.trim();
    !(t.is_empty()
        || t.starts_with('|')
        || t.starts_with('#')
        || t.starts_with("<!--")
        || t.starts_with("<details")
        || t.starts_with("</details")
        || t.starts_with("<summary")
        || t.starts_with("<sub>")
        || t.starts_with("```"))
}

/// The filler sentence in `line`, when it has one.
fn filler_in(line: &str) -> Option<String> {
    let lower = line.to_lowercase();
    for f in FILLER {
        for said in [
            format!("coderhelm's conclusion: {f}."),
            format!("coderhelm's conclusion: {f}"),
        ] {
            if let Some(at) = lower.find(&said) {
                return Some(line[at..at + said.len()].to_string());
            }
        }
        let bare = norm(line);
        if bare == *f || bare == format!("{f}.") {
            return Some(line.trim().to_string());
        }
    }
    None
}

/// "**Not verified on `x`:** a (still running); b (did not run)." as two
/// lines: what is still running, and what did not verify the change.
fn split_mixed(line: &str) -> Option<Vec<String>> {
    let at = line.find("**Not verified on ")?;
    let lead = &line[..at];
    let rest = &line[at..];
    let colon = rest.find(":** ")?;
    let head = &rest["**Not verified on ".len()..colon];
    let list = rest[colon + 4..].trim().trim_end_matches('.');
    let groups: Vec<&str> = list.split("; ").collect();
    let (running, other): (Vec<&str>, Vec<&str>) = groups
        .into_iter()
        .partition(|g| g.trim_end().ends_with("(still running)"));
    if running.is_empty() {
        return None;
    }
    let names: Vec<String> = running
        .iter()
        .map(|g| {
            g.trim_end()
                .trim_end_matches("(still running)")
                .trim()
                .to_string()
        })
        .collect();
    let mut out = vec![format!(
        "{lead}**Still running on {head}:** {}; updated here when {}.",
        names.join(", "),
        if names.len() == 1 {
            "it ends"
        } else {
            "they end"
        }
    )];
    if !other.is_empty() {
        out.push(format!(
            "{lead}**Not verified on {head}:** {}.",
            other.join("; ")
        ));
    }
    Some(out)
}

/// The final read: `body` as its reader should see it, and what was fixed.
pub fn review(body: &str) -> Reviewed {
    let mut findings: Vec<Finding> = Vec::new();
    let mut out: Vec<String> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    let mut in_code = false;
    for l in body.lines() {
        let t = l.trim();
        if t.starts_with("```") || t.starts_with("~~~") {
            in_code = !in_code;
            out.push(l.to_string());
            continue;
        }
        if in_code {
            out.push(l.to_string());
            continue;
        }
        let mut line = l.to_string();
        // A running check is never a result.
        if line.contains(FROZEN) {
            findings.push(Finding {
                kind: Kind::RunningAsResult,
                said: one_line(&line),
            });
            line = line
                .replace(
                    &format!("Not run: {FROZEN}"),
                    &format!("{}; updated here when it ends", crate::pr_body::RUNNING),
                )
                .replace(FROZEN, "running; updated here when it ends");
        }
        if let Some(lines) = split_mixed(&line) {
            findings.push(Finding {
                kind: Kind::Mixed,
                said: one_line(&line),
            });
            out.extend(lines);
            continue;
        }
        if is_prose(&line) {
            if let Some(f) = filler_in(&line) {
                findings.push(Finding {
                    kind: Kind::Filler,
                    said: f.clone(),
                });
                line = one_line(&line.replace(&f, ""));
                // Keep the list mark's indent.
                let indent: String = l.chars().take_while(|c| c.is_whitespace()).collect();
                line = format!("{indent}{line}");
                let rest = norm(&line);
                if rest.is_empty() {
                    continue;
                }
            }
            let fixed = without_raw_ids(&line);
            if fixed != line {
                findings.push(Finding {
                    kind: Kind::RawId,
                    said: one_line(&line),
                });
                line = fixed;
            }
            let n = norm(&line);
            if n.split_whitespace().count() >= 4 {
                if seen.contains(&n) {
                    findings.push(Finding {
                        kind: Kind::Duplicate,
                        said: one_line(&line),
                    });
                    continue;
                }
                seen.push(n);
            }
        }
        out.push(line);
    }
    let text = out.join("\n");
    let sunk = crate::person_gate::sink(&text);
    for s in &sunk.sent_back {
        findings.push(Finding {
            kind: Kind::PersonItem,
            said: one_line(s),
        });
    }
    let mut body_out = sunk.text;
    if body.ends_with('\n') && !body_out.ends_with('\n') {
        body_out.push('\n');
    }
    Reviewed {
        body: body_out,
        findings,
    }
}

/// The template's headings a description laid into it lacks in sight.
pub fn template_missing(body: &str, template: &str) -> Vec<String> {
    crate::pr_template::missing(template, &crate::pr_body::in_sight(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_running_check_is_never_a_result() {
        let r = review("| Lint (CI) | Not run: still running when this was written |");
        assert_eq!(r.body, "| Lint (CI) | Running; updated here when it ends |");
        assert_eq!(r.findings[0].kind, Kind::RunningAsResult);
    }

    #[test]
    fn filler_goes_and_the_fact_beside_it_stays() {
        let r = review("- Approach: decide whether this does what HARBOR-1 asks. CoderHelm's conclusion: nothing is out of the ordinary.");
        assert_eq!(
            r.body,
            "- Approach: decide whether this does what HARBOR-1 asks."
        );
        let r = review("Nothing is out of the ordinary.\nKept.");
        assert_eq!(r.body, "Kept.");
    }

    #[test]
    fn a_raw_id_beside_a_name_goes_and_one_alone_stays() {
        assert_eq!(
            without_raw_ids(
                "Studio: Slots page (0f3c2a1b-9d8e-4c7b-a6f5-e4d3c2b1a0f9), listing Summary field."
            ),
            "Studio: Slots page, listing Summary field."
        );
        let alone = "The document `0f3c2a1b-9d8e-4c7b-a6f5-e4d3c2b1a0f9` was read.";
        assert_eq!(without_raw_ids(alone), alone);
        let link = "[run](https://example.com/0f3c2a1b-9d8e-4c7b-a6f5-e4d3c2b1a0f9)";
        assert_eq!(without_raw_ids(link), link);
        assert_eq!(without_raw_ids("Retries (3 times)."), "Retries (3 times).");
    }

    #[test]
    fn a_line_said_twice_is_said_once() {
        let r = review("**Rollback:** Revert this pull request; nothing outside the code changes with it.\n\n* **Roll back:** revert this pull request; nothing outside the code changes with it.");
        assert!(!r.body.contains("Roll back:"), "{}", r.body);
        assert_eq!(r.findings[0].kind, Kind::Duplicate);
    }

    #[test]
    fn running_and_did_not_run_are_said_apart() {
        let r =
            review("* **Not verified on `6de2414`:** `lint` (still running); `e2e` (did not run).");
        assert_eq!(
            r.body,
            "* **Still running on `6de2414`:** `lint`; updated here when it ends.\n* **Not verified on `6de2414`:** `e2e` (did not run)."
        );
    }
}
