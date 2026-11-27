//! A test branch that has never run is unverified code.
//!
//! A portal spec had two branches on the page's state: booking closed
//! (the slot's button disabled, it must say so) and booking open (the
//! button enabled, choosing it must fill the cart). Only the closed one
//! ever ran; the open one was written from a guess, and the day the vendor
//! opened booking it failed on every branch, because a signed-out
//! visitor gets a sign-in drawer and the cart stays empty.
//!
//! - [`state_branches`] finds the branches of end-to-end code on a page's
//!   state: `if` on an awaited `isEnabled()`, `isVisible()`, `isChecked()`,
//!   `count()` and the like, directly or through a variable assigned from
//!   one, and `test.skip`/`test.fixme` on a condition. Each has two arms
//!   (an `if` with no `else` skips its block on the other), the expect
//!   messages each arm asserts with, and the test id its condition reads
//!   when the file names it ([`Branch::subject`]).
//! - [`executed`] reads which arms a recorded run went through, from the
//!   step titles of a Playwright report (an expect with a message is a step
//!   titled with it).
//! - [`runnable_now`] reads which arms the page's present state would run,
//!   from a probe of it: an arm no recorded run executed is labelled
//!   unverified in the pull request, and once the page shows the state it
//!   needs, the coder runs it against the live page and settles it.

use serde::{Deserialize, Serialize};

use super::probe::ProbeRecord;

/// The page-state reads a branch can turn on.
const STATE_READS: &[&str] = &[
    "isEnabled",
    "isDisabled",
    "isVisible",
    "isHidden",
    "isChecked",
    "isEditable",
    "count",
];

/// One arm of a branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Arm {
    /// Whether the condition holds on this arm.
    pub when: bool,
    /// Its first line (1-based); for an `if` with no `else`, the `if`'s.
    pub line: usize,
    /// The messages its expects carry (`expect(x, "…")`), which a report's
    /// steps are titled with.
    pub markers: Vec<String>,
}

/// A branch of end-to-end code on a page's state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Branch {
    pub path: String,
    pub line: usize,
    /// The condition as written.
    pub condition: String,
    /// The state read (`isEnabled`, `count`, …) and whether the condition
    /// negates it.
    pub read: String,
    pub negated: bool,
    /// The test id (or `/pattern/`) the condition reads, when the file
    /// names it.
    pub subject: Option<String>,
    pub arms: Vec<Arm>,
}

impl Branch {
    /// How a person reads an arm: "when `isEnabled()` is true".
    pub fn arm_words(&self, arm: &Arm) -> String {
        let on = match &self.subject {
            Some(s) => format!(" on `{s}`"),
            None => String::new(),
        };
        let holds = arm.when != self.negated;
        match self.read.as_str() {
            "count" => format!(
                "when `{}` finds {}{on}",
                self.condition.trim(),
                if arm.when {
                    "what it asks"
                } else {
                    "otherwise"
                }
            ),
            r => format!("when `{r}()`{on} is {holds}"),
        }
    }
}

fn line_of(code: &str, at: usize) -> usize {
    code[..at].matches('\n').count() + 1
}

/// The byte index just past the bracket that closes the one at `open`.
fn close_of(code: &str, open: usize) -> Option<usize> {
    let b = code.as_bytes();
    let (o, c) = match b[open] {
        b'(' => (b'(', b')'),
        b'{' => (b'{', b'}'),
        _ => return None,
    };
    let mut depth = 0i32;
    let mut i = open;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let ch = b[i];
        if let Some(q) = quote {
            if ch == b'\\' {
                i += 2;
                continue;
            }
            if ch == q {
                quote = None;
            }
        } else if matches!(ch, b'\'' | b'"' | b'`') {
            quote = Some(ch);
        } else if ch == o {
            depth += 1;
        } else if ch == c {
            depth -= 1;
            if depth == 0 {
                return Some(i + 1);
            }
        }
        i += 1;
    }
    None
}

/// The statement or block that starts at `from` (after whitespace): its
/// span.
fn body_at(code: &str, from: usize) -> (usize, usize) {
    let rest = &code[from..];
    let skip = rest.len() - rest.trim_start().len();
    let start = from + skip;
    if code[start..].starts_with('{') {
        let end = close_of(code, start).unwrap_or(code.len());
        return (start, end);
    }
    let end = code[start..]
        .find(['\n', ';'])
        .map(|i| start + i + 1)
        .unwrap_or(code.len());
    (start, end)
}

/// The messages of the expects in `text` (`expect(x, "…")`, with the
/// message on the same line or the next).
pub fn markers(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (i, _) in text.match_indices("expect(") {
        let open = i + "expect".len();
        let Some(end) = close_of(text, open) else {
            continue;
        };
        let args = &text[open + 1..end - 1];
        // The second top-level argument, when it is a plain string.
        let b = args.as_bytes();
        let mut depth = 0i32;
        let mut quote: Option<u8> = None;
        let mut j = 0;
        let mut comma = None;
        while j < b.len() {
            let ch = b[j];
            if let Some(q) = quote {
                if ch == b'\\' {
                    j += 2;
                    continue;
                }
                if ch == q {
                    quote = None;
                }
            } else if matches!(ch, b'\'' | b'"' | b'`') {
                quote = Some(ch);
            } else if matches!(ch, b'(' | b'[' | b'{') {
                depth += 1;
            } else if matches!(ch, b')' | b']' | b'}') {
                depth -= 1;
            } else if ch == b',' && depth == 0 {
                comma = Some(j);
                break;
            }
            j += 1;
        }
        let Some(c) = comma else { continue };
        let msg = args[c + 1..].trim().trim_end_matches(',').trim();
        let Some(q) = msg.chars().next().filter(|q| matches!(q, '"' | '\'' | '`')) else {
            continue;
        };
        if let Some(stop) = msg[1..].find(q) {
            let m = &msg[1..1 + stop];
            // A template's placeholders: keep its longest fixed words.
            let m = m
                .split("${")
                .enumerate()
                .map(|(k, part)| {
                    if k == 0 {
                        part
                    } else {
                        part.split_once('}').map_or("", |(_, rest)| rest)
                    }
                })
                .map(|p| p.trim_matches(|c: char| c == ' ' || c == '"' || c == '\''))
                .max_by_key(|p| p.len())
                .unwrap_or("");
            if m.len() >= 6 && !out.iter().any(|o| o == m) {
                out.push(m.to_string());
            }
        }
    }
    out
}

/// The state read a condition makes, if any: (read, negated).
fn read_in(cond: &str) -> Option<(String, String)> {
    for r in STATE_READS {
        let call = format!(".{r}(");
        if let Some(i) = cond.find(&call) {
            // The receiver: the expression the read is called on.
            let before = &cond[..i];
            let start = before
                .rfind(|c: char| !(c.is_alphanumeric() || matches!(c, '_' | '.' | '(' | ')')))
                .map(|k| k + 1)
                .unwrap_or(0);
            return Some((r.to_string(), before[start..].trim().to_string()));
        }
    }
    None
}

/// Variables assigned from a state read: name, (read, receiver).
fn state_vars(code: &str) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for line in code.lines() {
        let l = line.trim();
        let Some(rest) = ["const ", "let ", "var "]
            .iter()
            .find_map(|k| l.strip_prefix(k))
        else {
            continue;
        };
        let Some((name, value)) = rest.split_once('=') else {
            continue;
        };
        let name = name.trim().trim_end_matches(':').trim();
        if !name.chars().all(|c| c.is_alphanumeric() || c == '_') || name.is_empty() {
            continue;
        }
        if !value.contains("await") {
            continue;
        }
        if let Some((read, recv)) = read_in(value) {
            out.push((name.to_string(), read, recv));
        }
    }
    out
}

/// The test id a receiver reads: through a local variable assigned from
/// it, and a method of the file that returns a `getByTestId(…)`.
fn subject_of(code: &str, receiver: &str) -> Option<String> {
    let mut expr = receiver.to_string();
    for _ in 0..3 {
        if let Some(id) = super::probe::selectors_in(&expr).into_iter().next() {
            return Some(id);
        }
        let head: String = expr
            .trim_start_matches("this.")
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if head.is_empty() {
            return None;
        }
        // A local variable: `const head = <expr>;`.
        let var = ["const ", "let "].iter().find_map(|k| {
            let pat = format!("{k}{head} =");
            code.find(&pat).map(|i| {
                let v = &code[i + pat.len()..];
                v[..v.find(';').unwrap_or(v.len())].trim().to_string()
            })
        });
        if let Some(v) = var {
            expr = v;
            continue;
        }
        // A method of the file: `head(): Locator {` … `}`.
        let def = code.match_indices(&format!("{head}(")).find_map(|(i, _)| {
            let line_start = code[..i].rfind('\n').map(|k| k + 1).unwrap_or(0);
            let prefix = code[line_start..i].trim();
            if !(prefix.is_empty() || prefix == "async" || prefix == "get") {
                return None;
            }
            let open = i + code[i..].find('{')?;
            let end = close_of(code, open)?;
            Some(code[open..end].to_string())
        });
        return def.and_then(|body| super::probe::selectors_in(&body).into_iter().next());
    }
    None
}

/// The branches of end-to-end code on a page's state.
pub fn state_branches(path: &str, code: &str) -> Vec<Branch> {
    let vars = state_vars(code);
    let mut out = Vec::new();
    for (i, _) in code.match_indices("if (") {
        // A word ending in "if" (e.g. "elif") isn't one.
        if i > 0 {
            let prev = code[..i].chars().next_back().unwrap_or(' ');
            if prev.is_alphanumeric() || prev == '_' {
                continue;
            }
        }
        let open = i + 3;
        let Some(close) = close_of(code, open) else {
            continue;
        };
        let cond = code[open + 1..close - 1].trim().to_string();
        let negated = cond.starts_with('!');
        let (read, receiver) = match read_in(&cond) {
            Some(r) => r,
            None => match vars.iter().find(|(v, _, _)| {
                cond.split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .any(|w| w == v)
            }) {
                Some((_, r, recv)) => (r.clone(), recv.clone()),
                None => continue,
            },
        };
        let (ts, te) = body_at(code, close);
        let mut arms = vec![Arm {
            when: true,
            line: line_of(code, ts),
            markers: markers(&code[ts..te]),
        }];
        let after = code[te..].trim_start();
        let else_at = te + (code[te..].len() - after.len());
        if let Some(rest) = after.strip_prefix("else") {
            let from = else_at + 4 + (rest.len() - rest.trim_start().len());
            let (es, ee) = body_at(code, from);
            arms.push(Arm {
                when: false,
                line: line_of(code, es),
                markers: markers(&code[es..ee]),
            });
        } else {
            arms.push(Arm {
                when: false,
                line: line_of(code, i),
                markers: Vec::new(),
            });
        }
        out.push(Branch {
            path: path.to_string(),
            line: line_of(code, i),
            condition: cond,
            read,
            negated,
            subject: subject_of(code, &receiver),
            arms,
        });
    }
    for (i, _) in code
        .match_indices("test.skip(")
        .chain(code.match_indices("test.fixme("))
    {
        let open = i + code[i..].find('(').unwrap_or(0);
        let Some(close) = close_of(code, open) else {
            continue;
        };
        let args = code[open + 1..close - 1].trim();
        // `test.skip('title', fn)` declares a skipped test; a condition
        // starts with something other than a string or a function.
        if args.is_empty()
            || args.starts_with(['\'', '"', '`'])
            || args.starts_with("()")
            || args.starts_with("async")
        {
            continue;
        }
        let cond = args.split(',').next().unwrap_or(args).trim().to_string();
        out.push(Branch {
            path: path.to_string(),
            line: line_of(code, i),
            read: "skip".into(),
            negated: false,
            subject: None,
            arms: vec![
                Arm {
                    when: true,
                    line: line_of(code, i),
                    markers: Vec::new(),
                },
                Arm {
                    when: false,
                    line: line_of(code, i),
                    markers: Vec::new(),
                },
            ],
            condition: cond,
        });
    }
    out.sort_by_key(|b| b.line);
    out
}

/// The step titles of a Playwright JSON report, at every depth.
pub fn step_titles(report: &serde_json::Value) -> Vec<String> {
    fn walk(v: &serde_json::Value, out: &mut Vec<String>) {
        match v {
            serde_json::Value::Object(m) => {
                if let Some(steps) = m.get("steps").and_then(|s| s.as_array()) {
                    for s in steps {
                        if let Some(t) = s.get("title").and_then(|t| t.as_str()) {
                            out.push(t.to_string());
                        }
                    }
                }
                for x in m.values() {
                    walk(x, out);
                }
            }
            serde_json::Value::Array(a) => a.iter().for_each(|x| walk(x, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(report, &mut out);
    out
}

/// Whether a recorded run went through an arm: one of its expect messages
/// is a step title of the run.
pub fn executed(arm: &Arm, steps: &[String]) -> bool {
    arm.markers
        .iter()
        .any(|m| steps.iter().any(|s| s.contains(m.as_str())))
}

/// Which arm the page's present state would run, read from a probe: the
/// condition's test id's state there. `None` when the probe didn't see it
/// or the read isn't one a probe records.
pub fn runnable_now(b: &Branch, probe: &ProbeRecord) -> Option<bool> {
    let subject = b.subject.as_deref()?;
    let states: Vec<_> = probe.states_of(subject).collect();
    let first = states.iter().find(|s| s.visible).or(states.first())?;
    let holds = match b.read.as_str() {
        "isEnabled" => first.enabled,
        "isDisabled" => !first.enabled,
        "isVisible" => first.visible,
        "isHidden" => !first.visible,
        "count" => states.iter().any(|s| s.visible),
        _ => return None,
    };
    Some(holds != b.negated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::probe::ProbeState;

    const PAGE: &str = r#"
export class Portal {
  sessions(): Locator {
    return this.frame.getByTestId(
      /^portal-option-guest-\d+$/,
    );
  }

  async pickFirst() {
    const session = this.sessions().first();
    const open = await session.isEnabled();

    if (open) {
      await session.click();
      await expect(
        this.cartTotal(),
        `Choosing "${slot}" did not add it to the booking total`,
      ).not.toHaveText(/\$0$/);
    } else {
      await expect(
        session,
        `"${slot}" is disabled but doesn't say booking is closed`,
      ).toContainText(/not open/i);
    }
    if ((await more.count()) > 0) await more.first().click();
  }
}
"#;

    #[test]
    fn a_branch_on_the_pages_state_has_two_arms_and_a_subject() {
        let b = state_branches("e2e/pages/portal.ts", PAGE);
        assert_eq!(b.len(), 2);
        let open = &b[0];
        assert_eq!(open.read, "isEnabled");
        assert_eq!(
            open.subject.as_deref(),
            Some("/^portal-option-guest-\\d+$/")
        );
        assert_eq!(
            open.arms[0].markers,
            vec!["did not add it to the booking total"]
        );
        assert_eq!(
            open.arms[1].markers,
            vec!["is disabled but doesn't say booking is closed"]
        );
        assert!(open
            .arm_words(&open.arms[0])
            .contains("`isEnabled()` on `/^portal-option-guest-\\d+$/` is true"));
        // An `if` with no `else` skips its block on the other arm.
        assert_eq!(b[1].read, "count");
        assert!(b[1].arms[1].markers.is_empty());
    }

    #[test]
    fn a_report_says_which_arm_ran_and_a_probe_which_can_run_now() {
        let b = &state_branches("e2e/pages/portal.ts", PAGE)[0];
        let report = serde_json::json!({"suites": [{"specs": [{"tests": [{"results": [{"steps": [
            {"title": "\"Day slot\" is disabled but doesn't say booking is closed", "steps": []}
        ]}]}]}]}]});
        let steps = step_titles(&report);
        assert!(!executed(&b.arms[0], &steps));
        assert!(executed(&b.arms[1], &steps));
        let probe = |enabled| ProbeRecord {
            steps: vec![crate::e2e::probe::ProbeStep {
                click: "week-0".into(),
                clicked: true,
                states: vec![ProbeState {
                    testid: "portal-option-guest-0".into(),
                    visible: true,
                    enabled,
                    ..Default::default()
                }],
            }],
            ..Default::default()
        };
        assert_eq!(runnable_now(b, &probe(true)), Some(true));
        assert_eq!(runnable_now(b, &probe(false)), Some(false));
        assert_eq!(runnable_now(b, &ProbeRecord::default()), None);
    }

    #[test]
    fn a_conditional_skip_is_a_branch_and_a_skipped_test_is_not() {
        let code = "test.skip(isMobile, 'desktop only');\ntest.skip('later', async () => {});";
        let b = state_branches("e2e/a.spec.ts", code);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].condition, "isMobile");
        // Plain conditions on data are no page state.
        assert!(state_branches("e2e/a.spec.ts", "if (rows.length === 0) {\n}\n").is_empty());
    }
}
