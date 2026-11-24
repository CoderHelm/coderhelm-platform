//! Each acceptance criterion traced to the code that changed for it, the
//! tests that cover it, and what proves it on the head.
//!
//! A pull request said what it changed and listed its checks, but nothing
//! tied the two to what the ticket asked: a reader could not tell which
//! criterion a file served, whether any test exercised it, or whether a
//! criterion had been dropped along the way. The trace is that link, as a
//! record ([`Trace`], kept on the run as [`FIELD`] and served by the run
//! API) and as a compact table in the description's checks ([`table`]),
//! rebuilt from the facts of the head on every push like the rest of the
//! description.
//!
//! For each criterion (the ticket's, else the plan's criteria map:
//! `e2e_proof::criteria_or_map`) the trace reads, from facts only:
//!
//! - **Code** ([`Link`]): the diff's non-test files the plan's map line
//!   names (`| code: <paths or symbols>`), else the files whose path, added
//!   definitions or added lines share the criterion's words. A ticket with
//!   one criterion owns the whole change.
//! - **Tests**: the map line's end-to-end test, and the diff's test files
//!   that name the criterion's words in their path or test titles (all of
//!   them for a one-criterion ticket).
//! - **Proof** ([`Proof`]): the criterion's two-way end-to-end proof on the
//!   head (staging and the preview, with the server HTML check), else its
//!   tests' runs with and without the change, else nothing yet.
//!
//! A criterion with no traced code, no test, or no proof that holds is an
//! open gap ([`gaps`]): the description lists it under its open items, never
//! as done.

use serde::{Deserialize, Serialize};

use crate::e2e_proof::{self, Criterion, MapLine, Outcome};

/// The run record attribute that keeps the trace, as JSON.
pub const FIELD: &str = "traceability";

/// Characters of a criterion shown in the table.
const CELL_CHARS: usize = 90;
/// Definitions named per file.
const MAX_SYMBOLS: usize = 4;

/// One changed file traced to a criterion, with the definitions it adds or
/// changes there.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    pub path: String,
    #[serde(default)]
    pub symbols: Vec<String>,
}

/// What the proof of a criterion came to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Fails without the change and passes with it.
    Proven,
    /// Passes with and without the change: it doesn't test the criterion.
    NotTheTicket,
    /// Fails with the change.
    Failing,
    /// No test of it on the head.
    Missing,
    /// A side couldn't run.
    Unverified,
    /// Nothing has run for this head yet.
    #[default]
    NotRun,
}

impl State {
    pub fn words(self) -> &'static str {
        match self {
            State::Proven => "proven",
            State::NotTheTicket => "passes without the change",
            State::Failing => "fails with the change",
            State::Missing => "no test",
            State::Unverified => "unverified",
            State::NotRun => "not run yet",
        }
    }
}

/// How a criterion is proven on the head.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proof {
    pub state: State,
    /// What ran: `end to end on staging and the preview`, `tests with and
    /// without the change`, or empty.
    #[serde(default)]
    pub by: String,
    #[serde(default)]
    pub detail: String,
}

/// One criterion's row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub criterion: String,
    /// `ticket` or `plan`: where the criterion was written.
    pub source: String,
    #[serde(default)]
    pub page: String,
    #[serde(default)]
    pub code: Vec<Link>,
    #[serde(default)]
    pub tests: Vec<String>,
    pub proof: Proof,
}

/// The trace of one head.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace {
    pub head: String,
    pub rows: Vec<Row>,
}

impl Trace {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }
}

/// A changed test file's runs with and without the change
/// (`habits_check`), as the trace reads it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TestRun {
    pub path: String,
    pub state: State,
    pub detail: String,
}

/// What the trace is built from.
pub struct Inputs<'a> {
    pub head: &'a str,
    pub criteria: &'a [Criterion],
    /// The criteria came from the plan's map, not the ticket.
    pub from_plan: bool,
    pub map: &'a [MapLine],
    /// The diff's files: path, status, patch.
    pub files: &'a [(String, String, String)],
    /// The two-way end-to-end proofs kept on the run.
    pub proofs: &'a [e2e_proof::Proof],
    /// The changed tests' runs with and without the change on this head.
    pub test_runs: &'a [TestRun],
}

/// Whether `path` is a test file.
pub fn is_test_path(path: &str) -> bool {
    let low = path.to_ascii_lowercase();
    let name = low.rsplit('/').next().unwrap_or(&low);
    e2e_proof::is_e2e_path(path)
        || low.split('/').any(|s| {
            matches!(
                s,
                "test" | "tests" | "__tests__" | "spec" | "specs" | "testdata"
            )
        })
        || name.contains(".test.")
        || name.contains(".spec.")
        || name.contains("_test.")
        || name.starts_with("test_")
}

fn added_lines(patch: &str) -> impl Iterator<Item = &str> {
    patch
        .lines()
        .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
        .map(|l| &l[1..])
}

const DEF_PREFIXES: &[&str] = &[
    "pub(crate) async fn ",
    "pub(crate) fn ",
    "pub async fn ",
    "pub fn ",
    "async fn ",
    "fn ",
    "pub struct ",
    "struct ",
    "pub enum ",
    "enum ",
    "pub trait ",
    "export default function ",
    "export async function ",
    "export function ",
    "async function ",
    "function ",
    "export const ",
    "export class ",
    "class ",
    "export interface ",
    "interface ",
    "export type ",
    "async def ",
    "def ",
    "func ",
];

fn ident(s: &str) -> Option<String> {
    let s = s.trim_start_matches('(');
    let id: String = s
        .chars()
        .skip_while(|c| c.is_whitespace())
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
        .collect();
    (id.len() >= 2 && !id.chars().all(|c| c.is_ascii_digit())).then_some(id)
}

/// The definitions a patch adds or changes: declared on an added line, or
/// named by a hunk header's context.
pub fn symbols(patch: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |s: String| {
        if !out.contains(&s) {
            out.push(s);
        }
    };
    for line in patch.lines() {
        let body = if let Some(rest) = line.strip_prefix("@@") {
            rest.split_once("@@").map(|(_, ctx)| ctx).unwrap_or("")
        } else if line.starts_with('+') && !line.starts_with("+++") {
            &line[1..]
        } else {
            continue;
        };
        let t = body.trim_start();
        // Go methods: `func (r *T) Name(`.
        if let Some(rest) = t.strip_prefix("func (") {
            if let Some(id) = rest.split_once(") ").and_then(|(_, n)| ident(n)) {
                push(id);
            }
            continue;
        }
        for p in DEF_PREFIXES {
            if let Some(rest) = t.strip_prefix(p) {
                if let Some(id) = ident(rest) {
                    push(id);
                }
                break;
            }
        }
    }
    out
}

/// Five-letter stems of the words worth matching, as the criteria map's
/// matching reads them; identifiers split at their case changes and
/// underscores.
fn stems(text: &str) -> Vec<String> {
    const SKIP: &[&str] = &[
        "should", "there", "which", "where", "these", "those", "after", "when", "with", "from",
        "that", "this", "only", "into", "have", "test", "tests", "spec", "file", "index", "true",
        "false", "return", "const", "export", "import", "function", "string", "number",
    ];
    let mut split = String::with_capacity(text.len());
    let mut prev_lower = false;
    for c in text.chars() {
        if c.is_ascii_uppercase() && prev_lower {
            split.push(' ');
        }
        prev_lower = c.is_ascii_lowercase();
        split.push(c);
    }
    let mut out: Vec<String> = split
        .split(|c: char| !c.is_ascii_alphanumeric())
        .map(str::to_ascii_lowercase)
        .filter(|w| w.len() >= 4 && !SKIP.contains(&w.as_str()))
        .map(|w| w.chars().take(5).collect())
        .collect();
    out.sort();
    out.dedup();
    out
}

fn overlap(a: &[String], b: &[String]) -> usize {
    a.iter().filter(|w| b.contains(w)).count()
}

/// The words of a criterion the trace matches on: its text, page and what
/// its map line asserts.
fn criterion_stems(c: &Criterion, m: Option<&MapLine>) -> Vec<String> {
    let mut t = c.text.clone();
    if let Some(p) = &c.page {
        t.push(' ');
        t.push_str(p);
    }
    if let Some(m) = m {
        t.push(' ');
        t.push_str(&m.asserts);
        t.push(' ');
        t.push_str(&m.code);
    }
    stems(&t)
}

/// What the map line names as the code for its criterion
/// (`| code: a/b.ts, slugToIds`).
fn named_code(m: Option<&MapLine>) -> Vec<String> {
    m.map(|m| {
        m.code
            .split([',', ';'])
            .map(|s| s.trim().trim_matches('`').trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    })
    .unwrap_or_default()
}

fn names_file(named: &[String], path: &str, syms: &[String]) -> bool {
    named.iter().any(|n| {
        path == n
            || path.ends_with(&format!("/{n}"))
            || (!n.contains('/') && !n.contains('.') && syms.iter().any(|s| s == n))
    })
}

fn proof_of_two_way(p: &e2e_proof::Proof) -> Proof {
    let (state, detail) = match e2e_proof::outcome(p) {
        Outcome::Proven(w) => (State::Proven, w),
        Outcome::NotTheTicket(w) => (State::NotTheTicket, w),
        Outcome::CoderWork(w) => (State::Failing, w),
        Outcome::Missing(w) => (State::Missing, w),
        Outcome::Unverified(w) => (State::Unverified, w),
    };
    Proof {
        state,
        by: "end to end on staging and the preview".into(),
        detail,
    }
}

fn proof_of_runs(runs: &[&TestRun]) -> Option<Proof> {
    let pick = |s: State| runs.iter().find(|r| r.state == s);
    let order = [
        State::Proven,
        State::Failing,
        State::NotTheTicket,
        State::Unverified,
        State::NotRun,
    ];
    let r = order.iter().find_map(|s| pick(*s))?;
    let detail = match r.state {
        State::Proven => format!("`{}` fails without the change and passes with it", r.path),
        _ if r.detail.trim().is_empty() => format!("`{}`: {}", r.path, r.state.words()),
        _ => format!(
            "`{}`: {}",
            r.path,
            crate::truncate_str(r.detail.trim(), 200)
        ),
    };
    Some(Proof {
        state: r.state,
        by: "tests with and without the change".into(),
        detail,
    })
}

/// The trace of the head. Pure: the description builds it from what it
/// read, and the eval from recorded cases.
pub fn build(i: &Inputs) -> Trace {
    let one = i.criteria.len() == 1;
    let code_files: Vec<(&str, Vec<String>, Vec<String>)> = i
        .files
        .iter()
        .filter(|(p, status, _)| !is_test_path(p) && status != "removed")
        .map(|(p, _, patch)| {
            let syms = symbols(patch);
            let mut words = stems(p);
            words.extend(stems(&syms.join(" ")));
            words.extend(stems(
                &added_lines(patch).take(200).collect::<Vec<_>>().join(" "),
            ));
            words.sort();
            words.dedup();
            (p.as_str(), syms, words)
        })
        .collect();
    let test_files: Vec<(&str, Vec<String>, Vec<String>)> = i
        .files
        .iter()
        .filter(|(p, status, _)| is_test_path(p) && status != "removed")
        .map(|(p, _, patch)| {
            let titles = e2e_proof::test_titles(&added_lines(patch).collect::<Vec<_>>().join("\n"));
            let mut words = stems(p);
            words.extend(stems(&titles.join(" ")));
            (p.as_str(), titles, words)
        })
        .collect();
    let on_head = e2e_proof::on_head(i.proofs, i.head);
    // The specs the plan's map gives to a criterion: never another
    // criterion's test by its words alone.
    let mapped_specs: Vec<String> = i
        .map
        .iter()
        .filter(|m| !m.test.trim().is_empty())
        .map(MapLine::spec)
        .collect();
    let test_words = if one { 0 } else { 2 };
    let rows = i
        .criteria
        .iter()
        .map(|c| {
            let m = e2e_proof::line_for(c, i.map);
            let want = criterion_stems(c, m);
            let named = named_code(m);
            let mut code: Vec<Link> = code_files
                .iter()
                .filter(|(p, syms, words)| {
                    if !named.is_empty() {
                        return names_file(&named, p, syms);
                    }
                    overlap(&want, words) >= 2
                        || overlap(&want, &stems(&format!("{p} {}", syms.join(" ")))) >= 1
                })
                .map(|(p, syms, _)| Link {
                    path: p.to_string(),
                    symbols: syms.iter().take(MAX_SYMBOLS).cloned().collect(),
                })
                .collect();
            if code.is_empty() && one {
                code = code_files
                    .iter()
                    .map(|(p, syms, _)| Link {
                        path: p.to_string(),
                        symbols: syms.iter().take(MAX_SYMBOLS).cloned().collect(),
                    })
                    .collect();
            }
            let mut tests: Vec<String> = Vec::new();
            if let Some(m) = m.filter(|m| !m.test.trim().is_empty()) {
                tests.push(m.test.trim().to_string());
            }
            for (p, titles, words) in &test_files {
                let spec_of_map = tests
                    .iter()
                    .any(|t| crate::ci_repro::spec_and_title(t).0 == *p);
                if spec_of_map || mapped_specs.iter().any(|m| m == p) {
                    continue;
                }
                if overlap(&want, words) >= test_words {
                    let best = titles
                        .iter()
                        .max_by_key(|t| overlap(&want, &stems(t)))
                        .filter(|t| overlap(&want, &stems(t)) > 0);
                    tests.push(match best {
                        Some(t) => format!("{p} › {t}"),
                        None => p.to_string(),
                    });
                }
            }
            // The two-way proof made for this very criterion: the proof
            // keeps the criterion's text as the same list gave it.
            let two_way = on_head.iter().find(|p| p.criterion.trim() == c.text.trim());
            let proof = match two_way {
                Some(p) => proof_of_two_way(p),
                None => {
                    let runs: Vec<&TestRun> = i
                        .test_runs
                        .iter()
                        .filter(|r| {
                            tests
                                .iter()
                                .any(|t| crate::ci_repro::spec_and_title(t).0 == r.path)
                        })
                        .collect();
                    proof_of_runs(&runs).unwrap_or_else(|| Proof {
                        state: if tests.is_empty() {
                            State::Missing
                        } else {
                            State::NotRun
                        },
                        ..Default::default()
                    })
                }
            };
            Row {
                criterion: c.text.trim().to_string(),
                source: if i.from_plan { "plan" } else { "ticket" }.to_string(),
                page: c.page.clone().unwrap_or_default(),
                code,
                tests,
                proof,
            }
        })
        .collect();
    Trace {
        head: i.head.to_string(),
        rows,
    }
}

/// One open gap of the trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gap {
    pub criterion: String,
    pub what: String,
    /// The two-way proof already reports it (the description says it once).
    pub by_two_way: bool,
}

/// What the trace leaves open: a criterion no changed code is traced to,
/// one no test covers, one whose proof doesn't hold.
pub fn gaps(t: &Trace) -> Vec<Gap> {
    let mut out = Vec::new();
    for r in &t.rows {
        let two_way = r.proof.by.starts_with("end to end");
        let mut push = |what: String, by_two_way: bool| {
            out.push(Gap {
                criterion: r.criterion.clone(),
                what,
                by_two_way,
            })
        };
        if r.code.is_empty() {
            push("no change in the diff is traced to it".into(), false);
        }
        if r.tests.is_empty() {
            push("no test on the head covers it".into(), false);
            continue;
        }
        match r.proof.state {
            State::Proven => {}
            State::NotRun => push(
                "its tests haven't run with and without the change on this head".into(),
                false,
            ),
            s => push(
                if r.proof.detail.is_empty() {
                    s.words().to_string()
                } else {
                    r.proof.detail.clone()
                },
                two_way,
            ),
        }
    }
    out
}

/// What goes back to the coder for the trace's gaps: one instruction per
/// gap the two-way proof doesn't already send (`e2e_proof::coder_items`),
/// each starting with [`CODER_ITEM`]. A proof still running or a side that
/// couldn't run is not coder work.
pub fn coder_items(t: &Trace) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for r in &t.rows {
        // The two-way proof sends its own item for a test it found missing,
        // failing or not testing the ticket: only the untraced code is
        // the trace's to send for that criterion.
        let two_way = r.proof.by.starts_with("end to end");
        let c = r.criterion.trim();
        let item = if r.code.is_empty() {
            format!(
                "{CODER_ITEM} \"{c}\": no change in the diff is traced to it. Build it, or name the code that already does it in the plan's criteria map (`| code: <files or functions>`)."
            )
        } else if two_way {
            continue;
        } else if r.tests.is_empty() {
            format!(
                "{CODER_ITEM} \"{c}\": no test on the head covers it. Add a test that fails without the change and passes with it."
            )
        } else {
            match r.proof.state {
                State::Failing => format!(
                    "{CODER_ITEM} \"{c}\": its test fails with the change ({}). Fix the code, not the test.",
                    r.proof.detail
                ),
                State::NotTheTicket => format!(
                    "{CODER_ITEM} \"{c}\": its test passes without the change too ({}). Make it assert what the criterion asks, so it fails without the change.",
                    r.proof.detail
                ),
                State::Missing => format!(
                    "{CODER_ITEM} \"{c}\": no test of it on the head. Add one that fails without the change and passes with it."
                ),
                _ => continue,
            }
        };
        if !out.contains(&item) {
            out.push(item);
        }
    }
    out
}

/// How a trace item starts, so it is told from the two-way proof's.
pub const CODER_ITEM: &str = "Criteria trace:";

fn cell(s: &str) -> String {
    s.replace('|', "\\|").replace('\n', " ")
}

fn short(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// The trace as the description shows it: one row per criterion, compact.
/// Empty for no criteria.
pub fn table(t: &Trace) -> String {
    if t.rows.is_empty() {
        return String::new();
    }
    let head: String = t.head.chars().take(7).collect();
    let mut out = format!(
        "**Acceptance criteria, traced** (on `{head}`)\n\n| Criterion | Code | Tests | Proof |\n|---|---|---|---|\n"
    );
    for r in &t.rows {
        let crit = {
            let c = r.criterion.trim();
            if c.chars().count() > CELL_CHARS {
                format!("{}…", c.chars().take(CELL_CHARS).collect::<String>())
            } else {
                c.to_string()
            }
        };
        let code = if r.code.is_empty() {
            "none traced".to_string()
        } else {
            let mut parts: Vec<String> = r
                .code
                .iter()
                .take(3)
                .map(|l| match l.symbols.first() {
                    Some(s) => format!("`{}` (`{s}`)", short(&l.path)),
                    None => format!("`{}`", short(&l.path)),
                })
                .collect();
            if r.code.len() > 3 {
                parts.push(format!("+{}", r.code.len() - 3));
            }
            parts.join(", ")
        };
        let tests = if r.tests.is_empty() {
            "none".to_string()
        } else {
            let mut parts: Vec<String> = r
                .tests
                .iter()
                .take(2)
                .map(|t| {
                    let (spec, title) = crate::ci_repro::spec_and_title(t);
                    if title.is_empty() {
                        format!("`{}`", short(&spec))
                    } else {
                        format!("`{}` › {title}", short(&spec))
                    }
                })
                .collect();
            if r.tests.len() > 2 {
                parts.push(format!("+{}", r.tests.len() - 2));
            }
            parts.join(", ")
        };
        let proof = match (r.proof.state, r.proof.by.as_str()) {
            (State::Proven, b) if b.starts_with("end to end") => "proven both ways".to_string(),
            (State::Proven, _) => "proven: fails without the change".to_string(),
            (s, _) => s.words().to_string(),
        };
        out.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            cell(&crit),
            cell(&code),
            cell(&tests),
            cell(&proof)
        ));
    }
    out.trim_end().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci_repro::{Leg, Record};

    fn crit(text: &str, page: Option<&str>) -> Criterion {
        Criterion {
            text: text.into(),
            page: page.map(str::to_string),
            visible: page.is_some(),
        }
    }

    fn file(path: &str, patch: &str) -> (String, String, String) {
        (path.into(), "modified".into(), patch.into())
    }

    fn files() -> Vec<(String, String, String)> {
        vec![
            file(
                "apps/web/src/utils/filterMaps.ts",
                "@@ -1,3 +1,9 @@ export function slugToId(\n+export function slugToIds(slug: string, rows: Row[]) {\n+  // every berth row sharing the slug\n+}\n",
            ),
            file(
                "apps/web/src/components/BerthsFilter.tsx",
                "@@ -10 +10,2 @@\n+  const chips = dedupeBerthsBySlug(berths);\n+export const dedupeBerthsBySlug = (s) => s;\n",
            ),
            file(
                "apps/web/src/lib/analytics.ts",
                "@@ -1 +1 @@\n+export function trackPageView(page: string) {}\n",
            ),
            file(
                "e2e/slips.spec.ts",
                "@@ -0,0 +1,5 @@\n+test('shows one guest-berth filter chip', async ({ page }) => {\n+  await page.goto('/club/slips');\n+});\n",
            ),
        ]
    }

    fn map() -> Vec<MapLine> {
        e2e_proof::parse_map(
            "## Criteria map\n- Only one guest-berth filter chip shows | page: /club/slips | e2e: e2e/slips.spec.ts › shows one guest-berth filter chip | asserts: one Guest-Berth chip | code: filterMaps.ts, dedupeBerthsBySlug\n- Page views are tracked on the slips page | page: /club/slips | e2e: none: an analytics call | asserts: a page view event\n",
        )
    }

    fn proven(criterion: &str, head: &str) -> e2e_proof::Proof {
        e2e_proof::Proof {
            criterion: criterion.into(),
            page: "/club/slips".into(),
            test: "e2e/slips.spec.ts › shows one guest-berth filter chip".into(),
            record: Record {
                head: head.into(),
                base: Leg {
                    ran: true,
                    failed: 1,
                    ..Default::default()
                },
                change: Leg {
                    ran: true,
                    passed: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn each_criterion_links_its_code_tests_and_proof() {
        let criteria = vec![
            crit(
                "Only one guest-berth filter chip shows",
                Some("/club/slips"),
            ),
            crit("Page views are tracked on the slips page", None),
        ];
        let m = map();
        let f = files();
        let proofs = vec![proven(
            "Only one guest-berth filter chip shows",
            "abc1234def",
        )];
        let t = build(&Inputs {
            head: "abc1234def",
            criteria: &criteria,
            from_plan: false,
            map: &m,
            files: &f,
            proofs: &proofs,
            test_runs: &[],
        });
        let chip = &t.rows[0];
        let paths: Vec<&str> = chip.code.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "apps/web/src/utils/filterMaps.ts",
                "apps/web/src/components/BerthsFilter.tsx"
            ]
        );
        assert_eq!(chip.code[0].symbols, vec!["slugToId", "slugToIds"]);
        assert_eq!(
            chip.tests,
            vec!["e2e/slips.spec.ts › shows one guest-berth filter chip"]
        );
        assert_eq!(chip.proof.state, State::Proven);
        // The second criterion: its code by its words, no test, an open gap.
        let views = &t.rows[1];
        assert_eq!(views.code[0].path, "apps/web/src/lib/analytics.ts");
        assert!(views.tests.is_empty());
        assert_eq!(views.proof.state, State::Missing);
        let g = gaps(&t);
        assert_eq!(g.len(), 1, "{g:?}");
        assert_eq!(g[0].criterion, "Page views are tracked on the slips page");
        assert!(g[0].what.contains("no test"));
        let tbl = table(&t);
        assert!(tbl.contains("| Criterion | Code | Tests | Proof |"));
        assert!(tbl.contains("`filterMaps.ts` (`slugToId`)"));
        assert!(tbl.contains("proven both ways"));
        assert!(tbl.contains("| none | no test |"));
        assert_eq!(Trace::from_json(&t.to_json()), Some(t));
    }

    #[test]
    fn a_proof_of_another_head_does_not_count_and_tests_runs_do() {
        let criteria = vec![crit("Only one guest-berth filter chip shows", None)];
        let m = map();
        let f = files();
        let old = vec![proven("Only one guest-berth filter chip shows", "0000000")];
        let runs = vec![TestRun {
            path: "e2e/slips.spec.ts".into(),
            state: State::NotTheTicket,
            detail: String::new(),
        }];
        let t = build(&Inputs {
            head: "abc1234",
            criteria: &criteria,
            from_plan: true,
            map: &m,
            files: &f,
            proofs: &old,
            test_runs: &runs,
        });
        assert_eq!(t.rows[0].source, "plan");
        assert_eq!(t.rows[0].proof.state, State::NotTheTicket);
        assert_eq!(t.rows[0].proof.by, "tests with and without the change");
        let g = gaps(&t);
        assert_eq!(g.len(), 1);
        assert!(!g[0].by_two_way);
        // A criterion nothing in the diff serves, with several criteria.
        let criteria = vec![
            crit("Only one guest-berth filter chip shows", None),
            crit("Refunds are emailed within a day", None),
        ];
        let t = build(&Inputs {
            head: "abc1234",
            criteria: &criteria,
            from_plan: false,
            map: &[],
            files: &f,
            proofs: &[],
            test_runs: &[],
        });
        let refunds = &t.rows[1];
        assert!(refunds.code.is_empty() && refunds.tests.is_empty());
        let g = gaps(&t);
        assert!(g
            .iter()
            .any(|x| x.criterion.starts_with("Refunds") && x.what.contains("no change")));
        assert!(table(&t).contains("none traced"));
    }

    #[test]
    fn one_criterion_owns_the_whole_change() {
        let criteria = vec![crit("The export retries a timed out poll", None)];
        let f = vec![
            file("src/poller.rs", "@@ -1 +1 @@\n+pub fn backoff() {}\n"),
            file(
                "src/poller_test.rs",
                "@@ -1 +1 @@\n+fn gives_up_after_three() {}\n",
            ),
        ];
        let t = build(&Inputs {
            head: "h",
            criteria: &criteria,
            from_plan: false,
            map: &[],
            files: &f,
            proofs: &[],
            test_runs: &[TestRun {
                path: "src/poller_test.rs".into(),
                state: State::Proven,
                detail: String::new(),
            }],
        });
        assert_eq!(t.rows[0].code[0].path, "src/poller.rs");
        assert_eq!(t.rows[0].tests, vec!["src/poller_test.rs"]);
        assert_eq!(t.rows[0].proof.state, State::Proven);
        assert!(gaps(&t).is_empty());
        assert!(table(&t).contains("proven: fails without the change"));
    }

    #[test]
    fn definitions_are_read_from_added_lines_and_hunk_headers() {
        let p = "@@ -40,6 +40,8 @@ impl Poller {\n+    pub async fn poll_once(&self) {\n@@ -90 +92 @@ def handle(event):\n+def retry_later(job):\n+class Backoff:\n+func (p *Poller) Next() int {\n+  const x = 1;\n";
        assert_eq!(
            symbols(p),
            vec!["poll_once", "handle", "retry_later", "Backoff", "Next"]
        );
        assert!(is_test_path("src/__tests__/a.ts") && is_test_path("pkg/a_test.go"));
        assert!(!is_test_path("src/attest.ts"));
    }
}
