//! Each thing the ticket asks for is proven end to end, both ways.
//!
//! A ticket asked for one thing: one sport chip, not two, on one venue's
//! summer camps page. The run wrote about 530 lines of unit tests over
//! three new helpers and never loaded the page the ticket names. Its unit
//! tests passed before and after; nothing showed the chip was ever doubled
//! or that it is single now.
//!
//! The rules, each enforced in code:
//!
//! - **The plan maps each criterion to a page and what a person sees
//!   there** ([`criteria`], [`parse_map`], [`map_gaps`]). The criteria are
//!   the ticket's acceptance list, else its Expected lines; each carries
//!   the page it names, or the page the ticket's steps name. A criterion
//!   that names a page or a visible behaviour needs a line under
//!   [`MAP_HEADING`] with the page, an end-to-end spec in the repository's
//!   suite and the assertion. Unit tests may go with it; they never stand
//!   in for it. The plan goes back without it.
//! - **Each criterion's test runs both ways before the pull request is
//!   called done** ([`Proof`], [`outcome`]): on the base branch's
//!   deployment (staging) it must fail, which shows the bug; on the pull
//!   request's preview it must pass, which shows the fix. Both results are
//!   kept on the run ([`FIELD`]) and go into the pull request.
//!   - Passing on both: the test doesn't test the ticket, so it goes back
//!     to the coder ([`Outcome::NotTheTicket`]).
//!   - Failing on the preview: the change doesn't do it yet, coder work
//!     ([`Outcome::CoderWork`]).
//!   - No test for the criterion's page on the head: coder work
//!     ([`Outcome::Missing`]).
//!   - A side that couldn't run (no browser, no network, no address):
//!     labelled unverified with the reason ([`Outcome::Unverified`]), never
//!     a silent pass.

use serde::{Deserialize, Serialize};

use crate::ci_repro::{Leg, Record};

/// The run record attribute that keeps the proofs, as JSON.
pub const FIELD: &str = "e2e_proof";

/// The plan's section that maps each criterion to its end-to-end proof.
pub const MAP_HEADING: &str = "## Criteria map";

/// What the planner is told.
pub const PLANNER: &str = "\n\n## The criteria map (CoderHelm checks it)\n\
Start from the ticket's acceptance criteria (or its Expected lines) and plan how each one is \
proven end to end. Write a `## Criteria map` section in design.md with one line per criterion:\n\
`- <the criterion> | page: <the page or URL path it is about> | e2e: <spec path in the \
end-to-end suite> › <test title> | asserts: <what a person sees there>`\n\
- A criterion that names a page or something a person sees gets an end-to-end test that loads \
that page and asserts it. Unit tests may go with it but never replace it.\n\
- The test must fail on the base branch's deployment (staging) today, which shows the bug, and \
pass on this pull request's preview, which shows the fix. CoderHelm runs it both ways before \
the pull request is done; a test that passes on both doesn't test the ticket.\n\
- A criterion with nothing a person sees (a job, an API's data) says `e2e: none: <why>` and \
names the check that proves it instead.\n";

// ── The criteria ───────────────────────────────────────────────────────────

/// One thing the ticket asks for.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Criterion {
    pub text: String,
    /// The site path it is about (`/venue/summer-camps`), its own or the
    /// page the ticket's steps name.
    pub page: Option<String>,
    /// It describes something a person sees on a page.
    pub visible: bool,
}

impl Criterion {
    /// Whether it needs an end-to-end test.
    pub fn needs_e2e(&self) -> bool {
        self.visible || self.page.is_some()
    }

    /// Whether it is about what the server sends, before any script runs
    /// in the browser: the page's source, its server HTML, the page with
    /// scripts turned off, what a crawler reads. A browser test runs the
    /// page's scripts and can pass while the server sends none of it, so
    /// such a criterion is also proven on the served page itself
    /// ([`Served`]).
    pub fn about_served(&self) -> bool {
        let t = self.text.to_ascii_lowercase();
        SERVED_WORDS.iter().any(|w| t.contains(w))
    }
}

/// Words that say a criterion is about the served page ([`Criterion::about_served`]).
const SERVED_WORDS: &[&str] = &[
    "server html",
    "server-rendered",
    "server rendered",
    "server-side render",
    "server side render",
    "rendered on the server",
    "page source",
    "view source",
    "view-source",
    "raw html",
    "initial html",
    "html response",
    "curl",
    "wget",
    "without javascript",
    "without js",
    "javascript off",
    "javascript turned off",
    "javascript disabled",
    "javascript is off",
    "javascript is disabled",
    "js off",
    "js disabled",
    "js turned off",
    "no-js",
    "no javascript",
    "noscript",
    "crawler",
    "search engine",
    "seo",
    "json-ld",
    "structured data",
];

/// The criteria to prove: the ticket's own ([`criteria`]), else the lines
/// of the plan's criteria map. A run whose ticket text gives no criteria
/// (an empty issue body, a ticket whose criteria only the plan wrote down)
/// is still proven on what the plan said it would do, never waved through
/// with nothing to prove.
pub fn criteria_or_map(
    title: &str,
    body: &str,
    acceptance: &[String],
    map: &[MapLine],
) -> Vec<Criterion> {
    let from_ticket = criteria(title, body, acceptance);
    if from_ticket.iter().any(Criterion::needs_e2e) {
        return from_ticket;
    }
    let from_map: Vec<Criterion> = map
        .iter()
        .filter(|m| !m.criterion.trim().is_empty())
        .map(|m| {
            let page = Some(m.page.trim())
                .filter(|p| !p.is_empty())
                .map(|p| pages(p).into_iter().next().unwrap_or_else(|| p.to_string()));
            Criterion {
                text: m.criterion.trim().to_string(),
                visible: page.is_some() || is_visible(&m.criterion),
                page,
            }
        })
        .collect();
    if from_map.is_empty() {
        from_ticket
    } else {
        from_map
    }
}

/// Section names plain ticket text uses without any mark.
const HEADINGS: &[&str] = &[
    "steps to reproduce",
    "steps",
    "repro steps",
    "observed",
    "observed behavior",
    "observed behaviour",
    "actual",
    "actual result",
    "actual results",
    "actual behavior",
    "actual behaviour",
    "current behavior",
    "current behaviour",
    "expected",
    "expected result",
    "expected results",
    "expected behavior",
    "expected behaviour",
    "notes",
    "environment",
    "screenshots",
    "description",
    "acceptance criteria",
    "context",
    "background",
    "summary",
];

fn heading(line: &str) -> Option<String> {
    let t = line
        .trim()
        .trim_start_matches('#')
        .trim_matches(|c: char| c == '*' || c == '_' || c == ':' || c.is_whitespace())
        .to_ascii_lowercase();
    HEADINGS.contains(&t.as_str()).then_some(t)
}

/// The lines under the ticket's Expected heading, up to the next heading.
pub fn expected(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut under = false;
    for line in body.lines() {
        if let Some(h) = heading(line) {
            under = h.starts_with("expected");
            continue;
        }
        if !under {
            continue;
        }
        let t = line
            .trim()
            .trim_start_matches(['-', '*'])
            .trim_start_matches("[ ]")
            .trim();
        if !t.is_empty() {
            out.push(t.to_string());
        }
    }
    out
}

const CODE_EXT: &[&str] = &[
    ".ts", ".tsx", ".js", ".jsx", ".mjs", ".py", ".rs", ".go", ".rb", ".json", ".md", ".css",
    ".scss", ".yml", ".yaml", ".sql", ".png", ".jpg", ".svg", ".html",
];

fn path_ok(p: &str) -> bool {
    let low = p.to_ascii_lowercase();
    p.len() > 1
        && p.starts_with('/')
        && !p.starts_with("//")
        && p.chars().any(|c| c.is_ascii_alphabetic())
        && p.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '_' | '.'))
        && !CODE_EXT.iter().any(|e| low.ends_with(e))
}

/// The site paths `text` names: `/venue/summer-camps`, or the path of a
/// full address, without its query.
pub fn pages(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for tok in text.split(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                '(' | ')' | '[' | ']' | '<' | '>' | ',' | '"' | '\'' | '`'
            )
    }) {
        let tok = tok.trim_end_matches(['.', ',', ';', ':', '!', '?']);
        let path = if let Some(rest) = tok
            .strip_prefix("https://")
            .or_else(|| tok.strip_prefix("http://"))
        {
            match rest.find('/') {
                Some(i) => rest[i..].to_string(),
                None => continue,
            }
        } else if tok.starts_with('/') {
            tok.to_string()
        } else {
            continue;
        };
        let path = path
            .split(['?', '#'])
            .next()
            .unwrap_or("")
            .trim_end_matches('/')
            .to_string();
        if path_ok(&path) && !out.contains(&path) {
            out.push(path);
        }
    }
    out
}

const VISIBLE: &[&str] = &[
    "show",
    "shows",
    "shown",
    "showing",
    "display",
    "displays",
    "displayed",
    "visible",
    "appear",
    "appears",
    "see",
    "sees",
    "button",
    "buttons",
    "filter",
    "filters",
    "chip",
    "chips",
    "link",
    "links",
    "menu",
    "page",
    "drawer",
    "banner",
    "label",
    "heading",
    "title",
    "scroll",
    "scrolls",
    "click",
    "clicks",
    "render",
    "renders",
    "modal",
    "popup",
    "tab",
    "icon",
    "image",
    "card",
    "cards",
    "carousel",
    "dropdown",
    "form",
    "pill",
    "pills",
];

/// Whether `text` describes something a person sees.
pub fn is_visible(text: &str) -> bool {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .any(|w| VISIBLE.contains(&w.to_ascii_lowercase().as_str()))
}

/// The ticket's criteria: its acceptance list (`acceptance`, as the stop
/// judge reads it) when it has one, else its Expected lines, else the
/// title when the ticket names a page. Each carries the page it names, or
/// the ticket's own page when it describes something a person sees.
pub fn criteria(title: &str, body: &str, acceptance: &[String]) -> Vec<Criterion> {
    let ticket_page = pages(&format!("{title}\n{body}")).into_iter().next();
    let mut items: Vec<String> = acceptance.to_vec();
    if items.is_empty() {
        items = expected(body);
    }
    if items.is_empty() && ticket_page.is_some() && !title.trim().is_empty() {
        items.push(title.trim().to_string());
    }
    items
        .into_iter()
        .map(|text| {
            let visible = is_visible(&text);
            let page = pages(&text)
                .into_iter()
                .next()
                .or_else(|| ticket_page.clone().filter(|_| visible));
            Criterion {
                text,
                page,
                visible,
            }
        })
        .collect()
}

// ── The plan's map ─────────────────────────────────────────────────────────

/// One line of the plan's criteria map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MapLine {
    pub criterion: String,
    pub page: String,
    /// The spec path and the test title (`file › title`); empty when none.
    pub test: String,
    /// The reason it has no end-to-end test (`e2e: none: <why>`).
    pub none: Option<String>,
    pub asserts: String,
}

impl MapLine {
    /// The spec path of its test.
    pub fn spec(&self) -> String {
        crate::ci_repro::spec_and_title(&self.test).0
    }
}

/// The section `heading` of a markdown text, up to the next `## `.
fn section<'a>(text: &'a str, heading: &str) -> Option<&'a str> {
    let at = text.find(heading)?;
    let rest = &text[at + heading.len()..];
    let end = rest.find("\n## ").unwrap_or(rest.len());
    Some(&rest[..end])
}

/// The lines of the plan's [`MAP_HEADING`] section.
pub fn parse_map(design: &str) -> Vec<MapLine> {
    let Some(s) = section(design, MAP_HEADING) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for line in s.lines() {
        let t = line.trim();
        let Some(t) = t.strip_prefix("- ").or_else(|| t.strip_prefix("* ")) else {
            continue;
        };
        let mut parts = t.split(" | ");
        let mut m = MapLine {
            criterion: parts
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('`')
                .to_string(),
            ..Default::default()
        };
        for p in parts {
            let Some((k, v)) = p.split_once(':') else {
                continue;
            };
            let v = v.trim().trim_matches('`').trim().to_string();
            match k.trim().to_ascii_lowercase().as_str() {
                "page" | "url" => m.page = v,
                "e2e" | "test" => {
                    let low = v.to_ascii_lowercase();
                    if low == "none" || low.starts_with("none:") || low.starts_with("none ") {
                        m.none = Some(v[4..].trim_start_matches(':').trim().to_string());
                    } else {
                        // `>` is accepted for `›`.
                        m.test = v.replacen(" > ", " › ", 1);
                    }
                }
                "asserts" | "assert" | "expect" => m.asserts = v,
                _ => {}
            }
        }
        out.push(m);
    }
    out
}

/// Whether a path is an end-to-end spec, not a unit test: it sits in an
/// end-to-end folder (`e2e`, `playwright`, `cypress`) or is named as one.
pub fn is_e2e_path(path: &str) -> bool {
    let low = path.to_ascii_lowercase();
    let name = low.rsplit('/').next().unwrap_or(&low);
    low.split('/').any(|s| {
        matches!(
            s,
            "e2e" | "playwright" | "cypress" | "e2e-tests" | "tests-e2e"
        )
    }) || name.contains(".e2e.")
        || name.contains(".cy.")
}

/// Five-letter stems of the words worth matching.
fn stems(text: &str) -> Vec<String> {
    const SKIP: &[&str] = &[
        "should", "there", "which", "where", "these", "those", "after",
    ];
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .map(str::to_ascii_lowercase)
        .filter(|w| w.len() >= 4 && !SKIP.contains(&w.as_str()))
        .map(|w| w.chars().take(5).collect())
        .collect()
}

fn overlap(a: &str, b: &str) -> usize {
    let sb = stems(b);
    let mut sa = stems(a);
    sa.dedup();
    sa.iter().filter(|w| sb.contains(w)).count()
}

/// The map line for a criterion: the one sharing the most words with it.
pub fn line_for<'a>(c: &Criterion, map: &'a [MapLine]) -> Option<&'a MapLine> {
    map.iter()
        .map(|m| (overlap(&c.text, &m.criterion), m))
        .filter(|(n, _)| *n > 0)
        .max_by_key(|(n, _)| *n)
        .map(|(_, m)| m)
}

/// What the plan's criteria map misses, one line each for its send-back.
/// `suite`: the repository has an end-to-end suite, so a criterion a person
/// sees can't go without a test in it.
pub fn map_gaps(criteria: &[Criterion], map: &[MapLine], suite: bool) -> Vec<String> {
    let needed: Vec<&Criterion> = criteria.iter().filter(|c| c.needs_e2e()).collect();
    if needed.is_empty() {
        return Vec::new();
    }
    let quote = |c: &Criterion| match &c.page {
        Some(p) => format!("\"{}\" (page `{p}`)", c.text),
        None => format!("\"{}\"", c.text),
    };
    if map.is_empty() {
        return vec![format!(
            "design.md has no `{MAP_HEADING}`, and the ticket's criteria name a page or what a person sees: {}. Write one line per criterion: `- <criterion> | page: <path> | e2e: <spec path> › <test title> | asserts: <what a person sees>`. The end-to-end test loads that page and asserts it; unit tests can go with it but never replace it.",
            needed.iter().map(|c| quote(c)).collect::<Vec<_>>().join("; ")
        )];
    }
    let mut out = Vec::new();
    for c in needed {
        let Some(m) = line_for(c, map) else {
            out.push(format!(
                "`{MAP_HEADING}` has no line for {}. Add one with the page, the end-to-end test that loads it and what it asserts.",
                quote(c)
            ));
            continue;
        };
        if m.test.trim().is_empty() {
            if suite {
                out.push(format!(
                    "{} plans no end-to-end test{}. Plan a test in the repository's end-to-end suite that loads the page and asserts what the criterion says; unit tests don't replace it.",
                    quote(c),
                    m.none
                        .as_ref()
                        .filter(|w| !w.is_empty())
                        .map(|w| format!(" (\"{w}\")"))
                        .unwrap_or_default()
                ));
            }
            continue;
        }
        if !is_e2e_path(&m.spec()) {
            out.push(format!(
                "{} is planned with `{}`, which isn't an end-to-end spec. Plan a test in the end-to-end suite that loads the page; a unit test can go with it but never replaces it.",
                quote(c),
                m.spec()
            ));
        }
        if c.page.is_some() && m.page.trim().is_empty() {
            out.push(format!(
                "The map line for {} names no page. Say which page its test loads.",
                quote(c)
            ));
        }
        if m.asserts.trim().is_empty() {
            out.push(format!(
                "The map line for {} says no assertion. Say what a person sees there when it works (`asserts: ...`).",
                quote(c)
            ));
        }
    }
    out
}

// ── Running each test both ways ────────────────────────────────────────────

/// One criterion's end-to-end test, run on staging and on the preview.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proof {
    pub criterion: String,
    #[serde(default)]
    pub page: String,
    /// `file › title`, empty when no test of it is on the head.
    #[serde(default)]
    pub test: String,
    /// The two runs: `base` on staging, `change` on the preview.
    pub record: Record,
    /// The criterion's page as the server sends it, no script run: on
    /// staging (`served_base`) and on the preview (`served_change`), for a
    /// criterion about the served page ([`Criterion::about_served`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_base: Option<Served>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_change: Option<Served>,
}

/// One fetch of a criterion's page as the server sends it (a plain GET, no
/// cookies, no script run), and what the judgment read in it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Served {
    /// The address fetched.
    #[serde(default)]
    pub url: String,
    /// The fetch got an answer.
    #[serde(default)]
    pub fetched: bool,
    #[serde(default)]
    pub status: u16,
    /// Whether the served page shows what the criterion asks: `None` when
    /// nothing could judge it.
    #[serde(default)]
    pub shown: Option<bool>,
    /// What in the served page settles it, quoted, or what is missing.
    #[serde(default)]
    pub quote: String,
    /// Why it wasn't fetched or judged.
    #[serde(default)]
    pub note: String,
}

impl Served {
    /// The server answered with a page.
    pub fn ok(&self) -> bool {
        self.fetched && (200..300).contains(&self.status)
    }

    fn why(&self) -> String {
        let q = self.quote.trim();
        let n = self.note.trim();
        let w = if !q.is_empty() { q } else { n };
        if w.is_empty() {
            "no result".to_string()
        } else {
            crate::truncate_str(w, 200).to_string()
        }
    }

    fn at(&self) -> String {
        if self.url.is_empty() {
            String::new()
        } else {
            format!(" ({})", self.url)
        }
    }
}

/// What the served-page legs show, when they settle the proof: `None` when
/// they are absent or couldn't judge it (the end-to-end test decides).
fn served_outcome(p: &Proof) -> Option<Outcome> {
    let change = p.served_change.as_ref()?;
    let what = format!("\"{}\" in the server HTML (no script run)", p.criterion);
    if change.fetched && !change.ok() {
        return Some(Outcome::CoderWork(format!(
            "{what}: the preview{} answered {}",
            change.at(),
            change.status
        )));
    }
    match change.shown {
        Some(false) => {
            return Some(Outcome::CoderWork(format!(
                "{what}: not on the preview{}: {}",
                change.at(),
                change.why()
            )))
        }
        Some(true) => {}
        None => {
            // Not judged: the test decides; with no test, it is unverified.
            return p.test.is_empty().then(|| {
                Outcome::Unverified(format!(
                    "{what}: the preview{} couldn't be judged: {}",
                    change.at(),
                    change.why()
                ))
            });
        }
    }
    // Shown on the preview: a failing browser test still sends work back.
    if !p.test.is_empty() && p.record.change.ran && p.record.change.red() {
        return None;
    }
    let base = p.served_base.as_ref();
    Some(match base.and_then(|b| b.shown.filter(|_| b.ok())) {
        Some(false) => Outcome::Proven(format!(
            "{what}: missing on staging{} and shown on the preview{}: {}",
            base.map(Served::at).unwrap_or_default(),
            change.at(),
            change.why()
        )),
        Some(true) => Outcome::Unverified(format!(
            "{what}: shown on the preview{} and already on staging{}, so staging doesn't show what the ticket says is wrong",
            change.at(),
            base.map(Served::at).unwrap_or_default()
        )),
        None => Outcome::Unverified(format!(
            "{what}: shown on the preview{}, staging couldn't be read{}",
            change.at(),
            base.map(|b| format!(": {}", b.why())).unwrap_or_default()
        )),
    })
}

/// Characters of a served page a judgment reads.
pub const SERVED_VIEW_CHARS: usize = 60_000;

/// The served page as a judgment reads it: its markup with scripts and
/// styles taken out (structured data, `application/ld+json`, kept: the
/// server sends it for crawlers), whitespace collapsed, bounded.
pub fn served_view(html: &str) -> String {
    let mut out = String::with_capacity(html.len().min(SERVED_VIEW_CHARS));
    let low = html.to_ascii_lowercase();
    let mut i = 0;
    while i < html.len() {
        let rest = &low[i..];
        let skip_tag = ["<script", "<style"]
            .iter()
            .find(|t| rest.starts_with(**t))
            .copied();
        if let Some(tag) = skip_tag {
            let open_end = rest.find('>').map(|e| i + e + 1).unwrap_or(html.len());
            let open = &low[i..open_end];
            let close = if tag == "<script" {
                "</script>"
            } else {
                "</style>"
            };
            let end = low[open_end..]
                .find(close)
                .map(|e| open_end + e + close.len())
                .unwrap_or(html.len());
            if tag == "<script" && open.contains("ld+json") {
                out.push_str(&html[i..end]);
            }
            i = end;
            continue;
        }
        let c = html[i..].chars().next().unwrap_or(' ');
        if c.is_whitespace() {
            if !out.ends_with(' ') {
                out.push(' ');
            }
        } else {
            out.push(c);
        }
        i += c.len_utf8();
    }
    crate::truncate_str(out.trim(), SERVED_VIEW_CHARS).to_string()
}

/// What a proof shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// It fails on staging and passes on the preview.
    Proven(String),
    /// It passes on both: it doesn't test what the ticket says is wrong.
    NotTheTicket(String),
    /// It fails on the preview: the change doesn't do it yet.
    CoderWork(String),
    /// The head has no end-to-end test of the criterion.
    Missing(String),
    /// A side couldn't run: unverified, with why.
    Unverified(String),
}

impl Outcome {
    /// The coder has work to do for it.
    pub fn is_coder_work(&self) -> bool {
        matches!(
            self,
            Outcome::NotTheTicket(_) | Outcome::CoderWork(_) | Outcome::Missing(_)
        )
    }
}

fn leg_why(l: &Leg) -> String {
    let n = l.note.trim();
    if n.is_empty() {
        "no result".to_string()
    } else {
        crate::truncate_str(n, 200).to_string()
    }
}

fn at(l: &Leg) -> String {
    if l.target.is_empty() {
        String::new()
    } else {
        format!(" ({})", l.target)
    }
}

/// What the two runs show (see the module's rules).
pub fn outcome(p: &Proof) -> Outcome {
    if let Some(o) = served_outcome(p) {
        return o;
    }
    let (base, change) = (&p.record.base, &p.record.change);
    let what = if p.test.is_empty() {
        format!("\"{}\"", p.criterion)
    } else {
        format!("`{}`", p.test)
    };
    if p.test.is_empty() {
        return Outcome::Missing(format!(
            "\"{}\" has no end-to-end test{} on this head",
            p.criterion,
            if p.page.is_empty() {
                String::new()
            } else {
                format!(" that loads `{}`", p.page)
            }
        ));
    }
    if !change.ran {
        return Outcome::Unverified(format!(
            "{what} didn't run on the preview{}: {}",
            at(change),
            leg_why(change)
        ));
    }
    if change.red() {
        return Outcome::CoderWork(format!(
            "{what} fails on the preview{}: {}",
            at(change),
            leg_why(change)
        ));
    }
    if !base.ran {
        return Outcome::Unverified(format!(
            "{what} passes on the preview{} but didn't run on staging{}: {}",
            at(change),
            at(base),
            leg_why(base)
        ));
    }
    if base.red() {
        return Outcome::Proven(format!(
            "{what} fails on staging{} ({} failed) and passes on the preview{} ({} passed)",
            at(base),
            base.failed,
            at(change),
            change.passed
        ));
    }
    if base.green() {
        return Outcome::NotTheTicket(format!(
            "{what} passes on staging{} too, so it doesn't show what the ticket says is wrong",
            at(base)
        ));
    }
    Outcome::Unverified(format!("{what}: staging gave no count"))
}

impl Proof {
    /// One line for the run page and the pull request.
    pub fn line(&self) -> String {
        match outcome(self) {
            Outcome::Proven(w) => format!("Proven both ways: \"{}\": {w}.", self.criterion),
            Outcome::NotTheTicket(w) => {
                format!("Doesn't test the ticket: \"{}\": {w}.", self.criterion)
            }
            Outcome::CoderWork(w) => format!("Not done yet: \"{}\": {w}.", self.criterion),
            Outcome::Missing(w) => format!("No end-to-end test: {w}."),
            Outcome::Unverified(w) => format!("Unverified: \"{}\": {w}.", self.criterion),
        }
    }

    /// What the coder is told to do about it, when anything.
    pub fn instruction(&self) -> Option<String> {
        let page = if self.page.is_empty() {
            "the page".to_string()
        } else {
            format!("`{}`", self.page)
        };
        match outcome(self) {
            Outcome::NotTheTicket(w) => Some(format!(
                "Two-way proof: {w}. Make the test assert what the ticket says is wrong on {page} (\"{}\"), so it fails on staging and passes on this preview.",
                self.criterion
            )),
            Outcome::CoderWork(w) if self.served_change.is_some() => Some(format!(
                "Two-way proof: {w}. The criterion is about what the server sends, so a person (or a crawler) without scripts must get it from {page} as served: fix the code (not the test) so the server renders it, then fetch the preview's page source to see it there.",
            )),
            Outcome::CoderWork(w) => Some(format!(
                "Two-way proof: {w}. The change doesn't do \"{}\" on the preview yet; fix the code (not the test) until it passes there.",
                self.criterion
            )),
            Outcome::Missing(w) => Some(format!(
                "Two-way proof: {w}. Add a test to the end-to-end suite that loads {page} and asserts \"{}\"; it must fail on staging and pass on this preview.",
                self.criterion
            )),
            _ => None,
        }
    }
}

/// The proofs made on `head`.
pub fn on_head<'a>(proofs: &'a [Proof], head: &str) -> Vec<&'a Proof> {
    proofs
        .iter()
        .filter(|p| crate::ci_repro::same_sha(&p.record.head, head))
        .collect()
}

/// What goes back to the coder for `head`.
pub fn coder_items(proofs: &[Proof], head: &str) -> Vec<String> {
    on_head(proofs, head)
        .into_iter()
        .filter_map(Proof::instruction)
        .collect()
}

/// The proofs of `head` a side of which couldn't run: (what, why).
pub fn unverified(proofs: &[Proof], head: &str) -> Vec<(String, String)> {
    on_head(proofs, head)
        .into_iter()
        .filter_map(|p| match outcome(p) {
            Outcome::Unverified(w) => {
                Some((format!("\"{}\" end to end, both ways", p.criterion), w))
            }
            _ => None,
        })
        .collect()
}

/// The proofs of `head` that held: (what, result).
pub fn proven(proofs: &[Proof], head: &str) -> Vec<(String, String)> {
    on_head(proofs, head)
        .into_iter()
        .filter_map(|p| match outcome(p) {
            Outcome::Proven(w) => Some((format!("\"{}\"", p.criterion), w)),
            _ => None,
        })
        .collect()
}

pub fn to_json(proofs: &[Proof]) -> String {
    serde_json::to_string(proofs).unwrap_or_default()
}

pub fn from_json(s: &str) -> Vec<Proof> {
    serde_json::from_str(s).unwrap_or_default()
}

/// `proofs` with `p` in place of an older one for its head and criterion;
/// the newest thirty kept.
pub fn upsert(proofs: &[Proof], p: Proof) -> Vec<Proof> {
    let mut out: Vec<Proof> = proofs
        .iter()
        .filter(|o| {
            !(crate::ci_repro::same_sha(&o.record.head, &p.record.head)
                && o.criterion == p.criterion)
        })
        .cloned()
        .collect();
    out.push(p);
    let n = out.len();
    if n > 30 {
        out.drain(..n - 30);
    }
    out
}

// ── Reading specs ──────────────────────────────────────────────────────────

/// The test titles a Playwright or Cypress spec declares
/// (`test("...")`, `it('...')`).
pub fn test_titles(spec: &str) -> Vec<String> {
    let mut out = Vec::new();
    for call in ["test(", "it(", "test.only("] {
        for (i, _) in spec.match_indices(call) {
            let before = spec[..i].chars().last();
            if before.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.') {
                continue;
            }
            let rest = spec[i + call.len()..].trim_start();
            let Some(q) = rest
                .chars()
                .next()
                .filter(|c| matches!(c, '"' | '\'' | '`'))
            else {
                continue;
            };
            if let Some(end) = rest[1..].find(q) {
                let t = rest[1..1 + end].trim();
                if !t.is_empty() && !out.iter().any(|o| o == t) {
                    out.push(t.to_string());
                }
            }
        }
    }
    out
}

/// Whether a spec's text loads `page` (a `goto` with its path, or its last
/// segment in a page object call).
pub fn loads_page(spec: &str, page: &str) -> bool {
    let page = page.trim_end_matches('/');
    if page.is_empty() {
        return false;
    }
    spec.contains("goto(") && spec.contains(page)
}

/// The test of `c` on the head, when the plan named none that is there:
/// a changed spec (`(path, text)`) that loads its page, and its test whose
/// title shares the most words with the criterion (the whole spec when
/// none does).
pub fn find_test(c: &Criterion, specs: &[(String, String)]) -> Option<String> {
    let page = c.page.as_deref()?;
    let (path, text) = specs.iter().find(|(_, t)| loads_page(t, page))?;
    let best = test_titles(text)
        .into_iter()
        .map(|t| (overlap(&c.text, &t), t))
        .filter(|(n, _)| *n > 0)
        .max_by_key(|(n, _)| *n)
        .map(|(_, t)| t);
    Some(match best {
        Some(t) => format!("{path} › {t}"),
        None => path.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TICKET: &str = "Steps to reproduce\nGo to /acme-club/summer-camps\nScroll to the sports filter\nObserved\nTwo Multi-Sport filters show\nExpected\nOnly one multi-sport filter to show\n";

    fn leg(ran: bool, passed: u32, failed: u32, note: &str) -> Leg {
        Leg {
            target: "https://x.example.com".into(),
            ran,
            passed,
            failed,
            note: note.into(),
        }
    }

    fn proof(test: &str, base: Leg, change: Leg) -> Proof {
        Proof {
            criterion: "Only one multi-sport filter to show".into(),
            page: "/acme-club/summer-camps".into(),
            test: test.into(),
            record: Record {
                head: "abc1234".into(),
                check: "two-way proof".into(),
                tests: vec![test.into()],
                base,
                change,
                source: "sandbox".into(),
                at: String::new(),
            },
            ..Default::default()
        }
    }

    #[test]
    fn criteria_come_from_expected_lines_with_the_tickets_page() {
        let c = criteria("Duplicate filter on the camps page", TICKET, &[]);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].text, "Only one multi-sport filter to show");
        assert_eq!(c[0].page.as_deref(), Some("/acme-club/summer-camps"));
        assert!(c[0].needs_e2e());
        // An acceptance list wins; a criterion's own page wins.
        let c = criteria(
            "x",
            TICKET,
            &[
                "The pricing table shows on https://acme.example.com/pricing?x=1".into(),
                "The nightly job logs its count".into(),
            ],
        );
        assert_eq!(c[0].page.as_deref(), Some("/pricing"));
        assert!(!c[1].needs_e2e());
        // A file path is no page.
        assert!(pages("see apps/web/src/a.tsx and /src/utils/filterMaps.ts").is_empty());
    }

    #[test]
    fn the_map_needs_an_end_to_end_test_of_the_page() {
        let c = criteria("Duplicate filter", TICKET, &[]);
        let unit_only = "## Proof\nunit tests\n";
        let g = map_gaps(&c, &parse_map(unit_only), true);
        assert_eq!(g.len(), 1);
        assert!(g[0].contains("no `## Criteria map`"), "{g:?}");
        let unit = "## Criteria map\n- Only one multi-sport filter shows | page: /acme-club/summer-camps | e2e: apps/web/src/test/filter.test.tsx › one chip | asserts: one chip\n";
        let g = map_gaps(&c, &parse_map(unit), true);
        assert!(g[0].contains("isn't an end-to-end spec"), "{g:?}");
        let none = "## Criteria map\n- Only one multi-sport filter shows | page: /acme-club/summer-camps | e2e: none: unit tests cover it | asserts: one chip\n";
        assert!(map_gaps(&c, &parse_map(none), true)[0].contains("plans no end-to-end test"));
        // Without a suite in the repository it goes to the two-way proof,
        // which labels it unverified.
        assert!(map_gaps(&c, &parse_map(none), false).is_empty());
        let good = "## Criteria map\n- Only one multi-sport filter shows | page: /acme-club/summer-camps | e2e: e2e/tests/camps.spec.ts › one multi-sport chip | asserts: exactly one Multi-Sport chip\n\n## Proof\nx";
        let m = parse_map(good);
        assert_eq!(m[0].test, "e2e/tests/camps.spec.ts › one multi-sport chip");
        assert!(map_gaps(&c, &m, true).is_empty());
        // Nothing a person sees needs nothing here.
        assert!(map_gaps(
            &criteria("x", "Expected\nThe job logs 3 rows\n", &[]),
            &[],
            true
        )
        .is_empty());
    }

    #[test]
    fn the_two_runs_decide() {
        let t = "e2e/tests/camps.spec.ts › one multi-sport chip";
        let p = proof(
            t,
            leg(true, 0, 1, "Expected: 1 Received: 2"),
            leg(true, 1, 0, ""),
        );
        assert!(matches!(outcome(&p), Outcome::Proven(_)));
        assert!(p.line().starts_with("Proven both ways"));
        let both = proof(t, leg(true, 1, 0, ""), leg(true, 1, 0, ""));
        assert!(matches!(outcome(&both), Outcome::NotTheTicket(_)));
        assert!(both
            .instruction()
            .unwrap()
            .contains("fails on staging and passes"));
        let red = proof(t, leg(true, 0, 1, ""), leg(true, 0, 1, "Received: 2"));
        assert!(matches!(outcome(&red), Outcome::CoderWork(_)));
        let no_browser = proof(
            t,
            leg(false, 0, 0, "browserType.launch: Executable doesn't exist"),
            leg(true, 1, 0, ""),
        );
        match outcome(&no_browser) {
            Outcome::Unverified(w) => assert!(w.contains("Executable doesn't exist"), "{w}"),
            o => panic!("{o:?}"),
        }
        assert!(no_browser.instruction().is_none());
        let missing = proof("", Leg::default(), Leg::default());
        assert!(matches!(outcome(&missing), Outcome::Missing(_)));
        let all = vec![p.clone(), both, no_browser];
        assert_eq!(coder_items(&all, "abc1234").len(), 1);
        assert_eq!(unverified(&all, "abc").len(), 1);
        assert_eq!(proven(&all, "abc1234").len(), 1);
        assert!(coder_items(&all, "def5678").is_empty());
        // One proof per head and criterion: the newest.
        let kept = upsert(&all, p.clone());
        assert_eq!(kept, vec![p.clone()]);
        assert_eq!(from_json(&to_json(&kept)), kept);
    }

    #[test]
    fn a_changed_spec_that_loads_the_page_is_its_test() {
        let spec = "test.describe('camps', () => {\n  test('shows one multi-sport chip', async ({ page }) => {\n    await page.goto('/acme-club/summer-camps');\n  });\n  it(\"other\", () => {});\n});\n";
        assert_eq!(
            test_titles(spec),
            vec!["shows one multi-sport chip", "other"]
        );
        let c = &criteria("x", TICKET, &[])[0];
        assert_eq!(
            find_test(c, &[("e2e/tests/camps.spec.ts".into(), spec.into())]).as_deref(),
            Some("e2e/tests/camps.spec.ts › shows one multi-sport chip")
        );
        assert!(find_test(c, &[("e2e/a.spec.ts".into(), "goto('/other')".into())]).is_none());
    }
}
