//! End-to-end tests against a page another company serves are built from
//! what that page really has.
//!
//! A spec once clicked test ids and read JSON fields of a payment vendor's
//! page that nobody had looked at: they didn't exist, and the run said the
//! test "covered" the checkout. Later a portal spec checked "a camp goes in
//! the cart while registration is open" from strings found in the vendor's
//! script bundle: the ids were real, the behaviour wasn't (a signed-out
//! visitor gets the vendor's sign-in drawer and the cart stays at $0), and
//! the branch only ran the day the vendor opened registration. The rules:
//!
//! - [`probe_script`] loads the page in the sandbox with Playwright and
//!   lists its test ids (`data-testid`, `data-test`, `data-cy`, also inside
//!   frames) with each one's state (visible, enabled, its text), and the
//!   field names of the JSON it fetches. Given clicks (test ids or
//!   `/patterns/`), it clicks each in turn and lists what is there after
//!   it: the states a spec's branches depend on are seen, not guessed. The
//!   coder's `probe_page` tool runs it; the record is committed in the
//!   repository ([`record_path`]) and quoted in the pull request
//!   ([`quote`]).
//! - [`unprobed`] is the gate: an end-to-end file that reaches an outside
//!   origin (a host other than the team's own, or a page in a frame) may
//!   use only test ids and JSON fields a probe of that origin observed:
//!   the ones it adds and the ones it keeps. Test ids written over several
//!   lines or as patterns count like any other.

use serde::{Deserialize, Serialize};

/// What one test id looked like when the probe listed it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProbeState {
    pub testid: String,
    pub visible: bool,
    pub enabled: bool,
    /// Its first line of text, short.
    pub text: String,
}

/// What the page showed after one click.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProbeStep {
    /// The test id (or `/pattern/`) clicked.
    pub click: String,
    /// Whether something matched and was clicked.
    pub clicked: bool,
    pub states: Vec<ProbeState>,
}

/// What a probe of one page saw.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProbeRecord {
    /// The page's host.
    pub origin: String,
    pub url: String,
    /// When it was probed (RFC 3339).
    pub at: String,
    /// Test ids on the page and in its frames, on load and after each click.
    pub testids: Vec<String>,
    /// Field names of the JSON responses it fetched (dotted paths).
    pub json_fields: Vec<String>,
    /// Each test id's state on load.
    pub states: Vec<ProbeState>,
    /// What each click showed.
    pub steps: Vec<ProbeStep>,
}

/// The origin of end-to-end code that drives a page in a frame and names
/// no outside host: any probe's ids may cover it.
pub const FRAMED: &str = "a page in a frame";

/// Whether `selector` (a test id, or a `/pattern/` as a spec writes it)
/// names `testid`.
pub fn names(selector: &str, testid: &str) -> bool {
    match pattern(selector) {
        Some(re) => re.is_match(testid),
        None => selector == testid,
    }
}

fn pattern(selector: &str) -> Option<regex::Regex> {
    let s = selector.strip_prefix('/')?;
    let end = s.rfind('/')?;
    regex::Regex::new(&s[..end]).ok()
}

impl ProbeRecord {
    /// Every state the probe saw, on load and after each click, in order.
    pub fn all_states(&self) -> impl Iterator<Item = &ProbeState> {
        self.states
            .iter()
            .chain(self.steps.iter().flat_map(|s| s.states.iter()))
    }

    /// Whether the probe saw a test id `selector` names.
    pub fn observes(&self, selector: &str) -> bool {
        self.testids.iter().any(|t| names(selector, t))
            || self.all_states().any(|s| names(selector, &s.testid))
            || self.json_fields.iter().any(|f| f == selector)
    }

    /// The states the probe saw of what `selector` names.
    pub fn states_of<'a>(&'a self, selector: &'a str) -> impl Iterator<Item = &'a ProbeState> {
        self.all_states()
            .filter(move |s| names(selector, &s.testid))
    }
}

/// Where a probe of `host` is kept in the repository.
pub fn record_path(host: &str) -> String {
    let safe: String = host
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!(".coderhelm/probes/{safe}.json")
}

/// Whether a path is a probe record.
pub fn is_record(path: &str) -> bool {
    path.starts_with(".coderhelm/probes/") && path.ends_with(".json")
}

/// The line the probe tool's answer starts its record after.
pub const RECORD_MARK: &str = "Probe record:";

/// The record in a probe tool's answer, if it has one.
pub fn record_in(answer: &str) -> Option<ProbeRecord> {
    let at = answer.find(RECORD_MARK)?;
    let json = answer[at + RECORD_MARK.len()..].trim();
    serde_json::from_str(json).ok()
}

const SCRIPT: &str = r#"import { chromium } from 'playwright';
import { writeFileSync } from 'node:fs';
const out = process.env.CODERHELM_OUT || '.';
const url = __URL__;
const clicks = __CLICKS__;
const fields = new Set();
const walk = (v, p, d) => {
  if (d > 4 || v === null || typeof v !== 'object') return;
  for (const [k, x] of Object.entries(Array.isArray(v) ? (v[0] ? { '0': v[0] } : {}) : v)) {
    const key = Array.isArray(v) ? p : (p ? `${p}.${k}` : k);
    if (!Array.isArray(v)) fields.add(key);
    walk(x, key, d + 1);
  }
};
const SEL = '[data-testid],[data-test],[data-cy],[data-test-id]';
const idOf = (e) => e.getAttribute('data-testid') || e.getAttribute('data-test') || e.getAttribute('data-cy') || e.getAttribute('data-test-id');
const browser = await chromium.launch();
const page = await browser.newPage();
page.on('response', async (r) => {
  try {
    if ((r.headers()['content-type'] || '').includes('json')) walk(await r.json(), '', 0);
  } catch (e) {}
});
await page.goto(url, { waitUntil: 'networkidle', timeout: 45000 });
const settle = async () => {
  try { await page.waitForLoadState('networkidle', { timeout: 15000 }); } catch (e) {}
  await page.waitForTimeout(1500);
};
const snapshot = async () => {
  const seen = new Map();
  for (const f of page.frames()) {
    try {
      const got = await f.$$eval(SEL, (els) => els.map((e) => {
        const id = e.getAttribute('data-testid') || e.getAttribute('data-test') || e.getAttribute('data-cy') || e.getAttribute('data-test-id');
        const r = e.getBoundingClientRect();
        const st = getComputedStyle(e);
        const visible = r.width > 0 && r.height > 0 && st.visibility !== 'hidden' && st.display !== 'none';
        const enabled = !(e.disabled || e.getAttribute('aria-disabled') === 'true' || e.closest('[disabled],[aria-disabled="true"]'));
        const text = (e.innerText || '').split('\n')[0].trim().slice(0, 80);
        return { testid: id, visible, enabled, text };
      }));
      for (const s of got) if (s.testid && !seen.has(s.testid)) seen.set(s.testid, s);
    } catch (e) {}
  }
  return [...seen.values()];
};
const matches = (sel, id) => {
  if (sel.startsWith('/') && sel.lastIndexOf('/') > 0) {
    try { return new RegExp(sel.slice(1, sel.lastIndexOf('/'))).test(id); } catch (e) { return false; }
  }
  return sel === id;
};
await settle();
const states = await snapshot();
const steps = [];
for (const c of clicks) {
  let clicked = false;
  for (const f of page.frames()) {
    if (clicked) break;
    try {
      const handles = await f.$$(SEL);
      for (const h of handles) {
        const id = await h.evaluate(idOf);
        if (!id || !matches(c, id)) continue;
        if (!(await h.isVisible()) || !(await h.isEnabled())) continue;
        await h.click({ timeout: 10000 });
        clicked = true;
        break;
      }
    } catch (e) {}
  }
  await settle();
  steps.push({ click: c, clicked, states: await snapshot() });
}
await browser.close();
const ids = new Set();
for (const s of states) ids.add(s.testid);
for (const st of steps) for (const s of st.states) ids.add(s.testid);
writeFileSync(`${out}/probe.json`, JSON.stringify({
  origin: new URL(url).host, url, at: new Date().toISOString(),
  testids: [...ids].sort(), json_fields: [...fields].sort(), states, steps,
}, null, 2));
"#;

/// The sandbox script that probes `url`, clicking each of `clicks` (test
/// ids or `/patterns/`) in turn: it writes `probe.json` (a [`ProbeRecord`])
/// to `$CODERHELM_OUT`.
pub fn probe_script(url: &str, clicks: &[String]) -> String {
    let url = serde_json::to_string(url).unwrap_or_else(|_| "\"\"".into());
    let clicks = serde_json::to_string(clicks).unwrap_or_else(|_| "[]".into());
    SCRIPT
        .replace("__URL__", &url)
        .replace("__CLICKS__", &clicks)
}

/// The first quoted string or `/pattern/` at the start of `s` (after
/// whitespace), as the code writes it: a pattern keeps its slashes.
fn first_arg(s: &str) -> Option<String> {
    let t = s.trim_start();
    let q = t.chars().next()?;
    match q {
        '\'' | '"' | '`' => {
            let end = t[1..].find(q)?;
            let v = &t[1..1 + end];
            (!v.contains("${")).then(|| v.to_string())
        }
        '/' => {
            // A regex literal: up to the closing unescaped slash, flags dropped.
            let b = t.as_bytes();
            let mut i = 1;
            while i < b.len() {
                match b[i] {
                    b'\\' => i += 2,
                    b'/' => return Some(t[..=i].to_string()),
                    b'\n' => return None,
                    _ => i += 1,
                }
            }
            None
        }
        _ => None,
    }
}

/// The first argument of every `name(…)` call in `code`.
fn calls(code: &str, name: &str) -> Vec<String> {
    code.match_indices(name)
        .filter_map(|(i, _)| first_arg(&code[i + name.len()..]))
        .collect()
}

/// The test ids a piece of end-to-end code uses: `getByTestId(…)` with a
/// string or a pattern, on one line or several, and `[data-testid=…]`.
pub fn selectors_in(code: &str) -> Vec<String> {
    let mut out: Vec<String> = calls(code, "getByTestId(");
    for attr in ["data-testid=", "data-test=", "data-cy=", "data-test-id="] {
        out.extend(calls(code, attr));
    }
    out.retain(|s| !s.is_empty());
    out.sort();
    out.dedup();
    out
}

/// The JSON fields a piece of end-to-end code asserts on
/// (`toHaveProperty('a.b')`).
pub fn fields_in(code: &str) -> Vec<String> {
    let mut out = calls(code, "toHaveProperty(");
    out.retain(|s| !s.starts_with('/'));
    out.sort();
    out.dedup();
    out
}

/// Whether code drives a page inside a frame.
pub fn drives_frame(code: &str) -> bool {
    code.contains("frameLocator(") || code.contains(".contentFrame()")
}

/// The https hosts a piece of code reaches that aren't the team's own
/// (`own` hosts and their subdomains, previews included), and [`FRAMED`]
/// when it drives a frame and names no outside host.
pub fn outside_origins(code: &str, own: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (i, _) in code.match_indices("https://") {
        let host: String = code[i + 8..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
            .collect::<String>()
            .to_ascii_lowercase();
        if host.is_empty() || !host.contains('.') {
            continue;
        }
        let ours = own.iter().any(|o| {
            let o = o.to_ascii_lowercase();
            host == o || host.ends_with(&format!(".{o}"))
        }) || host.contains("preview")
            || host.contains("localhost")
            || host.ends_with("example.com");
        if !ours && !out.contains(&host) {
            out.push(host);
        }
    }
    if out.is_empty() && drives_frame(code) {
        out.push(FRAMED.to_string());
    }
    out
}

/// An e2e file's selectors or fields no probe observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unprobed {
    pub path: String,
    pub origin: String,
    pub missing: Vec<String>,
}

impl Unprobed {
    /// The review finding's text.
    pub fn body(&self) -> String {
        let record = if self.origin == FRAMED {
            record_path("<host of the framed page>")
        } else {
            record_path(&self.origin)
        };
        format!(
            "`{}` drives `{}`, a page another company serves, with {} that no probe of that page observed. Probe it first with the `probe_page` tool (on the preview or the vendor's URL, with `clicks` to reach the state each branch needs), commit the record at `{record}`, quote what it found in the pull request, and build the spec from the test ids and fields that are really there.",
            self.path,
            self.origin,
            self.missing
                .iter()
                .map(|m| format!("`{m}`"))
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

/// The gate: for each changed e2e file (path, its text at the head, its
/// added lines) that reaches an outside origin, the test ids and fields in
/// the file (the ones the change writes and the ones it keeps) that no
/// probe of that origin observed. A page in a frame with no host named is
/// covered by any probe.
pub fn unprobed(
    files: &[(String, String, String)],
    probes: &[ProbeRecord],
    own: &[String],
) -> Vec<Unprobed> {
    let mut out = Vec::new();
    for (path, full, _added) in files {
        let origins = outside_origins(full, own);
        if origins.is_empty() {
            continue;
        }
        let used: Vec<String> = selectors_in(full)
            .into_iter()
            .chain(fields_in(full))
            .collect();
        if used.is_empty() {
            continue;
        }
        for origin in origins {
            let seen: Vec<&ProbeRecord> = probes
                .iter()
                .filter(|p| origin == FRAMED || p.origin.eq_ignore_ascii_case(&origin))
                .collect();
            let missing: Vec<String> = used
                .iter()
                .filter(|u| !seen.iter().any(|p| p.observes(u)))
                .cloned()
                .collect();
            if !missing.is_empty() {
                out.push(Unprobed {
                    path: path.clone(),
                    origin,
                    missing,
                });
            }
        }
    }
    out
}

fn state_word(s: &ProbeState) -> String {
    let mut w = vec![if s.visible { "visible" } else { "hidden" }];
    if !s.enabled {
        w.push("disabled");
    }
    let text = if s.text.is_empty() {
        String::new()
    } else {
        format!(", \"{}\"", s.text.replace('`', "'"))
    };
    format!("`{}` ({}{text})", s.testid, w.join(", "))
}

const QUOTE_MAX: usize = 25;

fn listed(states: &[&ProbeState]) -> String {
    let mut parts: Vec<String> = states
        .iter()
        .take(QUOTE_MAX)
        .map(|s| state_word(s))
        .collect();
    if states.len() > QUOTE_MAX {
        parts.push(format!("and {} more", states.len() - QUOTE_MAX));
    }
    if parts.is_empty() {
        "nothing".to_string()
    } else {
        parts.join(", ")
    }
}

/// The pull request's lines quoting a probe: where and when, the visible
/// test ids and their states on load, and what each click brought up.
pub fn quote(r: &ProbeRecord) -> String {
    let mut out = format!(
        "- Probe of `{}` ({}) at {}: {} test ids.\n",
        r.origin,
        r.url,
        r.at,
        r.testids.len()
    );
    let on_load: Vec<&ProbeState> = r.states.iter().filter(|s| s.visible).collect();
    out.push_str(&format!("  - On load, visible: {}.\n", listed(&on_load)));
    let mut before: Vec<String> = r.states.iter().map(|s| s.testid.clone()).collect();
    for st in &r.steps {
        if !st.clicked {
            out.push_str(&format!(
                "  - Clicking `{}`: nothing visible and enabled matched.\n",
                st.click
            ));
            continue;
        }
        let new: Vec<&ProbeState> = st
            .states
            .iter()
            .filter(|s| s.visible && !before.contains(&s.testid))
            .collect();
        out.push_str(&format!(
            "  - After clicking `{}`, newly visible: {}.\n",
            st.click,
            listed(&new)
        ));
        before.extend(st.states.iter().map(|s| s.testid.clone()));
    }
    if !r.json_fields.is_empty() {
        out.push_str(&format!(
            "  - JSON fields fetched: {}.\n",
            r.json_fields
                .iter()
                .take(QUOTE_MAX)
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE_OBJECT: &str = r#"
export class VendorCheckout {
  url = 'https://checkout.vendor-pay.test/session';
  async pay() {
    await this.page.frameLocator('iframe').getByTestId('card-number').fill('4111');
    await this.page.getByTestId('pay-button').click();
    expect(await res.json()).toHaveProperty('payment.status');
  }
}
"#;

    #[test]
    fn selectors_on_an_outside_page_must_have_been_observed() {
        let files = vec![(
            "e2e/pages/vendor-checkout.ts".to_string(),
            PAGE_OBJECT.to_string(),
            PAGE_OBJECT.to_string(),
        )];
        let own = vec!["web.example.org".to_string()];
        // No probe at all: every selector and field is unobserved.
        let u = unprobed(&files, &[], &own);
        assert_eq!(u.len(), 1);
        assert_eq!(u[0].origin, "checkout.vendor-pay.test");
        assert_eq!(
            u[0].missing,
            vec!["card-number", "pay-button", "payment.status"]
        );
        assert!(u[0]
            .body()
            .contains(".coderhelm/probes/checkout.vendor-pay.test.json"));
        // A probe that saw two of them: the third is still flagged.
        let probe = ProbeRecord {
            origin: "checkout.vendor-pay.test".into(),
            testids: vec!["card-number".into(), "submit".into()],
            json_fields: vec!["payment.status".into()],
            ..Default::default()
        };
        let u = unprobed(&files, std::slice::from_ref(&probe), &own);
        assert_eq!(u[0].missing, vec!["pay-button"]);
        // Everything observed: nothing to flag.
        let full = ProbeRecord {
            testids: vec!["card-number".into(), "pay-button".into()],
            ..probe
        };
        assert!(unprobed(&files, &[full], &own).is_empty());
    }

    #[test]
    fn the_teams_own_pages_and_previews_need_no_probe() {
        let code = "await page.goto('https://pr-3.preview-web.example.org/');\nawait page.getByTestId('x').click();";
        let files = vec![(
            "e2e/a.spec.ts".to_string(),
            code.to_string(),
            code.to_string(),
        )];
        assert!(unprobed(&files, &[], &["example.org".to_string()]).is_empty());
        assert!(probe_script("https://a.test/x", &[]).contains("probe.json"));
    }

    /// Prettier breaks long calls over lines and specs match generated ids
    /// with patterns: both are selectors the gate reads.
    #[test]
    fn wrapped_calls_and_patterns_are_selectors() {
        let code = "this.frame.getByTestId(\n      \"portal-cart-subtotal\",\n    );\nthis.frame.getByTestId(\n  /^portal-option-guest-\\d+$/,\n);\nthis.frame.locator('[data-testid=\"drawer\"]');";
        assert_eq!(
            selectors_in(code),
            vec![
                "/^portal-option-guest-\\d+$/",
                "drawer",
                "portal-cart-subtotal"
            ]
        );
        let r = ProbeRecord {
            testids: vec!["portal-option-guest-0".into()],
            ..Default::default()
        };
        assert!(r.observes("/^portal-option-guest-\\d+$/"));
        assert!(!r.observes("/^portal-tag-guest-\\d+$/"));
    }

    /// Kept selectors count: the change touched the file, so what it
    /// leaves standing is held to the same probe.
    #[test]
    fn kept_selectors_in_a_changed_file_are_held_too() {
        let full = "const URL = 'https://portal.vendor.test/p/1';\nthis.f.getByTestId('kept-id');\nthis.f.getByTestId('new-id');";
        let files = vec![(
            "e2e/pages/portal.ts".to_string(),
            full.to_string(),
            "this.f.getByTestId('new-id');".to_string(),
        )];
        let u = unprobed(&files, &[], &[]);
        assert_eq!(u[0].missing, vec!["kept-id", "new-id"]);
    }

    /// A spec that drives a frame and names no host is still on a page
    /// another company serves.
    #[test]
    fn a_framed_page_with_no_host_named_needs_a_probe() {
        let code = "const f = page.frameLocator('iframe[id^=\"pay-link\"]');\nawait f.getByTestId('continue').click();";
        let files = vec![("e2e/pages/pay.ts".into(), code.into(), code.into())];
        let u = unprobed(&files, &[], &[]);
        assert_eq!(u[0].origin, FRAMED);
        let r = ProbeRecord {
            origin: "cdn.pay.test".into(),
            testids: vec!["continue".into()],
            ..Default::default()
        };
        assert!(unprobed(&files, &[r], &[]).is_empty());
    }

    #[test]
    fn a_probe_after_clicks_is_quoted_with_its_states() {
        let r = ProbeRecord {
            origin: "portal.vendor.test".into(),
            url: "https://portal.vendor.test/p/1".into(),
            at: "2026-11-17T15:40:00Z".into(),
            testids: vec!["week-0".into(), "option-0".into(), "sign-in-drawer".into()],
            states: vec![ProbeState {
                testid: "week-0".into(),
                visible: true,
                enabled: true,
                text: "Week 1".into(),
            }],
            steps: vec![
                ProbeStep {
                    click: "week-0".into(),
                    clicked: true,
                    states: vec![
                        ProbeState {
                            testid: "week-0".into(),
                            visible: true,
                            enabled: true,
                            ..Default::default()
                        },
                        ProbeState {
                            testid: "option-0".into(),
                            visible: true,
                            enabled: true,
                            text: "Mini camp (AM)".into(),
                        },
                    ],
                },
                ProbeStep {
                    click: "/^option-\\d+$/".into(),
                    clicked: true,
                    states: vec![ProbeState {
                        testid: "sign-in-drawer".into(),
                        visible: true,
                        enabled: true,
                        ..Default::default()
                    }],
                },
            ],
            ..Default::default()
        };
        let q = quote(&r);
        assert!(q.contains("Probe of `portal.vendor.test`"));
        assert!(q.contains(
            "After clicking `week-0`, newly visible: `option-0` (visible, \"Mini camp (AM)\")"
        ));
        assert!(q.contains("newly visible: `sign-in-drawer` (visible)"));
        let answer = format!(
            "Probe of x.\n{RECORD_MARK}\n{}",
            serde_json::to_string(&r).unwrap()
        );
        assert_eq!(record_in(&answer), Some(r));
        let script = probe_script("https://a.test/x", &["week-0".to_string()]);
        assert!(script.contains("const clicks = [\"week-0\"];"));
        assert!(!script.contains("__URL__"));
    }
}
