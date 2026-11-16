//! End-to-end tests against a page another company serves are built from
//! what that page really has.
//!
//! A spec once clicked test ids and read JSON fields of a payment vendor's
//! page that nobody had looked at: they didn't exist, and the run said the
//! test "covered" the checkout. The rules:
//!
//! - [`probe_script`] loads the page in the sandbox with Playwright and
//!   lists its test ids (`data-testid`, `data-test`, `data-cy`, also inside
//!   frames) and the field names of the JSON it fetches; the coder's
//!   `probe_page` tool runs it and the record is committed in the
//!   repository ([`record_path`]) and quoted in the pull request.
//! - [`unprobed`] is the gate: a new or changed end-to-end file that
//!   reaches an outside origin (a host other than the team's own) may use
//!   only test ids and JSON fields a probe of that origin observed. Any
//!   other is a must-fix finding of the review.

use serde::{Deserialize, Serialize};

/// What a probe of one page saw.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProbeRecord {
    /// The page's host.
    pub origin: String,
    pub url: String,
    /// When it was probed (RFC 3339).
    pub at: String,
    /// Test ids on the page and in its frames.
    pub testids: Vec<String>,
    /// Field names of the JSON responses it fetched (dotted paths).
    pub json_fields: Vec<String>,
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

/// The sandbox script that probes `url`: it writes `probe.json` (a
/// [`ProbeRecord`]) to `$CODERHELM_OUT`.
pub fn probe_script(url: &str) -> String {
    let url = serde_json::to_string(url).unwrap_or_else(|_| "\"\"".into());
    format!(
        r#"import {{ chromium }} from 'playwright';
import {{ writeFileSync }} from 'node:fs';
const out = process.env.CODERHELM_OUT || '.';
const url = {url};
const fields = new Set();
const walk = (v, p, d) => {{
  if (d > 4 || v === null || typeof v !== 'object') return;
  for (const [k, x] of Object.entries(Array.isArray(v) ? (v[0] ? {{ '0': v[0] }} : {{}}) : v)) {{
    const key = Array.isArray(v) ? p : (p ? `${{p}}.${{k}}` : k);
    if (!Array.isArray(v)) fields.add(key);
    walk(x, key, d + 1);
  }}
}};
const browser = await chromium.launch();
const page = await browser.newPage();
page.on('response', async (r) => {{
  try {{
    if ((r.headers()['content-type'] || '').includes('json')) walk(await r.json(), '', 0);
  }} catch (e) {{}}
}});
await page.goto(url, {{ waitUntil: 'networkidle', timeout: 45000 }});
const ids = new Set();
for (const f of page.frames()) {{
  try {{
    const got = await f.$$eval('[data-testid],[data-test],[data-cy],[data-test-id]', (els) =>
      els.map((e) => e.getAttribute('data-testid') || e.getAttribute('data-test') || e.getAttribute('data-cy') || e.getAttribute('data-test-id')));
    got.forEach((x) => x && ids.add(x));
  }} catch (e) {{}}
}}
await browser.close();
writeFileSync(`${{out}}/probe.json`, JSON.stringify({{
  origin: new URL(url).host, url, at: new Date().toISOString(),
  testids: [...ids].sort(), json_fields: [...fields].sort(),
}}, null, 2));
"#
    )
}

/// The quoted value of every `name('…')` call in `code`.
fn calls(code: &str, name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = code;
    while let Some(i) = rest.find(name) {
        let after = &rest[i + name.len()..];
        rest = after;
        let Some(q) = after
            .chars()
            .next()
            .filter(|c| matches!(c, '\'' | '"' | '`'))
        else {
            continue;
        };
        if let Some(end) = after[1..].find(q) {
            out.push(after[1..1 + end].to_string());
        }
    }
    out
}

/// The test ids a piece of end-to-end code uses.
pub fn selectors_in(code: &str) -> Vec<String> {
    let mut out: Vec<String> = calls(code, "getByTestId(");
    for attr in ["data-testid=", "data-test=", "data-cy=", "data-test-id="] {
        out.extend(calls(code, attr));
    }
    out.sort();
    out.dedup();
    out
}

/// The JSON fields a piece of end-to-end code asserts on
/// (`toHaveProperty('a.b')`).
pub fn fields_in(code: &str) -> Vec<String> {
    let mut out = calls(code, "toHaveProperty(");
    out.sort();
    out.dedup();
    out
}

/// The https hosts a piece of code reaches that aren't the team's own
/// (`own` hosts and their subdomains, previews included).
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
    out
}

/// A new or changed e2e file's selectors or fields no probe observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unprobed {
    pub path: String,
    pub origin: String,
    pub missing: Vec<String>,
}

impl Unprobed {
    /// The review finding's text.
    pub fn body(&self) -> String {
        format!(
            "`{}` drives `{}`, a page another company serves, with {} that no probe of that page observed. Probe it first with the `probe_page` tool (on the preview or the vendor's URL), commit the record at `{}`, quote what it found in the pull request, and build the spec from the test ids and fields that are really there.",
            self.path,
            self.origin,
            self.missing
                .iter()
                .map(|m| format!("`{m}`"))
                .collect::<Vec<_>>()
                .join(", "),
            record_path(&self.origin)
        )
    }
}

/// The gate: for each changed e2e file (path, its text at the head, its
/// added lines) that reaches an outside origin, the test ids and fields in
/// its added lines that no probe of that origin observed.
pub fn unprobed(
    files: &[(String, String, String)],
    probes: &[ProbeRecord],
    own: &[String],
) -> Vec<Unprobed> {
    let mut out = Vec::new();
    for (path, full, added) in files {
        let origins = outside_origins(full, own);
        if origins.is_empty() {
            continue;
        }
        let used: Vec<String> = selectors_in(added)
            .into_iter()
            .chain(fields_in(added))
            .collect();
        if used.is_empty() {
            continue;
        }
        for origin in origins {
            let seen: Vec<&ProbeRecord> = probes
                .iter()
                .filter(|p| p.origin.eq_ignore_ascii_case(&origin))
                .collect();
            let missing: Vec<String> = used
                .iter()
                .filter(|u| {
                    !seen.iter().any(|p| {
                        p.testids.iter().any(|t| t == *u) || p.json_fields.iter().any(|f| f == *u)
                    })
                })
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
        assert!(probe_script("https://a.test/x").contains("probe.json"));
    }
}
