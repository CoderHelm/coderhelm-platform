//! The reviewer settles "no probe observed" itself, from what it can read
//! or run, instead of blocking on it.
//!
//! A pull request's end-to-end spec drove a vendor portal by its test ids.
//! The preview's end-to-end run exercised those ids against the live portal
//! and passed, and the review still blocked on "selectors no probe
//! observed": the probe gate (`common::e2e::probe::unprobed`) only read
//! probe records committed in the repository. The rules:
//!
//! - A passing end-to-end run on the head whose published report shows a
//!   test id is a record of that id ([`from_report`]): the same
//!   [`ProbeRecord`] the gate reads, so the gate decides as it does for a
//!   committed probe.
//! - For what is still unobserved, the reviewer runs the probe itself
//!   (the coder's `probe_page`, through the review's own page check) on the
//!   outside page the spec names, and decides from its record
//!   ([`decide`]): seen ids clear the gap, ids the page doesn't have are a
//!   verified defect quoted from the record, and a probe that couldn't run
//!   is a note with the reason and the probe to run, never a block on the
//!   reviewer's own missing look. On CoderHelm's own pull request that note
//!   is still work for the coder (`own_review::required`).

use common::e2e::probe::{ProbeRecord, Unprobed};

use super::review_agent::Finding;

/// A record of the test ids a passing end-to-end run's report showed, for
/// `origin`. `found` are the ids the report's text holds.
pub fn from_report(origin: &str, report: &str, head: &str, found: &[String]) -> ProbeRecord {
    ProbeRecord {
        origin: origin.to_string(),
        url: report.to_string(),
        at: format!("a passing end-to-end run on {}", &head[..head.len().min(7)]),
        testids: found.to_vec(),
        ..Default::default()
    }
}

/// The first https URL on `origin` in `code` (the page the spec drives).
pub fn page_url(code: &str, origin: &str) -> Option<String> {
    let want = format!("https://{}", origin.to_ascii_lowercase());
    for (i, _) in code.match_indices("https://") {
        let rest = &code[i..];
        let end = rest
            .find(|c: char| c.is_whitespace() || matches!(c, '\'' | '"' | '`' | ')' | '>' | ','))
            .unwrap_or(rest.len());
        let url = &rest[..end];
        let low = url.to_ascii_lowercase();
        if low == want
            || low.starts_with(&format!("{want}/"))
            || low.starts_with(&format!("{want}?"))
        {
            // A template's placeholder can't be loaded.
            if url.contains("${") {
                continue;
            }
            return Some(url.to_string());
        }
    }
    None
}

/// The record in the probe tool's answer: the JSON object after the
/// answer's first line (or after its record mark).
pub fn record_in_answer(answer: &str) -> Option<ProbeRecord> {
    let from = answer
        .find("Probe record:")
        .map(|i| i + "Probe record:".len())
        .or_else(|| answer.find('\n'))?;
    let rest = &answer[from..];
    let start = rest.find('{')?;
    serde_json::Deserializer::from_str(&rest[start..])
        .into_iter::<ProbeRecord>()
        .next()?
        .ok()
}

/// What the reviewer's own probe settled for one gap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probed {
    /// The probe ran; its record.
    Ran(ProbeRecord),
    /// It couldn't run (no page named, a refused host, the sandbox): why.
    NotRun(String),
}

/// The review's finding for a gap after the reviewer's own probe, if any.
/// The gate (`unprobed`) is asked again with the probe's record: ids it saw
/// clear the gap.
pub fn decide(gap: &Unprobed, full: &str, added: &str, probed: &Probed) -> Option<Finding> {
    match probed {
        Probed::Ran(record) => {
            let still = common::e2e::probe::unprobed(
                &[(gap.path.clone(), full.to_string(), added.to_string())],
                std::slice::from_ref(record),
                &[],
            );
            let missing: Vec<String> = still
                .iter()
                .filter(|g| g.origin.eq_ignore_ascii_case(&gap.origin))
                .flat_map(|g| g.missing.clone())
                .filter(|m| gap.missing.contains(m))
                .collect();
            if missing.is_empty() {
                return None;
            }
            let seen: Vec<String> = record.testids.iter().take(30).cloned().collect();
            let gap = Unprobed {
                missing: missing.clone(),
                ..gap.clone()
            };
            let mut f = super::review_rules::probe_findings(std::slice::from_ref(&gap))
                .into_iter()
                .next()?;
            f.title = format!("Test ids `{}` doesn't have", gap.origin);
            f.basis = Some("external".into());
            f.body = format!(
                "The reviewer probed {} ({}): {} {} not on the page. The page's test ids: {}.\n\n{}",
                record.url,
                record.at,
                missing
                    .iter()
                    .map(|m| format!("`{m}`"))
                    .collect::<Vec<_>>()
                    .join(", "),
                if missing.len() == 1 { "is" } else { "are" },
                if seen.is_empty() {
                    "none".to_string()
                } else {
                    seen.iter()
                        .map(|t| format!("`{t}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                },
                f.body,
            );
            Some(f)
        }
        Probed::NotRun(why) => Some(Finding {
            file: gap.path.clone(),
            severity: "medium".into(),
            category: "tests".into(),
            title: format!("Selectors on `{}` not yet observed", gap.origin),
            body: format!(
                "{} The reviewer's own probe couldn't run ({why}), and no passing end-to-end run \
                 on this head shows them. Run `probe_page` on the page the spec drives, or a \
                 passing end-to-end run that exercises them.",
                gap.body()
            ),
            evidence: Some("unverified-concern".into()),
            basis: Some("external".into()),
            caused_by_diff: Some(true),
            ..Default::default()
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_page_a_spec_drives_is_read_from_its_code() {
        let code = "const P = 'https://portal.example-vendor.io/org/1/portal/2';\nawait page.goto(`https://portal.example-vendor.io/${id}`)";
        assert_eq!(
            page_url(code, "portal.example-vendor.io").as_deref(),
            Some("https://portal.example-vendor.io/org/1/portal/2")
        );
        assert_eq!(page_url(code, "other.io"), None);
    }

    #[test]
    fn the_record_is_read_from_the_probe_tools_answer() {
        let main = "Probe of https://v.io/p. Commit this record at `.coderhelm/probes/v.io.json`:\n{\"origin\":\"v.io\",\"url\":\"https://v.io/p\",\"testids\":[\"a\"]}";
        assert_eq!(record_in_answer(main).unwrap().testids, vec!["a"]);
        let marked = "Probe of https://v.io/p. It is committed.\nseen: a\nProbe record:\n{\"origin\":\"v.io\",\"testids\":[\"b\"]}\n";
        assert_eq!(record_in_answer(marked).unwrap().testids, vec!["b"]);
        assert_eq!(record_in_answer("https://v.io/p: refused: no"), None);
    }
}
