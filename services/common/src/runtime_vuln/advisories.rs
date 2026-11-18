//! Runtime advisories: the OpenJDK Vulnerability Group's pages, with what
//! the Amazon Linux advisory for the same CVE and the dated notes add.
//!
//! The OpenJDK page is the source of record for which lines a CVE affects
//! and the last affected build of each ("The affected versions are
//! 27.0.1.0.3, 25.0.5.0.3, 21.0.13.0.3, 17.0.21.0.3, 11.0.33, 8u512, and
//! earlier", then one risk-matrix row per CVE with a bullet under each
//! affected line). It does not say where a line starts being affected, what
//! hardware the flaw needs, or a JVM flag that avoids it: the Amazon Linux
//! advisory's overview often does ("affecting OpenJDK 17.0.6 and later on
//! x86-64 systems ... ARM64 systems are not affected"), and its package list
//! gives a vendor's fixed build. [`NOTES`] holds what no feed carries in a
//! form that can be read, each entry dated and with its source.
//!
//! Every parser answers `Err` when the page no longer looks the way it did:
//! the caller keeps the last good copy.

use regex::Regex;
use serde::{Deserialize, Serialize};

use super::{java_cmp, java_line, java_numbers, Runtime};
use crate::infra_vuln::feeds::{text_of, Advisory, ParseError};

pub const OPENJDK_INDEX: &str = "https://openjdk.org/groups/vulnerability/advisories/";
/// The feed's status row key, and its host (the self-host docs list it).
pub const FEED: &str = "openjdk";
pub const HOST: &str = "openjdk.org";

pub fn openjdk_page(date: &str) -> String {
    format!("{OPENJDK_INDEX}{date}")
}

fn re(p: &str) -> Regex {
    Regex::new(p).expect("a valid pattern")
}

fn err<T>(m: impl Into<String>) -> Result<T, ParseError> {
    Err(ParseError(m.into()))
}

/// One affected line of a runtime.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LineRange {
    /// `21`.
    pub line: String,
    /// The first affected build, empty when the whole line is.
    pub introduced: String,
    /// The last affected build (`21.0.13.0.3`).
    pub last_affected: String,
    /// The first fixed upstream build, when a source names it (`21.0.13.1`).
    pub fixed: String,
    /// Vendors' fixed builds (`corretto 21.0.13.1.2`).
    pub vendor_fixed: Vec<String>,
}

/// What hardware an advisory's flaw needs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Hardware {
    /// Architectures it is exploitable on (`x86_64`); empty when any.
    pub arch: Vec<String>,
    /// Architectures a source says are not affected (`arm64`).
    pub not_arch: Vec<String>,
    /// A CPU feature it needs (`avx512_vbmi`), empty when not named.
    pub cpu_feature: String,
    /// The source's words.
    pub words: String,
}

impl Hardware {
    pub fn is_empty(&self) -> bool {
        self.arch.is_empty() && self.not_arch.is_empty() && self.cpu_feature.is_empty()
    }
}

/// One runtime CVE.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeAdvisory {
    /// `CVE-2026-114938`.
    pub id: String,
    pub runtime: Runtime,
    /// `hotspot/compiler`.
    pub component: String,
    pub cvss: Option<f64>,
    /// The page's vector shorthand (`NHNNUHHH`).
    pub vector: String,
    /// YYYY-MM-DD.
    pub published: String,
    pub url: String,
    pub lines: Vec<LineRange>,
    pub hardware: Hardware,
    /// JVM options that turn the flawed code off until the fix runs.
    pub mitigations: Vec<String>,
    /// What it is, in a line.
    pub summary: String,
    /// Every page that said something about it.
    pub sources: Vec<String>,
}

impl RuntimeAdvisory {
    pub fn line(&self, line: &str) -> Option<&LineRange> {
        self.lines.iter().find(|l| l.line == line)
    }

    /// A severity word from the score.
    pub fn severity(&self) -> &'static str {
        match self.cvss.unwrap_or(5.0) {
            s if s >= 9.0 => "critical",
            s if s >= 7.0 => "important",
            s if s >= 4.0 => "medium",
            _ => "low",
        }
    }
}

/// The dates of every advisory the index lists, newest first.
pub fn parse_openjdk_index(html: &str) -> Result<Vec<String>, ParseError> {
    let mut out: Vec<String> = Vec::new();
    for c in re(r#"href="(?:/groups/vulnerability/advisories/)?(\d{4}-\d{2}-\d{2})/?""#)
        .captures_iter(html)
    {
        if !out.contains(&c[1].to_string()) {
            out.push(c[1].to_string());
        }
    }
    if out.is_empty() {
        return err(
            "OpenJDK advisories: no dated advisory on the index (its format may have changed)",
        );
    }
    out.sort();
    out.reverse();
    Ok(out)
}

/// The last affected build of each line from the page's opening sentence
/// (`27.0.1.0.3, ..., 11.0.33, 8u512, and earlier`).
fn last_affected(text: &str) -> Vec<(String, String)> {
    let Some(c) =
        re(r"(?i)affected versions are (.+?)(?:,?\s+and earlier|\.\s|\.$)").captures(text)
    else {
        return Vec::new();
    };
    c[1].split(',')
        .map(|v| v.trim().trim_start_matches("and ").trim())
        .filter(|v| v.starts_with(|c: char| c.is_ascii_digit()))
        .map(|v| (java_line(v), v.to_string()))
        .collect()
}

/// One OpenJDK advisory page: one advisory per CVE in its OpenJDK risk
/// matrix (the OpenJFX matrix is not the runtime's).
pub fn parse_openjdk_page(html: &str, url: &str) -> Result<Vec<RuntimeAdvisory>, ParseError> {
    let date = re(r"OpenJDK Vulnerability Advisory:\s*(\d{4})/(\d{2})/(\d{2})")
        .captures(html)
        .map(|c| format!("{}-{}-{}", &c[1], &c[2], &c[3]));
    let Some(date) = date else {
        return err(format!(
            "OpenJDK advisory {url}: no advisory date (its format may have changed)"
        ));
    };
    let opening = re(r"(?s)<p>(.*?)</p>")
        .captures(html)
        .map(|c| text_of(&c[1]))
        .unwrap_or_default();
    let last = last_affected(&opening);
    if last.is_empty() {
        return err(format!(
            "OpenJDK advisory {url}: no affected versions sentence (its format may have changed)"
        ));
    }
    // The OpenJDK matrix: from its heading to the next heading.
    let matrix = html
        .split_once(r#"id="risk-matrix""#)
        .map(|(_, rest)| rest.split("<h2").next().unwrap_or(rest))
        .unwrap_or("");
    let row_re = re(r"(?s)<tr>(.*?)</tr>");
    let cell_re = re(r"(?s)<t[dh][^>]*>(.*?)</t[dh]>");
    let mut columns: Vec<String> = Vec::new();
    let mut out = Vec::new();
    for r in row_re.captures_iter(matrix) {
        let raw: Vec<&str> = cell_re
            .captures_iter(&r[1])
            .map(|c| c.get(1).map(|m| m.as_str()).unwrap_or(""))
            .collect();
        let cells: Vec<String> = raw.iter().map(|c| text_of(c)).collect();
        if cells.first().is_some_and(|c| c == "CVE ID") {
            columns = cells.iter().skip(3).cloned().collect();
            continue;
        }
        if !cells.first().is_some_and(|c| c.starts_with("CVE-")) || columns.is_empty() {
            continue;
        }
        let (score, vector) = {
            let parts: Vec<&str> = raw.get(2).unwrap_or(&"").split("<br").collect();
            let score = text_of(parts.first().unwrap_or(&""));
            let vector = parts
                .get(1)
                .map(|v| text_of(v.trim_start_matches(|c| c != '>').trim_start_matches('>')))
                .unwrap_or_default();
            (score.parse::<f64>().ok(), vector)
        };
        let mut lines = Vec::new();
        for (i, col) in columns.iter().enumerate() {
            let mark = raw.get(3 + i).copied().unwrap_or("");
            if !(mark.contains("&#8226;") || mark.contains('\u{2022}')) {
                continue;
            }
            let last_build = last
                .iter()
                .find(|(l, _)| l == col)
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            lines.push(LineRange {
                line: col.clone(),
                last_affected: last_build,
                ..Default::default()
            });
        }
        out.push(RuntimeAdvisory {
            id: cells[0].clone(),
            runtime: Runtime::Java,
            component: cells.get(1).cloned().unwrap_or_default().replace("/ ", "/"),
            cvss: score,
            vector,
            published: date.clone(),
            url: url.to_string(),
            lines,
            sources: vec![url.to_string()],
            ..Default::default()
        });
    }
    if out.is_empty() && !matrix.contains("None") {
        return err(format!(
            "OpenJDK advisory {url}: no risk matrix rows (its format may have changed)"
        ));
    }
    Ok(out)
}

/// What an Amazon Linux advisory's words and packages add to the runtime
/// advisory for the same CVE: where a line starts being affected, the
/// hardware it needs, JVM flags, and the vendor's fixed builds.
pub fn apply_alas(adv: &mut RuntimeAdvisory, a: &Advisory) {
    if !a.cves.iter().any(|c| c.eq_ignore_ascii_case(&adv.id)) {
        return;
    }
    let text = &a.overview;
    // "affecting OpenJDK 17.0.6 and later": the first affected build of its line.
    for c in re(r"(?i)(?:OpenJDK|JDK|Java)\s+(\d+(?:\.\d+)+)\s+and later").captures_iter(text) {
        let v = c[1].to_string();
        if let Some(l) = adv.lines.iter_mut().find(|l| l.line == java_line(&v)) {
            if l.introduced.is_empty() {
                l.introduced = v;
            }
        }
    }
    read_hardware(&mut adv.hardware, text);
    for f in flags_in(text) {
        if !adv.mitigations.contains(&f) {
            adv.mitigations.push(f);
        }
    }
    // `java-21-amazon-corretto` 21.0.13.1.2: Corretto's build is the
    // upstream update with one more number (21.0.13.1 + build 2).
    for p in &a.fixed {
        let Some(c) = re(r"^java-(\d+)-amazon-corretto(?:-headless)?$").captures(&p.name) else {
            continue;
        };
        let line = c[1].to_string();
        let Some(l) = adv.lines.iter_mut().find(|l| l.line == line) else {
            continue;
        };
        let vendor = format!("corretto {}", p.version);
        if !l.vendor_fixed.contains(&vendor) {
            l.vendor_fixed.push(vendor);
        }
        let n = java_numbers(&p.version);
        if n.len() == 5 {
            let upstream = n[..4]
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(".");
            if l.fixed.is_empty() && java_cmp(&upstream, &l.last_affected).is_gt() {
                l.fixed = upstream;
            }
        }
    }
    if adv.summary.is_empty() {
        if let Some(s) = text
            .split_inclusive(". ")
            .find(|s| s.to_ascii_lowercase().contains("vulnerab"))
        {
            adv.summary = crate::truncate_str(s.trim(), 300).to_string();
        }
    }
    if !a.link.is_empty() && !adv.sources.contains(&a.link) {
        adv.sources.push(a.link.clone());
    }
}

/// Hardware words in an advisory's text.
pub fn read_hardware(h: &mut Hardware, text: &str) {
    let l = text.to_ascii_lowercase();
    let mut said = false;
    if re(r"(?i)\bon (?:x86-64|x86_64|amd64)\b").is_match(text)
        || l.contains("x86-64 only")
        || l.contains("x86_64 only")
    {
        if !h.arch.contains(&"x86_64".to_string()) {
            h.arch.push("x86_64".into());
        }
        said = true;
    }
    if re(r"(?i)\b(?:arm64|aarch64)\b[^.]{0,30}\bnot affected").is_match(text) {
        if !h.not_arch.contains(&"arm64".to_string()) {
            h.not_arch.push("arm64".into());
        }
        said = true;
    }
    if re(r"(?i)avx-?512[ _-]?vbmi").is_match(text) {
        h.cpu_feature = "avx512_vbmi".into();
        said = true;
    } else if h.cpu_feature.is_empty() && re(r"(?i)avx-?512").is_match(text) {
        h.cpu_feature = "avx512".into();
        said = true;
    }
    if said && h.words.is_empty() {
        if let Some(s) = text.split(". ").find(|s| {
            let s = s.to_ascii_lowercase();
            s.contains("x86") || s.contains("cpu") || s.contains("avx")
        }) {
            h.words = crate::truncate_str(s.trim(), 300).to_string();
        }
    }
}

/// JVM options an advisory names (`-XX:-UseBASE64Intrinsics`).
pub fn flags_in(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for m in re(r"-XX:[-+]?[A-Za-z][A-Za-z0-9_]*(?:=[A-Za-z0-9_]+)?").find_iter(text) {
        if !out.contains(&m.as_str().to_string()) {
            out.push(m.as_str().to_string());
        }
    }
    out
}

/// What no feed carries in a form that can be read, per CVE: dated, with
/// where it was read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Note {
    pub id: String,
    /// When this was read (YYYY-MM-DD).
    pub as_of: String,
    pub source: String,
    pub cpu_feature: String,
    pub arch: Vec<String>,
    pub not_arch: Vec<String>,
    /// `(line, first affected build)`.
    pub introduced: Vec<(String, String)>,
    /// `(line, first fixed upstream build)`.
    pub fixed: Vec<(String, String)>,
    pub mitigations: Vec<String>,
    pub summary: String,
}

/// The notes, as shipped.
pub const NOTES_JSON: &str = include_str!("notes.json");

pub fn notes() -> Vec<Note> {
    serde_json::from_str(NOTES_JSON).expect("runtime_vuln/notes.json is valid")
}

/// Applies the notes for this advisory: a feed's own value wins where it
/// has one.
pub fn apply_notes(adv: &mut RuntimeAdvisory, notes: &[Note]) {
    for n in notes.iter().filter(|n| n.id.eq_ignore_ascii_case(&adv.id)) {
        if (adv.hardware.cpu_feature.is_empty() || adv.hardware.cpu_feature == "avx512")
            && !n.cpu_feature.is_empty()
        {
            adv.hardware.cpu_feature = n.cpu_feature.clone();
        }
        for a in &n.arch {
            if !adv.hardware.arch.contains(a) {
                adv.hardware.arch.push(a.clone());
            }
        }
        for a in &n.not_arch {
            if !adv.hardware.not_arch.contains(a) {
                adv.hardware.not_arch.push(a.clone());
            }
        }
        for (line, v) in &n.introduced {
            if let Some(l) = adv.lines.iter_mut().find(|l| &l.line == line) {
                if l.introduced.is_empty() {
                    l.introduced = v.clone();
                }
            }
        }
        for (line, v) in &n.fixed {
            if let Some(l) = adv.lines.iter_mut().find(|l| &l.line == line) {
                if l.fixed.is_empty() {
                    l.fixed = v.clone();
                }
            }
        }
        for m in &n.mitigations {
            if !adv.mitigations.contains(m) {
                adv.mitigations.push(m.clone());
            }
        }
        if adv.summary.is_empty() {
            adv.summary = n.summary.clone();
        }
        let src = format!("{} (read {})", n.source, n.as_of);
        if !adv.sources.contains(&src) {
            adv.sources.push(src);
        }
    }
}

/// Every runtime advisory with what the Amazon Linux advisories and the
/// notes add.
pub fn enrich(advs: &mut [RuntimeAdvisory], alas: &[Advisory], notes: &[Note]) {
    for adv in advs.iter_mut() {
        for a in alas
            .iter()
            .filter(|a| a.package.starts_with("java-") && a.detailed)
        {
            apply_alas(adv, a);
        }
        apply_notes(adv, notes);
    }
}

/// How a version stands against an advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Standing {
    /// This build is affected.
    Affected,
    /// A line or a tag the registry moves: what runs is the newest build
    /// when it was built or deployed, and a new build takes the fix.
    Floating,
    NotAffected,
}

/// Where an exact build or a line stands. `exact` is false for a line
/// (`21`) or a moving tag; a moving tag at the last affected update
/// (`21.0.13`, republished with each build of that update) is floating.
pub fn standing<'a>(
    adv: &'a RuntimeAdvisory,
    version: &str,
    exact: bool,
) -> (Standing, Option<&'a LineRange>) {
    let Some(l) = adv.line(&java_line(version)) else {
        return (Standing::NotAffected, None);
    };
    if !exact {
        return (Standing::Floating, Some(l));
    }
    if !l.introduced.is_empty() && java_cmp(version, &l.introduced).is_lt() {
        return (Standing::NotAffected, Some(l));
    }
    if l.last_affected.is_empty() || !java_cmp(version, &l.last_affected).is_gt() {
        return (Standing::Affected, Some(l));
    }
    (Standing::NotAffected, Some(l))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::infra_vuln::feeds::{self, Distro};

    pub const PAGE: &str = include_str!("testdata/openjdk-2026-11-17.html");
    const INDEX: &str = include_str!("testdata/openjdk-index.html");
    const ALAS_PAGE: &str = include_str!("testdata/alas2023-2026-3479.html");
    const ALAS2_PAGE: &str = include_str!("testdata/alas2-2026-4123.html");
    const RSS: &str = include_str!("testdata/alas2023-java-rss.xml");

    /// The live advisory of 2026-11-17, read with the Amazon Linux pages
    /// and the notes, as the daily read stores it.
    pub fn java4shell() -> RuntimeAdvisory {
        let advs = parse_openjdk_page(PAGE, &openjdk_page("2026-11-17")).unwrap();
        let mut alas = Vec::new();
        for (html, distro) in [(ALAS_PAGE, Distro::Al2023), (ALAS2_PAGE, Distro::Al2)] {
            let p = feeds::parse_alas_page(html).unwrap();
            let mut a = Advisory {
                id: p.id.clone(),
                distro,
                package: if distro == Distro::Al2023 {
                    "java-21-amazon-corretto".into()
                } else {
                    "java-17-amazon-corretto".into()
                },
                link: format!("https://alas.aws.amazon.com/{}.html", p.id),
                ..Default::default()
            };
            a.apply_page(&p);
            alas.push(a);
        }
        let mut advs = advs;
        enrich(&mut advs, &alas, &notes());
        advs.into_iter()
            .find(|a| a.id == "CVE-2026-114938")
            .unwrap()
    }

    #[test]
    fn the_index_lists_the_november_advisory_first() {
        let d = parse_openjdk_index(INDEX).unwrap();
        assert_eq!(d[0], "2026-11-17");
        assert!(d.contains(&"2026-10-21".to_string()));
        assert!(parse_openjdk_index("<html>Service Unavailable</html>").is_err());
    }

    #[test]
    fn the_live_page_names_the_lines_and_last_affected_builds() {
        let advs = parse_openjdk_page(PAGE, &openjdk_page("2026-11-17")).unwrap();
        assert_eq!(advs.len(), 10, "OpenJDK rows only, not OpenJFX");
        let a = advs.iter().find(|a| a.id == "CVE-2026-114938").unwrap();
        assert_eq!(a.published, "2026-11-17");
        assert_eq!(a.component, "hotspot/compiler");
        assert_eq!(a.cvss, Some(8.1));
        assert_eq!(a.vector, "NHNNUHHH");
        let lines: Vec<(&str, &str)> = a
            .lines
            .iter()
            .map(|l| (l.line.as_str(), l.last_affected.as_str()))
            .collect();
        assert_eq!(
            lines,
            vec![
                ("17", "17.0.21.0.3"),
                ("21", "21.0.13.0.3"),
                ("25", "25.0.5.0.3"),
                ("27", "27.0.1.0.3")
            ],
            "8 and 11 are not affected"
        );
        // Another row affects 8 and 11 too.
        let all = advs.iter().find(|a| a.id == "CVE-2026-114939").unwrap();
        assert_eq!(all.line("8").unwrap().last_affected, "8u512");
        assert_eq!(all.line("11").unwrap().last_affected, "11.0.33");
        assert!(!advs.iter().any(|a| a.id == "CVE-2026-86144"), "OpenJFX");
    }

    #[test]
    fn a_page_that_changed_shape_is_an_error() {
        assert!(parse_openjdk_page("<html>nope</html>", "u").is_err());
        let no_sentence = PAGE.replace("The affected versions are", "Versions:");
        assert!(parse_openjdk_page(&no_sentence, "u").is_err());
    }

    #[test]
    fn amazon_linux_adds_the_start_the_hardware_and_the_fixed_builds() {
        let a = java4shell();
        assert_eq!(a.line("17").unwrap().introduced, "17.0.6");
        assert!(a.line("21").unwrap().introduced.is_empty());
        assert_eq!(a.hardware.arch, vec!["x86_64"]);
        assert_eq!(a.hardware.not_arch, vec!["arm64"]);
        assert_eq!(a.hardware.cpu_feature, "avx512_vbmi", "from the notes");
        assert!(a.hardware.words.contains("x86-64"), "{}", a.hardware.words);
        let l21 = a.line("21").unwrap();
        assert_eq!(l21.fixed, "21.0.13.1");
        assert_eq!(l21.vendor_fixed, vec!["corretto 21.0.13.1.2"]);
        assert_eq!(a.line("17").unwrap().fixed, "17.0.21.1");
        assert_eq!(a.mitigations, vec!["-XX:-UseBASE64Intrinsics"]);
        assert!(a.summary.contains("Base64"), "{}", a.summary);
        assert!(a.sources.iter().any(|s| s.contains("ALAS2023-2026-3479")));
        // The feed's own RSS lists the java advisories with the CVE.
        let rss = feeds::parse_alas_rss(RSS, Distro::Al2023).unwrap();
        assert!(rss.iter().any(|x| x.package == "java-21-amazon-corretto"
            && x.cves.contains(&"CVE-2026-114938".to_string())));
    }

    #[test]
    fn builds_stand_where_the_ranges_put_them() {
        let a = java4shell();
        let s = |v: &str, exact: bool| standing(&a, v, exact).0;
        assert_eq!(s("21.0.12", true), Standing::Affected);
        assert_eq!(s("21.0.13.0.3", true), Standing::Affected);
        assert_eq!(s("21.0.13.1", true), Standing::NotAffected);
        assert_eq!(s("17.0.15", true), Standing::Affected);
        assert_eq!(s("17.0.5", true), Standing::NotAffected, "before 17.0.6");
        assert_eq!(s("11.0.25", true), Standing::NotAffected);
        assert_eq!(s("8u402", true), Standing::NotAffected);
        assert_eq!(s("21", false), Standing::Floating);
        assert_eq!(s("11", false), Standing::NotAffected);
    }

    #[test]
    fn flags_and_hardware_are_read_from_words() {
        let mut h = Hardware::default();
        read_hardware(
            &mut h,
            "Only x86-64 CPUs with AVX-512 VBMI are affected. aarch64 systems are not affected.",
        );
        assert_eq!(h.cpu_feature, "avx512_vbmi");
        assert_eq!(h.not_arch, vec!["arm64"]);
        assert_eq!(
            flags_in("Start the JVM with -XX:-UseBASE64Intrinsics or -XX:UseAVX=2 until patched."),
            vec!["-XX:-UseBASE64Intrinsics", "-XX:UseAVX=2"]
        );
    }
}
