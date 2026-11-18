//! A repository's runtime declarations against the runtime advisories, as
//! dependency findings (`security_fixes`) of the [`super::ECOSYSTEM`]
//! ecosystem, so they take the same tiers, cards, fix runs and pull
//! requests.
//!
//! - One finding per declaration and CVE: package = the image's name (or
//!   the runtime's), version = the tag or value as written, manifest = the
//!   file.
//! - An exact build that is affected gets a pull request when a fixed build
//!   is known in the same form: for an image, a published tag of the same
//!   variant past the last affected build ([`fixed_tag`], from the
//!   registry's tag list); for a version file or a CI pin, the upstream fix
//!   (or the vendor's fixed build, when the advisory names the vendor's).
//! - A line, a floating tag (`21-jre`), or a tag the registry republishes
//!   for each build of an update (`amazoncorretto:21.0.13`) takes the fix
//!   on its next build: no pull request; the finding says to rebuild and
//!   redeploy, or to pin a fixed build.
//! - Every finding names the advisory's hardware condition and its JVM flag
//!   for the time until the fix runs. An image built only for an
//!   architecture the advisory says is not affected stays a finding, with
//!   that reason and the lowest tier ([`Verdict::NotReachable`]).
//! - Lambda runtimes are the infrastructure findings' (AWS patches them).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::advisories::{standing, LineRange, RuntimeAdvisory, Standing};
use super::decls::{Decl, DeclKind};
use super::{java_cmp, java_numbers, Runtime};
use crate::security_fixes::{
    self as sec, AdvisoryRecord, Evidence, Finding, FindingState, FixUnit, Observation, Relation,
    Scope, Status, Verdict,
};

/// Why a runtime finding has no code verdict.
pub const NOT_CODE: &str = "a runtime's own flaw: what runs it decides, not a call in the code";

/// What one read of a repository's runtime declarations found, for
/// closing findings the code no longer has.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Read {
    /// Files read in full this time.
    pub files: BTreeSet<String>,
    /// `(manifest, package, written)` to the advisory ids it is affected by.
    pub seen: BTreeMap<(String, String, String), BTreeSet<String>>,
}

/// A tag the registry republishes for each build of an update: three
/// numbers, no build, at the update the advisory's last affected build is
/// on (`21.0.13` while the last affected build is `21.0.13.0.3`).
fn moving_tag(d: &Decl, l: &LineRange) -> bool {
    if d.kind != DeclKind::Image || d.version.contains(['_', '+']) {
        return false;
    }
    let v = java_numbers(&d.version);
    let last = java_numbers(&l.last_affected);
    v.len() == 3 && last.len() > 3 && last[..3] == v[..]
}

/// The newest-variant-preserving fixed tag: the lowest published tag with
/// the same shape (`21.0.4_7-jre` keeps `_N-jre`) on the same line, past
/// the last affected build.
pub fn fixed_tag(current: &str, tags: &[String], l: &LineRange) -> Option<String> {
    let shape = |t: &str| -> Option<(Vec<u64>, String)> {
        let lead: String = t
            .chars()
            .take_while(|c| c.is_ascii_digit() || matches!(c, '.' | '_' | '+'))
            .collect();
        if lead.is_empty() {
            return None;
        }
        let suffix = t[lead.len()..].to_string();
        let build = lead.contains(['_', '+']);
        let mut n = java_numbers(&lead);
        if build {
            // Keep the build as a last number so `_9` sorts after `_7`.
            let b: u64 = lead
                .rsplit(['_', '+'])
                .next()
                .and_then(|x| x.parse().ok())
                .unwrap_or(0);
            n.push(b);
        }
        Some((n, format!("{}|{suffix}", if build { "b" } else { "" })))
    };
    let (_, want) = shape(current)?;
    let line = &l.line;
    let mut best: Option<(Vec<u64>, String)> = None;
    for t in tags {
        let Some((n, s)) = shape(t) else { continue };
        if s != want || n.first().map(|x| x.to_string()).as_deref() != Some(line.as_str()) {
            continue;
        }
        let lead: String = t
            .chars()
            .take_while(|c| c.is_ascii_digit() || c == &'.')
            .collect();
        if !java_cmp(&lead, &l.last_affected).is_gt() {
            continue;
        }
        if !l.introduced.is_empty() && java_cmp(&lead, &l.introduced).is_lt() {
            continue;
        }
        if best.as_ref().is_none_or(|(b, _)| n < *b) {
            best = Some((n, t.clone()));
        }
    }
    best.map(|(_, t)| t)
}

/// The fixed value for a version file or CI pin, written the way the file
/// writes it, when one is known for its vendor.
fn fixed_value(d: &Decl, l: &LineRange) -> Option<String> {
    let numbers_at = d.written.find(|c: char| c.is_ascii_digit())?;
    let rest = &d.written[numbers_at..];
    let numeric_len = rest
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(rest.len());
    let prefix = &d.written[..numbers_at];
    let to = match d.vendor.as_str() {
        "" | "openjdk" => {
            // A plain version (`17.0.15`): the upstream fix, nothing after it.
            if !prefix.is_empty() || numeric_len != rest.len() {
                return None;
            }
            l.fixed.clone()
        }
        "corretto" => l
            .vendor_fixed
            .iter()
            .find_map(|v| v.strip_prefix("corretto "))
            .map(|v| format!("{prefix}{v}"))?,
        // A vendor whose fixed build no source names yet.
        _ => return None,
    };
    (!to.is_empty()).then_some(to)
}

fn hardware_lines(adv: &RuntimeAdvisory) -> Vec<String> {
    let mut out = Vec::new();
    let h = &adv.hardware;
    if !h.is_empty() {
        let mut s = String::from("Hardware: exploitable only on ");
        s.push_str(if h.arch.is_empty() {
            "CPUs"
        } else {
            "x86-64 CPUs"
        });
        if h.cpu_feature == "avx512_vbmi" {
            s.push_str(&format!(
                " with AVX-512 VBMI ({})",
                super::cpu::families_with("avx512_vbmi")
            ));
        }
        if !h.not_arch.is_empty() {
            s.push_str(&format!("; {} is not affected", h.not_arch.join(", ")));
        }
        out.push(s);
    }
    if !adv.mitigations.is_empty() {
        out.push(format!(
            "Until the fix runs: start the JVM with {} (for example in JAVA_TOOL_OPTIONS), then remove it once the fixed build runs",
            adv.mitigations.join(" ")
        ));
    }
    out
}

/// The observations a repository's declarations make, and what the read
/// saw. `fixed_tags` maps `(image, tag)` to the fixed tag the registry has.
pub fn observations(
    repo: &str,
    decls: &[Decl],
    files_read: &BTreeSet<String>,
    advs: &[RuntimeAdvisory],
    fixed_tags: &BTreeMap<(String, String), String>,
) -> (Vec<Observation>, Read) {
    let mut out = Vec::new();
    let mut read = Read {
        files: files_read.clone(),
        ..Default::default()
    };
    for d in decls.iter().filter(|d| d.kind != DeclKind::Lambda) {
        for adv in advs.iter().filter(|a| a.runtime == d.runtime) {
            let Some(l) = adv.line(&super::java_line(&d.version)) else {
                continue;
            };
            let exact = d.exact && !moving_tag(d, l);
            let (st, _) = standing(adv, &d.version, exact);
            if st == Standing::NotAffected {
                continue;
            }
            read.seen
                .entry((d.file.clone(), d.package.clone(), d.written.clone()))
                .or_default()
                .insert(adv.id.clone());
            let mut path = vec![format!(
                "{} {}: {} {} on the {} line, affected through {}{}",
                d.describe(),
                if exact { "pins" } else { "follows" },
                d.runtime.label(),
                d.version,
                l.line,
                l.last_affected,
                if l.introduced.is_empty() {
                    String::new()
                } else {
                    format!(" from {}", l.introduced)
                }
            )];
            let fixed_words = if l.fixed.is_empty() {
                format!("the first build after {}", l.last_affected)
            } else {
                l.fixed.clone()
            };
            let to: Option<String> = match (st, d.kind) {
                (Standing::Floating, _) => {
                    path.push(format!(
                        "Fix: rebuild and redeploy so it runs {fixed_words} or later (this {} takes the newest build when it is built), or pin a fixed build",
                        if moving_tag(d, l) { "tag is republished for each build of the update and" } else { "line" }
                    ));
                    None
                }
                (_, DeclKind::Image) => {
                    let t = fixed_tags
                        .get(&(d.package.clone(), d.written.clone()))
                        .cloned();
                    match &t {
                        Some(t) => path.push(format!("Fix: move the image to `{}:{t}`, then rebuild and redeploy", d.package)),
                        None => path.push(format!(
                            "Fix: no `{}` tag of this variant past {} is published yet; until one is, rebuild on the line's floating tag or use the JVM flag below",
                            d.package, l.last_affected
                        )),
                    }
                    t
                }
                _ => {
                    let t = fixed_value(d, l);
                    match &t {
                        Some(t) => path.push(format!("Fix: set it to `{t}`")),
                        None => path.push(format!(
                            "Fix: move to {}'s build of {fixed_words} once it is published; until then use the JVM flag below",
                            if d.vendor.is_empty() { "the distribution" } else { d.vendor.as_str() }
                        )),
                    }
                    t
                }
            };
            path.extend(hardware_lines(adv));
            if !l.vendor_fixed.is_empty() {
                path.push(format!("Fixed builds: {}", l.vendor_fixed.join(", ")));
            }
            out.push(Observation {
                repo: repo.to_string(),
                manifest: d.file.clone(),
                ecosystem: super::ECOSYSTEM.into(),
                package: d.package.clone(),
                version: d.written.clone(),
                advisory: AdvisoryRecord {
                    ids: vec![adv.id.clone()],
                    title: format!(
                        "{} in the {} {} runtime ({})",
                        adv.id,
                        d.runtime.label(),
                        l.line,
                        adv.component
                    ),
                    summary: adv.summary.clone(),
                    cvss: adv.cvss,
                    severity: Some(adv.severity().into()),
                    published_at: Some(adv.published.clone()),
                    urls: adv
                        .sources
                        .iter()
                        .filter(|s| s.starts_with("https://"))
                        .cloned()
                        .collect(),
                    source: "openjdk".into(),
                    fixed: to.into_iter().collect(),
                    ..Default::default()
                },
                scope: Scope::Runtime,
                relation: Relation::Direct,
                path,
                ..Default::default()
            });
        }
    }
    (out, read)
}

/// The verdict a runtime finding carries: an image built only for an
/// architecture the advisory says is not affected is not reachable (the
/// lowest tier, with the reason); everything else is not known from code.
fn verdict_of(f: &Finding, decls: &[Decl], advs: &[RuntimeAdvisory]) -> (Verdict, String) {
    let platform_arch = decls
        .iter()
        .find(|d| d.file == f.manifest && d.package == f.package && d.written == f.version)
        .map(|d| d.platform.rsplit('/').next().unwrap_or("").to_string())
        .unwrap_or_default();
    let arch = match platform_arch.as_str() {
        "arm64" | "aarch64" => "arm64",
        "amd64" | "x86_64" => "x86_64",
        _ => "",
    };
    let not = advs
        .iter()
        .filter(|a| f.advisory.ids.iter().any(|i| i.eq_ignore_ascii_case(&a.id)))
        .all(|a| !arch.is_empty() && a.hardware.not_arch.iter().any(|x| x == arch));
    if not {
        return (
            Verdict::NotReachable,
            format!("The image is built for linux/{arch}, which the advisory says is not affected: kept at the lowest tier in case the platform changes"),
        );
    }
    (Verdict::Unknown, NOT_CODE.to_string())
}

/// What [`apply_reads`] changed on a finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Fixed,
    Updated,
}

/// Brings a repository's runtime findings up to date with the read: a
/// finding whose file was read and no longer declares an affected runtime
/// is fixed; the rest carry their verdict (never triaged in code) and tier.
pub fn apply_reads(
    findings: &mut [(Finding, FindingState)],
    repo: &str,
    read: &Read,
    decls: &[Decl],
    advs: &[RuntimeAdvisory],
    cfg: &sec::SecurityConfig,
) -> Vec<(usize, Change)> {
    let mut out = Vec::new();
    for (i, (f, st)) in findings.iter_mut().enumerate() {
        if f.ecosystem != super::ECOSYSTEM
            || !f.repo.eq_ignore_ascii_case(repo)
            || !f.status.is_open()
        {
            continue;
        }
        let before = (f.fixed_version.clone(), f.verdict, f.tier);
        if read.files.contains(&f.manifest) {
            let still = read
                .seen
                .get(&(f.manifest.clone(), f.package.clone(), f.version.clone()))
                .is_some_and(|ids| f.advisory.ids.iter().any(|id| ids.contains(id)));
            if !still {
                f.status = Status::Fixed;
                f.updated_at = chrono::Utc::now().to_rfc3339();
                out.push((i, Change::Fixed));
                continue;
            }
        }
        let (v, why) = verdict_of(f, decls, advs);
        if f.verdict != v || f.evidence.as_ref().map(|e| e.summary.as_str()) != Some(why.as_str()) {
            f.verdict = v;
            f.evidence = Some(Evidence {
                summary: why,
                lines: Vec::new(),
                checked:
                    "The runtime declarations in the repository, against the runtime advisories"
                        .into(),
                confidence: "medium".into(),
            });
        }
        sec::retier(f, st, cfg);
        if (f.fixed_version.clone(), f.verdict, f.tier) != before {
            f.updated_at = chrono::Utc::now().to_rfc3339();
            out.push((i, Change::Updated));
        }
    }
    out
}

/// The plan a runtime fix run follows: the exact line to change.
pub fn ticket_body(unit: &FixUnit, members: &[&Finding]) -> String {
    let mut s = format!(
        "{}\n<!-- coderhelm:security-branch {} -->\n\n",
        sec::MARKER,
        unit.branch
    );
    s.push_str("Fix these advisories in a language runtime the repository pins (a JDK build). Change only the lines named here.\n\n## Change\n");
    for b in &unit.bumps {
        let runtime_name = [Runtime::Java, Runtime::Node, Runtime::Python, Runtime::Go]
            .iter()
            .any(|r| r.name() == b.package);
        let file = if b.manifest.is_empty() {
            "the file"
        } else {
            b.manifest.as_str()
        };
        if runtime_name {
            s.push_str(&format!(
                "- In `{file}`, replace the value `{}` with `{}`. Keep the file's format; change nothing else in it.\n",
                b.from, b.to
            ));
        } else {
            s.push_str(&format!(
                "- In `{file}`, replace `FROM {}:{}` with `FROM {}:{}`. Keep the line's flags and its `AS` stage name. Every other `FROM {}:{}` in the repository changes the same way.\n",
                b.package, b.from, b.package, b.to, b.package, b.from
            ));
        }
    }
    s.push_str("\nThe new build is on the same feature line as the old one; don't move to another line or another vendor. Once merged, every image and service built from these files has to be rebuilt and redeployed for the fix to run.\n\n## Advisories\n");
    for f in members {
        s.push_str(&format!(
            "- {} ({})\n",
            f.advisory.primary,
            f.advisory.ids.join(", ")
        ));
        for p in &f.path {
            s.push_str(&format!("  - {p}\n"));
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_vuln::advisories::tests::java4shell;
    use crate::runtime_vuln::decls::{from_dockerfile, from_file};

    fn scan(files: &[(&str, &str)]) -> (Vec<Decl>, BTreeSet<String>) {
        let mut decls = Vec::new();
        let mut read = BTreeSet::new();
        for (p, t) in files {
            read.insert(p.to_string());
            if p.ends_with("Dockerfile") {
                decls.extend(from_dockerfile(p, t));
            } else {
                decls.extend(from_file(p, t));
            }
        }
        (decls, read)
    }

    fn temurin_tags() -> Vec<String> {
        [
            "21.0.4_7-jre",
            "21.0.12_7-jre",
            "21.0.13_11-jre",
            "21.0.13.1_2-jre",
            "21.0.13.1_2-jdk",
            "21.0.13.1_3-jre",
            "21-jre",
            "25.0.5.1_1-jre",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn findings_of(
        repo: &str,
        files: &[(&str, &str)],
        tags: &BTreeMap<(String, String), String>,
    ) -> (Vec<(Finding, FindingState)>, Vec<Decl>, Read) {
        let advs = vec![java4shell()];
        let (decls, read_files) = scan(files);
        let (obs, read) = observations(repo, &decls, &read_files, &advs, tags);
        let now = chrono::Utc::now();
        let mut out: Vec<(Finding, FindingState)> = Vec::new();
        for o in &obs {
            let idx = out.iter().position(|(f, _)| {
                sec::merges_into_at(
                    f,
                    &o.advisory.ids,
                    &o.repo,
                    &o.manifest,
                    &o.package,
                    &o.version,
                )
            });
            let (f, st, _) = sec::apply(idx.map(|i| out[i].clone()), o, now);
            match idx {
                Some(i) => out[i] = (f, st),
                None => out.push((f, st)),
            }
        }
        let cfg = sec::SecurityConfig {
            enabled: true,
            ..Default::default()
        };
        apply_reads(&mut out, repo, &read, &decls, &advs, &cfg);
        (out, decls, read)
    }

    fn cfg() -> sec::SecurityConfig {
        sec::SecurityConfig {
            enabled: true,
            ..Default::default()
        }
    }

    /// (a) A pinned JDK image and a floating one.
    #[test]
    fn a_pinned_jdk_image_gets_a_pull_request_to_the_fixed_tag() {
        let l = java4shell().line("21").unwrap().clone();
        let fixed = fixed_tag("21.0.4_7-jre", &temurin_tags(), &l).unwrap();
        assert_eq!(fixed, "21.0.13.1_2-jre", "same variant, lowest fixed");
        let tags = BTreeMap::from([(
            ("eclipse-temurin".to_string(), "21.0.4_7-jre".to_string()),
            fixed.clone(),
        )]);
        let (f, _, _) = findings_of(
            "acme/api",
            &[(
                "Dockerfile",
                "FROM eclipse-temurin:21.0.4_7-jre\nCOPY app.jar /app.jar\n",
            )],
            &tags,
        );
        assert_eq!(f.len(), 1);
        let (x, _) = &f[0];
        assert_eq!(x.advisory.primary, "CVE-2026-114938");
        assert_eq!(x.ecosystem, "runtime");
        assert_eq!(
            (x.package.as_str(), x.version.as_str()),
            ("eclipse-temurin", "21.0.4_7-jre")
        );
        assert_eq!(x.fixed_version.as_deref(), Some("21.0.13.1_2-jre"));
        assert_eq!(x.verdict, Verdict::Unknown);
        assert_eq!(
            x.tier,
            Some(sec::Tier::P1),
            "8.1, a runtime, hardware not known from code"
        );
        assert!(
            x.path.iter().any(|p| p.contains("AVX-512 VBMI")),
            "{:?}",
            x.path
        );
        assert!(x
            .path
            .iter()
            .any(|p| p.contains("-XX:-UseBASE64Intrinsics")));
        // One unit, one bump, and the ticket names the FROM line.
        let units = sec::plan_units(&f, &cfg(), chrono::Utc::now());
        assert_eq!(units.len(), 1, "{units:?}");
        assert_eq!(units[0].bumps[0].to, "21.0.13.1_2-jre");
        let members: Vec<&Finding> = f.iter().map(|(x, _)| x).collect();
        let body = sec::ticket_body(&units[0], &members, &cfg());
        assert!(
            body.contains("replace `FROM eclipse-temurin:21.0.4_7-jre` with `FROM eclipse-temurin:21.0.13.1_2-jre`"),
            "{body}"
        );
        assert!(!body.contains("lockfile"));
    }

    #[test]
    fn a_floating_jdk_tag_is_a_rebuild_not_a_pull_request() {
        let (f, _, _) = findings_of(
            "acme/api",
            &[("Dockerfile", "FROM eclipse-temurin:21-jre\n")],
            &BTreeMap::new(),
        );
        assert_eq!(f.len(), 1);
        let (x, _) = &f[0];
        assert!(x.fixed_version.is_none());
        assert!(
            x.path.iter().any(|p| p.contains("rebuild and redeploy")),
            "{:?}",
            x.path
        );
        assert!(x.path.iter().any(|p| p.contains("21.0.13.1")));
        assert!(sec::plan_units(&f, &cfg(), chrono::Utc::now()).is_empty());
        // A pinned build no registry has a fix for yet: a finding, no PR.
        let (g, _, _) = findings_of(
            "acme/api",
            &[("Dockerfile", "FROM eclipse-temurin:21.0.12_7-jre\n")],
            &BTreeMap::new(),
        );
        assert_eq!(g.len(), 1);
        assert!(g[0].0.fixed_version.is_none());
        // A tag republished for each build of the update.
        let (h, _, _) = findings_of(
            "acme/api",
            &[("Dockerfile", "FROM amazoncorretto:21.0.13-al2023\n")],
            &BTreeMap::new(),
        );
        assert!(
            h[0].0.path.iter().any(|p| p.contains("republished")),
            "{:?}",
            h[0].0.path
        );
    }

    /// (b) A version file.
    #[test]
    fn a_java_version_file_on_17_is_bumped_to_the_fix() {
        let (f, _, _) = findings_of(
            "acme/svc",
            &[(".java-version", "17.0.15\n")],
            &BTreeMap::new(),
        );
        assert_eq!(f.len(), 1);
        let (x, _) = &f[0];
        assert_eq!(x.fixed_version.as_deref(), Some("17.0.21.1"));
        let units = sec::plan_units(&f, &cfg(), chrono::Utc::now());
        let members: Vec<&Finding> = f.iter().map(|(x, _)| x).collect();
        let body = sec::ticket_body(&units[0], &members, &cfg());
        assert!(
            body.contains("In `.java-version`, replace the value `17.0.15` with `17.0.21.1`"),
            "{body}"
        );
        // A vendor whose fixed build no source names: a finding, no PR.
        let (g, _, _) = findings_of(
            "acme/svc",
            &[(".sdkmanrc", "java=21.0.4-tem\n")],
            &BTreeMap::new(),
        );
        assert_eq!(g.len(), 1);
        assert!(g[0].0.fixed_version.is_none());
        assert!(
            g[0].0.path.iter().any(|p| p.contains("temurin's build")),
            "{:?}",
            g[0].0.path
        );
    }

    /// (c) A Gradle toolchain, Maven's release, and CI's setup-java.
    #[test]
    fn toolchains_and_ci_lines_are_findings_to_rebuild() {
        let (f, _, _) = findings_of(
            "acme/svc",
            &[
                ("build.gradle.kts", "java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }\n"),
                ("pom.xml", "<maven.compiler.release>21</maven.compiler.release>\n"),
                (".github/workflows/ci.yml", "      - uses: actions/setup-java@v4\n        with:\n          java-version: '21'\n"),
            ],
            &BTreeMap::new(),
        );
        let manifests: BTreeSet<&str> = f.iter().map(|(x, _)| x.manifest.as_str()).collect();
        assert_eq!(
            manifests,
            BTreeSet::from(["build.gradle.kts", "pom.xml", ".github/workflows/ci.yml"])
        );
        assert!(f
            .iter()
            .all(|(x, _)| x.fixed_version.is_none() && x.status == Status::Open));
        assert!(f
            .iter()
            .all(|(x, _)| x.advisory.ids.contains(&"CVE-2026-114938".to_string())));
    }

    #[test]
    fn java_8_11_and_fixed_builds_are_not_findings() {
        let (f, _, _) = findings_of(
            "acme/old",
            &[
                ("Dockerfile", "FROM eclipse-temurin:11.0.25_9-jre\nFROM eclipse-temurin:8u432-b06-jre\nFROM eclipse-temurin:21.0.13.1_2-jre\nFROM openjdk:17.0.5-jdk\n"),
                (".java-version", "11.0.25\n"),
                ("pom.xml", "<java.version>1.8</java.version>\n"),
                (".tool-versions", "java 21.0.13.1\n"),
            ],
            &BTreeMap::new(),
        );
        assert!(
            f.is_empty(),
            "{:?}",
            f.iter()
                .map(|(x, _)| (&x.manifest, &x.version))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn an_arm64_only_image_is_kept_at_the_lowest_tier_with_the_reason() {
        let (f, _, _) = findings_of(
            "acme/api",
            &[(
                "Dockerfile",
                "FROM --platform=linux/arm64 eclipse-temurin:21-jre\n",
            )],
            &BTreeMap::new(),
        );
        assert_eq!(f.len(), 1, "never dropped");
        let (x, _) = &f[0];
        assert_eq!(x.verdict, Verdict::NotReachable);
        assert_eq!(x.tier, Some(sec::Tier::P3));
        assert!(x.evidence.as_ref().unwrap().summary.contains("arm64"));
    }

    #[test]
    fn a_file_that_no_longer_declares_an_affected_build_closes_its_finding() {
        let (mut f, _, _) = findings_of(
            "acme/svc",
            &[(".java-version", "17.0.15\n")],
            &BTreeMap::new(),
        );
        let advs = vec![java4shell()];
        let (decls, files) = scan(&[(".java-version", "17.0.21.1\n")]);
        let (obs, read) = observations("acme/svc", &decls, &files, &advs, &BTreeMap::new());
        assert!(obs.is_empty());
        let ch = apply_reads(&mut f, "acme/svc", &read, &decls, &advs, &cfg());
        assert_eq!(ch, vec![(0, Change::Fixed)]);
        assert_eq!(f[0].0.status, Status::Fixed);
        // A read that did not read the file closes nothing.
        let (mut g, _, _) = findings_of(
            "acme/svc",
            &[(".java-version", "17.0.15\n")],
            &BTreeMap::new(),
        );
        apply_reads(&mut g, "acme/svc", &Read::default(), &[], &advs, &cfg());
        assert!(g[0].0.status.is_open());
    }

    #[test]
    fn lambda_declarations_are_left_to_the_infrastructure_findings() {
        let (f, decls, _) = findings_of(
            "acme/infra",
            &[(
                "lambda.tf",
                "resource \"aws_lambda_function\" \"f\" {\n  runtime = \"java21\"\n}\n",
            )],
            &BTreeMap::new(),
        );
        assert_eq!(decls.len(), 1);
        assert!(f.is_empty());
    }
}
