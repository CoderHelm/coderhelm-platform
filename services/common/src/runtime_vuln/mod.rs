//! Language runtimes a team runs (a JDK, Node.js, Python, Go), matched by
//! version line against the runtime projects' own advisories. Libraries
//! are the dependency findings' (`security_fixes`), operating system
//! packages the base images' and the infrastructure findings'; a runtime is
//! neither: a JDK is not a Maven package, and a JDK unpacked into an image
//! is not an OS package.
//!
//! - [`decls`]: where code says which runtime runs: Dockerfile `FROM`
//!   images whose tag carries the version (`eclipse-temurin:21.0.4_7-jre`,
//!   `amazoncorretto:21`), version files (`.java-version`, `.sdkmanrc`,
//!   `.tool-versions`, `.nvmrc`, `.python-version`), build toolchains
//!   (Gradle `JavaLanguageVersion.of(21)`, Maven `maven.compiler.release`),
//!   CI setup steps (`actions/setup-java` and the others) and Lambda runtime
//!   declarations in infrastructure code (`java21`). Each says how precise
//!   it is: an exact build, or a line that takes whatever build is newest
//!   when it is built or deployed.
//!
//! - [`advisories`]: the OpenJDK Vulnerability Group's advisories (the
//!   index and each dated page, with its risk matrix and the last affected
//!   build of every line), read once a day with the infrastructure feeds and
//!   kept as `RUNTIME_ADV#<id>`. What the page does not say (the first
//!   affected build, the CPU it needs, a JVM flag that turns the code path
//!   off, a vendor's fixed build) comes from the Amazon Linux advisory for
//!   the same CVE, read the same day, and from [`advisories::NOTES`], dated
//!   and sourced. A page that no longer parses keeps the last good copy.
//!
//! - [`findings`]: a repository's declarations against the advisories, as
//!   dependency findings of the `runtime` ecosystem: a pull request that
//!   moves a pinned build to a fixed one when one is published, else a
//!   runbook that says to rebuild (a floating tag or line takes the fix on
//!   rebuild) and names the advisory's JVM flag.
//!
//! - [`cpu`]: what an EC2 instance type runs on (architecture, CPU
//!   generation, AVX-512 VBMI), so a finding states whether an advisory's
//!   hardware condition holds. The infrastructure findings use it for the
//!   packages SSM inventory lists on instances (`infra_vuln::matching`).
//!
//! Nothing here calls a model: every match is a comparison.

pub mod advisories;
pub mod cpu;
pub mod decls;
pub mod findings;
#[cfg(test)]
mod replay;

use serde::{Deserialize, Serialize};

/// The ecosystem runtime findings are recorded under.
pub const ECOSYSTEM: &str = "runtime";
/// One row per runtime advisory, under the infrastructure feeds' partition.
pub const ADVISORY_PREFIX: &str = "RUNTIME_ADV#";

pub fn advisory_sk(id: &str) -> String {
    format!("{ADVISORY_PREFIX}{}", id.to_ascii_uppercase())
}

/// A language runtime.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum Runtime {
    #[default]
    Java,
    Node,
    Python,
    Go,
}

impl Runtime {
    /// The name a finding's package carries when no image names it.
    pub fn name(self) -> &'static str {
        match self {
            Runtime::Java => "java",
            Runtime::Node => "node",
            Runtime::Python => "python",
            Runtime::Go => "go",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Runtime::Java => "Java (JDK)",
            Runtime::Node => "Node.js",
            Runtime::Python => "Python",
            Runtime::Go => "Go",
        }
    }
}

/// A Java version as numbers: `1.8.0_402` and `8u402` are `8.0.402`;
/// a build after `+` or `_` is dropped (`21.0.4+7`, `21.0.4_7`,
/// `17.0.21.1_p2-r0`); a vendor's prefix or suffix is dropped
/// (`temurin-21.0.4`, `21.0.4-tem`, `21.0.4.LTS`).
pub fn java_numbers(v: &str) -> Vec<u64> {
    let mut s = v.trim().to_ascii_lowercase();
    // A vendor prefix: everything before the first digit.
    if let Some(i) = s.find(|c: char| c.is_ascii_digit()) {
        s = s[i..].to_string();
    } else {
        return Vec::new();
    }
    // `8u402` and `1.8.0_402`.
    if let Some((major, update)) = s.split_once('u') {
        if major.chars().all(|c| c.is_ascii_digit()) {
            let upd: String = update.chars().take_while(|c| c.is_ascii_digit()).collect();
            return vec![major.parse().unwrap_or(0), 0, upd.parse().unwrap_or(0)];
        }
    }
    if let Some(rest) = s.strip_prefix("1.8.0_") {
        let upd: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        return vec![8, 0, upd.parse().unwrap_or(0)];
    }
    let core: String = s
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    core.trim_end_matches('.')
        .split('.')
        .filter(|p| !p.is_empty())
        .map(|p| p.parse().unwrap_or(0))
        .collect()
}

/// Compares two Java versions by their numbers, missing parts as zero.
pub fn java_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    let (x, y) = (java_numbers(a), java_numbers(b));
    let n = x.len().max(y.len());
    for i in 0..n {
        let o = x.get(i).unwrap_or(&0).cmp(y.get(i).unwrap_or(&0));
        if o != std::cmp::Ordering::Equal {
            return o;
        }
    }
    std::cmp::Ordering::Equal
}

/// The feature line of a Java version (`21` for `21.0.4`, `8` for `8u402`).
pub fn java_line(v: &str) -> String {
    java_numbers(v)
        .first()
        .map(|n| n.to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn java_versions_read_every_way_they_are_written() {
        assert_eq!(java_numbers("21.0.4_7-jre"), vec![21, 0, 4]);
        assert_eq!(java_numbers("21.0.4+7"), vec![21, 0, 4]);
        assert_eq!(java_numbers("temurin-17.0.15+6"), vec![17, 0, 15]);
        assert_eq!(java_numbers("21.0.4-tem"), vec![21, 0, 4]);
        assert_eq!(java_numbers("8u402"), vec![8, 0, 402]);
        assert_eq!(java_numbers("1.8.0_402"), vec![8, 0, 402]);
        assert_eq!(java_numbers("17.0.21.1_p2-r0"), vec![17, 0, 21, 1]);
        assert_eq!(java_line("21-jre"), "21");
        assert_eq!(java_cmp("21.0.13", "21.0.13.0.3"), Ordering::Less);
        assert_eq!(java_cmp("21.0.13.1", "21.0.13.0.3"), Ordering::Greater);
        assert_eq!(java_cmp("21.0.4_7", "21.0.4+9"), Ordering::Equal);
        assert_eq!(
            advisory_sk("cve-2026-114938"),
            "RUNTIME_ADV#CVE-2026-114938"
        );
    }
}
