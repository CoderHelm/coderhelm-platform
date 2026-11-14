//! What a security fix's version bump changes, read before its run starts.
//!
//! A fix run once bumped a package across a major version with nothing but
//! the version to go on: the coder had to guess what broke, the lockfile was
//! written by hand, and the pull request said nothing a reviewer could check.
//! The rules:
//!
//! - Before a fix run starts, the worker reads each bump's release notes
//!   between the current and the fixed version: the package's source
//!   repository from its registry (npm, PyPI, crates.io, RubyGems; a Go
//!   module path names it), then its GitHub releases, else its changelog
//!   file ([`releases_between`], [`changelog_between`]). What they say is
//!   breaking ([`breaking_lines`]) and the APIs those lines name
//!   ([`api_names`]) go in the ticket, with where the repository uses them
//!   ([`CallSite`]). Nothing read is said so, never guessed.
//! - A lockfile is written by the repository's own package manager, never by
//!   hand: [`lock_tools`] names, per lockfile, the command that writes it.
//!   The coder changes the manifest; the worker runs that command in the
//!   sandbox and commits what it writes (`worker::passes::generators`).
//! - The pull request's "What to check" section is written from these
//!   facts, the files the pull request touches and its CI
//!   ([`what_to_check`]), per package for a bundle. CI that fails is stated
//!   as failing, never as a pass.
//!
//! - A bump that crosses a major version says why it has to (the advisory
//!   with no patched release on the installed line), and its notes are read
//!   before any other package's. When the package is only pulled in by
//!   another one, the notes name that parent and say whether it declares
//!   support for the new major ([`ParentSupport`]): the range the installed
//!   parent declares for the package, and the range its newest release
//!   declares. An override that puts a parent on a major it does not
//!   declare is said in those words.
//!
//! Everything here is pure; the reads are the worker's
//! (`worker::passes::upgrade_notes`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::security_fixes::{compare_versions, same_line, Bump};

/// Characters of release notes kept per package.
pub const EXCERPT_CHARS: usize = 2_500;
/// Breaking-change lines kept per package.
pub const MAX_BREAKING: usize = 15;
/// APIs searched for per package.
pub const MAX_APIS: usize = 10;
/// Call sites kept per package.
pub const MAX_CALL_SITES: usize = 25;
/// Characters the ticket's upgrade notes take at most, all packages.
pub const SECTION_CHARS: usize = 14_000;
/// Packages whose notes are read for one fix run.
pub const MAX_PACKAGES: usize = 8;
/// Parents read per package that is only pulled in by others.
pub const MAX_PARENTS: usize = 3;

/// Whether a package that pulls in a bumped one declares support for the
/// bump's version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ParentSupport {
    pub parent: String,
    /// The parent's installed version (empty when the dependency list
    /// doesn't say, or holds several).
    pub version: String,
    /// The range the installed parent declares for the package.
    pub declares: String,
    /// Whether the bump's version is inside that range (`None`: not read,
    /// or a range CoderHelm can't evaluate).
    pub admits: Option<bool>,
    /// The parent's newest release and what it declares.
    pub latest: String,
    pub latest_declares: String,
    pub latest_admits: Option<bool>,
    /// Why nothing could be read; empty when something was.
    pub missing: String,
}

/// Where the repository uses an API a breaking change names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CallSite {
    pub api: String,
    pub file: String,
    pub line: u64,
    pub text: String,
}

/// One version's notes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Release {
    pub version: String,
    pub url: String,
    pub body: String,
}

/// What one bump changes, as read before the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PackageNotes {
    pub package: String,
    pub ecosystem: String,
    pub from: String,
    pub to: String,
    pub major: bool,
    pub manifest: String,
    /// Where the notes were read (`the GitHub releases of acme/parser`).
    pub source: String,
    pub url: String,
    /// The versions whose notes were read, oldest first.
    pub versions: Vec<String>,
    pub breaking: Vec<String>,
    pub apis: Vec<String>,
    pub call_sites: Vec<CallSite>,
    pub excerpt: String,
    /// Why no notes were read; empty when they were.
    pub missing: String,
    /// The lockfile and the command that writes it, when known.
    pub lockfile: String,
    pub lock_command: String,
    /// Why the bump leaves its version line, for a major (the advisory with
    /// no patched release on the line).
    pub major_reason: String,
    /// The bumped package is in this repository's own manifest.
    pub direct: bool,
    /// The packages that pull it in, when it is only a transitive
    /// dependency, with whether each declares support for the new version.
    pub parents: Vec<ParentSupport>,
}

impl PackageNotes {
    pub fn for_bump(b: &Bump, ecosystem: &str) -> Self {
        PackageNotes {
            package: b.package.clone(),
            ecosystem: ecosystem.to_string(),
            from: b.from.clone(),
            to: b.to.clone(),
            major: b.major,
            manifest: b.manifest.clone(),
            ..Default::default()
        }
    }

    pub fn read(&self) -> bool {
        self.missing.is_empty() && !self.versions.is_empty()
    }
}

// ── The source repository ───────────────────────────────────────────────

/// `owner/name` of a GitHub URL in any of the forms registries write
/// (`git+https://github.com/o/r.git`, `git@github.com:o/r`, `github:o/r`,
/// `https://github.com/o/r/tree/main/packages/x`), or npm's `o/r` shorthand.
pub fn github_repo(url: &str) -> Option<(String, String)> {
    let u = url.trim();
    let rest = if let Some(r) = u.strip_prefix("github:") {
        r
    } else if let Some(i) = u.find("github.com") {
        u[i + "github.com".len()..].trim_start_matches([':', '/'])
    } else if !u.contains(':')
        && !u.starts_with('.')
        && u.matches('/').count() == 1
        && !u.contains(' ')
    {
        u
    } else {
        return None;
    };
    let mut parts = rest.split(['/', '#', '?']);
    let owner = parts.next()?.trim();
    let name = parts.next()?.trim().trim_end_matches(".git");
    let ok = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    };
    (ok(owner) && ok(name)).then(|| (owner.to_string(), name.to_string()))
}

/// The package's registry record to read its repository from.
pub fn registry_url(ecosystem: &str, package: &str, version: &str) -> Option<String> {
    let enc = |s: &str| urlencoding::encode(s).to_string();
    match ecosystem {
        "npm" => Some(format!(
            "https://registry.npmjs.org/{}/{}",
            package.replace('/', "%2F"),
            enc(version)
        )),
        "PyPI" => Some(format!("https://pypi.org/pypi/{}/json", enc(package))),
        "crates.io" => Some(format!("https://crates.io/api/v1/crates/{}", enc(package))),
        "RubyGems" => Some(format!(
            "https://rubygems.org/api/v1/gems/{}.json",
            enc(package)
        )),
        _ => None,
    }
}

/// A source repository: `owner/name`, and the package's folder in it (a
/// monorepo's `packages/x`), when the registry says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRepo {
    pub owner: String,
    pub name: String,
    pub dir: Option<String>,
}

/// The source repository a registry record names. Go modules name theirs.
pub fn source_repo(ecosystem: &str, package: &str, meta: &Value) -> Option<SourceRepo> {
    let plain = |url: &str| {
        github_repo(url).map(|(owner, name)| SourceRepo {
            owner,
            name,
            dir: None,
        })
    };
    match ecosystem {
        "Go" => {
            let p = package.strip_prefix("github.com/")?;
            let mut it = p.split('/');
            let (o, n) = (it.next()?, it.next()?);
            plain(&format!("github.com/{o}/{n}"))
        }
        "npm" => {
            let repo = &meta["repository"];
            let (url, dir) = match repo {
                Value::String(s) => (s.as_str(), None),
                Value::Object(_) => (
                    repo["url"].as_str().unwrap_or(""),
                    repo["directory"]
                        .as_str()
                        .map(|d| d.trim_matches('/').to_string()),
                ),
                _ => ("", None),
            };
            let mut r = plain(url).or_else(|| meta["homepage"].as_str().and_then(plain))?;
            r.dir = dir.filter(|d| !d.is_empty());
            Some(r)
        }
        "PyPI" => {
            let urls = meta["info"]["project_urls"].as_object();
            let preferred = [
                "Source",
                "Source Code",
                "Repository",
                "Code",
                "GitHub",
                "Changelog",
            ];
            let from_urls = urls.and_then(|m| {
                preferred
                    .iter()
                    .filter_map(|k| m.get(*k).and_then(Value::as_str))
                    .chain(m.values().filter_map(Value::as_str))
                    .find_map(plain)
            });
            from_urls.or_else(|| meta["info"]["home_page"].as_str().and_then(plain))
        }
        "crates.io" => meta["crate"]["repository"]
            .as_str()
            .and_then(plain)
            .or_else(|| meta["crate"]["homepage"].as_str().and_then(plain)),
        "RubyGems" => ["source_code_uri", "changelog_uri", "homepage_uri"]
            .iter()
            .find_map(|k| meta[*k].as_str().and_then(plain)),
        _ => None,
    }
}

// ── A transitive package's parents ──────────────────────────────────────

/// A registry's record of one version of a package, with what it depends
/// on. `version`: `None` for the newest release. One row per ecosystem;
/// `None` where the registry has no such record (the parent's support is
/// then said to be unread, never guessed).
pub fn deps_url(ecosystem: &str, package: &str, version: Option<&str>) -> Option<String> {
    let enc = |s: &str| urlencoding::encode(s).to_string();
    match (ecosystem, version) {
        ("npm", v) => Some(format!(
            "https://registry.npmjs.org/{}/{}",
            package.replace('/', "%2F"),
            enc(v.unwrap_or("latest"))
        )),
        ("PyPI", Some(v)) => Some(format!(
            "https://pypi.org/pypi/{}/{}/json",
            enc(package),
            enc(v)
        )),
        ("PyPI", None) => Some(format!("https://pypi.org/pypi/{}/json", enc(package))),
        ("crates.io", Some(v)) => Some(format!(
            "https://crates.io/api/v1/crates/{}/{}/dependencies",
            enc(package),
            enc(v)
        )),
        ("RubyGems", Some(v)) => Some(format!(
            "https://rubygems.org/api/v2/rubygems/{}/versions/{}.json",
            enc(package),
            enc(v)
        )),
        ("RubyGems", None) => Some(format!(
            "https://rubygems.org/api/v1/gems/{}.json",
            enc(package)
        )),
        _ => None,
    }
}

/// The version a [`deps_url`] record is of.
pub fn record_version(ecosystem: &str, meta: &Value) -> Option<String> {
    match ecosystem {
        "npm" | "RubyGems" => meta["version"].as_str().map(String::from),
        "PyPI" => meta["info"]["version"].as_str().map(String::from),
        _ => None,
    }
}

/// The range a [`deps_url`] record declares for `child`, as the registry
/// writes it. `None` when the record doesn't depend on it.
pub fn declared_range(ecosystem: &str, meta: &Value, child: &str) -> Option<String> {
    match ecosystem {
        "npm" => ["dependencies", "peerDependencies", "optionalDependencies"]
            .iter()
            .find_map(|k| meta[*k][child].as_str().map(String::from)),
        "PyPI" => {
            let want = norm_name(child);
            meta["info"]["requires_dist"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                // An extra's requirement is not what an install pulls in.
                .filter(|r| !r.contains("extra =="))
                .find_map(|r| {
                    let r = r.split(';').next().unwrap_or(r).trim();
                    let end = r
                        .find(|c: char| {
                            !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
                        })
                        .unwrap_or(r.len());
                    (norm_name(&r[..end]) == want).then(|| {
                        let spec = r[end..]
                            .trim()
                            .trim_start_matches('(')
                            .trim_end_matches(')')
                            .trim();
                        if spec.is_empty() || spec.starts_with('[') {
                            "*".to_string()
                        } else {
                            spec.to_string()
                        }
                    })
                })
        }
        "crates.io" => meta["dependencies"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|d| {
                d["crate_id"].as_str() == Some(child)
                    && d["kind"].as_str().is_none_or(|k| k == "normal")
            })
            .and_then(|d| d["req"].as_str().map(String::from)),
        "RubyGems" => meta["dependencies"]["runtime"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|d| d["name"].as_str() == Some(child))
            .and_then(|d| d["requirements"].as_str().map(String::from)),
        _ => None,
    }
}

/// The leading numbers of a version or partial version (`1.2.x` is `[1, 2]`).
fn nums(v: &str) -> Vec<u64> {
    v.trim()
        .trim_start_matches(['v', '='])
        .split(['.', '-', '+'])
        .map_while(|p| p.parse::<u64>().ok())
        .collect()
}

fn starts_with_nums(v: &str, prefix: &[u64]) -> bool {
    let n = nums(v);
    n.len() >= prefix.len() && n[..prefix.len()] == *prefix
}

/// `^base`: at or above it, below the next breaking version.
fn caret(base: &str, v: &str) -> bool {
    use std::cmp::Ordering::Less;
    let b = nums(base);
    let keep = match b.as_slice() {
        [] => return true,
        [0, 0, ..] => 3,
        [0, ..] => 2,
        _ => 1,
    };
    compare_versions(v, base) != Less && starts_with_nums(v, &b[..keep.min(b.len())])
}

/// `~base`: at or above it, on the same minor (or major, for `~1`).
fn tilde(base: &str, v: &str) -> bool {
    use std::cmp::Ordering::Less;
    let b = nums(base);
    b.is_empty() || (compare_versions(v, base) != Less && starts_with_nums(v, &b[..b.len().min(2)]))
}

/// `~> base` and `~= base`: at or above it, with all but its last part kept.
fn pessimistic(base: &str, v: &str) -> bool {
    use std::cmp::Ordering::Less;
    let b = nums(base);
    compare_versions(v, base) != Less && (b.len() < 2 || starts_with_nums(v, &b[..b.len() - 1]))
}

/// One comparator of a range (`>=1.2`, `^7`, `~>2.1`, `1.x`).
fn comparator_admits(ecosystem: &str, c: &str, v: &str) -> Option<bool> {
    use std::cmp::Ordering::*;
    let c = c.trim();
    if c.is_empty() || c == "*" || c.eq_ignore_ascii_case("x") {
        return Some(true);
    }
    let versioned = |base: &str| base.chars().next().is_some_and(|x| x.is_ascii_digit());
    for (op, f) in [
        ("~>", pessimistic as fn(&str, &str) -> bool),
        ("~=", pessimistic),
        ("^", caret),
        ("~", tilde),
    ] {
        if let Some(base) = c.strip_prefix(op) {
            let base = base.trim();
            return versioned(base).then(|| f(base, v));
        }
    }
    for op in [">=", "<=", "==", "!=", ">", "<", "="] {
        if let Some(base) = c.strip_prefix(op) {
            let base = base.trim();
            if !versioned(base) {
                return None;
            }
            // `==1.4.*`, `<=2` and `>2`: a partial version stands for
            // every version that starts with it.
            let prefix = nums(base);
            let partial = prefix.len() < 3;
            let within = starts_with_nums(v, &prefix);
            let wild = base.contains('*') || base.to_ascii_lowercase().contains('x');
            let o = compare_versions(v, base);
            return Some(match op {
                ">=" => o != Less,
                "<=" => o != Greater || (partial && within),
                ">" => o == Greater && !(partial && within),
                "<" => o == Less,
                "!=" if wild => !within,
                "!=" => o != Equal,
                _ if wild => within,
                _ => o == Equal,
            });
        }
    }
    if !versioned(c) {
        // A tag, a URL, a workspace or git reference: not a range.
        return None;
    }
    let partial = nums(c).len() < 3 || c.contains('*') || c.to_ascii_lowercase().contains(".x");
    Some(match ecosystem {
        // Cargo reads a bare version as a caret range.
        "crates.io" => caret(c, v),
        _ if partial => starts_with_nums(v, &nums(c)),
        _ => compare_versions(v, c) == Equal,
    })
}

/// Whether `version` is inside a declared dependency range, in the range
/// syntax of the ecosystem (npm's `^7 || ^8`, Cargo's `1.2, <1.9`, PyPI's
/// `>=1,<3`, RubyGems' `~> 2.1`). `None` when the range isn't one CoderHelm
/// can evaluate (a tag, a URL, a git reference).
pub fn range_admits(ecosystem: &str, range: &str, version: &str) -> Option<bool> {
    let mut any = Some(false);
    for alt in range.split("||") {
        // Operators written apart from their version (`>= 1.2`).
        let mut alt = alt.trim().to_string();
        for op in [">=", "<=", "~>", "~=", "==", "!=", ">", "<", "^", "~", "="] {
            alt = alt.replace(&format!("{op} "), op);
        }
        let parts: Vec<&str> = if ecosystem == "npm" {
            alt.split_whitespace().collect()
        } else {
            alt.split(',').map(str::trim).collect()
        };
        // npm's `1.2.3 - 2.3.4`.
        let set = if let [lo, "-", hi] = parts.as_slice() {
            match (
                comparator_admits(ecosystem, &format!(">={lo}"), version),
                comparator_admits(ecosystem, &format!("<={hi}"), version),
            ) {
                (Some(a), Some(b)) => Some(a && b),
                _ => None,
            }
        } else {
            let mut all = Some(true);
            for c in &parts {
                match (all, comparator_admits(ecosystem, c, version)) {
                    (_, None) => {
                        all = None;
                        break;
                    }
                    (Some(a), Some(b)) => all = Some(a && b),
                    (None, _) => {}
                }
            }
            all
        };
        match set {
            Some(true) => return Some(true),
            Some(false) => {}
            None => any = None,
        }
    }
    any
}

/// The line of one parent, for the ticket and the pull request.
pub fn parent_line(p: &ParentSupport, package: &str, to: &str) -> String {
    if p.declares.is_empty() && p.latest_declares.is_empty() {
        return format!(
            "Pulled in by `{}`: whether it supports {to} could not be read ({}).",
            p.parent,
            if p.missing.is_empty() {
                "its registry record names no range for the package"
            } else {
                p.missing.as_str()
            }
        );
    }
    let mut s = format!("Pulled in by `{}`", p.parent);
    if !p.version.is_empty() {
        s.push_str(&format!(" {}", p.version));
    }
    if p.declares.is_empty() {
        s.push_str(", whose installed version's declared range could not be read.");
    } else {
        s.push_str(&format!(
            ", which declares `{package}: {}`: {to} is {}.",
            p.declares,
            match p.admits {
                Some(true) => "inside the range it declares",
                Some(false) =>
                    "outside the range it declares, so the override puts it on a version it does not declare support for",
                None => "not comparable with that range by CoderHelm",
            }
        ));
    }
    if !p.latest.is_empty() && p.latest != p.version && !p.latest_declares.is_empty() {
        s.push_str(&format!(
            " Its newest release {} declares `{}`, which {}.",
            p.latest,
            p.latest_declares,
            match p.latest_admits {
                Some(true) => format!(
                    "admits {to}: upgrading `{}` is the way its maintainers support",
                    p.parent
                ),
                Some(false) => format!("does not admit {to} either"),
                None => "CoderHelm can't compare".to_string(),
            }
        ));
    }
    s
}

/// What a major bump's notes say about why it is one and who uses the
/// package: this repository's own code, or the parents that pull it in.
pub fn usage_lines(n: &PackageNotes) -> Vec<String> {
    let mut out = vec![];
    if !n.major {
        return out;
    }
    if !n.major_reason.is_empty() {
        out.push(format!("Why a major version: {}.", n.major_reason));
    }
    if n.direct {
        out.push(
            "A direct dependency: this repository's own code calls it (its call sites are listed here)."
                .to_string(),
        );
    }
    for p in &n.parents {
        out.push(parent_line(p, &n.package, &n.to));
    }
    out
}

// ── Versions in tags and headings ───────────────────────────────────────

fn norm_name(s: &str) -> String {
    s.to_ascii_lowercase()
        .trim_start_matches('@')
        .replace(['_', '.'], "-")
}

/// The version a release tag names for `package`: `v1.2.3`, `1.2.3`,
/// `pkg@1.2.3`, `@scope/pkg@1.2.3`, `pkg-v1.2.3`, `release-1.2.3`. `None`
/// for another package's tag in a monorepo, or no version.
pub fn version_of_tag(tag: &str, package: &str) -> Option<String> {
    let tag = tag.trim().trim_start_matches("refs/tags/");
    let (prefix, ver) = match tag.rfind('@').filter(|i| *i > 0) {
        Some(i) => (&tag[..i], &tag[i + 1..]),
        None => {
            let bytes: Vec<char> = tag.chars().collect();
            let mut at = None;
            for (i, c) in bytes.iter().enumerate() {
                if c.is_ascii_digit()
                    && (i == 0 || matches!(bytes[i - 1], 'v' | 'V' | '-' | '_' | '/'))
                {
                    at = Some(i);
                    break;
                }
            }
            let i = at?;
            let byte_i: usize = bytes[..i].iter().map(|c| c.len_utf8()).sum();
            (&tag[..byte_i], &tag[byte_i..])
        }
    };
    let ver = ver.trim_start_matches(['v', 'V']);
    if !ver.chars().next()?.is_ascii_digit() || !ver.contains('.') {
        return None;
    }
    let prefix = prefix
        .trim_end_matches(['v', 'V'])
        .trim_end_matches(['-', '_', '/']);
    if !prefix.is_empty() {
        let p = norm_name(prefix);
        let full = norm_name(package);
        let last = norm_name(package.rsplit('/').next().unwrap_or(package));
        let generic = matches!(p.as_str(), "release" | "version" | "rel" | "tag");
        if !(generic || p == full || p == last || full.ends_with(&format!("/{p}"))) {
            return None;
        }
    }
    Some(ver.to_string())
}

/// After `from`, up to and including `to`.
pub fn in_window(v: &str, from: &str, to: &str) -> bool {
    use std::cmp::Ordering::*;
    compare_versions(v, from) == Greater && compare_versions(v, to) != Greater
}

/// GitHub releases (`GET /repos/{o}/{r}/releases`) of `package` between
/// `from` and `to`, oldest first.
pub fn releases_between(releases: &[Value], package: &str, from: &str, to: &str) -> Vec<Release> {
    let mut out: Vec<Release> = releases
        .iter()
        .filter(|r| r["draft"].as_bool() != Some(true))
        .filter_map(|r| {
            let tag = r["tag_name"].as_str()?;
            let v = version_of_tag(tag, package)?;
            in_window(&v, from, to).then(|| Release {
                version: v,
                url: r["html_url"].as_str().unwrap_or("").to_string(),
                body: r["body"].as_str().unwrap_or("").trim().to_string(),
            })
        })
        .collect();
    out.sort_by(|a, b| compare_versions(&a.version, &b.version));
    out.dedup_by(|a, b| a.version == b.version);
    out
}

/// A Markdown heading's level and text.
fn heading(line: &str) -> Option<(usize, &str)> {
    let t = line.trim_start();
    let level = t.chars().take_while(|c| *c == '#').count();
    (level > 0 && t[level..].starts_with([' ', '['])).then(|| (level, t[level..].trim()))
}

/// The first version a heading or a History-style line names
/// (`## [3.0.0] - 2026-01-02`, `## v3.0.0`, `3.0.0 / 2026-01-02`).
fn version_in(text: &str) -> Option<String> {
    let re = regex::Regex::new(r"(?:^|[\s\[(v])v?(\d+\.\d+(?:\.\d+)?(?:-[0-9A-Za-z.\-]+)?)")
        .expect("version pattern");
    re.captures(text).map(|c| c[1].to_string())
}

/// The sections of a changelog file (`CHANGELOG.md`, `HISTORY.md`) for the
/// versions between `from` and `to`, oldest first. A section runs from its
/// version's heading to the next heading at that level or above.
pub fn changelog_between(text: &str, from: &str, to: &str) -> Vec<Release> {
    let lines: Vec<&str> = text.lines().collect();
    let mut sections: Vec<(String, Vec<&str>)> = vec![];
    let mut level: Option<usize> = None;
    let mut in_fence = false;
    let mut i = 0;
    while i < lines.len() {
        let l = lines[i];
        if l.trim_start().starts_with("```") {
            in_fence = !in_fence;
        }
        let underline = lines
            .get(i + 1)
            .is_some_and(|n| n.trim().len() >= 3 && n.trim().chars().all(|c| c == '=' || c == '-'));
        let found = if in_fence {
            None
        } else if let Some((lv, h)) = heading(l) {
            match version_in(h) {
                Some(v) if level.is_none_or(|top| lv <= top) => {
                    level = Some(lv);
                    Some(Some(v))
                }
                // A heading at the versions' level without one ends a
                // section (an "Unreleased" heading).
                None if level.is_some_and(|top| lv <= top) => Some(None),
                _ => None,
            }
        } else if underline && !l.trim().is_empty() {
            version_in(l.trim())
                .filter(|_| {
                    l.trim()
                        .chars()
                        .next()
                        .is_some_and(|c| c.is_ascii_digit() || c == 'v')
                })
                .map(Some)
        } else {
            None
        };
        match found {
            Some(Some(v)) => {
                sections.push((v, vec![]));
                if underline && heading(l).is_none() {
                    i += 1;
                }
            }
            Some(None) => sections.push((String::new(), vec![])),
            None => {
                if let Some((_, body)) = sections.last_mut() {
                    body.push(l);
                }
            }
        }
        i += 1;
    }
    let mut out: Vec<Release> = sections
        .into_iter()
        .filter(|(v, _)| !v.is_empty() && in_window(v, from, to))
        .map(|(v, body)| Release {
            version: v,
            url: String::new(),
            body: body.join("\n").trim().to_string(),
        })
        .collect();
    out.sort_by(|a, b| compare_versions(&a.version, &b.version));
    out.dedup_by(|a, b| a.version == b.version);
    out
}

/// The changelog file names tried, in order, in the package's folder and
/// at the repository's root.
pub const CHANGELOG_FILES: &[&str] = &[
    "CHANGELOG.md",
    "Changelog.md",
    "changelog.md",
    "CHANGES.md",
    "HISTORY.md",
    "History.md",
    "RELEASES.md",
    "NEWS.md",
    "CHANGELOG.rst",
    "CHANGELOG",
];

// ── What breaks ─────────────────────────────────────────────────────────

fn clean_line(l: &str) -> String {
    let t = l
        .trim()
        .trim_start_matches(['-', '*', '+', '>'])
        .trim()
        .replace("**", "");
    crate::truncate_str(t.trim(), 300).to_string()
}

/// Words that make a line a breaking change.
fn says_breaking(lower: &str) -> bool {
    [
        "breaking",
        "removed",
        "no longer",
        "renamed",
        "dropped support",
        "drop support",
        "drops support",
        "now requires",
        "requires node",
        "minimum supported",
        "is now required",
        "changed the default",
        "default is now",
        "defaults to",
        "replaced by",
        "has been replaced",
        "must now",
        "incompatible",
        "⚠",
    ]
    .iter()
    .any(|w| lower.contains(w))
}

/// The lines release notes say are breaking: every item under a heading
/// that says "breaking", and any line that says something was removed,
/// renamed, no longer works, or now requires something.
pub fn breaking_lines(notes: &str) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    let mut under: Option<usize> = None;
    for l in notes.lines() {
        let lower = l.to_ascii_lowercase();
        if let Some((lv, h)) = heading(l) {
            let hl = h.to_ascii_lowercase();
            if hl.contains("breaking") || hl.contains("migration") || hl.contains("upgrad") {
                under = Some(lv);
                continue;
            }
            if under.is_some_and(|u| lv <= u) {
                under = None;
            }
            continue;
        }
        // "BREAKING CHANGES:" as a plain line opens the same kind of list.
        let bare = lower.trim().trim_end_matches(':');
        if matches!(bare, "breaking changes" | "breaking change" | "breaking") {
            under = Some(usize::MAX);
            continue;
        }
        let item = l.trim_start().starts_with(['-', '*', '+']);
        let take = (under.is_some() && item) || says_breaking(&lower);
        if take {
            let c = clean_line(l);
            if c.len() >= 8 && !out.contains(&c) {
                out.push(c);
            }
        }
        if out.len() >= MAX_BREAKING {
            break;
        }
    }
    out
}

/// The APIs breaking lines name in code spans (`` `parse()` ``,
/// `` `Client.connect` ``, `` `lib/fp` ``), as searchable names.
pub fn api_names(lines: &[String], package: &str) -> Vec<String> {
    const SKIP: &[&str] = &[
        "true",
        "false",
        "null",
        "undefined",
        "none",
        "npm",
        "node",
        "yarn",
        "pnpm",
        "pip",
        "cargo",
        "go",
        "async",
        "await",
        "string",
        "number",
        "object",
        "function",
        "class",
        "import",
        "require",
        "export",
        "default",
    ];
    let mut out: Vec<String> = vec![];
    for l in lines {
        for (i, span) in l.split('`').enumerate() {
            if i % 2 == 0 {
                continue;
            }
            let s = span.trim();
            let s = s.split('(').next().unwrap_or(s).trim();
            let s = s.trim_end_matches([';', ',', '.']);
            if s.len() < 3
                || s.contains(' ')
                || s.starts_with('-')
                || s.chars().next().is_some_and(|c| c.is_ascii_digit())
                || SKIP.contains(&s.to_ascii_lowercase().as_str())
                || s.eq_ignore_ascii_case(package)
            {
                continue;
            }
            let ok = s.chars().all(|c| {
                c.is_ascii_alphanumeric() || matches!(c, '_' | '$' | '.' | '/' | ':' | '@' | '-')
            });
            if !ok {
                continue;
            }
            // A path is only an API when it is the package's own subpath.
            if s.contains('/') && !s.starts_with(package) {
                continue;
            }
            if !out.iter().any(|o| o == s) {
                out.push(s.to_string());
            }
            if out.len() >= MAX_APIS {
                return out;
            }
        }
    }
    out
}

/// What a code search looks for to find an API's uses: the last part of a
/// dotted name (`Client.connect` is found as `connect(` and `.connect`),
/// unless that part is too short to mean anything alone.
pub fn search_term(api: &str) -> String {
    if api.contains('/') {
        return api.to_string();
    }
    let last = api.rsplit(['.', ':']).next().unwrap_or(api);
    if last.len() >= 4 {
        last.to_string()
    } else {
        api.to_string()
    }
}

/// Files a call site never comes from: lockfiles, manifests, changelogs,
/// vendored and built code.
pub fn is_call_site_file(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    !(crate::security_fixes::is_dependency_file(path)
        || lower.ends_with(".lock")
        || lower.ends_with(".sum")
        || lower.ends_with(".md")
        || lower.ends_with(".min.js")
        || lower.ends_with(".map")
        || name.starts_with("changelog")
        || [
            "node_modules/",
            "vendor/",
            "dist/",
            "build/",
            ".next/",
            "target/",
        ]
        .iter()
        .any(|d| lower.starts_with(d) || lower.contains(&format!("/{d}"))))
}

/// Call sites from a code search's results (`path` and `"12: text"`
/// fragments), the ones in real code only.
pub fn call_sites(api: &str, results: &[(String, Vec<String>)]) -> Vec<CallSite> {
    let mut out = vec![];
    for (path, fragments) in results {
        if !is_call_site_file(path) || path.starts_with('(') {
            continue;
        }
        for f in fragments {
            let Some((n, text)) = f.split_once(": ") else {
                continue;
            };
            let Ok(line) = n.trim().parse::<u64>() else {
                continue;
            };
            out.push(CallSite {
                api: api.to_string(),
                file: path.clone(),
                line,
                text: crate::truncate_str(text.trim(), 200).to_string(),
            });
        }
    }
    out
}

/// Fills a package's notes from what was read: the releases (or changelog
/// sections), and where they were read.
pub fn fill(n: &mut PackageNotes, source: &str, url: &str, read: &[Release]) {
    n.source = source.to_string();
    n.url = url.to_string();
    n.versions = read.iter().map(|r| r.version.clone()).collect();
    let all: String = read
        .iter()
        .map(|r| format!("## {}\n{}\n", r.version, r.body))
        .collect();
    n.breaking = breaking_lines(&all);
    n.apis = api_names(&n.breaking, &n.package);
    let mut ex = String::new();
    for r in read {
        let piece = format!("{}: {}\n", r.version, r.body.replace('\n', " "));
        ex.push_str(&piece);
    }
    n.excerpt = crate::truncate_str(ex.trim(), EXCERPT_CHARS).to_string();
    if read.is_empty() {
        n.missing = format!(
            "{source} has no notes for the versions after {} up to {}",
            n.from, n.to
        );
    } else {
        n.missing.clear();
    }
}

// ── The lockfile's own tool ─────────────────────────────────────────────

/// A lockfile and the command that writes it, run from the repository root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockTool {
    /// `pnpm`, `yarn`, `npm`, `bun`, `poetry`, `uv`, `pipenv`, `pip-tools`,
    /// `go`, `cargo`, `bundler`.
    pub manager: &'static str,
    pub lockfile: String,
    /// The files it reads (globs from the root): a change to one means the
    /// lockfile is written again.
    pub inputs: Vec<String>,
    pub command: String,
}

fn dir_of(path: &str) -> &str {
    path.rsplit_once('/').map(|(d, _)| d).unwrap_or("")
}

fn under(dir: &str, rel: &str) -> String {
    if dir.is_empty() {
        rel.to_string()
    } else {
        format!("{dir}/{rel}")
    }
}

fn in_dir(dir: &str, cmd: &str) -> String {
    if dir.is_empty() {
        cmd.to_string()
    } else {
        format!("cd '{dir}' && {cmd}")
    }
}

/// Every lockfile in the tree with its package manager's command, which
/// updates the lockfile to what the manifests say and changes nothing else
/// it doesn't have to (no install scripts run).
pub fn lock_tools(tree: &[String]) -> Vec<LockTool> {
    use crate::e2e::test_package::{ensure_manager, Manager};
    let has = |p: &str| tree.iter().any(|t| t == p);
    let mut out = vec![];
    let mut paths: Vec<&String> = tree.iter().collect();
    paths.sort();
    for p in paths {
        let lower = p.to_ascii_lowercase();
        if ["node_modules/", "vendor/"]
            .iter()
            .any(|d| lower.contains(d))
        {
            continue;
        }
        let name = p.rsplit('/').next().unwrap_or(p);
        let dir = dir_of(p);
        let js = vec![under(dir, "**/package.json")];
        let tool = match name {
            "pnpm-lock.yaml" => Some((
                "pnpm",
                {
                    let mut i = js.clone();
                    i.push(under(dir, "pnpm-workspace.yaml"));
                    i
                },
                format!(
                    "{}pnpm install --lockfile-only --ignore-scripts",
                    ensure_manager(Manager::Pnpm)
                ),
            )),
            "yarn.lock" => {
                let berry = has(&under(dir, ".yarnrc.yml"));
                Some((
                    "yarn",
                    js.clone(),
                    if berry {
                        format!(
                            "{}yarn install --mode=update-lockfile",
                            ensure_manager(Manager::Yarn)
                        )
                    } else {
                        format!(
                            "{}yarn install --ignore-scripts --non-interactive --ignore-engines",
                            ensure_manager(Manager::Yarn)
                        )
                    },
                ))
            }
            "package-lock.json" | "npm-shrinkwrap.json" => Some((
                "npm",
                js.clone(),
                "npm install --package-lock-only --ignore-scripts --no-audit --no-fund".into(),
            )),
            "bun.lock" | "bun.lockb" => Some((
                "bun",
                js.clone(),
                format!(
                    "{}bun install --lockfile-only --ignore-scripts",
                    ensure_manager(Manager::Bun)
                ),
            )),
            "poetry.lock" => Some((
                "poetry",
                vec![under(dir, "pyproject.toml")],
                "(command -v poetry >/dev/null 2>&1 || pip install -q poetry) && (poetry lock --no-update 2>/dev/null || poetry lock)".into(),
            )),
            "uv.lock" => Some((
                "uv",
                vec![under(dir, "pyproject.toml")],
                "(command -v uv >/dev/null 2>&1 || pip install -q uv) && uv lock".into(),
            )),
            "Pipfile.lock" => Some((
                "pipenv",
                vec![under(dir, "Pipfile")],
                "(command -v pipenv >/dev/null 2>&1 || pip install -q pipenv) && pipenv lock"
                    .into(),
            )),
            "requirements.txt" if has(&under(dir, "requirements.in")) => Some((
                "pip-tools",
                vec![under(dir, "requirements.in")],
                "(command -v pip-compile >/dev/null 2>&1 || pip install -q pip-tools) && pip-compile --quiet requirements.in".into(),
            )),
            "go.sum" => Some(("go", vec![under(dir, "go.mod")], "go mod tidy".into())),
            "Cargo.lock" => Some((
                "cargo",
                vec![under(dir, "**/Cargo.toml")],
                "cargo metadata --format-version 1 >/dev/null".into(),
            )),
            "Gemfile.lock" => Some(("bundler", vec![under(dir, "Gemfile")], "bundle lock".into())),
            _ => None,
        };
        if let Some((manager, inputs, cmd)) = tool {
            out.push(LockTool {
                manager,
                lockfile: p.clone(),
                inputs,
                command: in_dir(dir, &cmd),
            });
        }
    }
    // A workspace's nested lockfile belongs to its own folder: the root's
    // tool doesn't read the files a nested one does.
    let lockdirs: Vec<String> = out
        .iter()
        .map(|t| dir_of(&t.lockfile).to_string())
        .collect();
    for t in out.iter_mut() {
        let mine = dir_of(&t.lockfile).to_string();
        let nested: Vec<&String> = lockdirs
            .iter()
            .filter(|d| **d != mine && (mine.is_empty() || d.starts_with(&format!("{mine}/"))))
            .collect();
        if !nested.is_empty() {
            for d in nested {
                t.inputs.push(format!("!{d}/**"));
            }
        }
    }
    out
}

/// The lock tool that writes a bump's lockfile: the bump's manifest when it
/// is a lockfile, else the nearest lockfile of its folder or above.
pub fn lock_tool_for<'a>(
    tools: &'a [LockTool],
    manifest: &str,
    ecosystem: &str,
) -> Option<&'a LockTool> {
    if let Some(t) = tools.iter().find(|t| t.lockfile == manifest) {
        return Some(t);
    }
    let fits = |m: &str| match ecosystem {
        "npm" => matches!(m, "pnpm" | "yarn" | "npm" | "bun"),
        "PyPI" => matches!(m, "poetry" | "uv" | "pipenv" | "pip-tools"),
        "Go" => m == "go",
        "crates.io" => m == "cargo",
        "RubyGems" => m == "bundler",
        _ => false,
    };
    let mut dir = dir_of(manifest).to_string();
    loop {
        if let Some(t) = tools
            .iter()
            .find(|t| fits(t.manager) && dir_of(&t.lockfile) == dir)
        {
            return Some(t);
        }
        if dir.is_empty() {
            return None;
        }
        dir = dir_of(&dir).to_string();
    }
}

// ── The ticket's section ────────────────────────────────────────────────

/// The ticket's upgrade notes: per package, where the notes were read, the
/// breaking changes, where the repository uses what changed, an excerpt,
/// and the lockfile's command. Bounded to [`SECTION_CHARS`].
pub fn ticket_section(notes: &[PackageNotes]) -> String {
    if notes.is_empty() {
        return String::new();
    }
    let mut s = String::from("\n## Upgrade notes\nRead by CoderHelm before this run, from the packages' own release notes.\n");
    for n in notes {
        let mut p = format!(
            "\n### `{}` {} to {}{}\n",
            n.package,
            n.from,
            n.to,
            if n.major { " (major version)" } else { "" }
        );
        for l in usage_lines(n) {
            p.push_str(&format!("{l}\n"));
        }
        if n.read() {
            p.push_str(&format!(
                "Read from {} ({}){}.\n",
                n.source,
                n.versions.join(", "),
                if n.url.is_empty() {
                    String::new()
                } else {
                    format!(": {}", n.url)
                }
            ));
            if n.breaking.is_empty() {
                p.push_str("The notes name no breaking change.\n");
            } else {
                p.push_str("Breaking changes the notes name:\n");
                for b in &n.breaking {
                    p.push_str(&format!("- {b}\n"));
                }
            }
            if !n.apis.is_empty() {
                if n.call_sites.is_empty() {
                    p.push_str(&format!(
                        "No use of {} was found in this repository by a search before the run; search again before you decide nothing needs to change.\n",
                        n.apis.iter().map(|a| format!("`{a}`")).collect::<Vec<_>>().join(", ")
                    ));
                } else {
                    p.push_str("Where this repository uses what changed (a search before the run; there may be more):\n");
                    for c in &n.call_sites {
                        p.push_str(&format!(
                            "- `{}:{}` uses `{}`: {}\n",
                            c.file,
                            c.line,
                            c.api,
                            crate::truncate_str(&c.text, 140)
                        ));
                    }
                }
            }
            if !n.excerpt.is_empty() {
                p.push_str(&format!(
                    "Release notes (excerpt): {}\n",
                    crate::truncate_str(&n.excerpt, 1_200)
                ));
            }
        } else {
            p.push_str(&format!(
                "No release notes could be read ({}). Read the package's upgrade guide before changing code, and say in the description that the notes weren't found.\n",
                if n.missing.is_empty() { "nothing found" } else { n.missing.as_str() }
            ));
        }
        if !n.lockfile.is_empty() {
            p.push_str(&format!(
                "Lockfile: `{}` is written by `{}` (CoderHelm runs it after your change).\n",
                n.lockfile, n.lock_command
            ));
        }
        if s.len() + p.len() > SECTION_CHARS {
            s.push_str(&format!(
                "\n(Notes for `{}` and later packages are left out for length: read their release notes yourself.)\n",
                n.package
            ));
            break;
        }
        s.push_str(&p);
    }
    s
}

// ── The pull request's "What to check" ──────────────────────────────────

/// The pull request's CI, as the section states it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CiState {
    /// `passing`, `failing`, `pending`, or empty when not read yet.
    pub state: String,
    /// The checks that fail.
    pub failing: Vec<String>,
    /// The checks that ran (passed, failed or running).
    pub checks: Vec<String>,
}

/// What the section is written from, beyond the bumps.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WhatToCheck {
    pub notes: Vec<PackageNotes>,
    /// The files the pull request changes.
    pub files: Vec<String>,
    pub ci: CiState,
    /// A dependency fix run's account (`dep_fix::DepRun::report_md`): the
    /// bumps applied, the ones deferred with why, and the checks run in the
    /// sandbox. Empty for a run without one.
    pub dep_report: String,
    /// What the dependency run recorded for its head commit
    /// (`upgrade_quality::UpgradeFacts`): the reconciled advisory count,
    /// the families, the verification set's results, why each major is
    /// safe and what its behaviour probe showed.
    pub upgrade_facts: Option<crate::upgrade_quality::UpgradeFacts>,
    /// The head the description is written for: facts recorded on another
    /// commit are shown as that commit's.
    pub head_sha: String,
}

impl WhatToCheck {
    /// From a ticket record's `notes` (JSON); empty when it has none.
    pub fn from_notes(json: &str) -> Self {
        WhatToCheck {
            notes: serde_json::from_str(json).unwrap_or_default(),
            ..Default::default()
        }
    }
}

/// A file a person should look at as configuration.
fn is_config(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    name.starts_with(".env")
        || name.contains(".config.")
        || name.ends_with("rc")
        || name.ends_with("rc.json")
        || name.ends_with("rc.js")
        || name.ends_with(".yml")
        || name.ends_with(".yaml") && !name.starts_with("pnpm-lock")
        || name.ends_with(".toml") && name != "cargo.toml" && name != "pyproject.toml"
        || name == "dockerfile"
        || name.ends_with(".ini")
        || name == "tsconfig.json"
}

/// What changes at runtime or in configuration, among the breaking lines.
fn runtime_notes(n: &PackageNotes) -> Vec<String> {
    n.breaking
        .iter()
        .filter(|b| {
            let l = b.to_ascii_lowercase();
            [
                "default",
                "runtime",
                "behavio",
                "config",
                "option",
                "environment",
                "env ",
                "node ",
                "node.js",
                "python ",
                "requires",
                "timeout",
                "header",
                "esm",
                "commonjs",
            ]
            .iter()
            .any(|w| l.contains(w))
        })
        .take(5)
        .cloned()
        .collect()
}

/// The section, per package (a bundle lists each), then the files, the
/// tests and what a person should look at. CI failing is said, with which
/// checks; a pass is said only when CI passed.
pub fn what_to_check(bumps: &[Bump], w: &WhatToCheck) -> String {
    let mut s = String::from("## What to check\n\n");
    if !w.dep_report.trim().is_empty() {
        s.push_str(w.dep_report.trim());
        s.push_str("\n\n");
    }
    if let Some(facts) = &w.upgrade_facts {
        s.push_str(facts.section(&w.head_sha).trim());
        s.push_str("\n\n");
    }
    for b in bumps {
        s.push_str(&package_check(b, w, usize::MAX));
    }
    s.push_str(&check_footer(w));
    s
}

/// The notes read for a bump's package, if any.
pub fn notes_for<'a>(w: &'a WhatToCheck, package: &str) -> Option<&'a PackageNotes> {
    w.notes
        .iter()
        .find(|n| n.package.eq_ignore_ascii_case(package))
}

/// The notes read for a bump: its package's on its own version line (a
/// package bumped on two lines has notes for each), else its package's.
pub fn notes_for_bump<'a>(w: &'a WhatToCheck, b: &Bump) -> Option<&'a PackageNotes> {
    w.notes
        .iter()
        .find(|n| n.package.eq_ignore_ascii_case(&b.package) && same_line(&n.from, &b.from))
        .or_else(|| notes_for(w, &b.package))
}

/// A major version a person has to look at: its notes list breaking
/// changes, the code uses what changed, its notes weren't read, or a
/// parent does not declare support for it.
pub fn needs_eyes(b: &Bump, w: &WhatToCheck) -> bool {
    b.major
        && notes_for_bump(w, b).is_none_or(|n| {
            !n.read()
                || !n.breaking.is_empty()
                || !n.call_sites.is_empty()
                || n.parents.iter().any(|p| p.admits != Some(true))
        })
}

/// A bundle's section when it is too long to list every package: the
/// packages in `shown` with their notes (each list at most `cap` long),
/// `intro` above them and `rest` (what the others are, already written)
/// below, then the files and the tests as usual. A dependency fix run's
/// account comes first, whole, as in [`what_to_check`].
pub fn what_to_check_short(
    shown: &[&Bump],
    w: &WhatToCheck,
    cap: usize,
    intro: &str,
    rest: &str,
) -> String {
    let mut s = String::from("## What to check\n\n");
    if !w.dep_report.trim().is_empty() {
        s.push_str(w.dep_report.trim());
        s.push_str("\n\n");
    }
    if let Some(facts) = &w.upgrade_facts {
        s.push_str(facts.section(&w.head_sha).trim());
        s.push_str("\n\n");
    }
    if !intro.trim().is_empty() {
        s.push_str(intro.trim());
        s.push_str("\n\n");
    }
    for b in shown {
        s.push_str(&package_check(b, w, cap));
    }
    if !rest.trim().is_empty() {
        s.push_str(rest.trim_end());
        s.push_str("\n\n");
    }
    s.push_str(&check_footer(w));
    s
}

/// One package's part of the section: its notes, what changed, where the
/// code uses it, its lockfile. Each list at most `cap` long.
fn package_check(b: &Bump, w: &WhatToCheck, cap: usize) -> String {
    let mut s = String::new();
    let more = |s: &mut String, n: usize| {
        if n > cap {
            s.push_str(&format!("  - and {} more\n", n - cap));
        }
    };
    {
        let n = notes_for_bump(w, b);
        s.push_str(&format!(
            "### `{}` {} to {}{}\n\n",
            b.package,
            b.from,
            b.to,
            if b.major { " (major version)" } else { "" }
        ));
        for l in n.map(usage_lines).unwrap_or_default() {
            s.push_str(&format!("- {l}\n"));
        }
        match n {
            Some(n) if n.read() => {
                s.push_str(&format!(
                    "- Release notes: read from {} ({}).\n",
                    n.source,
                    n.versions.join(", ")
                ));
                if n.breaking.is_empty() {
                    s.push_str("- Breaking changes found: none in the notes.\n");
                } else {
                    s.push_str(&format!("- Breaking changes found: {}\n", n.breaking.len()));
                    for line in n.breaking.iter().take(cap) {
                        s.push_str(&format!("  - {line}\n"));
                    }
                    more(&mut s, n.breaking.len());
                }
                if !n.call_sites.is_empty() {
                    s.push_str("- Where the code uses what changed:\n");
                    for c in n.call_sites.iter().take(cap) {
                        let touched = w.files.iter().any(|f| f == &c.file);
                        s.push_str(&format!(
                            "  - `{}:{}` (`{}`): {}\n",
                            c.file,
                            c.line,
                            c.api,
                            if touched {
                                "changed in this pull request"
                            } else {
                                "not changed here: check it still works with the new version"
                            }
                        ));
                    }
                    more(&mut s, n.call_sites.len());
                } else if !n.apis.is_empty() {
                    s.push_str(&format!(
                        "- No use of {} was found in the code.\n",
                        n.apis.iter().map(|a| format!("`{a}`")).collect::<Vec<_>>().join(", ")
                    ));
                }
                for r in runtime_notes(n) {
                    s.push_str(&format!("- Look at (runtime or configuration): {r}\n"));
                }
            }
            Some(n) => s.push_str(&format!(
                "- Release notes: not found ({}). Read the package's changelog before merging{}.\n",
                if n.missing.is_empty() { "nothing read" } else { n.missing.as_str() },
                if b.major { "; this is a major version, so expect breaking changes" } else { "" }
            )),
            None => s.push_str(&format!(
                "- Release notes: not read for this package. Read its changelog before merging{}.\n",
                if b.major { "; this is a major version" } else { "" }
            )),
        }
        if let Some(n) = n.filter(|n| !n.lockfile.is_empty()) {
            let changed = w.files.iter().any(|f| f == &n.lockfile);
            s.push_str(&format!(
                "- Lockfile: `{}` {} `{}`.\n",
                n.lockfile,
                if changed {
                    "written by"
                } else {
                    "not changed; its tool is"
                },
                n.lock_command
            ));
        }
        s.push('\n');
    }
    s
}

/// The section's end: the files the pull request touches, and the tests.
fn check_footer(w: &WhatToCheck) -> String {
    let mut s = String::new();
    if !w.files.is_empty() {
        s.push_str(&format!("**Files touched** ({}):\n", w.files.len()));
        for f in w.files.iter().take(40) {
            s.push_str(&format!("- `{f}`\n"));
        }
        if w.files.len() > 40 {
            s.push_str(&format!("- and {} more\n", w.files.len() - 40));
        }
        let config: Vec<&String> = w.files.iter().filter(|f| is_config(f)).collect();
        if !config.is_empty() {
            s.push_str(&format!(
                "- Configuration changed: {}. Check it in every environment that uses it.\n",
                config
                    .iter()
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        s.push('\n');
    }
    s.push_str("**Tests**: ");
    let names = |v: &[String]| {
        v.iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match w.ci.state.as_str() {
        "passing" => s.push_str(&format!(
            "CI passes on this pull request{}.\n",
            if w.ci.checks.is_empty() { String::new() } else { format!(" ({})", names(&w.ci.checks)) }
        )),
        "failing" => s.push_str(&format!(
            "CI fails on this pull request: {}. It stays a draft until that is fixed; nothing here claims the upgrade works.\n",
            if w.ci.failing.is_empty() { "see the checks".to_string() } else { names(&w.ci.failing) }
        )),
        "pending" => s.push_str("CI is running on this pull request; this line is updated when it ends.\n"),
        _ => s.push_str("CI runs on this pull request; this line is updated when it ends. It stays a draft until CI passes.\n"),
    }
    s
}

/// The text of a `## ` section of `body` (its heading to the next `## `).
fn section_span(body: &str, heading: &str) -> Option<(usize, usize)> {
    let start = if body.starts_with(heading) {
        0
    } else {
        body.find(&format!("\n{heading}"))? + 1
    };
    let after = start + heading.len();
    let end = body[after..]
        .find("\n## ")
        .map(|i| after + i + 1)
        .unwrap_or(body.len());
    Some((start, end))
}

/// `body` with its `heading` sections taken from `fresh` (each section
/// replaced in place; one `body` lacks is left out). Keeps everything else
/// (the draft status CoderHelm writes at the top).
pub fn replace_sections(body: &str, fresh: &str, headings: &[&str]) -> String {
    let mut out = body.to_string();
    for h in headings {
        let (Some((a, b)), Some((c, d))) = (section_span(&out, h), section_span(fresh, h)) else {
            continue;
        };
        let mut piece = fresh[c..d].to_string();
        if !piece.ends_with('\n') {
            piece.push('\n');
        }
        out.replace_range(a..b, &piece);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn github_repos_read_from_every_registry_form() {
        for u in [
            "git+https://github.com/acme/parser.git",
            "git@github.com:acme/parser.git",
            "github:acme/parser",
            "https://github.com/acme/parser/tree/main/packages/x",
            "acme/parser",
        ] {
            assert_eq!(
                github_repo(u),
                Some(("acme".into(), "parser".into())),
                "{u}"
            );
        }
        assert_eq!(github_repo("https://gitlab.com/acme/parser"), None);
        let npm = json!({"repository": {"type": "git", "url": "git+https://github.com/acme/mono.git", "directory": "packages/parser"}});
        let r = source_repo("npm", "@acme/parser", &npm).unwrap();
        assert_eq!(
            (r.owner.as_str(), r.dir.as_deref()),
            ("acme", Some("packages/parser"))
        );
        let pypi = json!({"info": {"project_urls": {"Homepage": "https://example.com", "Source": "https://github.com/acme/py"}}});
        assert_eq!(source_repo("PyPI", "py", &pypi).unwrap().name, "py");
        assert_eq!(
            source_repo("Go", "github.com/acme/mod/v2/sub", &Value::Null)
                .unwrap()
                .name,
            "mod"
        );
    }

    #[test]
    fn tags_name_versions_only_for_their_own_package() {
        assert_eq!(version_of_tag("v3.0.0", "parser").as_deref(), Some("3.0.0"));
        assert_eq!(
            version_of_tag("parser@3.0.1", "parser").as_deref(),
            Some("3.0.1")
        );
        assert_eq!(
            version_of_tag("@acme/parser@3.0.1", "@acme/parser").as_deref(),
            Some("3.0.1")
        );
        assert_eq!(
            version_of_tag("parser-v3.1.0", "parser").as_deref(),
            Some("3.1.0")
        );
        assert_eq!(version_of_tag("other@3.0.1", "parser"), None);
        assert_eq!(version_of_tag("nightly", "parser"), None);
        let rel = vec![
            json!({"tag_name": "v2.4.1", "body": "old"}),
            json!({"tag_name": "v3.0.0", "body": "## Breaking\n- removed `parse()`", "html_url": "u3"}),
            json!({"tag_name": "v3.0.2", "body": "fix"}),
            json!({"tag_name": "v3.1.0", "body": "later"}),
            json!({"tag_name": "v3.0.1", "draft": true, "body": "draft"}),
        ];
        let got = releases_between(&rel, "parser", "2.4.1", "3.0.2");
        let vs: Vec<&str> = got.iter().map(|r| r.version.as_str()).collect();
        assert_eq!(vs, vec!["3.0.0", "3.0.2"]);
    }

    /// The changelog a major bump reads: its sections in the window, the
    /// breaking change, and the API it names.
    pub(crate) const FAKE_CHANGELOG: &str = "# Changelog\n\n## Unreleased\n- nothing yet\n\n## [3.0.2] - 2026-10-01\n### Fixes\n- Fix the prototype pollution in `merge` (CVE-2026-1111)\n\n## [3.0.0] - 2026-08-01\n### Breaking Changes\n- `parse()` is removed: use `parseAsync()`, which returns a Promise.\n- Node 18 is no longer supported; requires Node 20.\n### Features\n- Faster tokenizer\n\n```\n## 9.9.9 not a heading\n```\n\n## [2.5.0] - 2026-05-01\n- Added `strict` option\n\n## [2.4.1] - 2026-04-01\n- Fix\n";

    #[test]
    fn a_changelog_gives_the_sections_between_the_versions_and_what_breaks() {
        let got = changelog_between(FAKE_CHANGELOG, "2.4.1", "3.0.2");
        let vs: Vec<&str> = got.iter().map(|r| r.version.as_str()).collect();
        assert_eq!(vs, vec!["2.5.0", "3.0.0", "3.0.2"]);
        assert!(got[1].body.contains("parseAsync"));
        assert!(!got.iter().any(|r| r.version == "9.9.9"));
        let mut n = PackageNotes {
            package: "parser".into(),
            from: "2.4.1".into(),
            to: "3.0.2".into(),
            major: true,
            ..Default::default()
        };
        fill(&mut n, "`CHANGELOG.md` of acme/parser", "", &got);
        assert!(n.read());
        assert!(
            n.breaking
                .iter()
                .any(|b| b.contains("`parse()` is removed")),
            "{:?}",
            n.breaking
        );
        assert!(n.breaking.iter().any(|b| b.contains("requires Node 20")));
        assert_eq!(n.apis.first().map(String::as_str), Some("parse"));
        assert!(n.apis.contains(&"parseAsync".to_string()));
        // History-style files: a version line underlined.
        let hist = "3.0.0 / 2026-08-01\n==================\n\n  * BREAKING: drop `legacy()`\n\n2.0.0 / 2025-01-01\n==================\n\n  * old\n";
        let h = changelog_between(hist, "2.0.0", "3.0.0");
        assert_eq!(h.len(), 1);
        assert!(breaking_lines(&h[0].body)[0].contains("drop `legacy()`"));
    }

    #[test]
    fn call_sites_come_from_code_never_lockfiles_or_docs() {
        let results = vec![
            (
                "src/load.ts".to_string(),
                vec!["12: const doc = parse(text);".to_string()],
            ),
            (
                "pnpm-lock.yaml".to_string(),
                vec!["3: parse: 1".to_string()],
            ),
            ("CHANGELOG.md".to_string(), vec!["4: parse".to_string()]),
            (
                "node_modules/x/a.js".to_string(),
                vec!["1: parse(".to_string()],
            ),
        ];
        let c = call_sites("parse", &results);
        assert_eq!(c.len(), 1);
        assert_eq!((c[0].file.as_str(), c[0].line), ("src/load.ts", 12));
        assert_eq!(search_term("Client.connect"), "connect");
        assert_eq!(search_term("a.get"), "a.get");
    }

    #[test]
    fn every_lockfile_gets_its_own_managers_command() {
        let tree: Vec<String> = [
            "package.json",
            "pnpm-lock.yaml",
            "apps/web/package.json",
            "lambdas/resize/package.json",
            "lambdas/resize/package-lock.json",
            "py/pyproject.toml",
            "py/poetry.lock",
            "svc/go.mod",
            "svc/go.sum",
            "rs/Cargo.toml",
            "rs/Cargo.lock",
            "berry/.yarnrc.yml",
            "berry/package.json",
            "berry/yarn.lock",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let tools = lock_tools(&tree);
        let by = |l: &str| tools.iter().find(|t| t.lockfile == l).unwrap();
        assert!(by("pnpm-lock.yaml")
            .command
            .contains("pnpm install --lockfile-only"));
        assert!(!by("pnpm-lock.yaml").command.contains("frozen"));
        assert!(by("pnpm-lock.yaml")
            .inputs
            .contains(&"!lambdas/resize/**".to_string()));
        assert_eq!(
            by("lambdas/resize/package-lock.json").command,
            "cd 'lambdas/resize' && npm install --package-lock-only --ignore-scripts --no-audit --no-fund"
        );
        assert!(by("py/poetry.lock").command.contains("poetry lock"));
        assert_eq!(by("svc/go.sum").command, "cd 'svc' && go mod tidy");
        assert!(by("rs/Cargo.lock").command.contains("cargo metadata"));
        assert!(by("berry/yarn.lock")
            .command
            .contains("--mode=update-lockfile"));
        // A bump finds its lockfile from its manifest.
        assert_eq!(
            lock_tool_for(&tools, "apps/web/package.json", "npm")
                .unwrap()
                .lockfile,
            "pnpm-lock.yaml"
        );
        assert_eq!(
            lock_tool_for(&tools, "lambdas/resize/package-lock.json", "npm")
                .unwrap()
                .manager,
            "npm"
        );
        assert_eq!(lock_tool_for(&tools, "sbom", "Go"), None);
    }

    fn major_notes() -> PackageNotes {
        let mut n = PackageNotes {
            package: "parser".into(),
            ecosystem: "npm".into(),
            from: "2.4.1".into(),
            to: "3.0.2".into(),
            major: true,
            manifest: "package.json".into(),
            lockfile: "pnpm-lock.yaml".into(),
            lock_command: "pnpm install --lockfile-only --ignore-scripts".into(),
            ..Default::default()
        };
        fill(
            &mut n,
            "`CHANGELOG.md` of acme/parser",
            "https://github.com/acme/parser",
            &changelog_between(FAKE_CHANGELOG, "2.4.1", "3.0.2"),
        );
        n.call_sites = vec![
            CallSite {
                api: "parse".into(),
                file: "src/load.ts".into(),
                line: 12,
                text: "const doc = parse(text);".into(),
            },
            CallSite {
                api: "parse".into(),
                file: "src/cli.ts".into(),
                line: 4,
                text: "parse(argv)".into(),
            },
        ];
        n
    }

    #[test]
    fn the_ticket_lists_breaking_changes_and_call_sites() {
        let s = ticket_section(&[major_notes()]);
        assert!(s.contains("### `parser` 2.4.1 to 3.0.2 (major version)"));
        assert!(s.contains("`parse()` is removed"));
        assert!(s.contains("`src/load.ts:12` uses `parse`"));
        assert!(s.contains("`pnpm-lock.yaml` is written by `pnpm install --lockfile-only"));
        let missing = PackageNotes {
            package: "other".into(),
            missing: "the registry names no GitHub repository".into(),
            ..Default::default()
        };
        assert!(ticket_section(&[missing])
            .contains("No release notes could be read (the registry names no GitHub repository)"));
        assert!(!s.contains('→') && !s.to_lowercase().contains("phase"));
    }

    #[test]
    fn what_to_check_says_which_call_sites_changed_and_never_claims_a_failing_ci() {
        let bumps = vec![
            Bump {
                package: "parser".into(),
                from: "2.4.1".into(),
                to: "3.0.2".into(),
                major: true,
                manifest: "package.json".into(),
            },
            Bump {
                package: "undici".into(),
                from: "6.19.2".into(),
                to: "6.21.1".into(),
                ..Default::default()
            },
        ];
        let w = WhatToCheck {
            notes: vec![major_notes()],
            files: vec![
                "package.json".into(),
                "pnpm-lock.yaml".into(),
                "src/load.ts".into(),
                "vite.config.ts".into(),
            ],
            ci: CiState {
                state: "failing".into(),
                failing: vec!["test".into()],
                checks: vec!["build".into(), "test".into()],
            },
            ..Default::default()
        };
        let s = what_to_check(&bumps, &w);
        assert!(s.starts_with("## What to check"));
        assert!(s.contains("### `parser` 2.4.1 to 3.0.2 (major version)"));
        assert!(s.contains("### `undici` 6.19.2 to 6.21.1"));
        assert!(s.contains("`src/load.ts:12` (`parse`): changed in this pull request"));
        assert!(s.contains("`src/cli.ts:4` (`parse`): not changed here"));
        assert!(s.contains("Look at (runtime or configuration): Node 18 is no longer supported"));
        assert!(s.contains("`pnpm-lock.yaml` written by `pnpm install --lockfile-only"));
        assert!(s.contains("Configuration changed: `vite.config.ts`"));
        assert!(s.contains("CI fails on this pull request: `test`"));
        assert!(!s.contains("CI passes"));
        // The package without notes says so.
        assert!(s.contains("Release notes: not read for this package"));
        let passing = WhatToCheck {
            ci: CiState {
                state: "passing".into(),
                ..Default::default()
            },
            ..w.clone()
        };
        assert!(what_to_check(&bumps, &passing).contains("CI passes on this pull request."));
    }

    #[test]
    fn sections_are_replaced_in_place() {
        let body = "<!-- status -->\nheld\n\n## Summary\nx\n\n## What to check\nold\n\n## Proof\nold proof\n";
        let fresh = "## Summary\ny\n\n## What to check\nnew\n\n## Proof\nnew proof\n";
        let out = replace_sections(body, fresh, &["## What to check", "## Proof"]);
        assert_eq!(
            out,
            "<!-- status -->\nheld\n\n## Summary\nx\n\n## What to check\nnew\n\n## Proof\nnew proof\n"
        );
    }
}

#[cfg(test)]
mod parent_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_declared_range_admits_a_version_in_each_ecosystems_syntax() {
        let cases: &[(&str, &str, &str, Option<bool>)] = &[
            // npm
            ("npm", "^7.0.0", "7.5.4", Some(true)),
            ("npm", "^7.0.0", "8.1.1", Some(false)),
            ("npm", "^7.0.0 || ^8.0.0", "8.1.1", Some(true)),
            ("npm", ">=7.2.0 <9", "8.1.1", Some(true)),
            ("npm", ">= 7.2.0 < 8", "8.1.1", Some(false)),
            ("npm", "~7.2.0", "7.5.4", Some(false)),
            ("npm", "~7.2", "7.2.9", Some(true)),
            ("npm", "7.x", "7.5.4", Some(true)),
            ("npm", "7", "8.0.0", Some(false)),
            ("npm", "*", "8.1.1", Some(true)),
            ("npm", "", "8.1.1", Some(true)),
            ("npm", "7.0.0 - 8.5.0", "8.1.1", Some(true)),
            ("npm", "^0.12.1", "0.13.0", Some(false)),
            ("npm", "^0.12.1", "0.12.9", Some(true)),
            ("npm", "^0.0.3", "0.0.4", Some(false)),
            ("npm", "7.5.4", "7.5.4", Some(true)),
            ("npm", "workspace:*", "8.1.1", None),
            ("npm", "github:acme/glob-walk", "8.1.1", None),
            ("npm", "latest", "8.1.1", None),
            // Cargo: a bare version is a caret range.
            ("crates.io", "1.2", "1.9.0", Some(true)),
            ("crates.io", "1.2", "2.0.0", Some(false)),
            ("crates.io", ">=1.2, <1.9", "1.9.0", Some(false)),
            ("crates.io", "0.4.1", "0.5.0", Some(false)),
            // PyPI
            ("PyPI", ">=1.0,<3", "2.4.0", Some(true)),
            ("PyPI", ">=1.0,<3", "3.0.0", Some(false)),
            ("PyPI", "~=2.1", "2.9.0", Some(true)),
            ("PyPI", "~=2.1.0", "2.2.0", Some(false)),
            ("PyPI", "==2.1.*", "2.1.7", Some(true)),
            ("PyPI", "!=2.1.7,>=2", "2.1.7", Some(false)),
            // RubyGems
            ("RubyGems", "~> 2.1", "2.9.0", Some(true)),
            ("RubyGems", "~> 2.1", "3.0.0", Some(false)),
            ("RubyGems", ">= 1.0, < 3", "2.9.0", Some(true)),
        ];
        for (eco, range, version, want) in cases {
            assert_eq!(
                range_admits(eco, range, version),
                *want,
                "{eco}: `{range}` and {version}"
            );
        }
    }

    #[test]
    fn each_registrys_record_names_the_range_it_declares() {
        let npm = json!({"version": "3.2.1", "dependencies": {"glob-walk": "^7.0.0"},
            "peerDependencies": {"react": ">=17"}});
        assert_eq!(
            declared_range("npm", &npm, "glob-walk").as_deref(),
            Some("^7.0.0")
        );
        assert_eq!(
            declared_range("npm", &npm, "react").as_deref(),
            Some(">=17")
        );
        assert_eq!(declared_range("npm", &npm, "left-out"), None);
        assert_eq!(record_version("npm", &npm).as_deref(), Some("3.2.1"));
        let pypi = json!({"info": {"version": "2.0.0", "requires_dist": [
            "Yaml_Lite (>=1.0,<3)", "tomlkit>=0.11; python_version < '3.11'",
            "yaml-lite>=9 ; extra == 'fast'", "plainpkg"]}});
        assert_eq!(
            declared_range("PyPI", &pypi, "yaml-lite").as_deref(),
            Some(">=1.0,<3")
        );
        assert_eq!(
            declared_range("PyPI", &pypi, "tomlkit").as_deref(),
            Some(">=0.11")
        );
        assert_eq!(
            declared_range("PyPI", &pypi, "plainpkg").as_deref(),
            Some("*")
        );
        assert_eq!(record_version("PyPI", &pypi).as_deref(), Some("2.0.0"));
        let crates = json!({"dependencies": [
            {"crate_id": "xml-read", "req": "^0.31", "kind": "dev"},
            {"crate_id": "xml-read", "req": "^0.36", "kind": "normal"}]});
        assert_eq!(
            declared_range("crates.io", &crates, "xml-read").as_deref(),
            Some("^0.36")
        );
        let gems = json!({"version": "5.1.0", "dependencies": {"runtime": [
            {"name": "rack-lite", "requirements": "~> 2.1"}]}});
        assert_eq!(
            declared_range("RubyGems", &gems, "rack-lite").as_deref(),
            Some("~> 2.1")
        );
        // Every ecosystem with a record has its URL; the others say none.
        assert_eq!(
            deps_url("npm", "@acme/parent-lib", Some("3.2.1")).as_deref(),
            Some("https://registry.npmjs.org/@acme%2Fparent-lib/3.2.1")
        );
        assert_eq!(
            deps_url("npm", "parent-lib", None).as_deref(),
            Some("https://registry.npmjs.org/parent-lib/latest")
        );
        assert!(deps_url("PyPI", "parent", Some("1.0"))
            .unwrap()
            .ends_with("/parent/1.0/json"));
        assert!(deps_url("crates.io", "parent", Some("1.0"))
            .unwrap()
            .ends_with("/parent/1.0/dependencies"));
        assert_eq!(deps_url("crates.io", "parent", None), None);
        assert_eq!(deps_url("Go", "example.invalid/parent", Some("1.0")), None);
    }

    fn major_notes() -> PackageNotes {
        PackageNotes {
            package: "glob-walk".into(),
            ecosystem: "npm".into(),
            from: "7.2.0".into(),
            to: "8.1.1".into(),
            major: true,
            major_reason: "GHSA-eeee lists no patched release on the 7 line (patched releases it lists: 8.1.1)".into(),
            parents: vec![ParentSupport {
                parent: "build-kit".into(),
                version: "3.2.1".into(),
                declares: "^7.0.0".into(),
                admits: Some(false),
                latest: "4.0.0".into(),
                latest_declares: "^8.0.0".into(),
                latest_admits: Some(true),
                missing: String::new(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn a_transitive_major_names_its_parent_and_what_the_parent_declares() {
        let n = major_notes();
        assert_eq!(
            usage_lines(&n),
            vec![
                "Why a major version: GHSA-eeee lists no patched release on the 7 line (patched releases it lists: 8.1.1).",
                "Pulled in by `build-kit` 3.2.1, which declares `glob-walk: ^7.0.0`: 8.1.1 is outside the range it declares, so the override puts it on a version it does not declare support for. Its newest release 4.0.0 declares `^8.0.0`, which admits 8.1.1: upgrading `build-kit` is the way its maintainers support.",
            ]
        );
        // In the pull request, under the package, and it needs a look.
        let b = Bump {
            package: "glob-walk".into(),
            from: "7.2.0".into(),
            to: "8.1.1".into(),
            major: true,
            manifest: "pnpm-lock.yaml".into(),
        };
        let w = WhatToCheck {
            notes: vec![n.clone()],
            ..Default::default()
        };
        let text = what_to_check(std::slice::from_ref(&b), &w);
        assert!(
            text.contains(
                "- Why a major version: GHSA-eeee lists no patched release on the 7 line"
            ),
            "{text}"
        );
        assert!(text.contains("- Pulled in by `build-kit` 3.2.1, which declares `glob-walk: ^7.0.0`: 8.1.1 is outside the range it declares"), "{text}");
        assert!(needs_eyes(&b, &w));
        // And in the ticket the coder reads.
        let ticket = ticket_section(std::slice::from_ref(&n));
        assert!(
            ticket.contains("Pulled in by `build-kit` 3.2.1"),
            "{ticket}"
        );
        // A parent that declares the new major reads as inside its range.
        let mut ok = n.clone();
        ok.parents[0].declares = "^7.0.0 || ^8.0.0".into();
        ok.parents[0].admits = Some(true);
        ok.parents[0].latest.clear();
        assert_eq!(
            usage_lines(&ok)[1],
            "Pulled in by `build-kit` 3.2.1, which declares `glob-walk: ^7.0.0 || ^8.0.0`: 8.1.1 is inside the range it declares."
        );
        // Nothing read is said so, never guessed.
        let mut unread = n;
        unread.parents = vec![ParentSupport {
            parent: "build-kit".into(),
            missing: "the registry answered 404".into(),
            ..Default::default()
        }];
        assert_eq!(
            usage_lines(&unread)[1],
            "Pulled in by `build-kit`: whether it supports 8.1.1 could not be read (the registry answered 404)."
        );
    }

    #[test]
    fn a_package_bumped_on_two_lines_has_notes_for_each() {
        let mut seven = major_notes();
        seven.to = "7.5.4".into();
        seven.major = false;
        seven.parents.clear();
        seven.breaking = vec!["on the 7 line".into()];
        let mut eight = seven.clone();
        eight.from = "8.0.3".into();
        eight.to = "8.1.1".into();
        eight.breaking = vec!["on the 8 line".into()];
        let w = WhatToCheck {
            notes: vec![seven, eight],
            ..Default::default()
        };
        let b = |from: &str| Bump {
            package: "glob-walk".into(),
            from: from.into(),
            ..Default::default()
        };
        assert_eq!(
            notes_for_bump(&w, &b("8.0.3")).unwrap().breaking,
            vec!["on the 8 line"]
        );
        assert_eq!(
            notes_for_bump(&w, &b("7.1.0")).unwrap().breaking,
            vec!["on the 7 line"]
        );
        // A line with no notes of its own falls back to the package's.
        assert_eq!(
            notes_for_bump(&w, &b("6.0.0")).unwrap().breaking,
            vec!["on the 7 line"]
        );
    }
}
