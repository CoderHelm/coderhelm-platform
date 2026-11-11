//! Dockerfile base images, matched against OSV's distribution data (the
//! worker reads the registries: `worker::passes::base_images`).
//!
//! - A `FROM` line names an image and a tag or digest ([`ImageRef`]). Its
//!   distribution release comes from the packages' own purls when the
//!   image's package list can be read, else from the tag ([`release_of_tag`]:
//!   `debian:bookworm-slim`, `node:20-alpine3.20`, `ubuntu:24.04`).
//! - The package list is the image's published SBOM: Docker Hub's official
//!   images carry an in-toto SPDX attestation next to each platform's
//!   manifest. Images without one keep their release and say "no package
//!   list published"; nothing is matched for them.
//! - Each OS package is asked of OSV by its source package (`libc6` is
//!   `glibc`) in the release's ecosystem (`Debian:12`, `Ubuntu:24.04:LTS`,
//!   `Alpine:v3.20`). Only advisories with a fixed version in that release
//!   count: a base image bump can't fix the rest.
//! - One base image in one Dockerfile is one fix: the findings are under
//!   the image (package = the image's name, version = its tag as written),
//!   so the dependency findings' groups and fix units take them as they
//!   are. The fix pins the same tag to the digest that carries the fixed
//!   packages, in the same release.
//! - Base images are not searched in code: their verdict is Unknown with
//!   that reason, and no model call is made for them.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::security_fixes::{
    self as sec, Evidence, Finding, FindingState, FixUnit, Observation, Relation, Scope, Status,
    Verdict,
};

/// The ecosystem base images are recorded under.
pub const ECOSYSTEM: &str = "docker";
/// Docker Hub's registry and token service.
pub const DOCKER_HUB: &str = "registry-1.docker.io";
pub const DOCKER_AUTH: &str = "auth.docker.io";
/// The hosts the worker calls for base images (the self-host docs list them).
pub const HOSTS: &[&str] = &[DOCKER_AUTH, DOCKER_HUB, "production.cloudflare.docker.com"];
/// Why a base image's findings have no code verdict.
pub const NOT_CODE: &str = "a base image's packages are not searched in code";
/// Said of an image whose registry publishes no package list.
pub const NO_PACKAGE_LIST: &str = "no package list published";
/// Largest SBOM read (an official image's SPDX document is 2 to 30 MB).
pub const MAX_SBOM_BYTES: usize = 64 * 1024 * 1024;
/// Largest manifest read.
pub const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

pub const INDEX_TYPES: &str = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json";

// ── Image references ────────────────────────────────────────────────────

/// A base image as a `FROM` line names it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ImageRef {
    /// The registry host (`registry-1.docker.io` for Docker Hub).
    pub registry: String,
    /// The repository at the registry (`library/debian`).
    pub repository: String,
    /// The name as written, without tag or digest (`debian`,
    /// `public.ecr.aws/docker/library/node`).
    pub name: String,
    pub tag: String,
    /// `sha256:...` when pinned.
    pub digest: Option<String>,
}

impl ImageRef {
    /// Parses `[host/]repo[:tag][@digest]`.
    pub fn parse(s: &str) -> Option<ImageRef> {
        let s = s.trim();
        if s.is_empty() || s.contains('$') || s.contains(char::is_whitespace) {
            return None;
        }
        let (rest, digest) = match s.split_once('@') {
            Some((r, d)) if d.starts_with("sha256:") && d.len() == 71 => (r, Some(d.to_string())),
            Some(_) => return None,
            None => (s, None),
        };
        let slash = rest.rfind('/').map(|i| i + 1).unwrap_or(0);
        let (name, tag) = match rest[slash..].rsplit_once(':') {
            Some((n, t)) => (format!("{}{n}", &rest[..slash]), t.to_string()),
            None => (rest.to_string(), "latest".to_string()),
        };
        if name.is_empty() || tag.is_empty() {
            return None;
        }
        let first = name.split('/').next().unwrap_or("");
        let has_host = name.contains('/')
            && (first.contains('.') || first.contains(':') || first == "localhost");
        let (registry, repository) = if has_host {
            let host = if first == "docker.io" || first == "index.docker.io" {
                DOCKER_HUB
            } else {
                first
            };
            let repo = name[first.len() + 1..].to_string();
            let repo = if host == DOCKER_HUB && !repo.contains('/') {
                format!("library/{repo}")
            } else {
                repo
            };
            (host.to_string(), repo)
        } else if name.contains('/') {
            (DOCKER_HUB.to_string(), name.clone())
        } else {
            (DOCKER_HUB.to_string(), format!("library/{name}"))
        };
        Some(ImageRef {
            registry,
            repository: repository.to_ascii_lowercase(),
            name,
            tag,
            digest,
        })
    }

    /// From a `docker` dependency (`name`, `version` as
    /// `security_fixes::dockerfile_images` records them).
    pub fn from_dep(name: &str, version: &str) -> Option<ImageRef> {
        if version.starts_with("sha256:") {
            ImageRef::parse(&format!("{name}@{version}"))
        } else {
            ImageRef::parse(&format!("{name}:{version}"))
        }
    }

    /// The tag as written, with its digest when pinned (a finding's version).
    pub fn written(&self) -> String {
        match &self.digest {
            Some(d) => format!("{}@{d}", self.tag),
            None => self.tag.clone(),
        }
    }

    /// The reference to read: the digest when pinned, else the tag.
    pub fn reference(&self) -> &str {
        self.digest.as_deref().unwrap_or(&self.tag)
    }

    /// The same tag, not pinned.
    pub fn unpinned(&self, tag: &str) -> ImageRef {
        ImageRef {
            tag: tag.to_string(),
            digest: None,
            ..self.clone()
        }
    }

    /// The image's short name (`debian`, `node`).
    pub fn short(&self) -> &str {
        self.repository
            .rsplit('/')
            .next()
            .unwrap_or(&self.repository)
    }
}

/// The tags a pinned or patch-level tag can move to within its line: the
/// same tag (its current digest), and for a full version tag the same tag
/// without its patch number (`20.11.0-bookworm-slim` to `20.11-bookworm-slim`).
pub fn candidate_tags(r: &ImageRef) -> Vec<String> {
    let mut out = Vec::new();
    if r.digest.is_some() {
        out.push(r.tag.clone());
    }
    let (ver, variant) = match r.tag.split_once('-') {
        Some((v, rest)) => (v, Some(rest)),
        None => (r.tag.as_str(), None),
    };
    let parts: Vec<&str> = ver.split('.').collect();
    if parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
    {
        let minor = format!("{}.{}", parts[0], parts[1]);
        out.push(match variant {
            Some(v) => format!("{minor}-{v}"),
            None => minor,
        });
    }
    out.dedup();
    out
}

// ── Releases ────────────────────────────────────────────────────────────

/// A distribution release.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, PartialOrd, Ord)]
pub struct Release {
    /// `debian`, `ubuntu`, `alpine`, `amazonlinux`.
    pub distro: String,
    /// `12`, `24.04`, `3.20`, `2023`; empty when the tag doesn't say.
    pub version: String,
}

const CODENAMES: &[(&str, &str, &str)] = &[
    ("buster", "debian", "10"),
    ("bullseye", "debian", "11"),
    ("bookworm", "debian", "12"),
    ("trixie", "debian", "13"),
    ("forky", "debian", "14"),
    ("focal", "ubuntu", "20.04"),
    ("jammy", "ubuntu", "22.04"),
    ("noble", "ubuntu", "24.04"),
    ("oracular", "ubuntu", "24.10"),
    ("plucky", "ubuntu", "25.04"),
    ("questing", "ubuntu", "25.10"),
    ("resolute", "ubuntu", "26.04"),
];

fn major_minor(v: &str) -> String {
    v.split('.').take(2).collect::<Vec<_>>().join(".")
}

/// The release a tag names: an OS image's own version or codename, or a
/// language image's variant (`node:20-bookworm-slim`, `python:3.12-alpine3.20`).
pub fn release_of_tag(r: &ImageRef) -> Option<Release> {
    let tag = r.tag.to_ascii_lowercase();
    for (code, distro, ver) in CODENAMES {
        if tag.split(['-', '.', '_']).any(|w| w == *code) {
            return Some(Release {
                distro: distro.to_string(),
                version: ver.to_string(),
            });
        }
    }
    if let Some(at) = tag.find("alpine") {
        let ver: String = tag[at + 6..]
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect();
        return Some(Release {
            distro: "alpine".into(),
            version: major_minor(&ver),
        });
    }
    let lead: String = tag
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    match r.short() {
        "debian" => Some(Release {
            distro: "debian".into(),
            version: lead.split('.').next().unwrap_or("").to_string(),
        }),
        "ubuntu" => Some(Release {
            distro: "ubuntu".into(),
            version: major_minor(&lead),
        }),
        "alpine" => Some(Release {
            distro: "alpine".into(),
            version: major_minor(&lead),
        }),
        "amazonlinux" => Some(Release {
            distro: "amazonlinux".into(),
            version: lead.split('.').next().unwrap_or("").to_string(),
        }),
        _ => None,
    }
}

/// OSV's ecosystem for a release (`Debian:12`, `Ubuntu:24.04:LTS`,
/// `Alpine:v3.20`); `None` where OSV has no data (Amazon Linux) or the
/// version isn't known.
pub fn osv_ecosystem(r: &Release) -> Option<String> {
    if r.version.is_empty() {
        return None;
    }
    match r.distro.as_str() {
        "debian" => Some(format!(
            "Debian:{}",
            r.version.split('.').next().unwrap_or("")
        )),
        "ubuntu" => {
            let v = major_minor(&r.version);
            let (y, m) = v.split_once('.')?;
            let lts = m == "04" && y.parse::<u32>().ok()? % 2 == 0;
            Some(if lts {
                format!("Ubuntu:{v}:LTS")
            } else {
                format!("Ubuntu:{v}")
            })
        }
        "alpine" => Some(format!("Alpine:v{}", major_minor(&r.version))),
        _ => None,
    }
}

// ── Package lists (SPDX SBOM attestations) ──────────────────────────────

/// One OS package in an image.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OsPackage {
    pub name: String,
    pub version: String,
    /// The source package OSV keys advisories by.
    pub source: String,
    pub release: Release,
}

fn qualifiers(purl: &str) -> BTreeMap<String, String> {
    purl.split_once('?')
        .map(|(_, q)| q.split('#').next().unwrap_or(q))
        .unwrap_or("")
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| {
            (
                k.to_string(),
                urlencoding::decode(v)
                    .map(|s| s.into_owned())
                    .unwrap_or_else(|_| v.to_string()),
            )
        })
        .collect()
}

/// Debian and Ubuntu binary packages whose source package has another name,
/// for the packages base images carry. A purl's `upstream` qualifier wins.
const SOURCES: &[(&str, &str)] = &[
    ("libc6", "glibc"),
    ("libc-bin", "glibc"),
    ("libc-l10n", "glibc"),
    ("locales", "glibc"),
    ("libssl3", "openssl"),
    ("libssl3t64", "openssl"),
    ("libssl1.1", "openssl"),
    ("openssl-provider-legacy", "openssl"),
    ("libsystemd0", "systemd"),
    ("libsystemd-shared", "systemd"),
    ("libudev1", "systemd"),
    ("zlib1g", "zlib"),
    ("libgnutls30", "gnutls28"),
    ("libgnutls30t64", "gnutls28"),
    ("libsqlite3-0", "sqlite3"),
    ("libcurl4", "curl"),
    ("libcurl4t64", "curl"),
    ("libcurl3-gnutls", "curl"),
    ("libexpat1", "expat"),
    ("perl-base", "perl"),
    ("perl-modules-5.36", "perl"),
    ("libperl5.36", "perl"),
    ("libncursesw6", "ncurses"),
    ("libtinfo6", "ncurses"),
    ("ncurses-base", "ncurses"),
    ("ncurses-bin", "ncurses"),
    ("libgmp10", "gmp"),
    ("libnettle8", "nettle"),
    ("libnettle8t64", "nettle"),
    ("libhogweed6", "nettle"),
    ("libhogweed6t64", "nettle"),
    ("libkrb5-3", "krb5"),
    ("libkrb5support0", "krb5"),
    ("libk5crypto3", "krb5"),
    ("libgssapi-krb5-2", "krb5"),
    ("libp11-kit0", "p11-kit"),
    ("liblz4-1", "lz4"),
    ("liblzma5", "xz-utils"),
    ("libzstd1", "libzstd"),
    ("libbz2-1.0", "bzip2"),
    ("libpcre2-8-0", "pcre2"),
    ("libaudit1", "audit"),
    ("libaudit-common", "audit"),
    ("libacl1", "acl"),
    ("libattr1", "attr"),
    ("libblkid1", "util-linux"),
    ("libmount1", "util-linux"),
    ("libuuid1", "util-linux"),
    ("libsmartcols1", "util-linux"),
    ("bsdutils", "util-linux"),
    ("mount", "util-linux"),
    ("libcom-err2", "e2fsprogs"),
    ("libss2", "e2fsprogs"),
    ("logsave", "e2fsprogs"),
    ("libext2fs2", "e2fsprogs"),
    ("libgcc-s1", "gcc-12"),
    ("libstdc++6", "gcc-12"),
    ("gcc-12-base", "gcc-12"),
    ("libcrypt1", "libxcrypt"),
    ("libffi8", "libffi"),
    ("libidn2-0", "libidn2"),
    ("libunistring2", "libunistring"),
    ("libunistring5", "libunistring"),
    ("libseccomp2", "libseccomp"),
    ("libxxhash0", "xxhash"),
    ("libmd0", "libmd"),
    ("libpam0g", "pam"),
    ("libpam-modules", "pam"),
    ("libpam-modules-bin", "pam"),
    ("libpam-runtime", "pam"),
    ("libapt-pkg6.0", "apt"),
    ("libapt-pkg6.0t64", "apt"),
    ("gpgv", "gnupg2"),
    ("login", "shadow"),
    ("passwd", "shadow"),
    ("sysvinit-utils", "sysvinit"),
    ("libdebconfclient0", "cdebconf"),
    ("libselinux1", "libselinux"),
    ("libsemanage2", "libsemanage"),
    ("libsemanage-common", "libsemanage"),
    ("libsepol2", "libsepol"),
    ("libtasn1-6", "libtasn1-6"),
    ("libdb5.3", "db5.3"),
    ("libdb5.3t64", "db5.3"),
    ("libxml2", "libxml2"),
    ("libgcrypt20", "libgcrypt20"),
    ("libgpg-error0", "libgpg-error"),
    ("libcap2", "libcap2"),
    ("libcap-ng0", "libcap-ng"),
    ("libpcre3", "pcre3"),
    ("libprocps8", "procps"),
    ("libproc2-0", "procps"),
];

/// The source package for a binary package.
pub fn source_of(distro: &str, binary: &str, upstream: Option<&str>) -> String {
    if let Some(u) = upstream
        .map(|u| u.split(['(', ' ']).next().unwrap_or(u).trim())
        .filter(|u| !u.is_empty())
    {
        return u.to_string();
    }
    if matches!(distro, "debian" | "ubuntu") {
        if let Some((_, s)) = SOURCES.iter().find(|(b, _)| *b == binary) {
            return s.to_string();
        }
        return binary.strip_suffix("t64").unwrap_or(binary).to_string();
    }
    binary.to_string()
}

fn release_from_qualifiers(ty: &str, q: &BTreeMap<String, String>) -> Release {
    let distro_q = q.get("distro").cloned().unwrap_or_default();
    let (mut distro, mut version) = (
        q.get("os_name").cloned().unwrap_or_default(),
        q.get("os_version").cloned().unwrap_or_default(),
    );
    if distro.is_empty() || version.is_empty() {
        // `distro=alpine-3.20.3`, `distro=debian-12`, or a codename.
        if let Some((d, v)) = distro_q.split_once('-') {
            if v.starts_with(|c: char| c.is_ascii_digit()) {
                distro = d.to_string();
                version = v.to_string();
            }
        } else if let Some((_, d, v)) = CODENAMES.iter().find(|(c, _, _)| *c == distro_q) {
            distro = d.to_string();
            version = v.to_string();
        }
    }
    if distro.is_empty() {
        distro = match ty {
            "apk" => "alpine".into(),
            "deb" => "debian".into(),
            _ => String::new(),
        };
    }
    Release {
        distro: distro.to_ascii_lowercase(),
        version,
    }
}

/// The OS packages of an SPDX SBOM (an in-toto statement whose predicate is
/// the SPDX document, or the document itself).
pub fn sbom_packages(doc: &Value) -> Vec<OsPackage> {
    let spdx = if doc["predicate"].is_object() {
        &doc["predicate"]
    } else {
        doc
    };
    let mut out: Vec<OsPackage> = Vec::new();
    for p in spdx["packages"].as_array().into_iter().flatten() {
        for r in p["externalRefs"].as_array().into_iter().flatten() {
            if r["referenceType"].as_str() != Some("purl") {
                continue;
            }
            let purl = r["referenceLocator"].as_str().unwrap_or("");
            let Some((ty, name, ver)) = sec::parse_purl(purl) else {
                continue;
            };
            if !matches!(ty.as_str(), "deb" | "apk") {
                continue;
            }
            let q = qualifiers(purl);
            let release = release_from_qualifiers(&ty, &q);
            let name = name.rsplit('/').next().unwrap_or(&name).to_string();
            let version = p["versionInfo"]
                .as_str()
                .map(String::from)
                .unwrap_or_else(|| {
                    urlencoding::decode(&ver)
                        .map(|s| s.into_owned())
                        .unwrap_or(ver)
                });
            let source = source_of(
                &release.distro,
                &name,
                q.get("upstream").map(String::as_str),
            );
            let pkg = OsPackage {
                name,
                version,
                source,
                release,
            };
            if !out.contains(&pkg) {
                out.push(pkg);
            }
        }
    }
    out
}

/// The release most of the packages say.
pub fn release_of_packages(pkgs: &[OsPackage]) -> Option<Release> {
    let mut n: BTreeMap<&Release, usize> = BTreeMap::new();
    for p in pkgs.iter().filter(|p| !p.release.distro.is_empty()) {
        *n.entry(&p.release).or_default() += 1;
    }
    n.into_iter()
        .max_by_key(|(_, c)| *c)
        .map(|(r, _)| r.clone())
}

/// One OSV question per distinct (source package, version), in package
/// order: `(source, version, query)`.
pub fn osv_queries(pkgs: &[OsPackage], ecosystem: &str) -> Vec<(String, String, Value)> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for p in pkgs {
        if seen.insert((p.source.clone(), p.version.clone())) {
            out.push((
                p.source.clone(),
                p.version.clone(),
                json!({ "package": { "ecosystem": ecosystem, "name": p.source }, "version": p.version }),
            ));
        }
    }
    out
}

/// An OSV id worth a finding: one CVE in one distribution
/// (`DEBIAN-CVE-...`, `UBUNTU-CVE-...`, `ALPINE-CVE-...`), not a bundle
/// advisory that repeats them (`DSA-`, `DLA-`, `USN-`).
pub fn wanted_id(id: &str) -> bool {
    let u = id.to_ascii_uppercase();
    (u.starts_with("DEBIAN-CVE-") || u.starts_with("UBUNTU-CVE-") || u.starts_with("ALPINE-CVE-"))
        || u.starts_with("CVE-")
}

/// The fixed version an OSV record names for `source` in exactly
/// `ecosystem` (`Debian:12`, not `Debian:13`).
pub fn fixed_in(v: &Value, ecosystem: &str, source: &str) -> Option<String> {
    for a in v["affected"].as_array().into_iter().flatten() {
        let e = a["package"]["ecosystem"].as_str().unwrap_or("");
        let n = a["package"]["name"].as_str().unwrap_or("");
        if !e.eq_ignore_ascii_case(ecosystem) || !n.eq_ignore_ascii_case(source) {
            continue;
        }
        for r in a["ranges"].as_array().into_iter().flatten() {
            for ev in r["events"].as_array().into_iter().flatten() {
                if let Some(f) = ev["fixed"].as_str() {
                    return Some(f.to_string());
                }
            }
        }
    }
    None
}

/// An OS package's advisory as a finding of the base image, when the
/// release has a fixed version for it. `None` for advisories no newer
/// image can fix.
#[allow(clippy::too_many_arguments)]
pub fn observation(
    repo: &str,
    manifest: &str,
    image: &ImageRef,
    ecosystem: &str,
    source: &str,
    version: &str,
    record: &Value,
) -> Option<Observation> {
    let fixed = fixed_in(record, ecosystem, source)?;
    let base = ecosystem.split(':').next().unwrap_or(ecosystem);
    let mut adv = sec::advisory_from_osv(record, base, source)?;
    for c in record["upstream"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c.as_str())
    {
        if c.starts_with("CVE-") && !adv.ids.iter().any(|x| x == c) {
            adv.ids.push(c.to_string());
        }
    }
    // The distribution package's ranges aren't the image's versions.
    adv.ranges.clear();
    adv.fixed.clear();
    adv.symbols.clear();
    let cve = adv
        .ids
        .iter()
        .find(|i| i.starts_with("CVE-"))
        .cloned()
        .unwrap_or_else(|| adv.ids[0].clone());
    if adv.title.is_empty() {
        adv.title = format!("{cve} in {source} {version} ({ecosystem}), fixed in {fixed}");
    }
    Some(Observation {
        repo: repo.to_string(),
        manifest: manifest.to_string(),
        ecosystem: ECOSYSTEM.into(),
        package: image.name.clone(),
        version: image.written(),
        advisory: adv,
        scope: Scope::Runtime,
        relation: Relation::Direct,
        path: vec![format!("{ecosystem} {source} {version}, fixed in {fixed}")],
        ..Default::default()
    })
}

// ── Registry manifests ──────────────────────────────────────────────────

/// A manifest's digest (`sha256:...`), when the registry doesn't say it.
pub fn digest_of(body: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("sha256:{}", hex::encode(Sha256::digest(body)))
}

/// From a manifest index: the linux/amd64 image manifest's digest, and the
/// attestation manifest that describes it, when there is one.
pub fn platform_and_attestation(index: &Value) -> Option<(String, Option<String>)> {
    let ms = index["manifests"].as_array()?;
    let image = ms.iter().find(|m| {
        m["platform"]["os"].as_str() == Some("linux")
            && m["platform"]["architecture"].as_str() == Some("amd64")
    })?;
    let digest = image["digest"].as_str()?.to_string();
    let att = ms
        .iter()
        .find(|m| {
            m["annotations"]["vnd.docker.reference.type"].as_str() == Some("attestation-manifest")
                && m["annotations"]["vnd.docker.reference.digest"].as_str() == Some(digest.as_str())
        })
        .and_then(|m| m["digest"].as_str())
        .map(String::from);
    Some((digest, att))
}

/// The SPDX layer of an attestation manifest: `(digest, size)`.
pub fn spdx_layer(attestation: &Value) -> Option<(String, u64)> {
    attestation["layers"].as_array()?.iter().find_map(|l| {
        let spdx = l["annotations"]["in-toto.io/predicate-type"]
            .as_str()
            .is_some_and(|t| t.contains("spdx"));
        spdx.then(|| {
            (
                l["digest"].as_str().unwrap_or("").to_string(),
                l["size"].as_u64().unwrap_or(0),
            )
        })
    })
}

/// A registry's `WWW-Authenticate: Bearer realm=...,service=...,scope=...`.
pub fn bearer_challenge(header: &str) -> Option<(String, Vec<(String, String)>)> {
    let rest = header
        .trim()
        .strip_prefix("Bearer ")
        .or_else(|| header.trim().strip_prefix("bearer "))?;
    let mut realm = None;
    let mut params = Vec::new();
    for part in rest.split(',') {
        let (k, v) = part.trim().split_once('=')?;
        let v = v.trim_matches('"').to_string();
        if k == "realm" {
            realm = Some(v);
        } else {
            params.push((k.to_string(), v));
        }
    }
    let realm = realm.filter(|r| r.starts_with("https://"))?;
    Some((realm, params))
}

// ── What a read found, applied to the findings ──────────────────────────

/// One base image of one Dockerfile, as the last read found it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Resolution {
    pub manifest: String,
    /// The image's name as written (a finding's package).
    pub package: String,
    /// The tag as written (a finding's version).
    pub version: String,
    pub release: Option<Release>,
    /// The package list was read and matched.
    pub read: bool,
    /// Why not, in a line (`no package list published`).
    pub note: String,
    /// Advisory ids seen in the image now.
    pub seen: BTreeSet<String>,
    /// Advisory id to the reference that fixes it (`12-slim@sha256:...`).
    pub fixes: BTreeMap<String, String>,
    pub packages: usize,
}

/// What [`apply_resolutions`] changed on a finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// The image no longer has the vulnerable version.
    Fixed,
    /// Its fix, its verdict or its tier changed.
    Updated,
}

/// Brings the repository's base image findings up to date with the read:
/// - a finding whose image (same Dockerfile) was read and no longer shows
///   the advisory is fixed;
/// - otherwise its fix is the reference the read found, or none;
/// - its verdict is Unknown, with why, and never waits for triage.
pub fn apply_resolutions(
    findings: &mut [(Finding, FindingState)],
    repo: &str,
    rs: &[Resolution],
    cfg: &sec::SecurityConfig,
) -> Vec<(usize, Change)> {
    let mut out = Vec::new();
    for (i, (f, st)) in findings.iter_mut().enumerate() {
        if f.ecosystem != ECOSYSTEM || !f.repo.eq_ignore_ascii_case(repo) || !f.status.is_open() {
            continue;
        }
        let before = (f.fixed_version.clone(), f.verdict, f.tier);
        // The same image in the same Dockerfile, read now (its tag may have moved).
        let r = rs.iter().find(|r| {
            r.read && r.manifest == f.manifest && r.package.eq_ignore_ascii_case(&f.package)
        });
        if let Some(r) = r {
            if !f.advisory.ids.iter().any(|id| r.seen.contains(id)) {
                f.status = Status::Fixed;
                f.fixed_version = Some(r.version.clone());
                f.updated_at = chrono::Utc::now().to_rfc3339();
                out.push((i, Change::Fixed));
                continue;
            }
            f.fixed_version = f
                .advisory
                .ids
                .iter()
                .find_map(|id| r.fixes.get(id))
                .cloned();
        }
        if matches!(f.verdict, Verdict::Pending | Verdict::Unknown)
            && f.evidence.as_ref().map(|e| e.summary.as_str()) != Some(NOT_CODE)
        {
            f.verdict = Verdict::Unknown;
            f.evidence = Some(Evidence {
                summary: NOT_CODE.into(),
                lines: Vec::new(),
                checked: "The image's package list, against OSV's data for its release".into(),
                confidence: "low".into(),
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

/// The plan a base image fix run follows: the exact `FROM` change.
pub fn ticket_body(unit: &FixUnit, members: &[&Finding]) -> String {
    let mut s = format!(
        "{}\n<!-- coderhelm:security-branch {} -->\n\n",
        sec::MARKER,
        unit.branch
    );
    s.push_str("Fix these security advisories in a Dockerfile's base image. Change only the `FROM` line.\n\n## Change\n");
    for b in &unit.bumps {
        let written_from = format!("{}:{}", b.package, b.from);
        s.push_str(&format!(
            "- In `{}`, replace `FROM {written_from}` with `FROM {}:{}`. Keep the line's flags and its `AS` stage name. Every other `FROM {written_from}` in the repository changes the same way.\n",
            if b.manifest.is_empty() { "the Dockerfile" } else { &b.manifest },
            b.package,
            b.to
        ));
    }
    s.push_str("\nThe new reference is the same tag, pinned to the digest that carries the fixed packages, in the same distribution release. Don't change the release or the tag's version.\n\n## Advisories\n");
    for f in members {
        s.push_str(&format!(
            "- {} ({}){}\n",
            f.advisory.primary,
            f.advisory.ids.join(", "),
            f.path.first().map(|p| format!(": {p}")).unwrap_or_default()
        ));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const INDEX: &str = include_str!("security_testdata/registry_index_debian_12_slim.json");
    const ATTESTATION: &str = include_str!("security_testdata/registry_attestation.json");
    const SBOM_OLD: &str = include_str!("security_testdata/sbom_debian_old.json");
    const SBOM_NEW: &str = include_str!("security_testdata/sbom_debian_new.json");
    const OSV_OLD: &str = include_str!("security_testdata/osv_debian_old.json");
    const OSV_NEW: &str = include_str!("security_testdata/osv_debian_new.json");
    const V4911: &str = include_str!("security_testdata/osv_DEBIAN-CVE-2023-4911.json");
    const V4756: &str = include_str!("security_testdata/osv_DEBIAN-CVE-2010-4756.json");
    const V5535: &str = include_str!("security_testdata/osv_DEBIAN-CVE-2024-5535.json");

    const OLD: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const NEW: &str = "sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc";

    #[test]
    fn references_parse_like_docker_does() {
        let r = ImageRef::parse("debian:12-slim").unwrap();
        assert_eq!(
            (r.registry.as_str(), r.repository.as_str(), r.tag.as_str()),
            (DOCKER_HUB, "library/debian", "12-slim")
        );
        let p = ImageRef::from_dep("debian:12-slim", OLD).unwrap();
        assert_eq!(p.tag, "12-slim");
        assert_eq!(p.digest.as_deref(), Some(OLD));
        assert_eq!(p.written(), format!("12-slim@{OLD}"));
        let e = ImageRef::parse("public.ecr.aws/docker/library/node:20-alpine3.20").unwrap();
        assert_eq!(
            (e.registry.as_str(), e.repository.as_str(), e.short()),
            ("public.ecr.aws", "docker/library/node", "node")
        );
        let h = ImageRef::parse("localhost:5000/team/app").unwrap();
        assert_eq!(
            (h.registry.as_str(), h.tag.as_str()),
            ("localhost:5000", "latest")
        );
        assert_eq!(
            ImageRef::parse("docker.io/python:3.12").unwrap().repository,
            "library/python"
        );
        assert!(ImageRef::parse("${BASE}").is_none());
        assert!(ImageRef::parse("debian@md5:abc").is_none());
    }

    #[test]
    fn tags_name_their_release() {
        let rel =
            |s: &str| release_of_tag(&ImageRef::parse(s).unwrap()).and_then(|r| osv_ecosystem(&r));
        assert_eq!(rel("debian:12-slim").as_deref(), Some("Debian:12"));
        assert_eq!(rel("debian:bookworm").as_deref(), Some("Debian:12"));
        assert_eq!(rel("node:20-bookworm-slim").as_deref(), Some("Debian:12"));
        assert_eq!(rel("python:3.12-slim-trixie").as_deref(), Some("Debian:13"));
        assert_eq!(rel("ubuntu:24.04").as_deref(), Some("Ubuntu:24.04:LTS"));
        assert_eq!(rel("ubuntu:jammy").as_deref(), Some("Ubuntu:22.04:LTS"));
        assert_eq!(rel("ubuntu:24.10").as_deref(), Some("Ubuntu:24.10"));
        assert_eq!(rel("alpine:3.20.3").as_deref(), Some("Alpine:v3.20"));
        assert_eq!(rel("node:20-alpine3.20").as_deref(), Some("Alpine:v3.20"));
        // A release OSV has no data for, or one the tag doesn't say.
        assert_eq!(
            release_of_tag(&ImageRef::parse("amazonlinux:2023").unwrap())
                .unwrap()
                .distro,
            "amazonlinux"
        );
        assert_eq!(rel("amazonlinux:2023"), None);
        assert_eq!(rel("node:20-alpine"), None);
        assert_eq!(rel("node:20"), None);
    }

    #[test]
    fn the_attestation_leads_to_the_spdx_package_list() {
        let idx: Value = serde_json::from_str(INDEX).unwrap();
        let (image, att) = platform_and_attestation(&idx).unwrap();
        assert!(image.starts_with("sha256:c67e92c8"));
        assert!(att.unwrap().starts_with("sha256:4ed59d39"));
        let (layer, size) = spdx_layer(&serde_json::from_str(ATTESTATION).unwrap()).unwrap();
        assert!(layer.starts_with("sha256:91fa3352"));
        assert!(size > 1_000_000 && (size as usize) < MAX_SBOM_BYTES);
        // A single-platform manifest has no index entries.
        assert!(platform_and_attestation(&json!({"schemaVersion": 2, "layers": []})).is_none());
    }

    #[test]
    fn the_package_list_names_source_packages_and_the_release() {
        let p = sbom_packages(&serde_json::from_str(SBOM_OLD).unwrap());
        let names: Vec<(&str, &str, &str)> = p
            .iter()
            .map(|x| (x.name.as_str(), x.source.as_str(), x.version.as_str()))
            .collect();
        assert_eq!(names[0], ("libc6", "glibc", "2.36-9+deb12u1"));
        assert_eq!(names[1], ("libssl3", "openssl", "3.0.11-1~deb12u1"));
        assert!(names.contains(&("zlib1g", "zlib", "1:1.2.13.dfsg-1")));
        let r = release_of_packages(&p).unwrap();
        assert_eq!(osv_ecosystem(&r).as_deref(), Some("Debian:12"));
        let q = osv_queries(&p, "Debian:12");
        assert_eq!(
            q.iter().map(|x| x.0.as_str()).collect::<Vec<_>>(),
            vec!["glibc", "openssl", "bash", "zlib"]
        );
        // Alpine's purls carry the origin package and the release.
        let apk = json!({"packages": [{"name": "libssl3", "versionInfo": "3.3.2-r0", "externalRefs": [{"referenceType": "purl", "referenceLocator": "pkg:apk/alpine/libssl3@3.3.2-r0?arch=x86_64&upstream=openssl&distro=alpine-3.20.3"}]}]});
        let a = sbom_packages(&apk);
        assert_eq!(
            (
                a[0].source.as_str(),
                osv_ecosystem(&a[0].release).as_deref()
            ),
            ("openssl", Some("Alpine:v3.20"))
        );
    }

    fn observations(osv: &str, sbom: &str, image: &ImageRef) -> Vec<Observation> {
        let p = sbom_packages(&serde_json::from_str(sbom).unwrap());
        let q = osv_queries(&p, "Debian:12");
        let answer: Value = serde_json::from_str(osv).unwrap();
        let records: BTreeMap<String, Value> = [V4911, V4756, V5535]
            .iter()
            .map(|r| serde_json::from_str::<Value>(r).unwrap())
            .map(|v| (v["id"].as_str().unwrap().to_string(), v))
            .collect();
        let mut out = Vec::new();
        for (i, (source, version, _)) in q.iter().enumerate() {
            for id in answer["results"][i]["vulns"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v["id"].as_str())
            {
                if !wanted_id(id) {
                    continue;
                }
                if let Some(o) = records.get(id).and_then(|r| {
                    observation(
                        "acme/web",
                        "Dockerfile",
                        image,
                        "Debian:12",
                        source,
                        version,
                        r,
                    )
                }) {
                    out.push(o);
                }
            }
        }
        out
    }

    #[test]
    fn only_fixable_advisories_of_the_release_become_findings_of_the_image() {
        let image = ImageRef::from_dep("debian:12-slim", OLD).unwrap();
        let o = observations(OSV_OLD, SBOM_OLD, &image);
        // 4911 (glibc, fixed in deb12u3) and 5535 (openssl, fixed in
        // 3.0.15-1~deb12u1); not 2010-4756 (no fix in Debian 12), not the
        // DSA bundle.
        let ids: Vec<&str> = o.iter().map(|x| x.advisory.ids[0].as_str()).collect();
        assert_eq!(ids, vec!["DEBIAN-CVE-2023-4911", "DEBIAN-CVE-2024-5535"]);
        let g = &o[0];
        assert_eq!(
            (g.ecosystem.as_str(), g.package.as_str()),
            ("docker", "debian")
        );
        assert_eq!(g.version, format!("12-slim@{OLD}"));
        assert!(
            g.advisory.ids.contains(&"CVE-2023-4911".to_string()),
            "clusters with the CVE"
        );
        assert!(g.advisory.fixed.is_empty() && g.advisory.ranges.is_empty());
        assert_eq!(
            g.path,
            vec!["Debian:12 glibc 2.36-9+deb12u1, fixed in 2.36-9+deb12u3"]
        );
        assert!(g.advisory.title.contains("CVE-2023-4911"));
        // The current image: nothing fixable left.
        assert!(observations(OSV_NEW, SBOM_NEW, &image.unpinned("12-slim")).is_empty());
    }

    fn findings_of(obs: &[Observation]) -> Vec<(Finding, FindingState)> {
        let now = chrono::Utc::now();
        let mut out: Vec<(Finding, FindingState)> = Vec::new();
        for o in obs {
            let idx = out.iter().position(|(f, _)| {
                sec::merges_into(f, &o.advisory.ids, &o.repo, &o.manifest, &o.package)
            });
            let (f, st, _) = sec::apply(idx.map(|i| out[i].clone()), o, now);
            match idx {
                Some(i) => out[i] = (f, st),
                None => out.push((f, st)),
            }
        }
        out
    }

    #[test]
    fn one_base_image_is_one_fix_pinned_to_the_new_digest() {
        let image = ImageRef::from_dep("debian:12-slim", OLD).unwrap();
        let mut f = findings_of(&observations(OSV_OLD, SBOM_OLD, &image));
        assert_eq!(f.len(), 2);
        assert!(
            f.iter().all(|(x, _)| x.fixed_version.is_none()),
            "no fix until the read says"
        );
        let r = Resolution {
            manifest: "Dockerfile".into(),
            package: "debian".into(),
            version: image.written(),
            read: true,
            seen: f.iter().flat_map(|(x, _)| x.advisory.ids.clone()).collect(),
            fixes: f
                .iter()
                .map(|(x, _)| (x.advisory.ids[0].clone(), format!("12-slim@{NEW}")))
                .collect(),
            ..Default::default()
        };
        let cfg = sec::SecurityConfig {
            enabled: true,
            ..Default::default()
        };
        let changed = apply_resolutions(&mut f, "acme/web", std::slice::from_ref(&r), &cfg);
        assert_eq!(changed.len(), 2);
        for (x, _) in &f {
            assert_eq!(
                x.fixed_version.as_deref(),
                Some(format!("12-slim@{NEW}").as_str())
            );
            assert_eq!(x.verdict, Verdict::Unknown);
            assert_eq!(x.evidence.as_ref().unwrap().summary, NOT_CODE);
            assert!(x.tier.is_some());
        }
        // Applying the same read again changes nothing.
        assert!(apply_resolutions(&mut f, "acme/web", std::slice::from_ref(&r), &cfg).is_empty());
        // One group, one unit, one bump to the pinned digest.
        let groups = sec::groups(&f);
        assert_eq!(groups.len(), 1);
        let units = sec::plan_units(&f, &cfg, chrono::Utc::now());
        assert_eq!(units.len(), 1, "{units:?}");
        let u = &units[0];
        assert_eq!(u.ecosystem, "docker");
        assert_eq!(u.finding_ids.len(), 2);
        assert_eq!(u.bumps[0].to, format!("12-slim@{NEW}"));
        let members: Vec<&Finding> = f.iter().map(|(x, _)| x).collect();
        let body = sec::ticket_body(u, &members, &cfg);
        assert!(
            body.contains(&format!(
                "replace `FROM debian:12-slim@{OLD}` with `FROM debian:12-slim@{NEW}`"
            )),
            "{body}"
        );
        assert!(body.contains("Debian:12 glibc 2.36-9+deb12u1, fixed in 2.36-9+deb12u3"));
        assert!(
            !body.contains("lockfile"),
            "not the dependency instructions"
        );
    }

    #[test]
    fn a_from_line_whose_image_no_longer_has_the_version_closes_it() {
        let image = ImageRef::from_dep("debian:12-slim", OLD).unwrap();
        let mut f = findings_of(&observations(OSV_OLD, SBOM_OLD, &image));
        let cfg = sec::SecurityConfig::default();
        // The Dockerfile now pins the new digest, whose read shows neither.
        let now = Resolution {
            manifest: "Dockerfile".into(),
            package: "debian".into(),
            version: format!("12-slim@{NEW}"),
            read: true,
            ..Default::default()
        };
        let changed = apply_resolutions(&mut f, "acme/web", std::slice::from_ref(&now), &cfg);
        assert_eq!(
            changed.iter().filter(|(_, c)| *c == Change::Fixed).count(),
            2
        );
        assert!(f.iter().all(|(x, _)| x.status == Status::Fixed));
        // A read that failed (no package list) closes nothing.
        let mut g = findings_of(&observations(OSV_OLD, SBOM_OLD, &image));
        let unread = Resolution {
            read: false,
            note: NO_PACKAGE_LIST.into(),
            ..now
        };
        apply_resolutions(&mut g, "acme/web", &[unread], &cfg);
        assert!(g.iter().all(|(x, _)| x.status.is_open()));
        // Another repository's findings are left alone.
        let mut h = findings_of(&observations(OSV_OLD, SBOM_OLD, &image));
        assert!(apply_resolutions(&mut h, "acme/other", &[], &cfg).is_empty());
    }

    #[test]
    fn a_patch_tag_can_move_within_its_line() {
        assert_eq!(
            candidate_tags(&ImageRef::parse("node:20.11.0-bookworm-slim").unwrap()),
            vec!["20.11-bookworm-slim"]
        );
        assert_eq!(
            candidate_tags(&ImageRef::from_dep("debian:12-slim", OLD).unwrap()),
            vec!["12-slim"]
        );
        assert!(candidate_tags(&ImageRef::parse("debian:12-slim").unwrap()).is_empty());
    }

    #[test]
    fn registries_say_where_to_get_a_token() {
        let (realm, p) = bearer_challenge(r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/debian:pull""#).unwrap();
        assert_eq!(realm, "https://auth.docker.io/token");
        assert_eq!(
            p,
            vec![
                ("service".to_string(), "registry.docker.io".to_string()),
                (
                    "scope".to_string(),
                    "repository:library/debian:pull".to_string()
                )
            ]
        );
        assert!(bearer_challenge(r#"Bearer realm="http://insecure/token""#).is_none());
        assert!(bearer_challenge("Basic realm=x").is_none());
    }
}
