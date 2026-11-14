//! Supply-chain checks CoderHelm runs itself against the package registry.
//!
//! A dependency update's pull request once asked its reader to "run
//! `npm view <package>@<version> maintainers dist.integrity` and compare the
//! result to the lockfile hash". CoderHelm has the network and the registry
//! API; a lookup is never a person's chore. For every package line a
//! dependency change moves, CoderHelm reads the registry's record of the
//! version the lockfile resolves and compares:
//!
//! - the integrity hash the lockfile pins with the one the registry
//!   publishes for that version (a mismatch is a finding CoderHelm acts on,
//!   never an item for a person);
//! - who published the new version against the maintainers of the version
//!   it replaces (a publisher who maintained none of the earlier version is
//!   named, with both lists, as evidence).
//!
//! The parsing and the verdicts are pure; [`check`] does the fetching.

use serde_json::Value;

/// The public npm registry.
pub const NPM_REGISTRY: &str = "https://registry.npmjs.org";

/// The most package lines one run looks up.
pub const MAX_LOOKUPS: usize = 120;

/// One package version a lockfile resolves, with the hash it pins.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pinned {
    pub name: String,
    pub version: String,
    /// `sha512-...` (empty when the lockfile pins none).
    pub integrity: String,
}

/// What the registry says about one version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Published {
    pub integrity: String,
    pub maintainers: Vec<String>,
    /// Who published it, when the registry says.
    pub publisher: String,
}

/// The verdict on one package line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The lockfile's hash is the registry's, and the publisher maintained
    /// the earlier version too (or no earlier version is known).
    Matches,
    /// The hashes differ: the lockfile does not pin what the registry serves.
    Mismatch { lock: String, registry: String },
    /// The hashes match, and the publisher is new to the package.
    NewPublisher {
        publisher: String,
        earlier: Vec<String>,
    },
    /// The lockfile pins no hash for it, so only the registry's is known.
    NoLockHash,
    /// The registry gave no record (the reason).
    Unread(String),
}

/// One looked-up line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Looked {
    pub name: String,
    pub version: String,
    pub verdict: Verdict,
    pub maintainers: Vec<String>,
}

impl Looked {
    /// Whether the line is a finding CoderHelm must act on.
    pub fn is_finding(&self) -> bool {
        matches!(self.verdict, Verdict::Mismatch { .. })
    }

    /// The evidence line, as the pull request quotes it.
    pub fn line(&self) -> String {
        let who = if self.maintainers.is_empty() {
            String::new()
        } else {
            format!("; maintainers {}", self.maintainers.join(", "))
        };
        match &self.verdict {
            Verdict::Matches => format!(
                "`{}@{}`: lockfile integrity matches the registry{who}",
                self.name, self.version
            ),
            Verdict::Mismatch { lock, registry } => format!(
                "`{}@{}`: lockfile integrity `{}` differs from the registry's `{}`",
                self.name,
                self.version,
                short(lock),
                short(registry)
            ),
            Verdict::NewPublisher { publisher, earlier } => format!(
                "`{}@{}`: lockfile integrity matches the registry; published by `{publisher}`, \
                 who maintained none of the earlier version (its maintainers: {})",
                self.name,
                self.version,
                earlier.join(", ")
            ),
            Verdict::NoLockHash => format!(
                "`{}@{}`: the lockfile pins no integrity hash; the registry's record was read{who}",
                self.name, self.version
            ),
            Verdict::Unread(why) => format!(
                "`{}@{}`: the registry gave no record ({why})",
                self.name, self.version
            ),
        }
    }
}

fn short(hash: &str) -> String {
    let h = hash.trim();
    // The algorithm and enough of the digest to tell two apart; the rest of
    // the digest is noise in a sentence, so it is left out on purpose.
    match h.split_once('-') {
        Some((alg, digest)) => format!("{alg}-{}", digest.chars().take(16).collect::<String>()),
        None => h.chars().take(24).collect(),
    }
}

/// The verdict for one line from the lockfile's pin, the registry's record
/// of that version, and the maintainers of the version it replaces.
pub fn verdict(pin: &Pinned, now: &Published, earlier: Option<&Published>) -> Verdict {
    if pin.integrity.trim().is_empty() {
        return Verdict::NoLockHash;
    }
    if now.integrity.trim().is_empty() {
        return Verdict::Unread("the registry's record has no integrity hash".into());
    }
    if pin.integrity.trim() != now.integrity.trim() {
        return Verdict::Mismatch {
            lock: pin.integrity.trim().to_string(),
            registry: now.integrity.trim().to_string(),
        };
    }
    if let Some(e) = earlier.filter(|e| !e.maintainers.is_empty()) {
        if !now.publisher.is_empty() && !e.maintainers.iter().any(|m| m == &now.publisher) {
            return Verdict::NewPublisher {
                publisher: now.publisher.clone(),
                earlier: e.maintainers.clone(),
            };
        }
    }
    Verdict::Matches
}

/// The registry's record of one version (`GET /<name>/<version>`).
pub fn parse_published(v: &Value) -> Published {
    let names = |a: &Value| -> Vec<String> {
        a.as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| m["name"].as_str().map(String::from))
            .collect()
    };
    Published {
        integrity: v["dist"]["integrity"].as_str().unwrap_or("").to_string(),
        maintainers: names(&v["maintainers"]),
        publisher: v["_npmUser"]["name"].as_str().unwrap_or("").to_string(),
    }
}

/// The packages an npm `package-lock.json` (v2 or v3) resolves.
pub fn pins_package_lock(text: &str) -> Vec<Pinned> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    if let Some(pkgs) = v["packages"].as_object() {
        for (path, p) in pkgs {
            let Some(at) = path.rfind("node_modules/") else {
                continue;
            };
            let name = &path[at + "node_modules/".len()..];
            let (Some(version), integrity) = (p["version"].as_str(), p["integrity"].as_str())
            else {
                continue;
            };
            out.push(Pinned {
                name: name.to_string(),
                version: version.to_string(),
                integrity: integrity.unwrap_or("").to_string(),
            });
        }
    }
    out.sort_by(|a, b| (&a.name, &a.version).cmp(&(&b.name, &b.version)));
    out.dedup();
    out
}

/// The packages a `pnpm-lock.yaml` resolves: each `name@version:` key
/// under `packages:` with its `resolution: {integrity: ...}`.
pub fn pins_pnpm_lock(text: &str) -> Vec<Pinned> {
    let mut out = Vec::new();
    let mut in_packages = false;
    let mut current: Option<(String, String)> = None;
    for line in text.lines() {
        if !line.starts_with(' ') && !line.trim().is_empty() {
            in_packages = line.trim_end() == "packages:";
            current = None;
            continue;
        }
        if !in_packages {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        let t = line.trim();
        if indent == 2 && t.ends_with(':') {
            let key = t
                .trim_end_matches(':')
                .trim_matches(|c| c == '\'' || c == '"')
                .trim_start_matches('/');
            current = split_spec(key);
            continue;
        }
        if let (Some((name, version)), Some(at)) = (&current, t.find("integrity:")) {
            let integrity = t[at + "integrity:".len()..]
                .trim()
                .trim_end_matches('}')
                .trim()
                .to_string();
            out.push(Pinned {
                name: name.clone(),
                version: version.clone(),
                integrity,
            });
        }
    }
    out.dedup();
    out
}

/// The packages a yarn v1 `yarn.lock` resolves.
pub fn pins_yarn_lock(text: &str) -> Vec<Pinned> {
    let mut out = Vec::new();
    let mut name = String::new();
    let mut version = String::new();
    for line in text.lines() {
        if !line.starts_with(' ') && line.trim_end().ends_with(':') {
            let first = line.trim_end_matches(':').split(',').next().unwrap_or("");
            name = split_spec(first.trim().trim_matches('"'))
                .map(|(n, _)| n)
                .unwrap_or_default();
            version.clear();
            continue;
        }
        let t = line.trim();
        if let Some(v) = t.strip_prefix("version ") {
            version = v.trim_matches('"').to_string();
        } else if let Some(i) = t.strip_prefix("integrity ") {
            if !name.is_empty() && !version.is_empty() {
                out.push(Pinned {
                    name: name.clone(),
                    version: version.clone(),
                    integrity: i.trim().to_string(),
                });
            }
        }
    }
    out.dedup();
    out
}

/// `name@version` (a scoped name keeps its leading `@`); the version stops
/// at a peer suffix (`1.2.3(react@18.0.0)`).
fn split_spec(spec: &str) -> Option<(String, String)> {
    let at = spec.get(1..)?.find('@')? + 1;
    let name = &spec[..at];
    let version = spec[at + 1..].split('(').next().unwrap_or("").trim();
    (!name.is_empty() && !version.is_empty()).then(|| (name.to_string(), version.to_string()))
}

/// The pins a lockfile at `path` resolves, by its file name; empty for a
/// lockfile this module has no reader for.
pub fn pins(path: &str, text: &str) -> Vec<Pinned> {
    match path.rsplit('/').next().unwrap_or(path) {
        "package-lock.json" | "npm-shrinkwrap.json" => pins_package_lock(text),
        "pnpm-lock.yaml" => pins_pnpm_lock(text),
        "yarn.lock" => pins_yarn_lock(text),
        _ => Vec::new(),
    }
}

/// Whether CoderHelm has a registry check for this lockfile.
pub fn supported(path: &str) -> bool {
    matches!(
        path.rsplit('/').next().unwrap_or(path),
        "package-lock.json" | "npm-shrinkwrap.json" | "pnpm-lock.yaml" | "yarn.lock"
    )
}

/// The lines the head's lockfile moved from the base's: each new pin with
/// the version it replaced, when the base had the package.
pub fn moved(base: &[Pinned], head: &[Pinned]) -> Vec<(Pinned, Option<String>)> {
    head.iter()
        .filter(|h| {
            !base
                .iter()
                .any(|b| b.name == h.name && b.version == h.version)
        })
        .map(|h| {
            let from = base
                .iter()
                .filter(|b| b.name == h.name)
                .map(|b| b.version.clone())
                .next_back();
            (h.clone(), from)
        })
        .collect()
}

/// Reads one version's record from the registry.
pub async fn fetch(
    http: &reqwest::Client,
    registry: &str,
    name: &str,
    version: &str,
) -> Result<Published, String> {
    let url = format!("{}/{name}/{version}", registry.trim_end_matches('/'));
    let resp = http
        .get(&url)
        .header("accept", "application/json")
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| format!("the request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("the registry answered {}", resp.status().as_u16()));
    }
    let v: Value = resp
        .json()
        .await
        .map_err(|e| format!("the answer was not JSON: {e}"))?;
    Ok(parse_published(&v))
}

/// Looks up each moved line (at most [`MAX_LOOKUPS`]), a few at a time.
pub async fn check(
    http: &reqwest::Client,
    registry: &str,
    lines: &[(Pinned, Option<String>)],
) -> Vec<Looked> {
    use futures::stream::{self, StreamExt};
    stream::iter(lines.iter().take(MAX_LOOKUPS).cloned())
        .map(|(pin, from)| async move {
            let now = fetch(http, registry, &pin.name, &pin.version).await;
            let earlier = match &from {
                Some(v) => fetch(http, registry, &pin.name, v).await.ok(),
                None => None,
            };
            match now {
                Ok(p) => Looked {
                    verdict: verdict(&pin, &p, earlier.as_ref()),
                    maintainers: p.maintainers,
                    name: pin.name,
                    version: pin.version,
                },
                Err(e) => Looked {
                    name: pin.name,
                    version: pin.version,
                    verdict: Verdict::Unread(e),
                    maintainers: Vec::new(),
                },
            }
        })
        .buffer_unordered(6)
        .collect()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_lockfile_hash_is_compared_with_the_registrys() {
        let pin = Pinned {
            name: "left-pad".into(),
            version: "2.0.0".into(),
            integrity: "sha512-AAA".into(),
        };
        let reg = parse_published(&json!({
            "dist": {"integrity": "sha512-AAA"},
            "maintainers": [{"name": "alice"}],
            "_npmUser": {"name": "alice"}
        }));
        let earlier = Published {
            maintainers: vec!["alice".into()],
            ..Default::default()
        };
        let ok = verdict(&pin, &reg, Some(&earlier));
        assert_eq!(ok, Verdict::Matches);
        let bad = verdict(
            &Pinned {
                integrity: "sha512-BBB".into(),
                ..pin.clone()
            },
            &reg,
            None,
        );
        assert!(matches!(bad, Verdict::Mismatch { .. }));
        let looked = Looked {
            name: "left-pad".into(),
            version: "2.0.0".into(),
            verdict: bad,
            maintainers: vec![],
        };
        assert!(looked.is_finding());
        assert!(!looked.line().contains('…'));
        let stranger = Published {
            publisher: "mallory".into(),
            ..reg.clone()
        };
        assert!(matches!(
            verdict(&pin, &stranger, Some(&earlier)),
            Verdict::NewPublisher { .. }
        ));
    }

    #[test]
    fn lockfiles_are_read() {
        let npm = r#"{"lockfileVersion":3,"packages":{"":{"name":"x"},
            "node_modules/lodash.omit":{"version":"4.18.0","integrity":"sha512-AAA"},
            "node_modules/@scope/pkg":{"version":"1.0.0","integrity":"sha512-BBB"}}}"#;
        let p = pins("web/package-lock.json", npm);
        assert_eq!(p.len(), 2);
        assert!(p
            .iter()
            .any(|x| x.name == "@scope/pkg" && x.version == "1.0.0"));
        let pnpm = "lockfileVersion: '9.0'\n\npackages:\n\n  '@scope/pkg@1.0.0':\n    resolution: {integrity: sha512-BBB}\n\n  lodash.omit@4.18.0(react@18.0.0):\n    resolution: {integrity: sha512-AAA}\n\nsnapshots:\n\n  lodash.omit@4.18.0:\n    dependencies: {}\n";
        let p = pins("pnpm-lock.yaml", pnpm);
        assert_eq!(p.len(), 2, "{p:?}");
        assert_eq!(p[1].name, "lodash.omit");
        assert_eq!(p[1].integrity, "sha512-AAA");
        let yarn = "\"lodash.omit@^4.0.0\", lodash.omit@^4.18.0:\n  version \"4.18.0\"\n  resolved \"https://x\"\n  integrity sha512-AAA\n";
        let p = pins("yarn.lock", yarn);
        assert_eq!(p[0].name, "lodash.omit");
        let base = vec![Pinned {
            name: "lodash.omit".into(),
            version: "4.5.0".into(),
            integrity: "sha512-OLD".into(),
        }];
        let m = moved(&base, &p);
        assert_eq!(m[0].1.as_deref(), Some("4.5.0"));
    }
}
