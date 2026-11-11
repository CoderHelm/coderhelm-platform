//! Dockerfile base images: the worker's side (`common::base_images` has
//! the rules). For each `FROM` of a repository's Dockerfiles it reads the
//! image's package list from its registry (anonymous pulls only, bounded
//! sizes), asks OSV about each OS package in the image's release, and
//! looks for the same tag's newer digest that carries the fixes.

use std::collections::{BTreeSet, HashMap};

use common::base_images::{self as bi, ImageRef, OsPackage, Resolution};
use common::security_fixes::{Dep, Observation};
use serde_json::Value;

use crate::WorkerState;

/// Base images read per repository sync.
const MAX_IMAGES: usize = 8;
/// OSV records read per repository sync for base images.
const MAX_RECORDS: usize = 300;

/// An anonymous registry reader, with one token per repository.
pub struct Registry<'a> {
    http: &'a reqwest::Client,
    tokens: HashMap<String, String>,
}

/// What reading one reference gave.
pub struct Read {
    /// The manifest (index) digest the reference points at.
    pub digest: String,
    /// The linux/amd64 package list; `None` when none is published.
    pub packages: Option<Vec<OsPackage>>,
    pub note: String,
}

impl<'a> Registry<'a> {
    pub fn new(http: &'a reqwest::Client) -> Self {
        Registry {
            http,
            tokens: HashMap::new(),
        }
    }

    async fn token(&mut self, r: &ImageRef, challenge: &str) -> Result<String, String> {
        let (realm, params) =
            bi::bearer_challenge(challenge).ok_or("the registry asks for sign-in this can't do")?;
        let mut req = self
            .http
            .get(&realm)
            .timeout(std::time::Duration::from_secs(20));
        for (k, v) in &params {
            req = req.query(&[(k.as_str(), v.as_str())]);
        }
        if !params.iter().any(|(k, _)| k == "scope") {
            req = req.query(&[("scope", format!("repository:{}:pull", r.repository))]);
        }
        let v: Value = req
            .send()
            .await
            .map_err(|e| format!("token: {}", e.without_url()))?
            .json()
            .await
            .map_err(|_| "token: not JSON".to_string())?;
        let t = v["token"]
            .as_str()
            .or(v["access_token"].as_str())
            .ok_or("the registry gave no anonymous token")?;
        Ok(t.to_string())
    }

    /// `GET /v2/<repo>/<path>`, signing in anonymously when asked.
    async fn get(
        &mut self,
        r: &ImageRef,
        path: &str,
        accept: &str,
        max: usize,
    ) -> Result<(Vec<u8>, Option<String>), String> {
        let url = format!("https://{}/v2/{}/{path}", r.registry, r.repository);
        let key = format!("{}|{}", r.registry, r.repository);
        for attempt in 0..2 {
            let mut req = self
                .http
                .get(&url)
                .header("accept", accept)
                .timeout(std::time::Duration::from_secs(60));
            if let Some(t) = self.tokens.get(&key) {
                req = req.bearer_auth(t);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| format!("{}: {}", r.registry, e.without_url()))?;
            let code = resp.status().as_u16();
            if code == 401 && attempt == 0 {
                let challenge = resp
                    .headers()
                    .get("www-authenticate")
                    .and_then(|h| h.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let t = self.token(r, &challenge).await?;
                self.tokens.insert(key.clone(), t);
                continue;
            }
            if !(200..300).contains(&code) {
                return Err(format!("{} answered {code}", r.registry));
            }
            if resp.content_length().is_some_and(|n| n as usize > max) {
                return Err("larger than CoderHelm reads".into());
            }
            let digest = resp
                .headers()
                .get("docker-content-digest")
                .and_then(|h| h.to_str().ok())
                .map(String::from);
            let body = resp
                .bytes()
                .await
                .map_err(|e| format!("{}: {}", r.registry, e.without_url()))?;
            if body.len() > max {
                return Err("larger than CoderHelm reads".into());
            }
            return Ok((body.to_vec(), digest));
        }
        Err(format!("{} refused an anonymous pull", r.registry))
    }

    /// Reads a reference: its digest and, when published, the linux/amd64
    /// package list.
    pub async fn read(&mut self, r: &ImageRef) -> Result<Read, String> {
        let (body, header) = self
            .get(
                r,
                &format!("manifests/{}", r.reference()),
                bi::INDEX_TYPES,
                bi::MAX_MANIFEST_BYTES,
            )
            .await?;
        let digest = header.unwrap_or_else(|| bi::digest_of(&body));
        let index: Value =
            serde_json::from_slice(&body).map_err(|_| "the manifest isn't JSON".to_string())?;
        let none = |note: &str| Read {
            digest: digest.clone(),
            packages: None,
            note: note.to_string(),
        };
        let Some((_, Some(att))) = bi::platform_and_attestation(&index) else {
            return Ok(none(bi::NO_PACKAGE_LIST));
        };
        let (body, _) = self
            .get(
                r,
                &format!("manifests/{att}"),
                "application/vnd.oci.image.manifest.v1+json",
                bi::MAX_MANIFEST_BYTES,
            )
            .await?;
        let att: Value =
            serde_json::from_slice(&body).map_err(|_| "the attestation isn't JSON".to_string())?;
        let Some((layer, size)) = bi::spdx_layer(&att) else {
            return Ok(none(bi::NO_PACKAGE_LIST));
        };
        if size as usize > bi::MAX_SBOM_BYTES {
            return Ok(none("the package list is larger than CoderHelm reads"));
        }
        let (body, _) = self
            .get(r, &format!("blobs/{layer}"), "*/*", bi::MAX_SBOM_BYTES)
            .await?;
        let doc: Value =
            serde_json::from_slice(&body).map_err(|_| "the package list isn't JSON".to_string())?;
        Ok(Read {
            digest,
            packages: Some(bi::sbom_packages(&doc)),
            note: String::new(),
        })
    }
}

async fn records(state: &WorkerState, ids: &[String], cache: &mut HashMap<String, Value>) {
    let todo: Vec<&String> = ids
        .iter()
        .filter(|i| !cache.contains_key(*i))
        .take(MAX_RECORDS)
        .collect();
    for id in todo {
        let url = format!(
            "{}/v1/vulns/{}",
            common::security_fixes::OSV_API,
            urlencoding::encode(id)
        );
        if let Ok(r) = state
            .http
            .get(url)
            .timeout(std::time::Duration::from_secs(20))
            .send()
            .await
        {
            if r.status().is_success() {
                if let Ok(v) = r.json::<Value>().await {
                    cache.insert(id.clone(), v);
                }
            }
        }
    }
}

/// The repository's base images: observations for its findings, and what
/// each read found (to close or fix them after the upsert).
pub async fn sync(
    state: &WorkerState,
    repo: &str,
    deps: &[Dep],
    cache: &mut HashMap<String, Value>,
) -> (Vec<Observation>, Vec<Resolution>) {
    let mut reg = Registry::new(&state.http);
    let mut obs = Vec::new();
    let mut out = Vec::new();
    let mut seen_refs = BTreeSet::new();
    for d in deps.iter().filter(|d| d.ecosystem == bi::ECOSYSTEM) {
        if out.len() >= MAX_IMAGES
            || !seen_refs.insert((d.manifest.clone(), d.name.clone(), d.version.clone()))
        {
            continue;
        }
        let Some(image) = ImageRef::from_dep(&d.name, &d.version) else {
            continue;
        };
        let mut res = Resolution {
            manifest: d.manifest.clone(),
            package: image.name.clone(),
            version: image.written(),
            release: bi::release_of_tag(&image),
            ..Default::default()
        };
        let read = match reg.read(&image).await {
            Ok(r) => r,
            Err(e) => {
                res.note = e;
                out.push(res);
                continue;
            }
        };
        let Some(pkgs) = read.packages else {
            res.note = read.note;
            out.push(res);
            continue;
        };
        res.packages = pkgs.len();
        if let Some(r) = bi::release_of_packages(&pkgs) {
            res.release = Some(r);
        }
        let Some(eco) = res.release.as_ref().and_then(bi::osv_ecosystem) else {
            res.note = "OSV has no data for this image's release".into();
            out.push(res);
            continue;
        };
        let queries = bi::osv_queries(&pkgs, &eco);
        let hits = match super::security_fixes::osv_query(
            state,
            queries.iter().map(|q| q.2.clone()).collect(),
        )
        .await
        {
            Ok(h) => h,
            Err(e) => {
                res.note = e;
                out.push(res);
                continue;
            }
        };
        let hits: Vec<(usize, String)> = hits
            .into_iter()
            .filter(|(_, id)| bi::wanted_id(id))
            .collect();
        let ids: Vec<String> = hits
            .iter()
            .map(|(_, id)| id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        records(state, &ids, cache).await;
        // (source, version, ids) of each fixable advisory.
        let mut mine: Vec<(String, String, Vec<String>)> = Vec::new();
        for (i, id) in &hits {
            let (source, version, _) = &queries[*i];
            let Some(o) = cache
                .get(id)
                .and_then(|r| bi::observation(repo, &d.manifest, &image, &eco, source, version, r))
            else {
                continue;
            };
            res.seen.extend(o.advisory.ids.iter().cloned());
            mine.push((source.clone(), version.clone(), o.advisory.ids.clone()));
            obs.push(o);
        }
        res.read = true;
        // The fix: the same line's newer digest, when it carries the fixes.
        if !mine.is_empty() {
            for tag in bi::candidate_tags(&image) {
                let cand = image.unpinned(&tag);
                let Ok(c) = reg.read(&cand).await else {
                    continue;
                };
                if c.digest == read.digest {
                    continue;
                }
                let to = format!("{tag}@{}", c.digest);
                let Some(cpkgs) = c.packages else {
                    // No list to check, but a new build: offer it.
                    for (_, _, ids) in &mine {
                        for id in ids {
                            res.fixes.entry(id.clone()).or_insert_with(|| to.clone());
                        }
                    }
                    break;
                };
                let now_ver = |source: &str| {
                    cpkgs
                        .iter()
                        .find(|p| p.source == source)
                        .map(|p| p.version.clone())
                };
                let changed: Vec<OsPackage> = mine
                    .iter()
                    .filter_map(|(s, v, _)| {
                        now_ver(s).filter(|n| n != v).map(|n| OsPackage {
                            source: s.clone(),
                            version: n,
                            ..Default::default()
                        })
                    })
                    .collect();
                let q = bi::osv_queries(&changed, &eco);
                let still = super::security_fixes::osv_query(
                    state,
                    q.iter().map(|x| x.2.clone()).collect(),
                )
                .await
                .unwrap_or_default();
                let still: BTreeSet<(String, String)> = still
                    .into_iter()
                    .map(|(i, id)| (q[i].0.clone(), id))
                    .collect();
                for (source, version, ids) in &mine {
                    let fixed = match now_ver(source) {
                        None => true,
                        Some(n) if &n == version => false,
                        Some(_) => !ids
                            .iter()
                            .any(|id| still.contains(&(source.clone(), id.clone()))),
                    };
                    if fixed {
                        for id in ids {
                            res.fixes.entry(id.clone()).or_insert_with(|| to.clone());
                        }
                    }
                }
                if mine
                    .iter()
                    .all(|(_, _, ids)| ids.iter().any(|id| res.fixes.contains_key(id)))
                {
                    break;
                }
            }
        }
        out.push(res);
    }
    (obs, out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The live registry (run by hand: `cargo test -p worker base_images -- --ignored`).
    #[tokio::test]
    #[ignore]
    async fn reads_an_official_images_package_list() {
        let http = reqwest::Client::new();
        let mut reg = Registry::new(&http);
        let r = ImageRef::parse("debian:12-slim").unwrap();
        let read = reg.read(&r).await.unwrap();
        assert!(read.digest.starts_with("sha256:"));
        let pkgs = read.packages.expect("a package list");
        assert!(pkgs.iter().any(|p| p.source == "glibc"));
        assert_eq!(
            bi::release_of_packages(&pkgs)
                .and_then(|r| bi::osv_ecosystem(&r))
                .as_deref(),
            Some("Debian:12")
        );
    }
}
