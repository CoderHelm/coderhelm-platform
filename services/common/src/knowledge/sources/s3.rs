//! Importing documents from an Amazon S3 bucket (a folder of docs, or the
//! S3 data source behind a Bedrock knowledge base) into a knowledge base.
//!
//! The bucket is read with the team's `CoderHelmLogReader` role in the
//! bucket's account, taken on with the team's private ID, on hosted and
//! self-hosted alike: the role's `ReadImportBuckets` statement (both log
//! reader templates) names the buckets it may read, so an admin chooses
//! every bucket in AWS itself, and the install's own role never gains read
//! access to the account's buckets.
//!
//! - [`Connector::browse`] lists one level of the bucket (its folders and
//!   supported files); `/` is the whole bucket.
//! - [`Connector::list`] lists every supported object under the picked
//!   folders, with its ETag (and its sidecar's) as the version marker, so a
//!   sync fetches only what changed and removes what was deleted.
//! - [`Connector::fetch`] reads one object as text, and its
//!   `<file>.metadata.json` sidecar (the Bedrock data source format) for its
//!   title, original link and date.

use std::collections::HashMap;

use serde_json::{json, Value};

use super::super::extract::{extract, Kind as FileKind};
use super::connector::{Connector, ConnectorError, Document, Listed, Listing, Node};
use super::{Kind, Source};

/// A Bedrock data source's metadata file for `<key>`.
pub const SIDECAR: &str = ".metadata.json";
/// The selection id of the whole bucket.
pub const WHOLE: &str = "/";
/// The largest sidecar read (Bedrock allows about 10 KB).
const MAX_SIDECAR_BYTES: u64 = 64 * 1024;
/// Listing pages one sync reads at most (1,000 objects each).
const MAX_PAGES: usize = 50;

/// One object in a listing.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Object {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: String,
}

/// One page of a listing.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Page {
    pub objects: Vec<Object>,
    /// Folders directly under the prefix (a listing by `/`).
    pub prefixes: Vec<String>,
    pub next: Option<String>,
}

/// The bucket, as the connector reads it. The seam tests fake.
#[async_trait::async_trait]
pub trait Objects: Send + Sync {
    /// One page of the objects under `prefix`; `by_folder` lists one level.
    async fn list(
        &self,
        prefix: &str,
        by_folder: bool,
        token: Option<String>,
    ) -> Result<Page, ConnectorError>;
    /// One object, refused when it is larger than `max` bytes.
    async fn get(&self, key: &str, max: u64) -> Result<Vec<u8>, ConnectorError>;
}

/// A bucket name as S3 allows it.
pub fn valid_bucket(b: &str) -> bool {
    (3..=63).contains(&b.len())
        && b.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
        && b.starts_with(|c: char| c.is_ascii_alphanumeric())
        && b.ends_with(|c: char| c.is_ascii_alphanumeric())
        && !b.contains("..")
}

/// `bucket` from an `arn:aws:s3:::bucket` ARN or a plain name.
pub fn bucket_of(raw: &str) -> Option<String> {
    let b = raw
        .trim()
        .strip_prefix("arn:aws:s3:::")
        .unwrap_or(raw.trim());
    let b = b.trim_end_matches('/');
    valid_bucket(b).then(|| b.to_string())
}

/// What a source reads: its account, bucket and region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Where {
    pub account: String,
    pub bucket: String,
    pub region: String,
}

/// A source's options, checked.
pub fn where_of(options: &Value) -> Result<Where, String> {
    let account = options["account"].as_str().unwrap_or("").trim().to_string();
    if account.len() != 12 || !account.chars().all(|c| c.is_ascii_digit()) {
        return Err("Pick the AWS account the bucket is in.".into());
    }
    let bucket = bucket_of(options["bucket"].as_str().unwrap_or(""))
        .ok_or("That isn't an S3 bucket name. It looks like my-team-docs.")?;
    let region = options["region"].as_str().unwrap_or("").trim().to_string();
    if !region.is_empty()
        && !region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err("That isn't an AWS region.".into());
    }
    Ok(Where {
        account,
        bucket,
        region,
    })
}

/// The S3 console page of an object, which opens only for people who have
/// access there.
pub fn console_url(bucket: &str, region: &str, key: &str) -> String {
    format!(
        "https://s3.console.aws.amazon.com/s3/object/{bucket}?region={region}&prefix={}",
        urlencoding::encode(key)
    )
}

/// The file name of a key.
fn name_of(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}

/// Is `key` a document the knowledge base reads (not a sidecar, a folder
/// marker or a hidden file)?
pub fn readable(key: &str) -> bool {
    let name = name_of(key);
    !key.ends_with('/')
        && !key.ends_with(SIDECAR)
        && !name.starts_with('.')
        && FileKind::of(name).is_some()
}

/// A sidecar's values: `metadataAttributes` with plain values or Bedrock's
/// typed `{value: {type, stringValue}}` ones.
pub fn sidecar_values(bytes: &[u8]) -> HashMap<String, String> {
    let v: Value = serde_json::from_slice(bytes).unwrap_or_default();
    let attrs = v
        .get("metadataAttributes")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    attrs
        .into_iter()
        .filter_map(|(k, x)| {
            let val = match &x {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Object(_) => {
                    let inner = x.get("value").unwrap_or(&x);
                    inner["stringValue"]
                        .as_str()
                        .map(str::to_string)
                        .or_else(|| inner["numberValue"].as_f64().map(|n| n.to_string()))?
                }
                _ => return None,
            };
            (!val.trim().is_empty()).then(|| (k, val.trim().to_string()))
        })
        .collect()
}

fn first<'a>(m: &'a HashMap<String, String>, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|k| m.get(*k).map(String::as_str))
}

/// The S3 connector.
pub struct S3<'a> {
    pub objects: Box<dyn Objects + 'a>,
    pub bucket: String,
    pub region: String,
    /// Items one source keeps.
    pub max_items: usize,
}

impl S3<'_> {
    /// Every object under `prefix`, by page, at most `limit` plus one (so a
    /// bigger folder is known to be bigger).
    async fn all_under(&self, prefix: &str, limit: usize) -> Result<Vec<Object>, ConnectorError> {
        let mut out = Vec::new();
        let mut token = None;
        for _ in 0..MAX_PAGES {
            let page = self.objects.list(prefix, false, token.take()).await?;
            out.extend(page.objects);
            match page.next {
                Some(t) if out.len() <= limit => token = Some(t),
                _ => break,
            }
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl Connector for S3<'_> {
    fn kind(&self) -> Kind {
        Kind::S3
    }

    async fn browse(&self, parent: Option<&str>, query: &str) -> Result<Vec<Node>, ConnectorError> {
        let prefix = parent.filter(|p| *p != WHOLE).unwrap_or("");
        let mut nodes = Vec::new();
        if prefix.is_empty() {
            nodes.push(Node {
                id: WHOLE.into(),
                title: format!("Everything in {}", self.bucket),
                kind: "prefix".into(),
                selectable: true,
                ..Default::default()
            });
        }
        let mut token = None;
        for _ in 0..5 {
            let page = self.objects.list(prefix, true, token.take()).await?;
            for p in &page.prefixes {
                let title = p.trim_end_matches('/').rsplit('/').next().unwrap_or(p);
                if super::connector::matches(query, title) {
                    nodes.push(Node {
                        id: p.clone(),
                        title: title.to_string(),
                        kind: "prefix".into(),
                        has_children: true,
                        selectable: true,
                        detail: "Folder".into(),
                        ..Default::default()
                    });
                }
            }
            for o in page.objects.iter().filter(|o| readable(&o.key)) {
                let title = name_of(&o.key);
                if super::connector::matches(query, title) {
                    nodes.push(Node {
                        id: o.key.clone(),
                        title: title.to_string(),
                        kind: "file".into(),
                        selectable: true,
                        updated_at: o.last_modified.clone(),
                        bytes: Some(o.size),
                        url: console_url(&self.bucket, &self.region, &o.key),
                        ..Default::default()
                    });
                }
            }
            match page.next {
                Some(t) if nodes.len() < 500 => token = Some(t),
                _ => break,
            }
        }
        Ok(nodes)
    }

    async fn list(&self, source: &Source) -> Result<Listing, ConnectorError> {
        let limit = self.max_items;
        let mut objects: Vec<Object> = Vec::new();
        for sel in &source.selection {
            let prefix = if sel.id == WHOLE { "" } else { sel.id.as_str() };
            let under = self.all_under(prefix, limit).await?;
            if sel.kind == "file" {
                objects.extend(under.into_iter().filter(|o| o.key == sel.id));
            } else {
                objects.extend(under);
            }
        }
        // Sidecars, by the document they describe.
        let sidecars: HashMap<String, String> = objects
            .iter()
            .filter_map(|o| {
                o.key
                    .strip_suffix(SIDECAR)
                    .map(|doc| (doc.to_string(), o.etag.clone()))
            })
            .collect();
        let mut items: Vec<Listed> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for o in objects.iter().filter(|o| readable(&o.key)) {
            if !seen.insert(o.key.clone()) {
                continue;
            }
            let sidecar = sidecars.get(&o.key);
            let folder: Vec<&str> = o.key.split('/').collect();
            items.push(Listed {
                external_id: o.key.clone(),
                title: name_of(&o.key).to_string(),
                url: console_url(&self.bucket, &self.region, &o.key),
                updated_at: o.last_modified.clone(),
                version: match sidecar {
                    Some(s) => format!("{}+{s}", o.etag),
                    None => o.etag.clone(),
                },
                path: folder[..folder.len().saturating_sub(1)].join(" / "),
                hint: json!({ "sidecar": sidecar.is_some(), "size": o.size }),
            });
        }
        let truncated = items.len() > limit;
        items.truncate(limit);
        Ok(Listing {
            items,
            complete: true,
            truncated,
            ..Default::default()
        })
    }

    async fn fetch(&self, _source: &Source, item: &Listed) -> Result<Document, ConnectorError> {
        let key = &item.external_id;
        let kind = FileKind::of(name_of(key))
            .ok_or_else(|| ConnectorError::Config("This kind of file isn't read.".into()))?;
        let max = super::super::MAX_FILE_BYTES;
        if item.hint["size"].as_u64().is_some_and(|s| s > max) {
            return Err(ConnectorError::Config(format!(
                "The file is larger than {} MB.",
                max / (1024 * 1024)
            )));
        }
        let bytes = self.objects.get(key, max).await?;
        let extracted = tokio::task::spawn_blocking(move || extract(kind, &bytes))
            .await
            .map_err(|_| ConnectorError::Temporary("The file couldn't be read.".into()))?
            .map_err(ConnectorError::Config)?;
        let meta = if item.hint["sidecar"].as_bool() == Some(true) {
            match self
                .objects
                .get(&format!("{key}{SIDECAR}"), MAX_SIDECAR_BYTES)
                .await
            {
                Ok(b) => sidecar_values(&b),
                Err(_) => HashMap::new(),
            }
        } else {
            HashMap::new()
        };
        let title = first(
            &meta,
            &["title", "Title", "document_title", "x-amz-bedrock-kb-title"],
        )
        .map(str::to_string)
        .unwrap_or_else(|| item.title.clone());
        let url = first(
            &meta,
            &[
                "url",
                "source_url",
                "sourceUrl",
                "source",
                "link",
                "x-amz-bedrock-kb-source-uri",
            ],
        )
        .and_then(super::super::answer::original_url)
        .unwrap_or_else(|| item.url.clone());
        let updated_at = first(
            &meta,
            &[
                "updated_at",
                "updatedAt",
                "last_modified",
                "lastModified",
                "modified",
                "date",
            ],
        )
        .map(str::to_string)
        .or(extracted.as_of)
        .unwrap_or_else(|| item.updated_at.clone());
        let mut sections = extracted.sections;
        for s in sections.iter_mut() {
            if s.loc.is_empty() {
                s.loc = title.clone();
            }
        }
        Ok(Document {
            title,
            url,
            updated_at,
            sections,
        })
    }
}

// ── The bucket in AWS ────────────────────────────────────────────────────────

/// A bucket read with a session on the team's role.
pub struct AwsObjects {
    pub client: aws_sdk_s3::Client,
    pub bucket: String,
    /// Says which role and account in errors.
    pub account: String,
}

impl AwsObjects {
    pub fn new(s: &crate::live::aws::Session, w: &Where) -> AwsObjects {
        use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
        let creds = Credentials::new(
            &s.access_key_id,
            &s.secret_access_key,
            Some(s.session_token.clone()),
            None,
            "coderhelm-team-role",
        );
        let region = if w.region.is_empty() {
            s.region.clone()
        } else {
            w.region.clone()
        };
        AwsObjects {
            client: aws_sdk_s3::Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .behavior_version(BehaviorVersion::latest())
                    .region(Region::new(region))
                    .credentials_provider(creds)
                    .build(),
            ),
            bucket: w.bucket.clone(),
            account: w.account.clone(),
        }
    }
}

/// S3's error `code` for bucket `bucket`, in plain words.
pub fn s3_error(code: &str, message: &str, bucket: &str, account: &str) -> ConnectorError {
    match code {
        "AccessDenied" | "AccessDeniedException" | "AllAccessDisabled" => {
            if message.to_ascii_lowercase().contains("kms") {
                ConnectorError::Forbidden(format!(
                    "The CoderHelmLogReader role in account {account} can read bucket {bucket} but not decrypt it: its KMS key doesn't let the role use it through S3."
                ))
            } else {
                ConnectorError::Forbidden(format!(
                    "The CoderHelmLogReader role in account {account} can't read bucket {bucket}. Name the bucket in the role's update stack (ImportBuckets)."
                ))
            }
        }
        "NoSuchBucket" => ConnectorError::NotFound(format!("There is no bucket named {bucket}.")),
        "NoSuchKey" | "NotFound" => {
            ConnectorError::NotFound(format!("The file is no longer in bucket {bucket}."))
        }
        "PermanentRedirect" | "AuthorizationHeaderMalformed" | "IllegalLocationConstraintException" => {
            ConnectorError::Config(format!(
                "Bucket {bucket} is in another region. Edit the source and pick the bucket's region."
            ))
        }
        "SlowDown" | "ThrottlingException" | "RequestLimitExceeded" => {
            ConnectorError::RateLimited("S3 asked us to slow down.".into())
        }
        "" => ConnectorError::Temporary(format!(
            "Coderhelm couldn't reach S3 ({}). An install in a private network needs a VPC endpoint for S3.",
            crate::truncate_str(message, 160)
        )),
        other => ConnectorError::Temporary(format!(
            "S3 couldn't read bucket {bucket} ({other})."
        )),
    }
}

#[async_trait::async_trait]
impl Objects for AwsObjects {
    async fn list(
        &self,
        prefix: &str,
        by_folder: bool,
        token: Option<String>,
    ) -> Result<Page, ConnectorError> {
        use aws_sdk_s3::error::ProvideErrorMetadata;
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .set_continuation_token(token);
        if by_folder {
            req = req.delimiter("/");
        }
        let out = req.send().await.map_err(|e| match e.as_service_error() {
            Some(se) => s3_error(
                se.code().unwrap_or("Unknown"),
                se.message().unwrap_or(""),
                &self.bucket,
                &self.account,
            ),
            None => s3_error(
                "",
                &aws_sdk_s3::error::DisplayErrorContext(&e).to_string(),
                &self.bucket,
                &self.account,
            ),
        })?;
        Ok(Page {
            objects: out
                .contents()
                .iter()
                .filter_map(|o| {
                    Some(Object {
                        key: o.key()?.to_string(),
                        size: o.size().unwrap_or(0).max(0) as u64,
                        etag: o.e_tag().unwrap_or("").trim_matches('"').to_string(),
                        last_modified: o
                            .last_modified()
                            .and_then(|d| {
                                d.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime).ok()
                            })
                            .unwrap_or_default(),
                    })
                })
                .collect(),
            prefixes: out
                .common_prefixes()
                .iter()
                .filter_map(|p| p.prefix().map(str::to_string))
                .collect(),
            next: out
                .next_continuation_token()
                .filter(|_| out.is_truncated().unwrap_or(false))
                .map(str::to_string),
        })
    }

    async fn get(&self, key: &str, max: u64) -> Result<Vec<u8>, ConnectorError> {
        use aws_sdk_s3::error::ProvideErrorMetadata;
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| match e.as_service_error() {
                Some(se) => s3_error(
                    se.code().unwrap_or("Unknown"),
                    se.message().unwrap_or(""),
                    &self.bucket,
                    &self.account,
                ),
                None => s3_error(
                    "",
                    &aws_sdk_s3::error::DisplayErrorContext(&e).to_string(),
                    &self.bucket,
                    &self.account,
                ),
            })?;
        if out.content_length().unwrap_or(0).max(0) as u64 > max {
            return Err(ConnectorError::Config(format!(
                "The file is larger than {} MB.",
                max / (1024 * 1024)
            )));
        }
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|_| ConnectorError::Temporary("The file couldn't be read from S3.".into()))?
            .into_bytes();
        if bytes.len() as u64 > max {
            return Err(ConnectorError::Config("The file is too large.".into()));
        }
        Ok(bytes.to_vec())
    }
}

/// The region a bucket is in (`GetBucketLocation`), with a session in any
/// region.
pub async fn bucket_region(objects: &AwsObjects) -> Result<String, ConnectorError> {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    let out = objects
        .client
        .get_bucket_location()
        .bucket(&objects.bucket)
        .send()
        .await
        .map_err(|e| match e.as_service_error() {
            Some(se) => s3_error(
                se.code().unwrap_or("Unknown"),
                se.message().unwrap_or(""),
                &objects.bucket,
                &objects.account,
            ),
            None => s3_error(
                "",
                &aws_sdk_s3::error::DisplayErrorContext(&e).to_string(),
                &objects.bucket,
                &objects.account,
            ),
        })?;
    // No constraint means us-east-1; "EU" is the old name of eu-west-1.
    Ok(match out.location_constraint().map(|c| c.as_str()) {
        None | Some("") => "us-east-1".into(),
        Some("EU") => "eu-west-1".into(),
        Some(r) => r.to_string(),
    })
}

/// The buckets of a team's S3 sources in `account`, for the log reader
/// update that lets the role read them.
pub fn buckets_in(sources: &[Source], account: &str) -> Vec<String> {
    let mut out: Vec<String> = sources
        .iter()
        .filter(|s| s.kind == Some(Kind::S3))
        .filter_map(|s| where_of(&s.options).ok())
        .filter(|w| w.account == account)
        .map(|w| w.bucket)
        .collect();
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
pub mod fake {
    //! An in-memory bucket.

    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    pub struct FakeBucket {
        pub objects: Mutex<Vec<(Object, Vec<u8>)>>,
        /// Objects per listing page.
        pub page: usize,
        pub refuse: Mutex<Option<ConnectorError>>,
        pub gets: Mutex<Vec<String>>,
    }

    impl FakeBucket {
        pub fn put(&self, key: &str, etag: &str, body: &[u8]) {
            let mut o = self.objects.lock().unwrap();
            o.retain(|(x, _)| x.key != key);
            o.push((
                Object {
                    key: key.into(),
                    size: body.len() as u64,
                    etag: etag.into(),
                    last_modified: "2026-10-01T00:00:00Z".into(),
                },
                body.to_vec(),
            ));
            o.sort_by(|a, b| a.0.key.cmp(&b.0.key));
        }
        pub fn remove(&self, key: &str) {
            self.objects.lock().unwrap().retain(|(x, _)| x.key != key);
        }
    }

    #[async_trait::async_trait]
    impl Objects for FakeBucket {
        async fn list(
            &self,
            prefix: &str,
            by_folder: bool,
            token: Option<String>,
        ) -> Result<Page, ConnectorError> {
            if let Some(e) = self.refuse.lock().unwrap().clone() {
                return Err(e);
            }
            let all: Vec<Object> = self
                .objects
                .lock()
                .unwrap()
                .iter()
                .map(|(o, _)| o.clone())
                .filter(|o| o.key.starts_with(prefix))
                .collect();
            let (objects, prefixes): (Vec<Object>, Vec<String>) = if by_folder {
                let mut folders = std::collections::BTreeSet::new();
                let mut here = Vec::new();
                for o in all {
                    match o.key[prefix.len()..].split_once('/') {
                        Some((f, _)) => {
                            folders.insert(format!("{prefix}{f}/"));
                        }
                        None => here.push(o),
                    }
                }
                (here, folders.into_iter().collect())
            } else {
                (all, Vec::new())
            };
            let size = if self.page == 0 { 1000 } else { self.page };
            let start: usize = token.and_then(|t| t.parse().ok()).unwrap_or(0);
            let end = (start + size).min(objects.len());
            Ok(Page {
                objects: objects[start..end].to_vec(),
                prefixes: if start == 0 { prefixes } else { Vec::new() },
                next: (end < objects.len()).then(|| end.to_string()),
            })
        }

        async fn get(&self, key: &str, max: u64) -> Result<Vec<u8>, ConnectorError> {
            self.gets.lock().unwrap().push(key.into());
            let o = self.objects.lock().unwrap();
            let (obj, body) = o
                .iter()
                .find(|(x, _)| x.key == key)
                .ok_or_else(|| ConnectorError::NotFound("gone".into()))?;
            if obj.size > max {
                return Err(ConnectorError::Config("too large".into()));
            }
            Ok(body.clone())
        }
    }

    /// The bucket shared between a test and the connector reading it.
    pub struct Shared(pub std::sync::Arc<FakeBucket>);

    #[async_trait::async_trait]
    impl Objects for Shared {
        async fn list(
            &self,
            prefix: &str,
            by_folder: bool,
            token: Option<String>,
        ) -> Result<Page, ConnectorError> {
            self.0.list(prefix, by_folder, token).await
        }
        async fn get(&self, key: &str, max: u64) -> Result<Vec<u8>, ConnectorError> {
            self.0.get(key, max).await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::fake::{FakeBucket, Shared};
    use super::*;
    use crate::knowledge::search::testing::WordEmbedder;
    use crate::knowledge::sources::store::memory::MemSyncStore;
    use crate::knowledge::sources::store::SyncStore;
    use crate::knowledge::sources::{sync, Selected, Status};

    fn bucket() -> Arc<FakeBucket> {
        let b = Arc::new(FakeBucket::default());
        b.put(
            "runbooks/deploy.md",
            "e1",
            b"# Deploy\nRoll out region by region.",
        );
        b.put(
            "runbooks/queues/drain.md",
            "e2",
            b"# Drain\nDrain the queue before a restart.",
        );
        b.put(
            "runbooks/queues/drain.md.metadata.json",
            "m2",
            br#"{"metadataAttributes": {"title": "Draining queues", "source_url": "https://wiki.example.com/drain", "updated_at": {"value": {"type": "STRING", "stringValue": "2026-09-30"}}}}"#,
        );
        b.put("runbooks/diagram.png", "e3", b"\x89PNG");
        b.put("runbooks/.hidden.md", "e4", b"secret");
        b.put("hr/leave.txt", "e5", b"Sixteen weeks of leave.");
        b
    }

    fn connector(b: &Arc<FakeBucket>, max: usize) -> S3<'static> {
        S3 {
            objects: Box::new(Shared(b.clone())),
            bucket: "team-docs".into(),
            region: "eu-west-1".into(),
            max_items: max,
        }
    }

    fn source(sel: &[(&str, &str)]) -> Source {
        Source {
            id: "src".into(),
            kb_id: "kb".into(),
            kind: Some(Kind::S3),
            selection: sel
                .iter()
                .map(|(id, kind)| Selected {
                    id: id.to_string(),
                    title: id.to_string(),
                    kind: kind.to_string(),
                    include_children: true,
                    ..Default::default()
                })
                .collect(),
            options: json!({"account": "111122223333", "bucket": "team-docs", "region": "eu-west-1"}),
            ..Default::default()
        }
    }

    #[test]
    fn bucket_names_and_options_are_checked() {
        assert!(valid_bucket("team-docs"));
        assert!(valid_bucket("a.b-c1"));
        for bad in ["", "ab", "Team", "-x-", "a..b", "x_y", &"a".repeat(64)] {
            assert!(!valid_bucket(bad), "{bad}");
        }
        assert_eq!(
            bucket_of("arn:aws:s3:::team-docs").as_deref(),
            Some("team-docs")
        );
        assert_eq!(bucket_of("team-docs/").as_deref(), Some("team-docs"));
        let w = where_of(&json!({"account": "111122223333", "bucket": "team-docs"})).unwrap();
        assert_eq!(w.bucket, "team-docs");
        assert!(where_of(&json!({"account": "1", "bucket": "team-docs"}))
            .unwrap_err()
            .contains("account"));
        assert!(
            where_of(&json!({"account": "111122223333", "bucket": "No"}))
                .unwrap_err()
                .contains("bucket")
        );
        assert!(where_of(
            &json!({"account": "111122223333", "bucket": "abc", "region": "US EAST"})
        )
        .is_err());
    }

    #[tokio::test]
    async fn browsing_shows_one_level_of_folders_and_readable_files() {
        let b = bucket();
        let c = connector(&b, 100);
        let top = c.browse(None, "").await.unwrap();
        let ids: Vec<&str> = top.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(ids, [WHOLE, "hr/", "runbooks/"]);
        assert!(top[0].title.contains("team-docs"));
        let inside = c.browse(Some("runbooks/"), "").await.unwrap();
        let ids: Vec<&str> = inside.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(
            ids,
            ["runbooks/queues/", "runbooks/deploy.md"],
            "no images, sidecars or hidden files"
        );
        assert_eq!(inside[1].kind, "file");
        let found = c.browse(Some("runbooks/"), "dep").await.unwrap();
        assert_eq!(found.len(), 1);
    }

    #[tokio::test]
    async fn a_listing_covers_the_folders_picked_with_etags_and_sidecars() {
        let b = bucket();
        let c = connector(&b, 100);
        let l = c.list(&source(&[("runbooks/", "prefix")])).await.unwrap();
        let keys: Vec<&str> = l.items.iter().map(|i| i.external_id.as_str()).collect();
        assert_eq!(keys, ["runbooks/deploy.md", "runbooks/queues/drain.md"]);
        assert_eq!(l.items[0].version, "e1");
        assert_eq!(
            l.items[1].version, "e2+m2",
            "a sidecar change re-reads the document"
        );
        assert_eq!(l.items[1].path, "runbooks / queues");
        assert!(l.complete && !l.truncated);
        // The whole bucket, and one file.
        assert_eq!(
            c.list(&source(&[(WHOLE, "prefix")]))
                .await
                .unwrap()
                .items
                .len(),
            3
        );
        let one = c.list(&source(&[("hr/leave.txt", "file")])).await.unwrap();
        assert_eq!(one.items.len(), 1);
        // Overlapping picks list a document once.
        let both = c
            .list(&source(&[(WHOLE, "prefix"), ("runbooks/", "prefix")]))
            .await
            .unwrap();
        assert_eq!(both.items.len(), 3);
    }

    #[tokio::test]
    async fn a_big_bucket_is_listed_by_page_up_to_the_import_limit() {
        let b = Arc::new(FakeBucket {
            page: 7,
            ..Default::default()
        });
        for i in 0..40 {
            b.put(&format!("docs/{i:03}.md"), "e", b"x");
        }
        let c = connector(&b, 25);
        let l = c.list(&source(&[(WHOLE, "prefix")])).await.unwrap();
        assert_eq!(l.items.len(), 25);
        assert!(l.truncated);
        let c = connector(&b, 100);
        let l = c.list(&source(&[(WHOLE, "prefix")])).await.unwrap();
        assert_eq!(l.items.len(), 40, "every page read");
        assert!(!l.truncated);
    }

    #[tokio::test]
    async fn a_document_reads_its_sidecar_for_title_link_and_date() {
        let b = bucket();
        let c = connector(&b, 100);
        let src = source(&[("runbooks/", "prefix")]);
        let l = c.list(&src).await.unwrap();
        let d = c.fetch(&src, &l.items[1]).await.unwrap();
        assert_eq!(d.title, "Draining queues");
        assert_eq!(d.url, "https://wiki.example.com/drain");
        assert_eq!(d.updated_at, "2026-09-30");
        assert!(d
            .sections
            .iter()
            .any(|s| s.text.contains("Drain the queue")));
        let d = c.fetch(&src, &l.items[0]).await.unwrap();
        assert_eq!(d.title, "deploy.md");
        assert!(d.url.starts_with("https://s3.console.aws.amazon.com/s3/object/team-docs?region=eu-west-1&prefix=runbooks%2Fdeploy.md"));
    }

    #[test]
    fn sidecars_in_both_shapes_are_read() {
        let m = sidecar_values(br#"{"metadataAttributes": {"title": "A", "year": 2026, "kind": {"value": {"type": "STRING", "stringValue": "policy"}}, "n": {"value": {"type": "NUMBER", "numberValue": 3}}, "empty": ""}}"#);
        assert_eq!(m["title"], "A");
        assert_eq!(m["year"], "2026");
        assert_eq!(m["kind"], "policy");
        assert_eq!(m["n"], "3");
        assert!(!m.contains_key("empty"));
        assert!(sidecar_values(b"not json").is_empty());
    }

    #[test]
    fn s3_errors_are_said_in_plain_words() {
        let e = s3_error("AccessDenied", "Access Denied", "team-docs", "111122223333");
        assert!(matches!(e, ConnectorError::Forbidden(_)));
        assert!(e.to_string().contains("ImportBuckets"));
        let e = s3_error(
            "AccessDenied",
            "User is not authorized to perform kms:Decrypt",
            "b1x",
            "111122223333",
        );
        assert!(e.to_string().contains("decrypt"));
        assert!(matches!(
            s3_error("NoSuchBucket", "", "b1x", "1"),
            ConnectorError::NotFound(_)
        ));
        assert!(matches!(
            s3_error("PermanentRedirect", "", "b1x", "1"),
            ConnectorError::Config(_)
        ));
        assert!(matches!(
            s3_error("SlowDown", "", "b1x", "1"),
            ConnectorError::RateLimited(_)
        ));
        assert!(s3_error("", "dispatch failure", "b1x", "1")
            .to_string()
            .contains("VPC endpoint"));
    }

    #[test]
    fn the_update_names_only_this_accounts_buckets() {
        let mut a = source(&[(WHOLE, "prefix")]);
        let mut b = a.clone();
        b.options["bucket"] = json!("other-docs");
        let mut c = a.clone();
        c.options["account"] = json!("444455556666");
        a.id = "a".into();
        let notion = Source {
            kind: Some(Kind::Notion),
            ..Default::default()
        };
        assert_eq!(
            buckets_in(&[a.clone(), a, b, c, notion], "111122223333"),
            ["other-docs", "team-docs"]
        );
    }

    #[tokio::test]
    async fn a_resync_fetches_only_changes_and_removes_deleted_objects() {
        let b = bucket();
        let c = connector(&b, 100);
        let store = MemSyncStore::default();
        let src = source(&[(WHOLE, "prefix")]);
        store.put_source("t", &src).await.unwrap();
        let ctx = sync::Ctx {
            store: &store,
            connector: &c,
            embedder: &WordEmbedder,
            team: "t",
            now: &|| 1_000,
            used: None,
            trigger: "manual",
        };
        let first = sync::run(&ctx, "kb", "src").await;
        assert_eq!((first.fetched, first.indexed), (3, 3));
        let gets_before = b.gets.lock().unwrap().len();
        // Nothing changed: nothing fetched.
        let again = sync::run(&ctx, "kb", "src").await;
        assert_eq!(again.fetched, 0);
        assert_eq!(b.gets.lock().unwrap().len(), gets_before);
        // One edited, one deleted, one sidecar changed.
        b.put("hr/leave.txt", "e5b", b"Eighteen weeks of leave.");
        b.remove("runbooks/deploy.md");
        b.put(
            "runbooks/queues/drain.md.metadata.json",
            "m3",
            br#"{"metadataAttributes": {"title": "Queues"}}"#,
        );
        let third = sync::run(&ctx, "kb", "src").await;
        assert_eq!(third.fetched, 2);
        assert_eq!(third.removed, 1);
        let items = store.items("t", "kb", "src").await.unwrap();
        assert_eq!(items.len(), 2);
        assert!(items.iter().any(|f| f.name == "Queues"));
        let s = store.source("t", "kb", "src").await.unwrap().unwrap();
        assert_eq!(s.status, Status::Ready);
        assert_eq!(s.items, 2);
    }

    #[tokio::test]
    async fn a_bucket_the_role_cant_read_stops_the_sync_with_the_fix() {
        let b = bucket();
        *b.refuse.lock().unwrap() = Some(s3_error("AccessDenied", "", "team-docs", "111122223333"));
        let c = connector(&b, 100);
        let store = MemSyncStore::default();
        store
            .put_source("t", &source(&[(WHOLE, "prefix")]))
            .await
            .unwrap();
        let ctx = sync::Ctx {
            store: &store,
            connector: &c,
            embedder: &WordEmbedder,
            team: "t",
            now: &|| 1_000,
            used: None,
            trigger: "manual",
        };
        let out = sync::run(&ctx, "kb", "src").await;
        assert!(matches!(out.error, Some(ConnectorError::Forbidden(_))));
        let s = store.source("t", "kb", "src").await.unwrap().unwrap();
        assert_eq!(s.status, Status::Error);
        assert!(s.error.unwrap().message.contains("ImportBuckets"));
    }
}
