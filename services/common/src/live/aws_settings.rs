//! Settings of S3 buckets and KMS keys in a team's AWS account, read through
//! its CoderHelmLogReader role, so the reviewer can check a fact about them
//! instead of asking a person.
//!
//! Metadata only: a bucket's default encryption, its bucket policy and
//! whether that policy makes the bucket public; a KMS key's description and
//! key policy. Never an object in a bucket, and never an encrypt or decrypt
//! call. Each read is separate, so a refused one doesn't hide the others,
//! and a refusal names the exact IAM action that was refused.

use super::aws::Session;
use super::StepError;

pub const BUCKET_ENCRYPTION: &str = "s3:GetEncryptionConfiguration";
pub const BUCKET_POLICY: &str = "s3:GetBucketPolicy";
pub const BUCKET_POLICY_STATUS: &str = "s3:GetBucketPolicyStatus";
pub const KEY_DESCRIBE: &str = "kms:DescribeKey";
pub const KEY_POLICY: &str = "kms:GetKeyPolicy";

/// Every action these lookups use (the role's `settings` ability).
pub const ACTIONS: &[&str] = &[
    BUCKET_ENCRYPTION,
    BUCKET_POLICY,
    BUCKET_POLICY_STATUS,
    KEY_DESCRIBE,
    KEY_POLICY,
];

fn refused(message: impl Into<String>) -> StepError {
    StepError::new(
        "read_only",
        message,
        "Give a bucket name (or s3://bucket, or its ARN) or a KMS key id, alias or ARN.",
    )
}

/// A bucket name from a name, an `s3://bucket/…` URL or an
/// `arn:aws:s3:::bucket/…` ARN.
pub fn check_bucket(input: &str) -> Result<String, StepError> {
    let t = input.trim();
    let t = t
        .strip_prefix("s3://")
        .or_else(|| t.strip_prefix("arn:aws:s3:::"))
        .or_else(|| t.strip_prefix("arn:aws-us-gov:s3:::"))
        .or_else(|| t.strip_prefix("arn:aws-cn:s3:::"))
        .unwrap_or(t);
    let name = t.split('/').next().unwrap_or("");
    let ok = (3..=255).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric());
    if ok {
        Ok(name.to_string())
    } else {
        Err(refused(format!("`{input}` isn't an S3 bucket name.")))
    }
}

/// A KMS key as given: its id, alias or ARN, with the region and account
/// an ARN names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRef {
    pub id: String,
    pub region: Option<String>,
    pub account: Option<String>,
}

pub fn check_key(input: &str) -> Result<KeyRef, StepError> {
    let t = input.trim();
    let ok = !t.is_empty()
        && t.len() <= 300
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-' | ':'));
    if !ok {
        return Err(refused(format!(
            "`{input}` isn't a KMS key id, alias or ARN."
        )));
    }
    if t.starts_with("arn:") {
        let parts: Vec<&str> = t.splitn(6, ':').collect();
        let good = parts.len() == 6
            && parts[2] == "kms"
            && !parts[3].is_empty()
            && parts[4].len() == 12
            && (parts[5].starts_with("key/") || parts[5].starts_with("alias/"));
        if !good {
            return Err(refused(format!("`{input}` isn't a KMS key or alias ARN.")));
        }
        return Ok(KeyRef {
            id: t.to_string(),
            region: Some(parts[3].to_string()),
            account: Some(parts[4].to_string()),
        });
    }
    Ok(KeyRef {
        id: t.to_string(),
        region: None,
        account: None,
    })
}

/// How one read ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read<T> {
    Got(T),
    /// The thing read doesn't exist (no bucket policy, no such key).
    Absent(String),
    /// AWS refused the action.
    Denied,
    Failed(String),
}

impl<T> Read<T> {
    pub fn denied(&self) -> bool {
        matches!(self, Read::Denied)
    }
}

/// One default-encryption rule of a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionRule {
    /// `AES256`, `aws:kms` or `aws:kms:dsse`.
    pub algorithm: String,
    pub kms_key: Option<String>,
    pub bucket_key: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketSettings {
    pub bucket: String,
    /// The region the bucket is in, when it isn't the role's.
    pub region: Option<String>,
    pub encryption: Read<Vec<EncryptionRule>>,
    pub policy: Read<String>,
    pub public: Read<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyFacts {
    pub key_id: String,
    pub arn: String,
    /// `CUSTOMER` (customer managed) or `AWS` (AWS managed).
    pub manager: String,
    pub state: String,
    pub enabled: bool,
    pub spec: String,
    pub usage: String,
    pub origin: String,
    pub multi_region: Option<bool>,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySettings {
    pub key: String,
    pub facts: Read<KeyFacts>,
    pub policy: Read<String>,
}

/// What a refused action reads, in plain words.
fn reads(action: &str) -> &'static str {
    match action {
        BUCKET_ENCRYPTION => "the bucket's default encryption",
        BUCKET_POLICY => "the bucket policy",
        BUCKET_POLICY_STATUS => "whether the bucket policy makes it public",
        KEY_DESCRIBE => "the key's description (who manages it, its state)",
        KEY_POLICY => "the key policy",
        _ => "this setting",
    }
}

/// The sentence for a refused action. `role_has` is what the last check of
/// the role found: `Some(false)` it lacks the action, `Some(true)` it has
/// it (so something else refused it), `None` unknown.
pub fn denied_line(action: &str, resource: &str, role_has: Option<bool>) -> String {
    let what = reads(action);
    match role_has {
        Some(false) => format!(
            "Access denied: the CoderHelmLogReader role is missing `{action}`, so I couldn't read {what} for {resource}. An admin adds it with Update on the AWS page (Settings, Integrations, AWS)."
        ),
        Some(true) => format!(
            "Access denied for `{action}` on {resource}: the role allows it, so the resource's own policy, a permissions boundary or an organization policy refused it; I couldn't read {what}."
        ),
        None => format!(
            "Access denied for `{action}` on {resource}: the CoderHelmLogReader role is missing `{action}` (Update on the AWS page in Settings, Integrations, AWS adds it), or the resource's own policy refuses it. I couldn't read {what}."
        ),
    }
}

fn line_for<T>(
    r: &Read<T>,
    action: &str,
    resource: &str,
    role_has: &dyn Fn(&str) -> Option<bool>,
    got: impl Fn(&T) -> String,
) -> String {
    match r {
        Read::Got(v) => got(v),
        Read::Absent(why) => why.clone(),
        Read::Denied => denied_line(action, resource, role_has(action)),
        Read::Failed(why) => format!("Couldn't read {} ({why}).", reads(action)),
    }
}

fn encryption_text(rules: &[EncryptionRule]) -> String {
    if rules.is_empty() {
        return "Default encryption: no rule set.".to_string();
    }
    rules
        .iter()
        .map(|r| {
            let what = match (r.algorithm.as_str(), r.kms_key.as_deref()) {
                ("AES256", _) => {
                    "SSE-S3 (AES256): keys managed by S3, not a KMS key".to_string()
                }
                (alg, Some(k)) if alg.starts_with("aws:kms") => format!(
                    "{} with KMS key `{k}` (read it with aws_kms_key to see whether it is customer managed)",
                    if alg == "aws:kms:dsse" { "DSSE-KMS" } else { "SSE-KMS" }
                ),
                (alg, None) if alg.starts_with("aws:kms") => format!(
                    "{} with the AWS managed key `aws/s3` (not customer managed)",
                    if alg == "aws:kms:dsse" { "DSSE-KMS" } else { "SSE-KMS" }
                ),
                (alg, _) => format!("algorithm {alg}"),
            };
            let bk = match r.bucket_key {
                Some(true) => ", S3 Bucket Key on",
                Some(false) => ", S3 Bucket Key off",
                None => "",
            };
            format!("Default encryption: {what}{bk}.")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The bucket's settings as the agent reads them, with each refusal named.
pub fn format_bucket(b: &BucketSettings, role_has: &dyn Fn(&str) -> Option<bool>) -> String {
    let res = format!("bucket `{}`", b.bucket);
    let mut out = vec![format!(
        "S3 bucket `{}`{}",
        b.bucket,
        b.region
            .as_deref()
            .map(|r| format!(" (in {r})"))
            .unwrap_or_default()
    )];
    out.push(line_for(
        &b.encryption,
        BUCKET_ENCRYPTION,
        &res,
        role_has,
        |r| encryption_text(r),
    ));
    out.push(line_for(
        &b.public,
        BUCKET_POLICY_STATUS,
        &res,
        role_has,
        |p| {
            if *p {
                "Bucket policy status: PUBLIC (the policy grants public access).".to_string()
            } else {
                "Bucket policy status: not public.".to_string()
            }
        },
    ));
    out.push(line_for(&b.policy, BUCKET_POLICY, &res, role_has, |p| {
        format!(
            "Bucket policy:\n{}",
            crate::truncate_str(&pretty_policy(p), 6000)
        )
    }));
    out.join("\n")
}

/// The key's settings as the agent reads them, with each refusal named.
pub fn format_key(k: &KeySettings, role_has: &dyn Fn(&str) -> Option<bool>) -> String {
    let res = format!("KMS key `{}`", k.key);
    let mut out = vec![format!("KMS key `{}`", k.key)];
    out.push(line_for(&k.facts, KEY_DESCRIBE, &res, role_has, |f| {
        let managed = match f.manager.as_str() {
            "CUSTOMER" => "customer managed",
            "AWS" => "AWS managed (not customer managed)",
            _ => "managed by an unknown party",
        };
        format!(
            "Key {} ({}): {managed}; state {}{}; spec {}, usage {}, origin {}{}{}",
            f.key_id,
            f.arn,
            f.state,
            if f.enabled { ", enabled" } else { ", disabled" },
            f.spec,
            f.usage,
            f.origin,
            match f.multi_region {
                Some(true) => ", multi-Region",
                _ => "",
            },
            if f.description.trim().is_empty() {
                String::new()
            } else {
                format!("; description: {}", f.description.trim())
            }
        )
    }));
    out.push(line_for(&k.policy, KEY_POLICY, &res, role_has, |p| {
        format!(
            "Key policy:\n{}",
            crate::truncate_str(&pretty_policy(p), 6000)
        )
    }));
    out.join("\n")
}

fn pretty_policy(p: &str) -> String {
    serde_json::from_str::<serde_json::Value>(p)
        .ok()
        .and_then(|v| serde_json::to_string_pretty(&v).ok())
        .unwrap_or_else(|| p.to_string())
}

/// The actions refused in a bucket read.
pub fn bucket_denied(b: &BucketSettings) -> Vec<&'static str> {
    let mut out = Vec::new();
    if b.encryption.denied() {
        out.push(BUCKET_ENCRYPTION);
    }
    if b.policy.denied() {
        out.push(BUCKET_POLICY);
    }
    if b.public.denied() {
        out.push(BUCKET_POLICY_STATUS);
    }
    out
}

pub fn key_denied(k: &KeySettings) -> Vec<&'static str> {
    let mut out = Vec::new();
    if k.facts.denied() {
        out.push(KEY_DESCRIBE);
    }
    if k.policy.denied() {
        out.push(KEY_POLICY);
    }
    out
}

/// Pure: an AWS error code as the end of a read.
pub fn classify<T>(code: Option<&str>, message: &str, absent: &[(&str, &str)]) -> Read<T> {
    if crate::log_repo::live::is_refusal(code) {
        return Read::Denied;
    }
    if let Some((_, why)) = absent.iter().find(|(c, _)| Some(*c) == code) {
        return Read::Absent(why.to_string());
    }
    Read::Failed(super::error_detail(code.unwrap_or(""), message))
}

macro_rules! client {
    ($krate:ident, $s:expr, $region:expr) => {{
        use $krate::config::{BehaviorVersion, Credentials, Region};
        let s: &Session = $s;
        let creds = Credentials::new(
            &s.access_key_id,
            &s.secret_access_key,
            Some(s.session_token.clone()),
            None,
            "coderhelm-team-role",
        );
        $krate::Client::from_conf(
            $krate::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new($region))
                .credentials_provider(creds)
                .build(),
        )
    }};
}

use aws_sdk_s3::error::ProvideErrorMetadata;

const NO_BUCKET: (&str, &str) = (
    "NoSuchBucket",
    "There is no bucket by that name (check the name, or it is in another account).",
);

/// The region S3 says a bucket is in (`x-amz-bucket-region`), from a call
/// made in another region.
fn bucket_region<E>(
    e: &aws_sdk_s3::error::SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
) -> Option<String> {
    let r = e
        .raw_response()?
        .headers()
        .get("x-amz-bucket-region")?
        .trim();
    let ok = (5..=30).contains(&r.len())
        && r.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    ok.then(|| r.to_string())
}

/// Reads a bucket's encryption, policy and policy status. A call made in the
/// role's region for a bucket elsewhere is made again in the bucket's.
pub async fn read_bucket(s: &Session, bucket: &str) -> BucketSettings {
    let mut region = s.region.clone();
    let mut moved = None;
    let mut s3 = client!(aws_sdk_s3, s, region.clone());
    let mut enc = s3.get_bucket_encryption().bucket(bucket).send().await;
    if let Err(e) = &enc {
        let wrong_region = matches!(
            e.code(),
            Some(
                "PermanentRedirect"
                    | "AuthorizationHeaderMalformed"
                    | "IllegalLocationConstraintException"
            )
        ) || e.code().is_none();
        if let Some(r) = bucket_region(e).filter(|r| *r != region && wrong_region) {
            region = r.clone();
            moved = Some(r);
            s3 = client!(aws_sdk_s3, s, region.clone());
            enc = s3.get_bucket_encryption().bucket(bucket).send().await;
        }
    }
    let encryption = match enc {
        Ok(o) => Read::Got(
            o.server_side_encryption_configuration()
                .map(|c| {
                    c.rules()
                        .iter()
                        .filter_map(|r| {
                            let d = r.apply_server_side_encryption_by_default()?;
                            Some(EncryptionRule {
                                algorithm: d.sse_algorithm().as_str().to_string(),
                                kms_key: d.kms_master_key_id().map(str::to_string),
                                bucket_key: r.bucket_key_enabled(),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default(),
        ),
        Err(e) => classify(
            e.code(),
            e.message().unwrap_or(""),
            &[
                NO_BUCKET,
                (
                    "ServerSideEncryptionConfigurationNotFoundError",
                    "Default encryption: none configured.",
                ),
            ],
        ),
    };
    let policy = match s3.get_bucket_policy().bucket(bucket).send().await {
        Ok(o) => match o.policy() {
            Some(p) if !p.trim().is_empty() => Read::Got(p.to_string()),
            _ => Read::Absent("Bucket policy: none.".into()),
        },
        Err(e) => classify(
            e.code(),
            e.message().unwrap_or(""),
            &[NO_BUCKET, ("NoSuchBucketPolicy", "Bucket policy: none.")],
        ),
    };
    let public = match s3.get_bucket_policy_status().bucket(bucket).send().await {
        Ok(o) => Read::Got(
            o.policy_status()
                .and_then(|p| p.is_public())
                .unwrap_or(false),
        ),
        Err(e) => classify(
            e.code(),
            e.message().unwrap_or(""),
            &[
                NO_BUCKET,
                (
                    "NoSuchBucketPolicy",
                    "Bucket policy status: no bucket policy, so the policy makes nothing public.",
                ),
            ],
        ),
    };
    BucketSettings {
        bucket: bucket.to_string(),
        region: moved,
        encryption,
        policy,
        public,
    }
}

/// Reads a key's description and its key policy.
pub async fn read_key(s: &Session, key: &KeyRef) -> KeySettings {
    let region = key.region.clone().unwrap_or_else(|| s.region.clone());
    let kms = client!(aws_sdk_kms, s, region);
    const NOT_FOUND: (&str, &str) = (
        "NotFoundException",
        "There is no KMS key by that id or alias here (check it, its region and its account).",
    );
    let described = kms.describe_key().key_id(&key.id).send().await;
    let mut key_id = key.id.clone();
    let facts = match described {
        Ok(o) => match o.key_metadata() {
            Some(m) => {
                key_id = m.key_id().to_string();
                Read::Got(KeyFacts {
                    key_id: m.key_id().to_string(),
                    arn: m.arn().unwrap_or("").to_string(),
                    manager: m
                        .key_manager()
                        .map(|x| x.as_str().to_string())
                        .unwrap_or_default(),
                    state: m
                        .key_state()
                        .map(|x| x.as_str().to_string())
                        .unwrap_or_default(),
                    enabled: m.enabled(),
                    spec: m
                        .key_spec()
                        .map(|x| x.as_str().to_string())
                        .unwrap_or_default(),
                    usage: m
                        .key_usage()
                        .map(|x| x.as_str().to_string())
                        .unwrap_or_default(),
                    origin: m
                        .origin()
                        .map(|x| x.as_str().to_string())
                        .unwrap_or_default(),
                    multi_region: m.multi_region(),
                    description: m.description().unwrap_or("").to_string(),
                })
            }
            None => Read::Failed("AWS returned no key description".into()),
        },
        Err(e) => classify(e.code(), e.message().unwrap_or(""), &[NOT_FOUND]),
    };
    // A key policy is read by key id or ARN, never by alias: an alias the
    // description resolved is read by its key id.
    let policy = if key_id.contains("alias/") {
        Read::Failed("an alias can't be read for its key policy; give the key id".into())
    } else {
        match kms
            .get_key_policy()
            .key_id(&key_id)
            .policy_name("default")
            .send()
            .await
        {
            Ok(o) => match o.policy() {
                Some(p) => Read::Got(p.to_string()),
                None => Read::Absent("Key policy: none returned.".into()),
            },
            Err(e) => classify(e.code(), e.message().unwrap_or(""), &[NOT_FOUND]),
        }
    };
    KeySettings {
        key: key.id.clone(),
        facts,
        policy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_is_named_by_name_url_or_arn() {
        assert_eq!(check_bucket("data-in").unwrap(), "data-in");
        assert_eq!(check_bucket("s3://data-in/raw/2026/").unwrap(), "data-in");
        assert_eq!(check_bucket("arn:aws:s3:::data-in/*").unwrap(), "data-in");
        for bad in ["", "ab", "a b", "-x-bucket", "bucket;rm", "s3://"] {
            assert!(check_bucket(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_key_arn_names_its_region_and_account() {
        let k = check_key(
            "arn:aws:kms:eu-west-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
        )
        .unwrap();
        assert_eq!(k.region.as_deref(), Some("eu-west-1"));
        assert_eq!(k.account.as_deref(), Some("111122223333"));
        let a = check_key("alias/data-copy").unwrap();
        assert_eq!((a.region, a.account), (None, None));
        assert!(check_key("arn:aws:s3:::bucket").is_err());
        assert!(check_key("key id with spaces").is_err());
        assert!(check_key("arn:aws:kms:eu-west-1:1:key/x").is_err());
    }

    #[test]
    fn a_refusal_names_the_missing_permission() {
        let missing = denied_line(BUCKET_ENCRYPTION, "bucket `data-in`", Some(false));
        assert!(missing.contains("missing `s3:GetEncryptionConfiguration`"));
        assert!(missing.contains("Update"));
        let elsewhere = denied_line(KEY_POLICY, "KMS key `k`", Some(true));
        assert!(elsewhere.contains("`kms:GetKeyPolicy`"));
        assert!(elsewhere.contains("the role allows it"));
        let unknown = denied_line(BUCKET_POLICY, "bucket `b`", None);
        assert!(unknown.contains("missing `s3:GetBucketPolicy`"));
        for l in [missing, elsewhere, unknown] {
            assert!(!l.contains('\u{2192}'), "{l}");
        }
    }

    #[test]
    fn a_bucket_reads_as_evidence_with_each_refusal_named() {
        let b = BucketSettings {
            bucket: "data-in".into(),
            region: Some("eu-west-1".into()),
            encryption: Read::Got(vec![EncryptionRule {
                algorithm: "aws:kms".into(),
                kms_key: Some("arn:aws:kms:eu-west-1:111122223333:key/abc".into()),
                bucket_key: Some(true),
            }]),
            policy: Read::Denied,
            public: Read::Got(false),
        };
        let has = |a: &str| Some(a != BUCKET_POLICY);
        let t = format_bucket(&b, &has);
        assert!(t.contains("S3 bucket `data-in` (in eu-west-1)"));
        assert!(t.contains("SSE-KMS with KMS key `arn:aws:kms:eu-west-1:111122223333:key/abc`"));
        assert!(t.contains("S3 Bucket Key on"));
        assert!(t.contains("not public"));
        assert!(t.contains("missing `s3:GetBucketPolicy`"), "{t}");
        assert_eq!(bucket_denied(&b), vec![BUCKET_POLICY]);

        let sse_s3 = BucketSettings {
            encryption: Read::Got(vec![EncryptionRule {
                algorithm: "AES256".into(),
                kms_key: None,
                bucket_key: None,
            }]),
            policy: Read::Absent("Bucket policy: none.".into()),
            ..b
        };
        let t = format_bucket(&sse_s3, &|_| None);
        assert!(t.contains("SSE-S3 (AES256): keys managed by S3, not a KMS key"));
        assert!(t.contains("Bucket policy: none."));
        assert!(bucket_denied(&sse_s3).is_empty());
    }

    #[test]
    fn a_key_says_who_manages_it() {
        let k = KeySettings {
            key: "alias/data-copy".into(),
            facts: Read::Got(KeyFacts {
                key_id: "abc".into(),
                arn: "arn:aws:kms:eu-west-1:111122223333:key/abc".into(),
                manager: "CUSTOMER".into(),
                state: "Enabled".into(),
                enabled: true,
                spec: "SYMMETRIC_DEFAULT".into(),
                usage: "ENCRYPT_DECRYPT".into(),
                origin: "AWS_KMS".into(),
                multi_region: Some(false),
                description: "copy job".into(),
            }),
            policy: Read::Got(r#"{"Version":"2012-10-17","Statement":[]}"#.into()),
        };
        let t = format_key(&k, &|_| None);
        assert!(t.contains("customer managed; state Enabled, enabled"));
        assert!(t.contains("\"Version\": \"2012-10-17\""));
        let denied = KeySettings {
            facts: Read::Denied,
            policy: Read::Denied,
            ..k
        };
        assert_eq!(key_denied(&denied), vec![KEY_DESCRIBE, KEY_POLICY]);
        let t = format_key(&denied, &|_| Some(false));
        assert!(t.contains("missing `kms:DescribeKey`"));
        assert!(t.contains("missing `kms:GetKeyPolicy`"));
    }

    #[test]
    fn errors_are_read_as_refused_absent_or_failed() {
        let absent = [("NoSuchBucketPolicy", "Bucket policy: none.")];
        assert_eq!(
            classify::<String>(Some("AccessDenied"), "", &absent),
            Read::Denied
        );
        assert_eq!(
            classify::<String>(Some("NoSuchBucketPolicy"), "", &absent),
            Read::Absent("Bucket policy: none.".into())
        );
        assert!(matches!(
            classify::<String>(Some("SlowDown"), "too fast", &absent),
            Read::Failed(_)
        ));
    }

    #[test]
    fn every_action_is_a_read_of_settings() {
        for a in ACTIONS {
            let verb = a.split(':').nth(1).unwrap();
            assert!(
                verb.starts_with("Get") || verb.starts_with("Describe"),
                "{a}"
            );
        }
        // No object reads, no encrypt or decrypt.
        for a in ACTIONS {
            let verb = a.split(':').nth(1).unwrap();
            assert!(
                !verb.contains("Object")
                    && !verb.starts_with("Encrypt")
                    && !verb.starts_with("Decrypt"),
                "{a}"
            );
        }
    }
}
