//! Knowledge bases connected in place: an Amazon Bedrock knowledge base in
//! the team's own AWS account, searched live with `bedrock-agent-runtime`
//! Retrieve. Nothing is copied: Bedrock embeds the question and ranks its
//! own passages, and [`super::search::search`] merges that ranked list with
//! the local ones.
//!
//! Who calls Retrieve:
//!
//! - **Hosted**: a 15 minute session on the team's `CoderHelmLogReader` role
//!   in the knowledge base's account, taken on with the team's private ID
//!   ([`crate::live::aws::assume`]). The role's `ReadKnowledgeBases`
//!   statement (both log reader templates) names the knowledge bases it may
//!   search. A Bedrock resource policy naming Coderhelm's shared role is
//!   never used: any team that learned the ARN could search through it.
//! - **Self-hosted**, a knowledge base in the install's own account: the
//!   install's own role ([`crate::live::aws::own_session`]), whose
//!   permissions arrive with each version update. Its permissions boundary
//!   must allow `bedrock:Retrieve`, which comes with the access stack an AWS
//!   admin applies; until then AWS says the boundary refused, and that is
//!   said plainly ([`refusal`]).
//!
//! Search is HYBRID (vectors and keywords) where the vector store supports
//! it, with no reranking. The region is always the ARN's.

use serde::Serialize;
use serde_json::Value;

use super::search::{Hit, Retriever};
use super::{Access, Backend, Base, Provider, Remote};
use crate::live::aws::{self, Session};
use crate::live::aws_account::{self, AwsAccount};

// ── The ARN ──────────────────────────────────────────────────────────────────

/// A Bedrock knowledge base's ARN, read apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KbArn {
    pub region: String,
    pub account: String,
    /// The ten character id.
    pub id: String,
}

impl KbArn {
    pub fn arn(&self) -> String {
        format!(
            "arn:aws:bedrock:{}:{}:knowledge-base/{}",
            self.region, self.account, self.id
        )
    }
}

fn is_region(r: &str) -> bool {
    let parts: Vec<&str> = r.split('-').collect();
    parts.len() >= 3
        && parts.iter().all(|p| {
            !p.is_empty()
                && p.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        })
        && parts
            .last()
            .is_some_and(|p| p.chars().all(|c| c.is_ascii_digit()))
}

/// `arn:aws:bedrock:<region>:<account>:knowledge-base/<id>`, or what is
/// wrong with it in plain words.
pub fn parse_kb_arn(raw: &str) -> Result<KbArn, String> {
    let raw = raw.trim();
    let fix = "Paste the knowledge base's ARN from the Bedrock console. It looks like arn:aws:bedrock:us-east-1:111122223333:knowledge-base/ABCDE12345.";
    let parts: Vec<&str> = raw.splitn(6, ':').collect();
    let [p, partition, svc, region, account, resource] = parts.as_slice() else {
        return Err(format!("That isn't a knowledge base ARN. {fix}"));
    };
    let id = resource.strip_prefix("knowledge-base/").unwrap_or("");
    let ok = *p == "arn"
        && *partition == "aws"
        && *svc == "bedrock"
        && is_region(region)
        && account.len() == 12
        && account.chars().all(|c| c.is_ascii_digit())
        && id.len() == 10
        && id
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
    if !ok {
        return Err(format!("That isn't a knowledge base ARN. {fix}"));
    }
    Ok(KbArn {
        region: region.to_string(),
        account: account.to_string(),
        id: id.to_string(),
    })
}

// ── Failures, in plain words ─────────────────────────────────────────────────

/// Why a connected knowledge base couldn't be searched, and what to do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Refusal {
    /// `access`, `boundary`, `resource_policy`, `scp`, `not_found`,
    /// `invalid`, `busy`, `store`, `account`, `reach` or `other`.
    pub code: &'static str,
    pub message: String,
    pub fix: String,
}

impl Refusal {
    fn new(code: &'static str, message: impl Into<String>, fix: impl Into<String>) -> Refusal {
        Refusal {
            code,
            message: message.into(),
            fix: fix.into(),
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.message, self.fix)
    }
}

/// The message of the plain "the role may not" refusal.
pub const NOT_ALLOWED: &str =
    "The role in your account does not allow Retrieve on this knowledge base.";

/// Bedrock's error `code` and `message` for a Retrieve, in plain words.
pub fn refusal(code: &str, message: &str) -> Refusal {
    let lower = message.to_ascii_lowercase();
    let detail = crate::live::error_detail(code, message);
    match code {
        "AccessDeniedException" | "AccessDenied" | "UnauthorizedException" => {
            if crate::live::boundary_refusal(message) {
                Refusal::new(
                    "boundary",
                    "This install's permission limit doesn't allow searching Bedrock knowledge bases yet: the release that searches them needs a permission change an AWS admin hasn't applied.",
                    crate::live::PENDING_PERMISSION_FIX,
                )
            } else if lower.contains("service control policy") {
                Refusal::new(
                    "scp",
                    "An organization policy (SCP) blocks Retrieve on this knowledge base.",
                    "Ask your AWS organization's admin to allow bedrock:Retrieve for this role, then try again.",
                )
            } else if lower.contains("resource-based policy") || lower.contains("resource policy") {
                Refusal::new(
                    "resource_policy",
                    "The knowledge base's resource policy doesn't allow the role Coderhelm uses.",
                    "Add a statement to the knowledge base's resource policy that allows bedrock:Retrieve and bedrock:GetDocumentContent for that role on this knowledge base's ARN, then try again.",
                )
            } else {
                Refusal::new(
                    "access",
                    NOT_ALLOWED,
                    "Give the role bedrock:Retrieve and bedrock:GetDocumentContent on this knowledge base (the CoderHelmLogReader update does it), then try again.",
                )
            }
        }
        "ResourceNotFoundException" => Refusal::new(
            "not_found",
            "Bedrock has no knowledge base with that ARN.",
            "Check the ARN: copy it from the knowledge base's page in the Bedrock console. Its region and account are part of it.",
        ),
        "ValidationException" => Refusal::new(
            "invalid",
            format!("Bedrock refused the search ({detail})."),
            "Check that the knowledge base's status is Available and its data source has synced at least once.",
        ),
        "ThrottlingException" | "ServiceQuotaExceededException" => Refusal::new(
            "busy",
            "Bedrock is limiting searches on this knowledge base right now.",
            "Try again in a minute.",
        ),
        "DependencyFailedException" | "BadGatewayException" => Refusal::new(
            "store",
            format!("The vector store behind the knowledge base didn't answer Bedrock ({detail})."),
            "Check the vector store behind the knowledge base in your AWS account, then try again.",
        ),
        "" => Refusal::new(
            "reach",
            format!("Coderhelm couldn't reach Bedrock ({detail})."),
            "Try again. An install in a private network needs a VPC endpoint for bedrock-agent-runtime in the knowledge base's region.",
        ),
        _ => Refusal::new(
            "other",
            format!("Bedrock couldn't search the knowledge base ({detail})."),
            "Try again. If it keeps failing, check the knowledge base in the Bedrock console.",
        ),
    }
}

// ── Who searches it ──────────────────────────────────────────────────────────

/// How a knowledge base at `arn` is reached: with the install's own role
/// when it is in the self-hosted install's own account, otherwise with the
/// team's connected log reader role in that account.
pub fn access_for(
    arn: &KbArn,
    own_role: Option<&str>,
    accounts: &[AwsAccount],
) -> Result<Access, Refusal> {
    let own_account = own_role
        .and_then(|r| r.split(':').nth(4))
        .filter(|a| a.len() == 12);
    if own_account == Some(arn.account.as_str()) {
        return Ok(Access::Install);
    }
    if accounts.iter().any(|a| a.account == arn.account) {
        return Ok(Access::Account {
            account: arn.account.clone(),
        });
    }
    Err(Refusal::new(
        "account",
        format!(
            "AWS account {} isn't connected to Coderhelm.",
            arn.account
        ),
        "Connect it first on Settings, Integrations, AWS (it creates the read-only CoderHelmLogReader role), then paste the ARN again.",
    ))
}

/// A connected Bedrock knowledge base for `arn`, reached with `access`.
pub fn bedrock_remote(arn: &KbArn, access: Access, hybrid: bool) -> Remote {
    Remote {
        provider: Provider::BedrockKb,
        target: arn.arn(),
        region: arn.region.clone(),
        access,
        hybrid,
        per_person: false,
        search_tool: String::new(),
        fetch_tool: String::new(),
        name: String::new(),
    }
}

/// Takes on the identity `remote` is searched with, for `team_id`. The
/// team's private ID comes from Coderhelm's own record of the account.
pub async fn session(
    sts: &aws_sdk_sts::Client,
    dynamo: &aws_sdk_dynamodb::Client,
    accounts_table: &str,
    team_id: &str,
    remote: &Remote,
    actor: &str,
) -> Result<Session, Refusal> {
    match &remote.access {
        Access::Install => {
            if aws::install_read_role().is_none() {
                return Err(Refusal::new(
                    "account",
                    "This Coderhelm doesn't have an install role to search with.",
                    "Connect the knowledge base's AWS account on Settings, Integrations, AWS, then connect the knowledge base again.",
                ));
            }
            aws::own_session(&remote.region).ok_or_else(|| {
                Refusal::new(
                    "reach",
                    "This install's own AWS credentials aren't available here.",
                    "Try again in a minute.",
                )
            })
        }
        Access::Connection { .. } => Err(Refusal::new(
            "invalid",
            "This knowledge base is reached through a search tool, not AWS.",
            "Check it from its own page.",
        )),
        Access::Account { account } => {
            let accounts = if accounts_table.is_empty() {
                Vec::new()
            } else {
                aws_account::list(dynamo, accounts_table, team_id)
                    .await
                    .map_err(|e| {
                        tracing::warn!(error = %e, "Could not read AWS accounts");
                        Refusal::new(
                            "reach",
                            "Coderhelm couldn't read your connected AWS accounts.",
                            "Try again in a minute.",
                        )
                    })?
            };
            let Some(a) = accounts.iter().find(|a| &a.account == account) else {
                return Err(Refusal::new(
                    "account",
                    format!("AWS account {account} isn't connected to Coderhelm any more."),
                    "Connect it again on Settings, Integrations, AWS.",
                ));
            };
            let target = aws::AwsTarget {
                region: remote.region.clone(),
                ..a.target()
            };
            aws::assume(sts, &target, actor)
                .await
                .map_err(|e| Refusal::new("access", e.message, e.fix))
        }
    }
}

// ── Retrieve ─────────────────────────────────────────────────────────────────

/// One passage as Bedrock returned it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Passage {
    pub text: String,
    /// `S3`, `WEB`, `CONFLUENCE`, `SHAREPOINT`, `SALESFORCE`, `CUSTOM`, ...
    pub location_type: String,
    /// The location's URI, URL or id.
    pub location: String,
    pub metadata: serde_json::Map<String, Value>,
    pub score: f64,
}

fn runtime_client(s: &Session, region: &str) -> aws_sdk_bedrockagentruntime::Client {
    use aws_sdk_bedrockagentruntime::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    aws_sdk_bedrockagentruntime::Client::from_conf(
        aws_sdk_bedrockagentruntime::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .credentials_provider(creds)
            .build(),
    )
}

fn document_json(d: &aws_smithy_types::Document) -> Value {
    use aws_smithy_types::{Document, Number};
    match d {
        Document::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), document_json(v)))
                .collect(),
        ),
        Document::Array(a) => Value::Array(a.iter().map(document_json).collect()),
        Document::Number(Number::PosInt(n)) => Value::from(*n),
        Document::Number(Number::NegInt(n)) => Value::from(*n),
        Document::Number(Number::Float(f)) => Value::from(*f),
        Document::String(s) => Value::String(s.clone()),
        Document::Bool(b) => Value::Bool(*b),
        Document::Null => Value::Null,
    }
}

fn passage_of(r: &aws_sdk_bedrockagentruntime::types::KnowledgeBaseRetrievalResult) -> Passage {
    let (location_type, location) = r
        .location()
        .map(|l| {
            let at = l
                .s3_location()
                .and_then(|x| x.uri())
                .or_else(|| l.web_location().and_then(|x| x.url()))
                .or_else(|| l.confluence_location().and_then(|x| x.url()))
                .or_else(|| l.share_point_location().and_then(|x| x.url()))
                .or_else(|| l.salesforce_location().and_then(|x| x.url()))
                .or_else(|| l.kendra_document_location().and_then(|x| x.uri()))
                .or_else(|| l.custom_document_location().and_then(|x| x.id()))
                .unwrap_or("");
            (l.r#type().as_str().to_string(), at.to_string())
        })
        .unwrap_or_default();
    Passage {
        text: r
            .content()
            .map(|c| c.text().to_string())
            .unwrap_or_default(),
        location_type,
        location,
        metadata: r
            .metadata()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), document_json(v)))
                    .collect()
            })
            .unwrap_or_default(),
        score: r.score().unwrap_or(0.0),
    }
}

/// One Retrieve: up to `k` passages, HYBRID when `hybrid`, no reranking.
pub async fn retrieve_with(
    s: &Session,
    remote: &Remote,
    query: &str,
    k: usize,
    hybrid: bool,
) -> Result<Vec<Passage>, Refusal> {
    use aws_sdk_bedrockagentruntime::error::ProvideErrorMetadata;
    use aws_sdk_bedrockagentruntime::types::{
        KnowledgeBaseQuery, KnowledgeBaseRetrievalConfiguration,
        KnowledgeBaseVectorSearchConfiguration, SearchType,
    };
    let mut search =
        KnowledgeBaseVectorSearchConfiguration::builder().number_of_results(k.clamp(1, 100) as i32);
    if hybrid {
        search = search.override_search_type(SearchType::Hybrid);
    }
    let out = runtime_client(s, &remote.region)
        .retrieve()
        .knowledge_base_id(&remote.target)
        .retrieval_query(
            KnowledgeBaseQuery::builder()
                .text(crate::truncate_str(query, 1_000))
                .build(),
        )
        .retrieval_configuration(
            KnowledgeBaseRetrievalConfiguration::builder()
                .vector_search_configuration(search.build())
                .build(),
        )
        .send()
        .await
        .map_err(|e| match e.as_service_error() {
            Some(se) => refusal(se.code().unwrap_or("Unknown"), se.message().unwrap_or("")),
            None => refusal(
                "",
                &aws_sdk_bedrockagentruntime::error::DisplayErrorContext(&e).to_string(),
            ),
        })?;
    Ok(out.retrieval_results().iter().map(passage_of).collect())
}

/// Bedrock refused HYBRID because the vector store can't do it (S3
/// Vectors, Pinecone and others search by vector only).
fn hybrid_unsupported(r: &Refusal) -> bool {
    let m = r.message.to_ascii_lowercase();
    r.code == "invalid"
        && (m.contains("hybrid") || m.contains("search type") || m.contains("overridesearchtype"))
}

/// The test search run when a knowledge base is connected: `Ok(hybrid)`
/// says whether HYBRID works there (when the store refuses it, plain vector
/// search is tried and used instead).
pub async fn test_retrieve(s: &Session, remote: &Remote) -> Result<bool, Refusal> {
    let probe = "Coderhelm connection check";
    match retrieve_with(s, remote, probe, 1, true).await {
        Ok(_) => Ok(true),
        Err(r) if hybrid_unsupported(&r) => retrieve_with(s, remote, probe, 1, false)
            .await
            .map(|_| false),
        Err(r) => Err(r),
    }
}

// ── Passages as hits ─────────────────────────────────────────────────────────

fn meta_str(m: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|k| match m.get(*k)? {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    })
}

const DOC_ID_KEYS: &[&str] = &[
    "x-amz-bedrock-kb-document-id",
    "documentId",
    "document_id",
    "x-amz-bedrock-kb-source-uri",
];
const TITLE_KEYS: &[&str] = &[
    "x-amz-bedrock-kb-title",
    "title",
    "Title",
    "document_title",
    "_document_title",
];
const DATE_KEYS: &[&str] = &[
    "x-amz-bedrock-kb-updated-at",
    "updated_at",
    "updatedAt",
    "last_modified",
    "lastModified",
    "_last_updated_at",
    "modified",
    "date",
];

/// A metadata date as `YYYY-MM-DD` ([`as_of`]), for the other retrievers.
pub fn date_of(raw: &str) -> String {
    as_of(raw)
}

/// A metadata date as `YYYY-MM-DD`: RFC 3339, a plain date, or epoch
/// seconds or milliseconds. Empty when there is none (no recency nudge).
fn as_of(raw: &str) -> String {
    let raw = raw.trim();
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(raw) {
        return t.format("%Y-%m-%d").to_string();
    }
    if let Some(day) = raw.get(..10) {
        if chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d").is_ok() {
            return day.to_string();
        }
    }
    if let Ok(n) = raw.parse::<i64>() {
        let secs = if n > 100_000_000_000 { n / 1000 } else { n };
        if let Some(t) = chrono::DateTime::from_timestamp(secs, 0) {
            return t.format("%Y-%m-%d").to_string();
        }
    }
    String::new()
}

/// `s3://bucket/key` as (bucket, key).
fn s3_parts(uri: &str) -> Option<(&str, &str)> {
    let rest = uri.strip_prefix("s3://")?;
    let (bucket, key) = rest.split_once('/')?;
    (!bucket.is_empty() && !key.is_empty()).then_some((bucket, key))
}

/// Bedrock's passages as hits of `base`, best first. Each passage's document
/// is its document id (else its source address), its place the title or S3
/// object key, its date any updated date in its metadata, and its link the
/// original page (an S3 object links to the S3 console, which opens only
/// for people who have access there).
pub fn to_hits(base: &Base, remote: &Remote, passages: &[Passage]) -> Vec<Hit> {
    let mut out = Vec::new();
    for (i, p) in passages.iter().enumerate() {
        if p.text.trim().is_empty() {
            continue;
        }
        let source = if p.location.is_empty() {
            meta_str(&p.metadata, &["x-amz-bedrock-kb-source-uri"]).unwrap_or_default()
        } else {
            p.location.clone()
        };
        let document = meta_str(&p.metadata, DOC_ID_KEYS)
            .or_else(|| (!source.is_empty()).then(|| source.clone()))
            .or_else(|| meta_str(&p.metadata, &["x-amz-bedrock-kb-chunk-id"]))
            .unwrap_or_else(|| format!("passage-{i}"));
        let title = meta_str(&p.metadata, TITLE_KEYS);
        let (loc, url) = match s3_parts(&source) {
            Some((bucket, key)) => (
                key.to_string(),
                format!(
                    "https://s3.console.aws.amazon.com/s3/object/{bucket}?region={}&prefix={}",
                    remote.region,
                    urlencoding::encode(key)
                ),
            ),
            None => (
                title.clone().unwrap_or_else(|| source.clone()),
                super::answer::original_url(&source).unwrap_or_default(),
            ),
        };
        let file_name = title.unwrap_or_else(|| {
            loc.rsplit('/')
                .find(|s| !s.is_empty())
                .unwrap_or(&document)
                .to_string()
        });
        out.push(Hit {
            kb_id: base.id.clone(),
            kb_name: base.name.clone(),
            file_id: crate::truncate_str(&document, 512).to_string(),
            file_name: crate::truncate_str(&file_name, 200).to_string(),
            loc: crate::truncate_str(&loc, 300).to_string(),
            as_of: meta_str(&p.metadata, DATE_KEYS)
                .map(|d| as_of(&d))
                .unwrap_or_default(),
            text: p.text.clone(),
            score: p.score as f32,
            authoritative: base.authoritative,
            ord: i as u32,
            url,
            source_kind: super::SourceKind::BedrockKb.key().into(),
        });
    }
    out
}

// ── The retriever ────────────────────────────────────────────────────────────

/// Searches connected Bedrock knowledge bases, as the team. `actor` names
/// the session in the team's AWS records (`brain`, `coder`).
pub struct Bedrock<'a> {
    pub sts: &'a aws_sdk_sts::Client,
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    /// The table of connected AWS accounts (`AWS_CONN#`).
    pub accounts_table: &'a str,
    pub actor: &'a str,
}

#[async_trait::async_trait]
impl Retriever for Bedrock<'_> {
    async fn retrieve(
        &self,
        team_id: &str,
        base: &Base,
        query: &str,
        k: usize,
    ) -> Result<Vec<Hit>, String> {
        let Backend::Remote(remote) = &base.backend else {
            return Ok(Vec::new());
        };
        if remote.provider != Provider::BedrockKb {
            return Err("This knowledge base isn't a Bedrock knowledge base.".into());
        }
        let s = session(
            self.sts,
            self.dynamo,
            self.accounts_table,
            team_id,
            remote,
            self.actor,
        )
        .await
        .map_err(|r| r.message)?;
        let passages = retrieve_with(&s, remote, query, k, remote.hybrid)
            .await
            .map_err(|r| r.message)?;
        Ok(to_hits(base, remote, &passages))
    }
}

/// Every kind of connected base, each answered by its own retriever: what
/// [`super::search::search`] is given in production.
pub struct Connected<'a> {
    pub bedrock: Bedrock<'a>,
    pub mcp: super::remote_mcp::McpSearch<'a>,
}

#[async_trait::async_trait]
impl Retriever for Connected<'_> {
    async fn retrieve(
        &self,
        team_id: &str,
        base: &Base,
        query: &str,
        k: usize,
    ) -> Result<Vec<Hit>, String> {
        match base.remote().map(|r| r.provider) {
            Some(Provider::BedrockKb) => self.bedrock.retrieve(team_id, base, query, k).await,
            Some(Provider::Mcp) => self.mcp.retrieve(team_id, base, query, k).await,
            None => Ok(Vec::new()),
        }
    }
}

// ── Listing (self-hosted) ────────────────────────────────────────────────────

/// A knowledge base found in the install's own account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Listed {
    pub arn: String,
    pub id: String,
    pub name: String,
    pub description: String,
    /// `ACTIVE`, `CREATING`, `FAILED`, ...
    pub status: String,
    pub updated_at: String,
}

/// The knowledge bases in the install's own account in `region`, with the
/// install's own role (`bedrock:ListKnowledgeBases`).
pub async fn list_install_bases(region: &str) -> Result<Vec<Listed>, Refusal> {
    use aws_sdk_bedrockagent::config::{BehaviorVersion, Credentials, Region};
    use aws_sdk_bedrockagent::error::ProvideErrorMetadata;
    let own = aws::install_read_role().ok_or_else(|| {
        Refusal::new(
            "account",
            "Only a self-hosted install lists its own knowledge bases.",
            "Paste the knowledge base's ARN instead.",
        )
    })?;
    let account = own.split(':').nth(4).unwrap_or("").to_string();
    let s = aws::own_session(region).ok_or_else(|| {
        Refusal::new(
            "reach",
            "This install's own AWS credentials aren't available here.",
            "Paste the knowledge base's ARN instead.",
        )
    })?;
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-install-role",
    );
    let client = aws_sdk_bedrockagent::Client::from_conf(
        aws_sdk_bedrockagent::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .credentials_provider(creds)
            .build(),
    );
    let mut out = Vec::new();
    let mut next: Option<String> = None;
    for _ in 0..10 {
        let page = client
            .list_knowledge_bases()
            .max_results(100)
            .set_next_token(next.take())
            .send()
            .await
            .map_err(|e| match e.as_service_error() {
                Some(se) => {
                    let r = refusal(se.code().unwrap_or("Unknown"), se.message().unwrap_or(""));
                    if r.code == "access" {
                        Refusal::new(
                            "access",
                            "This install's role can't list knowledge bases yet.",
                            "It arrives with this install's next version update. Paste the knowledge base's ARN meanwhile.",
                        )
                    } else {
                        r
                    }
                }
                None => refusal(
                    "",
                    &aws_sdk_bedrockagent::error::DisplayErrorContext(&e).to_string(),
                ),
            })?;
        for kb in page.knowledge_base_summaries() {
            let arn = KbArn {
                region: region.to_string(),
                account: account.clone(),
                id: kb.knowledge_base_id().to_string(),
            };
            out.push(Listed {
                arn: arn.arn(),
                id: arn.id,
                name: kb.name().to_string(),
                description: kb.description().unwrap_or("").to_string(),
                status: kb.status().as_str().to_string(),
                updated_at: kb.updated_at().to_string(),
            });
        }
        match page.next_token() {
            Some(t) if !t.is_empty() => next = Some(t.to_string()),
            _ => break,
        }
    }
    out.sort_by_key(|a| a.name.to_lowercase());
    Ok(out)
}

/// The ARNs of a team's knowledge bases connected in `account`, for the
/// log reader update that lets the role search them.
pub fn arns_in(bases: &[Base], account: &str) -> Vec<String> {
    let mut out: Vec<String> = bases
        .iter()
        .filter_map(|b| b.remote())
        .filter(|r| matches!(&r.access, Access::Account { account: a } if a == account))
        .map(|r| r.target.clone())
        .collect();
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
fn meta(pairs: &[(&str, Value)]) -> serde_json::Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ARN: &str = "arn:aws:bedrock:eu-west-1:111122223333:knowledge-base/ABCDE12345";

    fn base() -> (Base, Remote) {
        let remote = bedrock_remote(
            &parse_kb_arn(ARN).unwrap(),
            Access::Account {
                account: "111122223333".into(),
            },
            true,
        );
        (
            Base {
                id: "kb".into(),
                name: "Runbooks".into(),
                authoritative: true,
                backend: Backend::Remote(remote.clone()),
                ..Default::default()
            },
            remote,
        )
    }

    #[test]
    fn the_arn_is_read_apart_and_its_region_is_the_one_used() {
        let a = parse_kb_arn(&format!("  {ARN}\n")).unwrap();
        assert_eq!(
            (a.region.as_str(), a.account.as_str(), a.id.as_str()),
            ("eu-west-1", "111122223333", "ABCDE12345")
        );
        assert_eq!(a.arn(), ARN);
        let (_, r) = base();
        assert_eq!(r.region, "eu-west-1");
        for bad in [
            "",
            "ABCDE12345",
            "arn:aws:bedrock:eu-west-1:111122223333:agent/ABCDE12345",
            "arn:aws:bedrock:eu-west-1:1111:knowledge-base/ABCDE12345",
            "arn:aws:bedrock:eu-west-1:111122223333:knowledge-base/abcde12345",
            "arn:aws:bedrock:eu-west-1:111122223333:knowledge-base/ABCDE1234",
            "arn:aws:s3:eu-west-1:111122223333:knowledge-base/ABCDE12345",
            "arn:aws:bedrock:EU:111122223333:knowledge-base/ABCDE12345",
            "arn:aws:bedrock:eu-west-1:111122223333:knowledge-base/ABCDE12345/x",
        ] {
            let e = parse_kb_arn(bad).unwrap_err();
            assert!(
                e.starts_with("That isn't a knowledge base ARN."),
                "{bad}: {e}"
            );
        }
    }

    #[test]
    fn refusals_are_said_in_plain_words() {
        let r = refusal("AccessDeniedException", "User: arn:aws:sts::111122223333:assumed-role/CoderHelmLogReader/coderhelm-brain is not authorized to perform: bedrock:Retrieve on resource: arn:aws:bedrock:eu-west-1:111122223333:knowledge-base/ABCDE12345 because no identity-based policy allows the bedrock:Retrieve action");
        assert_eq!(r.code, "access");
        assert_eq!(
            r.message,
            "The role in your account does not allow Retrieve on this knowledge base."
        );
        // A self-hosted install whose admin hasn't applied the access stack.
        let r = refusal("AccessDeniedException", "User: arn:aws:sts::111122223333:assumed-role/coderhelm-prod-gateway/x is not authorized to perform: bedrock:Retrieve on resource: arn:aws:bedrock:us-east-1:111122223333:knowledge-base/ABCDE12345 because no permissions boundary allows the bedrock:Retrieve action");
        assert_eq!(r.code, "boundary");
        assert!(r
            .message
            .contains("permission change an AWS admin hasn't applied"));
        assert!(r.fix.contains("Settings, System"));
        // A MANAGED base in another account whose resource policy is missing.
        let r = refusal(
            "AccessDeniedException",
            "... because no resource-based policy allows the bedrock:Retrieve action",
        );
        assert_eq!(r.code, "resource_policy");
        assert!(r.fix.contains("bedrock:Retrieve"));
        let r = refusal(
            "AccessDeniedException",
            "... with an explicit deny in a service control policy",
        );
        assert_eq!(r.code, "scp");
        assert_eq!(
            refusal("ResourceNotFoundException", "kb not found").code,
            "not_found"
        );
        assert_eq!(refusal("ThrottlingException", "slow").code, "busy");
        assert_eq!(
            refusal("DependencyFailedException", "aoss down").code,
            "store"
        );
        // An account number or ARN in AWS's message never reaches the page.
        let r = refusal("ValidationException", "Knowledge base arn:aws:bedrock:eu-west-1:111122223333:knowledge-base/ABCDE12345 is not ready");
        assert!(!r.message.contains("111122223333"), "{}", r.message);
        let r = refusal("", "dispatch failure: timeout");
        assert_eq!(r.code, "reach");
        assert!(r.fix.contains("VPC endpoint"));
        for r in [refusal("AccessDeniedException", "x"), refusal("Weird", "y")] {
            assert!(!format!("{}{}", r.message, r.fix).contains('\u{2192}'));
        }
        assert!(hybrid_unsupported(&refusal(
            "ValidationException",
            "HYBRID search type is not supported for this vector store"
        )));
        assert!(!hybrid_unsupported(&refusal(
            "ValidationException",
            "query is too long"
        )));
    }

    #[test]
    fn the_install_account_uses_the_install_role_and_others_the_teams_role() {
        let arn = parse_kb_arn(ARN).unwrap();
        let own = "arn:aws:iam::111122223333:role/coderhelm-prod-gateway-1a2b";
        assert_eq!(access_for(&arn, Some(own), &[]).unwrap(), Access::Install);
        let connected = AwsAccount {
            account: "111122223333".into(),
            role_arn: "arn:aws:iam::111122223333:role/CoderHelmLogReader".into(),
            external_id: "coderhelm-x".into(),
            region: "us-east-1".into(),
            ..Default::default()
        };
        // Hosted (no install role): the team's connected role in that account.
        assert_eq!(
            access_for(&arn, None, std::slice::from_ref(&connected)).unwrap(),
            Access::Account {
                account: "111122223333".into()
            }
        );
        // An install in another account: the team's role there.
        assert_eq!(
            access_for(
                &arn,
                Some("arn:aws:iam::999988887777:role/x"),
                std::slice::from_ref(&connected)
            )
            .unwrap(),
            Access::Account {
                account: "111122223333".into()
            }
        );
        // An account nobody connected: said plainly.
        let r = access_for(&arn, None, &[]).unwrap_err();
        assert_eq!(r.code, "account");
        assert!(r.message.contains("111122223333 isn't connected"));
    }

    #[test]
    fn passages_become_hits_with_document_place_date_and_link() {
        let (b, r) = base();
        let ps = vec![
            Passage {
                text: "Restart the consumer before draining the queue.".into(),
                location_type: "S3".into(),
                location: "s3://ops-docs/runbooks/queues/drain.md".into(),
                metadata: meta(&[
                    (
                        "x-amz-bedrock-kb-source-uri",
                        json!("s3://ops-docs/runbooks/queues/drain.md"),
                    ),
                    ("x-amz-bedrock-kb-chunk-id", json!("c-1")),
                    ("updated_at", json!("2026-09-30T10:00:00Z")),
                ]),
                score: 0.83,
            },
            Passage {
                text: "On-call rotates every Monday.".into(),
                location_type: "CONFLUENCE".into(),
                location: "https://example.atlassian.net/wiki/spaces/OPS/pages/1".into(),
                metadata: meta(&[
                    ("x-amz-bedrock-kb-document-id", json!("doc-77")),
                    ("title", json!("On-call")),
                    ("lastModified", json!(1_790_000_000_000i64)),
                ]),
                score: 0.5,
            },
            Passage {
                text: "   ".into(),
                ..Default::default()
            },
            Passage {
                text: "A custom document.".into(),
                location_type: "CUSTOM".into(),
                location: "my-doc".into(),
                ..Default::default()
            },
        ];
        let hits = to_hits(&b, &r, &ps);
        assert_eq!(hits.len(), 3, "an empty passage is dropped");
        let s3 = &hits[0];
        assert_eq!(s3.file_id, "s3://ops-docs/runbooks/queues/drain.md");
        assert_eq!(s3.loc, "runbooks/queues/drain.md");
        assert_eq!(s3.file_name, "drain.md");
        assert_eq!(s3.as_of, "2026-09-30");
        assert_eq!(s3.url, "https://s3.console.aws.amazon.com/s3/object/ops-docs?region=eu-west-1&prefix=runbooks%2Fqueues%2Fdrain.md");
        assert_eq!(s3.source_kind, "bedrock_kb");
        assert_eq!(
            (s3.kb_id.as_str(), s3.kb_name.as_str(), s3.authoritative),
            ("kb", "Runbooks", true)
        );
        let wiki = &hits[1];
        assert_eq!(wiki.file_id, "doc-77");
        assert_eq!(wiki.file_name, "On-call");
        assert_eq!(wiki.loc, "On-call");
        assert_eq!(
            wiki.url,
            "https://example.atlassian.net/wiki/spaces/OPS/pages/1"
        );
        assert_eq!(wiki.as_of, "2026-09-21");
        let custom = &hits[2];
        assert_eq!(custom.file_id, "my-doc");
        assert_eq!(custom.url, "", "an id is not a link");
        assert_eq!(custom.as_of, "", "no date, no recency nudge");
    }

    #[test]
    fn the_update_names_only_this_accounts_connected_bases() {
        let (b, _) = base();
        let other = Base {
            backend: Backend::Remote(bedrock_remote(
                &parse_kb_arn("arn:aws:bedrock:us-east-1:444455556666:knowledge-base/ZZZZZ00000")
                    .unwrap(),
                Access::Account {
                    account: "444455556666".into(),
                },
                true,
            )),
            ..b.clone()
        };
        let install = Base {
            backend: Backend::Remote(bedrock_remote(
                &parse_kb_arn(ARN).unwrap(),
                Access::Install,
                true,
            )),
            ..b.clone()
        };
        let local = Base::default();
        assert_eq!(
            arns_in(&[b.clone(), b, other, install, local], "111122223333"),
            [ARN]
        );
    }
}
