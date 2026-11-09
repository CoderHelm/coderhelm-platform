//! Importing a Bedrock knowledge base: finding where its documents are, so
//! Coderhelm can read the originals and index them itself.
//!
//! Bedrock has no export of its passages or vectors (and a MANAGED base's
//! store can't be reached at all), so the faithful copy is the knowledge
//! base's own data source. An S3 data source becomes an S3 source here
//! ([`super::s3`]) on its bucket and inclusion prefixes; a Confluence or web
//! data source points at Coderhelm's own connector, which reads the
//! originals rather than Bedrock's passages; the rest are said plainly.
//!
//! The knowledge base's settings are read with `bedrock:GetKnowledgeBase`,
//! `bedrock:ListDataSources` and `bedrock:GetDataSource`, by the same role
//! that searches it ([`super::super::remote::session`]): on hosted, the
//! team's `CoderHelmLogReader` role (its `ReadKnowledgeBases` statement);
//! on a self-hosted install, its own role (the boundary's `bedrock:Get*`
//! and `bedrock:List*`). Nothing in Bedrock changes.

use serde::Serialize;

use super::super::remote::{refusal, KbArn, Refusal};
use super::s3;
use crate::live::aws::Session;

/// One data source of a knowledge base, and how it is imported.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DataSource {
    pub id: String,
    pub name: String,
    /// Bedrock's type: `S3`, `CONFLUENCE`, `SHAREPOINT`, `SALESFORCE`,
    /// `WEB`, `CUSTOM`, `REDSHIFT_METADATA`.
    #[serde(rename = "type")]
    pub kind: String,
    pub status: String,
    /// An S3 data source's bucket.
    pub bucket: String,
    /// The account that owns the bucket (the knowledge base's, unless the
    /// data source names another).
    pub bucket_account: String,
    pub prefixes: Vec<String>,
    /// It can be imported as an S3 source.
    pub importable: bool,
    /// The Coderhelm connector to use instead (`confluence`, `web`).
    pub use_instead: String,
    /// What to do, in plain words.
    pub note: String,
}

/// A knowledge base's import plan.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Plan {
    pub name: String,
    pub account: String,
    pub region: String,
    pub data_sources: Vec<DataSource>,
}

/// A data source as Bedrock describes it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Described {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub status: String,
    pub bucket_arn: String,
    pub bucket_owner: String,
    pub prefixes: Vec<String>,
}

/// What Bedrock answers about a knowledge base. The seam tests fake.
#[async_trait::async_trait]
pub trait Catalog: Send + Sync {
    /// The knowledge base's name.
    async fn name(&self, arn: &KbArn) -> Result<String, Refusal>;
    /// Every data source, described.
    async fn data_sources(&self, arn: &KbArn) -> Result<Vec<Described>, Refusal>;
}

/// How one data source is imported.
pub fn plan_one(d: &Described, kb_account: &str) -> DataSource {
    let mut out = DataSource {
        id: d.id.clone(),
        name: d.name.clone(),
        kind: d.kind.clone(),
        status: d.status.clone(),
        ..Default::default()
    };
    match d.kind.as_str() {
        "S3" => match s3::bucket_of(&d.bucket_arn) {
            Some(b) => {
                out.bucket = b;
                out.bucket_account = if d.bucket_owner.len() == 12 {
                    d.bucket_owner.clone()
                } else {
                    kb_account.to_string()
                };
                out.prefixes = d
                    .prefixes
                    .iter()
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect();
                out.importable = true;
                out.note = format!(
                    "Coderhelm reads the documents in bucket {} itself and indexes them; nothing in Bedrock changes.",
                    out.bucket
                );
            }
            None => {
                out.note = "Bedrock didn't say which bucket this data source reads.".into();
            }
        },
        "CONFLUENCE" => {
            out.use_instead = "confluence".into();
            out.note = "Connect Confluence in Coderhelm instead: it reads the original pages and keeps them in sync.".into();
        }
        "WEB" => {
            out.use_instead = "web".into();
            out.note =
                "Add these pages as a Website source instead: Coderhelm reads them itself.".into();
        }
        "SHAREPOINT" | "SALESFORCE" => {
            out.note = format!(
                "Coderhelm has no {} connector yet. Connect the knowledge base in place to search it live instead.",
                if d.kind == "SHAREPOINT" { "SharePoint" } else { "Salesforce" }
            );
        }
        _ => {
            out.note = "This kind of data source can't be imported. Connect the knowledge base in place to search it live instead.".into();
        }
    }
    out
}

/// Where a knowledge base's documents are, and how each data source is
/// imported.
pub async fn plan(catalog: &dyn Catalog, arn: &KbArn) -> Result<Plan, Refusal> {
    let name = catalog.name(arn).await?;
    let mut data_sources: Vec<DataSource> = catalog
        .data_sources(arn)
        .await?
        .iter()
        .map(|d| plan_one(d, &arn.account))
        .collect();
    data_sources.sort_by(|a, b| b.importable.cmp(&a.importable).then(a.name.cmp(&b.name)));
    Ok(Plan {
        name,
        account: arn.account.clone(),
        region: arn.region.clone(),
        data_sources,
    })
}

/// A Bedrock refusal for reading a knowledge base's settings.
fn settings_refusal(code: &str, message: &str) -> Refusal {
    let r = refusal(code, message);
    if r.code == "access" {
        Refusal {
            code: "access",
            message: "The role in your account can't read this knowledge base's settings.".into(),
            fix: "Give the role bedrock:GetKnowledgeBase, bedrock:ListDataSources and bedrock:GetDataSource on this knowledge base (the CoderHelmLogReader update does it), then try again.".into(),
        }
    } else {
        r
    }
}

/// The production [`Catalog`]: `bedrock-agent` with a session.
pub struct Bedrock {
    pub client: aws_sdk_bedrockagent::Client,
}

impl Bedrock {
    pub fn new(s: &Session, region: &str) -> Bedrock {
        use aws_sdk_bedrockagent::config::{BehaviorVersion, Credentials, Region};
        let creds = Credentials::new(
            &s.access_key_id,
            &s.secret_access_key,
            Some(s.session_token.clone()),
            None,
            "coderhelm-team-role",
        );
        Bedrock {
            client: aws_sdk_bedrockagent::Client::from_conf(
                aws_sdk_bedrockagent::Config::builder()
                    .behavior_version(BehaviorVersion::latest())
                    .region(Region::new(region.to_string()))
                    .credentials_provider(creds)
                    .build(),
            ),
        }
    }
}

fn sdk_refusal<E, R>(e: &aws_sdk_bedrockagent::error::SdkError<E, R>) -> Refusal
where
    E: aws_sdk_bedrockagent::error::ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    match e.as_service_error() {
        Some(se) => settings_refusal(se.code().unwrap_or("Unknown"), se.message().unwrap_or("")),
        None => settings_refusal(
            "",
            &aws_sdk_bedrockagent::error::DisplayErrorContext(e).to_string(),
        ),
    }
}

#[async_trait::async_trait]
impl Catalog for Bedrock {
    async fn name(&self, arn: &KbArn) -> Result<String, Refusal> {
        let out = self
            .client
            .get_knowledge_base()
            .knowledge_base_id(&arn.id)
            .send()
            .await
            .map_err(|e| sdk_refusal(&e))?;
        Ok(out
            .knowledge_base()
            .map(|k| k.name().to_string())
            .unwrap_or_default())
    }

    async fn data_sources(&self, arn: &KbArn) -> Result<Vec<Described>, Refusal> {
        let mut ids: Vec<String> = Vec::new();
        let mut next: Option<String> = None;
        for _ in 0..10 {
            let page = self
                .client
                .list_data_sources()
                .knowledge_base_id(&arn.id)
                .max_results(100)
                .set_next_token(next.take())
                .send()
                .await
                .map_err(|e| sdk_refusal(&e))?;
            ids.extend(
                page.data_source_summaries()
                    .iter()
                    .map(|d| d.data_source_id().to_string()),
            );
            match page.next_token() {
                Some(t) if !t.is_empty() => next = Some(t.to_string()),
                _ => break,
            }
        }
        let mut out = Vec::new();
        for id in ids.iter().take(50) {
            let d = self
                .client
                .get_data_source()
                .knowledge_base_id(&arn.id)
                .data_source_id(id)
                .send()
                .await
                .map_err(|e| sdk_refusal(&e))?;
            let Some(ds) = d.data_source() else { continue };
            let conf = ds.data_source_configuration();
            let s3c = conf.and_then(|c| c.s3_configuration());
            out.push(Described {
                id: ds.data_source_id().to_string(),
                name: ds.name().to_string(),
                kind: conf
                    .map(|c| c.r#type().as_str().to_string())
                    .unwrap_or_default(),
                status: ds.status().as_str().to_string(),
                bucket_arn: s3c.map(|c| c.bucket_arn().to_string()).unwrap_or_default(),
                bucket_owner: s3c
                    .and_then(|c| c.bucket_owner_account_id())
                    .unwrap_or("")
                    .to_string(),
                prefixes: s3c
                    .map(|c| c.inclusion_prefixes().to_vec())
                    .unwrap_or_default(),
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::remote::parse_kb_arn;

    struct Fake(Result<Vec<Described>, Refusal>);

    #[async_trait::async_trait]
    impl Catalog for Fake {
        async fn name(&self, _arn: &KbArn) -> Result<String, Refusal> {
            self.0
                .as_ref()
                .map(|_| "Runbooks".to_string())
                .map_err(Clone::clone)
        }
        async fn data_sources(&self, _arn: &KbArn) -> Result<Vec<Described>, Refusal> {
            self.0.clone()
        }
    }

    fn arn() -> KbArn {
        parse_kb_arn("arn:aws:bedrock:eu-west-1:111122223333:knowledge-base/ABCDE12345").unwrap()
    }

    #[tokio::test]
    async fn an_s3_data_source_becomes_an_s3_import_and_the_rest_are_said_plainly() {
        let c = Fake(Ok(vec![
            Described {
                id: "W1".into(),
                name: "Wiki".into(),
                kind: "CONFLUENCE".into(),
                ..Default::default()
            },
            Described {
                id: "D1".into(),
                name: "Docs bucket".into(),
                kind: "S3".into(),
                status: "AVAILABLE".into(),
                bucket_arn: "arn:aws:s3:::ops-docs".into(),
                prefixes: vec!["runbooks/".into(), " ".into()],
                ..Default::default()
            },
            Described {
                id: "D2".into(),
                name: "Shared bucket".into(),
                kind: "S3".into(),
                bucket_arn: "arn:aws:s3:::shared-docs".into(),
                bucket_owner: "444455556666".into(),
                ..Default::default()
            },
            Described {
                id: "S1".into(),
                name: "Intranet".into(),
                kind: "SHAREPOINT".into(),
                ..Default::default()
            },
        ]));
        let p = plan(&c, &arn()).await.unwrap();
        assert_eq!(p.name, "Runbooks");
        assert_eq!(
            (p.account.as_str(), p.region.as_str()),
            ("111122223333", "eu-west-1")
        );
        let names: Vec<&str> = p.data_sources.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(
            names,
            ["Docs bucket", "Shared bucket", "Intranet", "Wiki"],
            "importable first"
        );
        let docs = &p.data_sources[0];
        assert!(docs.importable);
        assert_eq!(docs.bucket, "ops-docs");
        assert_eq!(docs.bucket_account, "111122223333");
        assert_eq!(docs.prefixes, ["runbooks/"]);
        assert_eq!(
            p.data_sources[1].bucket_account, "444455556666",
            "a bucket in another account"
        );
        assert!(!p.data_sources[2].importable);
        assert!(p.data_sources[2].note.contains("SharePoint"));
        assert_eq!(p.data_sources[3].use_instead, "confluence");
        for d in &p.data_sources {
            assert!(!d.note.contains('\u{2192}'));
        }
    }

    #[tokio::test]
    async fn a_role_that_cant_read_the_settings_is_told_what_to_add() {
        let denied = settings_refusal("AccessDeniedException", "not authorized to perform: bedrock:GetKnowledgeBase because no identity-based policy allows it");
        assert_eq!(denied.code, "access");
        assert!(denied.fix.contains("bedrock:ListDataSources"));
        let e = plan(&Fake(Err(denied)), &arn()).await.unwrap_err();
        assert!(e.message.contains("settings"));
        assert_eq!(
            settings_refusal("ResourceNotFoundException", "").code,
            "not_found"
        );
    }
}
