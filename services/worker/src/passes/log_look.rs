//! The real lookups an incident diagnosis makes
//! ([`common::log_incident::Look`]): the team's AWS account through its
//! read-only log reader role (OpenSearch domains, CloudTrail's record of
//! changes, whether an error is still logging) and its code through the
//! GitHub App (code search, a file's recent commits). Every call only
//! lists, describes or reads. A refused OpenSearch listing is named by its
//! IAM action (the role's infrastructure switch grants it); CloudTrail is
//! read only where a team's role allows it.

use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use chrono::{Duration, Utc};
use common::live::aws::Session;
use common::log_findings::{self as lf, Finding};
use common::log_incident::{Change, CodeRef, Commit, Domain, Look, Lookup};
use common::repo_read::RepoHost;

macro_rules! client {
    ($krate:ident, $s:expr) => {{
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
                .region(Region::new(s.region.clone()))
                .credentials_provider(creds)
                .build(),
        )
    }};
}

/// Most code search hits kept per query.
const MAX_HITS: usize = 12;

fn refusal(e: &impl ProvideErrorMetadata, action: &str) -> Option<String> {
    common::log_repo::live::is_refusal(e.code()).then(|| action.to_string())
}

pub struct LiveLook<'a> {
    /// A session on the team's log reader role in the finding's account.
    pub session: Option<Session>,
    pub host: Option<&'a dyn RepoHost>,
    /// The team's enabled repos (`owner/name`).
    pub repos: Vec<String>,
}

#[async_trait::async_trait]
impl Look for LiveLook<'_> {
    async fn domains(&self) -> Lookup<Vec<Domain>> {
        let s = self.session.as_ref().ok_or(None)?;
        let os = client!(aws_sdk_opensearch, s);
        let names: Vec<String> = match os.list_domain_names().send().await {
            Ok(o) => o
                .domain_names()
                .iter()
                .filter_map(|d| d.domain_name().map(String::from))
                .collect(),
            Err(e) => return Err(refusal(&e, "es:ListDomainNames")),
        };
        let mut out = Vec::new();
        for chunk in names.chunks(5).take(6) {
            match os
                .describe_domains()
                .set_domain_names(Some(chunk.to_vec()))
                .send()
                .await
            {
                Ok(o) => {
                    for d in o.domain_status_list() {
                        let vpc_endpoint = d
                            .endpoints()
                            .and_then(|m| m.get("vpc").cloned())
                            .unwrap_or_default();
                        out.push(Domain {
                            name: d.domain_name().to_string(),
                            endpoint: d
                                .endpoint()
                                .map(String::from)
                                .filter(|e| !e.is_empty())
                                .unwrap_or(vpc_endpoint),
                            vpc: d.vpc_options().is_some(),
                            deleted: d.deleted().unwrap_or(false),
                        });
                    }
                }
                Err(e) => return Err(refusal(&e, "es:DescribeDomains")),
            }
        }
        Ok(out)
    }

    async fn changes(&self, resource: &str) -> Lookup<Vec<Change>> {
        use aws_sdk_cloudtrail::types::{LookupAttribute, LookupAttributeKey};
        let s = self.session.as_ref().ok_or(None)?;
        let ct = client!(aws_sdk_cloudtrail, s);
        let attr = LookupAttribute::builder()
            .attribute_key(LookupAttributeKey::ResourceName)
            .attribute_value(resource)
            .build()
            .map_err(|_| None)?;
        let since = Utc::now() - Duration::days(7);
        let out = ct
            .lookup_events()
            .lookup_attributes(attr)
            .start_time(aws_smithy_types::DateTime::from_secs(since.timestamp()))
            .max_results(20)
            .send()
            .await
            // The standard read-only role doesn't grant CloudTrail: a refusal
            // is not named as a permission to add (the template doesn't
            // offer it); roles that allow it give the change's time.
            .map_err(|_| None)?;
        Ok(out
            .events()
            .iter()
            .filter(|e| {
                // Changes only: never the reads.
                e.read_only().is_none_or(|r| r != "true")
            })
            .map(|e| Change {
                at: e
                    .event_time()
                    .and_then(|t| chrono::DateTime::from_timestamp(t.secs(), 0))
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_default(),
                action: e.event_name().unwrap_or("").to_string(),
                resource: resource.to_string(),
                by: e.username().unwrap_or("").to_string(),
            })
            .collect())
    }

    async fn code(&self, query: &str) -> Vec<CodeRef> {
        let Some(host) = self.host else {
            return vec![];
        };
        let q = query.trim().replace('"', "");
        if q.len() < 3 || self.repos.is_empty() {
            return vec![];
        }
        let mut out = Vec::new();
        for chunk in self.repos.chunks(10).take(3) {
            match host.search(chunk, &format!("\"{q}\"")).await {
                Ok(hits) => out.extend(hits.into_iter().map(|h| CodeRef {
                    repo: h.repo,
                    path: h.path,
                    fragment: h.fragments.into_iter().next().unwrap_or_default(),
                })),
                Err(e) => {
                    tracing::warn!(query = %q, error = %e, "Incident diagnosis: code search failed");
                    break;
                }
            }
        }
        out.truncate(MAX_HITS);
        out
    }

    async fn history(&self, repo: &str, path: &str) -> Vec<Commit> {
        let Some(host) = self.host else {
            return vec![];
        };
        host.commits(repo, path)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(sha, message, at)| Commit {
                repo: repo.to_string(),
                path: path.to_string(),
                sha,
                message,
                at,
            })
            .collect()
    }

    async fn lines_since(&self, f: &Finding, since: &str) -> Option<u64> {
        let s = self.session.as_ref()?;
        if !lf::valid_log_group(&f.source_log_group) {
            return None;
        }
        let lit = lf::pattern_literal(&f.error_pattern)?;
        let from = chrono::DateTime::parse_from_rfc3339(since)
            .ok()?
            .timestamp_millis()
            + 1;
        let logs = common::live::aws::logs_client(s);
        // One matching line is enough to say it's still happening; an empty
        // page with more to read means it wasn't all searched yet.
        let mut token: Option<String> = None;
        for _ in 0..10 {
            let out = logs
                .filter_log_events()
                .log_group_name(&f.source_log_group)
                .filter_pattern(format!("\"{lit}\""))
                .start_time(from)
                .limit(1)
                .set_next_token(token.take())
                .send()
                .await
                .ok()?;
            if !out.events().is_empty() {
                return Some(out.events().len() as u64);
            }
            match out.next_token() {
                Some(t) => token = Some(t.to_string()),
                None => return Some(0),
            }
        }
        None
    }
}
