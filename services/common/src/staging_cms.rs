//! A repository's staging CMS write connection: optional, per repository,
//! staging only.
//!
//! When a feature changes a CMS schema and the repository's end-to-end tests
//! run against pages the CMS serves, the staging content the tests read can
//! go out of date. With this connection CoderHelm updates that content
//! itself, never production; without it the run stops before the pull
//! request and says exactly which content must change.
//!
//! The rules, enforced here and covered by the tests below:
//! - one provider today: Sanity (a project id and a dataset);
//! - the dataset must not look like production (`production`, `prod`,
//!   `live`, `master`, `main`, `prd` as a word) and must differ from the
//!   production dataset the team names; [`StagingCms::check_writable`] runs
//!   again before every write, so a record edited by hand can't reach
//!   production;
//! - the token is sealed by KMS bound to the team, the repository and this
//!   purpose, and never returned by the API.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};

/// The settings-table sort key for a repository's connection.
pub fn sk(repo: &str) -> String {
    format!("STAGING_CMS#REPO#{repo}")
}

/// Where the sealed token is kept on the record.
pub const TOKEN_ATTR: &str = "token_sealed";
const PURPOSE: &str = "staging-cms-write";
/// The Sanity HTTP API version CoderHelm calls.
pub const SANITY_API_VERSION: &str = "2021-06-07";
/// Providers CoderHelm can write to.
pub const PROVIDERS: &[&str] = &["sanity"];

/// Words that mark a production dataset.
const PRODUCTION_WORDS: &[&str] = &["production", "prod", "prd", "live", "master", "main"];

/// Whether a dataset name looks like production.
pub fn is_production_name(name: &str) -> bool {
    name.to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|w| PRODUCTION_WORDS.contains(&w))
}

/// A repository's staging CMS connection, without its token.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingCms {
    /// `owner/name`.
    pub repo: String,
    /// `sanity`.
    pub provider: String,
    pub project_id: String,
    /// The staging dataset CoderHelm writes to.
    pub dataset: String,
    /// The production dataset, named so it is refused. Optional.
    #[serde(default)]
    pub production_dataset: String,
}

fn valid_id(s: &str, max: usize, extra: &[char]) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || extra.contains(&c))
}

impl StagingCms {
    /// Checks a connection as a team entered it. The error is one sentence a
    /// person can act on.
    pub fn validate(&self) -> Result<(), String> {
        if !self.repo.contains('/') || self.repo.len() > 200 {
            return Err("Pick the repository this connection is for.".into());
        }
        if !PROVIDERS.contains(&self.provider.as_str()) {
            return Err("CoderHelm can write to Sanity staging datasets; pick Sanity.".into());
        }
        if !valid_id(&self.project_id, 32, &[]) {
            return Err("Enter the Sanity project id (lowercase letters and digits).".into());
        }
        if !valid_id(&self.dataset, 64, &['-', '_']) {
            return Err(
                "Enter the staging dataset name (lowercase letters, digits, - and _).".into(),
            );
        }
        if !self.production_dataset.is_empty()
            && !valid_id(&self.production_dataset, 64, &['-', '_'])
        {
            return Err("The production dataset name isn't valid.".into());
        }
        self.check_writable()
    }

    /// Whether CoderHelm may write here: never a dataset that looks like
    /// production or is the one named as production.
    pub fn check_writable(&self) -> Result<(), String> {
        if is_production_name(&self.dataset) {
            return Err(format!(
                "`{}` looks like a production dataset. CoderHelm writes only to staging; enter the staging dataset.",
                self.dataset
            ));
        }
        if !self.production_dataset.is_empty() && self.production_dataset == self.dataset {
            return Err(
                "The staging dataset is the same as the production dataset. CoderHelm writes only to staging."
                    .into(),
            );
        }
        Ok(())
    }

    /// Refuses a dataset the repository's own production settings name
    /// (for example `SANITY_DATASET=...` in `.env.production`).
    pub fn check_against_code(&self, production_in_code: &[String]) -> Result<(), String> {
        if production_in_code.iter().any(|d| d == &self.dataset) {
            return Err(format!(
                "The repository's production settings use the dataset `{}`. CoderHelm writes only to staging.",
                self.dataset
            ));
        }
        Ok(())
    }

    fn base(&self) -> String {
        format!(
            "https://{}.api.sanity.io/v{SANITY_API_VERSION}/data",
            self.project_id
        )
    }

    /// The query endpoint; the query goes in the `query` parameter.
    pub fn query_url(&self) -> String {
        format!("{}/query/{}", self.base(), self.dataset)
    }

    /// The mutate endpoint.
    pub fn mutate_url(&self) -> String {
        format!("{}/mutate/{}", self.base(), self.dataset)
    }

    /// The record, without the token.
    pub fn item(&self, team_id: &str) -> HashMap<String, AttributeValue> {
        let s = |v: &str| AttributeValue::S(v.to_string());
        HashMap::from([
            ("pk".to_string(), s(team_id)),
            ("sk".to_string(), s(&sk(&self.repo))),
            ("repo".to_string(), s(&self.repo)),
            ("provider".to_string(), s(&self.provider)),
            ("project_id".to_string(), s(&self.project_id)),
            ("dataset".to_string(), s(&self.dataset)),
            (
                "production_dataset".to_string(),
                s(&self.production_dataset),
            ),
        ])
    }

    /// Reads a record. `None` when it isn't one.
    pub fn from_item(item: &HashMap<String, AttributeValue>) -> Option<StagingCms> {
        let s = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        let c = StagingCms {
            repo: s("repo"),
            provider: s("provider"),
            project_id: s("project_id"),
            dataset: s("dataset"),
            production_dataset: s("production_dataset"),
        };
        (!c.repo.is_empty() && !c.dataset.is_empty()).then_some(c)
    }

    /// The API's view: never the token, only whether one is set.
    pub fn view(&self, token_set: bool) -> serde_json::Value {
        serde_json::json!({
            "configured": true,
            "repo": self.repo,
            "provider": self.provider,
            "project_id": self.project_id,
            "dataset": self.dataset,
            "production_dataset": self.production_dataset,
            "token_set": token_set,
        })
    }
}

/// Datasets a repository's production settings name: `*DATASET=value` lines
/// in production env files, given as (path, text).
pub fn production_datasets_in_code(files: &[(String, String)]) -> Vec<String> {
    let mut out = Vec::new();
    for (path, text) in files {
        let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
        if !(name.starts_with(".env") && name.contains("prod")) {
            continue;
        }
        for line in text.lines() {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            if k.trim().to_ascii_uppercase().ends_with("DATASET") {
                let v = v.trim().trim_matches(|c| c == '"' || c == '\'').to_string();
                if !v.is_empty() && !out.contains(&v) {
                    out.push(v);
                }
            }
        }
    }
    out
}

/// The encryption context: the team, the repository and the purpose.
pub fn context(team_id: &str, repo: &str) -> HashMap<String, String> {
    HashMap::from([
        ("team".to_string(), team_id.to_string()),
        ("repo".to_string(), repo.to_string()),
        ("purpose".to_string(), PURPOSE.to_string()),
    ])
}

/// Seals a token for one team's repository. Returns text safe to store.
pub async fn seal(
    cipher: &dyn crate::mcp_credentials::Cipher,
    team_id: &str,
    repo: &str,
    token: &str,
) -> Result<String, String> {
    use base64::Engine;
    let blob = cipher
        .encrypt(context(team_id, repo), token.as_bytes())
        .await?;
    Ok(base64::engine::general_purpose::STANDARD.encode(blob))
}

/// Opens a token sealed for one team's repository.
pub async fn open(
    cipher: &dyn crate::mcp_credentials::Cipher,
    team_id: &str,
    repo: &str,
    sealed: &str,
) -> Result<String, String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(sealed)
        .map_err(|_| "stored token is not valid".to_string())?;
    let plain = cipher.decrypt(context(team_id, repo), &bytes).await?;
    String::from_utf8(plain).map_err(|_| "stored token is not text".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(dataset: &str) -> StagingCms {
        StagingCms {
            repo: "acme/web".into(),
            provider: "sanity".into(),
            project_id: "abc123".into(),
            dataset: dataset.into(),
            production_dataset: "public".into(),
        }
    }

    #[test]
    fn a_staging_dataset_is_accepted() {
        assert!(conn("staging").validate().is_ok());
        assert!(conn("qa-2").validate().is_ok());
        assert_eq!(
            conn("staging").mutate_url(),
            "https://abc123.api.sanity.io/v2021-06-07/data/mutate/staging"
        );
    }

    #[test]
    fn production_is_never_writable() {
        for d in ["production", "prod", "site-prod", "live", "main", "prd_eu"] {
            let err = conn(d).validate().unwrap_err();
            assert!(err.contains("only to staging"), "{d}: {err}");
        }
        // The dataset the team names as production, whatever its name.
        assert!(conn("public").validate().is_err());
        // Words that only contain a production word are fine.
        assert!(!is_production_name("products-staging"));
        assert!(!is_production_name("maintenance"));
    }

    #[test]
    fn the_repositorys_production_settings_are_refused_too() {
        let files = vec![
            (
                ".env.production".to_string(),
                "NEXT_PUBLIC_SANITY_DATASET=\"stage-copy\"\nOTHER=1".to_string(),
            ),
            (".env.local".to_string(), "SANITY_DATASET=dev".to_string()),
        ];
        let prod = production_datasets_in_code(&files);
        assert_eq!(prod, vec!["stage-copy"]);
        assert!(conn("stage-copy").check_against_code(&prod).is_err());
        assert!(conn("staging").check_against_code(&prod).is_ok());
    }

    #[test]
    fn bad_input_reads_as_a_sentence() {
        let mut c = conn("staging");
        c.provider = "other".into();
        assert!(c.validate().unwrap_err().contains("Sanity"));
        let mut c = conn("staging");
        c.project_id = "Bad Id".into();
        assert!(c.validate().is_err());
        let mut c = conn("Staging Data");
        c.production_dataset.clear();
        assert!(c.validate().is_err());
    }

    #[test]
    fn the_record_round_trips_without_a_token() {
        let c = conn("staging");
        let item = c.item("team1");
        assert_eq!(StagingCms::from_item(&item), Some(c.clone()));
        assert!(!item.contains_key(TOKEN_ATTR));
        let v = c.view(true);
        assert_eq!(v["token_set"], true);
        assert!(v.get("token").is_none());
    }
}
