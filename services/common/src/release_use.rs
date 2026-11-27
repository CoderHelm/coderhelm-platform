//! How much real work finished on each release an install ran: a count of
//! finished runs and of Chat answers per version, nothing else. The
//! updater reads these counts to tell CoderHelm how a beta did on this
//! install when its admin turned that report on (lambda/updater/handler.py,
//! `release_report`), and stable promotion waits for them
//! (.github/scripts/auto_release.py).
//!
//! Record on the install's own settings table: `pk` = [`PK`], `sk` =
//! `VERSION#<version>`, with the numbers `runs` and `answers`. No team, no
//! person, no repository, no text: only how many.

use aws_sdk_dynamodb::types::AttributeValue;

/// The partition the counts live in.
pub const PK: &str = "RELEASE_USE";

/// What finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Done {
    /// A run that ended completed.
    Run,
    /// A Chat answer that was sent and kept.
    Answer,
}

impl Done {
    pub fn field(self) -> &'static str {
        match self {
            Done::Run => "runs",
            Done::Answer => "answers",
        }
    }
}

/// The release this process runs, when it is one (`CODERHELM_VERSION`,
/// set on every install's functions); None on a development build.
pub fn running_version() -> Option<String> {
    version_of(&std::env::var("CODERHELM_VERSION").unwrap_or_default())
}

/// `v` when it names a release (vX.Y.Z, with an optional suffix).
pub fn version_of(v: &str) -> Option<String> {
    let v = v.trim();
    let core = v.strip_prefix('v')?;
    let (nums, suffix) = core.split_once('-').unwrap_or((core, ""));
    let parts: Vec<&str> = nums.split('.').collect();
    let ok = parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
        && suffix
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
        && v.len() <= 40;
    ok.then(|| v.to_string())
}

/// The record's key for one version.
pub fn sk(version: &str) -> String {
    format!("VERSION#{version}")
}

/// Count one finished piece of work on the release running now. Never
/// fails the caller: a count not written is logged and dropped.
pub async fn record(dynamo: &aws_sdk_dynamodb::Client, table: &str, done: Done) {
    let Some(version) = running_version() else {
        return;
    };
    let r = dynamo
        .update_item()
        .table_name(table)
        .key("pk", AttributeValue::S(PK.into()))
        .key("sk", AttributeValue::S(sk(&version)))
        .update_expression("ADD #n :one")
        .expression_attribute_names("#n", done.field())
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .send()
        .await;
    if let Err(e) = r {
        tracing::warn!(error = %e, field = done.field(), "Release use not counted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_release_versions_are_counted() {
        assert_eq!(version_of("v0.9.3").as_deref(), Some("v0.9.3"));
        assert_eq!(
            version_of("v0.9.3-beta.20300101").as_deref(),
            Some("v0.9.3-beta.20300101")
        );
        for bad in ["", "dev", "0.9.3", "v0.9", "v0.9.x", "v1.2.3-<b>"] {
            assert_eq!(version_of(bad), None, "{bad}");
        }
    }

    #[test]
    fn the_record_holds_counts_only() {
        assert_eq!(sk("v1.2.3"), "VERSION#v1.2.3");
        assert_eq!(Done::Run.field(), "runs");
        assert_eq!(Done::Answer.field(), "answers");
    }
}
