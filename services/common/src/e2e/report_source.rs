//! Where a repository's test reports are kept, as a team enters it in Tests
//! settings, and the address its end-to-end tests run against.
//!
//! A fix for a failing or flaky test starts from the failing run's report, so
//! Coderhelm has to find that report the moment a ticket arrives (reports kept
//! by CI often expire after about a day). A team gives one location per
//! repository:
//!
//! - `s3://<bucket>/<path with {run_id} and {attempt}>/`: a bucket the team's
//!   read-only AWS connection can read. `{run_id}` is the GitHub Actions run id
//!   and `{attempt}` its attempt; the folder holds a report (a Playwright HTML
//!   report's `index.html` with its `data/` and `trace/` folders, a Playwright
//!   JSON report, JUnit XML or Cucumber JSON).
//! - `ci`: the run's own GitHub Actions artifacts.
//!
//! Everything here is pure: parsing, checking and expanding the location.

use serde::{Deserialize, Serialize};

/// A repository's report location and test address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportSource {
    /// `owner/name`.
    pub repo: String,
    /// `s3://bucket/path/{run_id}/{attempt}/` or `ci`.
    pub location: String,
    /// The preview or staging address the end-to-end tests run against when
    /// Coderhelm repeats them (`https://staging.example.com`). `{pr}` is
    /// replaced with the pull request number. Empty: none.
    #[serde(default)]
    pub target_url: String,
}

/// A parsed location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Location {
    /// A folder per run in an S3 bucket; `pattern` is the key with its
    /// placeholders, without a leading `/`.
    S3 { bucket: String, pattern: String },
    /// The run's GitHub Actions artifacts.
    CiArtifacts,
}

/// The placeholders a location may use.
pub const PLACEHOLDERS: &[&str] = &["{run_id}", "{attempt}"];

/// Most report sources a team keeps (one per repository).
pub const MAX_SOURCES: usize = 200;

fn valid_bucket(b: &str) -> bool {
    (3..=63).contains(&b.len())
        && b.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
        && b.chars().next().is_some_and(|c| c.is_ascii_alphanumeric())
        && b.chars().last().is_some_and(|c| c.is_ascii_alphanumeric())
        && !b.contains("..")
}

/// Every `{...}` in `s`, in order.
fn braces(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = s;
    while let Some(i) = rest.find('{') {
        match rest[i..].find('}') {
            Some(j) => {
                out.push(&rest[i..i + j + 1]);
                rest = &rest[i + j + 1..];
            }
            None => {
                out.push(&rest[i..]);
                break;
            }
        }
    }
    out
}

/// Reads a location as a team typed it. The error is one sentence a person
/// can act on.
pub fn parse_location(raw: &str) -> Result<Location, String> {
    let s = raw.trim();
    if matches!(
        s.to_ascii_lowercase().as_str(),
        "ci" | "ci artifacts" | "ci-artifacts" | "github" | "github-artifacts"
    ) {
        return Ok(Location::CiArtifacts);
    }
    let Some(rest) = s.strip_prefix("s3://") else {
        return Err(
            "Enter an S3 location like s3://bucket/reports/{run_id}/{attempt}/, or ci for the run's GitHub Actions artifacts."
                .into(),
        );
    };
    let (bucket, pattern) = rest.split_once('/').unwrap_or((rest, ""));
    if !valid_bucket(bucket) {
        return Err(format!("`{bucket}` isn't a valid S3 bucket name."));
    }
    if s.chars()
        .any(|c| c.is_whitespace() || c == '\'' || c == '"' || c == '\\')
    {
        return Err("The location can't contain spaces, quotes or backslashes.".into());
    }
    for b in braces(pattern) {
        if !PLACEHOLDERS.contains(&b) {
            return Err(format!(
                "`{b}` isn't a placeholder Coderhelm fills in; use {{run_id}} and {{attempt}}."
            ));
        }
    }
    if !pattern.contains("{run_id}") {
        return Err(
            "The location needs {run_id}, so each run's report can be found (s3://bucket/reports/{run_id}/{attempt}/)."
                .into(),
        );
    }
    if pattern.len() > 512 {
        return Err("The location is too long.".into());
    }
    Ok(Location::S3 {
        bucket: bucket.to_string(),
        pattern: pattern.trim_start_matches('/').to_string(),
    })
}

/// Checks a test address: `http(s)://host[/path]`, `{pr}` allowed.
pub fn check_target(raw: &str) -> Result<(), String> {
    let s = raw.trim();
    if s.is_empty() {
        return Ok(());
    }
    let rest = s
        .strip_prefix("https://")
        .or_else(|| s.strip_prefix("http://"))
        .ok_or("The test address starts with https:// (or http://).")?;
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty()
        || s.len() > 300
        || s.chars().any(|c| {
            c.is_whitespace() || c.is_control() || matches!(c, '\'' | '"' | '`' | '\\' | '$' | ';')
        })
    {
        return Err(
            "Enter the address the tests run against, like https://staging.example.com.".into(),
        );
    }
    for b in braces(s) {
        if b != "{pr}" {
            return Err(format!(
                "`{b}` isn't a placeholder here; only {{pr}} is filled in."
            ));
        }
    }
    Ok(())
}

impl Location {
    /// The folder of one run's report (`reports/123/1/`), ending in `/`.
    pub fn prefix_for(&self, run_id: u64, attempt: u32) -> Option<String> {
        let Location::S3 { pattern, .. } = self else {
            return None;
        };
        let p = pattern
            .replace("{run_id}", &run_id.to_string())
            .replace("{attempt}", &attempt.max(1).to_string());
        Some(if p.is_empty() || p.ends_with('/') {
            p
        } else {
            format!("{p}/")
        })
    }

    /// The part of the key before any placeholder: listing it finds every
    /// retained run.
    pub fn list_prefix(&self) -> Option<String> {
        let Location::S3 { pattern, .. } = self else {
            return None;
        };
        let cut = pattern.find('{').unwrap_or(pattern.len());
        Some(pattern[..cut].to_string())
    }

    /// The run (and attempt) a listed key belongs to.
    pub fn run_of_key(&self, key: &str) -> Option<(u64, u32)> {
        let Location::S3 { pattern, .. } = self else {
            return None;
        };
        let mut run: Option<u64> = None;
        let mut attempt: Option<u32> = None;
        let mut pat = pattern.as_str();
        let mut k = key;
        loop {
            match pat.find('{') {
                None => {
                    // The rest of the pattern is a literal folder the key
                    // must continue with.
                    if !k.starts_with(pat.trim_end_matches('/')) {
                        return None;
                    }
                    break;
                }
                Some(i) => {
                    let lit = &pat[..i];
                    k = k.strip_prefix(lit)?;
                    let end = pat[i..].find('}')? + i + 1;
                    let ph = &pat[i..end];
                    let digits = k.len() - k.trim_start_matches(|c: char| c.is_ascii_digit()).len();
                    if digits == 0 {
                        return None;
                    }
                    let n = &k[..digits];
                    match ph {
                        "{run_id}" => run = n.parse().ok(),
                        "{attempt}" => attempt = n.parse().ok(),
                        _ => return None,
                    }
                    k = &k[digits..];
                    pat = &pat[end..];
                }
            }
        }
        Some((run?, attempt.unwrap_or(1)))
    }

    /// `s3://bucket/key`, for people and the PR.
    pub fn uri(&self, key: &str) -> String {
        match self {
            Location::S3 { bucket, .. } => format!("s3://{bucket}/{key}"),
            Location::CiArtifacts => "the run's GitHub Actions artifacts".into(),
        }
    }

    pub fn bucket(&self) -> Option<&str> {
        match self {
            Location::S3 { bucket, .. } => Some(bucket),
            Location::CiArtifacts => None,
        }
    }
}

impl ReportSource {
    pub fn location(&self) -> Result<Location, String> {
        parse_location(&self.location)
    }

    /// The test address for a pull request, `None` when there is none.
    pub fn target_for(&self, pr: Option<u64>) -> Option<String> {
        let t = self.target_url.trim();
        if t.is_empty() || check_target(t).is_err() {
            return None;
        }
        match pr {
            Some(n) => Some(t.replace("{pr}", &n.to_string())),
            None if t.contains("{pr}") => None,
            None => Some(t.to_string()),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let r = self.repo.trim();
        if r.split('/').count() != 2 || r.starts_with('/') || r.ends_with('/') {
            return Err(format!("`{r}` isn't a repository (owner/name)."));
        }
        parse_location(&self.location).map_err(|e| format!("{r}: {e}"))?;
        check_target(&self.target_url).map_err(|e| format!("{r}: {e}"))
    }
}

/// The IAM statement the team's read-only role needs to read this location
/// (shown next to the setting; nothing is changed in the team's account).
pub fn read_statement(loc: &Location) -> Option<serde_json::Value> {
    let Location::S3 { bucket, .. } = loc else {
        return None;
    };
    let prefix = loc.list_prefix().unwrap_or_default();
    Some(serde_json::json!({
        "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:ListBucket"],
        "Resource": [
            format!("arn:aws:s3:::{bucket}"),
            format!("arn:aws:s3:::{bucket}/{prefix}*"),
        ],
    }))
}

/// The sources kept: trimmed, valid, one per repository.
pub fn sanitize(sources: Vec<ReportSource>) -> Vec<ReportSource> {
    let mut out: Vec<ReportSource> = Vec::new();
    for mut s in sources {
        s.repo = s.repo.trim().to_string();
        s.location = s.location.trim().to_string();
        s.target_url = s.target_url.trim().to_string();
        if s.location.is_empty() || s.validate().is_err() {
            continue;
        }
        if out.iter().any(|o| o.repo.eq_ignore_ascii_case(&s.repo)) {
            continue;
        }
        out.push(s);
        if out.len() >= MAX_SOURCES {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(loc: &str) -> ReportSource {
        ReportSource {
            repo: "acme/web".into(),
            location: loc.into(),
            target_url: String::new(),
        }
    }

    #[test]
    fn s3_locations_are_read_with_their_placeholders() {
        let l = parse_location("s3://acme-test-reports/web/{run_id}/{attempt}/").unwrap();
        assert_eq!(
            l,
            Location::S3 {
                bucket: "acme-test-reports".into(),
                pattern: "web/{run_id}/{attempt}/".into()
            }
        );
        assert_eq!(l.prefix_for(123, 2).unwrap(), "web/123/2/");
        assert_eq!(l.prefix_for(123, 0).unwrap(), "web/123/1/");
        assert_eq!(l.list_prefix().unwrap(), "web/");
        assert_eq!(l.run_of_key("web/123/2/index.html"), Some((123, 2)));
        assert_eq!(l.run_of_key("web/123/2/data/abc.md"), Some((123, 2)));
        assert_eq!(l.run_of_key("other/123/2/index.html"), None);
        assert_eq!(
            l.uri("web/1/1/index.html"),
            "s3://acme-test-reports/web/1/1/index.html"
        );
    }

    #[test]
    fn a_location_without_attempt_reads_as_the_first_attempt() {
        let l = parse_location("s3://reports/runs/{run_id}").unwrap();
        assert_eq!(l.prefix_for(9, 3).unwrap(), "runs/9/");
        assert_eq!(l.run_of_key("runs/9/report.json"), Some((9, 1)));
        let mid = parse_location("s3://reports/x-{run_id}-a{attempt}/html").unwrap();
        assert_eq!(mid.run_of_key("x-77-a2/html/index.html"), Some((77, 2)));
        assert_eq!(mid.run_of_key("x-77-a2/other/index.html"), None);
    }

    #[test]
    fn ci_artifacts_need_no_path() {
        assert_eq!(parse_location(" ci ").unwrap(), Location::CiArtifacts);
        assert_eq!(
            parse_location("CI-artifacts").unwrap(),
            Location::CiArtifacts
        );
        assert!(Location::CiArtifacts.prefix_for(1, 1).is_none());
    }

    #[test]
    fn bad_locations_say_what_to_fix() {
        for (bad, says) in [
            ("https://example.com/report", "S3 location"),
            ("s3://Bad_Bucket/{run_id}/", "valid S3 bucket"),
            ("s3://bucket/reports/latest/", "needs {run_id}"),
            ("s3://bucket/{run}/{attempt}/", "isn't a placeholder"),
            ("s3://bucket/a b/{run_id}/", "spaces"),
        ] {
            let e = parse_location(bad).unwrap_err();
            assert!(e.contains(says), "{bad}: {e}");
        }
    }

    #[test]
    fn targets_are_addresses_with_an_optional_pr() {
        assert!(check_target("").is_ok());
        assert!(check_target("https://staging.example.com").is_ok());
        assert!(check_target("https://pr-{pr}.preview.example.com/app").is_ok());
        assert!(check_target("staging.example.com").is_err());
        assert!(check_target("https://x.example.com/$(id)").is_err());
        assert!(check_target("https://x.example.com/{branch}").is_err());
        let mut s = src("ci");
        s.target_url = "https://pr-{pr}.preview.example.com".into();
        assert_eq!(
            s.target_for(Some(42)).unwrap(),
            "https://pr-42.preview.example.com"
        );
        assert_eq!(s.target_for(None), None);
        s.target_url = "https://staging.example.com".into();
        assert_eq!(s.target_for(None).unwrap(), "https://staging.example.com");
    }

    #[test]
    fn the_read_statement_covers_the_bucket_and_the_folder() {
        let l = parse_location("s3://acme-test-reports/web/{run_id}/").unwrap();
        let st = read_statement(&l).unwrap();
        assert_eq!(st["Action"][0], "s3:GetObject");
        assert_eq!(st["Resource"][1], "arn:aws:s3:::acme-test-reports/web/*");
        assert!(read_statement(&Location::CiArtifacts).is_none());
    }

    #[test]
    fn sources_are_sanitized_one_per_repo() {
        let kept = sanitize(vec![
            src(" s3://bucket/{run_id}/ "),
            ReportSource {
                repo: "ACME/web".into(),
                ..src("ci")
            },
            ReportSource {
                repo: "acme/api".into(),
                ..src("nope")
            },
            ReportSource {
                repo: "acme/app".into(),
                ..src("")
            },
        ]);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].location, "s3://bucket/{run_id}/");
        assert!(src("s3://bucket/{run_id}/").validate().is_ok());
        assert!(ReportSource {
            repo: "nope".into(),
            ..src("ci")
        }
        .validate()
        .is_err());
    }
}
