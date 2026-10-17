//! The shapes E2E results are kept in. Field names match the dashboard's
//! contract, so these serialize straight into API responses.

use serde::{Deserialize, Serialize};

/// Which report a result came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Framework {
    Playwright,
    Junit,
    Cucumber,
}

impl Framework {
    pub fn as_str(self) -> &'static str {
        match self {
            Framework::Playwright => "playwright",
            Framework::Junit => "junit",
            Framework::Cucumber => "cucumber",
        }
    }
}

/// How one attempt of a test ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptStatus {
    #[serde(rename = "passed")]
    Passed,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "timedOut")]
    TimedOut,
    #[serde(rename = "skipped")]
    Skipped,
    #[serde(rename = "interrupted")]
    Interrupted,
}

impl AttemptStatus {
    pub fn is_failure(self) -> bool {
        matches!(self, AttemptStatus::Failed | AttemptStatus::TimedOut)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptError {
    pub message: String,
    pub signature: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub retry: u32,
    pub status: AttemptStatus,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<AttemptError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub screenshot_url: Option<String>,
}

/// What a test did in one run, across its attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Passed,
    Failed,
    /// Failed, then passed on a retry of the same commit.
    Flaky,
    Skipped,
}

impl Outcome {
    pub fn of(attempts: &[Attempt]) -> Outcome {
        let ran: Vec<&Attempt> = attempts
            .iter()
            .filter(|a| !matches!(a.status, AttemptStatus::Skipped))
            .collect();
        if ran.is_empty() {
            return Outcome::Skipped;
        }
        let any_fail = ran.iter().any(|a| a.status.is_failure());
        let any_pass = ran.iter().any(|a| a.status == AttemptStatus::Passed);
        match (any_fail, any_pass) {
            (true, true) => Outcome::Flaky,
            (true, false) => Outcome::Failed,
            (false, true) => Outcome::Passed,
            // Only interrupted attempts: the run died, not the test.
            (false, false) => Outcome::Skipped,
        }
    }
}

/// One test as read from a report, before anything is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTest {
    pub framework: Framework,
    pub file: String,
    pub suite: Vec<String>,
    pub title: String,
    pub project: Option<String>,
    pub tags: Vec<String>,
    pub attempts: Vec<Attempt>,
}

impl ParsedTest {
    /// Stable id across runs: framework, file, suite path, title, project.
    pub fn id(&self) -> String {
        test_id(
            self.framework,
            &self.file,
            &self.suite,
            &self.title,
            self.project.as_deref(),
        )
    }

    pub fn full_title(&self) -> String {
        let mut parts = self.suite.clone();
        parts.push(self.title.clone());
        parts.join(" › ")
    }
}

pub fn test_id(
    framework: Framework,
    file: &str,
    suite: &[String],
    title: &str,
    project: Option<&str>,
) -> String {
    let key = format!(
        "{}|{}|{}|{}|{}",
        framework.as_str(),
        file,
        suite.join(" › "),
        title,
        project.unwrap_or("")
    );
    crate::content_hash(&key)[..20].to_string()
}

/// A workflow run as the dashboard lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub run_key: String,
    pub repo: String,
    pub workflow: String,
    pub run_id: u64,
    pub run_attempt: u32,
    pub branch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr: Option<u64>,
    pub sha: String,
    pub event: String,
    pub framework: String,
    pub started_at: String,
    pub duration_ms: u64,
    pub passed: u32,
    pub failed: u32,
    pub flaky: u32,
    pub skipped: u32,
    pub bad_run: bool,
    pub url: String,
    pub status: String,
    /// Failures of tests that were quarantined at the time.
    #[serde(default)]
    pub quarantined_failures: u32,
    /// Every failure in the run was quarantined, so it no longer blocks.
    #[serde(default)]
    pub rescued: bool,
}

impl Run {
    pub fn key(run_id: u64, run_attempt: u32) -> String {
        format!("{run_id}-{run_attempt}")
    }
}

/// A test's line in a stored run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Execution {
    pub test_id: String,
    pub title: String,
    pub file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub attempts: Vec<Attempt>,
    pub outcome: Outcome,
}

/// One run in a test's history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub run_key: String,
    pub sha: String,
    pub branch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr: Option<u64>,
    pub at: String,
    pub outcome: Outcome,
    /// Failures in a run that broke as a whole are not the test's fault.
    #[serde(default)]
    pub bad_run: bool,
    /// Kept for the most recent runs only; older entries keep the outcome.
    #[serde(default)]
    pub attempts: Vec<Attempt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Healthy,
    Flaky,
    Broken,
    Quarantined,
    Fixed,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Healthy => "healthy",
            State::Flaky => "flaky",
            State::Broken => "broken",
            State::Quarantined => "quarantined",
            State::Fixed => "fixed",
        }
    }
}

pub const CAUSES: &[&str] = &[
    "selector_changed",
    "missing_wait",
    "test_data_clash",
    "environment",
    "network",
    "timing",
    "order_dependence",
    "animation_loader",
    "unknown",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cause {
    pub label: String,
    pub evidence: String,
}

/// A change of state, with why. Kept as the test's audit log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateEvent {
    pub at: String,
    pub from: String,
    pub to: String,
    pub reason: String,
    pub actor: String,
}

/// Everything known about one test. Stored as one item; the dashboard's
/// `Test` shape is derived from it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestRecord {
    pub id: String,
    pub repo: String,
    pub framework: Framework,
    pub file: String,
    pub title: String,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    /// Whether the test is flaky or broken, before any quarantine.
    pub health: State,
    #[serde(default)]
    pub quarantined: bool,
    #[serde(default)]
    pub quarantined_since: Option<String>,
    /// Who quarantined it: `auto` or a person's email.
    #[serde(default)]
    pub quarantined_by: Option<String>,
    #[serde(default)]
    pub never_quarantine: bool,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub cause: Option<Cause>,
    #[serde(default)]
    pub last_flip_at: Option<String>,
    #[serde(default)]
    pub runs_since_flip: u32,
    #[serde(default)]
    pub fix_merged_at: Option<String>,
    #[serde(default)]
    pub ticket_url: Option<String>,
    #[serde(default)]
    pub fix_pr_url: Option<String>,
    #[serde(default)]
    pub fix_status: Option<String>,
    /// The branch the current fix (or its reproduction) runs on.
    #[serde(default)]
    pub fix_branch: Option<String>,
    /// When the current fix phase started; runs before it are ignored.
    #[serde(default)]
    pub fix_phase_at: Option<String>,
    /// The workflow file the test last ran in, for dispatching repeat runs.
    #[serde(default)]
    pub workflow_path: Option<String>,
    pub last_seen: String,
    #[serde(default)]
    pub history: Vec<HistoryEntry>,
    #[serde(default)]
    pub events: Vec<StateEvent>,
}

impl TestRecord {
    /// The state shown first: quarantined wins over flaky; broken is never
    /// quarantined.
    pub fn state(&self) -> State {
        match self.health {
            State::Broken => State::Broken,
            _ if self.quarantined => State::Quarantined,
            other => other,
        }
    }

    pub fn flags(&self) -> Vec<String> {
        let mut flags = Vec::new();
        if self.quarantined && !matches!(self.health, State::Broken | State::Healthy) {
            flags.push(self.health.as_str().to_string());
        }
        if self.never_quarantine {
            flags.push("never_quarantine".to_string());
        }
        flags
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    pub kind: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recommendation {
    pub id: String,
    pub repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub area: Option<String>,
    pub title: String,
    pub why: String,
    pub evidence: Vec<Evidence>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_url: Option<String>,
    #[serde(default)]
    pub created_at: String,
    /// Where a generated test stands: `burn_in` | `base_check` while it proves
    /// itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_at: Option<String>,
    /// Titles of the tests the generated PR added.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub new_tests: Vec<String>,
}

/// A team's E2E settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub auto_quarantine: bool,
    #[serde(default)]
    pub fix_prs: bool,
    /// `github` | `jira` | `off`
    #[serde(default = "default_tickets")]
    pub tickets: String,
    #[serde(default)]
    pub burn_in: bool,
    /// `owner/name` repos it applies to. Empty means all.
    #[serde(default)]
    pub repos: Vec<String>,
}

fn default_tickets() -> String {
    "off".to_string()
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            enabled: false,
            auto_quarantine: false,
            fix_prs: false,
            tickets: default_tickets(),
            burn_in: false,
            repos: Vec::new(),
        }
    }
}

impl Settings {
    pub fn covers(&self, repo: &str) -> bool {
        self.enabled
            && (self.repos.is_empty() || self.repos.iter().any(|r| r.eq_ignore_ascii_case(repo)))
    }

    /// Keeps only known values, so a bad request can't store junk.
    pub fn sanitized(mut self) -> Settings {
        if !matches!(self.tickets.as_str(), "github" | "jira" | "off") {
            self.tickets = "off".to_string();
        }
        self.repos.retain(|r| {
            let r = r.trim();
            r.split('/').count() == 2 && !r.starts_with('/') && !r.ends_with('/') && r.len() <= 200
        });
        self.repos.truncate(200);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn att(retry: u32, status: AttemptStatus) -> Attempt {
        Attempt {
            retry,
            status,
            duration_ms: 10,
            error: None,
            trace_url: None,
            video_url: None,
            screenshot_url: None,
        }
    }

    #[test]
    fn outcomes_follow_attempts() {
        use AttemptStatus::*;
        assert_eq!(Outcome::of(&[att(0, Passed)]), Outcome::Passed);
        assert_eq!(
            Outcome::of(&[att(0, Failed), att(1, Passed)]),
            Outcome::Flaky
        );
        assert_eq!(
            Outcome::of(&[att(0, TimedOut), att(1, Failed)]),
            Outcome::Failed
        );
        assert_eq!(Outcome::of(&[att(0, Skipped)]), Outcome::Skipped);
        assert_eq!(Outcome::of(&[att(0, Interrupted)]), Outcome::Skipped);
        assert_eq!(Outcome::of(&[]), Outcome::Skipped);
    }

    #[test]
    fn ids_are_stable_and_distinct() {
        let a = test_id(
            Framework::Playwright,
            "a.spec.ts",
            &["S".into()],
            "t",
            Some("chromium"),
        );
        assert_eq!(
            a,
            test_id(
                Framework::Playwright,
                "a.spec.ts",
                &["S".into()],
                "t",
                Some("chromium")
            )
        );
        assert_ne!(
            a,
            test_id(
                Framework::Playwright,
                "a.spec.ts",
                &["S".into()],
                "t",
                Some("webkit")
            )
        );
        assert_ne!(
            a,
            test_id(
                Framework::Junit,
                "a.spec.ts",
                &["S".into()],
                "t",
                Some("chromium")
            )
        );
        assert_eq!(a.len(), 20);
    }

    #[test]
    fn broken_is_never_shown_as_quarantined() {
        let mut r = TestRecord {
            id: "x".into(),
            repo: "acme/web".into(),
            framework: Framework::Playwright,
            file: "a".into(),
            title: "t".into(),
            project: None,
            tags: vec![],
            health: State::Flaky,
            quarantined: true,
            quarantined_since: None,
            quarantined_by: None,
            never_quarantine: false,
            owner: None,
            cause: None,
            last_flip_at: None,
            runs_since_flip: 0,
            fix_merged_at: None,
            ticket_url: None,
            fix_pr_url: None,
            fix_status: None,
            fix_branch: None,
            fix_phase_at: None,
            workflow_path: None,
            last_seen: String::new(),
            history: vec![],
            events: vec![],
        };
        assert_eq!(r.state(), State::Quarantined);
        assert_eq!(r.flags(), vec!["flaky"]);
        r.health = State::Broken;
        assert_eq!(r.state(), State::Broken);
    }

    #[test]
    fn settings_drop_junk() {
        let s = Settings {
            tickets: "email".into(),
            repos: vec!["acme/web".into(), "nope".into(), "/x/".into()],
            ..Default::default()
        }
        .sanitized();
        assert_eq!(s.tickets, "off");
        assert_eq!(s.repos, vec!["acme/web"]);
        let on = Settings {
            enabled: true,
            ..Default::default()
        };
        assert!(on.covers("acme/web"));
        let scoped = Settings {
            enabled: true,
            repos: vec!["acme/web".into()],
            ..Default::default()
        };
        assert!(scoped.covers("Acme/Web"));
        assert!(!scoped.covers("acme/api"));
        assert!(!Settings::default().covers("acme/web"));
    }
}
