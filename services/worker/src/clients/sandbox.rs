//! Execution sandbox: runs a repo's REAL build/lint/tests in an isolated,
//! credential-free AWS CodeBuild container and returns the actual output, so
//! the agent can iterate against ground truth instead of guessing blind at CI.
//!
//! Flow: the worker (which holds the GitHub token) downloads the branch tarball
//! and uploads it to S3; CodeBuild pulls ONLY that S3 object (no GitHub token,
//! no DynamoDB, no KMS in its role) and runs the pre-derived check commands.
//! The verdict is read from a log marker the buildspec emits, NOT from the
//! CodeBuild build status — that cleanly separates "checks ran and failed"
//! (build SUCCEEDED, marker exit!=0) from "the sandbox itself broke" (no
//! marker), so a sandbox fault degrades to today's blind flow, never worse.
//!
//! The buildspec that emits the marker lives in infra/lib/worker-stack.ts.

use std::time::{Duration, Instant};
use tracing::{info, warn};

use aws_sdk_codebuild::types::{EnvironmentVariable, EnvironmentVariableType, StatusType};

const MARKER_END: &str = "CODERHELM_CHECKS_END exit=";
const MARKER_START: &str = "===CODERHELM_CHECKS_START===";
/// Poll cadence while a build runs.
const POLL_INTERVAL: Duration = Duration::from_secs(6);
/// Absolute ceiling on how long we'll wait for one build when there is no
/// wall-clock deadline (there always is inside a pass, but be defensive).
const MAX_WAIT: Duration = Duration::from_secs(12 * 60);
/// Leave this much runway before the pass deadline so poll + cleanup + the
/// agent's final turn still fit. If less than this remains, don't even start.
const DEADLINE_BUFFER: Duration = Duration::from_secs(75);
/// Cap on how much real output we hand back to the model (char-safe tail).
const MAX_OUTPUT_CHARS: usize = 6000;

/// Result of one sandbox check run.
pub struct CheckOutcome {
    /// The check commands actually executed (marker present). When false the
    /// sandbox could not run them (timeout, infra fault) and the caller should
    /// fall back to blind CI — this is NOT a check failure.
    pub ran: bool,
    /// Checks exited 0.
    pub passed: bool,
    pub exit_code: Option<i32>,
    /// Tail of the real command output (stdout+stderr), for the agent to read.
    pub output: String,
}

impl CheckOutcome {
    fn not_run(reason: impl Into<String>) -> Self {
        Self {
            ran: false,
            passed: false,
            exit_code: None,
            output: reason.into(),
        }
    }
}

/// Thin driver over CodeBuild + S3 + CloudWatch Logs. Holds only borrows from
/// `WorkerState`; construct it per pass.
pub struct SandboxClient<'a> {
    codebuild: &'a aws_sdk_codebuild::Client,
    s3: &'a aws_sdk_s3::Client,
    logs: &'a aws_sdk_cloudwatchlogs::Client,
    bucket: &'a str,
    project: &'a str,
}

impl<'a> SandboxClient<'a> {
    pub fn new(
        codebuild: &'a aws_sdk_codebuild::Client,
        s3: &'a aws_sdk_s3::Client,
        logs: &'a aws_sdk_cloudwatchlogs::Client,
        bucket: &'a str,
        project: &'a str,
    ) -> Self {
        Self {
            codebuild,
            s3,
            logs,
            bucket,
            project,
        }
    }

    /// Is the sandbox configured (both bucket and project set)?
    pub fn is_enabled(&self) -> bool {
        !self.bucket.is_empty() && !self.project.is_empty()
    }

    /// True if there is enough runway before `deadline` to attempt a build.
    pub fn has_time(&self, deadline: Option<Instant>) -> bool {
        match deadline {
            Some(dl) => {
                dl.saturating_duration_since(Instant::now()) > DEADLINE_BUFFER + POLL_INTERVAL
            }
            None => true,
        }
    }

    /// Upload the tarball, run the checks in CodeBuild, and return the real
    /// output. Never returns Err for an ordinary check failure — a failing
    /// build is `CheckOutcome { ran: true, passed: false, .. }`. Err is
    /// reserved for hard faults the caller should log and shrug off.
    pub async fn run_checks(
        &self,
        run_id: &str,
        attempt: usize,
        tarball: Vec<u8>,
        checks_cmd: &str,
        node_version: Option<&str>,
        deadline: Option<Instant>,
    ) -> Result<CheckOutcome, Box<dyn std::error::Error + Send + Sync>> {
        if !self.is_enabled() {
            return Ok(CheckOutcome::not_run("sandbox not configured"));
        }
        if !self.has_time(deadline) {
            return Ok(CheckOutcome::not_run(
                "not enough time left before the pass deadline to run a sandbox build",
            ));
        }

        // Unique per (run, attempt) so concurrent/retried checks never collide.
        let key = format!("sandbox/{run_id}/{attempt}.tgz");
        let tarball_len = tarball.len();
        if let Err(e) = self
            .s3
            .put_object()
            .bucket(self.bucket)
            .key(&key)
            .body(aws_sdk_s3::primitives::ByteStream::from(tarball))
            .content_type("application/gzip")
            .send()
            .await
        {
            return Err(format!("sandbox tarball upload failed: {e}").into());
        }
        info!(key = %key, bytes = tarball_len, "sandbox: uploaded source tarball");

        // Drive the build, then ALWAYS clean up the tarball (it is transient;
        // the bucket also has a 1-day lifecycle backstop).
        let outcome = self
            .build_and_read(&key, checks_cmd, node_version, deadline)
            .await;
        let _ = self
            .s3
            .delete_object()
            .bucket(self.bucket)
            .key(&key)
            .send()
            .await;
        outcome
    }

    async fn build_and_read(
        &self,
        key: &str,
        checks_cmd: &str,
        node_version: Option<&str>,
        deadline: Option<Instant>,
    ) -> Result<CheckOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let mut env = vec![
            env_var("SANDBOX_BUCKET", self.bucket)?,
            env_var("SANDBOX_KEY", key)?,
            env_var("CHECKS_CMD", checks_cmd)?,
        ];
        // The buildspec switches Node to this via `n` before running checks, so
        // "green in the sandbox" tracks the repo's real toolchain (it defaults
        // to 20 when unset). Prevents false REDs like a Next.js repo needing
        // >=20.9 failing on the image's default Node 18.
        if let Some(v) = node_version {
            env.push(env_var("NODE_VERSION", v)?);
        }
        let started = self
            .codebuild
            .start_build()
            .project_name(self.project)
            .set_environment_variables_override(Some(env))
            .send()
            .await
            .map_err(|e| format!("codebuild start_build failed: {e}"))?;
        let build_id = started
            .build_value()
            .and_then(|b| b.id())
            .ok_or("codebuild returned no build id")?
            .to_string();
        info!(build_id = %build_id, "sandbox: build started");

        // Poll until terminal or we run low on time. Cap by both the pass
        // deadline (minus buffer) and an absolute MAX_WAIT.
        let poll_until = {
            let by_max = Instant::now() + MAX_WAIT;
            match deadline {
                Some(dl) => (dl - DEADLINE_BUFFER).min(by_max),
                None => by_max,
            }
        };

        loop {
            let builds = self
                .codebuild
                .batch_get_builds()
                .ids(&build_id)
                .send()
                .await
                .map_err(|e| format!("codebuild batch_get_builds failed: {e}"))?;
            let build = builds
                .builds()
                .first()
                .ok_or("codebuild returned no build record")?;

            let status = build.build_status();
            let done = !matches!(status, Some(StatusType::InProgress));
            if done {
                // Pull log location before we lose the borrow.
                let (group, stream) = build
                    .logs()
                    .and_then(|l| Some((l.group_name()?.to_string(), l.stream_name()?.to_string())))
                    .unzip_or_default();
                let status_str = status.map(|s| s.as_str().to_string()).unwrap_or_default();
                return self.read_outcome(&group, &stream, &status_str).await;
            }

            if Instant::now() >= poll_until {
                warn!(build_id = %build_id, "sandbox: deadline reached, stopping build");
                let _ = self.codebuild.stop_build().id(&build_id).send().await;
                return Ok(CheckOutcome::not_run(
                    "sandbox build exceeded the time budget and was stopped",
                ));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn read_outcome(
        &self,
        group: &str,
        stream: &str,
        status_str: &str,
    ) -> Result<CheckOutcome, Box<dyn std::error::Error + Send + Sync>> {
        if group.is_empty() || stream.is_empty() {
            return Ok(CheckOutcome::not_run(format!(
                "sandbox produced no logs (build {status_str}) — treating as infra fault"
            )));
        }
        let full = self.fetch_log(group, stream).await?;
        let exit = parse_exit(&full);
        let window = extract_window(&full);
        let output = common::tail_str(&window, MAX_OUTPUT_CHARS).to_string();

        match exit {
            // A kill signal is a SANDBOX RESOURCE problem, not the agent's code:
            // 137 = SIGKILL (OOM), 143 = SIGTERM, 124 = coreutils `timeout`.
            // Reporting these as a code RED sent the agent chasing a "failure"
            // that was really the container running out of memory. Treat as
            // unverified -> blind fallback.
            Some(code) if code == 137 || code == 143 || code == 124 => {
                Ok(CheckOutcome::not_run(format!(
                    "sandbox checks were killed (exit {code} — out of memory or timeout), \
                     which is a sandbox resource limit, not a code failure; unverified"
                )))
            }
            Some(code) => Ok(CheckOutcome {
                ran: true,
                passed: code == 0,
                exit_code: Some(code),
                output,
            }),
            // Terminal build but no marker => the check commands never reached
            // completion (download/extract failed, image error). Infra fault.
            None => Ok(CheckOutcome::not_run(format!(
                "sandbox checks did not complete (build {status_str}, no result marker)"
            ))),
        }
    }

    /// Fetch the build's CloudWatch log stream, oldest-first, bounded.
    async fn fetch_log(
        &self,
        group: &str,
        stream: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut token: Option<String> = None;
        let mut all = String::new();
        for _ in 0..6 {
            let mut req = self
                .logs
                .get_log_events()
                .log_group_name(group)
                .log_stream_name(stream)
                .start_from_head(true)
                .limit(10_000);
            if let Some(t) = &token {
                req = req.next_token(t);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| format!("get_log_events failed: {e}"))?;
            for e in resp.events() {
                if let Some(m) = e.message() {
                    all.push_str(m.trim_end());
                    all.push('\n');
                }
            }
            let next = resp.next_forward_token().map(|s| s.to_string());
            // CloudWatch returns the SAME forward token once the stream is
            // exhausted — that's the stop signal.
            if next.as_deref() == token.as_deref() {
                break;
            }
            token = next;
            if all.len() > 400_000 {
                break;
            }
        }
        Ok(all)
    }
}

fn env_var(
    name: &str,
    value: &str,
) -> Result<EnvironmentVariable, Box<dyn std::error::Error + Send + Sync>> {
    EnvironmentVariable::builder()
        .name(name)
        .value(value)
        .r#type(EnvironmentVariableType::Plaintext)
        .build()
        .map_err(|e| format!("bad codebuild env var {name}: {e}").into())
}

/// Parse the exit code from the buildspec's end marker.
fn parse_exit(log: &str) -> Option<i32> {
    let idx = log.find(MARKER_END)?;
    let rest = &log[idx + MARKER_END.len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Slice the log to just the check window (between the start/end markers), so
/// we hand the agent the command output and not the sandbox's own plumbing.
fn extract_window(log: &str) -> String {
    let start = log.find(MARKER_START);
    let end = log.find(MARKER_END);
    match (start, end) {
        (Some(s), _) => log[s..].to_string(),
        (None, Some(_)) | (None, None) => log.to_string(),
    }
}

/// Small helper: `Option<(A,B)>::unzip` with a default when None.
trait UnzipOrDefault<A, B> {
    fn unzip_or_default(self) -> (A, B);
}
impl<A: Default, B: Default> UnzipOrDefault<A, B> for Option<(A, B)> {
    fn unzip_or_default(self) -> (A, B) {
        self.unwrap_or_default()
    }
}
