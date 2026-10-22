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
/// What a check build realistically takes end to end (provisioning, source
/// download, dependency install, checks). Production builds run ~150s; starting
/// one with less runway than this got it killed mid-run by the deadline — a
/// wasted build AND a lost verdict. Used until enough builds have been
/// observed in this process to use their p90 instead.
const DEFAULT_BUILD_ESTIMATE: Duration = Duration::from_secs(180);
/// Builds observed before the measured p90 replaces the default.
const MIN_SAMPLES_FOR_P90: usize = 5;
/// Rolling window of observed check-build durations.
const MAX_SAMPLES: usize = 30;

static OBSERVED_BUILDS: std::sync::Mutex<std::collections::VecDeque<Duration>> =
    std::sync::Mutex::new(std::collections::VecDeque::new());

/// Record how long a completed check build took (start_build → terminal).
fn record_build_duration(d: Duration) {
    if let Ok(mut q) = OBSERVED_BUILDS.lock() {
        q.push_back(d);
        while q.len() > MAX_SAMPLES {
            q.pop_front();
        }
    }
}

/// Expected duration of the next build: the p90 of observed builds once there
/// are enough of them, else the default. Never below 90s (a build cannot
/// provision faster) nor above MAX_WAIT.
fn build_estimate_from(samples: &[Duration]) -> Duration {
    if samples.len() < MIN_SAMPLES_FOR_P90 {
        return DEFAULT_BUILD_ESTIMATE;
    }
    let mut v = samples.to_vec();
    v.sort();
    let idx = ((v.len() as f64) * 0.9).ceil() as usize;
    let p90 = v[idx.saturating_sub(1).min(v.len() - 1)];
    p90.clamp(Duration::from_secs(90), MAX_WAIT)
}

fn current_build_estimate() -> Duration {
    let samples: Vec<Duration> = OBSERVED_BUILDS
        .lock()
        .map(|q| q.iter().copied().collect())
        .unwrap_or_default();
    build_estimate_from(&samples)
}

/// Is `remaining` enough to start a build expected to take `estimate`, while
/// still leaving the post-build buffer?
fn enough_runway(remaining: Duration, estimate: Duration) -> bool {
    remaining >= DEADLINE_BUFFER + estimate
}

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

const MARKER_CODEGEN_END: &str = "CODERHELM_CODEGEN_END exit=";
const MARKER_CODEGEN_UPLOADED: &str = "CODERHELM_CODEGEN_UPLOADED";
/// Caps on captured codegen output (regenerated files are large but bounded).
const MAX_CODEGEN_FILES: usize = 40;
const MAX_CODEGEN_FILE_BYTES: u64 = 5 * 1024 * 1024;

/// Result of a sandbox codegen run — the files the repo's codegen CHANGED,
/// captured for the worker to commit back to the branch (making the sandbox
/// bidirectional so the agent can regenerate generated files).
pub struct CodegenOutcome {
    /// Codegen actually ran (marker present).
    pub ran: bool,
    /// Codegen exited 0.
    pub passed: bool,
    /// (path, content) for each file codegen created or modified.
    pub changed: Vec<(String, String)>,
    /// Tail of the codegen output.
    pub output: String,
}

impl CodegenOutcome {
    fn not_run(reason: impl Into<String>) -> Self {
        Self {
            ran: false,
            passed: false,
            changed: Vec::new(),
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

    /// True if there is enough runway before `deadline` to run a build to
    /// completion: the realistic build time PLUS the post-build buffer.
    /// (Previously only buffer + one poll interval, so a build started with
    /// ~81s left and was killed at the deadline.)
    pub fn has_time(&self, deadline: Option<Instant>) -> bool {
        match deadline {
            Some(dl) => {
                let remaining = dl.saturating_duration_since(Instant::now());
                let estimate = current_build_estimate();
                let ok = enough_runway(remaining, estimate);
                if !ok {
                    info!(
                        remaining_s = remaining.as_secs(),
                        estimate_s = estimate.as_secs(),
                        "sandbox: not enough runway for a build, skipping"
                    );
                }
                ok
            }
            None => true,
        }
    }

    /// The S3 key holding the source tarball, uploading it only when this run
    /// hasn't already uploaded that commit. `fetch` downloads the tarball and
    /// is called only on a miss. A repo's tarball runs to tens of MB, and
    /// re-uploading it on every check of an unchanged head cost minutes per
    /// check; the same commit is the same bytes, so one upload serves every
    /// check (and codegen run) of it in the run, continuations included. The
    /// bucket's 1-day lifecycle removes it.
    pub async fn ensure_source<F, Fut>(
        &self,
        run_id: &str,
        head_sha: &str,
        fetch: F,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce() -> Fut,
        Fut:
            std::future::Future<Output = Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let key = source_key(run_id, head_sha);
        if self
            .s3
            .head_object()
            .bucket(self.bucket)
            .key(&key)
            .send()
            .await
            .is_ok()
        {
            info!(key = %key, "sandbox: source for this commit already uploaded — reusing");
            return Ok(key);
        }
        let tarball = fetch().await?;
        let tarball_len = tarball.len();
        self.s3
            .put_object()
            .bucket(self.bucket)
            .key(&key)
            .body(aws_sdk_s3::primitives::ByteStream::from(tarball))
            .content_type("application/gzip")
            .send()
            .await
            .map_err(|e| format!("sandbox tarball upload failed: {e}"))?;
        info!(key = %key, bytes = tarball_len, "sandbox: uploaded source tarball");
        Ok(key)
    }

    /// Run the checks in CodeBuild against the source at `source_key` (from
    /// [`Self::ensure_source`]) and return the real output. Never returns Err
    /// for an ordinary check failure — a failing build is
    /// `CheckOutcome { ran: true, passed: false, .. }`. Err is reserved for
    /// hard faults the caller should log and shrug off.
    pub async fn run_checks(
        &self,
        source_key: &str,
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
        // The source stays for the run's next check of the same commit.
        self.build_and_read(source_key, checks_cmd, node_version, deadline)
            .await
    }

    /// Run the repo's codegen in the sandbox and return the files it CHANGED,
    /// for the caller to commit back to the branch. Never Err for an ordinary
    /// codegen failure — that's `CodegenOutcome { ran: true, passed: false }`.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_codegen(
        &self,
        run_id: &str,
        attempt: usize,
        source_key: &str,
        codegen_cmd: &str,
        node_version: Option<&str>,
        extra_env: &[(String, String)],
        deadline: Option<Instant>,
    ) -> Result<CodegenOutcome, Box<dyn std::error::Error + Send + Sync>> {
        if !self.is_enabled() {
            return Ok(CodegenOutcome::not_run("sandbox not configured"));
        }
        if !self.has_time(deadline) {
            return Ok(CodegenOutcome::not_run(
                "not enough time left before the pass deadline to run codegen",
            ));
        }
        let out_key = format!("codegen-out/{run_id}/{attempt}.tgz");
        let outcome = self
            .codegen_build_and_capture(
                source_key,
                &out_key,
                codegen_cmd,
                node_version,
                extra_env,
                deadline,
            )
            .await;
        // The output is transient; the source stays for the run's next
        // check of the same commit.
        let _ = self
            .s3
            .delete_object()
            .bucket(self.bucket)
            .key(&out_key)
            .send()
            .await;
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    async fn codegen_build_and_capture(
        &self,
        key: &str,
        out_key: &str,
        codegen_cmd: &str,
        node_version: Option<&str>,
        extra_env: &[(String, String)],
        deadline: Option<Instant>,
    ) -> Result<CodegenOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let mut env = vec![
            env_var("SANDBOX_BUCKET", self.bucket)?,
            env_var("SANDBOX_KEY", key)?,
            env_var("CODEGEN_CMD", codegen_cmd)?,
            env_var("CODEGEN_OUT_KEY", out_key)?,
        ];
        if let Some(v) = node_version {
            env.push(env_var("NODE_VERSION", v)?);
        }
        // Repo's public codegen env (e.g. Sanity projectId/dataset). Reserved
        // build vars are protected — an env entry can't hijack the tarball or
        // output location.
        const RESERVED: [&str; 5] = [
            "SANDBOX_BUCKET",
            "SANDBOX_KEY",
            "CODEGEN_CMD",
            "CODEGEN_OUT_KEY",
            "NODE_VERSION",
        ];
        for (k, v) in extra_env {
            if RESERVED.contains(&k.as_str()) {
                continue;
            }
            env.push(env_var(k, v)?);
        }
        let started = self
            .codebuild
            .start_build()
            .project_name(self.project)
            .set_environment_variables_override(Some(env))
            .send()
            .await
            .map_err(|e| format!("codebuild start_build (codegen) failed: {e}"))?;
        let build_id = started
            .build_value()
            .and_then(|b| b.id())
            .ok_or("codebuild returned no build id")?
            .to_string();
        info!(build_id = %build_id, "sandbox: codegen build started");

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
            let build = builds.builds().first().ok_or("no build record")?;
            let status = build.build_status();
            if !matches!(status, Some(StatusType::InProgress)) {
                let (group, stream) = build
                    .logs()
                    .and_then(|l| Some((l.group_name()?.to_string(), l.stream_name()?.to_string())))
                    .unzip_or_default();
                let build_succeeded = matches!(status, Some(StatusType::Succeeded));
                return self
                    .read_codegen_outcome(&group, &stream, out_key, build_succeeded)
                    .await;
            }
            if Instant::now() >= poll_until {
                warn!(build_id = %build_id, "sandbox: codegen deadline reached, stopping");
                let _ = self.codebuild.stop_build().id(&build_id).send().await;
                return Ok(CodegenOutcome::not_run("codegen exceeded the time budget"));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn read_codegen_outcome(
        &self,
        group: &str,
        stream: &str,
        out_key: &str,
        build_succeeded: bool,
    ) -> Result<CodegenOutcome, Box<dyn std::error::Error + Send + Sync>> {
        // Source of truth for "codegen produced changes" is the S3 artifact the
        // build uploads on success — NOT the CloudWatch log marker. The build
        // uploads out.tgz mid-run (before it finishes) and S3 is read-after-write
        // consistent, so the artifact is reliably there the moment the build is
        // done. The log marker, by contrast, lags CloudWatch delivery by several
        // seconds; reading it too early made the worker report "no result marker"
        // for builds that had ALREADY succeeded and uploaded the regenerated
        // files, so the agent re-ran the (expensive, always-succeeding) codegen
        // build over and over until the pass timed out. Check S3 first.
        let changed = self.download_changed(out_key).await.unwrap_or_default();
        if !changed.is_empty() {
            let output = self
                .fetch_log(group, stream)
                .await
                .map(|l| common::tail_str(&l, MAX_OUTPUT_CHARS).to_string())
                .unwrap_or_default();
            return Ok(CodegenOutcome {
                ran: true,
                passed: true,
                changed,
                output,
            });
        }

        // No artifact => codegen either passed with nothing to regenerate, or
        // failed. Read the log for the end marker, briefly retrying to let
        // CloudWatch flush; fall back to the CodeBuild build status if the marker
        // never lands (so a delivery lag is never mistaken for a codegen failure).
        if group.is_empty() {
            return Ok(CodegenOutcome::not_run(
                "codegen produced no logs (infra fault)",
            ));
        }
        let mut full = String::new();
        let mut code = None;
        for attempt in 0..4 {
            full = self.fetch_log(group, stream).await.unwrap_or_default();
            code = parse_marker_exit(&full, MARKER_CODEGEN_END);
            if code.is_some() {
                break;
            }
            if attempt < 3 {
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
        let output = common::tail_str(&full, MAX_OUTPUT_CHARS).to_string();
        // If the build logged an upload but the first artifact read came back empty
        // (a transient S3 read), try once more — those changes are real and must
        // not be dropped as "nothing to regenerate".
        if full.contains(MARKER_CODEGEN_UPLOADED) {
            let retry = self.download_changed(out_key).await.unwrap_or_default();
            if !retry.is_empty() {
                return Ok(CodegenOutcome {
                    ran: true,
                    passed: true,
                    changed: retry,
                    output,
                });
            }
        }
        // Marker exit code is authoritative for pass/fail when present; otherwise
        // trust the build status. Either way `changed` is empty here.
        let passed = match code {
            Some(c) => c == 0,
            None => build_succeeded,
        };
        Ok(CodegenOutcome {
            ran: true,
            passed,
            changed: Vec::new(),
            output,
        })
    }

    /// Download the captured tar.gz of changed files and read them into
    /// (path, content) pairs, bounded in count and per-file size.
    async fn download_changed(
        &self,
        out_key: &str,
    ) -> Result<Vec<(String, String)>, Box<dyn std::error::Error + Send + Sync>> {
        let obj = self
            .s3
            .get_object()
            .bucket(self.bucket)
            .key(out_key)
            .send()
            .await
            .map_err(|e| format!("codegen output download failed: {e}"))?;
        let bytes = obj.body.collect().await?.into_bytes();
        let gz = flate2::read::GzDecoder::new(&bytes[..]);
        let mut ar = tar::Archive::new(gz);
        let mut out = Vec::new();
        for entry in ar.entries()? {
            let mut entry = entry?;
            if entry.header().entry_type() != tar::EntryType::Regular {
                continue;
            }
            if entry.size() > MAX_CODEGEN_FILE_BYTES {
                warn!("codegen: skipping oversized changed file");
                continue;
            }
            let path = entry.path()?.to_string_lossy().replace('\\', "/");
            let mut content = String::new();
            use std::io::Read;
            if entry.read_to_string(&mut content).is_err() {
                continue; // skip binary/non-utf8
            }
            out.push((path, content));
            if out.len() >= MAX_CODEGEN_FILES {
                break;
            }
        }
        Ok(out)
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
        let build_started = Instant::now();

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
                let outcome = self.read_outcome(&group, &stream, &status_str).await;
                if matches!(&outcome, Ok(o) if o.ran) {
                    let took = build_started.elapsed();
                    record_build_duration(took);
                    info!(build_id = %build_id, secs = took.as_secs(), "sandbox: build finished");
                }
                return outcome;
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

/// The S3 key of a run's source tarball at one commit. Keyed by the commit,
/// so a check of an unchanged head finds the upload already there and a new
/// commit never reuses an older one's source.
pub fn source_key(run_id: &str, head_sha: &str) -> String {
    let safe = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '-'
                }
            })
            .collect::<String>()
    };
    format!("sandbox/{}/src-{}.tgz", safe(run_id), safe(head_sha))
}

/// Parse the exit code that follows a given end-marker.
fn parse_marker_exit(log: &str, marker: &str) -> Option<i32> {
    let idx = log.find(marker)?;
    let rest = &log[idx + marker.len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Parse the exit code from the checks end marker.
fn parse_exit(log: &str) -> Option<i32> {
    parse_marker_exit(log, MARKER_END)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_commit_is_one_upload_per_run() {
        let a = source_key("01RUN", "abc123");
        assert_eq!(a, source_key("01RUN", "abc123"));
        assert_ne!(a, source_key("01RUN", "def456"));
        assert_ne!(a, source_key("01OTHER", "abc123"));
        assert_eq!(a, "sandbox/01RUN/src-abc123.tgz");
        assert!(!source_key("review-a/b", "x").contains("a/b"));
    }

    #[test]
    fn a_build_needs_its_estimate_plus_the_buffer() {
        let est = DEFAULT_BUILD_ESTIMATE;
        // The production failure: ~81s left, checks take ~150s.
        assert!(!enough_runway(Duration::from_secs(81), est));
        assert!(!enough_runway(est, est), "buffer is still required");
        assert!(enough_runway(est + DEADLINE_BUFFER, est));
    }

    #[test]
    fn estimate_uses_default_until_enough_samples() {
        let few = vec![Duration::from_secs(60); MIN_SAMPLES_FOR_P90 - 1];
        assert_eq!(build_estimate_from(&few), DEFAULT_BUILD_ESTIMATE);
    }

    #[test]
    fn estimate_is_the_p90_clamped() {
        let mut s: Vec<Duration> = (1..=10)
            .map(|i| Duration::from_secs(100 + i * 10))
            .collect();
        s.reverse();
        // 110..=200 → p90 = 190s
        assert_eq!(build_estimate_from(&s), Duration::from_secs(190));
        let fast = vec![Duration::from_secs(20); 10];
        assert_eq!(build_estimate_from(&fast), Duration::from_secs(90));
        let slow = vec![Duration::from_secs(3600); 10];
        assert_eq!(build_estimate_from(&slow), MAX_WAIT);
    }

    #[test]
    fn no_deadline_always_has_time() {
        let c = aws_sdk_codebuild::Client::from_conf(
            aws_sdk_codebuild::Config::builder()
                .behavior_version(aws_sdk_codebuild::config::BehaviorVersion::latest())
                .build(),
        );
        let s3 = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .build(),
        );
        let logs = aws_sdk_cloudwatchlogs::Client::from_conf(
            aws_sdk_cloudwatchlogs::Config::builder()
                .behavior_version(aws_sdk_cloudwatchlogs::config::BehaviorVersion::latest())
                .build(),
        );
        let sb = SandboxClient::new(&c, &s3, &logs, "b", "p");
        assert!(sb.has_time(None));
        assert!(!sb.has_time(Some(Instant::now() + Duration::from_secs(81))));
        assert!(sb.has_time(Some(Instant::now() + Duration::from_secs(600))));
    }
}
