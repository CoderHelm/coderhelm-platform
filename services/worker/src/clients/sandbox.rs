//! Execution sandbox: runs a repo's REAL build/lint/tests in an isolated,
//! credential-free AWS CodeBuild container and returns the actual output, so
//! the agent can iterate against ground truth instead of guessing blind at CI.
//!
//! Flow: the worker (which holds the GitHub token) downloads the branch tarball
//! and uploads it to S3; CodeBuild fetches ONLY that S3 object through a
//! presigned GET URL and runs the pre-derived check commands. The build role
//! has no S3 access at all (no GitHub token, no DynamoDB, no KMS either): the
//! customer's build scripts run with that role, so the only objects a build
//! can touch are the ones whose URLs it was handed — its own input, and for
//! codegen its own output key (presigned PUT).
//! The verdict is read from a log marker the buildspec emits, NOT from the
//! CodeBuild build status — that cleanly separates "checks ran and failed"
//! (build SUCCEEDED, marker exit!=0) from "the sandbox itself broke" (no
//! marker), so a sandbox fault degrades to today's blind flow, never worse.
//!
//! The buildspec that emits the marker lives in infra/lib/worker-stack.ts.

use std::time::{Duration, Instant};
use tracing::{info, warn};

use aws_sdk_codebuild::types::{EnvironmentVariable, EnvironmentVariableType, StatusType};
use aws_sdk_s3::presigning::PresigningConfig;

const MARKER_END: &str = "CODERHELM_CHECKS_END exit=";
/// What a codegen build CodeBuild timed out reports: a tooling failure.
pub const CODEGEN_TIMED_OUT: &str = "the sandbox build timed out (CodeBuild TIMED_OUT)";
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

/// How long the presigned input/output URLs handed to a build stay valid:
/// the CodeBuild project timeout (15 min) plus time for the build to be
/// provisioned. Past this a leaked URL is useless.
const PRESIGNED_URL_TTL: Duration = Duration::from_secs(20 * 60);

/// Build env vars the worker sets; a repo's codegen env can't override them.
const RESERVED_ENV: [&str; 11] = [
    "SANDBOX_GET_URL",
    "SANDBOX_PUT_URL",
    "CHECKS_CMD",
    "CODEGEN_CMD",
    "NODE_VERSION",
    "TERRAFORM_VERSION",
    // The team's network allowlist and its proxy: a repo's env must not
    // widen or replace them.
    "EGRESS_ALLOW",
    "EGRESS_PROXY_B64",
    // Names the buildspec used before presigned URLs; still never settable.
    "SANDBOX_BUCKET",
    "SANDBOX_KEY",
    "CODEGEN_OUT_KEY",
];

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
    /// Tail of the real command output (stdout+stderr), for the agent to
    /// read. When the checks didn't run: why, then the build's last log lines.
    pub output: String,
    /// When the checks didn't run, why, in one clause people read in a PR
    /// (`the sandbox build ended FAILED and printed no result (INSTALL
    /// FAILED: ...)`). Empty when they ran.
    pub reason: String,
    /// A build ran and gave no result (the sandbox broke), as opposed to
    /// none starting for lack of time: what [`NoResults`] counts.
    pub broken: bool,
}

impl CheckOutcome {
    /// Didn't run because nothing was started (no sandbox, no time): not a
    /// fault of the sandbox.
    fn not_run(reason: impl Into<String>) -> Self {
        let reason = reason.into();
        Self {
            ran: false,
            passed: false,
            exit_code: None,
            output: reason.clone(),
            reason,
            broken: false,
        }
    }

    /// A build ran and gave no result: `reason`, then `log_tail` (the last
    /// lines of its log) for whoever reads the detail.
    fn broke(reason: impl Into<String>, log_tail: &str) -> Self {
        let reason = reason.into();
        let output = if log_tail.trim().is_empty() {
            reason.clone()
        } else {
            format!("{reason}\n\nLast lines of the sandbox build log:\n```\n{log_tail}\n```")
        };
        Self {
            ran: false,
            passed: false,
            exit_code: None,
            output,
            reason,
            broken: true,
        }
    }
}

/// How many sandbox builds of one run may end without a result before the
/// run stops starting more: the same setup failing twice fails the same way
/// a third time, and each try costs minutes of the pass.
pub const MAX_NO_RESULT: u32 = 2;

/// Sandbox builds that ended without a result, per run, in this process: a
/// later pass of the same run (a fix round) reads the count too.
pub struct NoResults;

type NoResultEntry = (String, u32, String);

static NO_RESULTS: std::sync::Mutex<Vec<NoResultEntry>> = std::sync::Mutex::new(Vec::new());

impl NoResults {
    /// Count one more build of `key` that gave no result, with its reason;
    /// returns how many there have been.
    pub fn record(key: &str, reason: &str) -> u32 {
        let Ok(mut v) = NO_RESULTS.lock() else {
            return 0;
        };
        if let Some(e) = v.iter_mut().find(|e| e.0 == key) {
            e.1 += 1;
            e.2 = reason.to_string();
            return e.1;
        }
        if v.len() >= 200 {
            v.remove(0);
        }
        v.push((key.to_string(), 1, reason.to_string()));
        1
    }

    /// A build of `key` gave a result: the setup works.
    pub fn clear(key: &str) {
        if let Ok(mut v) = NO_RESULTS.lock() {
            v.retain(|e| e.0 != key);
        }
    }

    /// How many builds of `key` gave no result and the last reason, once
    /// that has reached [`MAX_NO_RESULT`].
    pub fn given_up(key: &str) -> Option<(u32, String)> {
        let v = NO_RESULTS.lock().ok()?;
        v.iter()
            .find(|e| e.0 == key && e.1 >= MAX_NO_RESULT)
            .map(|e| (e.1, e.2.clone()))
    }
}

/// Builds of a run whose network call the allowlist refused, in this
/// process: a later step of the same run that can't start a build of its
/// own (out of time) still reports the refused host, not the change.
pub struct EgressBlocks;

type EgressBlockEntry = (String, common::stop_rules::tooling::ToolingFailure);

static EGRESS_BLOCKS: std::sync::Mutex<Vec<EgressBlockEntry>> = std::sync::Mutex::new(Vec::new());

impl EgressBlocks {
    /// Keep the run's latest refused build.
    pub fn record(run_id: &str, failure: &common::stop_rules::tooling::ToolingFailure) {
        let Ok(mut v) = EGRESS_BLOCKS.lock() else {
            return;
        };
        v.retain(|e| e.0 != run_id);
        if v.len() >= 200 {
            v.remove(0);
        }
        v.push((run_id.to_string(), failure.clone()));
    }

    /// The run's latest refused build, if any.
    pub fn last(run_id: &str) -> Option<common::stop_rules::tooling::ToolingFailure> {
        let v = EGRESS_BLOCKS.lock().ok()?;
        v.iter().find(|e| e.0 == run_id).map(|e| e.1.clone())
    }

    /// A build of the run ran cleanly: an earlier refusal no longer stands.
    pub fn clear(run_id: &str) {
        if let Ok(mut v) = EGRESS_BLOCKS.lock() {
            v.retain(|e| e.0 != run_id);
        }
    }
}

const MARKER_CODEGEN_END: &str = "CODERHELM_CODEGEN_END exit=";
const MARKER_CODEGEN_UPLOADED: &str = "===CODERHELM_CODEGEN_UPLOADED===";
const MARKER_CODEGEN_UPLOAD_FAILED: &str = "===CODERHELM_CODEGEN_UPLOAD_FAILED===";
/// Caps on captured codegen output (regenerated files are large but
/// bounded). A generated types file for a large CMS schema passed the old
/// 5 MB cap and was dropped without a word: the run committed one of the two
/// files the generator changed and called it regenerated. A file past a cap
/// is now named in `CodegenOutcome::dropped`, never left out silently. The
/// size cap stays well under GitHub's 100 MB blob limit and the worker's
/// memory.
const MAX_CODEGEN_FILES: usize = 200;
const MAX_CODEGEN_FILE_BYTES: u64 = 50 * 1024 * 1024;

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
    /// Files codegen changed that couldn't be brought back (too large, not
    /// text, past the file cap), each with why. A run with any is not a
    /// complete regeneration.
    pub dropped: Vec<String>,
    /// Tail of the codegen output.
    pub output: String,
}

impl CodegenOutcome {
    fn not_run(reason: impl Into<String>) -> Self {
        Self {
            ran: false,
            passed: false,
            changed: Vec::new(),
            dropped: Vec::new(),
            output: reason.into(),
        }
    }
}

/// The files of a codegen build's changed-files archive: the text files to
/// commit, and the ones left out with why.
#[derive(Debug, Default, PartialEq)]
pub struct Captured {
    pub files: Vec<(String, String)>,
    pub dropped: Vec<String>,
}

/// Reads a changed-files archive (tar.gz, paths from the repository root).
pub fn read_changed_tar(
    bytes: &[u8],
    max_files: usize,
    max_bytes: u64,
) -> Result<Captured, Box<dyn std::error::Error + Send + Sync>> {
    let gz = flate2::read::GzDecoder::new(bytes);
    let mut ar = tar::Archive::new(gz);
    let mut out = Captured::default();
    for entry in ar.entries()? {
        let mut entry = entry?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let path = entry.path()?.to_string_lossy().replace('\\', "/");
        let path = path.trim_start_matches("./").to_string();
        if out.files.len() >= max_files {
            out.dropped
                .push(format!("`{path}` (more than {max_files} changed files)"));
            continue;
        }
        if entry.size() > max_bytes {
            warn!(path = %path, bytes = entry.size(), "codegen: changed file over the size cap");
            out.dropped.push(format!(
                "`{path}` ({} MB, over the {} MB cap)",
                entry.size() / (1024 * 1024),
                max_bytes / (1024 * 1024)
            ));
            continue;
        }
        let mut content = String::new();
        use std::io::Read;
        if entry.read_to_string(&mut content).is_err() {
            out.dropped.push(format!("`{path}` (not a text file)"));
            continue;
        }
        out.files.push((path, content));
    }
    Ok(out)
}

/// Thin driver over CodeBuild + S3 + CloudWatch Logs. Holds only borrows from
/// `WorkerState`; construct it per pass.
pub struct SandboxClient<'a> {
    codebuild: &'a aws_sdk_codebuild::Client,
    s3: &'a aws_sdk_s3::Client,
    logs: &'a aws_sdk_cloudwatchlogs::Client,
    bucket: &'a str,
    project: &'a str,
    /// The team's network allowlist (comma separated), when its sandbox
    /// network is limited. The build starts the allowlist proxy with it.
    egress: Option<String>,
    /// Hosts the repository's generators reach (a CMS API, a remote schema),
    /// added to the list for builds that run a generator: every codegen
    /// build, and a check build whose command runs one of `generator_cmds`.
    generator_hosts: Vec<String>,
    /// The commands that run the repository's generators.
    generator_cmds: Vec<String>,
}

/// The allowlist one build gets: the team's list (None: the network is
/// open, the team's choice), plus the generator hosts when the build runs a
/// generator. Only those hosts are added; a limited network stays limited.
pub fn build_allowlist(
    team: Option<&str>,
    generator_hosts: &[String],
    runs_generator: bool,
) -> Option<String> {
    let team = team?;
    let mut list: Vec<String> = team
        .split(',')
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .map(str::to_string)
        .collect();
    if runs_generator {
        for h in generator_hosts {
            if !common::agent_policy::sandbox::domain_allowed(&list, h) {
                list.push(h.clone());
            }
        }
    }
    Some(list.join(","))
}

/// Whether a check command runs one of the repository's generators.
pub fn runs_generator(checks_cmd: &str, generator_cmds: &[String]) -> bool {
    generator_cmds
        .iter()
        .any(|g| !g.trim().is_empty() && checks_cmd.contains(g.trim()))
}

/// A log line that is exactly `marker` (the build prints its markers on
/// lines of their own; the echoed buildspec holds them inside an `echo`).
fn printed(log: &str, marker: &str) -> bool {
    log.lines().any(|l| l.trim() == marker)
}

/// The proxy the build starts when the network is limited.
const EGRESS_PROXY: &str = include_str!("../../../common/src/agent_policy/egress_proxy.py");

impl<'a> SandboxClient<'a> {
    /// Limits the build's network to these domains (None: open, the team's
    /// choice).
    pub fn with_egress(mut self, allow: Option<Vec<String>>) -> Self {
        self.egress = allow.map(|a| a.join(","));
        self
    }

    /// The hosts the repository's generators need and the commands that run
    /// them: codegen builds reach those hosts too, and so does a check build
    /// whose command runs a generator. Nothing changes when the network is
    /// open.
    pub fn with_generator_hosts(mut self, hosts: Vec<String>, cmds: Vec<String>) -> Self {
        self.generator_hosts = hosts;
        self.generator_cmds = cmds;
        self
    }

    /// The environment that turns the allowlist proxy on, when limited.
    /// `cmd` is a check build's command; None for a codegen build.
    fn egress_env(
        &self,
        cmd: Option<&str>,
    ) -> Result<Vec<EnvironmentVariable>, Box<dyn std::error::Error + Send + Sync>> {
        use base64::Engine;
        let gen = cmd.is_none_or(|c| runs_generator(c, &self.generator_cmds));
        let Some(allow) = build_allowlist(self.egress.as_deref(), &self.generator_hosts, gen)
        else {
            return Ok(Vec::new());
        };
        if gen && !self.generator_hosts.is_empty() {
            info!(hosts = ?self.generator_hosts, "sandbox: generator hosts allowed for this build");
        }
        Ok(vec![
            env_var("EGRESS_ALLOW", &allow)?,
            env_var(
                "EGRESS_PROXY_B64",
                &base64::engine::general_purpose::STANDARD.encode(EGRESS_PROXY),
            )?,
        ])
    }

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
            egress: None,
            generator_hosts: Vec::new(),
            generator_cmds: Vec::new(),
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
        terraform_version: Option<&str>,
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
        self.build_and_read(
            source_key,
            checks_cmd,
            node_version,
            terraform_version,
            deadline,
        )
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
            env_var("SANDBOX_GET_URL", &self.presigned_get(key).await?)?,
            env_var("SANDBOX_PUT_URL", &self.presigned_put(out_key).await?)?,
            env_var("CODEGEN_CMD", codegen_cmd)?,
        ];
        env.extend(self.egress_env(None)?);
        if let Some(v) = node_version {
            env.push(env_var("NODE_VERSION", v)?);
        }
        // Repo's public codegen env (e.g. Sanity projectId/dataset). Reserved
        // build vars are protected — an env entry can't hijack the tarball or
        // output location.
        for (k, v) in extra_env {
            if is_reserved_env(k) {
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
            if matches!(status, Some(StatusType::TimedOut)) {
                // CodeBuild's own build timeout: the sandbox, not the change
                // and not this step's time.
                return Ok(CodegenOutcome::not_run(CODEGEN_TIMED_OUT));
            }
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
        let captured = self.download_changed(out_key).await.unwrap_or_default();
        if !captured.files.is_empty() || !captured.dropped.is_empty() {
            let output = self
                .fetch_log(group, stream)
                .await
                .map(|l| common::tail_str(&l, MAX_OUTPUT_CHARS).to_string())
                .unwrap_or_default();
            return Ok(CodegenOutcome {
                ran: true,
                passed: true,
                changed: captured.files,
                dropped: captured.dropped,
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
        // Codegen changed files but could not hand them back: that is not
        // "nothing to regenerate", and must not be reported as a clean pass.
        if printed(&full, MARKER_CODEGEN_UPLOAD_FAILED) {
            return Ok(CodegenOutcome::not_run(format!(
                "codegen ran but its changed files could not be uploaded\n{output}"
            )));
        }
        // If the build logged an upload but the first artifact read came back empty
        // (a transient S3 read), try once more — those changes are real and must
        // not be dropped as "nothing to regenerate".
        if printed(&full, MARKER_CODEGEN_UPLOADED) {
            let retry = self.download_changed(out_key).await.unwrap_or_default();
            if !retry.files.is_empty() || !retry.dropped.is_empty() {
                return Ok(CodegenOutcome {
                    ran: true,
                    passed: true,
                    changed: retry.files,
                    dropped: retry.dropped,
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
            dropped: Vec::new(),
            output,
        })
    }

    /// Download the captured tar.gz of changed files and read them into
    /// (path, content) pairs, bounded in count and per-file size; what the
    /// bounds leave out is named.
    async fn download_changed(
        &self,
        out_key: &str,
    ) -> Result<Captured, Box<dyn std::error::Error + Send + Sync>> {
        let obj = self
            .s3
            .get_object()
            .bucket(self.bucket)
            .key(out_key)
            .send()
            .await
            .map_err(|e| format!("codegen output download failed: {e}"))?;
        let bytes = obj.body.collect().await?.into_bytes();
        read_changed_tar(&bytes, MAX_CODEGEN_FILES, MAX_CODEGEN_FILE_BYTES)
    }

    async fn build_and_read(
        &self,
        key: &str,
        checks_cmd: &str,
        node_version: Option<&str>,
        terraform_version: Option<&str>,
        deadline: Option<Instant>,
    ) -> Result<CheckOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let mut env = vec![
            env_var("SANDBOX_GET_URL", &self.presigned_get(key).await?)?,
            env_var("CHECKS_CMD", checks_cmd)?,
        ];
        env.extend(self.egress_env(Some(checks_cmd))?);
        // The buildspec switches Node to this via `n` before running checks, so
        // "green in the sandbox" tracks the repo's real toolchain (it defaults
        // to 20 when unset). Prevents false REDs like a Next.js repo needing
        // >=20.9 failing on the image's default Node 18.
        if let Some(v) = node_version {
            env.push(env_var("NODE_VERSION", v)?);
        }
        // The buildspec installs this exact Terraform release (signature and
        // checksum verified) before the checks, for a repository with `.tf`
        // files. Only an exact `x.y.z` reaches the shell.
        if let Some(v) = terraform_version.filter(|v| crate::passes::terraform::is_exact(v)) {
            env.push(env_var("TERRAFORM_VERSION", v)?);
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
                let phases = phase_notes(build);
                let outcome = self
                    .read_outcome(&group, &stream, &status_str, &phases)
                    .await;
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
                return Ok(CheckOutcome::not_run(format!(
                    "the sandbox build was stopped after {}s because the pass's time ran out",
                    build_started.elapsed().as_secs()
                )));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn read_outcome(
        &self,
        group: &str,
        stream: &str,
        status_str: &str,
        phases: &[String],
    ) -> Result<CheckOutcome, Box<dyn std::error::Error + Send + Sync>> {
        if group.is_empty() || stream.is_empty() {
            return Ok(CheckOutcome::broke(
                no_result_reason(status_str, phases, "wrote no log"),
                "",
            ));
        }
        let full = self.fetch_log(group, stream).await?;
        Ok(outcome_from_log(&full, status_str, phases))
    }

    /// A short-lived URL that downloads exactly `key` and nothing else.
    async fn presigned_get(
        &self,
        key: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let req = self
            .s3
            .get_object()
            .bucket(self.bucket)
            .key(key)
            .presigned(PresigningConfig::expires_in(PRESIGNED_URL_TTL)?)
            .await
            .map_err(|e| format!("sandbox: presigning the input URL failed: {e}"))?;
        Ok(req.uri().to_string())
    }

    /// A short-lived URL that uploads to exactly `key` and nothing else.
    async fn presigned_put(
        &self,
        key: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let req = self
            .s3
            .put_object()
            .bucket(self.bucket)
            .key(key)
            .presigned(PresigningConfig::expires_in(PRESIGNED_URL_TTL)?)
            .await
            .map_err(|e| format!("sandbox: presigning the output URL failed: {e}"))?;
        Ok(req.uri().to_string())
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

fn is_reserved_env(name: &str) -> bool {
    RESERVED_ENV.contains(&name)
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

/// The exit code a build printed after `marker`: the last line where the
/// marker is followed by a number. CodeBuild echoes the whole buildspec
/// command into the log before running it (`Running command ... echo
/// "===CODERHELM_CHECKS_END exit=$CODE==="`), so the first occurrence of the
/// marker is the script's own text, never a result. Reading that one made
/// every check build report "no result marker" though its checks had run.
fn parse_marker_exit(log: &str, marker: &str) -> Option<i32> {
    log.lines().rev().find_map(|line| {
        let idx = line.rfind(marker)?;
        let digits: String = line[idx + marker.len()..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        digits.parse().ok()
    })
}

/// Parse the exit code from the checks end marker.
fn parse_exit(log: &str) -> Option<i32> {
    parse_marker_exit(log, MARKER_END)
}

/// Slice the log to just the check window (from the start marker the checks
/// printed), so we hand the agent the command output and not the sandbox's
/// own plumbing. The marker counts only as a line of its own: the echoed
/// buildspec holds its text inside an `echo`.
fn extract_window(log: &str) -> String {
    let mut at = 0;
    let mut start = None;
    for line in log.split_inclusive('\n') {
        if line.trim() == MARKER_START {
            start = Some(at);
        }
        at += line.len();
    }
    match start {
        Some(s) => log[s..].to_string(),
        None => log.to_string(),
    }
}

/// What a finished check build came to, read from its whole log.
fn outcome_from_log(full: &str, status_str: &str, phases: &[String]) -> CheckOutcome {
    let exit = parse_exit(full);
    let window = extract_window(full);
    let output = common::tail_str(&window, MAX_OUTPUT_CHARS).to_string();
    match exit {
        // A kill signal is a SANDBOX RESOURCE problem, not the agent's code:
        // 137 = SIGKILL (OOM), 143 = SIGTERM, 124 = coreutils `timeout`.
        // Reporting these as a code RED sent the agent chasing a "failure"
        // that was really the container running out of memory. Treat as
        // unverified -> blind fallback.
        Some(code) if code == 137 || code == 143 || code == 124 => CheckOutcome::broke(
            format!(
                "the sandbox checks were killed (exit {code}: out of memory or a timeout), a \
                 sandbox limit rather than a code failure"
            ),
            &log_tail(full, LOG_TAIL_LINES),
        ),
        Some(code) => CheckOutcome {
            ran: true,
            passed: code == 0,
            exit_code: Some(code),
            output,
            reason: String::new(),
            broken: false,
        },
        // A finished build with no result line: the check commands never
        // reached the end (download or extract failed, image error).
        None => CheckOutcome::broke(
            no_result_reason(status_str, phases, "printed no result"),
            &log_tail(full, LOG_TAIL_LINES),
        ),
    }
}

/// How many of a build's last log lines a no-result report carries.
const LOG_TAIL_LINES: usize = 15;

/// Why a build gave no result, in one clause: its status, then each phase
/// that didn't succeed with CodeBuild's own message for it.
fn no_result_reason(status: &str, phases: &[String], missing: &str) -> String {
    let status = if status.is_empty() { "UNKNOWN" } else { status };
    let mut out = format!("the sandbox build ended {status} and {missing}");
    if !phases.is_empty() {
        out.push_str(&format!(" ({})", phases.join("; ")));
    }
    out
}

/// The phases of a build that didn't succeed, each with CodeBuild's own
/// message (`INSTALL FAILED: COMMAND_EXECUTION_ERROR ...`).
fn phase_notes(build: &aws_sdk_codebuild::types::Build) -> Vec<String> {
    build
        .phases()
        .iter()
        .filter(|p| {
            p.phase_status()
                .is_some_and(|s| !matches!(s, StatusType::Succeeded))
        })
        .map(|p| {
            let name = p.phase_type().map(|t| t.as_str()).unwrap_or("phase");
            let status = p.phase_status().map(|s| s.as_str()).unwrap_or("");
            let msg = p
                .contexts()
                .iter()
                .map(|c| {
                    [c.status_code(), c.message()]
                        .into_iter()
                        .flatten()
                        .filter(|s| !s.trim().is_empty())
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .filter(|m| !m.is_empty())
                .collect::<Vec<_>>()
                .join(" ");
            let note = if msg.is_empty() {
                format!("{name} {status}")
            } else {
                format!("{name} {status}: {msg}")
            };
            common::truncate_str(&note, 300).to_string()
        })
        .collect()
}

/// The last `n` lines of a build log that say something: CodeBuild's
/// routine report and artifact lines are left out.
fn log_tail(log: &str, n: usize) -> String {
    const ROUTINE: [&str; 6] = [
        "auto-discover",
        "Expanding",
        "Assembling file list",
        "Phase context status code:  Message:",
        "Entering phase",
        "Phase complete: UPLOAD_ARTIFACTS",
    ];
    let lines: Vec<&str> = log
        .lines()
        .filter(|l| !l.trim().is_empty() && !ROUTINE.iter().any(|r| l.contains(r)))
        .collect();
    lines[lines.len().saturating_sub(n)..]
        .iter()
        .map(|l| common::truncate_str(l, 300))
        .collect::<Vec<_>>()
        .join("\n")
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

    /// A check build's log as CodeBuild writes it: the whole buildspec
    /// command echoed first (marker text inside `echo`), then the checks.
    fn build_log(result: &str) -> String {
        format!(
            "[Container] 2026/10/22 20:38:58.014835 Running command set +e\n\
             curl -fsS --retry 3 -o /tmp/src.tgz \"$SANDBOX_GET_URL\"\n\
             if [ -n \"$CODEGEN_CMD\" ]; then\n\
             \x20 echo \"===CODERHELM_CODEGEN_START===\"; node --version\n\
             \x20 echo \"===CODERHELM_CODEGEN_END exit=$CODE===\"\n\
             else\n\
             \x20 echo \"===CODERHELM_CHECKS_START===\"; node --version\n\
             \x20 (eval \"$CHECKS_CMD\"); CODE=$?\n\
             \x20 echo \"===CODERHELM_CHECKS_END exit=$CODE===\"\n\
             fi\n\
             download: s3://bucket/sandbox/RUN/src-abc.tgz to ../tmp/src.tgz\n\
             {result}\
             [Container] 2026/10/22 20:41:07.549332 Phase complete: BUILD State: SUCCEEDED\n\
             [Container] 2026/10/22 20:41:07.549352 Phase context status code:  Message:\n\
             [Container] 2026/10/22 20:41:07.651953 No matching auto-discover report paths found\n"
        )
    }

    #[test]
    fn the_result_is_read_from_what_ran_not_the_echoed_script() {
        // The incident: every build printed its result, and the worker read
        // the echoed `exit=$CODE` first and reported "no result marker".
        let log = build_log(
            "===CODERHELM_CHECKS_START===\nv20.20.2\n=== CoderHelm check summary ===\n\
             CODERHELM_STEP 1 0\n===CODERHELM_CHECKS_END exit=0===\n",
        );
        assert_eq!(parse_exit(&log), Some(0));
        let o = outcome_from_log(&log, "SUCCEEDED", &[]);
        assert!(o.ran && o.passed && !o.broken);
        assert_eq!(o.exit_code, Some(0));
        // The agent reads the checks' output, not the echoed script.
        assert!(o
            .output
            .starts_with("===CODERHELM_CHECKS_START===\nv20.20.2"));
        assert!(!o.output.contains("eval"));

        let red = build_log(
            "===CODERHELM_CHECKS_START===\nerror TS2322\n===CODERHELM_CHECKS_END exit=2===\n",
        );
        let o = outcome_from_log(&red, "SUCCEEDED", &[]);
        assert!(o.ran && !o.passed);
        assert_eq!(o.exit_code, Some(2));
        // Codegen reads its marker the same way.
        let gen = build_log("===CODERHELM_CODEGEN_END exit=0===\n");
        assert_eq!(parse_marker_exit(&gen, MARKER_CODEGEN_END), Some(0));
    }

    #[test]
    fn the_upload_markers_count_only_when_printed_not_echoed() {
        // The incident: the echoed script holds the upload-failed marker,
        // so every codegen build without an artifact read "could not be
        // uploaded" and the generator's own failure was never seen.
        let echoed = format!(
            "{}===CODERHELM_CODEGEN_END exit=1===\n",
            build_log("")
                .replace(
                    "fi\ndownload:",
                    "  echo \"===CODERHELM_CODEGEN_UPLOADED===\"; else echo \"===CODERHELM_CODEGEN_UPLOAD_FAILED===\"; fi\nfi\ndownload:",
                )
        );
        assert!(echoed.contains(MARKER_CODEGEN_UPLOAD_FAILED));
        assert!(!printed(&echoed, MARKER_CODEGEN_UPLOAD_FAILED));
        assert!(!printed(&echoed, MARKER_CODEGEN_UPLOADED));
        let real = build_log("===CODERHELM_CODEGEN_UPLOAD_FAILED===\n");
        assert!(printed(&real, MARKER_CODEGEN_UPLOAD_FAILED));
    }

    #[test]
    fn generator_hosts_join_the_team_list_only_for_builds_that_run_a_generator() {
        let team = "registry.npmjs.org,github.com";
        let hosts = vec!["api.sanity.io".to_string(), "api.github.com".to_string()];
        assert_eq!(
            build_allowlist(Some(team), &hosts, true).as_deref(),
            // api.github.com is already covered by github.com.
            Some("registry.npmjs.org,github.com,api.sanity.io")
        );
        assert_eq!(
            build_allowlist(Some(team), &hosts, false).as_deref(),
            Some(team)
        );
        // An open network stays open; nothing is limited on the team's behalf.
        assert_eq!(build_allowlist(None, &hosts, true), None);
        let cmds = vec!["pnpm run typegen".to_string()];
        assert!(runs_generator(
            "cd apps/studio && pnpm run typegen && pnpm run lint",
            &cmds
        ));
        assert!(!runs_generator("pnpm run lint", &cmds));
        assert!(!runs_generator("pnpm run lint", &["  ".to_string()]));
    }

    #[test]
    fn a_refused_build_is_kept_for_the_run_until_one_runs_clean() {
        let run = "test-run-egress-blocks";
        assert_eq!(EgressBlocks::last(run), None);
        let f = common::stop_rules::tooling::ToolingFailure::new("pnpm run typegen", "blocked");
        EgressBlocks::record(run, &f);
        assert_eq!(EgressBlocks::last(run), Some(f));
        EgressBlocks::clear(run);
        assert_eq!(EgressBlocks::last(run), None);
    }

    #[test]
    fn a_build_without_a_result_says_why_with_its_last_log_lines() {
        let log = build_log("npm ERR! code ENOENT\nnpm ERR! path /tmp/src/package.json\n");
        let phases = vec![
            "BUILD FAILED: COMMAND_EXECUTION_ERROR Error while executing command: set +e"
                .to_string(),
        ];
        let o = outcome_from_log(&log, "FAILED", &phases);
        assert!(!o.ran && o.broken);
        assert_eq!(
            o.reason,
            "the sandbox build ended FAILED and printed no result (BUILD FAILED: \
             COMMAND_EXECUTION_ERROR Error while executing command: set +e)"
        );
        assert!(o.output.contains("npm ERR! path /tmp/src/package.json"));
        assert!(o.output.contains("Phase complete: BUILD State: SUCCEEDED"));
        assert!(!o.output.contains("auto-discover"));
        // Killed by memory: a sandbox limit, named as one.
        let oom = build_log("===CODERHELM_CHECKS_START===\n===CODERHELM_CHECKS_END exit=137===\n");
        let o = outcome_from_log(&oom, "SUCCEEDED", &[]);
        assert!(!o.ran && o.broken);
        assert!(o.reason.contains("exit 137"));
        // A build that never started for lack of time is not a broken sandbox.
        assert!(!CheckOutcome::not_run("no time").broken);
    }

    #[test]
    fn a_run_gives_up_on_a_sandbox_after_two_builds_without_a_result() {
        let key = "test-run-gives-up";
        assert_eq!(NoResults::given_up(key), None);
        assert_eq!(NoResults::record(key, "first"), 1);
        assert_eq!(NoResults::given_up(key), None);
        assert_eq!(
            NoResults::record(key, "the sandbox build ended FAILED"),
            MAX_NO_RESULT
        );
        assert_eq!(
            NoResults::given_up(key),
            Some((2, "the sandbox build ended FAILED".to_string()))
        );
        // A build with a result shows the setup works again.
        NoResults::clear(key);
        assert_eq!(NoResults::given_up(key), None);
    }

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
    fn repo_env_cannot_replace_the_build_locations() {
        for k in [
            "SANDBOX_GET_URL",
            "SANDBOX_PUT_URL",
            "CODEGEN_CMD",
            "CHECKS_CMD",
            "NODE_VERSION",
            "TERRAFORM_VERSION",
            "EGRESS_ALLOW",
            "EGRESS_PROXY_B64",
            "SANDBOX_BUCKET",
            "SANDBOX_KEY",
            "CODEGEN_OUT_KEY",
        ] {
            assert!(is_reserved_env(k), "{k}");
        }
        assert!(!is_reserved_env("SANITY_PROJECT_ID"));
    }

    #[tokio::test]
    async fn presigned_urls_name_one_key_and_expire() {
        let s3 = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "AKIDEXAMPLE",
                    "secret",
                    None,
                    None,
                    "test",
                ))
                .build(),
        );
        let c = aws_sdk_codebuild::Client::from_conf(
            aws_sdk_codebuild::Config::builder()
                .behavior_version(aws_sdk_codebuild::config::BehaviorVersion::latest())
                .build(),
        );
        let logs = aws_sdk_cloudwatchlogs::Client::from_conf(
            aws_sdk_cloudwatchlogs::Config::builder()
                .behavior_version(aws_sdk_cloudwatchlogs::config::BehaviorVersion::latest())
                .build(),
        );
        let sb = SandboxClient::new(&c, &s3, &logs, "sandbox-bucket", "p");
        let get = sb.presigned_get("sandbox/run-1/0.tgz").await.unwrap();
        let put = sb.presigned_put("codegen-out/run-1/0.tgz").await.unwrap();
        assert!(get.starts_with("https://"), "{get}");
        assert!(get.contains("sandbox/run-1/0.tgz"), "{get}");
        assert!(put.contains("codegen-out/run-1/0.tgz"), "{put}");
        // Plain curl sends no extra headers: only `host` may be signed.
        assert!(put.contains("X-Amz-SignedHeaders=host&"), "{put}");
        assert!(get.contains("X-Amz-SignedHeaders=host&"), "{get}");
        let ttl = format!("X-Amz-Expires={}", PRESIGNED_URL_TTL.as_secs());
        assert!(get.contains(&ttl) && put.contains(&ttl), "{get}");
    }

    /// The production layout: the generator runs in `apps/studio` and writes
    /// `../web/src/lib/sanity/sanity.types.ts`; the build lists both changed
    /// files from the repository root. Both come back, and a file over a cap
    /// is named, never dropped silently.
    #[test]
    fn both_changed_files_come_back_and_a_dropped_one_is_named() {
        fn archive(files: &[(&str, &[u8])]) -> Vec<u8> {
            let mut b = tar::Builder::new(flate2::write::GzEncoder::new(
                Vec::new(),
                flate2::Compression::fast(),
            ));
            for (p, data) in files {
                let mut h = tar::Header::new_gnu();
                h.set_size(data.len() as u64);
                h.set_mode(0o644);
                h.set_cksum();
                b.append_data(&mut h, p, *data).unwrap();
            }
            b.into_inner().unwrap().finish().unwrap()
        }
        let types = vec![b'x'; 7 * 1024 * 1024];
        let tgz = archive(&[
            ("apps/studio/schema.json", b"{}"),
            ("apps/web/src/lib/sanity/sanity.types.ts", &types),
        ]);
        let got = read_changed_tar(&tgz, MAX_CODEGEN_FILES, MAX_CODEGEN_FILE_BYTES).unwrap();
        let paths: Vec<&str> = got.files.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(
            paths,
            [
                "apps/studio/schema.json",
                "apps/web/src/lib/sanity/sanity.types.ts"
            ],
            "the 7 MB types file passed the old 5 MB cap"
        );
        assert!(got.dropped.is_empty());
        // Past a cap: named, with why.
        let small = read_changed_tar(&tgz, MAX_CODEGEN_FILES, 5 * 1024 * 1024).unwrap();
        assert_eq!(small.files.len(), 1);
        assert_eq!(small.dropped.len(), 1);
        assert!(small.dropped[0].contains("apps/web/src/lib/sanity/sanity.types.ts"));
        assert!(small.dropped[0].contains("over the 5 MB cap"));
        let one = read_changed_tar(&tgz, 1, MAX_CODEGEN_FILE_BYTES).unwrap();
        assert!(one.dropped[0].contains("more than 1 changed files"));
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
