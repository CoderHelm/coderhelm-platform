//! Checks on the pull request description of a fix run started from a log
//! finding, against the evidence the run gathered (see `log_evidence`):
//!
//! - **hedges**: no "unconfirmed / could not confirm / inferred" claim while
//!   the run had a source that could answer it. A hedge stands only when it
//!   names a source the evidence step tried and that failed.
//! - **limits**: every numeric limit the diff introduces (a size, a budget,
//!   a TTL, a threshold) is backed by a number in the evidence or in the
//!   description, in any common unit.
//! - **start_path**: when the change builds paths at runtime (from the
//!   working directory, `__dirname`, a cache directory), the start command
//!   production runs was resolved and the description names it.
//!
//! A failing description goes back once: the evidence is gathered again
//! (for hedges) and the writer revises with the reasons. What still fails
//! is rewritten in code. Each check's result and reason is recorded on the
//! run and in its progress notes.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::log_evidence::LogEvidence;

/// One check's result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckResult {
    pub check: String,
    pub passed: bool,
    pub reason: String,
}

impl CheckResult {
    fn pass(check: &str, reason: impl Into<String>) -> Self {
        Self {
            check: check.into(),
            passed: true,
            reason: reason.into(),
        }
    }
    fn fail(check: &str, reason: impl Into<String>) -> Self {
        Self {
            check: check.into(),
            passed: false,
            reason: reason.into(),
        }
    }
}

/// Words that leave a fact unsettled.
pub const HEDGES: &[&str] = &[
    "unconfirmed",
    "not confirmed",
    "could not confirm",
    "couldn't confirm",
    "cannot confirm",
    "can't confirm",
    "unable to confirm",
    "could not verify",
    "couldn't verify",
    "unable to verify",
    "unverified",
    "not verified",
    "inferred",
    "i infer",
    "could not read",
    "couldn't read",
    "appears to",
    "presumably",
];

/// Pure: the description's sentences (a list item or a line is at least one).
pub fn sentences(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        let mut cur = String::new();
        let chars: Vec<char> = line.chars().collect();
        for (i, c) in chars.iter().enumerate() {
            cur.push(*c);
            let end =
                matches!(c, '.' | '!' | '?') && chars.get(i + 1).is_none_or(|n| n.is_whitespace());
            if end {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            }
        }
        if !cur.trim().is_empty() {
            out.push(cur.trim().to_string());
        }
    }
    out
}

/// Pure: the hedged sentences that don't name a source the evidence step
/// tried and couldn't read.
pub fn unbacked_hedges(body: &str, ev: &LogEvidence) -> Vec<String> {
    let failed: Vec<String> = ev
        .failed_sources()
        .iter()
        .map(|t| t.source.to_ascii_lowercase())
        .collect();
    sentences(body)
        .into_iter()
        .filter(|s| {
            let l = s.to_ascii_lowercase();
            HEDGES.iter().any(|h| l.contains(h)) && !failed.iter().any(|f| l.contains(f))
        })
        .collect()
}

pub fn check_hedges(body: &str, ev: &LogEvidence) -> CheckResult {
    let h = unbacked_hedges(body, ev);
    if h.is_empty() {
        return CheckResult::pass("hedges", "no unsettled claims");
    }
    let answered: Vec<&str> = ev
        .tried
        .iter()
        .filter(|t| t.ok)
        .map(|t| t.source.as_str())
        .collect();
    CheckResult::fail(
        "hedges",
        format!(
            "{} claim(s) left unsettled while the run could read {}: {}",
            h.len(),
            if answered.is_empty() {
                "nothing it names as tried".to_string()
            } else {
                answered.join(", ")
            },
            h.iter()
                .map(|s| format!("\"{}\"", common::truncate_str(s, 160)))
                .collect::<Vec<_>>()
                .join("; ")
        ),
    )
}

/// A number the diff introduces in a line that sets a limit.
#[derive(Debug, Clone, PartialEq)]
pub struct Limit {
    pub file: String,
    pub text: String,
    pub value: f64,
    /// The literals the value is a product of.
    pub factors: Vec<f64>,
}

/// Words that make a line a limit.
const LIMIT_WORDS: &[&str] = &[
    "max",
    "limit",
    "budget",
    "size",
    "bytes",
    "ttl",
    "timeout",
    "capacity",
    "threshold",
    "quota",
    "retention",
    "evict",
    "entries",
    "interval",
    "storage",
    "percent",
    "period",
    "mib",
    "gib",
    "_mb",
    "_gb",
];

/// Unit factors a number is often written in.
const UNITS: &[f64] = &[
    1.0,
    1e3,
    1e6,
    1e9,
    1024.0,
    1_048_576.0,
    1_073_741_824.0,
    60.0,
    3600.0,
    86_400.0,
    60_000.0,
    3_600_000.0,
    86_400_000.0,
];

fn is_unit(v: f64) -> bool {
    UNITS.contains(&v) || v == 1024.0 * 1024.0 || v == 24.0 || v == 100.0
}

/// Pure: number chains in a piece of text (`512 * 1024 * 1024` is one
/// chain), each as its value and its factors.
pub fn number_chains(text: &str) -> Vec<(f64, Vec<f64>)> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let read_num = |i: &mut usize| -> Option<f64> {
        let start = *i;
        let mut s = String::new();
        while *i < chars.len() {
            let c = chars[*i];
            if c.is_ascii_digit() {
                s.push(c);
            } else if (c == '_' || c == ',')
                && *i + 1 < chars.len()
                && chars[*i + 1].is_ascii_digit()
                && !s.is_empty()
            {
            } else if c == '.'
                && *i + 1 < chars.len()
                && chars[*i + 1].is_ascii_digit()
                && !s.contains('.')
                && !s.is_empty()
            {
                s.push(c);
            } else {
                break;
            }
            *i += 1;
        }
        if *i == start {
            None
        } else {
            s.parse().ok()
        }
    };
    while i < chars.len() {
        let c = chars[i];
        let prev = if i > 0 { Some(chars[i - 1]) } else { None };
        let starts = c.is_ascii_digit()
            && !prev.is_some_and(|p| p.is_alphanumeric() || p == '_' || p == '.');
        if !starts {
            i += 1;
            continue;
        }
        let Some(first) = read_num(&mut i) else {
            i += 1;
            continue;
        };
        let mut factors = vec![first];
        loop {
            let mut j = i;
            while j < chars.len() && chars[j] == ' ' {
                j += 1;
            }
            if j < chars.len() && chars[j] == '*' && chars.get(j + 1) != Some(&'*') {
                j += 1;
                while j < chars.len() && chars[j] == ' ' {
                    j += 1;
                }
                if j < chars.len() && chars[j].is_ascii_digit() {
                    let mut k = j;
                    if let Some(n) = read_num(&mut k) {
                        factors.push(n);
                        i = k;
                        continue;
                    }
                }
            }
            break;
        }
        // An identifier right after the digits (`3d`, `0x1f`) isn't a number.
        if chars.get(i).is_some_and(|c| c.is_alphabetic() && *c != 'x')
            || chars.get(i) == Some(&'x')
        {
            let unitish = chars[i..]
                .iter()
                .take_while(|c| c.is_alphabetic())
                .collect::<String>()
                .to_ascii_lowercase();
            if !matches!(
                unitish.as_str(),
                "gib" | "mib" | "kib" | "gb" | "mb" | "kb" | "b" | "h" | "m" | "s" | "ms" | "d"
            ) {
                continue;
            }
        }
        let value = factors.iter().product();
        out.push((value, factors));
    }
    out
}

fn skip_file(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    p.contains("test")
        || p.contains("spec")
        || p.contains("fixture")
        || p.ends_with(".md")
        || p.ends_with(".lock")
        || p.ends_with("lock.json")
        || p.ends_with(".snap")
}

/// The code part of an added line (comments dropped).
fn code_part(line: &str) -> Option<&str> {
    let t = line.trim();
    if t.starts_with("//")
        || t.starts_with('#')
        || t.starts_with("/*")
        || t.starts_with('*')
        || t.starts_with("<!--")
        || t.starts_with("--")
    {
        return None;
    }
    Some(match t.find(" //") {
        Some(i) => &t[..i],
        None => t,
    })
}

/// Pure: the added lines of each file the diff changes (skipping tests,
/// docs and lock files).
pub fn added_lines(diff: &Value) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for f in diff["files"].as_array().into_iter().flatten() {
        let name = f["filename"].as_str().unwrap_or("");
        if skip_file(name) {
            continue;
        }
        for l in f["patch"].as_str().unwrap_or("").lines() {
            if let Some(added) = l.strip_prefix('+') {
                if !added.starts_with("++") {
                    out.push((name.to_string(), added.to_string()));
                }
            }
        }
    }
    out
}

/// Pure: the numeric limits the diff introduces.
pub fn introduced_limits(diff: &Value) -> Vec<Limit> {
    let mut out = Vec::new();
    for (file, line) in added_lines(diff) {
        let Some(code) = code_part(&line) else {
            continue;
        };
        let lower = code.to_ascii_lowercase();
        if !LIMIT_WORDS.iter().any(|w| lower.contains(w)) {
            continue;
        }
        for (value, factors) in number_chains(code) {
            if value <= 10.0 {
                continue;
            }
            out.push(Limit {
                file: file.clone(),
                text: code.trim().to_string(),
                value,
                factors,
            });
        }
    }
    out
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 0.02 * b.abs().max(1.0)
}

/// Pure: whether a limit is backed by one of `numbers`: its value in any
/// common unit, or one of its non-unit factors.
pub fn backed(limit: &Limit, numbers: &[f64]) -> bool {
    numbers.iter().any(|n| {
        UNITS.iter().any(|d| close(limit.value / d, *n))
            || limit
                .factors
                .iter()
                .any(|f| !is_unit(*f) && *f > 10.0 && close(*f, *n))
    })
}

/// Every number in the description, and the measured ones in the evidence
/// (disk, fill rate, hours to the first error). The evidence's other numbers
/// (cpu, memory, counts) are not measurements of a limit.
pub fn known_numbers(body: &str, ev: &LogEvidence) -> Vec<f64> {
    let mut out: Vec<f64> = number_chains(body)
        .into_iter()
        .flat_map(|(v, f)| std::iter::once(v).chain(f))
        .collect();
    if let Some(g) = ev.disk_gib {
        out.push(g as f64);
    }
    out.extend(ev.fill_mb_per_hour);
    out.extend(ev.tasks.iter().filter_map(|t| t.hours_to_first_error));
    out
}

pub fn check_limits(diff: &Value, body: &str, ev: &LogEvidence) -> CheckResult {
    let limits = introduced_limits(diff);
    if limits.is_empty() {
        return CheckResult::pass("limits", "the diff introduces no numeric limits");
    }
    let numbers = known_numbers(body, ev);
    let unbacked: Vec<&Limit> = limits.iter().filter(|l| !backed(l, &numbers)).collect();
    if unbacked.is_empty() {
        return CheckResult::pass(
            "limits",
            format!(
                "{} limit(s) backed by numbers in the evidence or the description",
                limits.len()
            ),
        );
    }
    CheckResult::fail(
        "limits",
        format!(
            "no measured number behind: {}",
            unbacked
                .iter()
                .map(|l| format!("`{}` in `{}`", common::truncate_str(&l.text, 100), l.file))
                .collect::<Vec<_>>()
                .join("; ")
        ),
    )
}

/// What builds a path at runtime.
const RUNTIME_PATH_MARKERS: &[&str] = &[
    "process.cwd(",
    "__dirname",
    "path.join(",
    "path.resolve(",
    "os.getcwd(",
    "os.path.join(",
    "current_dir(",
    "distdir",
    "cachedir",
    "cachehandler",
    "cache_dir",
    "\"./",
    "'./",
    "/tmp/",
    ".next/",
];

/// Pure: the added code lines that build a path at runtime.
pub fn runtime_path_lines(diff: &Value) -> Vec<String> {
    added_lines(diff)
        .into_iter()
        .filter_map(|(file, l)| {
            let code = code_part(&l)?;
            let lower = code.to_ascii_lowercase();
            RUNTIME_PATH_MARKERS
                .iter()
                .any(|m| lower.contains(m))
                .then(|| format!("`{}` in `{file}`", common::truncate_str(code.trim(), 100)))
        })
        .collect()
}

pub fn check_start_path(diff: &Value, body: &str, ev: &LogEvidence) -> CheckResult {
    let lines = runtime_path_lines(diff);
    if lines.is_empty() {
        return CheckResult::pass("start_path", "the change builds no paths at runtime");
    }
    let Some(sp) = ev.start.as_ref().filter(|s| s.verified) else {
        let why = ev
            .start
            .as_ref()
            .map(|s| s.missing.clone())
            .unwrap_or_else(|| "it was not resolved".into());
        return CheckResult::fail(
            "start_path",
            format!(
                "the change builds paths at runtime ({}) but the start command production runs is not verified: {why}",
                lines.first().cloned().unwrap_or_default()
            ),
        );
    };
    if body.contains(&sp.command) {
        CheckResult::pass(
            "start_path",
            format!("production starts with `{}` ({})", sp.command, sp.from),
        )
    } else {
        CheckResult::fail(
            "start_path",
            format!(
                "the change builds paths at runtime ({}) and the description doesn't say they resolve under the start command `{}`",
                lines.first().cloned().unwrap_or_default(),
                sp.command
            ),
        )
    }
}

/// Every check on a description.
pub fn run_all(diff: &Value, body: &str, ev: &LogEvidence) -> Vec<CheckResult> {
    vec![
        check_hedges(body, ev),
        check_limits(diff, body, ev),
        check_start_path(diff, body, ev),
    ]
}

/// The message that sends the writer back once.
pub fn revise_prompt(failed: &[CheckResult], ev: &LogEvidence) -> String {
    format!(
        "Your description fails these checks:\n{}\n\n{}\n\nRevise it once:\n\
         - Settle each unsettled claim from the facts above, or drop it. Keep a claim open only when no source above can answer it, naming the source that was tried.\n\
         - For each limit the change sets, give the arithmetic from the measured numbers (disk, hours to fill, rate, the budget that keeps N hours).\n\
         - State the start command production runs and that the paths the change builds resolve under it.\n\
         Keep it short. Return ONLY the full markdown body again.",
        failed
            .iter()
            .map(|c| format!("- {}: {}", c.check, c.reason))
            .collect::<Vec<_>>()
            .join("\n"),
        super::log_evidence::facts_markdown(ev)
    )
}

/// Pure: the description with what still fails rewritten in code:
/// unsettled sentences removed, unbacked limits and the start path stated.
pub fn rewrite(body: &str, diff: &Value, ev: &LogEvidence) -> String {
    let mut out = body.to_string();
    for s in unbacked_hedges(body, ev) {
        out = out.replace(&s, "");
    }
    // Lines left empty by a removed sentence go, list markers included.
    let mut out: String = out
        .lines()
        .filter(|l| !matches!(l.trim(), "*" | "-"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut notes = Vec::new();
    if !check_start_path(diff, &out, ev).passed {
        match ev.start.as_ref().filter(|s| s.verified) {
            Some(sp) => notes.push(format!("**Start path:** {}", sp.line())),
            None => notes.push(format!(
                "**Start path:** not verified ({}).",
                ev.start
                    .as_ref()
                    .map(|s| s.missing.as_str())
                    .unwrap_or("not resolved")
            )),
        }
    }
    let numbers = known_numbers(&out, ev);
    let unbacked: Vec<Limit> = introduced_limits(diff)
        .into_iter()
        .filter(|l| !backed(l, &numbers))
        .collect();
    if !unbacked.is_empty() {
        notes.push(format!(
            "**Limits without a measured number behind them:** {}.",
            unbacked
                .iter()
                .map(|l| format!("`{}` in `{}`", common::truncate_str(&l.text, 100), l.file))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    if !notes.is_empty() {
        out = format!("{}\n\n{}", out.trim_end(), notes.join("\n\n"));
    }
    out
}

/// Records the results on the run (`log_fix_checks`, appended per pull
/// request) and as progress notes, so the run page shows each reason.
pub async fn record(
    state: &crate::WorkerState,
    team_id: &str,
    run_id: &str,
    repo: &str,
    stage: &str,
    results: &[CheckResult],
) {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let items: Vec<A> = results
        .iter()
        .map(|r| {
            A::M(
                [
                    ("check".to_string(), A::S(r.check.clone())),
                    ("passed".to_string(), A::Bool(r.passed)),
                    (
                        "reason".to_string(),
                        A::S(common::truncate_str(&r.reason, 1_500).to_string()),
                    ),
                    ("repo".to_string(), A::S(repo.to_string())),
                    ("stage".to_string(), A::S(stage.to_string())),
                ]
                .into_iter()
                .collect(),
            )
        })
        .collect();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", A::S(team_id.to_string()))
        .key("run_id", A::S(run_id.to_string()))
        .update_expression(
            "SET log_fix_checks = list_append(if_not_exists(log_fix_checks, :empty), :c)",
        )
        .expression_attribute_values(":empty", A::L(vec![]))
        .expression_attribute_values(":c", A::L(items))
        .send()
        .await;
    for r in results {
        super::add_progress_note(
            state,
            team_id,
            run_id,
            &format!(
                "Description check `{}` on {repo} ({stage}): {} — {}",
                r.check,
                if r.passed { "passed" } else { "failed" },
                common::truncate_str(&r.reason, 400)
            ),
        )
        .await;
    }
}

/// Runs the checks on a drafted description; on a failure gathers the
/// evidence again once (for unsettled claims), has the writer revise once
/// on the same conversation, and rewrites in code what still fails. Every
/// model call goes through the provider, so it is metered.
#[allow(clippy::too_many_arguments)]
pub async fn enforce(
    state: &crate::WorkerState,
    msg: &crate::models::TicketMessage,
    run_id: &str,
    diff: &Value,
    body: String,
    ev: &LogEvidence,
    provider: &crate::agent::provider::ModelProvider,
    system: &str,
    messages: &mut Vec<(String, Vec<Value>)>,
    opts: crate::agent::llm::ConverseOptions,
    usage: &mut crate::models::TokenUsage,
) -> String {
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let first = run_all(diff, &body, ev);
    record(state, &msg.team_id, run_id, &repo, "draft", &first).await;
    let failed: Vec<CheckResult> = first.into_iter().filter(|c| !c.passed).collect();
    if failed.is_empty() {
        return body;
    }
    // Unsettled claims send the run back to the evidence once.
    let mut ev = ev.clone();
    if failed.iter().any(|c| c.check == "hedges") {
        if let Some(mut fresh) = super::log_evidence::gather_for_run(state, msg).await {
            fresh.start = ev.start.clone();
            super::log_evidence::save(state, &msg.team_id, run_id, &fresh).await;
            super::add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!(
                    "Gathered the evidence again for unsettled claims. {}",
                    super::log_evidence::summary_note(&fresh)
                ),
            )
            .await;
            ev = fresh;
        }
    }
    messages.push((
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": revise_prompt(&failed, &ev)})],
    ));
    let revised = match crate::agent::provider::converse(
        state,
        provider,
        common::agent_policy::Agent::Coder,
        provider.primary_model_id(),
        system,
        messages,
        &[],
        &super::triage::NoOpExecutor,
        usage,
        opts,
        None,
        None,
    )
    .await
    {
        Ok(t) if !t.trim().is_empty() => t,
        Ok(_) | Err(_) => body.clone(),
    };
    let second = run_all(diff, &revised, &ev);
    let rewritten = !second.iter().all(|c| c.passed);
    let out = if rewritten {
        rewrite(&revised, diff, &ev)
    } else {
        revised.clone()
    };
    // What the rewrite settled is reported as such; a limit it only lists
    // as unbacked stays failed.
    let after = run_all(diff, &out, &ev);
    let last: Vec<CheckResult> = second
        .into_iter()
        .zip(after)
        .map(
            |(before, after)| match (before.passed, before.check.as_str()) {
                (true, _) => before,
                (false, "limits") => CheckResult {
                    reason: format!("{} (listed in the description)", before.reason),
                    ..before
                },
                (false, _) if after.passed => CheckResult {
                    reason: format!("{} (rewritten in code: {})", before.reason, after.reason),
                    ..after
                },
                (false, _) => before,
            },
        )
        .collect();
    record(state, &msg.team_id, run_id, &repo, "final", &last).await;
    out
}

#[cfg(test)]
mod tests {
    use super::super::log_evidence::{SourceTry, StartPath};
    use super::*;
    use serde_json::json;

    fn ev() -> LogEvidence {
        LogEvidence {
            log_group: "/ecs/site".into(),
            disk_gib: Some(20),
            fill_mb_per_hour: Some(347.0),
            tried: vec![
                SourceTry {
                    source: "logs:StartQuery".into(),
                    ok: true,
                    ..Default::default()
                },
                SourceTry {
                    source: "ecs:DescribeTasks".into(),
                    ok: false,
                    outcome: "AccessDenied".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    #[test]
    fn hedges_need_a_failed_source() {
        let body = "The cache grows without bound. I inferred this from the framework's behaviour.\n* Task start times are unconfirmed: `ecs:DescribeTasks` was refused.";
        let h = unbacked_hedges(body, &ev());
        assert_eq!(
            h,
            vec!["I inferred this from the framework's behaviour.".to_string()]
        );
        assert!(!check_hedges(body, &ev()).passed);
        assert!(check_hedges("Disk fills in 59 h.", &ev()).passed);
    }

    #[test]
    fn number_chains_multiply() {
        let c = number_chains("const MAX = 2 * 1024 * 1024 * 1024; ttl 3600, v1 x2");
        assert_eq!(c[0].0, 2147483648.0);
        assert_eq!(c[0].1, vec![2.0, 1024.0, 1024.0, 1024.0]);
        assert_eq!(c[1].0, 3600.0);
        assert_eq!(c.len(), 2);
        assert_eq!(number_chains("20,480 MB and 1_000")[0].0, 20480.0);
    }

    fn diff(lines: &[&str]) -> Value {
        json!({"files": [{"filename": "cache-handler.js", "patch": lines.iter().map(|l| format!("+{l}")).collect::<Vec<_>>().join("\n")}]})
    }

    #[test]
    fn limits_must_be_backed() {
        let d = diff(&["const MAX_CACHE_BYTES = 2 * 1024 * 1024 * 1024;"]);
        assert!(!check_limits(&d, "Bounds the cache.", &LogEvidence::default()).passed);
        assert!(check_limits(&d, "A 2 GiB budget keeps about 6 h at 347 MB/h.", &ev()).passed);
        // Comments and tests don't count.
        let d = json!({"files": [
            {"filename": "a.js", "patch": "+// max 5000 entries"},
            {"filename": "a.test.js", "patch": "+const maxSize = 999;"}
        ]});
        assert!(introduced_limits(&d).is_empty());
    }

    #[test]
    fn runtime_paths_need_the_start_command() {
        let d = diff(&["const dir = path.join(process.cwd(), '.next/cache');"]);
        let mut e = ev();
        assert!(!check_start_path(&d, "x", &e).passed);
        e.start = Some(StartPath {
            command: "npm run serve".into(),
            from: "task definition site:9".into(),
            verified: true,
            ..Default::default()
        });
        assert!(!check_start_path(&d, "x", &e).passed);
        assert!(check_start_path(&d, "Prod runs `npm run serve` from /app.", &e).passed);
        assert!(check_start_path(&diff(&["x = 1"]), "x", &e).passed);
    }

    #[test]
    fn the_rewrite_drops_unsettled_claims_and_states_the_rest() {
        let d = diff(&[
            "const MAX_CACHE_BYTES = 777 * 1024 * 1024;",
            "const dir = path.join(process.cwd(), '.next/cache');",
        ]);
        let mut e = ev();
        e.start = Some(StartPath {
            command: "npm run serve".into(),
            from: "task definition site:9".into(),
            verified: true,
            ..Default::default()
        });
        let body = "Bounds the cache.\n* It appears to never evict.\n* Disk is 20 GiB.";
        let out = rewrite(body, &d, &e);
        assert!(!out.contains("appears to"));
        assert!(out.contains("Disk is 20 GiB."));
        assert!(
            out.contains("**Start path:** Production starts the container with `npm run serve`")
        );
        assert!(out.contains("Limits without a measured number"));
        assert!(run_all(&d, &out, &e)
            .iter()
            .filter(|c| c.check != "limits")
            .all(|c| c.passed));
    }
}
