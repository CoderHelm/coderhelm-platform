//! Model route measurements: how a lighter model does on the reviewer's and
//! the coder's work, scored against cases with known answers, so a route
//! moves to it only on numbers.
//!
//! Review (`review_cases/measure/`): small synthetic PRs with planted defects
//! of known location and cases with nothing wrong. Each is reviewed the way
//! the nightly diff evals review (`review_eval::live_review`: the reviewer's
//! prompt, its tools on the case's files, the critic, the enforcement rules
//! and the verdict). A planted defect is found when a finding that survives
//! is on its file within `NEAR` lines. Per case: found, missed, extra
//! findings, whether the verdict blocks as it should, tokens and cost.
//!
//! Implement (`model_route_cases/implement/`): a tiny Python repository and
//! medium tickets, each with a hidden test. The coder runs with its own
//! system and task prompts and tools (files answered in memory, `run_checks`
//! running the repository's visible tests); the ticket passes when the
//! hidden test and every visible test pass on what it wrote.
//!
//! Both are `#[ignore]`d live tests, run on demand by
//! `.github/workflows/reviewer-evals.yml` (`suite: route-review` or
//! `route-implement`). Each prints one `ROUTE_EVAL {json}` line per case and
//! a `ROUTE_EVAL_SUMMARY {json}` line. Models: `EVAL_REVIEW_MODEL` (the
//! reviewer), `EVAL_CRITIC_MODEL` (the critic, default the reviewer's),
//! `EVAL_IMPLEMENT_MODEL` (the coder). `EVAL_REPEAT` runs everything that
//! many times.

use super::review_agent::Finding;
use super::review_eval::{eval_client, live_review, LiveExpect, LiveReview};
use crate::agent::llm::ToolDefinition;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Mutex;

/// How far (in lines) a finding may sit from a planted defect and still be
/// the same problem: a reviewer anchors on the line it argues from, which
/// can be a neighbour of the planted one.
const NEAR: u64 = 6;

#[derive(Deserialize)]
struct MeasureCase {
    name: String,
    about: String,
    title: String,
    body: String,
    diff: String,
    files: BTreeMap<String, String>,
    defects: Vec<Defect>,
    expect_blocking: bool,
}

#[derive(Deserialize)]
struct Defect {
    file: String,
    line: u64,
    what: String,
}

fn measure_cases() -> Vec<MeasureCase> {
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/passes/review_cases/measure");
    let mut out: Vec<MeasureCase> = std::fs::read_dir(&dir)
        .expect("review_cases/measure exists")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
        .map(|p| {
            let text = std::fs::read_to_string(&p).unwrap();
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Whether `f` is about `d`: same file, and its anchored lines come within
/// `NEAR` of the defect's line.
fn finding_hits(f: &Finding, d: &Defect) -> bool {
    if f.file.trim_start_matches("./") != d.file {
        return false;
    }
    let start = f.line.min(f.end_line.unwrap_or(f.line));
    let end = f.line.max(f.end_line.unwrap_or(f.line));
    start <= d.line + NEAR && d.line <= end + NEAR
}

/// What one review of a measure case scored.
#[derive(Debug, Default, PartialEq, serde::Serialize)]
struct ReviewScore {
    defects: usize,
    found: usize,
    /// Found by a finding that holds the merge.
    found_blocking: usize,
    /// Surviving findings about none of the planted defects.
    extra: usize,
    extra_blocking: usize,
    verdict_blocks: bool,
    blocking_correct: bool,
}

fn score_review(case: &MeasureCase, findings: &[Finding], verdict: &str) -> ReviewScore {
    let found: Vec<bool> = case
        .defects
        .iter()
        .map(|d| findings.iter().any(|f| finding_hits(f, d)))
        .collect();
    let found_blocking = case
        .defects
        .iter()
        .filter(|d| {
            findings
                .iter()
                .any(|f| finding_hits(f, d) && f.holds_merge())
        })
        .count();
    let extras: Vec<&Finding> = findings
        .iter()
        .filter(|f| !case.defects.iter().any(|d| finding_hits(f, d)))
        .collect();
    let verdict_blocks = verdict == "REQUEST_CHANGES";
    ReviewScore {
        defects: case.defects.len(),
        found: found.iter().filter(|x| **x).count(),
        found_blocking,
        extra: extras.len(),
        extra_blocking: extras.iter().filter(|f| f.holds_merge()).count(),
        verdict_blocks,
        blocking_correct: verdict_blocks == case.expect_blocking,
    }
}

fn repeats() -> usize {
    std::env::var("EVAL_REPEAT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .clamp(1, 5)
}

/// Every measure case is well formed: each defect sits on an added line of
/// the diff, the files the diff touches are readable, and both kinds of
/// cases are there in numbers.
#[test]
fn route_review_cases_are_well_formed() {
    let cases = measure_cases();
    let with = cases.iter().filter(|c| !c.defects.is_empty()).count();
    assert!(with >= 20, "at least 20 cases with planted defects: {with}");
    assert!(cases.len() - with >= 5, "at least 5 clean cases");
    for c in &cases {
        assert!(c.diff.contains("\n@@"), "{}: the diff has a hunk", c.name);
        assert_eq!(c.expect_blocking, !c.defects.is_empty(), "{}", c.name);
        assert!(!c.about.is_empty() && !c.title.is_empty(), "{}", c.name);
        for d in &c.defects {
            assert!(!d.what.is_empty(), "{}: say what the defect is", c.name);
            let text = c
                .files
                .get(&d.file)
                .unwrap_or_else(|| panic!("{}: {} is readable", c.name, d.file));
            let line = text
                .lines()
                .nth(d.line as usize - 1)
                .unwrap_or_else(|| panic!("{}: line {} exists", c.name, d.line));
            assert!(
                c.diff.contains(&format!("### {} ", d.file))
                    && c.diff.contains(&format!("\n+{line}\n")),
                "{}: the defect's line is added by the diff: {line}",
                c.name
            );
        }
    }
}

#[test]
fn a_finding_hits_a_defect_on_its_file_near_its_line() {
    let d = Defect {
        file: "a/b.py".into(),
        line: 20,
        what: "x".into(),
    };
    let f = |file: &str, line: u64, end: Option<u64>| Finding {
        file: file.into(),
        line,
        end_line: end,
        ..serde_json::from_value(json!({"file": file})).unwrap()
    };
    assert!(finding_hits(&f("a/b.py", 20, None), &d));
    assert!(finding_hits(&f("./a/b.py", 14, None), &d));
    assert!(finding_hits(&f("a/b.py", 2, Some(14)), &d));
    assert!(!finding_hits(&f("a/b.py", 27, None), &d));
    assert!(!finding_hits(&f("a/c.py", 20, None), &d));
}

/// Live: every measure case reviewed with the chosen reviewer and critic
/// models, `EVAL_REPEAT` times; prints the per-case and summary lines.
#[tokio::test]
#[ignore]
async fn route_review_live() {
    let (client, model) = eval_client().await;
    let model = std::env::var("EVAL_REVIEW_MODEL").unwrap_or(model);
    let critic = std::env::var("EVAL_CRITIC_MODEL").unwrap_or_else(|_| model.clone());
    let cases = measure_cases();
    let mut errors = vec![];
    for rep in 0..repeats() {
        let (mut defects, mut found, mut found_blocking, mut extra, mut extra_blocking) =
            (0, 0, 0, 0, 0);
        let (mut correct, mut cost, mut tokens_in, mut tokens_out) = (0, 0.0, 0u64, 0u64);
        let mut reviewed = 0;
        for c in &cases {
            let lr = LiveReview {
                title: c.title.clone(),
                body: c.body.clone(),
                diff: c.diff.clone(),
                files: c.files.clone(),
                expect: LiveExpect::default(),
                after_reply: None,
            };
            let mut usage = crate::models::TokenUsage::default();
            let got = match live_review(&client, &model, &critic, &lr, "", &mut usage).await {
                Ok(got) => got,
                Err(e) => {
                    errors.push(format!("{}: {e}", c.name));
                    continue;
                }
            };
            let s = score_review(c, &got.findings, got.verdict);
            reviewed += 1;
            defects += s.defects;
            found += s.found;
            found_blocking += s.found_blocking;
            extra += s.extra;
            extra_blocking += s.extra_blocking;
            correct += usize::from(s.blocking_correct);
            cost += usage.cost_usd;
            tokens_in += usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens;
            tokens_out += usage.output_tokens;
            println!(
                "ROUTE_EVAL {}",
                json!({
                    "suite": "review", "rep": rep, "case": c.name, "about": c.about,
                    "model": model, "critic": critic, "score": s,
                    "findings": got.findings.iter().map(|f| format!(
                        "{}:{} [{} {} {}] {}", f.file, f.line, f.category, f.severity,
                        f.evidence().as_str(), f.title)).collect::<Vec<_>>(),
                    "tokens_in": usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens,
                    "tokens_out": usage.output_tokens,
                    "cost_usd": usage.cost_usd,
                })
            );
        }
        let ratio = |a: usize, b: usize| if b == 0 { 0.0 } else { a as f64 / b as f64 };
        println!(
            "ROUTE_EVAL_SUMMARY {}",
            json!({
                "suite": "review", "rep": rep, "model": model, "critic": critic,
                "cases": reviewed, "defects": defects, "found": found,
                "found_blocking": found_blocking, "extra": extra, "extra_blocking": extra_blocking,
                "recall": ratio(found, defects),
                "blocking_recall": ratio(found_blocking, defects),
                "precision": ratio(found, found + extra),
                "blocking_accuracy": ratio(correct, reviewed),
                "cost_usd": cost, "cost_per_review": cost / reviewed.max(1) as f64,
                "tokens_in": tokens_in, "tokens_out": tokens_out,
            })
        );
    }
    assert!(
        errors.is_empty(),
        "review calls failed:\n{}",
        errors.join("\n")
    );
}

// ── Implement ────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct Repo {
    files: BTreeMap<String, String>,
    test_command: Vec<String>,
}

#[derive(Deserialize)]
struct Ticket {
    name: String,
    title: String,
    body: String,
    proposal: String,
    design: String,
    tasks: String,
    spec: String,
    hidden: String,
}

fn implement_fixture() -> (Repo, Vec<Ticket>) {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/passes/model_route_cases/implement");
    let read = |f: &str| std::fs::read_to_string(dir.join(f)).unwrap();
    (
        serde_json::from_str(&read("repo.json")).unwrap(),
        serde_json::from_str(&read("tickets.json")).unwrap(),
    )
}

/// Runs the repository's tests on `files` (plus `extra`) in a fresh
/// directory: whether they passed, and the tail of the output.
fn run_tests(
    cmd: &[String],
    files: &BTreeMap<String, String>,
    extra: &[(&str, &str)],
) -> (bool, String) {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "route-eval-{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let all = files
        .iter()
        .map(|(p, c)| (p.as_str(), c.as_str()))
        .chain(extra.iter().copied());
    for (p, c) in all {
        if p.contains("..") || p.starts_with('/') {
            continue;
        }
        let path = dir.join(p);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, c).unwrap();
    }
    let ran = std::process::Command::new(&cmd[0])
        .args(&cmd[1..])
        .current_dir(&dir)
        .output();
    let _ = std::fs::remove_dir_all(&dir);
    match ran {
        Ok(o) => {
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            );
            (o.status.success(), common::head_tail_str(&text, 6000))
        }
        Err(e) => (false, format!("could not run the tests: {e}")),
    }
}

/// The coder's tools, answered from an in-memory copy of the repository.
/// Writes change the copy; `run_checks` runs the visible tests on it.
struct MemRepo<'a> {
    base: &'a BTreeMap<String, String>,
    files: Mutex<BTreeMap<String, String>>,
    test_command: &'a [String],
    checks: Mutex<u32>,
}

impl MemRepo<'_> {
    fn answer(&self, name: &str, input: &Value) -> String {
        let path = input["path"]
            .as_str()
            .unwrap_or("")
            .trim_start_matches("./")
            .to_string();
        let mut files = self.files.lock().unwrap();
        match name {
            "read_tree" => files.keys().cloned().collect::<Vec<_>>().join("\n"),
            "list_directory" => {
                let dir = path.trim_end_matches('/');
                let mut entries: Vec<String> = files
                    .keys()
                    .filter_map(|p| {
                        let rest = if dir.is_empty() {
                            p.as_str()
                        } else {
                            p.strip_prefix(&format!("{dir}/"))?
                        };
                        Some(match rest.split_once('/') {
                            Some((d, _)) => format!("{d}/"),
                            None => rest.to_string(),
                        })
                    })
                    .collect();
                entries.dedup();
                if entries.is_empty() {
                    format!("{path}: no such directory")
                } else {
                    entries.join("\n")
                }
            }
            "read_file" => files
                .get(&path)
                .cloned()
                .unwrap_or_else(|| format!("{path}: no such file")),
            "read_file_lines" => match files.get(&path) {
                None => format!("{path}: no such file"),
                Some(text) => {
                    let start = input["start_line"].as_u64().unwrap_or(1).max(1) as usize;
                    let end = input["end_line"].as_u64().unwrap_or(u64::MAX) as usize;
                    text.lines()
                        .enumerate()
                        .filter(|(i, _)| *i + 1 >= start && *i < end)
                        .map(|(i, l)| format!("{}: {l}", i + 1))
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            },
            "search_code" => {
                let q = input["query"].as_str().unwrap_or("");
                let hits: Vec<String> = files
                    .iter()
                    .flat_map(|(p, text)| {
                        text.lines()
                            .enumerate()
                            .filter(|(_, l)| !q.is_empty() && l.contains(q))
                            .map(move |(i, l)| format!("{p}:{}: {}", i + 1, l.trim()))
                    })
                    .take(50)
                    .collect();
                if hits.is_empty() {
                    format!("no matches for {q:?}")
                } else {
                    hits.join("\n")
                }
            }
            "write_file" => {
                let content = input["content"].as_str().unwrap_or("").to_string();
                files.insert(path.clone(), content);
                format!("Wrote {path} and committed it.")
            }
            "edit_file" => {
                let Some(text) = files.get(&path) else {
                    return format!("{path}: no such file; use write_file to create it");
                };
                let edits = input["edits"].as_array().cloned().unwrap_or_default();
                match super::implement::apply_text_edits(text, &edits) {
                    Ok(new) => {
                        files.insert(path.clone(), new);
                        format!("Edited {path} ({} edit(s)) and committed it.", edits.len())
                    }
                    Err(errs) => format!("No change to {path}: {}", errs.join("; ")),
                }
            }
            "batch_write" => {
                let mut done = vec![];
                for f in input["files"].as_array().cloned().unwrap_or_default() {
                    let p = f["path"].as_str().unwrap_or("").to_string();
                    if f["action"].as_str() == Some("delete") {
                        files.remove(&p);
                    } else {
                        files.insert(p.clone(), f["content"].as_str().unwrap_or("").into());
                    }
                    done.push(p);
                }
                format!("Committed {}.", done.join(", "))
            }
            "restore_file" => {
                match self.base.get(&path) {
                    Some(c) => files.insert(path.clone(), c.clone()),
                    None => files.remove(&path),
                };
                format!("Restored {path} to the base branch.")
            }
            "get_diff" => {
                let mut out = vec![];
                for (p, c) in files.iter() {
                    match self.base.get(p) {
                        None => out.push(format!("added {p}")),
                        Some(b) if b != c => out.push(format!("modified {p}")),
                        _ => {}
                    }
                }
                for p in self.base.keys().filter(|p| !files.contains_key(*p)) {
                    out.push(format!("deleted {p}"));
                }
                if out.is_empty() {
                    "no changes".into()
                } else {
                    out.join("\n")
                }
            }
            "run_checks" => {
                *self.checks.lock().unwrap() += 1;
                let (ok, out) = run_tests(self.test_command, &files, &[]);
                format!("{}\n{out}", if ok { "PASSED" } else { "FAILED" })
            }
            _ => format!("{name} is not available here"),
        }
    }
}

#[async_trait::async_trait]
impl crate::agent::llm::ToolExecutor for MemRepo<'_> {
    async fn execute(
        &self,
        name: &str,
        input: &Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Value::String(self.answer(name, input)))
    }
}

/// The coder's prompts for a medium ticket, built as `implement::run` builds
/// them (no repository or team instructions, tests not required).
fn coder_prompts(t: &Ticket) -> (String, String) {
    let plan = super::plan::PlanResult {
        proposal: t.proposal.clone(),
        design: t.design.clone(),
        tasks: t.tasks.clone(),
        spec: t.spec.clone(),
        repo_tasks: vec![],
        fix: None,
    };
    let mut system = super::implement::coder_system("acme", "inventory", "", "", "");
    system.push_str(super::implement::VERIFY_NOTE);
    let issue = format!(
        "\n\n## Original issue — source of truth for any data/values\n\
         The tasks above may reference data \"from the issue\"; the authoritative text is \
         below. Transcribe these EXACT values (ids, URLs, config, copy) — never treat \
         referenced data as missing, and never invent substitutes.\n\n{}",
        t.body
    );
    let prompt = super::implement::task_prompt(&super::implement::TaskPrompt {
        simple: false,
        number: 1,
        title: &t.title,
        design: "",
        continuation: "",
        openspec: &super::format_openspec_block(&plan),
        files_hint: "",
        feedback: "",
        issue: &issue,
        commit: "Write each commit subject as a short imperative sentence.",
    });
    (system, prompt)
}

fn coder_tools() -> Vec<ToolDefinition> {
    super::implement::coder_tools()
        .into_iter()
        .filter(|t| t.name != "run_codegen")
        .collect()
}

/// The ticket's hidden test file: where it goes, and its text.
fn hidden_test(t: &Ticket) -> (String, &str) {
    (format!("tests/test_hidden_{}.py", t.name), &t.hidden)
}

/// The fixture's hidden tests fail on the untouched repository (each ticket
/// asks for something that isn't there), and the visible tests pass.
#[test]
fn route_implement_fixture_is_sound() {
    let (repo, tickets) = implement_fixture();
    assert!((6..=10).contains(&tickets.len()), "6 to 10 tickets");
    if std::process::Command::new(&repo.test_command[0])
        .arg("--version")
        .output()
        .is_err()
    {
        return; // no Python on this machine: the live run checks it
    }
    let (ok, out) = run_tests(&repo.test_command, &repo.files, &[]);
    assert!(ok, "the visible tests pass on the fixture:\n{out}");
    for t in &tickets {
        let (path, text) = hidden_test(t);
        let (ok, _) = run_tests(&repo.test_command, &repo.files, &[(&path, text)]);
        assert!(!ok, "{}: the hidden test fails before the work", t.name);
        let (system, prompt) = coder_prompts(t);
        assert!(prompt.contains(&t.tasks) && prompt.contains(&t.body));
        assert!(system.contains("run_checks"));
    }
}

#[tokio::test]
async fn the_mem_repo_answers_and_writes() {
    let (repo, _) = implement_fixture();
    let m = MemRepo {
        base: &repo.files,
        files: Mutex::new(repo.files.clone()),
        test_command: &repo.test_command,
        checks: Mutex::new(0),
    };
    assert!(m
        .answer("read_tree", &json!({}))
        .contains("inventory/store.py"));
    assert_eq!(
        m.answer("list_directory", &json!({"path": ""}))
            .lines()
            .count(),
        3
    );
    assert!(m
        .answer(
            "edit_file",
            &json!({"path": "inventory/pricing.py", "edits": [{"old_text": "return item.price_cents * qty", "new_text": "return item.price_cents * qty * 2"}]})
        )
        .starts_with("Edited"));
    assert_eq!(
        m.answer("get_diff", &json!({})),
        "modified inventory/pricing.py"
    );
    m.answer("restore_file", &json!({"path": "inventory/pricing.py"}));
    assert_eq!(m.answer("get_diff", &json!({})), "no changes");
    // Every tool the coder is offered here has an answer.
    for t in coder_tools() {
        assert!(
            !m.answer(&t.name, &json!({})).contains("not available"),
            "{}",
            t.name
        );
    }
    // The coder's writes run under the starting policy, as on a new team.
    let gate = common::agent_policy::Gate::new(
        std::sync::Arc::new(common::agent_policy::store::MemStore::new()),
        common::agent_policy::GateCtx::new("route-eval", common::agent_policy::Agent::Coder),
    )
    .offering(coder_tools().iter().map(|t| t.name.clone()));
    let call = common::agent_policy::Call::tool(common::agent_policy::Agent::Coder, "write_file");
    assert!(matches!(
        gate.check_call(call, &json!({"path": "x"}), "test").await,
        common::agent_policy::Outcome::Run { .. }
    ));
}

/// Live: every ticket implemented with `EVAL_IMPLEMENT_MODEL`, the way a
/// medium ticket runs (60 turns, 16384 output tokens), `EVAL_REPEAT` times.
#[tokio::test]
#[ignore]
async fn route_implement_live() {
    let (client, model) = eval_client().await;
    let model = std::env::var("EVAL_IMPLEMENT_MODEL").unwrap_or(model);
    let (repo, tickets) = implement_fixture();
    let tools = coder_tools();
    let mut errors = vec![];
    for rep in 0..repeats() {
        let runs = tickets.iter().map(|t| {
            let (client, model, repo, tools) = (&client, &model, &repo, &tools);
            async move {
                let (system, prompt) = coder_prompts(t);
                let mem = MemRepo {
                    base: &repo.files,
                    files: Mutex::new(repo.files.clone()),
                    test_command: &repo.test_command,
                    checks: Mutex::new(0),
                };
                let gate = common::agent_policy::Gate::new(
                    std::sync::Arc::new(common::agent_policy::store::MemStore::new()),
                    common::agent_policy::GateCtx::new(
                        "route-eval",
                        common::agent_policy::Agent::Coder,
                    ),
                )
                .offering(tools.iter().map(|t| t.name.clone()));
                let mut messages = vec![(
                    "user".to_string(),
                    vec![json!({"type": "text", "text": prompt})],
                )];
                let mut usage = crate::models::TokenUsage::default();
                let started = std::time::Instant::now();
                let res = crate::agent::anthropic::converse_tool_loop(
                    client,
                    model,
                    &system,
                    &mut messages,
                    tools,
                    &mem,
                    &gate,
                    &mut usage,
                    60,
                    16384,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .map_err(|e| e.to_string());
                let files = mem.files.into_inner().unwrap();
                let (path, text) = hidden_test(t);
                let (passed, out) = run_tests(&repo.test_command, &files, &[(&path, text)]);
                (
                    t,
                    res,
                    passed,
                    out,
                    usage,
                    mem.checks.into_inner().unwrap(),
                    started.elapsed(),
                )
            }
        });
        // Four at a time: tickets are independent.
        use futures::StreamExt;
        let results: Vec<_> = futures::stream::iter(runs)
            .buffer_unordered(4)
            .collect()
            .await;
        let (mut passed_n, mut cost) = (0, 0.0);
        for (t, res, passed, out, usage, checks, took) in results {
            if let Err(e) = &res {
                errors.push(format!("{}: {e}", t.name));
            }
            passed_n += usize::from(passed);
            cost += usage.cost_usd;
            println!(
                "ROUTE_EVAL {}",
                json!({
                    "suite": "implement", "rep": rep, "case": t.name, "model": model,
                    "passed": passed, "loop_error": res.as_ref().err(),
                    "run_checks": checks, "seconds": took.as_secs(),
                    "tool_calls": usage.tool_calls,
                    "tokens_in": usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens,
                    "tokens_out": usage.output_tokens, "cost_usd": usage.cost_usd,
                    "test_output": if passed { String::new() } else { common::head_tail_str(&out, 1500) },
                })
            );
        }
        println!(
            "ROUTE_EVAL_SUMMARY {}",
            json!({
                "suite": "implement", "rep": rep, "model": model, "tickets": tickets.len(),
                "passed": passed_n, "pass_rate": passed_n as f64 / tickets.len() as f64,
                "cost_usd": cost, "cost_per_run": cost / tickets.len() as f64,
            })
        );
    }
    assert!(
        errors.is_empty(),
        "coder loops failed:\n{}",
        errors.join("\n")
    );
}
