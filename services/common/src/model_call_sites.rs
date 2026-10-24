//! Every model call CoderHelm makes is metered: this test fails the build
//! when one isn't.
//!
//! It scans `services/**/*.rs` and `lambda/**/*.py` for the ways a model is
//! called (the Anthropic Messages URL, Bedrock `invoke_model` / `converse` /
//! `converse_stream`, `InvokeModel`). Each match must be in one of the
//! metered helper files below, and the function it sits in must record the
//! call (one of that language's meter calls). A new raw call anywhere else,
//! or a helper whose meter call is removed, fails here.

use std::path::{Path, PathBuf};

/// The only files allowed to call a model, each a metered helper.
const METERED_HELPERS: &[&str] = &[
    // The worker's Anthropic client: SSE, every attempt recorded.
    "services/worker/src/agent/anthropic.rs",
    // The gateway's plan chat calls.
    "services/gateway/src/model_call.rs",
    // Titan embeddings (CoderHelm's own cost).
    "services/common/src/embed.rs",
    // The log analyzer lambda's one Anthropic call.
    "lambda/log-analyzer/usage.py",
];

/// What a model call looks like in source.
const RUST_CALLS: &[&str] = &[
    "api.anthropic.com/v1/messages",
    ".invoke_model(",
    ".invoke_model_with_response_stream(",
    ".converse(",
    ".converse_stream(",
    "InvokeModel",
];
const PY_CALLS: &[&str] = &[
    "api.anthropic.com/v1/messages",
    ".invoke_model(",
    ".invoke_model_with_response_stream(",
    ".converse(",
    ".converse_stream(",
    "InvokeModel",
];

/// What recording a call looks like, per language.
const RUST_METERS: &[&str] = &[
    "send_metered(",
    "InFlight::new(",
    ".record_call(",
    "meter.record(",
];
const PY_METERS: &[&str] = &["record_call(", "_record_attempt("];

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn walk(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if p.is_dir() {
            if matches!(
                name,
                "target" | "node_modules" | "__pycache__" | "tests" | ".git"
            ) {
                continue;
            }
            walk(&p, ext, out);
        } else if p.extension().is_some_and(|x| x == ext) {
            out.push(p);
        }
    }
}

/// True when `line` starts a function in this language.
fn starts_fn(line: &str, py: bool) -> bool {
    let t = line.trim_start();
    if py {
        return t.starts_with("def ") || t.starts_with("async def ");
    }
    let t = t
        .trim_start_matches("pub(crate) ")
        .trim_start_matches("pub ")
        .trim_start_matches("async ");
    t.starts_with("fn ")
}

/// Problems in one file's source: a model call outside a metered helper, or
/// a call in a helper whose function records nothing.
pub fn check_source(rel: &str, src: &str) -> Vec<String> {
    let py = rel.ends_with(".py");
    let (calls, meters) = if py {
        (PY_CALLS, PY_METERS)
    } else {
        (RUST_CALLS, RUST_METERS)
    };
    let allowed = METERED_HELPERS.contains(&rel);
    let lines: Vec<&str> = src.lines().collect();
    let mut problems = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let code = line.trim_start();
        if code.starts_with("//") || code.starts_with('#') || code.starts_with("///") {
            continue;
        }
        let Some(pat) = calls.iter().find(|p| line.contains(*p)) else {
            continue;
        };
        if !allowed {
            problems.push(format!(
                "{rel}:{}: `{pat}` is a model call outside a metered helper; call the helper instead",
                i + 1
            ));
            continue;
        }
        let start = (0..=i).rev().find(|&j| starts_fn(lines[j], py));
        let Some(start) = start else {
            problems.push(format!(
                "{rel}:{}: `{pat}` sits outside any function; use it inside the metered function",
                i + 1
            ));
            continue;
        };
        let end = (i + 1..lines.len())
            .find(|&j| starts_fn(lines[j], py))
            .unwrap_or(lines.len());
        let body = lines[start..end].join("\n");
        if !meters.iter().any(|m| body.contains(m)) {
            problems.push(format!(
                "{rel}:{}: `{pat}` in `{}` records nothing (expected one of {meters:?})",
                i + 1,
                lines[start].trim()
            ));
        }
    }
    problems
}

#[test]
fn every_model_call_is_in_a_metered_helper_that_records_it() {
    let root = root();
    let mut files = Vec::new();
    walk(&root.join("services"), "rs", &mut files);
    walk(&root.join("lambda"), "py", &mut files);
    assert!(files.len() > 50, "found the sources ({})", files.len());
    let mut problems = Vec::new();
    let mut seen_helpers = Vec::new();
    for f in files {
        let rel = f
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "services/common/src/model_call_sites.rs" {
            continue;
        }
        if METERED_HELPERS.contains(&rel.as_str()) {
            seen_helpers.push(rel.clone());
        }
        let src = std::fs::read_to_string(&f).unwrap();
        problems.extend(check_source(&rel, &src));
    }
    for h in METERED_HELPERS {
        assert!(
            seen_helpers.iter().any(|s| s == h),
            "{h} is gone; update the list"
        );
    }
    assert!(
        problems.is_empty(),
        "unmetered model calls:\n{}",
        problems.join("\n")
    );
}

#[test]
fn a_raw_call_outside_the_helpers_fails() {
    let src =
        "async fn sneaky(c: &Client) {\n    c.invoke_model().model_id(\"x\").send().await;\n}\n";
    assert_eq!(
        check_source("services/worker/src/passes/x.rs", src).len(),
        1
    );
    let src = "def f():\n    urlopen('https://api.anthropic.com/v1/messages')\n";
    assert_eq!(check_source("lambda/log-analyzer/handler.py", src).len(), 1);
}

#[test]
fn a_helper_whose_meter_call_is_removed_fails() {
    let metered = "pub async fn embed(c: &Client, meter: &UsageMeter) {\n    let out = c.invoke_model().send().await;\n    meter.record_call(rec).await;\n}\n";
    assert!(check_source("services/common/src/embed.rs", metered).is_empty());
    let unmetered = metered.replace("    meter.record_call(rec).await;\n", "");
    assert_eq!(
        check_source("services/common/src/embed.rs", &unmetered).len(),
        1
    );
}
