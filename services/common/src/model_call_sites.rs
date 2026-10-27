//! Every model call CoderHelm makes is metered and asks the team's token
//! limit first: this test fails the build when one doesn't.
//!
//! It scans `services/**/*.rs`, `lambda/**/*.py` and every JavaScript or
//! TypeScript source in the repository for the ways a model is called (the
//! Anthropic Messages endpoint, Bedrock `invoke_model` / `converse` /
//! `converse_stream`, `InvokeModel`, the Bedrock runtime client, the
//! Anthropic SDKs). Each match must be in one of the metered helper files
//! below, and the function it sits in must both record the call (one of
//! that language's meter calls) and ask the limit before it (one of the
//! admit calls). A new raw call anywhere else, or a helper whose meter or
//! limit check is removed, fails here. Test modules (`#[cfg(test)]` to the
//! end of a Rust file, `tests` directories, `*.test.*` files) talk to local
//! fake servers and are not scanned.

use std::path::{Path, PathBuf};

/// The only files allowed to call a model, each a metered helper.
const METERED_HELPERS: &[&str] = &[
    // The one streamed, metered, limit-checked path for the Anthropic API
    // and Bedrock: `send_metered`, `open_metered`, `send_bedrock_metered`,
    // `open_bedrock_metered`. Defines `MESSAGES_URL`.
    "services/common/src/model_stream.rs",
    // The worker's Anthropic client (every run, review and agent call).
    "services/worker/src/agent/anthropic.rs",
    // The gateway's plan chat, brain chat and setup probe calls.
    "services/gateway/src/model_call.rs",
    // Titan embeddings (CoderHelm's own cost).
    "services/common/src/embed.rs",
    // The log analyzer lambda's Anthropic and Bedrock calls.
    "lambda/log-analyzer/usage.py",
];

/// Helpers whose calls are billed to CoderHelm, never to a team, so no
/// team limit applies (they are still metered, under `coderhelm#`).
const UNGATED_HELPERS: &[&str] = &["services/common/src/embed.rs"];

/// Model calls that are not CoderHelm's and have nothing to meter into:
/// file, and why.
const NOT_METERED: &[(&str, &str)] = &[(
    "installer/lib/checks.mjs",
    "the installer's one-token key and Bedrock checks, run on the installing \
     admin's machine with the admin's own key or AWS login, before the \
     install (and its analytics table) exists",
)];

/// What a model call looks like in source.
const RUST_CALLS: &[&str] = &[
    "/v1/messages",
    "MESSAGES_URL",
    ".invoke_model(",
    ".invoke_model_with_response_stream(",
    ".converse(",
    ".converse_stream(",
    "InvokeModel",
];
const PY_CALLS: &[&str] = &[
    "/v1/messages",
    ".invoke_model(",
    ".invoke_model_with_response_stream(",
    ".converse(",
    ".converse_stream(",
    "InvokeModel",
    "bedrock-runtime",
    "import anthropic",
    "from anthropic",
];
const JS_CALLS: &[&str] = &[
    "/v1/messages",
    "bedrock-runtime",
    "InvokeModelCommand",
    "InvokeModelWithResponseStreamCommand",
    "ConverseCommand",
    "ConverseStreamCommand",
    // The Anthropic SDKs (not MCP servers that happen to be published
    // under the same scope).
    "@anthropic-ai/sdk",
    "@anthropic-ai/bedrock-sdk",
    "@anthropic-ai/vertex-sdk",
    "@anthropic-ai/foundry-sdk",
    "@anthropic-ai/claude-agent-sdk",
    "@anthropic-ai/claude-code",
];

/// What recording a call looks like, per language.
const RUST_METERS: &[&str] = &[
    "send_metered(",
    "open_metered(",
    "send_bedrock_metered(",
    "open_bedrock_metered(",
    "InFlight::new(",
    ".record_call(",
    "meter.record(",
];
const PY_METERS: &[&str] = &["record_call(", "_record_attempt("];

/// What asking the limit looks like (the metered senders ask it themselves).
const RUST_ADMITS: &[&str] = &[
    ".admit(",
    "send_metered(",
    "open_metered(",
    "send_bedrock_metered(",
    "open_bedrock_metered(",
];
const PY_ADMITS: &[&str] = &["admit("];

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
                "target" | "node_modules" | "__pycache__" | "tests" | "test" | ".git" | "cdk.out"
            ) {
                continue;
            }
            walk(&p, ext, out);
        } else if p.extension().is_some_and(|x| x == ext) && !name.contains(".test.") {
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

/// The language of a source file by its name.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lang {
    Rust,
    Py,
    Js,
}

fn lang_of(rel: &str) -> Lang {
    if rel.ends_with(".rs") {
        Lang::Rust
    } else if rel.ends_with(".py") {
        Lang::Py
    } else {
        Lang::Js
    }
}

/// Problems in one file's source: a model call outside a metered helper, or
/// a call in a helper whose function records nothing or never asks the
/// limit.
pub fn check_source(rel: &str, src: &str) -> Vec<String> {
    let lang = lang_of(rel);
    let py = lang == Lang::Py;
    let (calls, meters, admits) = match lang {
        Lang::Rust => (RUST_CALLS, RUST_METERS, RUST_ADMITS),
        Lang::Py => (PY_CALLS, PY_METERS, PY_ADMITS),
        Lang::Js => (JS_CALLS, &[][..], &[][..]),
    };
    if NOT_METERED.iter().any(|(f, _)| *f == rel) {
        return Vec::new();
    }
    let allowed = METERED_HELPERS.contains(&rel);
    let gated = !UNGATED_HELPERS.contains(&rel);
    let mut lines: Vec<&str> = src.lines().collect();
    // A Rust file's test module runs against local fake servers.
    if lang == Lang::Rust {
        if let Some(t) = lines.iter().position(|l| l.trim() == "#[cfg(test)]") {
            lines.truncate(t);
        }
    }
    let mut problems = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let code = line.trim_start();
        if code.starts_with("//") || code.starts_with('#') || code.starts_with('*') {
            continue;
        }
        // Naming the endpoint constant in an import is not a call.
        if lang == Lang::Rust && code.starts_with("use ") {
            continue;
        }
        let Some(pat) = calls.iter().find(|p| line.contains(*p)) else {
            continue;
        };
        if !allowed || lang == Lang::Js {
            problems.push(format!(
                "{rel}:{}: `{pat}` is a model call outside a metered helper; call the helper instead",
                i + 1
            ));
            continue;
        }
        // The endpoint's one definition.
        if rel == "services/common/src/model_stream.rs"
            && code.starts_with("pub const MESSAGES_URL")
        {
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
        if gated && !admits.iter().any(|m| body.contains(m)) {
            problems.push(format!(
                "{rel}:{}: `{pat}` in `{}` never asks the token limit (expected one of {admits:?})",
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
    let rust_and_py = files.len();
    for ext in ["js", "mjs", "cjs", "ts", "tsx"] {
        walk(&root, ext, &mut files);
    }
    assert!(rust_and_py > 50, "found the sources ({rust_and_py})");
    assert!(files.len() > rust_and_py + 20, "found the JS/TS sources");
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
    for (f, _) in NOT_METERED {
        assert!(root.join(f).exists(), "{f} is gone; update the list");
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

#[test]
fn a_team_call_that_never_asks_the_limit_fails() {
    let ok = "async fn start(c: &Client, meter: &UsageMeter) {\n    meter.admit().await?;\n    c.invoke_model_with_response_stream().send().await;\n    let f = InFlight::new(meter, m, None, 0);\n}\n";
    let rel = "services/common/src/model_stream.rs";
    assert!(check_source(rel, ok).is_empty());
    let unchecked = ok.replace("    meter.admit().await?;\n", "");
    let p = check_source(rel, &unchecked);
    assert_eq!(p.len(), 1, "{p:?}");
    assert!(p[0].contains("never asks the token limit"));
    // A Python helper the same way.
    let py = "def call(client):\n    admit(client, t, team)\n    urlopen('https://api.anthropic.com/v1/messages')\n    _record_attempt(client)\n";
    assert!(check_source("lambda/log-analyzer/usage.py", py).is_empty());
    let py = py.replace("    admit(client, t, team)\n", "");
    assert_eq!(check_source("lambda/log-analyzer/usage.py", &py).len(), 1);
}

#[test]
fn any_messages_url_or_sdk_counts_as_a_call() {
    // A URL built from a base, not the literal host.
    let src = "async fn f(base: &str) {\n    http.post(format!(\"{base}/v1/messages\"));\n}\n";
    assert_eq!(
        check_source("services/worker/src/passes/x.rs", src).len(),
        1
    );
    let src = "async fn f() {\n    http.post(common::model_stream::MESSAGES_URL);\n}\n";
    assert_eq!(
        check_source("services/gateway/src/routes/x.rs", src).len(),
        1
    );
    let py = "import anthropic\n";
    assert_eq!(check_source("lambda/updater/handler.py", py).len(), 1);
    let js = "import Anthropic from \"@anthropic-ai/sdk\";\n";
    assert_eq!(check_source("plugins/x/handler.mjs", js).len(), 1);
    let js = "await sh(\"aws\", [\"bedrock-runtime\", \"converse\"]);\n";
    assert_eq!(check_source("selfhost/launch/x.mjs", js).len(), 1);
    // A test module's fake server is not a call.
    let src = "fn real() {}\n#[cfg(test)]\nmod tests {\n    fn t() { let u = format!(\"http://{a}/v1/messages\"); }\n}\n";
    assert!(check_source("services/common/src/model_retry.rs", src).is_empty());
}
