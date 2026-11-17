//! bu. A coding run needed to know whether a vendor's page could be shown
//! in an iframe (its `X-Frame-Options` and `frame-ancestors`) and what the
//! vendor's embed script did. The coder sent `curl -sI <page>` and `curl
//! -sS -m 20 <script>`; both came back "Not allowed: not a command for
//! here", and the reason reached the run page only after the step was
//! saved. A week of runs showed the same wall for other shapes: reads
//! chained with `;`, `git show`/`git log`, and `corepack enable && pnpm
//! install && <test>` chains. Meanwhile every `pnpm ...` command the
//! sandbox did run ended with exit 127 after about 36s: the sandbox ran the
//! bare command, with no install and no pnpm, and the coder sent the same
//! command again and again.
//!
//! Guarantee: a read-only `curl`/`wget` is a page fetch with the page
//! tool's safety checks, returning the status, every header and part of
//! the body; one that writes, sends data or credentials is refused with
//! the reason and the read-only form. Chained reads run as their tools;
//! chained sandbox commands run as one build with each part asked of the
//! gate; git history reads run as the history tool. A sandbox command runs
//! after the repository's install, and a program the sandbox still lacks
//! is diagnosed once, with the routes that work, and never built for
//! again in that step. Every refusal names what to use instead, and the
//! run log's line says which refusal it was.

use common::coder_shell::{classify, Shell};

use crate::passes::implement::{
    command_script_with, missing_note, part_program, refusal_summary, setup_for,
};

const CHECKS: &str =
    "corepack enable && pnpm install --frozen-lockfile && pnpm --filter web check-types";

#[test]
fn the_page_checks_run_as_fetches() {
    for line in [
        "curl -sI https://www.example.com/program-portals/1",
        "curl -sS -m 20 https://www.example.com/statics/portal-embed.js",
    ] {
        match classify(line) {
            Shell::Fetch(f) => assert_eq!(f.urls.len(), 1, "{line}"),
            other => panic!("{line}: {other:?}"),
        }
    }
    match classify("curl -X POST -d a=1 https://www.example.com/api") {
        Shell::Refuse(why) => {
            assert!(
                why.contains("POST") && why.contains("curl -sI URL"),
                "{why}"
            );
            assert_eq!(
                refusal_summary(&why),
                "not run: This `curl` sends a POST request, which can change data, so it doesn't run here"
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_test_command_runs_after_the_install_and_a_missing_tool_is_said_once() {
    // A plain test command: the repository's whole install runs first.
    let parts = vec!["pnpm --filter web vitest run src/a.test.ts".to_string()];
    let setup = setup_for(Some(CHECKS), &parts);
    assert_eq!(
        setup.as_deref(),
        Some("corepack enable && pnpm install --frozen-lockfile")
    );
    let script = command_script_with(setup.as_deref(), &parts[0], None);
    let install_at = script.find("pnpm install").expect("installs");
    let run_at = script.find("vitest run").expect("runs");
    assert!(install_at < run_at, "{script}");
    assert!(script.contains("coderhelm-setup.log"), "{script}");
    // The coder's own install (a sub package): only corepack first.
    let own = match classify("cd e2e && pnpm install --ignore-workspace && pnpm lint") {
        Shell::Run { parts, .. } => parts,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        setup_for(Some(CHECKS), &own).as_deref(),
        Some("corepack enable")
    );
    // A repository whose checks install nothing.
    assert_eq!(setup_for(Some("cargo test"), &parts), None);
    // exit 127: what is missing and the routes that work.
    let note = missing_note("pnpm", setup.as_deref());
    assert!(
        note.contains("`pnpm` isn't installed in the sandbox"),
        "{note}"
    );
    assert!(note.contains("run_checks") && note.contains("won't start another build"));
    assert_eq!(part_program("CI=1 TZ=UTC pnpm test"), "pnpm");
    assert_eq!(part_program("./node_modules/.bin/vitest run"), "vitest");
}
