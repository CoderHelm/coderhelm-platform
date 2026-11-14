//! bd. A team's report skill (merge rate by contributor across its
//! repositories) was judged "Needs tools: 3 of 7 steps run as written, 1
//! translated, 3 need tools". Its login check (`gh auth status`) was called
//! a gap and "left out of the translation"; its output folder was "Nothing
//! can be written outside the repository"; its bundled script was
//! "Coderhelm cannot run shell commands or scripts"; the files the script
//! writes were "only produced by the script, which cannot run in Coderhelm".
//! Coderhelm has a code sandbox, reads GitHub through the team's
//! installation, and can keep a run's files: the check's list of what
//! Coderhelm can do left them out, and its rules called every script and
//! every write outside the repository impossible.
//!
//! Guarantee: every reader of text written for a person's machine applies
//! one rule from `common::host_steps`. A step that only sets up a laptop
//! (a login check, an output folder, an install, a `cd`) is not needed on
//! Coderhelm: left out silently, never a gap, never counted against the
//! item. The script runs in the sandbox (`run_skill_script`) with a
//! read-only token for the repositories it reads, taken out of everything
//! that comes back; its files are kept as the run's outputs, read back with
//! `read_output` and linked. The check reads "7 of 7 steps run", and a
//! re-check against the new catalogue says which steps changed.
use crate::clients::sandbox::{script_outcome, Captured};
use crate::models::TokenUsage;
use crate::passes::skill_compat::{self as check, Checker};
use crate::passes::skill_run;
use common::skill_compat::{self, Inventory, Last};
use common::skills::{Kind, SkillMeta};
use serde_json::{json, Value};

const TOKEN: &str = "ghs_madeUpTokenForTheIncident0123";

fn item() -> SkillMeta {
    SkillMeta {
        repo: "acme/team-playbook".into(),
        path: "skills/report-rate/SKILL.md".into(),
        kind: Kind::Skill,
        name: "report-rate".into(),
        description: "Merge rate by contributor".into(),
        sha: "s".into(),
        hash: "h2".into(),
        enabled: true,
        files: vec![
            "skills/report-rate/report_rate.py".into(),
            "skills/report-rate/test_report_rate.py".into(),
        ],
        missing_tools: vec![],
        uses: 0,
        last_used_at: String::new(),
        last_used_by: String::new(),
        updated_at: String::new(),
        asks: 0,
        last_asked_at: String::new(),
        switched_by: String::new(),
        switched_at: String::new(),
        compat: Default::default(),
        always: Default::default(),
        suggestion: Default::default(),
        reviews: Default::default(),
    }
}

const BODY: &str = "---\nname: report-rate\ndescription: Merge rate by contributor\n---\n\
1. Parse the arguments: repos, since, until, --verify.\n\
2. Choose an output directory, ./reports/<date>.\n\
3. Check access with `gh auth status`; if it fails, tell the user to log in.\n\
4. Run `python3 report_rate.py --repos <repos> --out <dir>`: it fetches every pull request \
through GitHub GraphQL, saves a snapshot and writes CSVs, summary.json, tables.md and \
report.html.\n\
5. Read tables.md and summary.json.\n\
6. Write a narrative report from the tables with a link to report.html.\n\
7. Tell the user where everything is.\n";

fn q(asks: &str, kind: &str, status: &str, uses: &[&str], write: &str) -> Value {
    let missing = status == "missing";
    json!({ "asks": asks, "kind": kind, "status": status, "uses": uses, "write": write,
            "needs": if missing { "Shell access" } else { "" },
            "fix": if missing { "Coderhelm cannot run shell commands or scripts" } else { "" },
            "connect": if missing { "github" } else { "none" },
            "server": "", "connection": "" })
}

fn s(title: &str, dropped: bool, reqs: Vec<Value>) -> Value {
    json!({ "title": title, "does": title, "dropped": dropped, "requirements": reqs })
}

/// What the check answered before: no sandbox, no outputs in its list.
fn before() -> Value {
    json!({
        "steps": [
            s("Parse the arguments", false, vec![]),
            s("Choose an output directory", false, vec![q("./reports/<date>", "path", "missing", &[], "repo")]),
            s("Check access", true, vec![q("gh auth status", "command", "missing", &[], "none")]),
            s("Run the bundled script", false, vec![
                q("python3 report_rate.py", "command", "missing", &[], "none"),
                q("Pull request history", "data", "translated", &["github_history"], "none")]),
            s("Read the tables", false, vec![q("tables.md and summary.json", "path", "missing", &[], "none")]),
            s("Write the narrative report", false, vec![]),
            s("Tell the user where everything is", false, vec![q("Tell the user", "person", "available", &[], "none")]),
        ],
        "translated": "GAP: Run the bundled script.",
        "reviews": []
    })
}

/// What it answers with the shared rule and the full catalogue.
fn after() -> Value {
    json!({
        "steps": [
            s("Parse the arguments", false, vec![]),
            s("Choose an output directory", true, vec![q("./reports/<date>", "path", "missing", &[], "repo")]),
            s("Check access", true, vec![q("gh auth status", "command", "missing", &[], "none")]),
            s("Run the bundled script", false, vec![
                q("python3 report_rate.py", "command", "translated", &["run_skill_script"], "none"),
                q("Pull request history through GitHub GraphQL", "data", "translated", &["run_skill_script"], "none")]),
            s("Read the tables", false, vec![q("tables.md and summary.json", "path", "translated", &["read_output"], "none")]),
            s("Write the narrative report", false, vec![q("report.md", "path", "translated", &["save_output"], "none")]),
            s("Tell the user where everything is", false, vec![q("Tell the user", "person", "available", &[], "none")]),
        ],
        "translated": "1. Read the arguments.\n2. Run `run_skill_script` with skill:report-rate, report_rate.py, `--repos <repos> --out {out}`, and the repositories for its token.\n3. Read tables.md and summary.json with `read_output`.\n4. Write the narrative and keep it with `save_output`, linking report.html.\n5. Give the links to the outputs.",
        "reviews": []
    })
}

struct Answer(Value);

#[async_trait::async_trait]
impl Checker for Answer {
    fn model(&self) -> &str {
        "fake-light"
    }
    async fn check(
        &self,
        system: &str,
        shared: &str,
        _user: &str,
        _schema: &Value,
        _attempt: usize,
    ) -> (Result<Value, String>, TokenUsage) {
        // The check reads the shared rule, and the inventory it is given.
        assert!(system.contains(common::host_steps::PURPOSE));
        assert!(shared.starts_with("## Coderhelm inventory"));
        (Ok(self.0.clone()), TokenUsage::default())
    }
}

/// The inventory the check used before: no script tool, no catalogue.
fn old_inventory() -> Inventory {
    let builtin: Vec<_> = check::builtin_tools()
        .into_iter()
        .filter(|t| !skill_run::is_tool(&t.name))
        .collect();
    Inventory {
        translations: check::translations(&builtin),
        builtin,
        ..Default::default()
    }
    .sorted()
}

/// The inventory now, on an install with the sandbox.
fn inventory() -> Inventory {
    let builtin = check::builtin_for(true);
    Inventory {
        translations: check::translations(&builtin),
        builtin,
        capabilities: common::host_steps::catalogue(&check::setup(true, &[], &[], false)),
        ..Default::default()
    }
    .sorted()
}

#[tokio::test]
async fn every_step_of_the_report_skill_runs_on_coderhelm() {
    let m = item();
    let old = old_inventory();
    let (last, last_t) = check::check_item(&Answer(before()), &old, &m, BODY)
        .await
        .unwrap();
    assert_eq!(last.status, "needs_tools");
    assert_eq!(last.counts.missing, 3, "{}", last.summary);

    let inv = inventory();
    // The catalogue the check reads lists the sandbox and the outputs.
    let shared = skill_compat::inventory_message(&inv);
    assert!(shared.contains("\"capabilities\"") && shared.contains("Code sandbox"));
    assert!(shared.contains("Work outputs"));
    assert!(!skill_compat::same_tools(
        &inv,
        &old.fingerprint(),
        skill_compat::FORMAT
    ));
    let (new, new_t) = check::check_item(&Answer(after()), &inv, &m, BODY)
        .await
        .unwrap();
    let (a, translated) = skill_compat::stabilize(
        new,
        new_t,
        &Last {
            analysis: &last,
            translated: &last_t,
            tools_changed: true,
        },
        &inv,
    );
    let (a, translated) = skill_compat::enforce(a, translated, &inv);

    assert_eq!(a.status, "translated");
    assert_eq!(
        a.summary,
        "7 of 7 steps run · 3 with Coderhelm's tools · 2 not needed here"
    );
    assert!(a.needs.is_empty(), "{:?}", a.needs);
    let st: Vec<&str> = a.steps.iter().map(|s| s.status.as_str()).collect();
    assert_eq!(
        st,
        [
            "available",
            "not_needed",
            "not_needed",
            "translated",
            "translated",
            "translated",
            "available"
        ]
    );
    // The re-check says what got better.
    for (step, from, to) in [
        ("Choose an output directory", "missing", "not needed"),
        ("Run the bundled script", "missing", "translated"),
        ("Read the tables", "missing", "translated"),
        ("Write the narrative report", "available", "translated"),
    ] {
        let want = format!("Step \"{step}\" changed from {from} to {to}");
        assert!(
            a.notes.iter().any(|n| n.contains(&want)),
            "{want}: {:?}",
            a.notes
        );
    }
    // The old verdicts never come back.
    let shown = format!("{} {:?} {translated}", a.summary, a.needs);
    for old in [
        "Coderhelm cannot run shell commands",
        "Nothing can be written outside the repository",
        "Assumes a local machine",
        "left out of the translation",
    ] {
        assert!(!shown.contains(old), "{old}");
    }
    assert!(skill_compat::violations(&a, &translated, &inv).is_empty());
    assert!(translated.contains("`run_skill_script`") && translated.contains("{out}"));
}

#[test]
fn the_script_run_keeps_its_files_as_outputs_and_never_its_token() {
    // The command the translated step runs: the skill's own script, its
    // output folder as the sandbox's.
    let cmd = skill_run::script_command(
        "report_rate.py",
        &[
            "--repos".into(),
            "acme/web,acme/api".into(),
            "--out".into(),
            "{out}".into(),
        ],
    )
    .unwrap();
    assert!(cmd.starts_with("python3 'report_rate.py'") && cmd.contains("\"$CODERHELM_OUT\""));
    // The token covers only the team's repositories it names.
    let team = vec!["acme/web".to_string(), "acme/api".to_string()];
    assert_eq!(
        skill_run::token_repos(&["acme/web".into(), "acme/api".into()], &team).unwrap(),
        ["web", "api"]
    );
    assert!(skill_run::token_repos(&["acme/payroll".into()], &team).is_err());
    // A script that printed its token: the log and every file come back
    // without it.
    let log = format!(
        "Running command bash -c \"$SCRIPT_CMD\"\n===CODERHELM_SCRIPT_START===\nusing token {TOKEN}\nfetched 412 pull requests\n===CODERHELM_SCRIPT_END exit=0===\n===CODERHELM_SCRIPT_UPLOADED===\n"
    );
    let out = script_outcome(
        &log,
        Captured {
            files: vec![
                (
                    "summary.json".into(),
                    format!("{{\"merged\": 301, \"debug\": \"{TOKEN}\"}}"),
                ),
                ("tables.md".into(), "| author | merged |\n".into()),
                ("report.html".into(), "<html></html>".into()),
            ],
            dropped: vec![],
        },
    );
    assert!(out.ran && out.exit_code == Some(0));
    assert!(out.output.contains("fetched 412 pull requests"));
    assert!(!out.output.contains("CODERHELM_SCRIPT_END"));
    let shown = skill_run::redact(&out.output, Some(TOKEN));
    assert!(!shown.contains(TOKEN) && shown.contains(skill_run::REDACTED));
    for (name, body) in &out.files {
        let kept = skill_run::redact(body, Some(TOKEN));
        assert!(!kept.contains(TOKEN), "{name}");
        assert!(common::outputs::clean_name(name).is_ok());
    }
    // Each output is linked on the run's page; HTML downloads, never renders
    // as the dashboard's page.
    assert_eq!(
        common::outputs::page_link("https://app.example.com", "run-1", "report.html"),
        "https://app.example.com/runs/detail?id=run-1&output=report.html#outputs"
    );
    assert!(common::outputs::disposition("report.html").starts_with("attachment"));
}
