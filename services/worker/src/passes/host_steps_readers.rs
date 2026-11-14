//! Guard: every place that reads instructions written for a person's
//! machine (team skills, team agents, AGENTS.md, CLAUDE.md, `.claude`,
//! CONTRIBUTING and README setup notes, plans, ticket text, the steps
//! written for after a pull request, the reviewer's guidance) goes through
//! `common::host_steps`, so one rule and one capability catalogue decide
//! each step everywhere.
//!
//! A source file that names such instructions is either a reader listed in
//! [`ROUTED`] (and calls `host_steps`), or listed in [`NOT_A_READER`] with
//! why. A new file that starts reading them fails here until it is routed or
//! explained.

#[cfg(test)]
mod tests {
    use common::host_steps::{Reader, READERS};

    /// Readers that go through `host_steps`, with the readers each covers.
    const ROUTED: &[(&str, &[Reader])] = &[
        (
            "common/src/skill_compat.rs",
            &[Reader::TeamItems, Reader::TeamAgents],
        ),
        ("common/src/skills.rs", &[Reader::TeamAgents]),
        (
            "common/src/setup/ai.rs",
            &[Reader::SetupDocs, Reader::RepoGuidance],
        ),
        (
            "worker/src/passes/implement.rs",
            &[Reader::RepoGuidance, Reader::TeamItems, Reader::TicketText],
        ),
        (
            "worker/src/passes/plan.rs",
            &[
                Reader::Plan,
                Reader::OutsideSteps,
                Reader::RepoGuidance,
                Reader::TeamItems,
                Reader::TicketText,
            ],
        ),
        (
            "worker/src/passes/plan_chat.rs",
            &[Reader::Plan, Reader::TeamItems],
        ),
        (
            "worker/src/passes/review_agent.rs",
            &[Reader::Review, Reader::RepoGuidance, Reader::TeamItems],
        ),
        ("worker/src/passes/review_pr.rs", &[Reader::Review]),
        (
            "worker/src/passes/review_fix/suite.rs",
            &[Reader::SetupDocs],
        ),
        ("worker/src/passes/skill_compat.rs", &[Reader::TeamItems]),
        ("worker/src/passes/skill_run.rs", &[Reader::TeamItems]),
    ];

    /// Files that name such instructions without following their steps.
    const NOT_A_READER: &[(&str, &str)] = &[
        ("common/src/host_steps.rs", "the shared module itself"),
        ("common/src/agent_file.rs", "writes Coderhelm's own agent file"),
        ("common/src/guide.rs", "writes the repository's guide"),
        ("common/src/repo_read.rs", "lists files for reading code"),
        ("common/src/pr_conventions.rs", "reads how pull request titles are written, not steps to run"),
        ("common/src/setup/facts.rs", "gathers the facts setup/ai.rs reads (routed)"),
        ("common/src/skill_apply.rs", "fits team standards into a reader's prompt; each reader is routed"),
        ("common/src/tool_registry.rs", "tool risk table"),
        ("common/src/agent_policy/registry.rs", "tool risk table"),
        ("common/src/agent_policy/model.rs", "records an admin's accepted sandbox host"),
        ("common/src/agent_policy/sandbox.rs", "suggests sandbox hosts from AGENTS.md for an admin to accept; runs nothing"),
        ("common/src/plugins/manifest.rs", "plugin skills are written for Coderhelm's plugin runtime, not a laptop"),
        ("common/src/plugins/package.rs", "plugin skills are written for Coderhelm's plugin runtime, not a laptop"),
        ("common/src/plugins/release_check.rs", "plugin skills are written for Coderhelm's plugin runtime, not a laptop"),
        ("worker/src/passes/mod.rs", "assembles the coder's instructions; implement.rs adds the shared section to that prompt"),
        ("worker/src/passes/skills.rs", "serves read_skill and asks team agents; the reading agent's prompt carries the section, and the asked agent's comes from common/src/skills.rs"),
        ("worker/src/passes/cross_repo.rs", "a line in the planner's prompt, which carries the section"),
        ("worker/src/passes/e2e.rs", "writes a ticket that a coding run (routed) carries out"),
        ("worker/src/passes/infra_repo.rs", "writes a checklist line the coder (routed) follows"),
        ("worker/src/passes/generators.rs", "finds the repository's code generators, which run in the sandbox"),
        ("worker/src/passes/guide_pr.rs", "writes the repository's guide"),
        ("worker/src/passes/onboard.rs", "writes Coderhelm's own agent file"),
        ("worker/src/passes/release_notes.rs", "writes release notes in a guide's wording from facts it is given, with no tools and no steps to run"),
        ("worker/src/passes/review_labels.rs", "picks labels the repository's docs allow"),
        ("worker/src/passes/review_scope.rs", "decides which files a review reads"),
        ("worker/src/passes/host_steps_readers.rs", "this guard"),
        ("gateway/src/routes/brain.rs", "chat answers questions; it offers read_skill as a source and carries out no steps"),
        ("gateway/src/routes/chat_code.rs", "chat's code reads"),
        ("gateway/src/routes/chat_eval.rs", "chat evaluations"),
        ("gateway/src/routes/learning.rs", "shows where lessons were written"),
        ("gateway/src/routes/memory.rs", "describes what reviews read"),
        ("gateway/src/routes/plan_chat_tools.rs", "forwards the plan chat's tools to the worker's plan_chat (routed)"),
    ];

    const MARKERS: &[&str] = &[
        "AGENTS.md",
        "CLAUDE.md",
        "CONTRIBUTING",
        "read_skill",
        "SKILL.md",
    ];

    fn services() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf()
    }

    fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                let name = p.file_name().unwrap().to_string_lossy().to_string();
                if name == "incidents" || name.ends_with("_cases") || name == "testdata" {
                    continue;
                }
                rust_files(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }

    #[test]
    fn every_routed_reader_calls_the_shared_module() {
        let root = services();
        for (file, readers) in ROUTED {
            let text = std::fs::read_to_string(root.join(file)).unwrap();
            assert!(
                text.contains("host_steps::") || text.contains("crate::host_steps"),
                "{file} reads instructions written for a person's machine but doesn't go through host_steps"
            );
            assert!(!readers.is_empty(), "{file}");
        }
        // Every kind of reader has at least one routed place.
        for r in READERS {
            assert!(
                ROUTED.iter().any(|(_, rs)| rs.contains(&r)),
                "no routed reader for {}",
                r.as_str()
            );
        }
    }

    #[test]
    fn a_new_reader_of_person_instructions_cannot_bypass_the_shared_module() {
        let root = services();
        let mut files = Vec::new();
        for d in ["worker/src", "common/src", "gateway/src"] {
            rust_files(&root.join(d), &mut files);
        }
        let mut unlisted = Vec::new();
        for p in files {
            let rel = p
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            let text = std::fs::read_to_string(&p).unwrap();
            if !MARKERS.iter().any(|m| text.contains(m)) {
                continue;
            }
            let listed = ROUTED.iter().any(|(f, _)| *f == rel)
                || NOT_A_READER.iter().any(|(f, _)| *f == rel);
            if !listed {
                unlisted.push(rel);
            }
        }
        assert!(
            unlisted.is_empty(),
            "These files name instructions written for a person's machine but are neither routed \
             through common::host_steps nor listed as not a reader: {unlisted:?}"
        );
        for (f, why) in NOT_A_READER {
            assert!(root.join(f).exists(), "{f} is listed but gone");
            assert!(!why.is_empty());
        }
    }

    #[test]
    fn each_reader_gets_the_rule_in_its_prompt() {
        let caps = crate::passes::skill_run::standard_caps();
        // The planner and plan chat: plan steps and the steps for after the
        // pull request.
        let planner = common::host_steps::section(&crate::passes::plan::PLANNER_READERS, &caps);
        assert!(planner.contains(common::host_steps::PURPOSE));
        assert!(planner.contains("never give a person a step a capability below lets Coderhelm do"));
        // The reviewer.
        let reviewer =
            common::host_steps::section(&crate::passes::review_agent::REVIEW_READERS, &caps);
        assert!(reviewer.contains("its laptop setup is not a rule for the change"));
        // How the tests run: setup docs.
        let suite = crate::passes::review_fix::suite::system();
        assert!(suite.contains(common::host_steps::PURPOSE));
        assert!(suite.contains("CONTRIBUTING and README setup and test instructions"));
        // The settings drafted from the README and AGENTS.md.
        let setup = common::setup::ai::system(&caps);
        assert!(setup.starts_with(common::setup::ai::SYSTEM));
        assert!(setup.contains(common::host_steps::PURPOSE));
        // A team agent: only what it reads with.
        let (_, agent) =
            common::skills::agent_system_prompt("---\nname: a\n---\nCheck specs.", "planner", &[]);
        assert!(agent.contains(common::host_steps::PURPOSE));
        assert!(!agent.contains("- Code sandbox"));
        // The skill check.
        assert!(common::skill_compat::system().contains(common::host_steps::PURPOSE));
    }
}
