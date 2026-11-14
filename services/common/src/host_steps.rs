//! Instructions written for a person's machine, read on Coderhelm.
//!
//! Teams write skills, commands, agent briefs, AGENTS.md, CLAUDE.md,
//! CONTRIBUTING and README setup notes, ticket text and comments for someone
//! sitting at a laptop with a terminal. Every place Coderhelm reads such text
//! decides each step the same way, from one rule ([`PURPOSE`]) and one list
//! of what Coderhelm can do ([`catalogue`]):
//!
//! 1. **Not needed here**: the step only exists because the text assumed a
//!    person's own computer (sign in, check a login, install a tool, pick or
//!    create an output folder, change directory). It is left out silently:
//!    no equivalent check, no gap. If Coderhelm can't reach something, the
//!    real step that uses it fails and says so.
//! 2. **Runs as written**: Coderhelm has exactly that.
//! 3. **Runs with Coderhelm's equivalent**: a capability below does it (a
//!    script runs in the sandbox, files go to the work's outputs, GitHub is
//!    read through the team's installation).
//! 4. **Missing**: nothing does it; the step names the one action (a
//!    setting, a connection, a permission) that makes it available.
//!
//! Every reader listed in [`READERS`] goes through [`section`] (models that
//! follow the text as they work) or through the skill check
//! (`skill_compat`, which keeps a verdict per step, cached by the item's
//! content hash and re-checked when this catalogue changes, because the
//! catalogue is part of the inventory's fingerprint). Adding a capability
//! here improves every reader at once.

use serde::{Deserialize, Serialize};

/// Bumped when the rule or the catalogue's wording changes, so kept
/// verdicts are checked again.
pub const VERSION: u32 = 1;

/// One thing Coderhelm can do that a person's machine would otherwise do.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capability {
    /// Stable id: see the `ID_*` constants.
    pub id: String,
    pub name: String,
    /// What it does, in one sentence.
    pub does: String,
    /// What it never does, or its bounds.
    pub limits: String,
    /// The tools agents call for it (empty when it works by itself).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<String>,
    /// Whether this team has it right now.
    pub available: bool,
    /// When not available: the one action that makes it available.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub enable: String,
    /// When not available: the dashboard page where that is done.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub link: String,
}

pub const ID_SANDBOX: &str = "sandbox";
pub const ID_GITHUB: &str = "github";
pub const ID_OUTPUTS: &str = "outputs";
pub const ID_DATA_SOURCES: &str = "data_sources";
pub const ID_APPS: &str = "connected_apps";
pub const ID_KNOWLEDGE: &str = "knowledge";
pub const ID_CODE_GRAPH: &str = "code_graph";
pub const ID_MEMORY: &str = "team_memory";

/// The tool that runs a script kept with a skill (or in the repository) in
/// the sandbox.
pub const RUN_SCRIPT: &str = "run_skill_script";
/// The tool that keeps a file with the work's outputs.
pub const SAVE_OUTPUT: &str = "save_output";
/// The tool that reads a file back from the work's outputs.
pub const READ_OUTPUT: &str = "read_output";

/// What a team has set up, as far as the catalogue cares.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Setup {
    /// The install has the code sandbox (it runs checks and scripts).
    pub sandbox: bool,
    /// Connected data sources by name, with the tools that read them.
    pub data_sources: Vec<(String, Vec<String>)>,
    /// MCP servers switched on with credentials saved, by name, with tools.
    pub apps: Vec<(String, Vec<String>)>,
    /// Knowledge bases attached.
    pub knowledge: bool,
}

fn cap(id: &str, name: &str, does: &str, limits: &str, tools: &[&str]) -> Capability {
    Capability {
        id: id.into(),
        name: name.into(),
        does: does.into(),
        limits: limits.into(),
        tools: tools.iter().map(|t| t.to_string()).collect(),
        available: true,
        enable: String::new(),
        link: String::new(),
    }
}

fn off(mut c: Capability, enable: &str, link: &str) -> Capability {
    c.available = false;
    c.enable = enable.into();
    c.link = link.into();
    c
}

/// Everything Coderhelm can do in place of a person's machine, each with its
/// limits and, when the team doesn't have it, the one action that adds it.
/// Sorted by id, so equal setups give equal fingerprints.
pub fn catalogue(s: &Setup) -> Vec<Capability> {
    let sandbox = cap(
        ID_SANDBOX,
        "Code sandbox",
        "Runs code in an isolated build container: the repository's own checks and code \
         generation, and a script kept with a team skill or in the repository \
         (run_skill_script) with the arguments the step gives. Python 3, Node, bash and \
         the usual build tools are there. A script that reads GitHub gets a read-only \
         token for the repositories it names, as GH_TOKEN and GITHUB_TOKEN. Files it \
         writes to $CODERHELM_OUT come back as the work's outputs.",
        "No interactive terminal and nothing from a person's computer. At most 15 \
         minutes a run. The network follows the team's sandbox allowlist (GitHub's API \
         is added for a script given a token). The token is read-only, limited to the \
         named repositories of the team's installation, revoked when the script ends, \
         and never printed or kept in outputs.",
        &[RUN_SCRIPT, "run_checks", "run_codegen"],
    );
    let sandbox = if s.sandbox {
        sandbox
    } else {
        off(
            sandbox,
            "Set up the code sandbox for this install (the worker stack's sandbox bucket and project).",
            "/settings/agents",
        )
    };
    let data = if s.data_sources.is_empty() {
        off(
            cap(
                ID_DATA_SOURCES,
                "Data sources",
                "Reads the team's connected databases, APIs, logs and search clusters.",
                "Reads only, one query per lookup; writes only where an admin turned them on.",
                &[],
            ),
            "Connect the database, API or logs in Data sources.",
            "/data-sources",
        )
    } else {
        let mut c = cap(
            ID_DATA_SOURCES,
            "Data sources",
            &format!(
                "Reads the team's connected sources: {}.",
                s.data_sources
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            "Reads only, one query per lookup; writes only where an admin turned them on.",
            &[],
        );
        let mut tools: Vec<String> = s
            .data_sources
            .iter()
            .flat_map(|(_, t)| t.iter().cloned())
            .collect();
        tools.sort();
        tools.dedup();
        c.tools = tools;
        c
    };
    let apps = if s.apps.is_empty() {
        off(
            cap(
                ID_APPS,
                "Connected apps",
                "Uses the team's connected apps (MCP servers) when the work names them.",
                "Only servers switched on with their credentials saved.",
                &[],
            ),
            "Connect the app in Settings, Integrations.",
            "/settings/integrations#other-tools",
        )
    } else {
        let mut c = cap(
            ID_APPS,
            "Connected apps",
            &format!(
                "Uses the team's connected apps: {}.",
                s.apps
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            "Only servers switched on with their credentials saved; each acts with its own token.",
            &[],
        );
        let mut tools: Vec<String> = s.apps.iter().flat_map(|(_, t)| t.iter().cloned()).collect();
        tools.sort();
        tools.dedup();
        c.tools = tools;
        c
    };
    let knowledge = cap(
        ID_KNOWLEDGE,
        "Knowledge",
        "Searches the team's knowledge bases (docs, wikis, files it uploaded).",
        "Reads only what was attached.",
        &["search_knowledge"],
    );
    let knowledge = if s.knowledge {
        knowledge
    } else {
        off(
            knowledge,
            "Attach a knowledge base in Knowledge.",
            "/knowledge",
        )
    };
    let mut out = vec![
        sandbox,
        cap(
            ID_GITHUB,
            "GitHub through the team's installation",
            "Already signed in as the team's GitHub App: reads and searches the repository, \
             compares branches, reads history (commits, blame, pull requests, reviews, \
             releases, deployments, Actions runs) through REST and GraphQL, and opens and \
             updates pull requests. No login step is ever needed.",
            "Only repositories the installation covers; GraphQL reads are checked as \
             read-only before they are sent.",
            &[
                "read_file",
                "search_code",
                "get_diff",
                "github_history",
                RUN_SCRIPT,
            ],
        ),
        cap(
            ID_OUTPUTS,
            "Work outputs",
            "Keeps files the work makes (reports, CSV, JSON, HTML, snapshots) with the run, \
             each with a link on the run's page, instead of a folder on someone's computer; \
             reads them back later in the same work.",
            "Up to 50 files of 5 MB each per run, text only; never committed unless the \
             work asks for that.",
            &[SAVE_OUTPUT, READ_OUTPUT],
        ),
        data,
        apps,
        knowledge,
        cap(
            ID_CODE_GRAPH,
            "Code graph",
            "Finds where a symbol is defined, who calls it and what a change reaches.",
            "Reflects the default branch.",
            &["graph_definition", "graph_callers", "graph_impact"],
        ),
        cap(
            ID_MEMORY,
            "Team memory",
            "Recalls what the team's earlier work learned and keeps what this work learns, \
             in place of a coding assistant's memory files.",
            "Works by itself; nothing to call.",
            &[],
        ),
    ];
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out
}

/// The catalogue every run has before a team's own connections are known:
/// GitHub, outputs, the code graph and memory, and the sandbox when the
/// install has one.
pub fn standard(sandbox: bool) -> Vec<Capability> {
    catalogue(&Setup {
        sandbox,
        ..Default::default()
    })
}

/// The capabilities a team agent reads with: it never runs a script, keeps
/// an output or uses a connected app.
pub fn for_agents(caps: &[Capability]) -> Vec<Capability> {
    caps.iter()
        .filter(|c| {
            [
                ID_GITHUB,
                ID_DATA_SOURCES,
                ID_KNOWLEDGE,
                ID_CODE_GRAPH,
                ID_MEMORY,
            ]
            .contains(&c.id.as_str())
        })
        .cloned()
        .collect()
}

/// Changes whenever the catalogue or the rule does.
pub fn fingerprint(caps: &[Capability]) -> String {
    let text = serde_json::to_string(caps).unwrap_or_default();
    crate::skills::content_hash(&format!("{VERSION}|{PURPOSE}|{text}"))
}

/// The rule every reader applies to each step it reads. Judged on the
/// step's purpose, never on a list of command names.
pub const PURPOSE: &str = "Decide every step by its purpose on Coderhelm, before its tools.

1. Not needed here. The step only exists because the text assumed a person's own computer and \
terminal: signing in or checking a login (for example a CLI's auth status), telling the person \
to log in, checking that a tool is installed, installing tools or packages to run it, choosing \
or asking where to save files, creating or clearing an output folder, changing directory, \
activating an environment, opening a file in an editor or a browser just to look. Coderhelm \
already acts through the team's GitHub App installation and its connections, keeps files with \
the work's outputs, and starts in the right place. Leave such a step out silently: don't \
translate it into a check of your own, don't report it as a gap. If Coderhelm can't reach \
something, the real step that uses it fails and says so.
2. Real work is never left out: fetching data, computing, running the team's own script, \
writing a file or a report, reading results back, telling the person what came out. Map each \
to the best Coderhelm has:
   - runs as written when Coderhelm has exactly that;
   - runs with Coderhelm's equivalent from the capabilities: a script or program does its work \
in the sandbox (run_skill_script for a script kept with a skill or in the repository, with a \
read-only GitHub token when it reads GitHub); files the text saves to a local folder are kept \
with save_output and read back with read_output, and the person gets their links; GitHub \
reads (REST or GraphQL) go through the installation; data comes from the connected data \
sources and apps;
   - missing only when no capability does it, and then name the one action that makes it \
available (a setting, a connection, a permission).
When unsure whether a step is setup or work, it is work.";

/// Everywhere Coderhelm reads instructions written for a person's machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reader {
    /// Team skills and commands, and their bundled files (the skill check
    /// and `read_skill`).
    TeamItems,
    /// Team agents, asked a question.
    TeamAgents,
    /// AGENTS.md, CLAUDE.md and `.claude` guidance the coder and planner
    /// read in the repository.
    RepoGuidance,
    /// CONTRIBUTING and README setup and test instructions the worker
    /// follows to build or verify.
    SetupDocs,
    /// The plan's steps.
    Plan,
    /// Ticket text and comments that tell Coderhelm to run something.
    TicketText,
    /// The steps outside the code Coderhelm writes down, some for people.
    OutsideSteps,
    /// The reviewer reading the repository's and team's guidance.
    Review,
}

/// Every reader, for the guard tests.
pub const READERS: [Reader; 8] = [
    Reader::TeamItems,
    Reader::TeamAgents,
    Reader::RepoGuidance,
    Reader::SetupDocs,
    Reader::Plan,
    Reader::TicketText,
    Reader::OutsideSteps,
    Reader::Review,
];

impl Reader {
    pub fn as_str(self) -> &'static str {
        match self {
            Reader::TeamItems => "team_items",
            Reader::TeamAgents => "team_agents",
            Reader::RepoGuidance => "repo_guidance",
            Reader::SetupDocs => "setup_docs",
            Reader::Plan => "plan",
            Reader::TicketText => "ticket_text",
            Reader::OutsideSteps => "outside_steps",
            Reader::Review => "review",
        }
    }

    /// What this reader reads, and what the rule means for it.
    fn line(self) -> &'static str {
        match self {
            Reader::TeamItems => {
                "This applies to the team's skills and commands you read, and to the files kept \
                 with them: run a kept script with run_skill_script when its step does real work."
            }
            Reader::TeamAgents => {
                "This applies to a team agent's brief: it answers from what it can read; setup \
                 for a person's machine in its brief is not needed here."
            }
            Reader::RepoGuidance => {
                "This applies to the repository's AGENTS.md, CLAUDE.md and .claude guidance: \
                 follow its conventions and real commands; its laptop setup is not needed here."
            }
            Reader::SetupDocs => {
                "This applies to CONTRIBUTING and README setup and test instructions: keep the \
                 commands that build, install the project's own dependencies for a check, or \
                 test it; leave out logins, tool installs for a person's machine, editor and \
                 folder chores."
            }
            Reader::Plan => {
                "This applies to the plan's steps: never plan a step that is only laptop setup, \
                 and never hand a person a step Coderhelm can do with the capabilities below."
            }
            Reader::TicketText => {
                "This applies to the ticket and its comments: when they tell you to run \
                 something, do the real work with Coderhelm's equivalent; skip their laptop \
                 setup."
            }
            Reader::OutsideSteps => {
                "This applies to the steps you write for after the pull request: never write a \
                 laptop chore (log in, install a tool, open a terminal) as a step, and never \
                 give a person a step a capability below lets Coderhelm do."
            }
            Reader::Review => {
                "This applies to the guidance you review against: hold the code to its real \
                 rules; its laptop setup is not a rule for the change."
            }
        }
    }
}

/// The section a model is given for the readers it is (a coder reads the
/// repository's guidance, the team's skills and the ticket): the rule once,
/// what it means for each reader, and the capabilities (with the action
/// that adds a missing one).
pub fn section(readers: &[Reader], caps: &[Capability]) -> String {
    let mut lines: Vec<String> = Vec::new();
    for c in caps {
        if c.available {
            let tools = if c.tools.is_empty() {
                String::new()
            } else {
                format!(" ({})", c.tools.join(", "))
            };
            lines.push(format!("- {}{tools}: {} {}", c.name, c.does, c.limits));
        } else {
            lines.push(format!(
                "- {}: not set up for this team. {}",
                c.name, c.enable
            ));
        }
    }
    let applies: Vec<&str> = readers.iter().map(|r| r.line()).collect();
    format!(
        "\n\n## Instructions written for a person's machine\n{}\n\n{PURPOSE}\n\nWhat Coderhelm can do \
         instead of a person's machine:\n{}\n\n{}",
        applies.join("\n"),
        lines.join("\n"),
        crate::person_gate::prompt_section()
    )
}

/// The verdict on one step, as every reader names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    NotNeeded,
    AsWritten,
    Equivalent,
    Missing,
}

impl Verdict {
    /// From the skill check's step status.
    pub fn from_status(status: &str) -> Verdict {
        match status {
            NOT_NEEDED => Verdict::NotNeeded,
            "available" => Verdict::AsWritten,
            "translated" => Verdict::Equivalent,
            _ => Verdict::Missing,
        }
    }

    /// Whether the step runs on Coderhelm (or needs nothing there).
    pub fn runs(self) -> bool {
        !matches!(self, Verdict::Missing)
    }
}

/// The step status of a step that is not needed on Coderhelm.
pub const NOT_NEEDED: &str = "not_needed";

/// What a requirement of a step asks for, as far as the guard cares.
pub struct Need<'a> {
    pub kind: &'a str,
    pub write: &'a str,
}

/// Whether a step the model left out as not needed does real work, so it
/// must be kept: it relies on data, or writes to an outside system. A login
/// check, an output folder, an install or a `cd` relies on neither. This
/// holds the model's purpose judgment to the step's own requirements; it
/// is not a list of commands.
pub fn real_work(needs: &[Need<'_>]) -> bool {
    needs
        .iter()
        .any(|n| n.kind == "data" || n.write == "outside")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalogue_lists_every_real_capability_with_its_limits() {
        let caps = catalogue(&Setup {
            sandbox: true,
            data_sources: vec![("Warehouse".into(), vec!["db_query".into()])],
            apps: vec![],
            knowledge: false,
        });
        let ids: Vec<&str> = caps.iter().map(|c| c.id.as_str()).collect();
        for id in [
            ID_SANDBOX,
            ID_GITHUB,
            ID_OUTPUTS,
            ID_DATA_SOURCES,
            ID_APPS,
            ID_KNOWLEDGE,
            ID_CODE_GRAPH,
            ID_MEMORY,
        ] {
            assert!(ids.contains(&id), "{id}");
        }
        for c in &caps {
            assert!(!c.does.is_empty() && !c.limits.is_empty(), "{}", c.id);
            if !c.available {
                assert!(!c.enable.is_empty() && !c.link.is_empty(), "{}", c.id);
            }
        }
        let sandbox = caps.iter().find(|c| c.id == ID_SANDBOX).unwrap();
        assert!(sandbox.tools.contains(&RUN_SCRIPT.to_string()));
        assert!(sandbox.limits.contains("read-only") && sandbox.limits.contains("revoked"));
        let gh = caps.iter().find(|c| c.id == ID_GITHUB).unwrap();
        assert!(gh.does.contains("GraphQL") && gh.does.contains("No login"));
        let out = caps.iter().find(|c| c.id == ID_OUTPUTS).unwrap();
        assert_eq!(out.tools, [SAVE_OUTPUT, READ_OUTPUT]);
        let data = caps.iter().find(|c| c.id == ID_DATA_SOURCES).unwrap();
        assert!(data.available && data.tools == ["db_query"]);
        let apps = caps.iter().find(|c| c.id == ID_APPS).unwrap();
        assert!(!apps.available && apps.link.starts_with("/settings/integrations"));
    }

    #[test]
    fn an_install_without_a_sandbox_says_the_one_action_that_adds_it() {
        let caps = standard(false);
        let s = caps.iter().find(|c| c.id == ID_SANDBOX).unwrap();
        assert!(!s.available);
        assert!(s.enable.contains("sandbox"));
        assert_ne!(fingerprint(&caps), fingerprint(&standard(true)));
        assert_eq!(fingerprint(&standard(true)), fingerprint(&standard(true)));
    }

    #[test]
    fn every_reader_gets_the_same_rule_and_catalogue() {
        let caps = standard(true);
        for r in READERS {
            let s = section(&[r], &caps);
            assert!(s.contains(PURPOSE), "{}", r.as_str());
            assert!(
                s.contains(RUN_SCRIPT) && s.contains(SAVE_OUTPUT),
                "{}",
                r.as_str()
            );
            assert!(s.contains(r.line()), "{}", r.as_str());
        }
        let both = section(&[Reader::RepoGuidance, Reader::TicketText], &caps);
        assert_eq!(both.matches(PURPOSE).count(), 1);
        assert!(both.contains(Reader::TicketText.line()));
    }

    #[test]
    fn the_rule_drops_laptop_setup_and_keeps_real_work() {
        for chore in [
            "signing in or checking a login",
            "installing tools",
            "choosing or asking where to save files",
            "changing directory",
        ] {
            assert!(PURPOSE.contains(chore), "{chore}");
        }
        assert!(PURPOSE.contains("Real work is never left out"));
        assert!(PURPOSE.contains("When unsure whether a step is setup or work, it is work."));
    }

    #[test]
    fn a_step_that_relies_on_data_or_writes_outside_is_real_work() {
        let auth = [Need {
            kind: "command",
            write: "none",
        }];
        assert!(!real_work(&auth));
        let folder = [Need {
            kind: "path",
            write: "repo",
        }];
        assert!(!real_work(&folder));
        let fetch = [
            Need {
                kind: "command",
                write: "none",
            },
            Need {
                kind: "data",
                write: "none",
            },
        ];
        assert!(real_work(&fetch));
        let post = [Need {
            kind: "tool",
            write: "outside",
        }];
        assert!(real_work(&post));
    }

    #[test]
    fn verdicts_follow_the_step_status() {
        assert_eq!(Verdict::from_status(NOT_NEEDED), Verdict::NotNeeded);
        assert_eq!(Verdict::from_status("available"), Verdict::AsWritten);
        assert_eq!(Verdict::from_status("translated"), Verdict::Equivalent);
        assert_eq!(Verdict::from_status("blocked"), Verdict::Missing);
        assert!(Verdict::NotNeeded.runs() && !Verdict::Missing.runs());
    }
}
