//! Every agent that works on a repository reads its memories.
//!
//! The planner reads them inside a run that holds the repository's memory
//! (the lock in `common::memlock`, see [`super::AgentMemory::open`]), because
//! that run also writes what it learned. The reviewer, the feedback agent
//! (review comments and CI fixes on a pull request) and the stop judge only
//! read: they often run while a ticket's run holds the lock, and waiting for
//! it would hold a review for the length of a run, so [`read`] takes no lock.
//! It downloads the last uploaded snapshot into a directory of its own, makes
//! the same selection the planner does ([`super::select`]: pinned memories,
//! lessons on where fixes belonged, then the most relevant learnings above
//! the relevance floor), and never uploads.
//!
//! [`block`] turns that selection into one agent's prompt section, inside
//! that agent's budget and with the rules for its job (the reviewer's are
//! that a memory never blocks on its own, and a memory the code contradicts
//! is flagged, not followed). [`record`] counts what each agent was handed,
//! per memory and per agent, so the dashboard says truthfully who uses them.

use super::{select, PickKind, Picked};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

/// The agents that read a repository's memories (ids as in
/// `common::memory_meta::AGENTS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agent {
    Reviewer,
    Planner,
    Coder,
    Feedback,
    StopJudge,
}

/// Longest text of one memory in a prompt.
const MAX_MEMORY_CHARS: usize = 500;

/// Memories recalled for a read (pinned ones and lessons come on top, as
/// for the planner).
pub const READ_K: usize = 5;

/// Days the per-agent counts are kept.
const AGENT_USE_DAYS: i64 = 45;

impl Agent {
    pub fn id(self) -> &'static str {
        match self {
            Agent::Reviewer => "reviewer",
            Agent::Planner => "planner",
            Agent::Coder => "coder",
            Agent::Feedback => "feedback",
            Agent::StopJudge => "stop_judge",
        }
    }

    /// Most characters of memory text this agent's prompt takes. The
    /// planner's context is the triage summary's (see
    /// [`super::planner_context`]); these are the agents that read memories
    /// beside a prompt of their own.
    pub fn budget(self) -> usize {
        match self {
            Agent::Reviewer => 3_000,
            Agent::Coder | Agent::Feedback => 2_500,
            Agent::StopJudge => 1_500,
            Agent::Planner => 6_000,
        }
    }

    fn heading(self) -> &'static str {
        match self {
            Agent::Reviewer => {
                "\n\n## This repository's memories\n\
                 What CoderHelm learned on earlier work here and what the team wrote down: \
                 conventions, accepted patterns, known false alarms, earlier answers. Use them to \
                 choose where to look and to leave settled points alone, by these rules:\n\
                 - A memory never blocks on its own. A finding that rests on one lists its label \
                 in `\"memories\": [\"M1\"]` and still cites the code it is about: `file` and \
                 `line` in this diff, with `basis` code, tests or ci. Without that it is posted as \
                 a note.\n\
                 - A memory that says a pattern is accepted or a concern is a known false alarm: \
                 don't raise that concern again unless this code differs in a way you can name.\n\
                 - A memory the code contradicts (it names a file, function, command or rule the \
                 code here shows is gone or different) is out of date: don't follow it. List it at \
                 the top level of your JSON in `\"memory_conflicts\": [{\"memory\": \"M2\", \
                 \"code\": \"path:line\", \"why\": \"one sentence\"}]`.\n\n"
            }
            Agent::Coder => {
                "\n\n## This repository's memories\n\
                 Learned on earlier work here or written by the team: how to build and test, \
                 conventions, past fixes and what to avoid. Follow the ones that apply to this \
                 change. Where the code you read contradicts one, the code wins: follow the code \
                 and say which memory looked out of date in your summary.\n\n"
            }
            Agent::Feedback => {
                "\n\n## This repository's memories\n\
                 Learned on earlier work here or written by the team: how to build and test, \
                 past fixes, and the team's preferences. Use them for the fixes below. A comment \
                 on this pull request wins over a memory, and where the code contradicts one, the \
                 code wins.\n\n"
            }
            Agent::StopJudge | Agent::Planner => "",
        }
    }
}

/// One agent's memory section, and what it handed over.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Block {
    /// The prompt section (empty when no memory applies).
    pub text: String,
    /// The memories in it, in order, as their labels (`M1`...) and ids.
    pub labels: Vec<(String, String)>,
    /// The same memories as plain lines (for a prompt that frames them
    /// itself, like the stop judge's).
    pub lines: Vec<String>,
}

impl Block {
    pub fn ids(&self) -> Vec<String> {
        self.labels.iter().map(|(_, id)| id.clone()).collect()
    }

    /// The memory id behind a label the agent cited (`M2`, `m2`, `[M2]`).
    pub fn id_of(&self, label: &str) -> Option<&str> {
        let l = label.trim().trim_matches(|c| c == '[' || c == ']');
        self.labels
            .iter()
            .find(|(m, _)| m.eq_ignore_ascii_case(l))
            .map(|(_, id)| id.as_str())
    }
}

fn one_line(s: &str) -> String {
    let flat = s.split_whitespace().collect::<Vec<_>>().join(" ");
    common::truncate_str(&flat, MAX_MEMORY_CHARS).to_string()
}

/// The agent's memory section from the chosen memories: pinned ones first,
/// then lessons, then the learnings by relevance, each labelled, until the
/// agent's budget is spent. A memory that doesn't fit is left out whole.
pub fn block(agent: Agent, picked: &[Picked]) -> Block {
    let mut out = Block::default();
    let heading = agent.heading();
    let mut body = String::new();
    let order = [PickKind::Pinned, PickKind::Lesson, PickKind::Learned];
    for kind in order {
        for p in picked.iter().filter(|p| p.kind == kind) {
            let text = one_line(&p.content);
            if text.is_empty() {
                continue;
            }
            let label = format!("M{}", out.labels.len() + 1);
            let tag = match kind {
                PickKind::Pinned => " (pinned by the team: always applies)",
                PickKind::Lesson => " (the team's lesson on where a fix belonged)",
                PickKind::Learned => "",
            };
            let line = format!("- [{label}]{tag} {text}\n");
            if body.len() + line.len() > agent.budget() {
                continue;
            }
            body.push_str(&line);
            out.lines.push(text);
            out.labels.push((label, p.id.clone()));
        }
    }
    if !out.labels.is_empty() && !heading.is_empty() {
        out.text = format!("{heading}{body}");
    }
    out
}

/// The repository's memories for a task, read without the lock (see the
/// module doc). Empty when memory is off, the repository has none, or
/// anything fails: a read never fails the agent's work.
pub async fn read(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
    query: &str,
) -> Vec<Picked> {
    if std::env::var("MEMORY_ENABLED").unwrap_or_default() != "true" || query.trim().is_empty() {
        return Vec::new();
    }
    let dir = std::path::PathBuf::from(format!("/tmp/mentedb-read/{}", uuid::Uuid::new_v4()));
    let dir = match super::s3_sync::download_memory_to(
        &state.s3,
        &state.config.bucket_name,
        team_id,
        owner,
        name,
        dir.clone(),
    )
    .await
    {
        Ok(d) => d,
        Err(e) => {
            warn!(error = %e, "Could not read the repository's memories");
            super::s3_sync::cleanup_local(&dir);
            return Vec::new();
        }
    };
    let empty = std::fs::read_dir(&dir)
        .map(|mut d| d.next().is_none())
        .unwrap_or(true);
    if empty {
        super::s3_sync::cleanup_local(&dir);
        return Vec::new();
    }
    let meter = common::usage::UsageMeter::dynamo(
        state.dynamo.clone(),
        &state.config.analytics_table_name,
        team_id,
    );
    let picked = match super::embedder::embed_async(&state.bedrock, &meter, query).await {
        Err(e) => {
            warn!(error = %e, "Failed to embed a memory read");
            Vec::new()
        }
        Ok(embedding) => {
            let embedder = super::embedder::BedrockEmbedder::new(state.bedrock.clone(), meter);
            match super::open_db(&dir, Box::new(embedder)) {
                Ok(db) => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as u64;
                    let picked = select(&db, &embedding, query, READ_K, now);
                    let _ = db.close();
                    picked
                }
                Err(e) => {
                    warn!(error = %e, "Could not open the repository's memories");
                    Vec::new()
                }
            }
        }
    };
    super::s3_sync::cleanup_local(&dir);
    picked
}

/// Reads the memories for `agent`, makes its block and counts the use.
#[allow(clippy::too_many_arguments)]
pub async fn for_agent(
    state: &WorkerState,
    agent: Agent,
    team_id: &str,
    owner: &str,
    name: &str,
    query: &str,
    run_ref: &str,
    title: &str,
) -> Block {
    let picked = read(state, team_id, owner, name, query).await;
    let b = block(agent, &picked);
    if !b.labels.is_empty() {
        info!(
            agent = agent.id(),
            memories = b.labels.len(),
            "Repository memories given to the agent"
        );
        record(state, agent, team_id, owner, name, run_ref, title, &b.ids()).await;
    }
    b
}

/// Counts that `agent` was handed the memories `ids`: each memory's use
/// (`common::memory_meta::usage_sk`, what the memory list shows) and the
/// agent's reads for the day (`agent_use_sk`, what "Who uses memory"
/// shows). Best effort: a failed count never fails the work.
#[allow(clippy::too_many_arguments)]
pub async fn record(
    state: &WorkerState,
    agent: Agent,
    team_id: &str,
    owner: &str,
    name: &str,
    run_ref: &str,
    title: &str,
    ids: &[String],
) {
    if ids.is_empty() {
        return;
    }
    let now = chrono::Utc::now();
    let at = now.to_rfc3339();
    let table = &state.config.settings_table_name;
    for id in ids {
        let res = state
            .dynamo
            .update_item()
            .table_name(table)
            .key("pk", AttributeValue::S(team_id.to_string()))
            .key(
                "sk",
                AttributeValue::S(common::memory_meta::usage_sk(owner, name, id)),
            )
            .update_expression(
                "ADD uses :one SET last_used_at = :now, last_run_id = :run, last_title = :title, last_agent = :agent",
            )
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":now", AttributeValue::S(at.clone()))
            .expression_attribute_values(":run", AttributeValue::S(run_ref.to_string()))
            .expression_attribute_values(
                ":title",
                AttributeValue::S(common::truncate_str(title, 120).to_string()),
            )
            .expression_attribute_values(":agent", AttributeValue::S(agent.id().to_string()))
            .send()
            .await;
        if let Err(e) = res {
            warn!(run_ref, error = %e, "Could not count a memory's use");
        }
    }
    let day = now.format("%Y-%m-%d").to_string();
    let ttl = (now + chrono::Duration::days(AGENT_USE_DAYS)).timestamp();
    let res = state
        .dynamo
        .update_item()
        .table_name(table)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key(
            "sk",
            AttributeValue::S(common::memory_meta::agent_use_sk(
                owner,
                name,
                agent.id(),
                &day,
            )),
        )
        .update_expression("ADD reads :one, memories :n SET last_at = :now, #ttl = :ttl")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .expression_attribute_values(":n", AttributeValue::N(ids.len().to_string()))
        .expression_attribute_values(":now", AttributeValue::S(at))
        .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
        .send()
        .await;
    if let Err(e) = res {
        warn!(run_ref, error = %e, "Could not count the agent's memory reads");
    }
}

/// A memory the reviewer found the code contradicts: kept on the memory's
/// usage item so the memory page shows it as possibly out of date, with the
/// pull request that showed it. The memory itself is left as it is: a
/// person decides whether to edit or forget it.
pub async fn flag_out_of_date(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
    memory_id: &str,
    note: &str,
    link: &str,
) {
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key(
            "sk",
            AttributeValue::S(common::memory_meta::usage_sk(owner, name, memory_id)),
        )
        .update_expression("SET conflict_at = :at, conflict_note = :note, conflict_link = :link")
        .expression_attribute_values(":at", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(
            ":note",
            AttributeValue::S(common::truncate_str(note.trim(), 400).to_string()),
        )
        .expression_attribute_values(":link", AttributeValue::S(link.to_string()))
        .send()
        .await;
    if let Err(e) = res {
        warn!(memory_id, error = %e, "Could not flag an out-of-date memory");
    }
}

/// What a feedback round teaches: the change requests people made on the
/// pull request and what the agent did, for the extraction (see
/// [`learn_from_feedback`]). Empty when no person asked for a change.
pub fn feedback_summary(requests: &[String], replies: &str) -> String {
    let asks: Vec<String> = requests
        .iter()
        .map(|r| one_line(r))
        .filter(|r| !r.is_empty())
        .collect();
    if asks.is_empty() {
        return String::new();
    }
    let mut s = String::new();
    for a in &asks {
        s.push_str(&format!("[Reviewer asked]: {a}\n"));
    }
    let replies = common::truncate_str(replies.trim(), 4_000);
    if !replies.is_empty() {
        s.push_str(&format!("[Agent did]: {replies}\n"));
    }
    s
}

/// The extraction task for a feedback round: only what the team's requests
/// say about how this repository wants things done, never the one-off fix.
pub const FEEDBACK_TASK: &str = "These are change requests a person on the team made on an AI coding \
     agent's pull request, and what the agent changed. Extract ONLY the team's general preferences or \
     procedures they reveal for this repository (a convention, a pattern to use or avoid, how to build \
     or test), written as a rule that applies to future changes. A request that only fixes this one \
     change teaches nothing: return [] for it.";

/// Stores what a feedback round's accepted requests teach, through the
/// repository's memory (and its lock: when a run holds it, nothing is
/// stored this time and the round goes on).
pub async fn learn_from_feedback(
    state: &WorkerState,
    provider: &crate::agent::provider::ModelProvider,
    team_id: &str,
    owner: &str,
    name: &str,
    origin: &super::LearningOrigin,
    summary: &str,
) {
    if summary.trim().is_empty() {
        return;
    }
    let Some(mut mem) = super::AgentMemory::open(state, team_id, owner, name).await else {
        info!(
            run_id = %origin.run_id,
            "Repository memory busy or off; this round's preferences not stored"
        );
        return;
    };
    mem.extract_and_store(
        summary,
        provider,
        origin,
        FEEDBACK_TASK,
        &[common::memory_meta::learned_by_tag("feedback")],
    )
    .await;
    if let Err(e) = mem.close_and_upload(state).await {
        warn!(error = %e, "Failed to persist what the feedback round taught");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mentedb_core::memory::MemoryType;

    fn p(id: &str, kind: PickKind, content: &str) -> Picked {
        Picked {
            id: id.into(),
            content: content.into(),
            kind,
            memory_type: MemoryType::Semantic,
            score: 0.05,
        }
    }

    fn sample() -> Vec<Picked> {
        vec![
            p(
                "a",
                PickKind::Learned,
                "Run the service tests from services/ with cargo test.",
            ),
            p(
                "b",
                PickKind::Pinned,
                "Every handler returns a typed error, never a bare string.",
            ),
            p(
                "c",
                PickKind::Lesson,
                "Schedule bugs belong in the sync job, not the site.",
            ),
        ]
    }

    #[test]
    fn every_agent_gets_the_same_selection_pinned_first_and_labelled() {
        for agent in [Agent::Reviewer, Agent::Coder, Agent::Feedback] {
            let b = block(agent, &sample());
            assert_eq!(
                b.labels,
                vec![
                    ("M1".to_string(), "b".to_string()),
                    ("M2".to_string(), "c".to_string()),
                    ("M3".to_string(), "a".to_string())
                ],
                "{agent:?}"
            );
            assert!(b.text.contains("## This repository's memories"));
            assert!(b
                .text
                .contains("- [M1] (pinned by the team: always applies) Every handler"));
            assert!(b.text.find("[M1]") < b.text.find("[M3]"));
        }
        // The stop judge frames them itself: lines, no heading.
        let j = block(Agent::StopJudge, &sample());
        assert!(j.text.is_empty());
        assert_eq!(j.lines.len(), 3);
        assert_eq!(j.ids(), vec!["b", "c", "a"]);
        // Nothing chosen, nothing added.
        assert_eq!(block(Agent::Reviewer, &[]), Block::default());
    }

    #[test]
    fn a_block_stays_inside_the_agents_budget_and_never_cuts_a_memory() {
        let long = "word ".repeat(400);
        let many: Vec<Picked> = (0..40)
            .map(|i| p(&format!("m{i}"), PickKind::Learned, &format!("{i} {long}")))
            .collect();
        for agent in [
            Agent::Reviewer,
            Agent::Coder,
            Agent::Feedback,
            Agent::StopJudge,
        ] {
            let b = block(agent, &many);
            let body: usize = b
                .text
                .lines()
                .filter(|l| l.starts_with("- [M"))
                .map(|l| l.len() + 1)
                .sum();
            assert!(body <= agent.budget(), "{agent:?}: {body}");
            assert!(!b.labels.is_empty(), "{agent:?} got none");
            assert!(b.labels.len() < 40, "{agent:?} took all");
            // Each memory is clipped to one line of at most MAX_MEMORY_CHARS.
            assert!(b
                .lines
                .iter()
                .all(|l| l.len() <= MAX_MEMORY_CHARS && !l.contains('\n')));
        }
    }

    #[test]
    fn the_reviewer_is_told_a_memory_never_blocks_alone_and_conflicts_are_flagged() {
        let b = block(Agent::Reviewer, &sample());
        assert!(b.text.contains("A memory never blocks on its own"));
        assert!(b.text.contains("\"memories\": [\"M1\"]"));
        assert!(b.text.contains("\"memory_conflicts\""));
        assert!(b.text.contains("known false alarm"));
        assert_eq!(b.id_of("[m2]"), Some("c"));
        assert_eq!(b.id_of("M9"), None);
    }

    /// Every agent that reads memories counts what it read: each reads
    /// through `for_agent` (which records) or records itself, under its own
    /// id, in the pass that builds its prompt.
    #[test]
    fn every_agent_records_what_it_reads() {
        let sources = [
            (Agent::Reviewer, include_str!("../passes/review_pr.rs")),
            (Agent::Feedback, include_str!("../passes/feedback.rs")),
            (Agent::StopJudge, include_str!("../passes/stop_judge.rs")),
            (Agent::Planner, include_str!("../passes/mod.rs")),
            (Agent::Coder, include_str!("../passes/mod.rs")),
        ];
        for (agent, src) in sources {
            let src: String = src.split_whitespace().collect();
            let variant = format!("{agent:?}");
            let reads = src.contains(&format!(
                "recall::for_agent(state,crate::memory::recall::Agent::{variant},"
            ));
            let records = src.contains(&format!("recall::record(state,Agent::{variant},"));
            assert!(
                reads || records,
                "{variant} reads memories without counting them"
            );
        }
        // Each agent's id is one the dashboard lists.
        for agent in [
            Agent::Reviewer,
            Agent::Planner,
            Agent::Coder,
            Agent::Feedback,
            Agent::StopJudge,
        ] {
            assert!(
                common::memory_meta::AGENTS
                    .iter()
                    .any(|(id, _)| *id == agent.id()),
                "{agent:?}"
            );
        }
    }

    #[test]
    fn a_feedback_round_teaches_only_from_what_people_asked() {
        assert!(feedback_summary(&[], "Fixed it").is_empty());
        assert!(feedback_summary(&["  ".into()], "Fixed it").is_empty());
        let s = feedback_summary(
            &["Use the shared retry helper\nhere, not a loop".into()],
            "Switched to `retry::with_backoff`.",
        );
        assert!(s.contains("[Reviewer asked]: Use the shared retry helper here, not a loop"));
        assert!(s.contains("[Agent did]: Switched"));
        assert!(FEEDBACK_TASK.contains("return []"));
    }
}
