//! Before a pull request opens, the change must rest on what the run proved.
//! A change that relied on a vendor field nobody looked at, said "not
//! measured" while a connected source could count it, or logged every record
//! at INFO on every scheduled sync, shipped as a PR because nothing checked
//! these before it opened. This gate does: a judge reads the change, the plan,
//! the sources the team connected and every lookup the run made, and names
//! each gap. A gap sends the work back for one fix round with the finding;
//! what is still unproven after it is put at the top of the PR description.

use serde::Deserialize;
use tracing::{info, warn};

use crate::agent::provider;
use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::passes::plan::PlanResult;
use crate::passes::FileCache;
use crate::WorkerState;

/// How many fix rounds a gap gets before the PR opens with it named.
pub const MAX_FIX_ROUNDS: u32 = 1;

/// What every change must do, whatever it touches. Each rule is a way a
/// change shipped resting on a guess.
pub const CODER_RULES: &str = "
## Prove what the change rests on (every change)
- Prove an assumption before you write code on it. When the fix relies on a field, value or behaviour of a system this code doesn't own (a vendor API, another service, a table another system writes), confirm it from the real thing first: one read through a connected data source (a live lookup, `rest_get`, a database query, a search) or that system's own data in the warehouse. Read an actual payload or row; a type, a schema or a name in the code is not proof of what the field holds.
- Measure the impact when the data is in a connected source: count the records the change affects, with a query, and note a few concrete examples (ids or names).
- Logging: INFO only for actual changes and decisions; diagnostics go to DEBUG. Never add an INFO log that fires for every record or on every scheduled run.
- Never leave \"unconfirmed\", \"not verified\" or \"not measured\" for something a connected source could answer: look it up. When no connected source can reach it, say exactly which source is missing.
";

/// How the coder ends: its evidence becomes the PR description's.
pub const EVIDENCE_OUTPUT: &str = "When you finish, output a one-line summary, then a section that starts with `Evidence:` listing: each fact about a system this code doesn't own that the change relies on, the lookup that confirmed it and what came back (quote the field and its value); how many records the change affects, where you counted them, and a few examples (ids or names); and anything no connected source could answer, naming the source that is missing. Write `Evidence: none needed` when the change relies on nothing outside the code and changes no stored records. Do NOT describe each code change in detail.";

/// What the run gathered that the change rests on.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunEvidence {
    /// The data sources this work could read, one line each.
    pub sources: Vec<String>,
    /// Each lookup made and what came back.
    pub lookups: Vec<String>,
    /// The coder's own account: its closing summary and `Evidence:` section.
    pub notes: String,
}

impl RunEvidence {
    /// Adds a later pass's evidence (a fix round) to this run's.
    pub fn merge(&mut self, later: RunEvidence) {
        for s in later.sources {
            if !self.sources.contains(&s) {
                self.sources.push(s);
            }
        }
        self.lookups.extend(later.lookups);
        let notes = later.notes.trim();
        if !notes.is_empty() {
            if self.notes.trim().is_empty() {
                self.notes = notes.to_string();
            } else {
                self.notes = format!("{}\n\nAfter a fix round:\n{notes}", self.notes.trim());
            }
        }
    }

    /// What the judge reads about the run's evidence.
    pub fn for_judge(&self) -> String {
        let list = |items: &[String], none: &str| {
            if items.is_empty() {
                format!("{none}\n")
            } else {
                items
                    .iter()
                    .map(|i| format!("{}\n", i.trim_end()))
                    .collect()
            }
        };
        format!(
            "## Data sources this team connected for this repository\n{}\n\
             ## Lookups the run made, and what came back\n{}\n\
             ## The coder's evidence notes (they become the PR description's evidence)\n{}\n",
            list(&self.sources, "(none connected)"),
            list(&self.lookups, "(no lookups were made)"),
            match self.notes.trim() {
                "" => "(the coder wrote none)",
                n => n,
            },
        )
    }

    /// What the PR description writer is given to report.
    pub fn for_pr(&self) -> String {
        if self.lookups.is_empty() && self.notes.trim().is_empty() {
            return String::new();
        }
        let mut out =
            String::from("\n## What the run looked up (the source for the Evidence section)\n");
        for l in &self.lookups {
            out.push_str(&format!("{}\n", l.trim_end()));
        }
        if !self.notes.trim().is_empty() {
            out.push_str(&format!("\nThe coder's notes:\n{}\n", self.notes.trim()));
        }
        out
    }
}

/// One of the four things the gate judges.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Finding {
    /// `external_fact`, `impact`, `logging` or `assumption`.
    pub point: String,
    /// What is missing and what would prove or fix it.
    pub detail: String,
}

impl Finding {
    fn label(&self) -> &str {
        match self.point.trim() {
            "external_fact" => "Unconfirmed external field or behaviour",
            "impact" => "Impact not measured",
            "logging" => "Noisy logging",
            "assumption" => "Unproven assumption",
            other => other,
        }
    }

    fn line(&self) -> String {
        format!(
            "- **{}:** {}",
            self.label(),
            self.detail.split_whitespace().collect::<Vec<_>>().join(" ")
        )
    }
}

#[derive(Deserialize)]
struct Judged {
    #[serde(default)]
    findings: Vec<Finding>,
}

/// The judge's system prompt.
pub const JUDGE_SYSTEM: &str = "You check a code change before its pull request opens. You judge \
     four points from the evidence you are given: the diff, the plan, the data sources the team \
     connected, every lookup the run made with what came back, and the coder's evidence notes \
     (they become the PR description's evidence).\n\
     1. external_fact: the change relies on a field, value or behaviour of a system it does not \
     own (a vendor API, another service, a table another system writes). A miss when no lookup \
     listed confirms it from the real thing (a payload, a row) and the notes don't quote what \
     was seen. Not a miss when no connected source can reach that system and the notes name \
     exactly which source is missing.\n\
     2. impact: the change alters behaviour for stored records whose data sits in a connected \
     source. A miss when the notes give no count of affected records from a lookup with a few \
     concrete examples (ids or names), or when they say impact was not measured, not verified or \
     unconfirmed while a connected source could have answered.\n\
     3. logging: a miss when the diff adds an INFO (or louder) log that fires for every record or \
     on every scheduled run without reporting an actual change or decision. Diagnostics belong \
     at DEBUG.\n\
     4. assumption: a miss when the change rests on an assumption (what another system sends, \
     what values a field takes, uniqueness, ordering) that nothing in the lookups or the code \
     proves.\n\
     Many changes touch none of these: a change that relies on nothing outside its own code, \
     changes no stored records and adds no logs passes all four. Judge only what the evidence \
     shows and do not invent concerns. A fact the lookups confirm is proven even when the notes \
     are short. Each finding names the exact field, records, log line or assumption, and what \
     would settle it (which connected source to read, what to count, which log to lower).\n\
     Output ONLY a fenced ```json block: {\"findings\":[{\"point\":\"external_fact|impact|logging|assumption\",\"detail\":\"...\"}]}. \
     An empty list when all four pass.";

/// The judge's prompt: the change and everything the run gathered.
pub fn judge_prompt(title: &str, plan_context: &str, diff: &str, evidence: &RunEvidence) -> String {
    format!(
        "# Change for: {title}\n\n## Plan\n{plan}\n\n{evidence}\n## The diff\n{diff}\n",
        plan = match plan_context.trim() {
            "" => "(no plan carried into this check)",
            p => p,
        },
        evidence = evidence.for_judge(),
    )
}

/// The judge's findings, or `None` when its reply can't be read.
pub fn parse_findings(reply: &str) -> Option<Vec<Finding>> {
    let block = match reply.rfind("```json") {
        Some(s) => {
            let after = &reply[s + 7..];
            after.find("```").map(|e| &after[..e])?
        }
        None => reply,
    };
    let judged: Judged = serde_json::from_str(block.trim()).ok()?;
    Some(
        judged
            .findings
            .into_iter()
            .filter(|f| !f.detail.trim().is_empty())
            .collect(),
    )
}

/// What happens after a judgment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    /// Nothing to prove (or the judge couldn't answer): open the PR.
    Open,
    /// Send the findings back to the coder for a fix round.
    FixRound,
    /// Gaps remain and no round is left: open the PR with them at the top.
    OpenWithGaps,
}

/// Decides what follows a judgment. `None` is a judge that couldn't answer:
/// it never holds a PR.
pub fn next(findings: Option<&[Finding]>, rounds_done: u32, has_time: bool) -> Next {
    match findings {
        None | Some([]) => Next::Open,
        Some(_) if rounds_done < MAX_FIX_ROUNDS && has_time => Next::FixRound,
        Some(_) => Next::OpenWithGaps,
    }
}

/// What the coder is told in a fix round.
pub fn fix_feedback(findings: &[Finding]) -> String {
    let lines: Vec<String> = findings.iter().map(Finding::line).collect();
    format!(
        "Before the pull request opens, the check of what this change rests on found these gaps. \
         Settle each one: look the fact up through a connected data source and quote what came \
         back, count the affected records and name a few, lower per-record or per-run INFO logs \
         to DEBUG, or change the code so it no longer depends on what you can't prove. When no \
         connected source can answer, say which source is missing. Then write your `Evidence:` \
         section again in full.\n\n{}",
        lines.join("\n")
    )
}

/// The section put at the top of the PR description when gaps remain.
pub fn gaps_section(findings: &[Finding]) -> String {
    if findings.is_empty() {
        return String::new();
    }
    let lines: Vec<String> = findings.iter().map(Finding::line).collect();
    format!(
        "> [!WARNING]\n> **Not proven before this PR opened.** CoderHelm's check of what this change rests on still finds:\n{}\n\n",
        lines
            .iter()
            .map(|l| format!("> {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// What the plan says the change is for, for the judge.
fn plan_context(plan: &PlanResult) -> String {
    let mut out = plan.proposal.trim().to_string();
    if let Some(d) = &plan.fix {
        out.push_str(&super::fix_location::reviewer_context(d));
    }
    out
}

/// The judge's reading of the change on `branch`. `None` when it couldn't
/// judge (an API or model failure, an unreadable reply).
#[allow(clippy::too_many_arguments)]
pub async fn judge(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    plan: &PlanResult,
    evidence: &RunEvidence,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Option<Vec<Finding>> {
    let diff = match github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await
    {
        Ok(d) => super::pr::format_diff_patches(&d),
        Err(e) => {
            warn!(error = %e, "Proof check could not read the diff");
            return None;
        }
    };
    let prompt = judge_prompt(&msg.title, &plan_context(plan), &diff, evidence);
    match provider::converse_simple(
        state,
        provider,
        provider.heavy_model_id(),
        JUDGE_SYSTEM,
        &prompt,
        usage,
    )
    .await
    {
        Ok(reply) => {
            let parsed = parse_findings(&reply);
            if parsed.is_none() {
                warn!(
                    reply = common::truncate_str(&reply, 300),
                    "Proof check reply was unreadable"
                );
            }
            parsed
        }
        Err(e) => {
            warn!(error = %e, "Proof check errored");
            None
        }
    }
}

/// Everything a fix round needs, as the pipeline has it.
pub struct Work<'a> {
    pub state: &'a WorkerState,
    pub msg: &'a TicketMessage,
    pub github: &'a GitHubClient,
    pub plan: &'a PlanResult,
    pub branch: &'a str,
    pub rules: &'a [String],
    pub repo_instructions: &'a str,
    pub provider: &'a ModelProvider,
    pub file_cache: &'a FileCache,
    pub run_id: &'a str,
}

/// Judges the change, runs the fix round a gap earns, and judges again.
/// Returns the gaps still open when the PR is about to open (empty when
/// everything is proven or the judge couldn't answer). `evidence` gains what
/// the fix round looked up.
pub async fn prove(
    w: &Work<'_>,
    evidence: &mut RunEvidence,
    usage: &mut TokenUsage,
    has_time: impl Fn() -> bool,
) -> Vec<Finding> {
    let mut rounds = 0;
    loop {
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let findings = judge(
            w.state, w.msg, w.github, w.branch, w.plan, evidence, w.provider, usage,
        )
        .await;
        super::write_pass_trace(
            w.state,
            &w.msg.team_id,
            w.run_id,
            "proof",
            pass_start,
            &usage_before,
            usage,
            None,
        )
        .await;
        info!(
            run_id = w.run_id,
            judged = findings.is_some(),
            gaps = findings.as_ref().map_or(0, Vec::len),
            rounds,
            "Proof check complete"
        );
        match next(findings.as_deref(), rounds, has_time()) {
            Next::Open => {
                if findings.is_some() {
                    super::add_progress_note(
                        w.state,
                        &w.msg.team_id,
                        w.run_id,
                        "Proof check passed: what the change rests on is confirmed",
                    )
                    .await;
                }
                return Vec::new();
            }
            Next::OpenWithGaps => {
                let gaps = findings.unwrap_or_default();
                super::add_progress_note(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    &format!(
                        "Proof check: {} gap(s) remain, named at the top of the PR",
                        gaps.len()
                    ),
                )
                .await;
                return gaps;
            }
            Next::FixRound => {
                let gaps = findings.unwrap_or_default();
                rounds += 1;
                super::add_progress_note(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    &format!("Proof check found {} gap(s), fixing", gaps.len()),
                )
                .await;
                if super::check_cancelled(w.state, &w.msg.team_id, w.run_id)
                    .await
                    .is_err()
                {
                    return gaps;
                }
                let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "implement").await;
                let pass_start = std::time::Instant::now();
                let usage_before = usage.clone();
                let fixed = super::implement::run(
                    w.state,
                    w.msg,
                    w.github,
                    w.plan,
                    w.branch,
                    w.rules,
                    w.repo_instructions,
                    Some(&fix_feedback(&gaps)),
                    "medium",
                    w.provider,
                    usage,
                    w.file_cache,
                    Some(w.run_id),
                    None,
                    true, // bot-driven proof fix — scoped to the PR
                )
                .await;
                super::write_pass_trace(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    "implement:proof_fix",
                    pass_start,
                    &usage_before,
                    usage,
                    None,
                )
                .await;
                match fixed {
                    Ok(r) => evidence.merge(r.evidence),
                    Err(e) => {
                        warn!(run_id = w.run_id, error = %e, "Proof fix round failed");
                        return gaps;
                    }
                }
                let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "review").await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(point: &str, detail: &str) -> Finding {
        Finding {
            point: point.into(),
            detail: detail.into(),
        }
    }

    #[test]
    fn reads_the_judges_findings() {
        let reply = "Looked at it.\n```json\n{\"findings\":[{\"point\":\"external_fact\",\"detail\":\"`name` on products is never read from a real payload\"},{\"point\":\"logging\",\"detail\":\"\"}]}\n```";
        let f = parse_findings(reply).unwrap();
        assert_eq!(
            f,
            vec![finding(
                "external_fact",
                "`name` on products is never read from a real payload"
            )]
        );
        assert_eq!(parse_findings("{\"findings\":[]}"), Some(vec![]));
        assert_eq!(parse_findings("LGTM"), None);
        assert_eq!(parse_findings("```json\n{\"findings\": [\n"), None);
    }

    #[test]
    fn a_gap_earns_one_fix_round_then_opens_with_it_named() {
        let gaps = [finding("impact", "affected records not counted")];
        assert_eq!(next(Some(&gaps), 0, true), Next::FixRound);
        assert_eq!(next(Some(&gaps), 1, true), Next::OpenWithGaps);
        assert_eq!(next(Some(&gaps), 0, false), Next::OpenWithGaps);
        assert_eq!(next(Some(&[]), 0, true), Next::Open);
        // A judge that couldn't answer never holds the PR.
        assert_eq!(next(None, 0, true), Next::Open);
    }

    #[test]
    fn gaps_lead_the_pr_description_and_the_fix_round_gets_them() {
        let gaps = [
            finding(
                "external_fact",
                "confirm `name` on products from the vendor API",
            ),
            finding("logging", "INFO log per record in the sync loop"),
        ];
        let top = gaps_section(&gaps);
        assert!(top.starts_with("> [!WARNING]"));
        assert!(top.contains("> - **Unconfirmed external field or behaviour:** confirm `name`"));
        assert!(top.contains("> - **Noisy logging:** INFO log per record"));
        assert_eq!(gaps_section(&[]), "");
        let fb = fix_feedback(&gaps);
        assert!(fb.contains("- **Noisy logging:** INFO log per record in the sync loop"));
        assert!(fb.contains("write your `Evidence:` section again"));
    }

    #[test]
    fn the_judge_sees_sources_lookups_and_notes() {
        let ev = RunEvidence {
            sources: vec!["warehouse (database)".into()],
            lookups: vec!["- Asked warehouse: select count(*) ...\n  Came back: 42".into()],
            notes: "Evidence: 42 records affected".into(),
        };
        let p = judge_prompt("Fix sync", "Use the vendor field", "+ x", &ev);
        assert!(p.contains("warehouse (database)"));
        assert!(p.contains("Came back: 42"));
        assert!(p.contains("Evidence: 42 records affected"));
        assert!(p.contains("## The diff\n+ x"));
        let empty = judge_prompt("t", "", "", &RunEvidence::default());
        assert!(empty.contains("(none connected)"));
        assert!(empty.contains("(no lookups were made)"));
        assert!(empty.contains("(the coder wrote none)"));
        for point in ["external_fact", "impact", "logging", "assumption"] {
            assert!(JUDGE_SYSTEM.contains(point), "{point}");
        }
    }

    #[test]
    fn a_fix_round_adds_to_the_runs_evidence() {
        let mut ev = RunEvidence {
            sources: vec!["vendor (api)".into()],
            lookups: vec!["- one".into()],
            notes: "Evidence: none needed".into(),
        };
        ev.merge(RunEvidence {
            sources: vec!["vendor (api)".into()],
            lookups: vec!["- two".into()],
            notes: "Evidence: field confirmed".into(),
        });
        assert_eq!(ev.sources, vec!["vendor (api)".to_string()]);
        assert_eq!(ev.lookups, vec!["- one".to_string(), "- two".to_string()]);
        assert!(ev
            .notes
            .ends_with("After a fix round:\nEvidence: field confirmed"));
        assert!(ev.for_pr().contains("- two"));
        assert_eq!(RunEvidence::default().for_pr(), "");
    }

    #[test]
    fn the_rules_cover_all_four_points() {
        assert!(CODER_RULES.contains("Prove an assumption before you write code on it"));
        assert!(CODER_RULES.contains("count the records the change affects"));
        assert!(CODER_RULES.contains("diagnostics go to DEBUG"));
        assert!(CODER_RULES.contains("say exactly which source is missing"));
        assert!(EVIDENCE_OUTPUT.contains("`Evidence:`"));
    }
}
