//! The plan's own check, before it is kept. A plan once framed new work as a
//! fault to trace, said it "could not find out why" earlier pull requests
//! for the ticket were closed while their close events were one call away,
//! hedged about which environment it had queried while a lookup could have
//! told, and found an existing object close to what the ticket named without
//! following it up. Each is a point here:
//!
//! - `history_used`: the ticket has earlier pull requests and the plan
//!   doesn't build on them, or leaves a closed one's who and when unsaid;
//! - `env_unproven`: the evidence hedges about which environment a lookup
//!   hit while a connected source could have shown it;
//! - `near_match_unresolved`: the evidence names an existing object close to
//!   what the ticket asks for without its owner, contents and origin, or
//!   without one decisive question.
//!
//! The structured part is checked in code ([`structural`]); a model reads
//! the plan's words for the rest ([`judge`]). A gap sends the planner back
//! once; what is still open is kept on the decision and shown wherever the
//! plan is.

use serde::Deserialize;
use tracing::warn;

use crate::agent::provider::{self, ModelProvider};
use crate::models::TokenUsage;
use crate::passes::fix_location::FixDecision;
use crate::passes::plan::PlanResult;
use crate::passes::ticket_history::TicketHistory;
use crate::WorkerState;

/// One gap in a plan.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct PlanFinding {
    /// `history_used`, `env_unproven` or `near_match_unresolved`.
    pub point: String,
    /// What is missing, in one sentence.
    pub detail: String,
    /// How to settle it, in one sentence.
    #[serde(default)]
    pub settle: String,
}

impl PlanFinding {
    fn new(point: &str, detail: String, settle: &str) -> Self {
        Self {
            point: point.into(),
            detail,
            settle: settle.into(),
        }
    }

    /// One line, as the planner is sent back with it and a report shows it.
    pub fn line(&self) -> String {
        let flat = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
        match flat(&self.settle) {
            s if s.is_empty() => format!("[{}] {}", self.point, flat(&self.detail)),
            s => format!("[{}] {} {s}", self.point, flat(&self.detail)),
        }
    }
}

/// Everything the plan says in words: the openspec files and the decision's
/// own text, where its history check, questions and evidence live.
pub fn plan_text(files: &PlanResult) -> String {
    let mut out = format!(
        "{}\n{}\n{}\n{}\n",
        files.proposal, files.design, files.tasks, files.spec
    );
    if let Some(d) = &files.fix {
        out.push_str(&decision_text(d));
    }
    out
}

fn decision_text(d: &FixDecision) -> String {
    let mut out = format!("{}\n{}\n", d.cause, d.history_check);
    for q in &d.questions {
        out.push_str(q);
        out.push('\n');
    }
    for l in &d.evidence.looked_up {
        out.push_str(l);
        out.push('\n');
    }
    out.push_str(&d.evidence.data_shows);
    out
}

/// The gaps the plan's structure shows, whatever a model says:
/// - the ticket's history has earlier pull requests the plan doesn't name,
///   or a closed one whose close event was read and the plan doesn't say
///   who closed it and when (`history_used`);
/// - an environment the plan lists as unproven while a lookup could reach
///   it, or listed as proven with no proof (`env_unproven`);
/// - an existing object close to what the ticket names, listed without its
///   owner, contents, origin or the question about it (`near_match_unresolved`).
pub fn structural(
    files: &PlanResult,
    history: &TicketHistory,
    can_look_up: bool,
) -> Vec<PlanFinding> {
    let text = plan_text(files);
    let mut out: Vec<PlanFinding> = history
        .plan_gaps(&text)
        .into_iter()
        .map(|g| PlanFinding::new("history_used", g, ""))
        .collect();
    let Some(d) = files.fix.as_ref() else {
        return out;
    };
    for e in &d.evidence.environments {
        let env = e.env.trim();
        // Proof is asked for only where the plan's claims rest on that
        // environment's data: a plan whose cause and data say nothing about
        // `production` owes no production-only proof just because a source
        // is connected (a schedule data fix was once sent back for proof
        // in production and staging it never made a claim about).
        if !rests_on_env(d, env) {
            continue;
        }
        if !e.proven && can_look_up {
            out.push(PlanFinding::new(
                "env_unproven",
                format!("The plan leaves `{env}` unproven while a connected source can be looked up."),
                "Look it up and quote data only that environment has in `proof`, or name the access \
                 that is missing when no connected source reaches it.",
            ));
        } else if e.proven && e.proof.trim().is_empty() {
            out.push(PlanFinding::new(
                "env_unproven",
                format!("The plan says `{env}` is confirmed but quotes nothing that proves it."),
                "Quote the data the lookup returned that only exists in that environment.",
            ));
        }
    }
    for m in &d.evidence.near_matches {
        let missing = m.missing();
        if !missing.is_empty() {
            out.push(PlanFinding::new(
                "near_match_unresolved",
                format!(
                    "The plan found `{}`, close to what the ticket names, without its {}.",
                    m.name.trim(),
                    missing.join(", ")
                ),
                "Look up who owns it, its grants and what is in it, search the team's repositories \
                 for what created it, and ask one question: use it instead of a new one?",
            ));
        }
    }
    out
}

/// Whether the decision's claims rest on data from `env`: its cause, what
/// the data shows, the scope it counted or what it looked up name that
/// environment (`prod`, `production` and `live` are one; so are `staging`
/// and `stage`; any other name, an account or a cluster, by its own words).
pub fn rests_on_env(d: &FixDecision, env: &str) -> bool {
    let claims = format!(
        "{}\n{}\n{}\n{}",
        d.cause,
        d.evidence.data_shows,
        d.evidence.scope,
        d.evidence.looked_up.join("\n")
    )
    .to_ascii_lowercase();
    let said: Vec<&str> = claims
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
        .filter(|w| !w.is_empty())
        .collect();
    let env = env.trim().to_ascii_lowercase();
    if env.is_empty() {
        return false;
    }
    let aliases: &[&str] = match env.as_str() {
        "prod" | "production" | "prd" | "live" => &["prod", "production", "prd", "live"],
        "staging" | "stage" | "stg" => &["staging", "stage", "stg"],
        _ => &[],
    };
    if aliases.is_empty() {
        said.contains(&env.as_str())
    } else {
        said.iter().any(|w| aliases.contains(w))
    }
}

/// Whether a model reading the plan could add anything: there is history to
/// build on, or the plan rests on lookups (environments, near matches).
pub fn worth_judging(history: &TicketHistory, lookups: usize) -> bool {
    !history.earlier_pr_numbers().is_empty() || lookups > 0
}

/// The plan judge's system prompt.
pub const SYSTEM: &str = "You check an implementation plan before it is kept. You judge only the \
     three points below, from what you are given: the ticket, its history (earlier pull requests \
     for it and how they ended), the data sources the team connected, every lookup the planner \
     made with what came back, and the plan.\n\
     1. history_used: the ticket has earlier pull requests. A miss when the plan doesn't say what \
     it reuses from them and what changes this time and why; when it says it couldn't find out \
     why one was closed while the history lists who closed it and when; or when a review comment \
     on one raised something the plan ignores.\n\
     2. env_unproven: only when the plan's claims (its cause, what the data shows, the scope it \
     counted) rest on data from a specific environment. A miss when the plan or its evidence \
     hedges about which environment a lookup hit (it didn't confirm it was production, isn't sure which cluster or account) while \
     a connected source could have shown it with data only that environment has. Not a miss when \
     no connected source reaches it and the plan names the missing access, and never a miss for a \
     plan whose claims don't depend on which environment the data came from.\n\
     3. near_match_unresolved: a miss when the evidence mentions an existing object close to what \
     the ticket names (a similar name, the same purpose) without saying who owns it, what is in \
     it and what created it, or without one decisive question for a person about using it \
     instead.\n\
     Judge only what is shown and do not invent concerns; a plan with no history, no lookups and \
     no near match passes all three. Each finding is short: `detail` is ONE sentence naming what \
     is missing; `settle` is ONE sentence on how to settle it.\n\
     Output ONLY a fenced ```json block: {\"findings\":[{\"point\":\"history_used|env_unproven|near_match_unresolved\",\"detail\":\"one sentence\",\"settle\":\"one sentence\"}]}. \
     An empty list when all pass.";

/// The plan judge's prompt.
pub fn prompt(
    title: &str,
    body: &str,
    history_note: &str,
    sources: &[String],
    lookups: &[String],
    files: &PlanResult,
) -> String {
    let list = |items: &[String], none: &str| {
        if items.is_empty() {
            none.to_string()
        } else {
            items
                .iter()
                .map(|i| format!("{}\n", i.trim_end()))
                .collect()
        }
    };
    let decision = files
        .fix
        .as_ref()
        .and_then(|d| serde_json::to_string_pretty(d).ok())
        .unwrap_or_else(|| "(none)".to_string());
    format!(
        "# Ticket: {title}\n{body}\n\n## The ticket's history\n{history}\n\n\
         ## Data sources connected\n{sources}\n\n## Lookups the planner made\n{lookups}\n\n\
         ## The plan\n{draft}\n\n## The plan's decision block\n{decision}\n",
        history = match history_note.trim() {
            "" => "(none)",
            h => h,
        },
        sources = list(sources, "(none connected)"),
        lookups = list(lookups, "(no lookups were made)"),
        draft = super::plan::review_draft(files),
    )
}

#[derive(Deserialize)]
struct Judged {
    #[serde(default)]
    findings: Vec<PlanFinding>,
}

/// The judge's findings, or `None` when its reply can't be read. Points
/// other than the three are dropped.
pub fn parse_findings(reply: &str) -> Option<Vec<PlanFinding>> {
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
            .filter(|f| {
                matches!(
                    f.point.trim(),
                    "history_used" | "env_unproven" | "near_match_unresolved"
                ) && !f.detail.trim().is_empty()
            })
            .collect(),
    )
}

/// What a model finds in the plan's words. Empty when it couldn't judge: a
/// judge that can't answer never holds a plan.
pub async fn judge(
    state: &WorkerState,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    prompt: &str,
) -> Vec<PlanFinding> {
    match provider::converse_simple(
        state,
        provider,
        provider.primary_model_id(),
        SYSTEM,
        prompt,
        usage,
    )
    .await
    {
        Ok(reply) => parse_findings(&reply).unwrap_or_else(|| {
            warn!(
                reply = common::truncate_str(&reply, 300),
                "Plan check reply was unreadable"
            );
            Vec::new()
        }),
        Err(e) => {
            warn!(error = %e, "Plan check errored");
            Vec::new()
        }
    }
}

/// `structural` and `judged` together, a structural finding first and the
/// judge's same-point duplicate of a structural one dropped.
pub fn merge(structural: Vec<PlanFinding>, judged: Vec<PlanFinding>) -> Vec<PlanFinding> {
    let mut out = structural;
    for f in judged {
        if !out.iter().any(|g| g == &f) {
            out.push(f);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::fix_location::{EnvCheck, FixEvidence, NearMatch};
    use crate::passes::ticket_history::EarlierPr;

    fn files(fix: FixDecision) -> PlanResult {
        PlanResult {
            proposal: "Create the new reporting user and its schema.".into(),
            design: "Mirror the existing vendor user.".into(),
            tasks: "- [ ] a.tf: add the secret".into(),
            spec: "Given the user exists...".into(),
            repo_tasks: vec![],
            fix: Some(fix),
        }
    }

    fn closed_pr() -> TicketHistory {
        TicketHistory {
            prs: vec![EarlierPr {
                repo: "acme/infra".into(),
                number: 323,
                title: "Add the vendor user's secret".into(),
                author: "coderhelm-bot".into(),
                state: "closed".into(),
                created_at: "2026-01-10T10:00:00Z".into(),
                closed_by: "a-maintainer".into(),
                closed_at: "2026-01-11T09:30:00Z".into(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn a_plan_that_ignores_earlier_prs_is_a_history_gap() {
        let h = closed_pr();
        let silent = files(FixDecision {
            history_check: "Two earlier PRs were closed and I could not find out why.".into(),
            ..Default::default()
        });
        let g = structural(&silent, &h, false);
        assert_eq!(g.len(), 2, "{g:?}");
        assert!(g.iter().all(|f| f.point == "history_used"));
        assert!(g[0].detail.contains("#323 in acme/infra"), "{g:?}");
        assert!(g[1].detail.contains("a-maintainer on 2026-01-11"), "{g:?}");
        assert!(g[1].detail.contains("ask ONE question"), "{g:?}");

        // Names it, but not who closed it or when: still a gap.
        let named = files(FixDecision {
            history_check: "#323 added only the secret; this plan keeps it.".into(),
            ..Default::default()
        });
        let g = structural(&named, &h, false);
        assert_eq!(g.len(), 1, "{g:?}");
        assert!(g[0]
            .detail
            .contains("closed without merging by a-maintainer"));

        // Who and when, from the close event, and one question: settled.
        let answered = files(FixDecision {
            history_check: "#323 (secret only) was closed by a-maintainer on 2026-01-11 with no \
                            reason given; this plan keeps its secret and adds the user."
                .into(),
            questions: vec!["Why was #323 closed?".into()],
            ..Default::default()
        });
        assert!(structural(&answered, &h, false).is_empty());
        // `#3230` is not `#323`.
        let other = files(FixDecision {
            history_check: "see #3230, closed by a-maintainer on 2026-01-11".into(),
            ..Default::default()
        });
        assert!(structural(&other, &h, false)
            .iter()
            .any(|f| f.detail.contains("names none")));
        // No history, nothing to answer.
        assert!(structural(&silent, &TicketHistory::default(), false).is_empty());
    }

    #[test]
    fn an_unproven_environment_is_a_gap_only_when_it_could_be_looked_up() {
        let d = FixDecision {
            cause: "The grant exists in staging but not in prod.".into(),
            evidence: FixEvidence {
                environments: vec![
                    EnvCheck {
                        env: "prod".into(),
                        proof: "".into(),
                        proven: false,
                    },
                    EnvCheck {
                        env: "staging".into(),
                        proof: "".into(),
                        proven: true,
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        };
        let g = structural(&files(d.clone()), &TicketHistory::default(), true);
        assert_eq!(g.len(), 2, "{g:?}");
        assert!(g.iter().all(|f| f.point == "env_unproven"));
        assert!(g[0].detail.contains("`prod` unproven"));
        assert!(g[1].detail.contains("quotes nothing"));
        // Nothing can reach it: unproven is an honest answer.
        let g = structural(&files(d), &TicketHistory::default(), false);
        assert_eq!(g.len(), 1, "{g:?}");
        let proven = FixDecision {
            cause: "Production lacks the grant.".into(),
            evidence: FixEvidence {
                environments: vec![EnvCheck {
                    env: "prod".into(),
                    proof: "the production-only marker table has 1 row".into(),
                    proven: true,
                }],
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(structural(&files(proven), &TicketHistory::default(), true).is_empty());
    }

    /// A schedule data fix with a connected source: its claims are about
    /// one class's stored time, in no particular environment. Listing
    /// production and staging as unchecked owes no environment-only proof.
    #[test]
    fn an_environment_the_claims_dont_rest_on_needs_no_proof() {
        let unproven = |env: &str| EnvCheck {
            env: env.into(),
            proof: "".into(),
            proven: false,
        };
        let d = FixDecision {
            cause: "The Saturday class is stored with a 9am start; the ticket says 8am.".into(),
            evidence: FixEvidence {
                data_shows: "class_schedule row 42 has start_time 09:00".into(),
                environments: vec![unproven("production"), unproven("staging")],
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            structural(&files(d.clone()), &TicketHistory::default(), true).is_empty(),
            "{:?}",
            structural(&files(d), &TicketHistory::default(), true)
        );
        // The same plan claiming what production holds owes production's
        // proof (and only production's).
        let d = FixDecision {
            cause: "The Saturday class is stored with a 9am start in production.".into(),
            evidence: FixEvidence {
                environments: vec![unproven("production"), unproven("staging")],
                ..Default::default()
            },
            ..Default::default()
        };
        let g = structural(&files(d), &TicketHistory::default(), true);
        assert_eq!(g.len(), 1, "{g:?}");
        assert!(g[0].detail.contains("`production` unproven"), "{g:?}");
        // Aliases and other names.
        let named = |cause: &str| FixDecision {
            cause: cause.into(),
            ..Default::default()
        };
        assert!(rests_on_env(&named("the live site shows it"), "prod"));
        assert!(rests_on_env(&named("on stage it differs"), "staging"));
        assert!(rests_on_env(&named("cluster east-2 has it"), "east-2"));
        assert!(!rests_on_env(&named("product page shows 9am"), "prod"));
        assert!(!rests_on_env(&named("the class time is wrong"), "staging"));
    }

    #[test]
    fn a_near_match_must_be_followed_to_the_end() {
        let half = NearMatch {
            name: "reports_upload".into(),
            asked_for: "reports_table_upload".into(),
            owner: "".into(),
            contents: "3 tables".into(),
            ..Default::default()
        };
        let d = FixDecision {
            evidence: FixEvidence {
                near_matches: vec![half.clone()],
                ..Default::default()
            },
            ..Default::default()
        };
        let g = structural(&files(d), &TicketHistory::default(), true);
        assert_eq!(g.len(), 1, "{g:?}");
        assert_eq!(g[0].point, "near_match_unresolved");
        assert!(g[0].detail.contains("owner, origin, question"), "{g:?}");
        let full = NearMatch {
            owner: "the data team's admin user".into(),
            grants: "usage to the loader role".into(),
            origin: "no repository creates it; made by hand".into(),
            question: "Use reports_upload instead of a new schema?".into(),
            ..half
        };
        let d = FixDecision {
            evidence: FixEvidence {
                near_matches: vec![full],
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(structural(&files(d), &TicketHistory::default(), true).is_empty());
    }

    #[test]
    fn reads_the_judges_findings_and_drops_other_points() {
        let reply = "Checked.\n```json\n{\"findings\":[\
            {\"point\":\"env_unproven\",\"detail\":\"It says it did not confirm the cluster was production.\",\"settle\":\"Query the production-only table.\"},\
            {\"point\":\"logging\",\"detail\":\"not a plan point\"},\
            {\"point\":\"near_match_unresolved\",\"detail\":\"\"}]}\n```";
        let f = parse_findings(reply).unwrap();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].point, "env_unproven");
        assert!(f[0].line().starts_with("[env_unproven] It says"));
        assert!(parse_findings("no json").is_none());
        assert!(SYSTEM.contains("1. history_used"));
        assert!(SYSTEM.contains("2. env_unproven"));
        assert!(SYSTEM.contains("3. near_match_unresolved"));
        let a = PlanFinding::new("history_used", "x".into(), "");
        assert_eq!(merge(vec![a.clone()], vec![a.clone()]), vec![a]);
    }

    #[test]
    fn only_a_plan_with_history_or_lookups_is_worth_a_model() {
        assert!(!worth_judging(&TicketHistory::default(), 0));
        assert!(worth_judging(&TicketHistory::default(), 2));
        assert!(worth_judging(&closed_pr(), 0));
    }
}
