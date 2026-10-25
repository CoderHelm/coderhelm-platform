//! Presets: one click sets a sensible starting point for the team's rules.
//!
//! - **Cautious**: read only. Agents read on their own and ask before any
//!   change, including writes on their own branch.
//! - **Standard** (the default): agents read and work on their branch on
//!   their own; anything that reaches people asks.
//! - **Autonomous within the cap**: every write runs without asking, up to
//!   the team's autonomy limit. Anything above the limit still asks.
//!
//! A preset replaces only the rules a preset wrote; rules people wrote and
//! rules from "always" answers stay.

use serde::{Deserialize, Serialize};

use super::model::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Preset {
    Cautious,
    Standard,
    Autonomous,
}

impl Preset {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "cautious" => Some(Preset::Cautious),
            "standard" => Some(Preset::Standard),
            "autonomous" => Some(Preset::Autonomous),
            _ => None,
        }
    }
    pub fn key(self) -> &'static str {
        match self {
            Preset::Cautious => "cautious",
            Preset::Standard => "standard",
            Preset::Autonomous => "autonomous",
        }
    }
}

/// The build and test commands CoderHelm reads from a repository's own CI
/// and package scripts. The model never writes these: they come from the
/// repository. Standard and Autonomous let them run; Cautious asks.
fn ci_commands_rule(decision: Decision, note: &str, by: &str, now: &str) -> Rule {
    Rule {
        id: "preset_ci_commands".into(),
        scope: Scope::Team,
        repo: None,
        agent: None,
        kind: Some(CallKind::Command),
        server: None,
        tool: None,
        risk: None,
        command: None,
        decision,
        locked: false,
        note: note.to_string(),
        source: RuleSource::Preset,
        created_by: by.to_string(),
        created_at: now.to_string(),
    }
}

fn preset_rule(id: &str, risk: Risk, decision: Decision, note: &str, by: &str, now: &str) -> Rule {
    Rule {
        id: format!("preset_{id}"),
        scope: Scope::Team,
        repo: None,
        agent: None,
        kind: None,
        server: None,
        tool: None,
        risk: Some(risk),
        command: None,
        decision,
        locked: false,
        note: note.to_string(),
        source: RuleSource::Preset,
        created_by: by.to_string(),
        created_at: now.to_string(),
    }
}

/// The team's rules with `preset` applied: the preset's own rules replaced,
/// everything else kept. Autonomous allows writes, and consequential only
/// when the cap is Full.
pub fn apply(policy: &mut TeamPolicy, preset: Preset, by: &str, now: &str) {
    policy.rules.retain(|r| r.source != RuleSource::Preset);
    let mut add = Vec::new();
    match preset {
        Preset::Cautious => {
            add.push(preset_rule(
                "write_ask",
                Risk::Write,
                Decision::Ask,
                "Cautious: every change asks.",
                by,
                now,
            ));
            add.push(preset_rule(
                "consequential_ask",
                Risk::Consequential,
                Decision::Ask,
                "Cautious: actions that reach people ask.",
                by,
                now,
            ));
            add.push(ci_commands_rule(
                Decision::Ask,
                "Cautious: sandbox commands not on a repository's allowlist ask.",
                by,
                now,
            ));
        }
        Preset::Standard => {
            add.push(preset_rule(
                "write_allow",
                Risk::Write,
                Decision::Allow,
                "Standard: work on the agent's branch runs on its own.",
                by,
                now,
            ));
            add.push(preset_rule(
                "consequential_ask",
                Risk::Consequential,
                Decision::Ask,
                "Standard: actions that reach people ask.",
                by,
                now,
            ));
            add.push(ci_commands_rule(
                Decision::Allow,
                "Standard: the repository's own build and test commands run in the sandbox.",
                by,
                now,
            ));
        }
        Preset::Autonomous => {
            add.push(preset_rule(
                "write_allow",
                Risk::Write,
                Decision::Allow,
                "Autonomous: changes run on their own.",
                by,
                now,
            ));
            add.push(preset_rule(
                "consequential",
                Risk::Consequential,
                if policy.autonomy == Autonomy::Full {
                    Decision::Allow
                } else {
                    Decision::Ask
                },
                "Autonomous: actions that reach people run on their own when the limit allows.",
                by,
                now,
            ));
            add.push(ci_commands_rule(
                Decision::Allow,
                "Autonomous: the repository's own build and test commands run in the sandbox.",
                by,
                now,
            ));
        }
    }
    // Cautious also lowers the cap's effect: writes are held by the rule;
    // the cap itself is the team's to choose.
    policy.rules.extend(add);
}

/// The preset the team's rules match now, if any.
pub fn current(policy: &TeamPolicy) -> Option<Preset> {
    let ids: Vec<(&str, Decision)> = policy
        .rules
        .iter()
        .filter(|r| r.source == RuleSource::Preset)
        .map(|r| (r.id.as_str(), r.decision))
        .collect();
    let has = |id: &str, d: Decision| ids.contains(&(id, d));
    if has("preset_write_ask", Decision::Ask) {
        Some(Preset::Cautious)
    } else if has("preset_consequential", Decision::Allow)
        || has("preset_consequential", Decision::Ask)
    {
        Some(Preset::Autonomous)
    } else if has("preset_write_allow", Decision::Allow) {
        Some(Preset::Standard)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_policy::engine::decide;

    const NOW: &str = "2026-10-24T12:00:00Z";

    #[test]
    fn presets_decide_as_described_and_keep_people_rules() {
        let mut p = TeamPolicy::default();
        p.rules.push(Rule {
            id: "mine".into(),
            scope: Scope::Team,
            repo: None,
            agent: None,
            kind: None,
            server: None,
            tool: Some("confluence_append".into()),
            risk: None,
            command: None,
            decision: Decision::Deny,
            locked: false,
            note: String::new(),
            source: RuleSource::Manual,
            created_by: String::new(),
            created_at: String::new(),
        });
        let write = Call::tool(Agent::Coder, "write_file").in_repo(Some("acme/web"));
        let send =
            Call::tool(Agent::Coder, "slack__send_message").on_server(CallKind::Mcp, "slack");

        apply(&mut p, Preset::Cautious, "a@x.dev", NOW);
        assert_eq!(current(&p), Some(Preset::Cautious));
        assert_eq!(
            decide(&p, &write, Some(Risk::Write), NOW).decision,
            Decision::Ask
        );

        apply(&mut p, Preset::Standard, "a@x.dev", NOW);
        assert_eq!(current(&p), Some(Preset::Standard));
        assert_eq!(
            decide(&p, &write, Some(Risk::Write), NOW).decision,
            Decision::Allow
        );
        assert_eq!(
            decide(&p, &send, Some(Risk::Consequential), NOW).decision,
            Decision::Ask
        );

        apply(&mut p, Preset::Autonomous, "a@x.dev", NOW);
        assert_eq!(current(&p), Some(Preset::Autonomous));
        // Within the Standard cap, consequential still asks.
        assert_eq!(
            decide(&p, &send, Some(Risk::Consequential), NOW).decision,
            Decision::Ask
        );
        p.autonomy = Autonomy::Full;
        apply(&mut p, Preset::Autonomous, "a@x.dev", NOW);
        assert_eq!(
            decide(&p, &send, Some(Risk::Consequential), NOW).decision,
            Decision::Allow
        );

        // The rule a person wrote is still there, once.
        assert_eq!(p.rules.iter().filter(|r| r.id == "mine").count(), 1);
        assert_eq!(
            p.rules
                .iter()
                .filter(|r| r.source == RuleSource::Preset)
                .count(),
            3
        );
    }
}
