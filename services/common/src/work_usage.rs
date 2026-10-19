//! What a piece of work used: the team skills it read, the team agents it
//! asked (and the tools they called), the data sources it looked up and the
//! standards it was given in full. Worked out from the access log and the
//! data source log, which every read, ask and lookup is kept on with the
//! run or work it happened during.

use serde::Serialize;

use crate::live::store::Lookup;
use crate::skills::{SkillAccess, SkillStore, ACCESS_SCAN_CAP, AGENT_TOOL};

/// The work: a run (by id), or work without a run, by where its records say
/// they came from ("owner/name#12" for a review, "plan chat <id>"), with the
/// pull request a review's lookups name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Work {
    pub run_id: String,
    pub origin: String,
    pub repo: String,
    pub pr: u64,
}

impl Work {
    fn has_access(&self, a: &SkillAccess) -> bool {
        (!self.run_id.is_empty() && a.run_id == self.run_id)
            || (self.run_id.is_empty() && !self.origin.is_empty() && a.origin == self.origin)
    }

    fn has_lookup(&self, l: &Lookup) -> bool {
        if !self.run_id.is_empty() {
            return l.run_id == self.run_id;
        }
        (!self.origin.is_empty() && l.origin == self.origin)
            || (!self.repo.is_empty() && self.pr > 0 && l.repo == self.repo && l.pr == self.pr)
    }
}

/// One item read, asked or given in full.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Item {
    pub name: String,
    pub kind: String,
    pub repo: String,
    pub path: String,
    /// Reads, asks or applications.
    pub count: usize,
    /// For an agent: the tools it called answering, each once.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Source {
    pub id: String,
    pub name: String,
    pub lookups: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Used {
    pub skills: Vec<Item>,
    pub agents: Vec<Item>,
    pub sources: Vec<Source>,
    pub standards: Vec<Item>,
}

fn bump(list: &mut Vec<Item>, a: &SkillAccess) -> usize {
    if let Some(i) = list
        .iter()
        .position(|x| x.repo == a.repo && x.path == a.path)
    {
        return i;
    }
    list.push(Item {
        name: a.name.clone(),
        kind: a.kind.clone(),
        repo: a.repo.clone(),
        path: a.path.clone(),
        count: 0,
        tools: Vec::new(),
    });
    list.len() - 1
}

/// What `work` used, from its records, in the order first used.
pub fn tally(work: &Work, access: &[SkillAccess], lookups: &[Lookup]) -> Used {
    let mut u = Used::default();
    for a in access.iter().filter(|a| work.has_access(a)) {
        match a.action.as_str() {
            "apply" => {
                let i = bump(&mut u.standards, a);
                u.standards[i].count += 1;
            }
            "ask" => {
                let i = bump(&mut u.agents, a);
                u.agents[i].count += 1;
            }
            AGENT_TOOL => {
                let i = bump(&mut u.agents, a);
                if !a.file.is_empty() && !u.agents[i].tools.contains(&a.file) {
                    u.agents[i].tools.push(a.file.clone());
                }
            }
            _ if a.outcome == "ok" => {
                let i = bump(&mut u.skills, a);
                u.skills[i].count += 1;
            }
            _ => {}
        }
    }
    for l in lookups.iter().filter(|l| work.has_lookup(l)) {
        match u
            .sources
            .iter_mut()
            .find(|s| s.id == l.source_id && s.name == l.source_name)
        {
            Some(s) => s.lookups += 1,
            None => u.sources.push(Source {
                id: l.source_id.clone(),
                name: l.source_name.clone(),
                lookups: 1,
            }),
        }
    }
    u
}

/// What `work` used between `since` and `until` (RFC 3339).
pub async fn load(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team: &str,
    work: &Work,
    since: &str,
    until: &str,
) -> Result<Used, String> {
    let st = SkillStore {
        dynamo,
        table,
        team,
    };
    let (access, lookups) = tokio::join!(
        st.access_between(since, until, |a| work.has_access(a)),
        crate::live::store::lookups_between(dynamo, table, team, since, until, ACCESS_SCAN_CAP),
    );
    Ok(tally(work, &access?, &lookups?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn access(action: &str, path: &str, run: &str, file: &str) -> SkillAccess {
        SkillAccess {
            repo: "acme/playbook".into(),
            path: path.into(),
            name: path.split('/').next_back().unwrap_or("").into(),
            kind: "skill".into(),
            action: action.into(),
            run_id: run.into(),
            file: file.into(),
            outcome: "ok".into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_runs_records_add_up_to_what_it_used() {
        let mut failed = access("read", "skills/x", "r1", "");
        failed.outcome = "limit".into();
        let recs = [
            access("apply", "skills/standards", "r1", ""),
            access("read", "skills/debug", "r1", ""),
            access("read", "skills/debug", "r1", ""),
            failed,
            access("ask", "agents/debt", "r1", ""),
            access(AGENT_TOOL, "agents/debt", "r1", "read_file"),
            access(AGENT_TOOL, "agents/debt", "r1", "read_file"),
            access(AGENT_TOOL, "agents/debt", "r1", "search_code"),
            access("read", "skills/other-run", "r2", ""),
        ];
        let lookups = [
            Lookup {
                source_id: "s1".into(),
                source_name: "Orders DB".into(),
                run_id: "r1".into(),
                ..Default::default()
            },
            Lookup {
                source_id: "s1".into(),
                source_name: "Orders DB".into(),
                run_id: "r1".into(),
                ..Default::default()
            },
            Lookup {
                source_id: "s2".into(),
                source_name: "Logs".into(),
                run_id: "r2".into(),
                ..Default::default()
            },
        ];
        let w = Work {
            run_id: "r1".into(),
            ..Default::default()
        };
        let u = tally(&w, &recs, &lookups);
        assert_eq!(u.standards.len(), 1);
        assert_eq!(u.skills.len(), 1, "a read that didn't happen isn't a use");
        assert_eq!(u.skills[0].count, 2);
        assert_eq!(u.agents[0].count, 1);
        assert_eq!(u.agents[0].tools, ["read_file", "search_code"]);
        assert_eq!(
            u.sources,
            [Source {
                id: "s1".into(),
                name: "Orders DB".into(),
                lookups: 2
            }]
        );
    }

    #[test]
    fn a_review_is_found_by_its_origin_and_pull_request() {
        let mut a = access("read", "skills/debug", "", "");
        a.origin = "acme/web#7".into();
        let l = Lookup {
            source_name: "Logs".into(),
            repo: "acme/web".into(),
            pr: 7,
            ..Default::default()
        };
        let w = Work {
            origin: "acme/web#7".into(),
            repo: "acme/web".into(),
            pr: 7,
            ..Default::default()
        };
        let u = tally(&w, std::slice::from_ref(&a), std::slice::from_ref(&l));
        assert_eq!((u.skills.len(), u.sources.len()), (1, 1));
        let other = Work {
            origin: "acme/web#8".into(),
            repo: "acme/web".into(),
            pr: 8,
            ..Default::default()
        };
        assert_eq!(tally(&other, &[a], &[l]), Used::default());
    }
}
