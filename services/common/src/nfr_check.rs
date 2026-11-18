//! What a plan settles before code when its change reaches infrastructure,
//! CI, auth and security, data, a hot path or observability.
//!
//! A plan for a change to a Terraform module, a migration or an alarm read
//! the code and planned the edit, and left unsaid what a careful engineer
//! settles first: how it rolls out and back, what else the resource
//! serves, the permissions and cost it adds, the data it meets in each
//! environment. Here those questions are data, one short checklist per
//! area, in one file ([`CHECKLISTS`], `nfr_checklists.json`):
//!
//! - **Which areas apply** ([`areas_for`]): a planned file matches the
//!   area's paths (an extension, a file name, a folder, a path prefix, a
//!   word in the path), or the ticket names one of its words.
//! - **The plan answers each item** under [`HEADING`] in design.md, one
//!   line per item: `- <item>: <answer> (source: <file, command or
//!   lookup>)` under a `### <area>` heading, or `- <area>.<item>: ...`.
//!   An answer comes from the code or a lookup, so it says where; `n/a:
//!   <reason>` says why the item doesn't apply.
//! - **What is unanswered is a plan gap** ([`plan_gaps`]), sent back with
//!   the plan's other gaps. It adds no stop and no approval: the plan goes
//!   back once like any other gap, and nothing waits on a person.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use serde::Deserialize;

/// The checklists, as data.
pub const CHECKLISTS: &str = include_str!("nfr_checklists.json");

/// The design.md section the answers go under.
pub const HEADING: &str = "## Non-functional checks";

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Paths {
    #[serde(default)]
    pub ext: Vec<String>,
    #[serde(default)]
    pub names: Vec<String>,
    #[serde(default)]
    pub dirs: Vec<String>,
    #[serde(default)]
    pub prefixes: Vec<String>,
    #[serde(default)]
    pub contains: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Item {
    pub id: String,
    pub ask: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Area {
    pub id: String,
    pub title: String,
    pub when: String,
    #[serde(default)]
    pub paths: Paths,
    #[serde(default)]
    pub words: Vec<String>,
    pub items: Vec<Item>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct File {
    areas: Vec<Area>,
}

/// Every area, read once from [`CHECKLISTS`].
pub fn areas() -> &'static [Area] {
    static AREAS: OnceLock<Vec<Area>> = OnceLock::new();
    AREAS.get_or_init(|| {
        serde_json::from_str::<File>(CHECKLISTS)
            .map(|f| f.areas)
            .unwrap_or_default()
    })
}

impl Area {
    /// Whether `path` is one of this area's files.
    pub fn matches_path(&self, path: &str) -> bool {
        let low = path.trim_start_matches("./").to_ascii_lowercase();
        let name = low.rsplit('/').next().unwrap_or(&low);
        let dirs: Vec<&str> = low.split('/').collect();
        let dirs = &dirs[..dirs.len().saturating_sub(1)];
        let p = &self.paths;
        p.ext.iter().any(|e| name.ends_with(e.as_str()))
            || p.names.iter().any(|n| name == n)
            || p.dirs.iter().any(|d| dirs.contains(&d.as_str()))
            || p.prefixes.iter().any(|x| low.starts_with(x.as_str()))
            || p.contains.iter().any(|c| {
                low.split('/')
                    .any(|seg| seg.contains(c.as_str()) && !seg.contains("author"))
            })
    }

    /// Whether `text` (the ticket) names one of this area's words: a
    /// phrase anywhere, a single word as a whole word.
    pub fn named_in(&self, text: &str) -> bool {
        let low = text.to_ascii_lowercase();
        let words: BTreeSet<&str> = low
            .split(|c: char| !(c.is_ascii_alphanumeric() || c == '+' || c == '-'))
            .filter(|w| !w.is_empty())
            .collect();
        self.words.iter().any(|w| {
            if w.contains(' ') {
                low.contains(w.as_str())
            } else {
                words.contains(w.as_str())
            }
        })
    }
}

/// Why an area applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applies {
    pub area: String,
    /// The planned files that matched, or empty when the ticket's words did.
    pub files: Vec<String>,
}

/// The areas a plan answers for: by its planned files, or by the ticket.
pub fn areas_for<S: AsRef<str>>(files: &[S], ticket: &str) -> Vec<Applies> {
    areas()
        .iter()
        .filter_map(|a| {
            let hit: Vec<String> = files
                .iter()
                .map(|f| f.as_ref())
                .filter(|f| a.matches_path(f))
                .map(str::to_string)
                .collect();
            (!hit.is_empty() || a.named_in(ticket)).then(|| Applies {
                area: a.id.clone(),
                files: hit,
            })
        })
        .collect()
}

fn area(id: &str) -> Option<&'static Area> {
    areas().iter().find(|a| a.id == id)
}

/// One answer line of the plan's section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub area: String,
    pub item: String,
    pub text: String,
}

fn key(s: &str) -> String {
    s.trim()
        .trim_matches(|c: char| c == '`' || c == '*')
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_")
}

/// The area a `### ` heading names: its id or its title.
fn area_of_heading(h: &str) -> Option<String> {
    let k = key(h);
    areas()
        .iter()
        .find(|a| key(&a.id) == k || key(&a.title) == k)
        .map(|a| a.id.clone())
}

/// The answers under [`HEADING`] in `design`.
pub fn parse(design: &str) -> Option<Vec<Answer>> {
    let at = design.find(HEADING)?;
    let rest = &design[at + HEADING.len()..];
    let end = rest.find("\n## ").unwrap_or(rest.len());
    let mut out = Vec::new();
    let mut current: Option<String> = None;
    for line in rest[..end].lines() {
        let t = line.trim();
        if let Some(h) = t.strip_prefix("### ") {
            current = area_of_heading(h);
            continue;
        }
        let Some(t) = t.strip_prefix("- ").or_else(|| t.strip_prefix("* ")) else {
            continue;
        };
        let Some((k, v)) = t.split_once(':') else {
            continue;
        };
        let k = key(k);
        let (a, i) = match k.split_once(['.', '/']) {
            Some((a, i)) if area(a).is_some() => (a.to_string(), i.to_string()),
            _ => match &current {
                Some(c) => (c.clone(), k.clone()),
                None => continue,
            },
        };
        out.push(Answer {
            area: a,
            item: i,
            text: v.trim().to_string(),
        });
    }
    Some(out)
}

/// An answer that says the item doesn't apply, and its reason (possibly
/// empty).
fn not_applicable(text: &str) -> Option<&str> {
    let low = text.to_ascii_lowercase();
    for p in [
        "n/a",
        "na",
        "not applicable",
        "doesn't apply",
        "does not apply",
        "none",
    ] {
        if low == p || low.starts_with(&format!("{p}:")) || low.starts_with(&format!("{p} ")) {
            let rest = &text[p.len()..];
            return Some(rest.trim_start_matches([':', ' ', '-', '—', '(']).trim());
        }
    }
    None
}

/// Whether an answer says where it comes from: a `source:`, a backticked
/// file or command, or the lookup it rests on.
fn has_source(text: &str) -> bool {
    let low = text.to_ascii_lowercase();
    low.contains("source:")
        || low.contains("from the lookup")
        || low.contains("lookup:")
        || text.matches('`').count() >= 2
}

/// What the plan leaves unanswered, one line each for its send-back. Empty
/// when no area applies.
pub fn plan_gaps<S: AsRef<str>>(design: &str, files: &[S], ticket: &str) -> Vec<String> {
    let applies = areas_for(files, ticket);
    if applies.is_empty() {
        return Vec::new();
    }
    let why = |a: &Applies| {
        if a.files.is_empty() {
            "the ticket names it".to_string()
        } else {
            format!(
                "the plan changes {}",
                a.files
                    .iter()
                    .take(3)
                    .map(|f| format!("`{f}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    };
    let Some(answers) = parse(design) else {
        let list = applies
            .iter()
            .filter_map(|a| area(&a.area).map(|ar| (a, ar)))
            .map(|(a, ar)| {
                format!(
                    "{} ({}): {}",
                    ar.title,
                    why(a),
                    ar.items
                        .iter()
                        .map(|i| format!("`{}`", i.id))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        return vec![format!(
            "design.md has no `{HEADING}` section, and the change reaches {list}. Under it, write a `### <area>` heading and one line per item: `- <item>: <answer> (source: <file, command or lookup>)`, or `- <item>: n/a: <why it doesn't apply>`. Answer from the code and lookups, not from a person."
        )];
    };
    let mut out = Vec::new();
    for a in &applies {
        let Some(ar) = area(&a.area) else { continue };
        for item in &ar.items {
            let found = answers
                .iter()
                .find(|x| x.area == ar.id && key(&x.item) == key(&item.id));
            let name = format!("{} `{}`", ar.title, item.id);
            match found {
                None => out.push(format!(
                    "`{HEADING}` doesn't answer {name} ({}): {}. Answer it from the code or a lookup, with its source, or say `n/a: <why>`.",
                    why(a),
                    item.ask
                )),
                Some(x) => match not_applicable(&x.text) {
                    Some(reason) if reason.split_whitespace().count() < 3 => out.push(format!(
                        "{name} says it doesn't apply without saying why. Give the reason (`n/a: <why>`), or answer it."
                    )),
                    Some(_) => {}
                    None if x.text.split_whitespace().count() < 3 => out.push(format!(
                        "{name} has no real answer (\"{}\"): {}.",
                        x.text, item.ask
                    )),
                    None if !has_source(&x.text) => out.push(format!(
                        "{name} doesn't say where its answer comes from. Name the file, command or lookup it rests on (`source: ...`)."
                    )),
                    None => {}
                },
            }
        }
    }
    out
}

/// What the planner is told: the rule, and every area's checklist in one
/// line each.
pub fn planner_section() -> String {
    let mut out = format!(
        "\n\n## Non-functional checks (CoderHelm checks them)\n\
When the change reaches one of the areas below (by the files it plans, or because the ticket \
names it), write a `{HEADING}` section in design.md with a `### <area>` heading per area and one \
line per item: `- <item>: <answer> (source: <file, command or lookup>)`. Answer from the code \
and from lookups, never from a guess; an item that doesn't apply says `n/a: <why>`. A plan whose \
change reaches none of them leaves the section out.\n"
    );
    for a in areas() {
        out.push_str(&format!(
            "- {} (`{}`), when {}: {}\n",
            a.title,
            a.id,
            a.when,
            a.items
                .iter()
                .map(|i| format!("`{}` ({})", i.id, i.ask))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_checklists_load_and_each_area_has_items() {
        let ids: Vec<&str> = areas().iter().map(|a| a.id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "infrastructure",
                "ci",
                "security",
                "data",
                "performance",
                "observability"
            ]
        );
        for a in areas() {
            assert!(a.items.len() >= 4, "{}", a.id);
            assert!(!a.when.is_empty() && !a.title.is_empty());
            let ids: BTreeSet<&str> = a.items.iter().map(|i| i.id.as_str()).collect();
            assert_eq!(ids.len(), a.items.len(), "duplicate item in {}", a.id);
        }
    }

    #[test]
    fn areas_apply_by_planned_files_or_the_ticket() {
        let got = |files: &[&str], ticket: &str| -> Vec<String> {
            areas_for(files, ticket)
                .into_iter()
                .map(|a| a.area)
                .collect()
        };
        assert_eq!(
            got(&["infra/queues.tf"], "Add a queue"),
            vec!["infrastructure"]
        );
        assert_eq!(
            got(&[".github/workflows/deploy.yml"], "Speed up deploys"),
            vec!["ci"]
        );
        assert_eq!(
            got(&["db/migrations/0042_add_slug.sql"], "Add a slug"),
            vec!["data"]
        );
        assert_eq!(
            got(&["src/auth/session.ts"], "Fix sign out"),
            vec!["security"]
        );
        assert_eq!(
            got(&["ops/alarms/api.ts"], "Page on 5xx"),
            vec!["observability"]
        );
        assert_eq!(
            got(&["src/search.ts"], "Search is slow on large teams"),
            vec!["performance"]
        );
        assert!(got(
            &["src/components/Button.tsx", "docs/authors.md"],
            "Fix the label"
        )
        .is_empty());
    }

    const DESIGN_OK: &str = "## Approach\nAdd the queue.\n\n## Non-functional checks\n### Infrastructure\n- rollout: applied by the `deploy-staging` then `deploy-prod` workflow (source: `.github/workflows/deploy.yml`)\n- rollback: revert the commit; the queue is empty until the producer ships (source: `infra/queues.tf`)\n- blast_radius: only the new queue; no existing resource changes (source: `terraform plan` in CI)\n- environments: staging and prod both apply `infra/` (source: `infra/envs/`)\n- permissions: the worker role gets `sqs:SendMessage` on this queue only (source: `infra/iam.tf`)\n- cost: n/a: under the free tier at the measured 2k messages a day\n- limits: message size 256 KB; payloads are under 4 KB (source: `src/jobs/payload.ts`)\n- monitoring: the age-of-oldest-message alarm in `infra/alarms.tf` (source: `infra/alarms.tf`)\n";

    #[test]
    fn a_plan_that_answers_every_item_has_no_gaps() {
        let gaps = plan_gaps(DESIGN_OK, &["infra/queues.tf"], "Add a queue");
        assert!(gaps.is_empty(), "{gaps:#?}");
        // The `area.item` form reads the same.
        let flat = "## Non-functional checks\n- data.migration_safety: 3k rows, no lock (source: lookup: row count)\n- data.compatibility: the column is nullable first (source: `db/migrations/0042.sql`)\n- data.rollback: drop the column (source: `db/migrations/0042.down.sql`)\n- data.environments: staging 1.2k rows, prod 3k (source: lookup: counts per environment)\n- data.existing_rows: n/a: the column is new and stays empty until the next release\n";
        assert!(plan_gaps(flat, &["db/migrations/0042.sql"], "").is_empty());
    }

    #[test]
    fn missing_section_items_reasons_and_sources_are_gaps() {
        let none = plan_gaps("## Approach\nx\n", &["infra/queues.tf"], "Add a queue");
        assert_eq!(none.len(), 1);
        assert!(none[0].contains("`## Non-functional checks`") && none[0].contains("`rollout`"));
        let weak = "## Non-functional checks\n### Infrastructure\n- rollout: by the deploy workflow\n- rollback: n/a\n- cost: tbd\n";
        let gaps = plan_gaps(weak, &["infra/queues.tf"], "");
        assert!(
            gaps.iter()
                .any(|g| g.contains("`rollout`") && g.contains("where its answer comes from")),
            "{gaps:#?}"
        );
        assert!(gaps
            .iter()
            .any(|g| g.contains("`rollback`") && g.contains("without saying why")));
        assert!(gaps
            .iter()
            .any(|g| g.contains("`cost`") && g.contains("no real answer")));
        assert!(gaps
            .iter()
            .any(|g| g.contains("`monitoring`") && g.contains("doesn't answer")));
        // Five not answered at all, three answered badly.
        assert_eq!(gaps.len(), 8, "{gaps:#?}");
    }

    #[test]
    fn a_change_outside_every_area_needs_nothing() {
        assert!(plan_gaps(
            "## Approach\nx",
            &["src/components/Card.tsx"],
            "Fix the card title"
        )
        .is_empty());
    }

    #[test]
    fn the_planner_is_told_every_area() {
        let s = planner_section();
        for a in areas() {
            assert!(s.contains(&format!("(`{}`)", a.id)), "{}", a.id);
        }
        assert!(s.contains(HEADING));
    }
}
