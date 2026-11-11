//! Recommendations and the plan Review shows.
//!
//! The policy: progress first (the reviewer on, security fixes opening pull
//! requests, the code graph on); a setting that acts (closes, merges, posts
//! outside CoderHelm) carries evidence of what it would do and starts at
//! its safe start; auto-merge only where branch protection requires checks.
//! An inferred value with no evidence is never recommended.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::ai::Inferred;
use super::facts::{self, Found, RepoFacts};
use super::registry::{self, Current, Entry, Feature, Kind, Risk, Scope, REGISTRY};
use super::{Evidence, Source};
use crate::stale_prs::StaleConfig;

/// One recommended value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rec {
    pub value: Value,
    pub source: Source,
    pub evidence: Vec<Evidence>,
}

/// The review choice at the top of Review.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Choice {
    /// Anything already set stays; only settings never set take the
    /// recommendation.
    #[default]
    KeepMine,
    /// "Do it for me": every setting takes the recommendation unless a
    /// person deliberately chose its value for this repo ([`is_deliberate`]);
    /// a value an earlier Setup run wrote, a default, or a repo missing
    /// from a team list is not a choice. A deliberate choice the repo's
    /// facts contradict is kept and listed as worth a look.
    Smart,
    /// Every setting takes the recommendation (Review lists each overwrite).
    UseRecommended,
    /// Each setting where yours and the recommendation differ is picked one
    /// by one; until picked, yours stays.
    Decide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Use,
    Keep,
    Edit,
}

/// One explicit pick in Review.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemDecision {
    pub repo: String,
    pub setting_id: String,
    pub decision: Decision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

/// The current value as Review shows it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CurrentView {
    pub value: Value,
    /// Someone set it (not only the default).
    pub set: bool,
    /// A person chose this value for this repo ([`is_deliberate`]). Older
    /// plans don't have it: then it is `set`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deliberate: Option<bool>,
    /// Who saved it and when, where the row says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set_by: Option<registry::SetBy>,
}

impl CurrentView {
    pub fn deliberate(&self) -> bool {
        self.deliberate.unwrap_or(self.set)
    }
}

/// One setting of one repo's plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanItem {
    pub setting_id: String,
    pub feature: Feature,
    pub title: String,
    pub help: String,
    pub scope: Scope,
    pub risk: Risk,
    /// The value's shape (`registry::Kind`).
    pub kind: Value,
    pub since: String,
    pub capability: String,
    pub current: CurrentView,
    pub recommended: Value,
    pub source: Source,
    pub evidence: Vec<Evidence>,
    /// The current value differs from what the last Setup run applied: it
    /// was changed by hand.
    pub custom: bool,
    /// Not reviewed by the team before (a new recommendation).
    #[serde(default)]
    pub is_new: bool,
    /// The decision as things stand (the run's choice, or an explicit pick).
    pub decision: Decision,
    /// The edited value, for `edit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Text settings: current against recommended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff: Option<String>,
    /// The capability the viewer lacks to change it (filled per viewer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needs: Option<String>,
    /// A deliberate choice the repo's facts argue against ([`worth_a_look`]),
    /// filled when the run is shown.
    #[serde(default)]
    pub worth_a_look: bool,
}

fn rec(value: Value, source: Source, evidence: Vec<Evidence>) -> Rec {
    Rec {
        value,
        source,
        evidence,
    }
}

fn from_found(f: &Found) -> Rec {
    rec(f.value.clone(), Source::Detected, f.evidence.clone())
}

fn from_ai(i: &Inferred) -> Rec {
    rec(i.value.clone(), Source::Inferred, i.evidence.clone())
}

fn default_rec(e: &Entry) -> Rec {
    let why = if e.policy.is_some() && registry::policy_value(e) != registry::default_value(e) {
        match e.id {
            "reviewer.enabled" => "CoderHelm recommends the reviewer on for every repo".to_string(),
            "security.open_prs" => {
                "CoderHelm recommends security fixes that open pull requests".to_string()
            }
            _ => "CoderHelm recommends this for every repo".to_string(),
        }
    } else {
        "CoderHelm's default; nothing in the repo says otherwise".to_string()
    };
    rec(
        registry::policy_value(e),
        Source::Default,
        vec![Evidence::text(why)],
    )
}

/// The tier schedule security fix pull requests open on, in words.
fn security_schedule() -> String {
    let c = crate::security_fixes::SecurityConfig::default();
    let word = |x: crate::security_fixes::Cadence| match x {
        crate::security_fixes::Cadence::Now => "at once",
        crate::security_fixes::Cadence::Hour => "hourly",
        crate::security_fixes::Cadence::Daily => "daily",
        crate::security_fixes::Cadence::Weekly => "weekly",
        crate::security_fixes::Cadence::Off => "never",
    };
    format!(
        "Fix pull requests open on the security policy's schedule: P0 {}, P1 {}, P2 {}, P3 {}",
        word(c.open_prs.p0),
        word(c.open_prs.p1),
        word(c.open_prs.p2),
        word(c.open_prs.p3)
    )
}

/// Every setting's recommendation for one repo, from what the detectors
/// found and what the AI read inferred (already checked by `ai::parse`).
pub fn recommend(
    f: &RepoFacts,
    found: &HashMap<&'static str, Found>,
    inferred: &HashMap<&'static str, Inferred>,
    now: DateTime<Utc>,
) -> HashMap<&'static str, Rec> {
    let mut out = HashMap::new();
    for e in REGISTRY {
        let r = match e.id {
            // Auto-merge: detected "no" without required checks; otherwise
            // the AI read decides, citing the required checks as its effect.
            "auto_merge.enabled" => match (found.get(e.id), inferred.get(e.id)) {
                (Some(d), _) => from_found(d),
                (None, Some(i)) if !f.required_checks.is_empty() => {
                    let mut r = from_ai(i);
                    if r.value == json!(true) {
                        let n = f.merged_within(now, facts::STALE_WINDOW_DAYS);
                        r.evidence.insert(
                            0,
                            Evidence::text(format!(
                                "Over the last {} days this would have merged {n} pull requests, each once it was approved and {} passed",
                                facts::STALE_WINDOW_DAYS,
                                f.required_checks.join(", ")
                            )),
                        );
                        r.evidence.push(Evidence::text(
                            "It starts as a preview: nothing merges by itself until you switch it on",
                        ));
                    }
                    r
                }
                _ => default_rec(e),
            },
            // Stale cleanup: on, as a preview, with what it would have done.
            "stale_prs.enabled" if !f.open.is_empty() || !f.merged.is_empty() => {
                let mut cfg = StaleConfig::default();
                if let Some(i) = inferred.get("stale_prs.stale_days") {
                    cfg.stale_days = i.value.as_u64().unwrap_or(cfg.stale_days as u64) as u32;
                }
                let (marked, closed) =
                    facts::stale_effect(&cfg, &f.open, now, facts::STALE_WINDOW_DAYS);
                // The numbers are the ones this recommendation applies.
                let days = |n: u32| {
                    if n == 1 {
                        "1 day".to_string()
                    } else {
                        format!("{n} days")
                    }
                };
                let mut ev = vec![Evidence::text(format!(
                    "Over the last {} days this would have marked {marked} and closed {closed} of the {} open pull requests ({} with no activity, then closed {} later)",
                    facts::STALE_WINDOW_DAYS,
                    f.open.len() + f.open_not_read,
                    days(cfg.stale_days),
                    days(cfg.close_days)
                ))];
                if let Some(h) = f.median_hours_to_merge() {
                    ev.push(Evidence::text(format!(
                        "Median time to merge here is {:.1} days",
                        h / 24.0
                    )));
                }
                ev.push(Evidence::text(
                    "It starts as a preview: nothing is marked or closed until you turn the preview off",
                ));
                rec(json!(true), Source::Detected, ev)
            }
            "security.open_prs" => {
                let mut r = default_rec(e);
                r.evidence.push(Evidence::text(security_schedule()));
                r
            }
            id => match (found.get(id), inferred.get(id)) {
                (_, Some(i)) if e.recommend == registry::Recommend::Ai => from_ai(i),
                (Some(d), _) => from_found(d),
                (None, Some(i)) => from_ai(i),
                _ => default_rec(e),
            },
        };
        // A value the setting can't take is never recommended.
        let r = match registry::check(e, &r.value) {
            Ok(v) => Rec { value: v, ..r },
            Err(_) => default_rec(e),
        };
        // An inferred value always carries evidence.
        let r = if r.source == Source::Inferred && r.evidence.is_empty() {
            default_rec(e)
        } else {
            r
        };
        out.insert(e.id, safe_start(e, r));
    }
    out
}

/// A setting that acts lands on its safe start: the stale cleanup previews
/// first (its `dry_run` stays on). Auto-merge is already limited to repos
/// whose branch protection requires checks, and security fix pull requests
/// follow the security policy's schedule and daily limits.
pub fn safe_start(e: &Entry, r: Rec) -> Rec {
    match e.id {
        "stale_prs.dry_run" => rec(
            json!(true),
            Source::Default,
            vec![Evidence::text(
                "Stale cleanup starts as a preview that shows what it would mark and close",
            )],
        ),
        _ => r,
    }
}

/// What Setup applied to one repo, and when (`SETUP_APPLIED#<repo>`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Applied {
    /// The value applied, per setting id.
    #[serde(default)]
    pub values: BTreeMap<String, Value>,
    /// Settings Setup filled with their default as a side effect of
    /// creating their row; they don't count as set while still default.
    #[serde(default)]
    pub filled: BTreeSet<String>,
    /// Values a person picked or edited in Review (not the recommendation
    /// Setup chose on its own): they count as that person's choice.
    #[serde(default)]
    pub chosen: BTreeSet<String>,
    #[serde(default)]
    pub run_id: String,
    #[serde(default)]
    pub at: String,
}

/// Whether someone set the value (not Setup filling a row with defaults).
pub fn is_set(e: &Entry, cur: &Current, applied: Option<&Applied>) -> bool {
    cur.stored
        && !(applied.is_some_and(|a| a.filled.contains(e.id))
            && cur.value == registry::default_value(e))
}

/// A person chose this value for this repo: it is set ([`is_set`]), and it
/// isn't the value an earlier Setup run wrote on its own.
pub fn is_deliberate(e: &Entry, cur: &Current, applied: Option<&Applied>) -> bool {
    is_set(e, cur, applied)
        && !applied
            .is_some_and(|a| a.values.get(e.id) == Some(&cur.value) && !a.chosen.contains(e.id))
}

/// A deliberate value the repo's own facts argue against: kept, and listed
/// first as worth a look. Only detected evidence counts (a file, a
/// workflow, a GitHub setting), never a default or a guess.
pub fn worth_a_look(item: &PlanItem) -> bool {
    item.current.deliberate()
        && item.current.value != item.recommended
        && item.source == super::Source::Detected
        && item.evidence.iter().any(|e| !e.text.is_empty())
        && !registry::get(&item.setting_id).is_some_and(|e| e.preview_only)
}

/// The value differs from what the last Setup run applied.
pub fn is_custom(e: &Entry, cur: &Current, applied: Option<&Applied>) -> bool {
    applied
        .and_then(|a| a.values.get(e.id))
        .is_some_and(|v| *v != cur.value)
}

/// The plan for one repo over `scope` (setting ids), in registry order.
pub fn items(
    repo: &str,
    scope: &[String],
    current: &HashMap<&'static str, Current>,
    recs: &HashMap<&'static str, Rec>,
    applied: Option<&Applied>,
    seen: Option<&BTreeSet<String>>,
) -> Vec<PlanItem> {
    let mut out = Vec::new();
    for e in REGISTRY.iter().filter(|e| scope.iter().any(|s| s == e.id)) {
        let Some(cur) = current.get(e.id) else {
            continue;
        };
        let r = recs.get(e.id).cloned().unwrap_or_else(|| default_rec(e));
        let set = is_set(e, cur, applied);
        let diff = match (e.kind, cur.value.as_str(), r.value.as_str()) {
            (
                Kind::Text {
                    multiline: true, ..
                },
                Some(a),
                Some(b),
            ) if a != b => Some(crate::e2e::ci_setup::unified_diff(
                &format!("{repo} {}", e.id),
                a,
                b,
            )),
            _ => None,
        };
        let mut item = PlanItem {
            setting_id: e.id.to_string(),
            feature: e.feature,
            title: e.title.to_string(),
            help: e.help.to_string(),
            scope: e.scope,
            risk: e.risk,
            kind: serde_json::to_value(e.kind).unwrap_or(Value::Null),
            since: e.since.to_string(),
            capability: e.capability.to_string(),
            current: CurrentView {
                value: cur.value.clone(),
                set,
                deliberate: Some(is_deliberate(e, cur, applied)),
                set_by: None,
            },
            recommended: r.value,
            source: r.source,
            evidence: r.evidence,
            custom: is_custom(e, cur, applied),
            is_new: seen.is_some_and(|s| !s.contains(e.id)),
            decision: Decision::Keep,
            value: None,
            diff,
            needs: None,
            worth_a_look: false,
        };
        item.decision = decide(&item, Choice::KeepMine, None).0;
        out.push(item);
    }
    out
}

/// The decision and the value to write (`None`: nothing changes), from the
/// run's choice and an explicit pick. `item.current` must be fresh when
/// this decides a write.
pub fn decide(
    item: &PlanItem,
    choice: Choice,
    explicit: Option<&ItemDecision>,
) -> (Decision, Option<Value>) {
    if let Some(d) = explicit {
        return match d.decision {
            Decision::Use => (Decision::Use, Some(item.recommended.clone())),
            Decision::Edit => (Decision::Edit, d.value.clone()),
            Decision::Keep => (Decision::Keep, None),
        };
    }
    // Anything that merges or closes starts as a preview: only a person's
    // own pick turns it on.
    if registry::get(&item.setting_id).is_some_and(|e| e.preview_only) {
        return (Decision::Keep, None);
    }
    let use_it = match choice {
        Choice::UseRecommended => true,
        Choice::Smart => !item.current.deliberate(),
        Choice::KeepMine | Choice::Decide => !item.current.set,
    };
    if use_it {
        (Decision::Use, Some(item.recommended.clone()))
    } else {
        (Decision::Keep, None)
    }
}

/// An overwrite `use_recommended` would make: a value someone set,
/// replaced by a different recommendation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Overwrite {
    pub repo: String,
    pub setting_id: String,
    pub title: String,
    pub current: Value,
    pub recommended: Value,
}

pub fn overwrites(
    repo: &str,
    items: &[PlanItem],
    choice: Choice,
    explicit: &[ItemDecision],
) -> Vec<Overwrite> {
    items
        .iter()
        .filter(|i| i.current.set)
        .filter_map(|i| {
            let ex = explicit
                .iter()
                .find(|d| d.repo == repo && d.setting_id == i.setting_id);
            let (_, target) = decide(i, choice, ex);
            let target = target?;
            (target != i.current.value).then(|| Overwrite {
                repo: repo.to_string(),
                setting_id: i.setting_id.clone(),
                title: i.title.clone(),
                current: i.current.value.clone(),
                recommended: target,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::facts::tests::{docs_only, rust_service, web_app};

    fn recs(f: &RepoFacts) -> HashMap<&'static str, Rec> {
        recommend(f, &facts::detect(f), &HashMap::new(), Utc::now())
    }

    #[test]
    fn every_recommendation_carries_evidence_and_a_valid_value() {
        for f in [web_app(), rust_service(), docs_only()] {
            let r = recs(&f);
            assert_eq!(r.len(), REGISTRY.len());
            for e in REGISTRY {
                let x = &r[e.id];
                assert!(!x.evidence.is_empty(), "{} {}", f.repo, e.id);
                assert!(registry::check(e, &x.value).is_ok(), "{} {}", f.repo, e.id);
            }
        }
    }

    #[test]
    fn progress_first() {
        let r = recs(&docs_only());
        assert_eq!(r["reviewer.enabled"].value, true);
        assert_eq!(r["reviewer.enabled"].source, Source::Default);
        assert_eq!(r["security.open_prs"].value, true);
        assert_eq!(r["code_graph.enabled"].value, true);
    }

    #[test]
    fn acts_settings_land_on_their_safe_start_with_their_effect() {
        let mut f = web_app();
        let now = Utc::now();
        f.open = vec![(
            // Quiet 10 days: marked on day 7, closes on day 14.
            json!({"number": 9, "created_at": (now - chrono::Duration::days(10)).to_rfc3339(),
                   "user": {"login": "dev"}, "labels": []}),
            vec![],
        )];
        let r = recommend(&f, &facts::detect(&f), &HashMap::new(), now);
        assert_eq!(r["stale_prs.enabled"].value, true);
        assert!(r["stale_prs.enabled"].evidence[0]
            .text
            .contains("would have marked 1 and closed 0"));
        // The default timing, read from the values it recommends: 7 days
        // quiet, then 7 more after the warning.
        assert!(r["stale_prs.enabled"].evidence[0]
            .text
            .contains("(7 days with no activity, then closed 7 days later)"));
        assert_eq!(r["stale_prs.stale_days"].value, 7);
        assert_eq!(r["stale_prs.close_days"].value, 7);
        // The cleanup only previews.
        assert_eq!(r["stale_prs.dry_run"].value, true);
        assert!(r["security.open_prs"]
            .evidence
            .iter()
            .any(|e| e.text.contains("schedule")));
        // Settings that act and have no evidence stay off.
        for id in [
            "tags.auto_tag",
            "release_notes.enabled",
            "reminders.enabled",
            "tests.fix_prs",
        ] {
            assert_eq!(r[id].value, false, "{id}");
        }
        // Auto-merge: only with required checks, and only on the AI's
        // cited judgement.
        assert_eq!(r["auto_merge.enabled"].value, false);
        let inferred = HashMap::from([(
            "auto_merge.enabled",
            Inferred {
                value: json!(true),
                evidence: vec![Evidence::text("Most pull requests merge as approved")],
            },
        )]);
        let r = recommend(&f, &facts::detect(&f), &inferred, now);
        assert_eq!(r["auto_merge.enabled"].value, true);
        assert!(r["auto_merge.enabled"].evidence[0]
            .text
            .contains("build, e2e"));
        let s = rust_service();
        let r = recommend(&s, &facts::detect(&s), &inferred, now);
        assert_eq!(r["auto_merge.enabled"].value, false);
    }

    fn item(set: bool, current: Value, recommended: Value) -> PlanItem {
        let e = registry::get("reviewer.label").unwrap();
        let cur = HashMap::from([(
            e.id,
            Current {
                value: current,
                stored: set,
                row: set,
            },
        )]);
        let recs = HashMap::from([(
            e.id,
            rec(recommended, Source::Detected, vec![Evidence::text("x")]),
        )]);
        items("o/r", &[e.id.to_string()], &cur, &recs, None, None)
            .pop()
            .unwrap()
    }

    #[test]
    fn decisions_follow_the_choice() {
        let unset = item(false, json!("ch-review"), json!("review"));
        let set = item(true, json!("mine"), json!("review"));
        assert_eq!(
            decide(&unset, Choice::KeepMine, None).1,
            Some(json!("review"))
        );
        assert_eq!(decide(&set, Choice::KeepMine, None), (Decision::Keep, None));
        assert_eq!(decide(&set, Choice::Decide, None), (Decision::Keep, None));
        assert_eq!(
            decide(&set, Choice::UseRecommended, None).1,
            Some(json!("review"))
        );
        let edit = ItemDecision {
            repo: "o/r".into(),
            setting_id: "reviewer.label".into(),
            decision: Decision::Edit,
            value: Some(json!("mine-2")),
        };
        assert_eq!(
            decide(&set, Choice::KeepMine, Some(&edit)).1,
            Some(json!("mine-2"))
        );
    }

    #[test]
    fn use_recommended_lists_every_overwrite() {
        let items = vec![
            item(true, json!("mine"), json!("review")),
            item(false, json!("ch-review"), json!("review")),
            item(true, json!("review"), json!("review")),
        ];
        let o = overwrites("o/r", &items, Choice::UseRecommended, &[]);
        assert_eq!(o.len(), 1);
        assert_eq!(
            (o[0].current.clone(), o[0].recommended.clone()),
            (json!("mine"), json!("review"))
        );
        assert!(overwrites("o/r", &items, Choice::KeepMine, &[]).is_empty());
    }

    #[test]
    fn custom_means_changed_since_setup_applied_it() {
        let e = registry::get("reviewer.label").unwrap();
        let applied = Applied {
            values: BTreeMap::from([(e.id.to_string(), json!("ch-review"))]),
            ..Default::default()
        };
        let same = Current {
            value: json!("ch-review"),
            stored: true,
            row: true,
        };
        let changed = Current {
            value: json!("by-hand"),
            stored: true,
            row: true,
        };
        assert!(!is_custom(e, &same, Some(&applied)));
        assert!(is_custom(e, &changed, Some(&applied)));
        assert!(!is_custom(e, &changed, None));
    }

    #[test]
    fn a_default_setup_filled_is_not_set_until_someone_changes_it() {
        let e = registry::get("reviewer.label").unwrap();
        let applied = Applied {
            filled: BTreeSet::from([e.id.to_string()]),
            ..Default::default()
        };
        let filled = Current {
            value: registry::default_value(e),
            stored: true,
            row: true,
        };
        assert!(!is_set(e, &filled, Some(&applied)));
        let changed = Current {
            value: json!("x"),
            stored: true,
            row: true,
        };
        assert!(is_set(e, &changed, Some(&applied)));
    }
}
