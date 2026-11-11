//! One repo's features as the Setup page lists them: each feature on, a
//! preview, off or not used here, its main settings in words, whether they
//! match the recommendation, and why, so "5 of 12" explains itself.
//!
//! Everything here is read, never stored: the current values come from
//! [`registry::read_all`] (the same reads as each settings page) and the
//! reasons from the repo's newest Setup plan (`SETUP_RUN_REPO#`), whose
//! items keep every recommendation with its evidence. A repo Setup never
//! read falls back to the registry's own recommendation policy.

use std::collections::HashMap;

use serde::Serialize;
use serde_json::Value;

use super::plan::{self, Applied, Choice, Decision, ItemDecision, PlanItem};
use super::registry::{self, Current, Entry, Feature, Kind, Risk, REGISTRY};
use super::{Evidence, Source};

/// The repo's newest Setup plan, and what its run decided.
#[derive(Debug, Clone, Copy)]
pub struct LastPlan<'a> {
    pub run_id: &'a str,
    pub at: &'a str,
    pub choice: Choice,
    pub decisions: &'a [ItemDecision],
    pub items: &'a [PlanItem],
    /// The run applied to this repo (and wasn't undone).
    pub applied: bool,
    /// Something the analysis skipped (the AI read over budget).
    pub warning: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    On,
    /// On, but only reporting what it would do; or recommended and waiting
    /// for a person to switch it on.
    Preview,
    Off,
    /// Nothing in the repo for it to work on (no test job, no lockfile, no
    /// release tags): it doesn't count against the total.
    NotUsed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mark {
    /// Every setting matches what Setup recommends.
    Recommended,
    /// Someone chose differently from the recommendation.
    Yours,
    Preview,
}

/// The run and setting Switch on takes (`POST /api/setup/runs/:id/switch-on`).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SwitchOn {
    pub run_id: String,
    pub setting_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SettingView {
    pub setting_id: String,
    pub title: String,
    pub value_text: String,
    /// The recommendation in words, when it differs from the value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommended_text: Option<String>,
    /// `None` when there is no recommendation to compare with.
    pub matches: Option<bool>,
    /// Changed by hand after Setup applied it.
    pub custom: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FeatureView {
    pub feature: Feature,
    pub title: String,
    pub state: State,
    /// Part of the "N on" count (features with a main switch).
    pub counted: bool,
    /// The main setting(s) in plain words.
    pub value_text: String,
    /// The main switch's recommended value, when there is one.
    pub recommended: Option<Value>,
    pub matches: Option<bool>,
    pub mark: Option<Mark>,
    /// Why it is in this state, one sentence.
    pub why: String,
    /// A few words for the collapsed row ("no required checks").
    pub short: String,
    pub evidence: Vec<Evidence>,
    pub settings_href: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub switch_on: Option<SwitchOn>,
    /// The permission the viewer lacks to change it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub needs: Option<String>,
    pub settings: Vec<SettingView>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Counts {
    pub on: usize,
    pub previews: usize,
    pub off: usize,
    pub not_used: usize,
    /// The features that fit this repo: on + previews + off.
    pub total: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RepoView {
    pub repo: String,
    pub counts: Counts,
    /// One line for the collapsed row.
    pub summary: String,
    pub run_id: Option<String>,
    pub run_at: Option<String>,
    pub features: Vec<FeatureView>,
}

/// The dashboard page that holds a feature's settings.
pub fn settings_href(f: Feature, repo: &str) -> String {
    let reviewer = |section: &str| {
        format!(
            "/reviewer/config?section={section}&repo={}",
            urlencoding::encode(repo)
        )
    };
    match f {
        Feature::Reviewer => reviewer("reviews"),
        Feature::Tests => reviewer("tests"),
        Feature::CiLabels => reviewer("labels"),
        Feature::AutoMerge => reviewer("merge"),
        Feature::Tags => reviewer("after-merge"),
        Feature::ReleaseNotes => reviewer("release-notes"),
        Feature::Reminders => reviewer("reminders"),
        Feature::StalePrs => "/reviewer/stale?view=settings".into(),
        Feature::Security => "/security/policy".into(),
        Feature::Instructions | Feature::AgentFile => "/settings/instructions".into(),
        Feature::GitConventions => "/settings/workflow".into(),
        Feature::Alerts => "/settings/alerts".into(),
        Feature::CodeGraph => "/graph".into(),
    }
}

/// The features in the order the page lists them.
const ORDER: &[Feature] = &[
    Feature::Reviewer,
    Feature::Tests,
    Feature::CiLabels,
    Feature::AutoMerge,
    Feature::Tags,
    Feature::ReleaseNotes,
    Feature::Reminders,
    Feature::StalePrs,
    Feature::Security,
    Feature::Instructions,
    Feature::GitConventions,
    Feature::Alerts,
    Feature::CodeGraph,
    Feature::AgentFile,
];

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::String(s) => !s.trim().is_empty(),
        Value::Number(n) => n.as_u64() != Some(0),
        _ => false,
    }
}

fn int(v: &Value) -> i64 {
    v.as_i64().unwrap_or(0)
}

fn s(v: &Value) -> &str {
    v.as_str().unwrap_or("")
}

fn thousands(n: i64) -> String {
    let d = n.abs().to_string();
    let mut out = String::new();
    for (i, c) in d.chars().enumerate() {
        if i > 0 && (d.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 {
        format!("-{out}")
    } else {
        out
    }
}

fn plural(n: i64, one: &str, many: &str) -> String {
    if n == 1 {
        format!("1 {one}")
    } else {
        format!("{} {many}", thousands(n))
    }
}

fn cap(t: &str) -> String {
    let mut c = t.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

/// One setting's value in words.
pub fn words(e: &Entry, v: &Value) -> String {
    match (e.id, e.kind) {
        ("tags.tag_batch_minutes", _) => match int(v) {
            0 => "Every merge".into(),
            n => format!(
                "Merges within {} share one tag",
                plural(n, "minute", "minutes")
            ),
        },
        ("reminders.cooldown_hours", _) => {
            format!("At most every {}", plural(int(v), "hour", "hours"))
        }
        ("stale_prs.stale_days" | "stale_prs.close_days", _) => plural(int(v), "day", "days"),
        ("alerts.daily_budget", _) => match int(v) {
            0 => "0 tokens a day (alert runs stopped)".into(),
            n => format!("{} tokens a day", thousands(n)),
        },
        (id, Kind::Text { multiline, .. }) => {
            let t = s(v).trim();
            if t.is_empty() {
                if id.starts_with("git_conventions.") {
                    "CoderHelm's default".into()
                } else {
                    "None".into()
                }
            } else if multiline {
                plural(t.lines().count() as i64, "line", "lines")
            } else {
                t.to_string()
            }
        }
        (_, Kind::Bool) => if truthy(v) { "On" } else { "Off" }.into(),
        (_, Kind::Int { .. }) => thousands(int(v)),
        ("tags.tag_mode", _) => match s(v) {
            "semver" => "Version numbers".into(),
            "date" => "Dates".into(),
            o => cap(o),
        },
        ("auto_merge.merge_method", _) => match s(v) {
            "merge" => "Merge commit".into(),
            o => cap(o),
        },
        (_, Kind::Choice { .. }) => cap(s(v)),
    }
}

/// The feature's main settings in a few words, from the values now.
fn feature_words(f: Feature, cur: &dyn Fn(&str) -> Value, on: bool) -> String {
    let b = |id: &str| truthy(&cur(id));
    match f {
        Feature::Reviewer if on => format!(
            "Reviews pull requests labelled {}",
            s(&cur("reviewer.label"))
        ),
        Feature::Tests => {
            let mut p = Vec::new();
            if b("tests.verify_tests") {
                p.push("checks tests in reviews");
            }
            if b("tests.require_tests") {
                p.push("requires tests");
            }
            if b("tests.e2e") {
                p.push("reads test results from CI");
            }
            if p.is_empty() {
                "Off".into()
            } else {
                cap(&p.join(", "))
            }
        }
        Feature::CiLabels if on => match s(&cur("ci_labels.allow")).trim() {
            "" => "Adds no labels yet: the allowed list is empty".into(),
            l => format!("Adds {l}"),
        },
        Feature::AutoMerge if on => {
            let m = words(
                registry::get("auto_merge.merge_method").unwrap(),
                &cur("auto_merge.merge_method"),
            );
            if b("auto_merge.require_human_approval") {
                format!("{m}, after a person approves")
            } else {
                format!("{m}, once the reviewer approves")
            }
        }
        Feature::Tags if on => {
            let mode = words(
                registry::get("tags.tag_mode").unwrap(),
                &cur("tags.tag_mode"),
            );
            let prefix = s(&cur("tags.tag_prefix")).to_string();
            let batch = words(
                registry::get("tags.tag_batch_minutes").unwrap(),
                &cur("tags.tag_batch_minutes"),
            );
            let mut t = if prefix.is_empty() {
                mode
            } else {
                format!("{mode}, prefix {prefix}")
            };
            t.push_str(&format!(", {}", batch.to_lowercase()));
            if b("tags.health_check") {
                t.push_str(", health checked");
            }
            t
        }
        Feature::ReleaseNotes if on => "Notes for every release tag".into(),
        Feature::Reminders if on => words(
            registry::get("reminders.cooldown_hours").unwrap(),
            &cur("reminders.cooldown_hours"),
        ),
        Feature::StalePrs if on => {
            let t = format!(
                "Stale after {}, closed {} later",
                plural(int(&cur("stale_prs.stale_days")), "day", "days"),
                plural(int(&cur("stale_prs.close_days")), "day", "days")
            );
            if b("stale_prs.dry_run") {
                format!("{t}, preview only")
            } else {
                t
            }
        }
        Feature::Security if on => {
            if b("security.open_prs") {
                "Watches dependencies and opens fix pull requests".into()
            } else {
                "Watches dependencies; shows fixes without opening pull requests".into()
            }
        }
        Feature::Instructions if on => format!(
            "{} of instructions",
            plural(
                s(&cur("instructions.repo")).trim().lines().count() as i64,
                "line",
                "lines"
            )
        ),
        Feature::GitConventions => {
            let custom: Vec<&str> = [
                ("git_conventions.branch", "branch"),
                ("git_conventions.commit", "commit"),
                ("git_conventions.pr_title", "pull request title"),
            ]
            .iter()
            .filter(|(id, _)| b(id))
            .map(|(_, w)| *w)
            .collect();
            if custom.is_empty() {
                "CoderHelm's default names for branches, commits and pull requests".into()
            } else {
                format!("Your templates for {}", custom.join(", "))
            }
        }
        Feature::Alerts => words(
            registry::get("alerts.daily_budget").unwrap(),
            &cur("alerts.daily_budget"),
        ),
        Feature::CodeGraph if on => "Kept up to date for the agents".into(),
        Feature::AgentFile => "On, refreshes by itself".into(),
        _ => "Off".into(),
    }
}

/// Why a setting that acts on its own is off by default, in words.
fn off_by_default(f: Feature, e: &Entry) -> (String, String) {
    match f {
        Feature::Reminders => (
            "Off by default: reminders post in your team's chat channel, so Setup leaves them to you.".into(),
            "off by default".into(),
        ),
        Feature::ReleaseNotes => (
            "Off by default: notes are posted for each release tag, so Setup leaves them to you.".into(),
            "off by default".into(),
        ),
        Feature::Tags => (
            "Off by default: CoderHelm pushes release tags only when you ask it to.".into(),
            "off by default".into(),
        ),
        _ if e.risk == Risk::Acts => (
            "Off by default: it acts on its own, so Setup leaves it to you.".into(),
            "off by default".into(),
        ),
        _ => ("Off: CoderHelm's default for this setting.".into(), "off by default".into()),
    }
}

fn first_text(ev: &[Evidence]) -> Option<&str> {
    ev.first()
        .map(|e| e.text.as_str())
        .filter(|t| !t.is_empty())
}

fn sentence(t: &str) -> String {
    let t = t.trim();
    if t.ends_with('.') {
        t.to_string()
    } else {
        format!("{t}.")
    }
}

/// One repo's features, with their states and reasons.
pub fn repo_view(
    repo: &str,
    current: &HashMap<&'static str, Current>,
    applied: Option<&Applied>,
    last: Option<LastPlan<'_>>,
    missing: &dyn Fn(&str) -> Option<String>,
) -> RepoView {
    let item = |id: &str| last.and_then(|l| l.items.iter().find(|i| i.setting_id == id));
    let cur = |id: &str| {
        current
            .get(id)
            .map(|c| c.value.clone())
            .unwrap_or(Value::Null)
    };
    let set = |e: &Entry| {
        current
            .get(e.id)
            .is_some_and(|c| plan::is_set(e, c, applied))
    };
    let custom = |e: &Entry| {
        current
            .get(e.id)
            .is_some_and(|c| plan::is_custom(e, c, applied))
    };
    let decision = |i: &PlanItem| -> Decision {
        let l = last.unwrap();
        let ex = l
            .decisions
            .iter()
            .find(|d| d.repo == repo && d.setting_id == i.setting_id);
        plan::decide(i, l.choice, ex).0
    };
    let mut features = Vec::new();
    for &f in ORDER {
        let entries: Vec<&Entry> = REGISTRY.iter().filter(|e| e.feature == f).collect();
        let primary = entries.iter().copied().find(|e| e.primary);
        let settings: Vec<SettingView> = entries
            .iter()
            .map(|e| {
                let v = cur(e.id);
                let rec = item(e.id).map(|i| &i.recommended);
                let matches = rec.map(|r| *r == v);
                SettingView {
                    setting_id: e.id.into(),
                    title: e.title.into(),
                    value_text: words(e, &v),
                    recommended_text: rec.filter(|r| **r != v).map(|r| words(e, r)),
                    matches,
                    custom: custom(e),
                }
            })
            .collect();
        let matches = if settings.iter().any(|x| x.matches.is_some()) {
            Some(settings.iter().all(|x| x.matches != Some(false)))
        } else {
            None
        };
        let href = settings_href(f, repo);
        let base = |state, why: String, short: &str| FeatureView {
            feature: f,
            title: f.title().into(),
            state,
            counted: primary.is_some(),
            value_text: String::new(),
            recommended: primary
                .and_then(|p| item(p.id))
                .map(|i| i.recommended.clone()),
            matches,
            mark: None,
            why,
            short: short.into(),
            evidence: vec![],
            settings_href: href.clone(),
            switch_on: None,
            needs: None,
            settings: settings.clone(),
        };

        // The agent file has no switch (it refreshes by itself); git
        // conventions are team templates, always in use.
        let Some(p) = primary else {
            let mut v = if f == Feature::AgentFile {
                base(
                    State::On,
                    "Always on: CoderHelm refreshes the repo's agent file by itself after merges."
                        .into(),
                    "",
                )
            } else {
                let mine = entries.iter().any(|e| truthy(&cur(e.id)));
                let mut v = base(
                    State::On,
                    if mine {
                        "Your team's templates name CoderHelm's branches, commits and pull requests.".into()
                    } else {
                        "CoderHelm names its branches, commits and pull requests its own way until you set templates.".into()
                    },
                    "",
                );
                v.mark = match matches {
                    Some(true) => Some(Mark::Recommended),
                    Some(false) if mine => Some(Mark::Yours),
                    _ => None,
                };
                v
            };
            v.counted = false;
            v.value_text = feature_words(f, &cur, true);
            features.push(v);
            continue;
        };

        let on = truthy(&cur(p.id));
        let pi = item(p.id);
        let rec_on = pi.map(|i| truthy(&i.recommended));
        let needs = missing(p.capability);
        let mut v;

        // A preview: on but only reporting, or recommended and waiting.
        let dry = match f {
            Feature::StalePrs => on && truthy(&cur("stale_prs.dry_run")),
            Feature::Security => on && !truthy(&cur("security.open_prs")),
            _ => false,
        };
        let waiting = p.preview_only && !on && rec_on == Some(true);
        if dry || waiting {
            let ev_item = match f {
                Feature::Security => item("security.open_prs").or(pi),
                _ => pi,
            };
            let ev = ev_item.map(|i| i.evidence.clone()).unwrap_or_default();
            let why = match f {
                Feature::StalePrs => match first_text(&ev) {
                    Some(t) => format!("Preview: stale cleanup starts as a dry run. {}", sentence(t)),
                    None => "Preview: stale cleanup works out what it would mark and close, and changes nothing on GitHub.".into(),
                },
                Feature::Security => "Preview: security fixes show what they would open; fix pull requests are off.".into(),
                _ => match first_text(&ev) {
                    Some(t) => format!("Preview: recommended here, and nothing merges until you switch it on. {}", sentence(t)),
                    None => "Preview: recommended here, and nothing merges until you switch it on.".into(),
                },
            };
            v = base(State::Preview, why, "preview waiting");
            v.mark = Some(Mark::Preview);
            v.evidence = ev;
            let target = match f {
                Feature::StalePrs => "stale_prs.enabled",
                Feature::Security => "security.open_prs",
                _ => p.id,
            };
            let switch_need =
                registry::switch_on_target(target).and_then(|(e, _)| missing(e.capability));
            if let Some(l) = last.filter(|_| switch_need.is_none()) {
                v.switch_on = Some(SwitchOn {
                    run_id: l.run_id.to_string(),
                    setting_id: target.to_string(),
                });
            }
            v.needs = switch_need;
        } else if on {
            let why = match (pi, matches) {
                (Some(i), Some(true)) => match first_text(&i.evidence) {
                    Some(t) => format!("On, as recommended: {}", sentence(&lower_first(t))),
                    None => "On, as recommended.".into(),
                },
                (Some(_), Some(false)) => {
                    "On, with some settings your own; the recommended values are listed below."
                        .into()
                }
                _ => "On.".into(),
            };
            v = base(State::On, why, "");
            v.mark = match matches {
                Some(true) => Some(Mark::Recommended),
                Some(false) => Some(Mark::Yours),
                None => None,
            };
            v.evidence = pi.map(|i| i.evidence.clone()).unwrap_or_default();
        } else {
            v = off_view(
                f,
                p,
                pi,
                last,
                set(p),
                custom(p),
                needs.clone(),
                &base,
                &decision,
            );
            v.needs = needs;
        }
        v.value_text = feature_words(f, &cur, on);
        features.push(v);
    }

    let mut c = Counts::default();
    for v in features.iter().filter(|v| v.counted) {
        match v.state {
            State::On => c.on += 1,
            State::Preview => c.previews += 1,
            State::Off => c.off += 1,
            State::NotUsed => c.not_used += 1,
        }
    }
    c.total = c.on + c.previews + c.off;
    RepoView {
        repo: repo.into(),
        summary: summary_line(&c, &features),
        counts: c,
        run_id: last.map(|l| l.run_id.to_string()),
        run_at: last.map(|l| l.at.to_string()).filter(|t| !t.is_empty()),
        features,
    }
}

fn lower_first(t: &str) -> String {
    let mut c = t.chars();
    match c.next() {
        // Keep names and acronyms ("CODEOWNERS", "Rust") as they are.
        Some(f) if c.clone().next().is_some_and(|n| n.is_lowercase()) && !t.starts_with("Rust") => {
            f.to_lowercase().collect::<String>() + c.as_str()
        }
        _ => t.to_string(),
    }
}

/// Why a feature is off, or not used here.
#[allow(clippy::too_many_arguments)]
fn off_view(
    f: Feature,
    p: &Entry,
    pi: Option<&PlanItem>,
    last: Option<LastPlan<'_>>,
    is_set: bool,
    is_custom: bool,
    needs: Option<String>,
    base: &dyn Fn(State, String, &str) -> FeatureView,
    decision: &dyn Fn(&PlanItem) -> Decision,
) -> FeatureView {
    let Some(i) = pi else {
        // Setup hasn't read this repo: the registry's own policy.
        let why = if truthy(&registry::policy_value(p)) {
            "Off: CoderHelm recommends it on for every repo. Run Setup to turn it on.".to_string()
        } else if p.risk == Risk::Acts {
            off_by_default(f, p).0
        } else {
            "Off: Setup hasn't read this repo yet, so there's no recommendation for it.".to_string()
        };
        let mut v = base(State::Off, why, if is_set { "yours" } else { "not set up" });
        v.mark = is_set.then_some(Mark::Yours);
        return v;
    };
    let l = last.unwrap();
    let item = |id: &str| l.items.iter().find(|x| x.setting_id == id);
    let ev = i.evidence.clone();
    let ev_text = first_text(&ev).map(sentence);

    // Recommended on, but off.
    if truthy(&i.recommended) {
        let because = ev_text
            .clone()
            .map(|t| format!(" Recommended on: {t}"))
            .unwrap_or_default();
        let (why, short, mark) = if let Some(n) = needs {
            (
                format!("Off: turning it on needs the \"{n}\" permission.{because}"),
                "needs permission".to_string(),
                None,
            )
        } else if is_custom {
            (
                format!("Off: changed by hand after Setup turned it on.{because}"),
                "yours".into(),
                Some(Mark::Yours),
            )
        } else if l.applied && decision(i) == Decision::Keep {
            (
                format!("Off: you kept your setting.{because}"),
                "kept yours".into(),
                Some(Mark::Yours),
            )
        } else if !l.applied {
            (
                format!("Off: Setup recommends it on; the run is waiting for review.{because}"),
                "waiting for review".into(),
                None,
            )
        } else {
            (
                format!("Off: Setup recommends it on.{because}"),
                "recommended on".into(),
                None,
            )
        };
        let mut v = base(State::Off, why, &short);
        v.mark = mark;
        v.evidence = ev;
        return v;
    }

    // Recommended off. Nothing in the repo for it to work on: not used here.
    let not_used = |why: String, short: &str, ev: Vec<Evidence>| {
        let mut v = base(State::NotUsed, why, short);
        v.evidence = ev;
        v
    };
    if i.source == Source::Detected {
        let short = match f {
            Feature::AutoMerge => "no required checks",
            Feature::Security => "no lockfile",
            _ => "doesn't fit",
        };
        return not_used(
            format!(
                "Not used here: {}",
                ev_text.unwrap_or_else(|| "nothing in the repo for it to work on.".into())
            ),
            short,
            ev,
        );
    }
    let tags_found = item("tags.tag_mode").is_some_and(|t| t.source == Source::Detected);
    match f {
        Feature::Tests if i.source == Source::Default => {
            let e2e = item("tests.e2e").filter(|x| !truthy(&x.recommended));
            let mut why = "Not used here: Setup found no test runner in this repo".to_string();
            if let Some(t) = e2e.and_then(|x| first_text(&x.evidence)) {
                why.push_str(&format!(", and {}", sentence(&lower_first(t))));
            } else {
                why.push('.');
            }
            return not_used(
                why,
                "no tests found",
                e2e.map(|x| x.evidence.clone()).unwrap_or(ev),
            );
        }
        Feature::CiLabels if i.source == Source::Default => {
            return not_used(
                "Not used here: no workflow in this repo runs on a label.".into(),
                "no label workflows",
                vec![],
            );
        }
        Feature::Tags if !tags_found => {
            return not_used(
                "Not used here: this repo has no release tags yet.".into(),
                "no release tags",
                vec![],
            );
        }
        Feature::ReleaseNotes if !tags_found => {
            return not_used(
                "Not used here: notes are written for release tags, and this repo has none.".into(),
                "no release tags",
                vec![],
            );
        }
        _ => {}
    }
    let (why, short) = match (f, i.source) {
        (Feature::Alerts, _) => (
            "Off: the daily alert budget is 0, which stops alert runs.".to_string(),
            "budget 0".to_string(),
        ),
        (Feature::Instructions, _) => match l.warning {
            Some(w) => (
                format!("Off: no instructions yet. {}", sentence(w)),
                "none written".into(),
            ),
            None => (
                "Off: no instructions written for this repo yet.".into(),
                "none written".into(),
            ),
        },
        (_, Source::Inferred) => (
            format!(
                "Off: not recommended for this repo. {}",
                ev_text.clone().unwrap_or_default()
            )
            .trim()
            .to_string(),
            "not recommended".into(),
        ),
        _ => off_by_default(f, p),
    };
    let mut v = base(State::Off, why, &short);
    v.mark = Some(Mark::Recommended);
    v.evidence = ev;
    v
}

/// "5 on, 2 previews, 4 off: Auto-merge (no required checks), …".
fn summary_line(c: &Counts, features: &[FeatureView]) -> String {
    let mut head = vec![format!("{} on", c.on)];
    if c.previews > 0 {
        head.push(plural(c.previews as i64, "preview", "previews"));
    }
    if c.off > 0 {
        head.push(format!("{} off", c.off));
    }
    let mut out = head.join(", ");
    if c.not_used > 0 {
        out.push_str(&format!(" of the {} that fit this repo", c.total));
    }
    let why: Vec<String> = features
        .iter()
        .filter(|v| {
            v.counted
                && matches!(v.state, State::Off | State::Preview | State::NotUsed)
                && !v.short.is_empty()
        })
        .map(|v| format!("{} ({})", v.title, v.short))
        .collect();
    if !why.is_empty() {
        let shown: Vec<&str> = why.iter().take(3).map(String::as_str).collect();
        out.push_str(": ");
        out.push_str(&shown.join(", "));
        if why.len() > 3 {
            out.push_str(&format!(" and {} more", why.len() - 3));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::registry::Current;
    use crate::setup::Evidence;
    use serde_json::json;

    fn current(over: &[(&str, Value)]) -> HashMap<&'static str, Current> {
        REGISTRY
            .iter()
            .map(|e| {
                let v = over
                    .iter()
                    .find(|(id, _)| *id == e.id)
                    .map(|(_, v)| v.clone());
                (
                    e.id,
                    Current {
                        stored: v.is_some(),
                        row: v.is_some(),
                        value: v.unwrap_or_else(|| registry::default_value(e)),
                    },
                )
            })
            .collect()
    }

    fn item(
        id: &str,
        cur: &HashMap<&'static str, Current>,
        rec: Value,
        source: Source,
        ev: &str,
    ) -> PlanItem {
        let e = registry::get(id).unwrap();
        let c = &cur[e.id];
        PlanItem {
            setting_id: id.into(),
            feature: e.feature,
            title: e.title.into(),
            help: e.help.into(),
            scope: e.scope,
            risk: e.risk,
            kind: Value::Null,
            since: e.since.into(),
            capability: e.capability.into(),
            current: plan::CurrentView {
                value: c.value.clone(),
                set: c.stored,
                deliberate: None,
                set_by: None,
            },
            recommended: rec,
            source,
            evidence: vec![Evidence::text(ev)],
            custom: false,
            is_new: false,
            decision: Decision::Keep,
            value: None,
            diff: None,
            needs: None,
            worth_a_look: false,
        }
    }

    fn none(_: &str) -> Option<String> {
        None
    }

    fn feature(v: &RepoView, f: Feature) -> &FeatureView {
        v.features.iter().find(|x| x.feature == f).unwrap()
    }

    #[test]
    fn every_registry_feature_is_listed_once_with_its_settings_and_page() {
        let cur = current(&[]);
        let v = repo_view("acme/web", &cur, None, None, &none);
        for e in REGISTRY {
            let f = feature(&v, e.feature);
            assert!(f.settings.iter().any(|s| s.setting_id == e.id), "{}", e.id);
            assert!(f.settings_href.starts_with('/'), "{}", e.id);
        }
        assert_eq!(v.features.len(), ORDER.len());
        // The counted features are the ones with a main switch.
        assert_eq!(
            v.features.iter().filter(|f| f.counted).count(),
            registry::features().len()
        );
        assert_eq!(
            v.counts.total + v.counts.not_used,
            registry::features().len()
        );
        let agent = feature(&v, Feature::AgentFile);
        assert_eq!((agent.state, agent.counted), (State::On, false));
        assert_eq!(agent.value_text, "On, refreshes by itself");
        assert_eq!(
            feature(&v, Feature::Reviewer).settings_href,
            "/reviewer/config?section=reviews&repo=acme%2Fweb"
        );
    }

    #[test]
    fn states_and_reasons_for_on_off_preview_custom_and_not_used() {
        let cur = current(&[
            ("reviewer.enabled", json!(true)),
            ("reviewer.label", json!("ch-review")),
            ("stale_prs.enabled", json!(true)),
            ("stale_prs.dry_run", json!(true)),
            ("code_graph.enabled", json!(false)),
            ("tags.auto_tag", json!(false)),
        ]);
        let items = vec![
            item("reviewer.enabled", &cur, json!(true), Source::Detected, "Label ch-review exists, used on 9 of the last 20 merged pull requests"),
            item("reviewer.label", &cur, json!("ch-review"), Source::Detected, "Label ch-review exists"),
            item("stale_prs.enabled", &cur, json!(true), Source::Detected, "Over the last 14 days this would have marked 4 and closed 3 of the 12 open pull requests"),
            item("stale_prs.dry_run", &cur, json!(true), Source::Default, "Starts as a preview"),
            item("auto_merge.enabled", &cur, json!(false), Source::Detected, "Branch protection on main requires no checks, so nothing would hold a merge back"),
            item("code_graph.enabled", &cur, json!(true), Source::Default, "CoderHelm recommends this for every repo"),
            item("tags.auto_tag", &cur, json!(false), Source::Default, "CoderHelm's default"),
            item("tags.tag_mode", &cur, json!("semver"), Source::Detected, "Tags like v1.2.3"),
            item("security.enabled", &cur, json!(true), Source::Detected, "npm lockfile package-lock.json"),
        ];
        let applied = Applied {
            run_id: "R1".into(),
            ..Default::default()
        };
        let last = LastPlan {
            run_id: "R1",
            at: "2026-11-10T10:00:00Z",
            choice: Choice::KeepMine,
            decisions: &[],
            items: &items,
            applied: true,
            warning: None,
        };
        let v = repo_view("acme/web", &cur, Some(&applied), Some(last), &none);

        let r = feature(&v, Feature::Reviewer);
        assert_eq!((r.state, r.mark), (State::On, Some(Mark::Recommended)));
        assert_eq!(r.value_text, "Reviews pull requests labelled ch-review");
        assert!(r.why.contains("label ch-review exists"), "{}", r.why);

        let s = feature(&v, Feature::StalePrs);
        assert_eq!((s.state, s.mark), (State::Preview, Some(Mark::Preview)));
        assert!(
            s.why.contains("would have marked 4 and closed 3"),
            "{}",
            s.why
        );
        assert_eq!(
            s.switch_on,
            Some(SwitchOn {
                run_id: "R1".into(),
                setting_id: "stale_prs.enabled".into()
            })
        );

        let a = feature(&v, Feature::AutoMerge);
        assert_eq!(a.state, State::NotUsed);
        assert!(
            a.why
                .starts_with("Not used here: Branch protection on main requires no checks"),
            "{}",
            a.why
        );

        // Recommended on, kept off by someone: theirs, with the reason.
        let g = feature(&v, Feature::CodeGraph);
        assert_eq!((g.state, g.mark), (State::Off, Some(Mark::Yours)));
        assert!(
            g.why.starts_with("Off: you kept your setting."),
            "{}",
            g.why
        );
        assert!(g
            .why
            .contains("Recommended on: CoderHelm recommends this for every repo."));

        // Off by default for a repo that has tags.
        let t = feature(&v, Feature::Tags);
        assert_eq!((t.state, t.mark), (State::Off, Some(Mark::Recommended)));
        assert!(t.why.starts_with("Off by default"), "{}", t.why);

        // Security was recommended on but the repo isn't covered: Setup recommends it.
        let sec = feature(&v, Feature::Security);
        assert_eq!(sec.state, State::Off);

        // Counts add up, without the features that don't fit.
        let c = &v.counts;
        assert_eq!(c.on + c.previews + c.off, c.total);
        assert!(c.not_used >= 1);
        assert!(
            v.summary
                .contains(&format!("of the {} that fit this repo", c.total)),
            "{}",
            v.summary
        );
        assert!(v.summary.contains("1 preview"), "{}", v.summary);
    }

    #[test]
    fn a_waiting_auto_merge_is_a_preview_and_a_custom_value_is_yours() {
        let cur = current(&[
            ("reviewer.enabled", json!(true)),
            ("reviewer.label", json!("mine")),
        ]);
        let items = vec![
            item(
                "auto_merge.enabled",
                &cur,
                json!(true),
                Source::Inferred,
                "Over the last 14 days this would have merged 6 pull requests",
            ),
            item("reviewer.enabled", &cur, json!(true), Source::Default, "x"),
            item(
                "reviewer.label",
                &cur,
                json!("ch-review"),
                Source::Default,
                "x",
            ),
        ];
        let applied = Applied {
            values: [("reviewer.label".to_string(), json!("ch-review"))]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let last = LastPlan {
            run_id: "R2",
            at: "",
            choice: Choice::KeepMine,
            decisions: &[],
            items: &items,
            applied: true,
            warning: None,
        };
        let v = repo_view("acme/api", &cur, Some(&applied), Some(last), &none);
        let a = feature(&v, Feature::AutoMerge);
        assert_eq!(a.state, State::Preview);
        assert!(a.why.contains("would have merged 6"), "{}", a.why);
        assert_eq!(
            a.switch_on.as_ref().map(|s| s.setting_id.as_str()),
            Some("auto_merge.enabled")
        );
        let r = feature(&v, Feature::Reviewer);
        assert_eq!(
            (r.state, r.mark, r.matches),
            (State::On, Some(Mark::Yours), Some(false))
        );
        let label = r
            .settings
            .iter()
            .find(|s| s.setting_id == "reviewer.label")
            .unwrap();
        assert!(label.custom);
        assert_eq!(label.recommended_text.as_deref(), Some("ch-review"));

        // A viewer who can't change it sees why, and no Switch on.
        let lacks = |_: &str| Some("Manage agent settings".to_string());
        let v = repo_view("acme/api", &cur, Some(&applied), Some(last), &lacks);
        let a = feature(&v, Feature::AutoMerge);
        assert_eq!(a.switch_on, None);
        assert_eq!(a.needs.as_deref(), Some("Manage agent settings"));
    }

    #[test]
    fn a_repo_setup_never_read_falls_back_to_the_registry_policy() {
        let cur = current(&[("code_graph.enabled", json!(false))]);
        let v = repo_view("acme/docs", &cur, None, None, &none);
        let g = feature(&v, Feature::CodeGraph);
        assert_eq!(g.state, State::Off);
        assert!(
            g.why.contains("recommends it on for every repo"),
            "{}",
            g.why
        );
        let rem = feature(&v, Feature::Reminders);
        assert!(rem.why.starts_with("Off by default"), "{}", rem.why);
        assert_eq!(v.counts.not_used, 0);
        assert!(v.run_id.is_none());
    }

    #[test]
    fn values_read_as_words() {
        let e = |id| registry::get(id).unwrap();
        assert_eq!(words(e("tags.tag_batch_minutes"), &json!(0)), "Every merge");
        assert_eq!(
            words(e("alerts.daily_budget"), &json!(1_500_000)),
            "1,500,000 tokens a day"
        );
        assert_eq!(
            words(e("reminders.cooldown_hours"), &json!(1)),
            "At most every 1 hour"
        );
        assert_eq!(
            words(e("auto_merge.merge_method"), &json!("merge")),
            "Merge commit"
        );
        assert_eq!(
            words(e("git_conventions.branch"), &json!("")),
            "CoderHelm's default"
        );
        assert_eq!(words(e("instructions.repo"), &json!("a\nb\nc")), "3 lines");
    }
}
