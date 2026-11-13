//! A dependency fix run, decided by rules and proven in the sandbox before
//! anything is pushed.
//!
//! Two bundle runs once went wrong the same way. One planned nine tasks for
//! 64 packages (eight manifests and a README table no one asked for), spent
//! five steps on the README task while the coder ran out of output tokens
//! before writing it, and stopped asking a person; the branch it left had
//! manifests without a lockfile, so seven CI checks failed. The other
//! migrated 24 files for 37 major upgrades of packages that weren't
//! reachable, never ran a check (the sandbox's frozen install refused the
//! manifests before the lockfile was written) and finished red.
//!
//! Now:
//!
//! 1. [`plan`] reads the findings and decides each bump by rule. A P0 or P1
//!    finding, or a reachable one, is always fixed. Every other finding gets
//!    the lowest fixed version on its own version line (a patch or minor
//!    bump, or an override that keeps the line). A major upgrade goes in only
//!    for an urgent finding, or when no fixed version exists on the line, the
//!    reachability is not known and the policy fixes majors in the pull
//!    request. A major for a finding that is not reachable is deferred with
//!    its reason, and the finding stays open. Bumps come out in risk batches:
//!    urgent same-line bumps, safe same-line bumps, urgent majors, other
//!    majors.
//! 2. [`DepRun`] verifies them: the repository's own checks run once on the
//!    base branch (what fails there is noted, never blamed on a bump), then
//!    on each batch with its bumps applied and the lockfile written by the
//!    repository's own package manager. A red batch is halved until the bump
//!    that breaks a check is found; that bump is dropped (or, for a major
//!    the run must fix, handed to the coder once with the failing output and
//!    its release notes) and the rest is committed green. Only verified sets
//!    are committed, one commit per green set, so progress is kept across
//!    steps.
//! 3. The state is plain data ([`DepRun`] serializes): a step that runs out
//!    of time, or a run that reaches its budget, resumes where it stopped.
//!
//! 4. A run of 64 packages later showed three more gaps. It finished with
//!    its checks passed though the application's build never ran and the
//!    application was never started, with four overrides crossing a major
//!    version. Its pull request did not say why a bump left its version line,
//!    and it did not say how many advisories stayed open or why. So: a bump
//!    that leaves its version line records which advisory has no patched
//!    release on the line ([`PlanBump::major_reason`]); once the batches are
//!    settled the repository's real build and its start command run in the
//!    sandbox in a step of their own, on the base branch and on the branch
//!    ([`RuntimeProof`], `crate::runtime_proof`), and what the bumps break
//!    there takes out the newest commit of bumps and defers them with the
//!    cause; and the report ends with the advisories that stay open, by
//!    reason ([`remaining`]).
//!
//! Nothing here asks a person. A bump that cannot ship is deferred with the
//! reason, and the run ends with a pull request of what is green.
//!
//! The rules are pure; the worker (`passes::dep_fix`) runs the probes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::runtime_proof as rp;
use crate::security_fixes::{
    compare_versions, is_major_bump, version_line, Finding, FindingState, MajorBumps,
    SecurityConfig, Status, Tier, Verdict,
};

// ── Limits ──────────────────────────────────────────────────────────────

/// Major upgrades one run attempts; the rest wait for the next run.
pub const MAX_MAJORS_PER_RUN: usize = 6;
/// Bumps one run attempts in all.
pub const MAX_BUMPS_PER_RUN: usize = 120;
/// Sandbox probes one run may start (a probe is one sandbox build).
pub const MAX_PROBES: u32 = 40;
/// Probes started at once.
pub const MAX_PARALLEL_PROBES: usize = 4;
/// Coder attempts at call sites for a major the run must fix.
pub const MAX_CODE_FIXES: u32 = 2;
/// A sandbox build that gives no result is tried this many times in all.
pub const MAX_BROKEN_PROBES: u32 = 2;
/// The default model budget of one dependency fix run, in cents.
pub const DEFAULT_RUN_BUDGET_CENTS: u32 = 300;
/// Sandbox builds one run's runtime proof may start (the base branch's, the
/// branch's, and one more for each commit of bumps the proof takes out).
pub const MAX_PROOF_RUNS: u32 = 6;

// ── The plan ────────────────────────────────────────────────────────────

/// How risky a bump is; batches are verified in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Risk {
    /// A P0/P1 or reachable finding, fixed on its own version line.
    Urgent,
    /// Every other finding fixed on its own version line.
    Safe,
    /// A major upgrade an urgent finding needs.
    UrgentMajor,
    /// A major upgrade with no fixed version on the line, reachability not
    /// known, and the policy fixing majors in the pull request.
    Major,
}

impl Risk {
    pub fn label(self) -> &'static str {
        match self {
            Risk::Urgent => "urgent fixes on their version line",
            Risk::Safe => "fixes on their version line",
            Risk::UrgentMajor => "major upgrades urgent findings need",
            Risk::Major => "major upgrades with no fix on the line",
        }
    }
    pub fn is_major(self) -> bool {
        matches!(self, Risk::UrgentMajor | Risk::Major)
    }
}

/// One package bump the run applies: one version line of one package in one
/// lockfile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PlanBump {
    pub package: String,
    /// The lowest current version on the line.
    pub from: String,
    pub to: String,
    /// The version line it applies to (`4`, `0.12`): an override selector.
    pub line: String,
    /// The lockfile (or manifest) the findings name.
    pub manifest: String,
    pub major: bool,
    pub risk: Option<Risk>,
    pub finding_ids: Vec<String>,
    /// Why it is in the run, in a few words.
    pub why: String,
    /// Why it leaves its version line, for a major: the advisory whose data
    /// lists no patched release on the line, with the releases it does list.
    /// Empty for a bump that stays on its line; never empty for a major.
    pub major_reason: String,
}

impl PlanBump {
    pub fn key(&self) -> String {
        format!(
            "{}@{}:{}",
            self.package.to_ascii_lowercase(),
            self.line,
            self.manifest
        )
    }
    pub fn risk(&self) -> Risk {
        self.risk.unwrap_or(Risk::Safe)
    }
}

/// The kind of reason a bump is left out: what the pull request groups the
/// advisories that stay open by.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum Cause {
    /// It broke the repository's checks, its build or its start, in the
    /// sandbox or in the pull request's CI.
    Broke,
    /// Another package holds it: a parent pins the version, or the package
    /// manager could not install the fixed version.
    Parent,
    /// A major upgrade the rules leave out (not reachable, not urgent).
    MajorNotNeeded,
    /// A cap, the budget or time: the next run takes it.
    Later,
    /// It could not be placed or verified (no sandbox result, the lockfile
    /// can't be told).
    #[default]
    Unverified,
}

/// A bump the run leaves out, with why. Its findings stay open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Deferral {
    pub package: String,
    pub from: String,
    /// The version that would fix it (a major, for most).
    pub to: String,
    pub manifest: String,
    pub finding_ids: Vec<String>,
    pub reason: String,
    /// It can come back by itself in a later run (a cap, the budget, time):
    /// not a decision about the bump.
    pub later: bool,
    /// The kind of reason, for the pull request's count of what stays open.
    pub cause: Cause,
}

impl Deferral {
    /// `` `@x/y` 4.11.3 to 5.5.0 deferred: <reason> ``
    pub fn line(&self) -> String {
        let at = if self.manifest.contains('/') {
            format!(" in `{}`", self.manifest)
        } else {
            String::new()
        };
        format!(
            "`{}` {} to {}{at} deferred: {}",
            self.package, self.from, self.to, self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DepPlan {
    pub bumps: Vec<PlanBump>,
    pub deferred: Vec<Deferral>,
}

impl DepPlan {
    /// The bumps of each risk batch, in verification order (indices into
    /// `bumps`). Majors are verified one per batch: one that breaks never
    /// holds another back.
    pub fn batches(&self) -> Vec<(Risk, Vec<usize>)> {
        let mut out = vec![];
        for risk in [Risk::Urgent, Risk::Safe] {
            let ids: Vec<usize> = (0..self.bumps.len())
                .filter(|&i| self.bumps[i].risk() == risk)
                .collect();
            if !ids.is_empty() {
                out.push((risk, ids));
            }
        }
        for risk in [Risk::UrgentMajor, Risk::Major] {
            for i in 0..self.bumps.len() {
                if self.bumps[i].risk() == risk {
                    out.push((risk, vec![i]));
                }
            }
        }
        out
    }

    /// The tasks a run of this plan has: one per lockfile, from the bump
    /// list. Never documentation.
    pub fn tasks_md(&self) -> String {
        let mut by: BTreeMap<&str, Vec<&PlanBump>> = BTreeMap::new();
        for b in &self.bumps {
            by.entry(b.manifest.as_str()).or_default().push(b);
        }
        let mut s = String::new();
        for (manifest, bumps) in by {
            let names: Vec<String> = bumps
                .iter()
                .take(6)
                .map(|b| format!("`{}` {} to {}", b.package, b.from, b.to))
                .collect();
            let more = bumps.len().saturating_sub(6);
            s.push_str(&format!(
                "- [ ] `{}`: apply {} bump(s) for this lockfile ({}{}) and write the lockfile with its own package manager\n",
                manifest,
                bumps.len(),
                names.join(", "),
                if more > 0 {
                    format!(", and {more} more")
                } else {
                    String::new()
                }
            ));
        }
        s
    }
}

/// A finding that must be fixed in this run, whatever it takes.
pub fn urgent(f: &Finding) -> bool {
    matches!(f.tier, Some(Tier::P0) | Some(Tier::P1)) || f.verdict == Verdict::Reachable
}

fn is_prerelease(v: &str) -> bool {
    v.contains('-') || v.chars().any(|c| c.is_ascii_alphabetic())
}

/// The fixed versions the sources name for a finding, its own first.
fn fixed_versions(f: &Finding, st: &FindingState) -> Vec<String> {
    let mut v: Vec<String> = st.fixed.clone();
    if let Some(x) = &f.fixed_version {
        v.push(x.clone());
    }
    v.retain(|x| !x.trim().is_empty() && !is_prerelease(x));
    v.sort_by(|a, b| compare_versions(a, b));
    v.dedup();
    v
}

/// The lowest fixed version above the current one on its own line.
pub fn same_line_fix(f: &Finding, st: &FindingState) -> Option<String> {
    fixed_versions(f, st).into_iter().find(|x| {
        compare_versions(x, &f.version) == std::cmp::Ordering::Greater
            && version_line(x) == version_line(&f.version)
    })
}

/// The lowest fixed version above the current one on any line.
pub fn lowest_fix(f: &Finding, st: &FindingState) -> Option<String> {
    fixed_versions(f, st)
        .into_iter()
        .find(|x| compare_versions(x, &f.version) == std::cmp::Ordering::Greater)
}

/// Why a finding's fix leaves its version line: its advisory data lists no
/// patched release on the line. Names the advisory and what it does list.
pub fn major_reason(f: &Finding, st: &FindingState) -> String {
    let listed = fixed_versions(f, st);
    let advisory = if f.advisory.primary.is_empty() {
        "its advisory".to_string()
    } else {
        f.advisory.primary.clone()
    };
    format!(
        "{advisory} lists no patched release on the {} line (patched releases it lists: {})",
        version_line(&f.version),
        if listed.is_empty() {
            "none".to_string()
        } else {
            listed.join(", ")
        }
    )
}

/// Whether the rules fix a finding by leaving its version line (a major
/// upgrade that goes in): such a fix gets a pull request of its own.
pub fn crosses_major(f: &Finding, st: &FindingState, cfg: &SecurityConfig) -> bool {
    matches!(
        choose(f, st, cfg),
        Some(Choice::Include { ref to, risk }) if risk.is_major() || is_major_bump(&f.version, to)
    )
}

/// What the plan does with one finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Choice {
    /// Fix it at this version (`risk` says which batch).
    Include { to: String, risk: Risk },
    /// Leave it out of this run, with the reason.
    Defer { to: String, reason: String },
    /// The policy opens only an issue for its major upgrade (it stands
    /// alone, outside the bundle).
    IssueOnly,
}

/// The rule for one finding (see the module notes).
pub fn choose(f: &Finding, st: &FindingState, cfg: &SecurityConfig) -> Option<Choice> {
    let must = urgent(f);
    if let Some(to) = same_line_fix(f, st) {
        let risk = if must { Risk::Urgent } else { Risk::Safe };
        return Some(Choice::Include { to, risk });
    }
    let to = lowest_fix(f, st)?;
    if !is_major_bump(&f.version, &to) {
        // A fixed version off the line the version scheme still calls
        // compatible (rare): fixed like a same-line bump.
        let risk = if must { Risk::Urgent } else { Risk::Safe };
        return Some(Choice::Include { to, risk });
    }
    if must {
        return Some(Choice::Include {
            to,
            risk: Risk::UrgentMajor,
        });
    }
    if matches!(f.verdict, Verdict::NotReachable | Verdict::NotLoaded) {
        let why = if f.verdict == Verdict::NotLoaded {
            "not loaded at runtime"
        } else {
            "not reachable"
        };
        return Some(Choice::Defer {
            reason: format!(
                "{why}, and only a major version ({} to {}) fixes it, with breaking changes; it opens when it becomes reachable or urgent, or when a person asks for it",
                version_line(&f.version),
                version_line(&to)
            ),
            to,
        });
    }
    if cfg.major_bumps == MajorBumps::IssueOnly {
        return Some(Choice::IssueOnly);
    }
    Some(Choice::Include {
        to,
        risk: Risk::Major,
    })
}

/// Why a fixable finding stays out of the bundle, when the rule defers it
/// (the dashboard shows it as the finding's wait reason).
pub fn defer_reason(f: &Finding, st: &FindingState, cfg: &SecurityConfig) -> Option<String> {
    if let Some(d) = &st.deferred {
        if d.holds(f) {
            return Some(d.reason.clone());
        }
    }
    match choose(f, st, cfg)? {
        Choice::Defer { to, reason } => Some(format!(
            "`{}` {} to {} deferred: {reason}",
            f.package, f.version, to
        )),
        _ => None,
    }
}

/// A run's own deferral of a finding: a bump that broke the checks and
/// could not be fixed in the run. It holds while the finding reads the same
/// (tier, verdict, fixed version) and for [`RUN_DEFERRAL_DAYS`]; a person
/// asking for the fix lifts it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RunDeferral {
    pub reason: String,
    pub at: String,
    pub tier: Option<Tier>,
    pub verdict: Verdict,
    pub to: String,
}

pub const RUN_DEFERRAL_DAYS: i64 = 30;

impl RunDeferral {
    pub fn new(f: &Finding, reason: &str, now: chrono::DateTime<chrono::Utc>) -> Self {
        RunDeferral {
            reason: reason.to_string(),
            at: now.to_rfc3339(),
            tier: f.tier,
            verdict: f.verdict,
            to: f.fixed_version.clone().unwrap_or_default(),
        }
    }

    /// Still holds: the finding reads as it did, and it isn't too old.
    pub fn holds(&self, f: &Finding) -> bool {
        self.holds_at(f, chrono::Utc::now())
    }

    pub fn holds_at(&self, f: &Finding, now: chrono::DateTime<chrono::Utc>) -> bool {
        let fresh = crate::security_fixes::parse_time(&self.at)
            .is_some_and(|t| now - t < chrono::Duration::days(RUN_DEFERRAL_DAYS));
        fresh
            && self.tier == f.tier
            && self.verdict == f.verdict
            && self.to == f.fixed_version.clone().unwrap_or_default()
    }
}

/// The lockfile a finding's bump is applied through: its own when it names
/// one; for a manifest, the lockfile beside it (or the root's, for a
/// workspace member); for a finding read from the SBOM alone, the root's
/// when the repository has exactly one JavaScript lockfile there. None when
/// it can't be told.
pub fn lockfile_for(manifest: &str, lockfiles: &[String]) -> Option<String> {
    if Manager::of_lockfile(manifest).is_some() {
        return Some(manifest.to_string());
    }
    let js: Vec<&String> = lockfiles
        .iter()
        .filter(|l| Manager::of_lockfile(l).is_some())
        .collect();
    let root: Vec<&&String> = js.iter().filter(|l| !l.contains('/')).collect();
    let name = manifest.rsplit('/').next().unwrap_or(manifest);
    if name == "package.json" {
        let dir = dir_of(manifest);
        if let Some(l) = js.iter().find(|l| dir_of(l) == dir) {
            return Some((*l).clone());
        }
    }
    match root.as_slice() {
        [one] => Some((**one).clone()),
        _ => None,
    }
}

/// [`plan`] for a repository with these lockfiles: each finding's bump goes
/// through the lockfile [`lockfile_for`] names, and one whose lockfile can't
/// be told is deferred with that reason.
pub fn plan_in(
    members: &[(&Finding, &FindingState)],
    cfg: &SecurityConfig,
    lockfiles: &[String],
) -> DepPlan {
    let mut placed: Vec<(Finding, FindingState)> = vec![];
    let mut lost: Vec<&Finding> = vec![];
    for (f, st) in members {
        match lockfile_for(&f.manifest, lockfiles) {
            Some(lock) => {
                let mut f = (*f).clone();
                f.manifest = lock;
                placed.push((f, (*st).clone()));
            }
            None => lost.push(f),
        }
    }
    let pairs: Vec<(&Finding, &FindingState)> = placed.iter().map(|(f, s)| (f, s)).collect();
    let mut out = plan(&pairs, cfg);
    let mut by: BTreeMap<(String, String), Deferral> = BTreeMap::new();
    for f in lost {
        let d = by
            .entry((f.package.to_ascii_lowercase(), f.version.clone()))
            .or_insert_with(|| Deferral {
                package: f.package.clone(),
                from: f.version.clone(),
                to: f.fixed_version.clone().unwrap_or_default(),
                manifest: f.manifest.clone(),
                reason: "the lockfile it is installed through can't be told from the repository, so the sandbox can't apply and verify it".into(),
                cause: Cause::Unverified,
                ..Default::default()
            });
        d.finding_ids.push(f.id.clone());
    }
    out.deferred.extend(by.into_values());
    out
}

/// The plan for a unit's findings.
pub fn plan(members: &[(&Finding, &FindingState)], cfg: &SecurityConfig) -> DepPlan {
    // One group per (package, lockfile, version line of the current version).
    #[derive(Default)]
    struct Group {
        package: String,
        manifest: String,
        line: String,
        from: String,
        to: String,
        risk: Option<Risk>,
        ids: Vec<String>,
        deferred: Vec<(String, String, String, Cause)>, // (id, to, reason, cause)
        urgent: usize,
        n: usize,
        /// The member that takes the group off its version line, in words.
        major_why: String,
    }
    let mut groups: BTreeMap<(String, String, String), Group> = BTreeMap::new();
    for (f, st) in members {
        if f.version.trim().is_empty() {
            continue;
        }
        let key = (
            f.package.to_ascii_lowercase(),
            f.manifest.clone(),
            version_line(&f.version),
        );
        let g = groups.entry(key).or_insert_with(|| Group {
            package: f.package.clone(),
            manifest: f.manifest.clone(),
            line: version_line(&f.version),
            from: f.version.clone(),
            ..Default::default()
        });
        if compare_versions(&f.version, &g.from) == std::cmp::Ordering::Less {
            g.from = f.version.clone();
        }
        g.n += 1;
        let held = st.deferred.as_ref().filter(|d| d.holds(f));
        let cause = if held.is_some() {
            Cause::Broke
        } else {
            Cause::MajorNotNeeded
        };
        let choice = match held {
            Some(d) => Some(Choice::Defer {
                to: f.fixed_version.clone().unwrap_or_default(),
                reason: d.reason.clone(),
            }),
            None => choose(f, st, cfg),
        };
        match choice {
            Some(Choice::Include { to, risk }) => {
                if urgent(f) {
                    g.urgent += 1;
                }
                // The first member with no patched release on the line is
                // the reason the whole line moves.
                if is_major_bump(&f.version, &to) && g.major_why.is_empty() {
                    g.major_why = major_reason(f, st);
                }
                if g.to.is_empty() || compare_versions(&to, &g.to) == std::cmp::Ordering::Greater {
                    g.to = to;
                }
                g.risk = Some(match g.risk {
                    Some(r) => worst(r, risk),
                    None => risk,
                });
                g.ids.push(f.id.clone());
            }
            Some(Choice::Defer { to, reason }) => {
                g.deferred.push((f.id.clone(), to, reason, cause))
            }
            // Its own issue (`stands_alone`), never in the bundle.
            Some(Choice::IssueOnly) | None => {}
        }
    }
    let mut out = DepPlan::default();
    // A yarn lockfile pins one version of a package for all its parents:
    // one line per package (the highest) goes in, the others wait.
    let mut yarn_lines: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut majors = 0usize;
    let mut groups: Vec<Group> = groups.into_values().collect();
    // Urgent first, so a cap never drops one of them.
    groups.sort_by_key(|g| (g.risk.unwrap_or(Risk::Safe), std::cmp::Reverse(g.urgent)));
    for g in groups {
        if !g.ids.is_empty() {
            let major = is_major_bump(&g.from, &g.to);
            let risk = g.risk.unwrap_or(Risk::Safe);
            let yarn = g.manifest.ends_with("yarn.lock");
            let yarn_taken = yarn
                && yarn_lines.contains_key(&(g.package.to_ascii_lowercase(), g.manifest.clone()));
            let reason = if yarn_taken {
                Some((
                    "a yarn lockfile pins one version of a package; another version line of it is fixed in this run, and this one needs its parent upgraded".to_string(),
                    false,
                    Cause::Parent,
                ))
            } else if major && majors >= MAX_MAJORS_PER_RUN && !matches!(risk, Risk::UrgentMajor) {
                Some((
                    format!("one run takes at most {MAX_MAJORS_PER_RUN} major upgrades; the next run takes it"),
                    true,
                    Cause::Later,
                ))
            } else if out.bumps.len() >= MAX_BUMPS_PER_RUN
                && !matches!(risk, Risk::Urgent | Risk::UrgentMajor)
            {
                Some((
                    format!(
                        "one run takes at most {MAX_BUMPS_PER_RUN} bumps; the next run takes it"
                    ),
                    true,
                    Cause::Later,
                ))
            } else {
                None
            };
            match reason {
                Some((reason, later, cause)) => out.deferred.push(Deferral {
                    package: g.package.clone(),
                    from: g.from.clone(),
                    to: g.to.clone(),
                    manifest: g.manifest.clone(),
                    finding_ids: g.ids.clone(),
                    reason,
                    later,
                    cause,
                }),
                None => {
                    if major {
                        majors += 1;
                    }
                    if yarn {
                        yarn_lines.insert((g.package.to_ascii_lowercase(), g.manifest.clone()), 1);
                    }
                    let why = match risk {
                        Risk::Urgent => format!("{} urgent finding(s)", g.urgent),
                        Risk::Safe => "a fix on the same version line".to_string(),
                        Risk::UrgentMajor => format!(
                            "{} urgent finding(s) and no fix on the {} line",
                            g.urgent, g.line
                        ),
                        Risk::Major => {
                            format!("no fix on the {} line and reachability not known", g.line)
                        }
                    };
                    out.bumps.push(PlanBump {
                        package: g.package.clone(),
                        from: g.from.clone(),
                        to: g.to.clone(),
                        line: g.line.clone(),
                        manifest: g.manifest.clone(),
                        major,
                        risk: Some(if major && !risk.is_major() {
                            // Off the line though the scheme called it
                            // compatible: verified on its own like a major.
                            Risk::Major
                        } else {
                            risk
                        }),
                        finding_ids: g.ids.clone(),
                        why,
                        // A bump never leaves its line without the advisory
                        // that has no patched release on it.
                        major_reason: match (major, g.major_why.is_empty()) {
                            (false, _) => String::new(),
                            (true, false) => g.major_why.clone(),
                            (true, true) => format!(
                                "no patched release on the {} line in the advisory data",
                                g.line
                            ),
                        },
                    });
                }
            }
        }
        if !g.deferred.is_empty() {
            // One line per package and line: the highest version it needs.
            let mut to = String::new();
            for (_, t, _, _) in &g.deferred {
                if to.is_empty() || compare_versions(t, &to) == std::cmp::Ordering::Greater {
                    to = t.clone();
                }
            }
            let reason = g.deferred[0].2.clone();
            out.deferred.push(Deferral {
                package: g.package.clone(),
                from: g.from.clone(),
                to,
                manifest: g.manifest.clone(),
                finding_ids: g.deferred.iter().map(|(id, _, _, _)| id.clone()).collect(),
                reason,
                later: false,
                cause: g.deferred[0].3,
            });
        }
    }
    out.bumps
        .sort_by_key(|b| (b.risk(), b.package.to_ascii_lowercase(), b.manifest.clone()));
    out.deferred
        .sort_by_key(|d| (d.package.to_ascii_lowercase(), d.manifest.clone()));
    out
}

fn worst(a: Risk, b: Risk) -> Risk {
    // A group's batch is its riskiest member's, an urgent one winning a tie
    // within majors and within same-line fixes.
    match (a.is_major() || b.is_major(), a, b) {
        (true, Risk::UrgentMajor, _) | (true, _, Risk::UrgentMajor) => Risk::UrgentMajor,
        (true, _, _) => Risk::Major,
        (false, Risk::Urgent, _) | (false, _, Risk::Urgent) => Risk::Urgent,
        _ => Risk::Safe,
    }
}

// ── Applying bumps in the sandbox ───────────────────────────────────────

/// The package manager of a lockfile, for its manifest edits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Manager {
    Pnpm,
    Npm,
    Yarn,
}

impl Manager {
    pub fn of_lockfile(path: &str) -> Option<Manager> {
        let name = path.rsplit('/').next().unwrap_or(path);
        match name {
            "pnpm-lock.yaml" => Some(Manager::Pnpm),
            "package-lock.json" | "npm-shrinkwrap.json" => Some(Manager::Npm),
            "yarn.lock" => Some(Manager::Yarn),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Manager::Pnpm => "pnpm",
            Manager::Npm => "npm",
            Manager::Yarn => "yarn",
        }
    }
}

/// Whether every bump of the plan is in a lockfile the sandbox can edit and
/// write (npm, pnpm, yarn). Other ecosystems keep the coder's flow.
pub fn applies_to(plan: &DepPlan) -> bool {
    !plan.bumps.is_empty()
        && plan
            .bumps
            .iter()
            .all(|b| Manager::of_lockfile(&b.manifest).is_some())
}

/// What the apply script is told for one lockfile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplySpec {
    pub manager: Manager,
    /// The lockfile's folder (`""` for the root).
    pub root: String,
    pub bumps: Vec<ApplyBump>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyBump {
    pub name: String,
    pub line: String,
    pub to: String,
}

fn dir_of(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(d, _)| d.to_string())
        .unwrap_or_default()
}

/// The apply specs for a set of bumps, one per lockfile.
pub fn specs(bumps: &[&PlanBump]) -> Vec<ApplySpec> {
    let mut by: BTreeMap<String, ApplySpec> = BTreeMap::new();
    for b in bumps {
        let Some(manager) = Manager::of_lockfile(&b.manifest) else {
            continue;
        };
        let spec = by.entry(b.manifest.clone()).or_insert_with(|| ApplySpec {
            manager,
            root: dir_of(&b.manifest),
            bumps: vec![],
        });
        spec.bumps.push(ApplyBump {
            name: b.package.clone(),
            line: b.line.clone(),
            to: b.to.clone(),
        });
    }
    by.into_values().collect()
}

/// The script that edits the manifests: a direct dependency's range is
/// raised where a manifest declares it on the bump's version line (its
/// prefix kept); a transitive one gets an override scoped to its line
/// (`pnpm.overrides["pkg@4"]`, npm `overrides["pkg@4"]`, yarn
/// `resolutions["pkg"]`). Formatting is kept (indent, trailing newline).
/// It prints one `CODERHELM_APPLY <json>` line saying what it did.
pub const APPLY_JS: &str = r#"
const fs = require('fs'), path = require('path');
const specs = JSON.parse(Buffer.from(process.argv[2], 'base64').toString('utf8'));
const LOCKS = ['pnpm-lock.yaml', 'package-lock.json', 'npm-shrinkwrap.json', 'yarn.lock'];
const SECTIONS = ['dependencies', 'devDependencies', 'optionalDependencies'];
function lineOf(v) {
  const m = String(v).match(/(\d+)(?:\.(\d+))?/);
  if (!m) return null;
  return m[1] === '0' ? '0.' + (m[2] || '0') : m[1];
}
function walk(dir, root, out) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    if (e.name === 'node_modules' || e.name === '.git') continue;
    const p = path.join(dir, e.name);
    if (e.isDirectory()) {
      // A folder with its own lockfile is another project.
      if (LOCKS.some((l) => fs.existsSync(path.join(p, l)))) continue;
      walk(p, root, out);
    } else if (e.name === 'package.json') out.push(p);
  }
  return out;
}
function read(p) {
  const text = fs.readFileSync(p, 'utf8');
  const m = text.match(/^([ \t]+)"/m);
  return { json: JSON.parse(text), indent: m ? m[1] : '  ', nl: text.endsWith('\n') };
}
function write(p, f) {
  fs.writeFileSync(p, JSON.stringify(f.json, null, f.indent) + (f.nl ? '\n' : ''));
}
const report = [];
for (const spec of specs) {
  const root = spec.root || '.';
  const files = walk(root, root, []);
  const loaded = new Map(files.map((p) => [p, read(p)]));
  const rootPkg = path.join(root, 'package.json');
  const changed = new Set();
  for (const b of spec.bumps) {
    let raised = [];
    for (const [p, f] of loaded) {
      for (const s of SECTIONS) {
        const deps = f.json[s];
        if (!deps || typeof deps[b.name] !== 'string') continue;
        const cur = deps[b.name];
        if (/^(workspace:|npm:|file:|link:|git|http|catalog:)/.test(cur)) continue;
        if (lineOf(cur) !== b.line) continue;
        const m = cur.match(/^([~^>=< ]*)/);
        const prefix = m ? m[1].trim() : '';
        const next = (prefix.startsWith('>') || prefix.startsWith('<') ? '^' : prefix) + b.to;
        if (cur !== next) { deps[b.name] = next; changed.add(p); }
        raised.push(path.relative('.', p));
      }
    }
    if (raised.length) { report.push({ name: b.name, line: b.line, how: 'raised', where: raised }); continue; }
    const f = loaded.get(rootPkg) || (fs.existsSync(rootPkg) ? read(rootPkg) : null);
    if (!f) { report.push({ name: b.name, line: b.line, how: 'skipped', where: ['no package.json at ' + root] }); continue; }
    loaded.set(rootPkg, f);
    let table, key;
    if (spec.manager === 'pnpm') {
      f.json.pnpm = f.json.pnpm || {};
      table = f.json.pnpm.overrides = f.json.pnpm.overrides || {};
      key = b.name + '@' + b.line;
    } else if (spec.manager === 'npm') {
      table = f.json.overrides = f.json.overrides || {};
      key = b.name + '@' + b.line;
    } else {
      table = f.json.resolutions = f.json.resolutions || {};
      key = b.name;
    }
    // An unscoped override already on the same line is raised, not doubled.
    if (key !== b.name && typeof table[b.name] === 'string' && lineOf(table[b.name]) === b.line) key = b.name;
    const value = '^' + b.to;
    if (table[key] !== value) { table[key] = value; changed.add(rootPkg); }
    report.push({ name: b.name, line: b.line, how: 'override', where: [path.relative('.', rootPkg) + ' ' + key] });
  }
  for (const p of changed) write(p, loaded.get(p));
}
console.log('CODERHELM_APPLY ' + JSON.stringify(report));
"#;

/// Base64 (standard, padded), so the script and its specs pass through the
/// shell untouched.
pub fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// The shell line that applies `specs` (nothing for none).
pub fn apply_command(specs: &[ApplySpec]) -> String {
    let json = serde_json::to_string(specs).unwrap_or_else(|_| "[]".into());
    format!(
        "echo '{}' | base64 -d > /tmp/coderhelm-apply.js && node /tmp/coderhelm-apply.js '{}'",
        b64(APPLY_JS.as_bytes()),
        b64(json.as_bytes())
    )
}

/// The marker a probe prints after the apply and the lockfile commands.
pub const PROBE_MARK: &str = "CODERHELM_DEP";

/// One probe's command: apply the bumps, write each touched lockfile with
/// its own manager's command (`locks`: lockfile, command), then the checks
/// script, always ending with one `CODERHELM_DEP apply=<n> lock=<n>` line.
/// It exits 0 so the build hands back the manifests and lockfiles it wrote
/// whatever the checks say; the verdict is read from the markers.
pub fn probe_command(specs: &[ApplySpec], locks: &[(String, String)], checks: &str) -> String {
    let mut s = String::new();
    if specs.is_empty() {
        s.push_str("ch_apply=0\n");
    } else {
        s.push_str(&format!(
            "( {} ) > /tmp/coderhelm-apply.log 2>&1; ch_apply=$?; cat /tmp/coderhelm-apply.log | tail -c 2000\n",
            apply_command(specs)
        ));
    }
    let touched: Vec<&(String, String)> = locks
        .iter()
        .filter(|(lock, _)| specs.iter().any(|sp| dir_of(lock) == sp.root))
        .collect();
    s.push_str("ch_lock=0\n");
    for (lock, cmd) in touched {
        s.push_str(&format!(
            "if [ $ch_apply -eq 0 ] && [ $ch_lock -eq 0 ]; then ( {cmd} ) > /tmp/coderhelm-lock.log 2>&1 || {{ ch_lock=$?; echo \"--- writing {lock} failed (exit $ch_lock), last output ---\"; tail -c 2500 /tmp/coderhelm-lock.log; }}; fi\n"
        ));
    }
    s.push_str(&format!(
        "if [ $ch_apply -eq 0 ] && [ $ch_lock -eq 0 ]; then ( {checks} ); fi\n"
    ));
    s.push_str(&format!(
        "echo \"{PROBE_MARK} apply=$ch_apply lock=$ch_lock\"\ntrue"
    ));
    s
}

/// `(apply, lock)` exit codes from a probe's output.
pub fn probe_marks(output: &str) -> Option<(i32, i32)> {
    let line = output
        .lines()
        .rev()
        .find(|l| l.trim_start().starts_with(PROBE_MARK))?;
    let mut apply = None;
    let mut lock = None;
    for part in line.split_whitespace() {
        if let Some(v) = part.strip_prefix("apply=") {
            apply = v.parse().ok();
        } else if let Some(v) = part.strip_prefix("lock=") {
            lock = v.parse().ok();
        }
    }
    Some((apply?, lock?))
}

/// The bumps the apply script could not place (`how: skipped`), from its
/// `CODERHELM_APPLY` line.
pub fn apply_skipped(output: &str) -> Vec<String> {
    let Some(line) = output
        .lines()
        .rev()
        .find_map(|l| l.trim_start().strip_prefix("CODERHELM_APPLY "))
    else {
        return vec![];
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
        return vec![];
    };
    v.as_array()
        .map(|a| {
            a.iter()
                .filter(|e| e["how"] == "skipped")
                .filter_map(|e| e["name"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// The files a probe may hand back to commit: manifests and lockfiles,
/// never what a build or test wrote.
pub fn commit_path(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    !path.contains("node_modules/")
        && matches!(
            name,
            "package.json"
                | "pnpm-lock.yaml"
                | "pnpm-workspace.yaml"
                | "package-lock.json"
                | "npm-shrinkwrap.json"
                | "yarn.lock"
        )
}

// ── Verifying: the run's state ──────────────────────────────────────────

/// What one probe came to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProbeResult {
    /// The sandbox ran it and reported (false: the sandbox broke or had no
    /// time; not a verdict).
    pub ran: bool,
    /// The bumps applied and every lockfile was written.
    pub applied: bool,
    /// Each check by name and whether it passed.
    pub checks: Vec<(String, bool)>,
    /// The output's tail, for a reason.
    pub tail: String,
}

impl ProbeResult {
    pub fn failing(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .checks
            .iter()
            .filter(|(_, ok)| !ok)
            .map(|(n, _)| n.clone())
            .collect();
        if self.ran && !self.applied {
            v.push(LOCKFILE_CHECK.to_string());
        }
        v
    }
}

/// The check name for "the bumps could not be applied or the lockfile
/// could not be written".
pub const LOCKFILE_CHECK: &str = "lockfile";
/// The install step's name.
pub const INSTALL_CHECK: &str = "install";

/// One set of bumps being verified together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Group {
    pub bumps: Vec<usize>,
    /// The checks it broke (that pass on the base), once probed red.
    pub red: Option<Vec<String>>,
    pub tail: String,
    /// Probed green on the current head, waiting to be committed.
    pub green: bool,
    /// Its parts each passed alone: when it fails together, they go one by
    /// one.
    pub parts: Vec<Vec<usize>>,
    /// Sandbox builds of it that gave no result.
    pub broken: u32,
    /// A part of a set that failed together: never merged again.
    pub solo: bool,
}

/// What a bump came to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Dropped {
    pub bump: usize,
    pub checks: Vec<String>,
    pub error: String,
}

/// Tokens, dollars and probes one batch used.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BatchUse {
    pub label: String,
    pub probes: u32,
    pub tokens: u64,
    pub usd: f64,
    pub applied: usize,
    pub dropped: usize,
}

/// What the worker does next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Run the checks once on the base branch, nothing applied.
    Base,
    /// Probe each of these groups (indices into `groups`) on the current
    /// head, at once.
    Probe(Vec<usize>),
    /// Commit the files the green probe of this group wrote.
    Commit(usize),
    /// Hand this major to the coder: apply it, fix the call sites the
    /// failing checks name, then probe the head.
    CodeFix {
        bump: usize,
        failing: Vec<String>,
        error: String,
    },
    /// Find what the runtime proof runs: the repository's build and start
    /// commands (`runtime_proof::discover`).
    ProofPlan,
    /// Run the runtime proof in a sandbox build of its own: on the base
    /// branch (`base`), or on the branch with the committed bumps.
    Proof { base: bool },
    /// The build or the start fails with the bumps and passes without them:
    /// take out the newest commit of verified bumps
    /// ([`DepRun::take_out_last_commit`]) with this reason, then prove the
    /// branch again.
    ProofTakeOut { why: String },
    /// Nothing left: open the pull request with what is committed.
    Done,
}

/// The runtime proof of one run: the repository's real build and its start
/// command, run on the base branch and on the branch with the bumps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RuntimeProof {
    /// What runs, found once.
    pub plan: Option<rp::ProofPlan>,
    pub base: Option<rp::ProofResult>,
    /// The branch's result, read on the head `proved`.
    pub head: Option<rp::ProofResult>,
    pub proved: String,
    /// Sandbox builds started for it.
    pub runs: u32,
    /// Builds in a row that gave no result.
    pub broken: u32,
    /// The run already moved to a fresh step so the next proof build gets a
    /// full time window; it never waits twice for the same build.
    pub waited: bool,
    /// The sets of bumps the proof took out, oldest first: each as where
    /// its deferrals start in the run's `deferred` and how many they are.
    pub taken_out: Vec<(usize, usize)>,
    /// What still fails with the bumps and passes without them, when nothing
    /// more could be taken out.
    pub unresolved: Vec<String>,
    /// Why no proof ran; empty when one did.
    pub skipped: String,
    pub done: bool,
}

/// Why a bump the checks passed is deferred after all: the repository's
/// build or start fails with it. `blamed`: what fails, from
/// [`rp::blamed`].
pub fn proof_deferral_reason(blamed: &[String]) -> String {
    format!(
        "with it applied {}, which passes on the base branch; taken out so the bumps before it ship",
        crate::truncate_str(&blamed.join("; "), 300)
    )
}

/// How the code-fix attempt ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeFixed {
    /// The head with the bump and its call sites passes.
    Green { head: String },
    /// It didn't pass; the branch is back where it was.
    Failed { checks: Vec<String>, error: String },
}

/// One dependency fix run, resumable from its JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DepRun {
    pub plan: DepPlan,
    /// (risk, bump indices) in order; `batch` is the one being verified.
    pub batches: Vec<(Option<Risk>, Vec<usize>)>,
    pub batch: usize,
    pub groups: Vec<Group>,
    /// The checks that fail on the base branch: noted, never blamed.
    pub base_failing: Option<Vec<String>>,
    /// The base ran no check at all (install fails, no sandbox result):
    /// nothing can be verified locally.
    pub unverified: bool,
    pub applied: Vec<usize>,
    pub dropped: Vec<Dropped>,
    /// Majors that broke alone and the run must fix: waiting for the coder.
    pub code_queue: Vec<Dropped>,
    pub code_fixes: u32,
    /// Bumps the run took out after planning (dropped, budget, time).
    pub deferred: Vec<Deferral>,
    pub probes: u32,
    pub max_probes: u32,
    pub head: String,
    /// The checks the last probe ran, by name, with their results.
    pub last_checks: Vec<(String, bool)>,
    pub budget_usd: f64,
    pub spent_usd: f64,
    /// The budget stopped the run: what it left is deferred `later`, and
    /// [`DepRun::resume_with_budget`] picks it up.
    pub budget_stop: bool,
    pub uses: Vec<BatchUse>,
    pub done: bool,
    /// Each commit of verified bumps on the branch, oldest first: what the
    /// pull request's own CI can take out again, one at a time
    /// ([`DepRun::take_out_last_commit`]).
    pub commits: Vec<BatchCommit>,
    /// The build and the start, proven after the batches settle.
    pub proof: RuntimeProof,
}

/// One commit of verified bumps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BatchCommit {
    /// The head the commit was made on: its files are the branch without
    /// this commit and everything after it.
    pub before: String,
    pub head: String,
    pub bumps: Vec<usize>,
}

/// What [`DepRun::take_out_last_commit`] took out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TakenOut {
    /// The head whose files the branch goes back to.
    pub restore_to: String,
    /// Each bump taken out, as its deferral line.
    pub lines: Vec<String>,
}

/// Why a bump the sandbox verified is deferred after all: the pull
/// request's own CI failed with it applied. `failure` is what CI printed.
pub fn ci_deferral_reason(failure: &str) -> String {
    let failure = failure.trim();
    if failure.is_empty() {
        "the pull request's CI failed with it applied; taken out so the bumps before it ship"
            .to_string()
    } else {
        format!(
            "the pull request's CI failed with it applied ({}); taken out so the bumps before it ship",
            crate::truncate_str(failure, 300)
        )
    }
}

impl DepRun {
    pub fn new(plan: DepPlan, head: &str, budget_usd: f64) -> Self {
        let batches: Vec<(Option<Risk>, Vec<usize>)> = plan
            .batches()
            .into_iter()
            .map(|(r, ids)| (Some(r), ids))
            .collect();
        let uses = batches
            .iter()
            .map(|(r, ids)| BatchUse {
                label: match (r, ids.as_slice()) {
                    (Some(r), [one]) if r.is_major() => format!(
                        "{}: `{}` {} to {}",
                        r.label(),
                        plan.bumps[*one].package,
                        plan.bumps[*one].from,
                        plan.bumps[*one].to
                    ),
                    (Some(r), _) => format!("{} ({})", r.label(), ids.len()),
                    (None, _) => String::new(),
                },
                ..Default::default()
            })
            .collect();
        DepRun {
            plan,
            batches,
            head: head.to_string(),
            max_probes: MAX_PROBES,
            budget_usd,
            uses,
            ..Default::default()
        }
    }

    /// A short token that moves whenever the run makes progress (a probe
    /// read, a commit, a batch done): steps compare it to tell a stall.
    pub fn progress(&self) -> String {
        format!(
            "dep:{}:{}:{}:{}:{}:{}",
            self.batch,
            self.applied.len(),
            self.probes,
            self.dropped.len() + self.deferred.len(),
            self.proof.runs + u32::from(self.proof.waited) + u32::from(self.proof.plan.is_some()),
            self.proof.taken_out.len()
        )
    }

    fn base(&self) -> &[String] {
        self.base_failing.as_deref().unwrap_or(&[])
    }

    fn use_mut(&mut self) -> Option<&mut BatchUse> {
        let i = self.batch;
        self.uses.get_mut(i)
    }

    /// The next thing to do.
    pub fn next_action(&mut self) -> Action {
        if self.done {
            return Action::Done;
        }
        if self.base_failing.is_none() {
            return Action::Base;
        }
        loop {
            if let Some(i) = self.groups.iter().position(|g| g.green) {
                return Action::Commit(i);
            }
            if self.groups.is_empty() {
                if let Some(d) = self.code_queue.first().cloned() {
                    if self.code_fixes < MAX_CODE_FIXES && !self.over_budget() {
                        return Action::CodeFix {
                            bump: d.bump,
                            failing: d.checks.clone(),
                            error: d.error.clone(),
                        };
                    }
                    let (why, cause) = if self.over_budget() {
                        self.budget_stop = true;
                        (
                            "the run's budget was reached before its call sites were changed",
                            Cause::Later,
                        )
                    } else {
                        (
                            "it breaks the checks and the run's attempts at its call sites are used up",
                            Cause::Broke,
                        )
                    };
                    self.code_queue.remove(0);
                    self.drop_bump(d, why, self.over_budget(), cause);
                    self.end_batch_if_settled();
                    continue;
                }
                if self.batch >= self.batches.len() {
                    if let Some(a) = self.proof_action() {
                        return a;
                    }
                    self.done = true;
                    return Action::Done;
                }
                let ids = self.batches[self.batch].1.clone();
                self.groups.push(Group {
                    bumps: ids,
                    ..Default::default()
                });
            }
            if self.probes >= self.max_probes {
                self.give_up_unverified("not verified: the run used all its sandbox probes");
                continue;
            }
            let unknown: Vec<usize> = (0..self.groups.len())
                .filter(|&i| self.groups[i].red.is_none() && !self.groups[i].green)
                .take(MAX_PARALLEL_PROBES)
                .collect();
            if !unknown.is_empty() {
                return Action::Probe(unknown);
            }
            // Every group is red: split them, or settle a single bump.
            let reds = std::mem::take(&mut self.groups);
            for g in reds {
                let checks = g.red.clone().unwrap_or_default();
                if g.bumps.len() == 1 {
                    self.settle_single(g.bumps[0], checks, g.tail.clone());
                } else if !g.parts.is_empty() {
                    for p in g.parts {
                        self.groups.push(Group {
                            bumps: p,
                            solo: true,
                            ..Default::default()
                        });
                    }
                } else {
                    let mid = g.bumps.len() / 2;
                    let (a, b) = g.bumps.split_at(mid);
                    for half in [a, b] {
                        self.groups.push(Group {
                            bumps: half.to_vec(),
                            ..Default::default()
                        });
                    }
                }
            }
            if self.groups.is_empty() {
                self.end_batch_if_settled();
            }
        }
    }

    fn end_batch_if_settled(&mut self) {
        if self.groups.is_empty() && self.code_queue.is_empty() {
            self.batch += 1;
        }
    }

    fn settle_single(&mut self, bump: usize, checks: Vec<String>, error: String) {
        let b = &self.plan.bumps[bump];
        let d = Dropped {
            bump,
            checks: checks.clone(),
            error,
        };
        let only_lock = checks.iter().all(|c| c == LOCKFILE_CHECK);
        if b.major && b.risk() == Risk::UrgentMajor && !only_lock {
            self.code_queue.push(d);
        } else if only_lock {
            self.drop_bump(
                d,
                "its version could not be installed (the lockfile could not be written with it)",
                false,
                Cause::Parent,
            );
        } else {
            let why = format!("it breaks {} in the sandbox", checks.join(", "));
            self.drop_bump(d, &why, false, Cause::Broke);
        }
    }

    fn drop_bump(&mut self, d: Dropped, why: &str, later: bool, cause: Cause) {
        let b = self.plan.bumps[d.bump].clone();
        let mut reason = why.to_string();
        if !d.error.trim().is_empty() {
            reason.push_str(&format!(
                " ({})",
                crate::truncate_str(first_error_line(&d.error), 160)
            ));
        }
        self.deferred.push(Deferral {
            package: b.package,
            from: b.from,
            to: b.to,
            manifest: b.manifest,
            finding_ids: b.finding_ids,
            reason,
            later,
            cause,
        });
        if let Some(u) = self.use_mut() {
            u.dropped += 1;
        }
        self.dropped.push(d);
    }

    fn give_up_unverified(&mut self, why: &str) {
        let left: Vec<usize> = self
            .groups
            .drain(..)
            .flat_map(|g| g.bumps)
            .chain(
                self.batches
                    .iter()
                    .skip(self.batch + 1)
                    .flat_map(|(_, ids)| ids.clone()),
            )
            .collect();
        for i in left {
            self.drop_bump(
                Dropped {
                    bump: i,
                    checks: vec![],
                    error: String::new(),
                },
                why,
                true,
                Cause::Later,
            );
        }
        for d in std::mem::take(&mut self.code_queue) {
            self.drop_bump(d, why, true, Cause::Later);
        }
        self.batch = self.batches.len();
    }

    /// The base branch's checks.
    pub fn on_base(&mut self, r: &ProbeResult) {
        self.probes += 1;
        if !r.ran {
            self.base_failing = Some(vec![]);
            self.unverified = true;
        } else {
            let failing = r.failing();
            let names: Vec<&String> = r.checks.iter().map(|(n, _)| n).collect();
            // Nothing ran past the install: no check can tell a bump apart.
            self.unverified = names.is_empty()
                || failing
                    .iter()
                    .any(|c| c == INSTALL_CHECK || c == LOCKFILE_CHECK)
                || failing.len() == r.checks.len();
            self.base_failing = Some(failing);
            self.last_checks = r.checks.clone();
        }
        if self.unverified {
            // Without a local signal only same-line bumps go, applied as one
            // set; CI on the pull request is the check. Majors wait.
            let keep: Vec<usize> = (0..self.plan.bumps.len())
                .filter(|&i| !self.plan.bumps[i].major)
                .collect();
            let majors: Vec<usize> = (0..self.plan.bumps.len())
                .filter(|&i| self.plan.bumps[i].major)
                .collect();
            for i in majors {
                self.drop_bump(
                    Dropped {
                        bump: i,
                        checks: vec![],
                        error: String::new(),
                    },
                    "a major upgrade, and the sandbox could not run the repository's checks on the base branch to verify it",
                    true,
                    Cause::Unverified,
                );
            }
            self.batches = if keep.is_empty() {
                vec![]
            } else {
                vec![(Some(Risk::Safe), keep)]
            };
            self.uses = vec![BatchUse {
                label: "same-line fixes, applied without a local check".into(),
                ..Default::default()
            }];
            self.batch = 0;
        }
    }

    /// The probes of the groups `Action::Probe` named, in any order.
    pub fn on_probes(&mut self, results: &[(usize, ProbeResult)]) {
        let base = self.base().to_vec();
        for (gi, r) in results {
            let Some(g) = self.groups.get_mut(*gi) else {
                continue;
            };
            if !r.ran {
                g.broken += 1;
                if g.broken >= MAX_BROKEN_PROBES {
                    g.red = Some(vec!["sandbox".into()]);
                    g.tail = "the sandbox gave no result".into();
                    // Never blamed on a bump: the whole group waits.
                    let ids = g.bumps.clone();
                    g.bumps.clear();
                    for i in ids {
                        self.deferred.push(Deferral {
                            package: self.plan.bumps[i].package.clone(),
                            from: self.plan.bumps[i].from.clone(),
                            to: self.plan.bumps[i].to.clone(),
                            manifest: self.plan.bumps[i].manifest.clone(),
                            finding_ids: self.plan.bumps[i].finding_ids.clone(),
                            reason: "not verified: the sandbox gave no result twice".into(),
                            later: true,
                            cause: Cause::Unverified,
                        });
                    }
                }
                continue;
            }
            self.probes += 1;
            if let Some(u) = self.uses.get_mut(self.batch) {
                u.probes += 1;
            }
            let new: Vec<String> = if self.unverified {
                // Only the apply and the lockfile can fail without a base.
                r.failing()
                    .into_iter()
                    .filter(|c| c == LOCKFILE_CHECK)
                    .collect()
            } else {
                r.failing()
                    .into_iter()
                    .filter(|c| !base.contains(c))
                    .collect()
            };
            if new.is_empty() {
                g.green = true;
                self.last_checks = r.checks.clone();
            } else {
                g.red = Some(new);
                g.tail = crate::tail_str(&r.tail, 1500).to_string();
            }
        }
        self.groups.retain(|g| !g.bumps.is_empty());
        if self.groups.is_empty() {
            self.end_batch_if_settled();
            return;
        }
        // Two groups green on the same head are committed as one, after one
        // more probe of them together (a part of a set that already failed
        // together is committed on its own).
        let greens: Vec<usize> = (0..self.groups.len())
            .filter(|&i| self.groups[i].green && !self.groups[i].solo)
            .collect();
        if greens.len() > 1 {
            let mut parts = vec![];
            let mut all = vec![];
            for &i in &greens {
                parts.push(self.groups[i].bumps.clone());
                all.extend(self.groups[i].bumps.clone());
            }
            let mut keep: Vec<Group> = self
                .groups
                .drain(..)
                .enumerate()
                .filter(|(i, _)| !greens.contains(i))
                .map(|(_, g)| g)
                .collect();
            keep.insert(
                0,
                Group {
                    bumps: all,
                    parts,
                    ..Default::default()
                },
            );
            self.groups = keep;
        }
    }

    /// The group `Action::Commit` named is on the branch at `head`.
    pub fn on_commit(&mut self, gi: usize, head: &str) {
        if gi >= self.groups.len() {
            return;
        }
        let g = self.groups.remove(gi);
        if let Some(u) = self.uses.get_mut(self.batch) {
            u.applied += g.bumps.len();
        }
        if head != self.head {
            self.commits.push(BatchCommit {
                before: self.head.clone(),
                head: head.to_string(),
                bumps: g.bumps.clone(),
            });
        }
        self.applied.extend(g.bumps);
        self.head = head.to_string();
        // A green probe of the rest was on the old head: probed again. A red
        // one stays informative on the new head.
        for g in &mut self.groups {
            g.green = false;
        }
        self.end_batch_if_settled();
    }

    /// The group `Action::Commit` named could not be committed (GitHub
    /// refused): its bumps wait for a later run.
    pub fn on_commit_failed(&mut self, gi: usize, why: &str) {
        if gi >= self.groups.len() {
            return;
        }
        let g = self.groups.remove(gi);
        for i in g.bumps {
            self.drop_bump(
                Dropped {
                    bump: i,
                    checks: vec![],
                    error: String::new(),
                },
                &format!("its verified change could not be committed ({why})"),
                true,
                Cause::Later,
            );
        }
        self.end_batch_if_settled();
    }

    /// The coder's attempt at `Action::CodeFix` ended. `usd`/`tokens`: what
    /// it used.
    pub fn on_code_fix(&mut self, bump: usize, r: CodeFixed, tokens: u64, usd: f64) {
        self.code_fixes += 1;
        self.spent_usd += usd;
        if let Some(u) = self.uses.get_mut(self.batch) {
            u.tokens += tokens;
            u.usd += usd;
        }
        let Some(pos) = self.code_queue.iter().position(|d| d.bump == bump) else {
            return;
        };
        let d = self.code_queue.remove(pos);
        match r {
            CodeFixed::Green { head } => {
                if let Some(u) = self.uses.get_mut(self.batch) {
                    u.applied += 1;
                }
                self.applied.push(bump);
                self.head = head;
            }
            CodeFixed::Failed { checks, error } => {
                let checks = if checks.is_empty() { d.checks } else { checks };
                self.drop_bump(
                    Dropped {
                        bump,
                        checks: checks.clone(),
                        error,
                    },
                    &format!(
                        "it breaks {} and the coder's change to its call sites did not make them pass",
                        checks.join(", ")
                    ),
                    false,
                    Cause::Broke,
                );
            }
        }
        self.end_batch_if_settled();
    }

    /// Model spend outside a code fix (none today; kept for the report).
    pub fn over_budget(&self) -> bool {
        self.budget_usd > 0.0 && self.spent_usd >= self.budget_usd
    }

    /// A run stopped by its budget, retried with more: what the budget left
    /// goes back in, the applied bumps stay.
    pub fn resume_with_budget(&mut self, budget_usd: f64) {
        self.budget_usd = budget_usd;
        if !self.budget_stop {
            return;
        }
        self.budget_stop = false;
        self.done = false;
        let mut back: Vec<usize> = vec![];
        for x in self.deferred.iter().filter(|x| x.later) {
            if let Some(i) = (0..self.plan.bumps.len()).find(|&i| {
                let b = &self.plan.bumps[i];
                b.package == x.package && b.manifest == x.manifest && b.from == x.from
            }) {
                if !back.contains(&i) && !self.applied.contains(&i) {
                    back.push(i);
                }
            }
        }
        self.deferred.retain(|x| !x.later);
        self.dropped.retain(|d| !back.contains(&d.bump));
        for i in back {
            self.batches.push((self.plan.bumps[i].risk, vec![i]));
            self.uses.push(BatchUse {
                label: format!("resumed: `{}`", self.plan.bumps[i].package),
                ..Default::default()
            });
        }
        self.code_fixes = 0;
    }

    /// Ends the run where it stands (no step left): what is committed
    /// ships, everything not yet verified is deferred `later`.
    /// The pull request's CI is red after every fix: the newest commit of
    /// verified bumps comes out, its bumps deferred with `why` (not to come
    /// back by themselves: they broke the checks), and the commits before
    /// it stay. `None` when one commit or none is left: the pull request is
    /// never emptied. Called again while CI stays red, it takes out the one
    /// before, so only what breaks CI is deferred.
    pub fn take_out_last_commit(&mut self, why: &str) -> Option<TakenOut> {
        if self.commits.len() < 2 {
            return None;
        }
        let c = self.commits.pop()?;
        let mut lines = Vec::new();
        for &i in &c.bumps {
            let Some(b) = self.plan.bumps.get(i).cloned() else {
                continue;
            };
            let d = Deferral {
                package: b.package,
                from: b.from,
                to: b.to,
                manifest: b.manifest,
                finding_ids: b.finding_ids,
                reason: why.to_string(),
                later: false,
                cause: Cause::Broke,
            };
            lines.push(d.line());
            self.deferred.push(d);
        }
        self.applied.retain(|i| !c.bumps.contains(i));
        self.head = c.before.clone();
        Some(TakenOut {
            restore_to: c.before,
            lines,
        })
    }

    /// The runtime proof's next action, once every batch is settled; `None`
    /// when the proof is over (or has nothing to prove).
    fn proof_action(&mut self) -> Option<Action> {
        if self.proof.done {
            return None;
        }
        let skip = |p: &mut RuntimeProof, why: String| -> Option<Action> {
            p.skipped = why;
            p.done = true;
            None
        };
        if self.applied.is_empty() {
            self.proof.done = true;
            return None;
        }
        if self.unverified {
            return skip(
                &mut self.proof,
                "the sandbox could not run the repository's commands on the base branch".into(),
            );
        }
        let Some(plan) = self.proof.plan.clone() else {
            return Some(Action::ProofPlan);
        };
        if plan.is_empty() {
            return skip(
                &mut self.proof,
                format!("{}; {}", plan.no_build, plan.no_start),
            );
        }
        let Some(base) = self.proof.base.clone() else {
            return Some(Action::Proof { base: true });
        };
        if !base.ran {
            return skip(
                &mut self.proof,
                format!(
                    "the sandbox gave no result for the base branch ({})",
                    crate::truncate_str(first_error_line(&base.tail), 160)
                ),
            );
        }
        let head = match &self.proof.head {
            Some(h) if self.proof.proved == self.head => h.clone(),
            _ => return Some(Action::Proof { base: false }),
        };
        let blamed = rp::blamed(&plan, &base, &head);
        if blamed.is_empty() {
            // The set whose removal made it pass is the cause. The sets
            // taken out before it were only newer: removing them changed
            // nothing, so they wait for the next run instead of being held
            // as breaking.
            let cleared = self.proof.taken_out.len().saturating_sub(1);
            self.release_taken_out(cleared);
            self.proof.done = true;
            return None;
        }
        if self.commits.len() >= 2 && self.proof.runs < MAX_PROOF_RUNS {
            return Some(Action::ProofTakeOut {
                why: proof_deferral_reason(&blamed),
            });
        }
        // One commit left (the pull request is never emptied) or the proof's
        // builds are used up: what still fails is said, and the pull
        // request's own CI decides. No set taken out was shown to be the
        // cause.
        self.release_taken_out(self.proof.taken_out.len());
        self.proof.unresolved = blamed;
        self.proof.done = true;
        None
    }

    /// The first `n` sets the proof took out were not shown to break
    /// anything (the build still failed without them): their bumps come
    /// back in the next run instead of being held out as breaking.
    fn release_taken_out(&mut self, n: usize) {
        for &(start, len) in self.proof.taken_out.iter().take(n) {
            for d in self.deferred.iter_mut().skip(start).take(len) {
                d.later = true;
                d.cause = Cause::Later;
                d.reason = "taken out while the application's build or start failed with a newer set; removing it did not make it pass, so the next run verifies it again".into();
            }
        }
    }

    /// What the runtime proof runs was found.
    pub fn on_proof_plan(&mut self, plan: rp::ProofPlan) {
        self.proof.plan = Some(plan);
    }

    /// The proof was not permitted or could not be set up: said, never a
    /// claim.
    pub fn skip_proof(&mut self, why: &str) {
        self.proof.skipped = why.to_string();
        self.proof.done = true;
    }

    /// The result of `Action::Proof`. A build that gave no result is run
    /// once more before it stands as "no result".
    pub fn on_proof(&mut self, base: bool, r: rp::ProofResult) {
        self.proof.runs += 1;
        self.proof.waited = false;
        if !r.ran {
            self.proof.broken += 1;
            if self.proof.broken < MAX_BROKEN_PROBES && self.proof.runs < MAX_PROOF_RUNS {
                return;
            }
        }
        self.proof.broken = 0;
        if base {
            self.proof.base = Some(r);
        } else {
            self.proof.head = Some(r);
            self.proof.proved = self.head.clone();
        }
    }

    /// The run moved to a fresh step so the next proof build has a full time
    /// window.
    pub fn on_proof_wait(&mut self) {
        self.proof.waited = true;
    }

    /// `Action::ProofTakeOut` was written to the branch, now at `head`: the
    /// bumps it took out are deferred, and the branch is proven again.
    pub fn on_proof_taken_out(&mut self, out: &TakenOut, head: &str) {
        let n = out.lines.len();
        self.proof
            .taken_out
            .push((self.deferred.len().saturating_sub(n), n));
        self.head = head.to_string();
        self.proof.head = None;
    }

    /// `Action::ProofTakeOut` could not be written: what fails is said, and
    /// the pull request's own CI decides.
    pub fn on_proof_take_out_failed(&mut self) {
        let p = &self.proof;
        let blamed = match (&p.plan, &p.base, &p.head) {
            (Some(plan), Some(base), Some(head)) => rp::blamed(plan, base, head),
            _ => vec![],
        };
        self.release_taken_out(self.proof.taken_out.len());
        self.proof.unresolved = blamed;
        self.proof.done = true;
    }

    /// The report's account of the runtime proof.
    fn proof_md(&self) -> String {
        let p = &self.proof;
        let mut s = String::from(
            "\n**Runtime proof (the repository's build and start, run in the sandbox with the bumps):**\n",
        );
        if !p.skipped.is_empty() {
            s.push_str(&format!(
                "- Not run: {}. Nothing here claims the application builds or starts; CI on this pull request is the check.\n",
                p.skipped
            ));
            return s;
        }
        let Some(plan) = &p.plan else {
            s.push_str("- Not run in this run. Nothing here claims the application builds or starts; CI on this pull request is the check.\n");
            return s;
        };
        s.push_str(&rp::report(plan, p.base.as_ref(), p.head.as_ref()));
        if p.head.is_some() && p.proved != self.head {
            s.push_str("- This proof is of the branch before the pull request's CI took out its newest set of bumps; what is left was not built again here.\n");
        }
        for &(start, len) in &p.taken_out {
            for d in self.deferred.iter().skip(start).take(len) {
                s.push_str(&format!("- Taken out by this proof: {}\n", d.line()));
            }
        }
        if !p.unresolved.is_empty() {
            s.push_str(&format!(
                "- Still failing with the bumps in this pull request: {}. It passes on the base branch, and no further set of bumps could be taken out here, so the pull request's CI decides.\n",
                p.unresolved.join("; ")
            ));
        }
        s
    }

    pub fn finish_now(&mut self, why: &str) {
        self.give_up_unverified(why);
        if !self.proof.done {
            // The proof never ran to its end: said as not run, with why.
            self.proof.skipped = "the run had no step left for it".into();
            self.proof.done = true;
        }
        self.done = true;
    }

    /// The commit the run started from: the base branch's head when it was
    /// planned. `None` when no commit of bumps recorded it.
    pub fn base_sha(&self) -> Option<&str> {
        self.commits.first().map(|c| c.before.as_str())
    }

    /// Every deferral: the plan's and the run's.
    pub fn all_deferred(&self) -> Vec<Deferral> {
        let mut v = self.plan.deferred.clone();
        v.extend(self.deferred.clone());
        v
    }

    pub fn applied_bumps(&self) -> Vec<&PlanBump> {
        self.applied.iter().map(|&i| &self.plan.bumps[i]).collect()
    }

    /// The findings the run's pull request fixes.
    pub fn fixed_ids(&self) -> Vec<String> {
        self.applied_bumps()
            .iter()
            .flat_map(|b| b.finding_ids.clone())
            .collect()
    }

    /// The findings it leaves open, with why.
    pub fn deferred_ids(&self) -> Vec<(String, String)> {
        self.all_deferred()
            .iter()
            .flat_map(|d| d.finding_ids.iter().map(move |id| (id.clone(), d.line())))
            .collect()
    }

    /// The pull request's account of the run, under "What to check": the
    /// bumps applied, the ones deferred with why, the checks run in the
    /// sandbox (the base's failures named as such), and what each batch
    /// used.
    pub fn report_md(&self) -> String {
        let mut s = String::from("### Dependency fix run\n\n");
        let applied = self.applied_bumps();
        s.push_str(&format!("**Applied ({}):**\n", applied.len()));
        if applied.is_empty() {
            s.push_str("- none\n");
        }
        for b in applied.iter().take(80) {
            s.push_str(&format!(
                "- `{}` {} to {} ({}; {})\n",
                b.package,
                b.from,
                b.to,
                b.manifest,
                match (b.major, b.major_reason.is_empty()) {
                    (true, false) => format!("major version: {}", b.major_reason),
                    (true, true) => "major version".to_string(),
                    (false, _) => "same version line".to_string(),
                }
            ));
        }
        if applied.len() > 80 {
            s.push_str(&format!("- and {} more\n", applied.len() - 80));
        }
        let deferred = self.all_deferred();
        if !deferred.is_empty() {
            s.push_str(&format!(
                "\n**Deferred ({}), the findings stay open:**\n",
                deferred.len()
            ));
            for d in deferred.iter().take(60) {
                s.push_str(&format!("- {}\n", d.line()));
            }
            if deferred.len() > 60 {
                s.push_str(&format!("- and {} more\n", deferred.len() - 60));
            }
        }
        s.push_str("\n**Checks run in the sandbox:**\n");
        if self.unverified {
            s.push_str("- The sandbox could not run the repository's checks on the base branch, so same-line fixes were applied with the lockfile written by the repository's own package manager and CI on this pull request is the check; major upgrades wait.\n");
        } else {
            for (name, ok) in &self.last_checks {
                let pre = self.base().contains(name);
                s.push_str(&format!(
                    "- {name}: {}\n",
                    match (ok, pre) {
                        (true, _) => "passes with these bumps",
                        (false, true) =>
                            "fails on the base branch too (not caused by this change; not fixed here)",
                        (false, false) => "fails",
                    }
                ));
            }
        }
        if !self.applied.is_empty() {
            s.push_str(&self.proof_md());
        }
        s.push_str(&format!(
            "\n**Cost:** {} sandbox probe(s){}; model ${:.2}",
            self.probes,
            if self.proof.runs > 0 {
                format!(" and {} runtime proof build(s)", self.proof.runs)
            } else {
                String::new()
            },
            self.spent_usd
        ));
        let used: Vec<&BatchUse> = self
            .uses
            .iter()
            .filter(|u| u.probes > 0 || u.tokens > 0)
            .collect();
        if !used.is_empty() {
            s.push_str(" by batch:\n");
            for u in used {
                s.push_str(&format!(
                    "- {}: {} probe(s), {} applied, {} deferred, {} tokens, ${:.2}\n",
                    u.label, u.probes, u.applied, u.dropped, u.tokens, u.usd
                ));
            }
        } else {
            s.push('\n');
        }
        s
    }
}

// ── What stays open after the run ───────────────────────────────────────

/// Why an advisory stays open after the run's pull request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Open {
    /// The advisory data lists no patched release at all.
    NoPatch,
    /// A parent package holds the version, or the fixed version could not
    /// be installed.
    Parent,
    /// The bump broke the checks, the build or the start and was deferred.
    Broke,
    /// Only a major upgrade fixes it and the rules leave it out.
    MajorNotNeeded,
    /// A cap, the budget or time: the next run takes it.
    Later,
    /// It could not be placed or verified.
    Unverified,
    /// Another open pull request fixes it.
    OtherPr,
    /// It was not part of this run.
    NotInRun,
}

impl Open {
    pub fn label(self) -> &'static str {
        match self {
            Open::NoPatch => "no patched release exists yet",
            Open::Parent => {
                "blocked by a parent package (it pins the version, or the fixed version could not be installed)"
            }
            Open::Broke => "deferred because the build or the checks failed with the bump",
            Open::MajorNotNeeded => {
                "only a major upgrade fixes it, and the advisory is not reachable or urgent"
            }
            Open::Later => "left for the next run (a cap, the budget or time)",
            Open::Unverified => "could not be placed or verified in the sandbox",
            Open::OtherPr => "fixed by another open pull request",
            Open::NotInRun => "not part of this run (not due yet, still being triaged, or in another run)",
        }
    }

    fn of(c: Cause) -> Open {
        match c {
            Cause::Broke => Open::Broke,
            Cause::Parent => Open::Parent,
            Cause::MajorNotNeeded => Open::MajorNotNeeded,
            Cause::Later => Open::Later,
            Cause::Unverified => Open::Unverified,
        }
    }
}

/// The advisories of one reason that stay open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenGroup {
    pub why: Open,
    /// Advisories counted once per package (an advisory on two version
    /// lines of a package is one).
    pub advisories: usize,
    pub packages: Vec<String>,
}

/// What stays open in the repository after the run: every finding of the
/// repository still open that the run's pull request does not fix, grouped
/// by why. `findings`: the repository's findings (any status; handled ones
/// are left out here).
pub fn remaining(run: &DepRun, findings: &[&Finding]) -> Vec<OpenGroup> {
    let fixed = run.fixed_ids();
    let deferred: BTreeMap<String, Cause> = run
        .all_deferred()
        .iter()
        .flat_map(|d| d.finding_ids.iter().map(|id| (id.clone(), d.cause)))
        .collect();
    let mut by: BTreeMap<Open, Vec<(String, String)>> = BTreeMap::new();
    for f in findings {
        if !f.status.is_open() || fixed.contains(&f.id) {
            continue;
        }
        let why = if let Some(c) = deferred.get(&f.id) {
            Open::of(*c)
        } else if f.fixed_version.is_none() || f.status == Status::NoFix {
            Open::NoPatch
        } else if f.status == Status::PrOpen {
            Open::OtherPr
        } else {
            Open::NotInRun
        };
        let key = (f.advisory.primary.clone(), f.package.clone());
        let v = by.entry(why).or_default();
        if !v.contains(&key) {
            v.push(key);
        }
    }
    by.into_iter()
        .map(|(why, pairs)| {
            let mut packages: Vec<String> = pairs.iter().map(|(_, p)| p.clone()).collect();
            packages.sort();
            packages.dedup();
            OpenGroup {
                why,
                advisories: pairs.len(),
                packages,
            }
        })
        .collect()
}

/// The report's last part: how many advisories stay open after the pull
/// request, and why, by group. Counted from CoderHelm's own findings for
/// the repository (its advisory sources), which is said.
pub fn remaining_md(groups: &[OpenGroup]) -> String {
    let total: usize = groups.iter().map(|g| g.advisories).sum();
    let mut s = format!(
        "**Advisories that stay open after this pull request: {total}** (counted from the advisories CoderHelm tracks for this repository)\n"
    );
    if total == 0 {
        s.push_str(
            "- None: every open advisory CoderHelm tracks for this repository is fixed here.\n",
        );
        return s;
    }
    for g in groups {
        let shown: Vec<String> = g
            .packages
            .iter()
            .take(8)
            .map(|p| format!("`{p}`"))
            .collect();
        let more = g.packages.len().saturating_sub(8);
        s.push_str(&format!(
            "- {}: {} ({}{})\n",
            g.advisories,
            g.why.label(),
            shown.join(", "),
            if more > 0 {
                format!(", and {more} more")
            } else {
                String::new()
            }
        ));
    }
    s
}

fn first_error_line(tail: &str) -> &str {
    tail.lines()
        .map(str::trim)
        .find(|l| {
            let low = l.to_ascii_lowercase();
            low.contains("error") || low.contains("err_") || low.contains("failed")
        })
        .or_else(|| tail.lines().map(str::trim).find(|l| !l.is_empty()))
        .unwrap_or("")
}

// ── Tasks a security ticket is held to ──────────────────────────────────

/// A planned task path a dependency run is never held to unless the ticket
/// names it: documentation, changelogs, and similar prose.
pub fn is_doc_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    lower.starts_with("docs/")
        || lower.contains("/docs/")
        || name.ends_with(".md")
        || name.ends_with(".mdx")
        || name.ends_with(".rst")
        || name.ends_with(".adoc")
        || name == "changelog"
        || name == "license"
}

/// Whether a task a run's plan holds it to is core to a dependency fix: a
/// manifest, a lockfile, or code (call sites); never documentation the
/// ticket doesn't name.
pub fn core_to_dep_fix(path: &str, ticket_body: &str) -> bool {
    !is_doc_path(path) || ticket_body.contains(path)
}

#[cfg(test)]
mod tests;
