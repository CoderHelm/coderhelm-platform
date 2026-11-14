//! Why a bump across a major version is safe, from evidence.
//!
//! A dependency pull request moved four packages across a major version,
//! three of them through overrides. The code was sound. The description
//! did not say who depends on each, whether that code runs in production
//! or only when the project installs, builds or lints, whether the feature
//! that uses it is configured at all, or what was run for the one that
//! sits behind a request.
//!
//! For each major, [`evidence`] assembles what a reviewer asks, each part
//! from a recorded fact:
//!
//! - the dependency paths that reach it, each classed ([`classify`]) as
//!   production, development, build time or install time, from where the
//!   manifest declares the path's root and which of the repository's files
//!   import it;
//! - whether the repository uses the package or the feature that needs it,
//!   from a search of its files, with what was searched;
//! - the reachability verdict with its limit written beside it: "not
//!   reachable" means no path was found, and is never proof of safety;
//! - how it is fixed ([`Route`]): raised in the manifest, through a parent
//!   release that declares the fixed range ([`parent_route`], preferred
//!   over an override), or by an override scoped to the version line, with
//!   every parent the override pushes outside its declared range named;
//! - whether it needs a functional probe: a package on a request or job
//!   path in production does, and a development tool does not.
//!
//! Everything here is pure.

use serde::{Deserialize, Serialize};

use crate::dep_fix::Companion;
use crate::reachability::{is_test_path, is_tooling_config};
use crate::security_fixes::{is_major_bump, version_line, Verdict};
use crate::upgrade_notes::{range_admits, ParentSupport};

/// When the code of a dependency path runs.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum PathClass {
    /// In the running application.
    Production,
    /// When the project is built (a bundler, a compiler plugin).
    BuildTime,
    /// When the project's dependencies are installed (an install script).
    InstallTime,
    /// In a development tool: tests, lint, local tooling.
    Development,
    #[default]
    NotKnown,
}

impl PathClass {
    pub fn words(self) -> &'static str {
        match self {
            PathClass::Production => "production",
            PathClass::BuildTime => "build time only",
            PathClass::InstallTime => "install time only",
            PathClass::Development => "development only",
            PathClass::NotKnown => "not known",
        }
    }
}

/// Where a manifest declares a direct dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Declared {
    Production,
    Development,
    #[default]
    NotKnown,
}

/// One dependency path that reaches a package: the direct dependency at
/// its root, and what is known of it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PathIn {
    /// The direct dependency that brings the package in (the package
    /// itself, when it is direct).
    pub root: String,
    pub declared: Declared,
    /// The repository's files that import the root.
    pub importers: Vec<String>,
    /// A package on the path runs a script when it is installed.
    pub install_script: bool,
    /// The manifest scripts that run the root (`build`, `lint`).
    pub scripts: Vec<String>,
}

/// One path with its class and the reason for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PathFact {
    pub root: String,
    pub class: PathClass,
    pub why: String,
}

fn name_list(files: &[&String]) -> String {
    let shown: Vec<String> = files.iter().take(3).map(|f| format!("`{f}`")).collect();
    format!(
        "{}{}",
        shown.join(", "),
        if files.len() > 3 {
            format!(" and {} more", files.len() - 3)
        } else {
            String::new()
        }
    )
}

/// When the code of one path runs, with why. The files that import the
/// root decide before the manifest's section does: a package declared for
/// development and imported by application code runs in production.
pub fn classify(p: &PathIn) -> PathFact {
    let source: Vec<&String> = p
        .importers
        .iter()
        .filter(|f| !is_test_path(f) && !is_tooling_config(f))
        .collect();
    let tooling: Vec<&String> = p
        .importers
        .iter()
        .filter(|f| is_tooling_config(f))
        .collect();
    let tests: Vec<&String> = p.importers.iter().filter(|f| is_test_path(f)).collect();
    let (class, why) = match p.declared {
        _ if !source.is_empty() => (
            PathClass::Production,
            match p.declared {
                Declared::Development => format!(
                    "`{}` is declared for development and imported by application code ({})",
                    p.root,
                    name_list(&source)
                ),
                _ => format!("`{}` is imported by {}", p.root, name_list(&source)),
            },
        ),
        Declared::Development => (
            PathClass::Development,
            if tests.is_empty() && tooling.is_empty() {
                format!(
                    "`{}` is declared for development, and no application file imports it",
                    p.root
                )
            } else {
                let both: Vec<&String> = tooling.iter().chain(tests.iter()).copied().collect();
                format!(
                    "`{}` is declared for development and imported only by tests or tooling ({})",
                    p.root,
                    name_list(&both)
                )
            },
        ),
        Declared::Production if !tooling.is_empty() || !p.scripts.is_empty() => (
            PathClass::BuildTime,
            if tooling.is_empty() {
                format!(
                    "`{}` is run by the manifest's {} script{} and no application file imports it",
                    p.root,
                    p.scripts
                        .iter()
                        .map(|s| format!("`{s}`"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    if p.scripts.len() == 1 { "" } else { "s" }
                )
            } else {
                format!(
                    "`{}` is imported only by build configuration ({})",
                    p.root,
                    name_list(&tooling)
                )
            },
        ),
        Declared::Production if p.install_script => (
            PathClass::InstallTime,
            format!(
                "a package on the path through `{}` runs a script at install, and no file of the repository imports it",
                p.root
            ),
        ),
        Declared::Production => (
            PathClass::Production,
            format!(
                "`{}` is declared as a production dependency; no import of it was found, which is not proof it is unused",
                p.root
            ),
        ),
        Declared::NotKnown => (
            PathClass::NotKnown,
            format!(
                "where the manifest declares `{}` could not be read",
                p.root
            ),
        ),
    };
    PathFact {
        root: p.root.clone(),
        class,
        why,
    }
}

/// The reachability verdict as a sentence, with what it does not show.
pub fn reachability_words(v: Verdict) -> &'static str {
    match v {
        Verdict::Reachable => {
            "reachable: a path from the repository's code to the vulnerable code was found (a path is not proof it can be exploited)"
        }
        Verdict::NotReachable => {
            "no path from the repository's code to the vulnerable code was found. That is not proof of safety: calls made by name at runtime, through reflection or from generated code are not seen"
        }
        Verdict::NotLoaded => {
            "the package is not loaded by the repository's code. That is a read of its imports, not proof: a dependency can load it"
        }
        Verdict::Unknown => "reachability could not be decided from the code",
        Verdict::Pending => "reachability has not been read yet",
    }
}

/// How the fixed version gets installed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "by")]
pub enum Route {
    /// The repository's own manifest declares it: its range is raised.
    #[default]
    Direct,
    /// A parent's release declares the fixed range: the parent is raised.
    Parent {
        parent: String,
        from: String,
        to: String,
        declares: String,
    },
    /// An override scoped to the package's version line.
    Override { selector: String },
}

/// The parent to raise instead of writing an override, when exactly one
/// parent brings the package in, the repository's manifest declares that
/// parent, the parent's newest release declares a range that admits the
/// fixed version and no longer admits the vulnerable one (so raising it
/// moves the package by itself), and raising the parent stays inside the
/// parent's own major version. `None` otherwise: an override is then the
/// way.
pub fn parent_route(
    ecosystem: &str,
    parents: &[ParentSupport],
    parent_is_direct: bool,
    from: &str,
) -> Option<Companion> {
    let [p] = parents else {
        return None;
    };
    if !parent_is_direct
        || p.version.is_empty()
        || p.latest.is_empty()
        || p.latest == p.version
        || p.admits == Some(true)
        || p.latest_admits != Some(true)
        || is_major_bump(&p.version, &p.latest)
        || range_admits(ecosystem, &p.latest_declares, from) != Some(false)
    {
        return None;
    }
    Some(Companion {
        package: p.parent.clone(),
        from: p.version.clone(),
        to: p.latest.clone(),
        line: version_line(&p.version),
    })
}

/// Routes each major of `plan` through its parent where [`parent_route`]
/// finds one: the parent is raised and no override is written. `notes`:
/// the upgrade notes read for the unit (each major's parents and what they
/// declare); `findings`: the unit's findings (their dependency paths say
/// whether the parent is in the repository's own manifest).
pub fn route_through_parents(
    plan: &mut crate::dep_fix::DepPlan,
    notes: &[crate::upgrade_notes::PackageNotes],
    findings: &[(
        crate::security_fixes::Finding,
        crate::security_fixes::FindingState,
    )],
) {
    for b in plan
        .bumps
        .iter_mut()
        .filter(|b| b.major && b.via_parent.is_none())
    {
        let Some(n) = notes.iter().find(|n| {
            n.package.eq_ignore_ascii_case(&b.package) && version_line(&n.from) == b.line
        }) else {
            continue;
        };
        if n.direct {
            continue;
        }
        let mine: Vec<&crate::security_fixes::Finding> = findings
            .iter()
            .map(|(f, _)| f)
            .filter(|f| b.finding_ids.contains(&f.id))
            .collect();
        // A path of two is the direct dependency and the package itself.
        let parent_is_direct = !mine.is_empty() && mine.iter().all(|f| f.path.len() == 2);
        b.via_parent = parent_route(&n.ecosystem, &n.parents, parent_is_direct, &b.from);
    }
}

/// Whether a major needs a functional probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "probe")]
pub enum ProbeNeed {
    /// It runs in production behind the repository's own code.
    Needed { why: String },
    /// It does not run in the application.
    #[default]
    NotNeeded,
}

/// What [`evidence`] reads for one major.
#[derive(Debug, Clone, Default)]
pub struct MajorIn<'a> {
    pub package: &'a str,
    pub from: &'a str,
    pub to: &'a str,
    pub ecosystem: &'a str,
    /// Why it leaves its version line.
    pub why_major: &'a str,
    pub paths: &'a [PathIn],
    /// The repository's files that use the package or the feature that
    /// needs it, as `path:line`.
    pub used_in: &'a [String],
    /// What was searched for, in words.
    pub searched: &'a str,
    pub verdict: Verdict,
    pub direct: bool,
    pub parents: &'a [ParentSupport],
    /// The parent the plan raises instead of an override, when it does.
    pub via_parent: Option<&'a Companion>,
    /// The override's key, when one is written.
    pub selector: &'a str,
}

/// Why one major is safe, or what is not known about it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct MajorSafety {
    pub package: String,
    pub from: String,
    pub to: String,
    pub why_major: String,
    pub paths: Vec<PathFact>,
    /// The class that decides: production when any path is.
    pub class: PathClass,
    pub used_in: Vec<String>,
    pub searched: String,
    pub reachability: String,
    pub route: Route,
    /// A parent release that would fix it without an override, named when
    /// an override is used anyway, with why.
    pub parent_available: String,
    /// Each parent the fixed version is outside the declared range of.
    pub outside_range: Vec<String>,
    pub probe: ProbeNeed,
}

/// The evidence for one major (see the module notes).
pub fn evidence(i: &MajorIn<'_>) -> MajorSafety {
    let paths: Vec<PathFact> = i.paths.iter().map(classify).collect();
    // The lowest class is the one that runs closest to production.
    let class = paths
        .iter()
        .map(|p| p.class)
        .filter(|c| *c != PathClass::NotKnown)
        .min()
        .unwrap_or(PathClass::NotKnown);
    let route = match (i.direct, i.via_parent) {
        (true, _) => Route::Direct,
        (false, Some(p)) => Route::Parent {
            parent: p.package.clone(),
            from: p.from.clone(),
            to: p.to.clone(),
            declares: i
                .parents
                .iter()
                .find(|s| s.parent == p.package)
                .map(|s| s.latest_declares.clone())
                .unwrap_or_default(),
        },
        (false, None) => Route::Override {
            selector: if i.selector.is_empty() {
                format!("{}@{}", i.package, version_line(i.from))
            } else {
                i.selector.to_string()
            },
        },
    };
    let overridden = matches!(route, Route::Override { .. });
    let outside_range: Vec<String> = if overridden {
        i.parents
            .iter()
            .filter(|p| p.admits == Some(false) && !p.declares.is_empty())
            .map(|p| {
                format!(
                    "`{}`{} declares `{}` for it, and the override installs {}, outside that range",
                    p.parent,
                    if p.version.is_empty() {
                        String::new()
                    } else {
                        format!(" {}", p.version)
                    },
                    p.declares,
                    i.to
                )
            })
            .collect()
    } else {
        vec![]
    };
    let parent_available = if overridden {
        i.parents
            .iter()
            .find(|p| p.latest_admits == Some(true) && p.admits != Some(true))
            .map(|p| {
                format!(
                    "`{}` {} declares `{}`, which admits {}. An override is used here because raising the parent would not by itself be enough ({})",
                    p.parent,
                    p.latest,
                    p.latest_declares,
                    i.to,
                    if i.parents.len() > 1 {
                        "more than one parent brings the package in"
                    } else if is_major_bump(&p.version, &p.latest) {
                        "the parent's own release is a major version away"
                    } else if p.version.is_empty() {
                        "the parent's installed version is not known"
                    } else {
                        "the repository's manifest does not declare the parent, or its new range still admits the vulnerable version"
                    }
                )
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    let probe = match class {
        PathClass::Production if !i.used_in.is_empty() => ProbeNeed::Needed {
            why: format!(
                "it runs in production and the repository uses it ({})",
                i.used_in
                    .iter()
                    .take(3)
                    .map(|u| format!("`{u}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        },
        PathClass::Production => ProbeNeed::Needed {
            why: "it is installed for production, and no use of it was found to rule a request or job path out".to_string(),
        },
        _ => ProbeNeed::NotNeeded,
    };
    MajorSafety {
        package: i.package.to_string(),
        from: i.from.to_string(),
        to: i.to.to_string(),
        why_major: i.why_major.to_string(),
        paths,
        class,
        used_in: i.used_in.to_vec(),
        searched: i.searched.to_string(),
        reachability: reachability_words(i.verdict).to_string(),
        route,
        parent_available,
        outside_range,
        probe,
    }
}

impl MajorSafety {
    /// The evidence as the pull request states it.
    pub fn section(&self) -> String {
        let mut s = format!(
            "- `{}` {} to {} (major): runs in {}.\n",
            self.package,
            self.from,
            self.to,
            match self.class {
                PathClass::NotKnown => "a place that could not be worked out",
                c => c.words(),
            }
        );
        if !self.why_major.is_empty() {
            s.push_str(&format!("  - Why a major: {}.\n", self.why_major));
        }
        for p in self.paths.iter().take(4) {
            s.push_str(&format!(
                "  - Path through `{}` ({}): {}.\n",
                p.root,
                p.class.words(),
                p.why
            ));
        }
        if self.paths.is_empty() {
            s.push_str("  - Dependency paths: not read from the lockfile, so who depends on it is not known.\n");
        } else if self.paths.len() > 4 {
            s.push_str(&format!("  - and {} more paths.\n", self.paths.len() - 4));
        }
        if !self.searched.is_empty() || !self.used_in.is_empty() {
            s.push_str(&match self.used_in.as_slice() {
                [] => format!(
                    "  - Use in this repository: none found (searched {}).\n",
                    self.searched
                ),
                used => format!(
                    "  - Use in this repository: {}{}.\n",
                    used.iter()
                        .take(4)
                        .map(|u| format!("`{u}`"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    if used.len() > 4 {
                        format!(" and {} more", used.len() - 4)
                    } else {
                        String::new()
                    }
                ),
            });
        }
        s.push_str(&format!("  - Reachability: {}.\n", self.reachability));
        s.push_str(&match &self.route {
            Route::Direct => {
                "  - Fixed by raising its range in the repository's own manifest.\n".to_string()
            }
            Route::Parent {
                parent,
                from,
                to,
                declares,
            } => format!(
                "  - Fixed by raising its parent `{parent}` {from} to {to}, which declares `{declares}`: no override.\n"
            ),
            Route::Override { selector } => format!(
                "  - Fixed by an override scoped to its version line (`{selector}`); other lines of the package are left alone.\n"
            ),
        });
        if !self.parent_available.is_empty() {
            s.push_str(&format!(
                "  - A parent release admits it: {}.\n",
                self.parent_available
            ));
        }
        for o in &self.outside_range {
            s.push_str(&format!("  - Outside a declared range: {o}.\n"));
        }
        if let ProbeNeed::Needed { why } = &self.probe {
            s.push_str(&format!(
                "  - Needs a functional probe: {why}. See Behaviour changes.\n"
            ));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(root: &str, declared: Declared, importers: &[&str]) -> PathIn {
        PathIn {
            root: root.into(),
            declared,
            importers: importers.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn a_path_is_classed_by_who_imports_its_root_then_by_where_it_is_declared() {
        // Production: declared for production and imported by the app.
        let f = classify(&path(
            "web-kit",
            Declared::Production,
            &["src/uploads/controller.ts"],
        ));
        assert_eq!(f.class, PathClass::Production);
        assert_eq!(
            f.why,
            "`web-kit` is imported by `src/uploads/controller.ts`"
        );
        // Development: declared for development, used by tests only.
        let f = classify(&path(
            "test-runner",
            Declared::Development,
            &["src/app.test.ts"],
        ));
        assert_eq!(f.class, PathClass::Development);
        // Declared for development, imported by application code: it runs
        // in production, and the mismatch is said.
        let f = classify(&path(
            "date-kit",
            Declared::Development,
            &["src/billing/invoice.ts"],
        ));
        assert_eq!(f.class, PathClass::Production);
        assert!(f
            .why
            .contains("declared for development and imported by application code"));
        // Build time: only a build configuration imports it.
        let f = classify(&path(
            "bundle-kit",
            Declared::Production,
            &["webpack.config.js"],
        ));
        assert_eq!(f.class, PathClass::BuildTime);
        // Build time: run by a manifest script, imported nowhere.
        let f = classify(&PathIn {
            scripts: vec!["build".into()],
            ..path("compile-kit", Declared::Production, &[])
        });
        assert_eq!(f.class, PathClass::BuildTime);
        assert!(f.why.contains("run by the manifest's `build` script"));
        // Install time: an install script on the path, imported nowhere.
        let f = classify(&PathIn {
            install_script: true,
            ..path("native-kit", Declared::Production, &[])
        });
        assert_eq!(f.class, PathClass::InstallTime);
        // Declared for production with nothing found: production, and the
        // absence is not called proof.
        let f = classify(&path("quiet-kit", Declared::Production, &[]));
        assert_eq!(f.class, PathClass::Production);
        assert!(f.why.contains("not proof it is unused"));
        let f = classify(&path("lost", Declared::NotKnown, &[]));
        assert_eq!(f.class, PathClass::NotKnown);
    }

    fn parent(version: &str, admits: Option<bool>, latest: &str, declares: &str) -> ParentSupport {
        ParentSupport {
            parent: "web-kit".into(),
            version: version.into(),
            declares: "^1.4.0".into(),
            admits,
            latest: latest.into(),
            latest_declares: declares.into(),
            latest_admits: Some(true),
            missing: String::new(),
        }
    }

    #[test]
    fn a_parent_release_that_declares_the_fixed_range_is_preferred_to_an_override() {
        // One direct parent whose newest release moved to the fixed major.
        let p = parent("10.4.15", Some(false), "10.4.22", "^2.0.0");
        let route = parent_route("npm", std::slice::from_ref(&p), true, "1.4.5").unwrap();
        assert_eq!(
            (
                route.package.as_str(),
                route.from.as_str(),
                route.to.as_str()
            ),
            ("web-kit", "10.4.15", "10.4.22")
        );
        // Not when the parent is not in the repository's manifest, when its
        // own release is a major away, when its new range still admits the
        // vulnerable version, or when two parents bring the package in.
        assert!(parent_route("npm", std::slice::from_ref(&p), false, "1.4.5").is_none());
        let far = parent("10.4.15", Some(false), "11.0.0", "^2.0.0");
        assert!(parent_route("npm", &[far], true, "1.4.5").is_none());
        let wide = parent("10.4.15", Some(false), "10.4.22", ">=1.4.0");
        assert!(parent_route("npm", &[wide], true, "1.4.5").is_none());
        assert!(parent_route("npm", &[p.clone(), p], true, "1.4.5").is_none());
    }

    #[test]
    fn the_evidence_for_a_major_on_a_request_path_asks_for_a_probe() {
        let paths = [path(
            "web-kit",
            Declared::Production,
            &["src/uploads/controller.ts"],
        )];
        let used = vec!["src/uploads/controller.ts:14".to_string()];
        let parents = [ParentSupport {
            latest_admits: None,
            ..parent("10.4.15", Some(false), "10.4.15", "")
        }];
        let m = evidence(&MajorIn {
            package: "upload-kit",
            from: "1.4.5",
            to: "2.0.2",
            ecosystem: "npm",
            why_major: "GHSA-1 lists no patched release on the 1 line",
            paths: &paths,
            used_in: &used,
            searched: "`upload-kit` and the APIs its notes name",
            verdict: Verdict::NotReachable,
            parents: &parents,
            ..Default::default()
        });
        assert_eq!(m.class, PathClass::Production);
        assert!(matches!(m.probe, ProbeNeed::Needed { .. }));
        assert_eq!(
            m.route,
            Route::Override {
                selector: "upload-kit@1".into()
            }
        );
        let text = m.section();
        assert!(text.contains("- `upload-kit` 1.4.5 to 2.0.2 (major): runs in production."));
        assert!(text.contains("Path through `web-kit` (production)"));
        // The verdict carries its limit.
        assert!(text.contains("That is not proof of safety"));
        // The override pushes the parent outside what it declares.
        assert!(text.contains(
            "Outside a declared range: `web-kit` 10.4.15 declares `^1.4.0` for it, and the override installs 2.0.2, outside that range."
        ));
        assert!(text.contains("override scoped to its version line (`upload-kit@1`)"));
        assert!(text.contains("Needs a functional probe"));
    }

    #[test]
    fn a_development_tool_or_an_install_helper_needs_no_probe_and_says_why() {
        let dev = [path("lint-kit", Declared::Development, &[".eslintrc.js"])];
        let m = evidence(&MajorIn {
            package: "glob-walk",
            from: "7.2.0",
            to: "9.0.1",
            paths: &dev,
            searched: "`glob-walk`",
            verdict: Verdict::NotLoaded,
            ..Default::default()
        });
        assert_eq!(m.class, PathClass::Development);
        assert_eq!(m.probe, ProbeNeed::NotNeeded);
        let text = m.section();
        assert!(text.contains("runs in development only"));
        assert!(text.contains("Use in this repository: none found (searched `glob-walk`)"));
        assert!(!text.contains("Needs a functional probe"));
        // Two paths, one of them production: production decides.
        let both = [
            path("lint-kit", Declared::Development, &[]),
            path("web-kit", Declared::Production, &["src/main.ts"]),
        ];
        let m = evidence(&MajorIn {
            package: "glob-walk",
            from: "7.2.0",
            to: "9.0.1",
            paths: &both,
            ..Default::default()
        });
        assert_eq!(m.class, PathClass::Production);
        // No path read at all: said as not known, never as safe.
        let none = evidence(&MajorIn {
            package: "x",
            from: "1.0.0",
            to: "2.0.0",
            direct: true,
            ..Default::default()
        });
        assert_eq!(none.class, PathClass::NotKnown);
        assert!(none.section().contains("who depends on it is not known"));
        assert!(none
            .section()
            .contains("runs in a place that could not be worked out"));
    }

    #[test]
    fn a_parent_route_is_stated_and_an_unused_one_is_explained() {
        let via = Companion {
            package: "web-kit".into(),
            from: "10.4.15".into(),
            to: "10.4.22".into(),
            line: "10".into(),
        };
        let parents = [parent("10.4.15", Some(false), "10.4.22", "^2.0.0")];
        let m = evidence(&MajorIn {
            package: "upload-kit",
            from: "1.4.5",
            to: "2.0.2",
            parents: &parents,
            via_parent: Some(&via),
            ..Default::default()
        });
        assert!(m.section().contains(
            "Fixed by raising its parent `web-kit` 10.4.15 to 10.4.22, which declares `^2.0.0`: no override."
        ));
        assert!(m.outside_range.is_empty());
        // The same parent, not taken: the override says why.
        let m = evidence(&MajorIn {
            package: "upload-kit",
            from: "1.4.5",
            to: "2.0.2",
            parents: &parents,
            ..Default::default()
        });
        assert!(m
            .parent_available
            .contains("`web-kit` 10.4.22 declares `^2.0.0`, which admits 2.0.2"));
    }

    #[test]
    fn the_plan_raises_the_parent_only_where_the_manifest_declares_it() {
        use crate::dep_fix::{DepPlan, PlanBump};
        use crate::security_fixes::{Finding, FindingState};
        use crate::upgrade_notes::PackageNotes;
        let bump = |id: &str| PlanBump {
            package: "upload-kit".into(),
            from: "1.4.5".into(),
            to: "2.0.2".into(),
            line: "1".into(),
            manifest: "pnpm-lock.yaml".into(),
            major: true,
            finding_ids: vec![id.into()],
            ..Default::default()
        };
        let notes = vec![PackageNotes {
            package: "upload-kit".into(),
            ecosystem: "npm".into(),
            from: "1.4.5".into(),
            to: "2.0.2".into(),
            major: true,
            parents: vec![parent("10.4.15", Some(false), "10.4.22", "^2.0.0")],
            ..Default::default()
        }];
        let finding = |id: &str, path: &[&str]| {
            (
                Finding {
                    id: id.into(),
                    path: path.iter().map(|s| s.to_string()).collect(),
                    ..Default::default()
                },
                FindingState::default(),
            )
        };
        let findings = vec![
            finding("f-direct", &["web-kit", "upload-kit"]),
            finding("f-deep", &["app-shell", "web-kit", "upload-kit"]),
        ];
        let mut plan = DepPlan {
            bumps: vec![bump("f-direct"), bump("f-deep")],
            deferred: vec![],
        };
        route_through_parents(&mut plan, &notes, &findings);
        // The parent is in the manifest: it is raised, and the apply script
        // is told the parent, never an override of the package.
        let via = plan.bumps[0].via_parent.as_ref().unwrap();
        assert_eq!(
            (via.package.as_str(), via.to.as_str()),
            ("web-kit", "10.4.22")
        );
        let specs = crate::dep_fix::specs(&[&plan.bumps[0]]);
        assert_eq!(specs[0].bumps.len(), 1);
        assert_eq!(specs[0].bumps[0].name, "web-kit");
        // The parent is itself brought in by another package: an override.
        assert!(plan.bumps[1].via_parent.is_none());
        assert_eq!(
            crate::dep_fix::specs(&[&plan.bumps[1]])[0].bumps[0].name,
            "upload-kit"
        );
        // A lockfile where the raised parent left the package behind is
        // found, so the run falls back to the override.
        let lock = "lockfileVersion: '9.0'\nimporters:\n  .:\n    dependencies:\n      web-kit:\n        specifier: ^10.4.22\n        version: 10.4.22\npackages:\n  upload-kit@1.4.5: {}\n  web-kit@10.4.22: {}\nsnapshots:\n  upload-kit@1.4.5: {}\n  web-kit@10.4.22:\n    dependencies:\n      upload-kit: 1.4.5\n";
        let (split, unmoved) = crate::dep_fix::lockfile_violations(
            &[],
            &[&plan.bumps[0]],
            &[("pnpm-lock.yaml".to_string(), lock.to_string())],
        );
        assert!(split.is_empty());
        assert_eq!(unmoved, vec!["upload-kit"]);
        let moved = lock.replace("1.4.5", "2.0.2");
        let (_, unmoved) = crate::dep_fix::lockfile_violations(
            &[],
            &[&plan.bumps[0]],
            &[("pnpm-lock.yaml".to_string(), moved)],
        );
        assert!(unmoved.is_empty());
    }
}
