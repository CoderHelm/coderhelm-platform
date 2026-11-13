use super::*;
use crate::security_fixes::{same_line, Relation, Scope, SecurityConfig};

fn finding(
    id: &str,
    pkg: &str,
    version: &str,
    fixed: &[&str],
    tier: Tier,
    verdict: Verdict,
) -> (Finding, FindingState) {
    let st = FindingState {
        fixed: fixed.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    };
    let f = Finding {
        id: id.into(),
        repo: "acme/shop".into(),
        manifest: "pnpm-lock.yaml".into(),
        ecosystem: "npm".into(),
        package: pkg.into(),
        version: version.into(),
        fixed_version: lowest_fix(
            &Finding {
                version: version.into(),
                ..Default::default()
            },
            &st,
        ),
        tier: Some(tier),
        verdict,
        relation: Relation::Transitive,
        scope: Scope::Runtime,
        status: Status::Open,
        ..Default::default()
    };
    (f, st)
}

fn pairs(v: &[(Finding, FindingState)]) -> Vec<(&Finding, &FindingState)> {
    v.iter().map(|(f, s)| (f, s)).collect()
}

fn cfg() -> SecurityConfig {
    SecurityConfig::default()
}

fn bump<'a>(p: &'a DepPlan, pkg: &str) -> &'a PlanBump {
    p.bumps.iter().find(|b| b.package == pkg).unwrap()
}

// ── The plan ────────────────────────────────────────────────────────────

#[test]
fn a_p3_major_that_is_not_reachable_is_deferred_with_its_reason() {
    let fs = vec![finding(
        "a",
        "graph-server",
        "4.11.3",
        &["5.5.0"],
        Tier::P3,
        Verdict::NotReachable,
    )];
    let p = plan(&pairs(&fs), &cfg());
    assert!(p.bumps.is_empty(), "{:?}", p.bumps);
    assert_eq!(p.deferred.len(), 1);
    let line = p.deferred[0].line();
    assert!(
        line.starts_with("`graph-server` 4.11.3 to 5.5.0 deferred: not reachable"),
        "{line}"
    );
    assert!(line.contains("major version"), "{line}");
    assert!(line.contains("when a person asks"), "{line}");
    // The finding stays out of every bundle, with the reason as its wait.
    let (f, st) = &fs[0];
    assert!(crate::security_fixes::deferred(f, st, &cfg()).is_some());
    let mut asked = st.clone();
    asked.requested_by = Some("dev@acme.test".into());
    assert!(crate::security_fixes::deferred(f, &asked, &cfg()).is_none());
}

#[test]
fn a_p1_or_reachable_major_is_included() {
    let fs = vec![
        finding(
            "a",
            "uploader",
            "1.4.4",
            &["2.3.0"],
            Tier::P1,
            Verdict::Unknown,
        ),
        finding(
            "b",
            "router",
            "10.4.15",
            &["11.1.18"],
            Tier::P3,
            Verdict::Reachable,
        ),
    ];
    let p = plan(&pairs(&fs), &cfg());
    assert!(p.deferred.is_empty(), "{:?}", p.deferred);
    let u = bump(&p, "uploader");
    assert!(u.major);
    assert_eq!(u.risk(), Risk::UrgentMajor);
    assert_eq!(bump(&p, "router").risk(), Risk::UrgentMajor);
}

#[test]
fn the_lowest_fixed_version_on_the_line_is_chosen() {
    // Fixed on 4.x at 4.5.4 and 4.5.5 (two advisories), and on 5.x: the line
    // keeps 4.x at the higher of the two lowest fixes, never "latest".
    let fs = vec![
        finding(
            "a",
            "xml-reader",
            "4.5.3",
            &["4.5.4", "5.3.6"],
            Tier::P2,
            Verdict::NotReachable,
        ),
        finding(
            "b",
            "xml-reader",
            "4.5.3",
            &["4.5.5", "5.5.7"],
            Tier::P3,
            Verdict::NotReachable,
        ),
        // Only a major fixes this one: deferred, the others still go.
        finding(
            "c",
            "xml-reader",
            "4.5.3",
            &["5.12.0"],
            Tier::P3,
            Verdict::NotReachable,
        ),
    ];
    let p = plan(&pairs(&fs), &cfg());
    let b = bump(&p, "xml-reader");
    assert_eq!(
        (b.from.as_str(), b.to.as_str(), b.major),
        ("4.5.3", "4.5.5", false)
    );
    assert_eq!(b.finding_ids, vec!["a", "b"]);
    assert_eq!(b.risk(), Risk::Safe);
    assert_eq!(p.deferred.len(), 1);
    assert_eq!(p.deferred[0].finding_ids, vec!["c"]);
    assert_eq!(p.deferred[0].to, "5.12.0");
}

#[test]
fn each_version_line_of_a_package_is_its_own_bump() {
    let fs = vec![
        finding(
            "a",
            "globber",
            "7.2.3",
            &["7.2.4", "14.0.2"],
            Tier::P2,
            Verdict::Unknown,
        ),
        finding(
            "b",
            "globber",
            "10.4.5",
            &["10.5.0", "14.0.2"],
            Tier::P2,
            Verdict::Unknown,
        ),
    ];
    let p = plan(&pairs(&fs), &cfg());
    let lines: Vec<(&str, &str, &str)> = p
        .bumps
        .iter()
        .map(|b| (b.line.as_str(), b.from.as_str(), b.to.as_str()))
        .collect();
    assert_eq!(
        lines,
        vec![("10", "10.4.5", "10.5.0"), ("7", "7.2.3", "7.2.4")]
    );
    assert!(p.bumps.iter().all(|b| !b.major));
}

#[test]
fn no_fix_on_the_line_and_unknown_reachability_follows_the_major_policy() {
    let fs = vec![finding(
        "a",
        "tiny-tmp",
        "0.0.33",
        &["0.2.6"],
        Tier::P2,
        Verdict::Unknown,
    )];
    let p = plan(&pairs(&fs), &cfg());
    assert_eq!(bump(&p, "tiny-tmp").risk(), Risk::Major);
    let mut issue_only = cfg();
    issue_only.major_bumps = MajorBumps::IssueOnly;
    let p = plan(&pairs(&fs), &issue_only);
    assert!(p.bumps.is_empty() && p.deferred.is_empty());
}

#[test]
fn a_run_deferral_holds_until_the_finding_changes() {
    let (f, mut st) = finding(
        "a",
        "pdf-kit",
        "2.1.0",
        &["2.1.4"],
        Tier::P2,
        Verdict::Unknown,
    );
    let now = chrono::Utc::now();
    st.deferred = Some(RunDeferral::new(
        &f,
        "it breaks typecheck in the sandbox",
        now,
    ));
    let fs = vec![(f.clone(), st.clone())];
    let p = plan(&pairs(&fs), &cfg());
    assert!(p.bumps.is_empty());
    assert!(p.deferred[0].reason.contains("typecheck"));
    // It becomes urgent: the deferral no longer holds.
    let mut urgent_f = f.clone();
    urgent_f.tier = Some(Tier::P1);
    assert!(!st.deferred.as_ref().unwrap().holds_at(&urgent_f, now));
    // A month on, it is tried again.
    assert!(!st
        .deferred
        .as_ref()
        .unwrap()
        .holds_at(&f, now + chrono::Duration::days(RUN_DEFERRAL_DAYS + 1)));
}

#[test]
fn a_yarn_lockfile_takes_one_line_of_a_package() {
    let mut fs = vec![
        finding(
            "a",
            "sock",
            "7.5.10",
            &["7.5.11"],
            Tier::P2,
            Verdict::Unknown,
        ),
        finding(
            "b",
            "sock",
            "8.17.0",
            &["8.20.1"],
            Tier::P1,
            Verdict::Unknown,
        ),
    ];
    for (f, _) in &mut fs {
        f.manifest = "yarn.lock".into();
    }
    let p = plan(&pairs(&fs), &cfg());
    assert_eq!(p.bumps.len(), 1);
    assert_eq!(p.bumps[0].line, "8");
    assert!(p.deferred[0].reason.contains("yarn lockfile"));
}

#[test]
fn majors_past_the_cap_wait_for_the_next_run() {
    let fs: Vec<_> = (0..(MAX_MAJORS_PER_RUN + 3))
        .map(|i| {
            let id = format!("f{i}");
            let pkg = format!("pkg-{i}");
            finding(&id, &pkg, "1.0.0", &["2.0.0"], Tier::P2, Verdict::Unknown)
        })
        .collect();
    let p = plan(&pairs(&fs), &cfg());
    assert_eq!(p.bumps.len(), MAX_MAJORS_PER_RUN);
    assert_eq!(p.deferred.len(), 3);
    assert!(p
        .deferred
        .iter()
        .all(|d| d.later && d.reason.contains("next run")));
}

#[test]
fn findings_read_from_the_sbom_go_through_the_root_lockfile() {
    let locks = vec![
        "pnpm-lock.yaml".to_string(),
        "tools/t2/package-lock.json".to_string(),
    ];
    assert_eq!(
        lockfile_for("sbom", &locks).as_deref(),
        Some("pnpm-lock.yaml")
    );
    assert_eq!(
        lockfile_for("tools/t2/package.json", &locks).as_deref(),
        Some("tools/t2/package-lock.json")
    );
    assert_eq!(
        lockfile_for("apps/web/package.json", &locks).as_deref(),
        Some("pnpm-lock.yaml")
    );
    let two = vec!["package-lock.json".to_string(), "yarn.lock".to_string()];
    assert_eq!(lockfile_for("sbom", &two), None);
    let (mut f, st) = finding("a", "ms", "2.0.0", &["2.1.3"], Tier::P2, Verdict::Unknown);
    f.manifest = "sbom".into();
    let fs = vec![(f, st)];
    let p = plan_in(&pairs(&fs), &cfg(), &two);
    assert!(p.bumps.is_empty());
    assert!(p.deferred[0].reason.contains("lockfile"));
    let p = plan_in(&pairs(&fs), &cfg(), &locks);
    assert_eq!(p.bumps[0].manifest, "pnpm-lock.yaml");
}

#[test]
fn a_dependency_plan_has_no_documentation_task() {
    let fs = vec![
        finding(
            "a",
            "lib-a",
            "1.2.0",
            &["1.2.9"],
            Tier::P2,
            Verdict::Unknown,
        ),
        finding(
            "b",
            "lib-b",
            "3.0.0",
            &["3.0.4"],
            Tier::P1,
            Verdict::Unknown,
        ),
    ];
    let p = plan(&pairs(&fs), &cfg());
    let tasks = p.tasks_md();
    assert_eq!(tasks.lines().count(), 1, "{tasks}");
    assert!(
        tasks.starts_with("- [ ] `pnpm-lock.yaml`: apply 2 bump(s)"),
        "{tasks}"
    );
    assert!(!tasks.to_lowercase().contains("readme"));
    assert!(!core_to_dep_fix(
        "README.md",
        "Fix these security advisories"
    ));
    assert!(!core_to_dep_fix("docs/deps.md", ""));
    assert!(core_to_dep_fix(
        "README.md",
        "Also update README.md with the table"
    ));
    assert!(core_to_dep_fix("package.json", ""));
    assert!(core_to_dep_fix("src/server.ts", ""));
}

// ── The apply script (needs node) ───────────────────────────────────────

fn node() -> bool {
    std::process::Command::new("node")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn run_apply(dir: &std::path::Path, specs: &[ApplySpec]) -> String {
    let script = dir.join("apply.js");
    std::fs::write(&script, APPLY_JS).unwrap();
    let arg = b64(serde_json::to_string(specs).unwrap().as_bytes());
    let out = std::process::Command::new("node")
        .arg(&script)
        .arg(arg)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("dep-fix-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn json_at(p: &std::path::Path) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap()
}

fn ab(name: &str, line: &str, to: &str) -> ApplyBump {
    ApplyBump {
        name: name.into(),
        line: line.into(),
        to: to.into(),
    }
}

#[test]
fn overrides_for_transitive_packages_in_pnpm_npm_and_yarn() {
    if !node() {
        eprintln!("node is not installed: the apply script is not run");
        return;
    }
    for (manager, lock, table, key) in [
        (
            Manager::Pnpm,
            "pnpm-lock.yaml",
            "/pnpm/overrides",
            "tiny-tmp@0.0",
        ),
        (
            Manager::Npm,
            "package-lock.json",
            "/overrides",
            "tiny-tmp@0.0",
        ),
        (Manager::Yarn, "yarn.lock", "/resolutions", "tiny-tmp"),
    ] {
        let d = tmp(manager.as_str());
        std::fs::write(
            d.join("package.json"),
            "{\n    \"name\": \"shop\",\n    \"dependencies\": {\n        \"zod\": \"^3.24.2\",\n        \"local\": \"workspace:*\"\n    }\n}\n",
        )
        .unwrap();
        std::fs::write(d.join(lock), "").unwrap();
        // A workspace member declares a direct dependency on another line.
        std::fs::create_dir_all(d.join("apps/web")).unwrap();
        std::fs::write(
            d.join("apps/web/package.json"),
            "{\n  \"devDependencies\": {\n    \"zod\": \"~3.22.0\",\n    \"vite\": \"7.1.9\"\n  }\n}\n",
        )
        .unwrap();
        // A folder with its own lockfile is another project: untouched.
        std::fs::create_dir_all(d.join("tools/t1")).unwrap();
        std::fs::write(d.join("tools/t1/package-lock.json"), "").unwrap();
        let other = "{\n  \"dependencies\": {\n    \"zod\": \"^3.20.0\"\n  }\n}\n";
        std::fs::write(d.join("tools/t1/package.json"), other).unwrap();
        let out = run_apply(
            &d,
            &[ApplySpec {
                manager,
                root: String::new(),
                bumps: vec![
                    ab("zod", "3", "3.25.76"),
                    ab("vite", "7", "7.1.11"),
                    ab("tiny-tmp", "0.0", "0.0.34"),
                ],
            }],
        );
        assert!(out.contains("CODERHELM_APPLY"), "{out}");
        let root = json_at(&d.join("package.json"));
        assert_eq!(root["dependencies"]["zod"], "^3.25.76", "{manager:?}");
        assert_eq!(root["dependencies"]["local"], "workspace:*");
        assert_eq!(
            root.pointer(table).unwrap()[key],
            "^0.0.34",
            "{manager:?} {root}"
        );
        let web = json_at(&d.join("apps/web/package.json"));
        assert_eq!(web["devDependencies"]["zod"], "~3.25.76");
        assert_eq!(web["devDependencies"]["vite"], "7.1.11");
        assert_eq!(
            std::fs::read_to_string(d.join("tools/t1/package.json")).unwrap(),
            other
        );
        // The indent and the trailing newline are kept.
        let text = std::fs::read_to_string(d.join("package.json")).unwrap();
        assert!(
            text.contains("\n    \"dependencies\"") && text.ends_with("}\n"),
            "{text}"
        );
        let _ = std::fs::remove_dir_all(&d);
    }
}

#[test]
fn an_override_already_on_the_line_is_raised_not_doubled() {
    if !node() {
        return;
    }
    let d = tmp("raise-override");
    std::fs::write(
        d.join("package.json"),
        "{\n  \"pnpm\": {\n    \"overrides\": {\n      \"sock\": \"^7.5.9\"\n    }\n  }\n}\n",
    )
    .unwrap();
    std::fs::write(d.join("pnpm-lock.yaml"), "").unwrap();
    run_apply(
        &d,
        &[ApplySpec {
            manager: Manager::Pnpm,
            root: String::new(),
            bumps: vec![ab("sock", "7", "7.5.11")],
        }],
    );
    let root = json_at(&d.join("package.json"));
    assert_eq!(
        root["pnpm"]["overrides"],
        serde_json::json!({"sock": "^7.5.11"})
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn a_probe_applies_writes_the_lockfile_then_checks_and_always_hands_back() {
    let b = PlanBump {
        package: "lib-a".into(),
        from: "1.0.0".into(),
        to: "1.0.3".into(),
        line: "1".into(),
        manifest: "pnpm-lock.yaml".into(),
        ..Default::default()
    };
    let sp = specs(&[&b]);
    let locks = vec![
        (
            "pnpm-lock.yaml".to_string(),
            "pnpm install --lockfile-only --ignore-scripts".to_string(),
        ),
        (
            "tools/t1/package-lock.json".to_string(),
            "cd 'tools/t1' && npm install --package-lock-only".to_string(),
        ),
    ];
    let cmd = probe_command(&sp, &locks, "pnpm run check-types");
    assert!(cmd.contains("node /tmp/coderhelm-apply.js"));
    assert!(cmd.contains("pnpm install --lockfile-only"));
    // A lockfile no bump touched is not written.
    assert!(!cmd.contains("npm install --package-lock-only"));
    let apply_at = cmd.find("coderhelm-apply.js").unwrap();
    let lock_at = cmd.find("--lockfile-only").unwrap();
    let checks_at = cmd.find("check-types").unwrap();
    assert!(apply_at < lock_at && lock_at < checks_at);
    assert!(cmd.trim_end().ends_with("true"));
    assert_eq!(
        probe_marks("x\nCODERHELM_DEP apply=0 lock=1\nCHANGED:"),
        Some((0, 1))
    );
    assert_eq!(probe_marks("nothing"), None);
    assert_eq!(
        apply_skipped("x\nCODERHELM_APPLY [{\"name\":\"a\",\"how\":\"raised\"},{\"name\":\"b\",\"how\":\"skipped\"}]\n"),
        vec!["b".to_string()]
    );
    assert!(apply_skipped("no line").is_empty());
    assert!(commit_path("apps/web/package.json") && commit_path("pnpm-lock.yaml"));
    assert!(!commit_path("apps/web/next-env.d.ts") && !commit_path("node_modules/x/package.json"));
}

// ── Verifying: a fake sandbox ───────────────────────────────────────────

/// A sandbox whose checks fail when any of `breaks` (bump package, check)
/// is applied, and where `base_fails` fail on the base whatever is applied.
struct FakeSandbox {
    breaks: Vec<(String, String)>,
    base_fails: Vec<String>,
    /// Packages whose call sites the coder can fix.
    coder_fixes: Vec<String>,
    probes: u32,
    commits: u32,
    /// What the runtime proof runs (`None`: the repository names no build
    /// and no start command).
    proof_plan: Option<rp::ProofPlan>,
    /// Packages the application's build fails with.
    build_breaks: Vec<String>,
    /// The build fails on the base branch too.
    base_build_fails: bool,
    proof_runs: u32,
}

const CHECKS: [&str; 4] = ["install", "lint", "typecheck", "tests"];

impl FakeSandbox {
    fn result(&self, applied: &[&PlanBump]) -> ProbeResult {
        let checks = CHECKS
            .iter()
            .map(|c| {
                let broken = self.base_fails.iter().any(|b| b == c)
                    || applied.iter().any(|b| {
                        self.breaks.iter().any(|(p, k)| p == &b.package && k == c)
                            && !self.coder_fixes.contains(&format!("fixed:{}", b.package))
                    });
                (c.to_string(), !broken)
            })
            .collect();
        ProbeResult {
            ran: true,
            applied: true,
            checks,
            tail: "error TS2345: Argument of type 'Opts' is not assignable".into(),
        }
    }

    /// Drives a run to the end, as the worker does.
    fn drive(&mut self, run: &mut DepRun) {
        for _ in 0..500 {
            match run.next_action() {
                Action::Base => {
                    self.probes += 1;
                    let r = self.result(&[]);
                    run.on_base(&r);
                }
                Action::Probe(groups) => {
                    let results: Vec<(usize, ProbeResult)> = groups
                        .iter()
                        .map(|&g| {
                            self.probes += 1;
                            let mut on: Vec<&PlanBump> = run.applied_bumps();
                            on.extend(run.groups[g].bumps.iter().map(|&i| &run.plan.bumps[i]));
                            (g, self.result(&on))
                        })
                        .collect();
                    run.on_probes(&results);
                }
                Action::Commit(g) => {
                    self.commits += 1;
                    let head = format!("sha{}", self.commits);
                    run.on_commit(g, &head);
                }
                Action::CodeFix { bump, .. } => {
                    let pkg = run.plan.bumps[bump].package.clone();
                    let r = if self.coder_fixes.contains(&pkg) {
                        self.coder_fixes.push(format!("fixed:{pkg}"));
                        CodeFixed::Green {
                            head: "sha-code".into(),
                        }
                    } else {
                        CodeFixed::Failed {
                            checks: vec!["typecheck".into()],
                            error: "error TS2339: Property 'listen' does not exist".into(),
                        }
                    };
                    run.on_code_fix(bump, r, 40_000, 0.6);
                }
                Action::ProofPlan => run.on_proof_plan(self.proof_plan.clone().unwrap_or_default()),
                Action::Proof { base } => {
                    self.proof_runs += 1;
                    let broken = self.base_build_fails
                        || (!base
                            && run
                                .applied_bumps()
                                .iter()
                                .any(|b| self.build_breaks.contains(&b.package)));
                    let plan = run.proof.plan.clone().unwrap();
                    run.on_proof(base, proof_result(&plan, broken));
                }
                Action::ProofTakeOut { why } => {
                    let out = run.take_out_last_commit(&why).expect("a set to take out");
                    self.commits += 1;
                    run.on_proof_taken_out(&out, &format!("sha{}", self.commits));
                }
                Action::Done => return,
            }
        }
        panic!("the run never ended");
    }
}

fn sandbox(breaks: &[(&str, &str)]) -> FakeSandbox {
    FakeSandbox {
        breaks: breaks
            .iter()
            .map(|(p, c)| (p.to_string(), c.to_string()))
            .collect(),
        base_fails: vec![],
        coder_fixes: vec![],
        probes: 0,
        commits: 0,
        proof_plan: None,
        build_breaks: vec![],
        base_build_fails: false,
        proof_runs: 0,
    }
}

/// A repository whose CI builds with `pnpm build` and that declares a start
/// script.
fn web_proof_plan() -> rp::ProofPlan {
    rp::discover(
        "pnpm-lock.yaml",
        &[rp::CiBuild {
            cmd: "pnpm build".into(),
            dir: None,
            workflow: "ci.yml".into(),
            job: "build".into(),
            on_pull_request: true,
        }],
        &[(
            "package.json".to_string(),
            r#"{"scripts":{"build":"next build","start":"next start"}}"#.to_string(),
        )],
    )
}

/// What the proof script prints: a failed build, or a build, a start and
/// its pages.
fn proof_result(plan: &rp::ProofPlan, build_fails: bool) -> rp::ProofResult {
    let out = if build_fails {
        "Module not found: Can't resolve 'lib/legacy'\nCODERHELM_PROOF_STEP build 1 1\nCODERHELM_PROOF install=0 start=skipped port=0\n"
    } else {
        "CODERHELM_PROOF_STEP build 1 0\nCODERHELM_PROOF_PAGE / 200\nCODERHELM_PROOF_PAGE /healthz 404\nCODERHELM_PROOF_PAGE /health 404\nCODERHELM_PROOF_PAGE /api/health 200\nCODERHELM_PROOF install=0 start=up port=38411\n"
    };
    rp::parse(out, plan)
}

fn ten_bumps() -> DepPlan {
    let fs: Vec<_> = (0..10)
        .map(|i| {
            let id = format!("f{i}");
            let pkg = format!("lib-{i:02}");
            finding(&id, &pkg, "2.1.0", &["2.1.5"], Tier::P2, Verdict::Unknown)
        })
        .collect();
    plan(&pairs(&fs), &cfg())
}

#[test]
fn bisect_finds_the_one_bump_of_ten_that_breaks_typecheck() {
    let p = ten_bumps();
    assert_eq!(p.batches().len(), 1);
    let mut run = DepRun::new(p, "base", 3.0);
    let mut sb = sandbox(&[("lib-06", "typecheck")]);
    sb.drive(&mut run);
    let applied: Vec<&str> = run
        .applied_bumps()
        .iter()
        .map(|b| b.package.as_str())
        .collect();
    assert_eq!(applied.len(), 9, "{applied:?}");
    assert!(!applied.contains(&"lib-06"));
    assert_eq!(run.deferred.len(), 1);
    assert_eq!(run.deferred[0].package, "lib-06");
    assert!(
        run.deferred[0].reason.contains("typecheck"),
        "{}",
        run.deferred[0].reason
    );
    // What ships is green: the last probe passed every check.
    assert!(
        run.last_checks.iter().all(|(_, ok)| *ok),
        "{:?}",
        run.last_checks
    );
    // Halving, not one probe per bump.
    assert!(run.probes <= 10, "{} probes", run.probes);
    let report = run.report_md();
    assert!(report.contains("**Applied (9):**"), "{report}");
    assert!(
        report.contains("`lib-06` 2.1.0 to 2.1.5 deferred: it breaks typecheck"),
        "{report}"
    );
    assert!(
        report.contains("- typecheck: passes with these bumps"),
        "{report}"
    );
}

#[test]
fn a_check_that_fails_on_the_base_is_noted_not_blamed() {
    let mut run = DepRun::new(ten_bumps(), "base", 3.0);
    let mut sb = sandbox(&[]);
    sb.base_fails = vec!["tests".into()];
    sb.drive(&mut run);
    assert_eq!(run.applied.len(), 10);
    assert!(run.deferred.is_empty());
    // Everything green but the base's own failure: one probe for the base,
    // one for the batch.
    assert_eq!(run.probes, 2);
    let report = run.report_md();
    assert!(
        report.contains(
            "- tests: fails on the base branch too (not caused by this change; not fixed here)"
        ),
        "{report}"
    );
}

#[test]
fn two_breaking_bumps_are_both_found() {
    let mut run = DepRun::new(ten_bumps(), "base", 3.0);
    let mut sb = sandbox(&[("lib-01", "tests"), ("lib-08", "lint")]);
    sb.drive(&mut run);
    let mut gone: Vec<&str> = run.deferred.iter().map(|d| d.package.as_str()).collect();
    gone.sort();
    assert_eq!(gone, vec!["lib-01", "lib-08"]);
    assert_eq!(run.applied.len(), 8);
    assert!(run.last_checks.iter().all(|(_, ok)| *ok));
}

#[test]
fn an_urgent_major_gets_one_coder_pass_and_a_failed_one_is_deferred() {
    let fs = vec![
        finding(
            "a",
            "lib-a",
            "1.2.0",
            &["1.2.9"],
            Tier::P2,
            Verdict::Unknown,
        ),
        finding(
            "b",
            "gql-kit",
            "4.1.0",
            &["5.0.2"],
            Tier::P1,
            Verdict::Reachable,
        ),
        finding(
            "c",
            "web-core",
            "10.4.0",
            &["11.1.0"],
            Tier::P0,
            Verdict::Unknown,
        ),
    ];
    let mut run = DepRun::new(plan(&pairs(&fs), &cfg()), "base", 3.0);
    let mut sb = sandbox(&[("gql-kit", "typecheck"), ("web-core", "typecheck")]);
    sb.coder_fixes = vec!["web-core".into()];
    sb.drive(&mut run);
    let applied: Vec<&str> = run
        .applied_bumps()
        .iter()
        .map(|b| b.package.as_str())
        .collect();
    assert!(
        applied.contains(&"lib-a") && applied.contains(&"web-core"),
        "{applied:?}"
    );
    assert_eq!(run.deferred.len(), 1);
    assert!(run.deferred[0]
        .reason
        .contains("coder's change to its call sites did not make them pass"));
    assert_eq!(run.code_fixes, 2);
    assert!((run.spent_usd - 1.2).abs() < 1e-9);
    let report = run.report_md();
    assert!(report.contains("40000 tokens, $0.60"), "{report}");
}

#[test]
fn a_budget_stop_leaves_a_checkpointed_resumable_run() {
    let fs = vec![
        finding(
            "a",
            "lib-a",
            "1.2.0",
            &["1.2.9"],
            Tier::P2,
            Verdict::Unknown,
        ),
        finding(
            "b",
            "gql-kit",
            "4.1.0",
            &["5.0.2"],
            Tier::P1,
            Verdict::Unknown,
        ),
    ];
    let mut run = DepRun::new(plan(&pairs(&fs), &cfg()), "base", 0.5);
    run.spent_usd = 0.5; // the triage and earlier steps used it
    let mut sb = sandbox(&[("gql-kit", "typecheck")]);
    sb.coder_fixes = vec!["gql-kit".into()];
    sb.drive(&mut run);
    assert!(run.budget_stop);
    // One probe for the base, one per batch: the stopped major is not
    // probed again and again.
    assert_eq!(run.probes, 3);
    assert_eq!(run.applied_bumps().len(), 1);
    assert!(run.deferred[0].later && run.deferred[0].reason.contains("budget"));
    // The checkpoint is plain JSON; a retry with more budget picks up only
    // what the budget left, and keeps what is applied.
    let saved = serde_json::to_string(&run).unwrap();
    let mut back: DepRun = serde_json::from_str(&saved).unwrap();
    assert_eq!(back, run);
    back.resume_with_budget(3.0);
    assert!(!back.done && back.deferred.is_empty());
    let probes_before = back.probes;
    sb.drive(&mut back);
    let applied: Vec<&str> = back
        .applied_bumps()
        .iter()
        .map(|b| b.package.as_str())
        .collect();
    assert_eq!(applied, vec!["lib-a", "gql-kit"]);
    // The base was not probed again.
    assert!(back.probes > probes_before);
    assert_eq!(sb.probes, run.probes + (back.probes - probes_before));
}

#[test]
fn a_stalled_run_finishes_with_what_is_green_and_never_asks() {
    // The two stuck runs, together: the plan had a README task that never
    // moved, and a bump that broke typecheck on every attempt. The new plan
    // has no README task, the bump is deferred with its error, and the run
    // ends Done with the rest applied and green.
    let mut fs: Vec<_> = (0..6)
        .map(|i| {
            let id = format!("f{i}");
            let pkg = format!("lib-{i:02}");
            finding(&id, &pkg, "3.0.0", &["3.0.7"], Tier::P2, Verdict::Unknown)
        })
        .collect();
    fs.push(finding(
        "m",
        "server-kit",
        "4.0.0",
        &["5.1.0"],
        Tier::P2,
        Verdict::Unknown,
    ));
    let p = plan(&pairs(&fs), &cfg());
    assert!(!p.tasks_md().to_lowercase().contains("readme"));
    let mut run = DepRun::new(p, "base", 3.0);
    let mut sb = sandbox(&[("server-kit", "typecheck")]);
    sb.drive(&mut run);
    assert!(run.done);
    assert_eq!(run.applied.len(), 6);
    assert_eq!(run.deferred.len(), 1);
    assert!(run.deferred[0].reason.contains("typecheck"));
    assert!(!run.budget_stop);
    assert!(run.last_checks.iter().all(|(_, ok)| *ok));
}

#[test]
fn without_a_local_signal_only_same_line_fixes_go_and_majors_wait() {
    let fs = vec![
        finding(
            "a",
            "lib-a",
            "1.2.0",
            &["1.2.9"],
            Tier::P2,
            Verdict::Unknown,
        ),
        finding(
            "b",
            "gql-kit",
            "4.1.0",
            &["5.0.2"],
            Tier::P1,
            Verdict::Unknown,
        ),
    ];
    let mut run = DepRun::new(plan(&pairs(&fs), &cfg()), "base", 3.0);
    let mut sb = sandbox(&[]);
    sb.base_fails = vec!["install".into()];
    sb.drive(&mut run);
    assert!(run.unverified);
    assert_eq!(run.applied_bumps().len(), 1);
    assert_eq!(run.applied_bumps()[0].package, "lib-a");
    assert!(run.deferred[0]
        .reason
        .contains("could not run the repository's checks"));
    assert!(run
        .report_md()
        .contains("CI on this pull request is the check"));
}

#[test]
fn a_version_that_cannot_be_installed_is_deferred_without_the_coder() {
    let fs = vec![
        finding(
            "a",
            "lib-a",
            "1.2.0",
            &["1.2.9"],
            Tier::P2,
            Verdict::Unknown,
        ),
        finding(
            "b",
            "gql-kit",
            "4.1.0",
            &["5.0.2"],
            Tier::P1,
            Verdict::Unknown,
        ),
    ];
    let mut run = DepRun::new(plan(&pairs(&fs), &cfg()), "base", 3.0);
    assert_eq!(run.next_action(), Action::Base);
    run.on_base(&FakeSandbox::result(&sandbox(&[]), &[]));
    // Batch 1: lib-a green.
    let Action::Probe(g) = run.next_action() else {
        panic!()
    };
    run.on_probes(&[(g[0], FakeSandbox::result(&sandbox(&[]), &[]))]);
    assert_eq!(run.next_action(), Action::Commit(0));
    run.on_commit(0, "sha1");
    // Batch 2: gql-kit can't be resolved.
    let Action::Probe(g) = run.next_action() else {
        panic!()
    };
    run.on_probes(&[(
        g[0],
        ProbeResult {
            ran: true,
            applied: false,
            checks: vec![],
            tail: "ERR_PNPM_NO_MATCHING_VERSION No matching version found for gql-kit@^5.0.2"
                .into(),
        },
    )]);
    // The batches are settled: the runtime proof, here with nothing to run.
    assert_eq!(run.next_action(), Action::ProofPlan);
    run.on_proof_plan(rp::ProofPlan::default());
    assert_eq!(run.next_action(), Action::Done);
    assert_eq!(run.code_fixes, 0);
    assert_eq!(run.deferred[0].cause, Cause::Parent);
    assert!(run.deferred[0].reason.contains("could not be installed"));
    assert!(run.deferred[0]
        .reason
        .contains("ERR_PNPM_NO_MATCHING_VERSION"));
}

#[test]
fn a_sandbox_without_a_result_never_blames_a_bump() {
    let mut run = DepRun::new(ten_bumps(), "base", 3.0);
    run.on_base(&FakeSandbox::result(&sandbox(&[]), &[]));
    for _ in 0..MAX_BROKEN_PROBES {
        let Action::Probe(g) = run.next_action() else {
            panic!()
        };
        run.on_probes(&[(g[0], ProbeResult::default())]);
    }
    assert_eq!(run.next_action(), Action::Done);
    assert_eq!(run.deferred.len(), 10);
    assert!(run
        .deferred
        .iter()
        .all(|d| d.later && d.reason.contains("no result")));
    assert!(run.dropped.is_empty());
}

// ── The two stuck runs, replayed (names invented) ───────────────────────

#[derive(serde::Deserialize)]
struct Row {
    id: String,
    package: String,
    version: String,
    fixed_version: Option<String>,
    fixed: Vec<String>,
    tier: String,
    verdict: Verdict,
    relation: Relation,
    scope: Scope,
    manifest: String,
}

fn replay(name: &str) -> Vec<(Finding, FindingState)> {
    let all: std::collections::BTreeMap<String, Vec<Row>> =
        serde_json::from_str(include_str!("../security_testdata/dep_fix_replay.json")).unwrap();
    all[name]
        .iter()
        .map(|r| {
            (
                Finding {
                    id: r.id.clone(),
                    repo: format!("acme/{name}"),
                    manifest: r.manifest.clone(),
                    ecosystem: "npm".into(),
                    package: r.package.clone(),
                    version: r.version.clone(),
                    fixed_version: r.fixed_version.clone(),
                    tier: Tier::parse(&r.tier),
                    verdict: r.verdict,
                    relation: r.relation,
                    scope: r.scope,
                    ..Default::default()
                },
                FindingState {
                    fixed: r.fixed.clone(),
                    ..Default::default()
                },
            )
        })
        .collect()
}

/// The old plan's bumps: one per package, to the highest fixed version any
/// advisory needs (what `add_bump` gave the coder).
fn old_bumps(fs: &[(Finding, FindingState)]) -> Vec<(String, String, String, bool)> {
    let mut by: BTreeMap<String, (String, String)> = BTreeMap::new();
    for (f, _) in fs {
        let to = f.fixed_version.clone().unwrap_or_default();
        let e = by
            .entry(f.package.to_ascii_lowercase())
            .or_insert((f.version.clone(), to.clone()));
        if compare_versions(&to, &e.1) == std::cmp::Ordering::Greater {
            e.1 = to;
        }
    }
    by.into_iter()
        .map(|(p, (from, to))| {
            let major = is_major_bump(&from, &to);
            (p, from, to, major)
        })
        .collect()
}

fn show(name: &str, locks: &[&str]) -> DepPlan {
    let fs = replay(name);
    let old = old_bumps(&fs);
    let locks: Vec<String> = locks.iter().map(|s| s.to_string()).collect();
    let p = plan_in(&pairs(&fs), &cfg(), &locks);
    let mut text = format!(
        "\n=== {name}: {} findings\nold plan: {} package bumps, {} major\nnew plan: {} bumps ({} major) in {} batches, {} deferred\n",
        fs.len(),
        old.len(),
        old.iter().filter(|b| b.3).count(),
        p.bumps.len(),
        p.bumps.iter().filter(|b| b.major).count(),
        p.batches().len(),
        p.deferred.len()
    );
    for (risk, ids) in p.batches() {
        text.push_str(&format!(
            "  batch: {} ({} bumps)\n",
            risk.label(),
            ids.len()
        ));
        for &i in ids.iter().take(4) {
            let b = &p.bumps[i];
            text.push_str(&format!(
                "    `{}` {} to {} ({})\n",
                b.package, b.from, b.to, b.why
            ));
        }
    }
    text.push_str("  deferred:\n");
    for d in p.deferred.iter().take(8) {
        text.push_str(&format!("    {}\n", d.line()));
    }
    text.push_str("  tasks:\n");
    text.push_str(&p.tasks_md());
    println!("{text}");
    p
}

#[test]
fn the_web_monorepo_run_replayed() {
    let p = show(
        "web-monorepo",
        &[
            "pnpm-lock.yaml",
            "tools/t2/package-lock.json",
            "tools/t3/package-lock.json",
            "tools/t5/pnpm-lock.yaml",
        ],
    );
    let fs = replay("web-monorepo");
    let old = old_bumps(&fs);
    assert!(old.iter().filter(|b| b.3).count() >= 20, "old majors");
    // Far fewer majors, none of them for a finding that isn't reachable.
    let majors: Vec<&PlanBump> = p.bumps.iter().filter(|b| b.major).collect();
    assert!(majors.len() <= MAX_MAJORS_PER_RUN, "{}", majors.len());
    let by_id: std::collections::HashMap<&str, &Finding> =
        fs.iter().map(|(f, _)| (f.id.as_str(), f)).collect();
    for b in &majors {
        assert!(b.finding_ids.iter().any(|id| {
            let f = by_id[id.as_str()];
            urgent(f) || f.verdict == Verdict::Unknown
        }));
    }
    for d in p.deferred.iter().filter(|d| !d.later) {
        for id in &d.finding_ids {
            let f = by_id[id.as_str()];
            assert!(!urgent(f), "an urgent finding was deferred: {}", d.line());
        }
    }
    // Every P0/P1 or reachable finding is in a bump or deferred only for a
    // cap or a lockfile it can't be placed in, never by the rules.
    let in_bumps: std::collections::HashSet<&str> = p
        .bumps
        .iter()
        .flat_map(|b| b.finding_ids.iter().map(String::as_str))
        .collect();
    for (f, _) in &fs {
        if urgent(f) && !in_bumps.contains(f.id.as_str()) {
            let d = p
                .deferred
                .iter()
                .find(|d| d.finding_ids.contains(&f.id))
                .unwrap();
            assert!(
                d.later || d.reason.contains("lockfile") || d.reason.contains("yarn"),
                "{}",
                d.line()
            );
        }
    }
    // The tasks are the lockfiles, never the README.
    assert!(!p.tasks_md().to_lowercase().contains("readme"));
    assert!(p.tasks_md().lines().count() <= 4);
}

#[test]
fn the_api_service_run_replayed() {
    let p = show("api-service", &["pnpm-lock.yaml"]);
    let fs = replay("api-service");
    let old = old_bumps(&fs);
    // The old run bumped the server framework, its GraphQL server and
    // GraphQL helpers across majors though none was reachable.
    assert!(old.iter().filter(|b| b.3).count() >= 15);
    let by_id: std::collections::HashMap<&str, &Finding> =
        fs.iter().map(|(f, _)| (f.id.as_str(), f)).collect();
    let deferred_majors: Vec<&Deferral> = p
        .deferred
        .iter()
        .filter(|d| d.reason.contains("not reachable"))
        .collect();
    assert!(deferred_majors.len() >= 8, "{}", deferred_majors.len());
    for d in &deferred_majors {
        assert!(d
            .finding_ids
            .iter()
            .all(|id| by_id[id.as_str()].verdict == Verdict::NotReachable
                || by_id[id.as_str()].verdict == Verdict::NotLoaded));
    }
    // Every bump in the plan that isn't urgent stays on its line.
    for b in p.bumps.iter().filter(|b| matches!(b.risk(), Risk::Safe)) {
        assert!(
            same_line(&b.from, &b.to),
            "{} {} {}",
            b.package,
            b.from,
            b.to
        );
    }
}

// ── Through the bundle planner (these fail on the code before) ──────────

fn proof_finding(
    id: &str,
    pkg: &str,
    v: &str,
    to: &str,
    fixed: &[&str],
    tier: Tier,
    verdict: Verdict,
) -> (Finding, FindingState) {
    (
        Finding {
            id: id.into(),
            repo: "acme/shop".into(),
            manifest: "pnpm-lock.yaml".into(),
            ecosystem: "npm".into(),
            package: pkg.into(),
            version: v.into(),
            fixed_version: Some(to.into()),
            tier: Some(tier),
            verdict,
            status: Status::Open,
            ..Default::default()
        },
        FindingState {
            fixed: fixed.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        },
    )
}

#[test]
fn proof_a_bundle_leaves_out_a_major_that_is_not_reachable() {
    let fs = vec![
        proof_finding(
            "keep",
            "lib-a",
            "1.2.0",
            "1.2.9",
            &["1.2.9"],
            Tier::P2,
            Verdict::Unknown,
        ),
        proof_finding(
            "defer",
            "graph-server",
            "4.11.3",
            "5.5.0",
            &["5.5.0"],
            Tier::P3,
            Verdict::NotReachable,
        ),
    ];
    let cfg = SecurityConfig::default();
    let units = crate::security_fixes::plan_units(&fs, &cfg, chrono::Utc::now());
    let ids: Vec<&String> = units.iter().flat_map(|u| u.finding_ids.iter()).collect();
    assert!(ids.contains(&&"keep".to_string()), "{ids:?}");
    assert!(
        !ids.contains(&&"defer".to_string()),
        "a P3 not-reachable major went into a pull request: {ids:?}"
    );
}

#[test]
fn proof_a_fix_ticket_never_asks_for_documentation() {
    let fs = vec![proof_finding(
        "keep",
        "lib-a",
        "1.2.0",
        "1.2.9",
        &["1.2.9"],
        Tier::P2,
        Verdict::Unknown,
    )];
    let cfg = SecurityConfig::default();
    let units = crate::security_fixes::plan_units(&fs, &cfg, chrono::Utc::now());
    let members: Vec<&Finding> = fs.iter().map(|(f, _)| f).collect();
    let body = crate::security_fixes::ticket_body(&units[0], &members, &cfg);
    assert!(body.contains("Never edit documentation"), "{body}");
}

/// The pull request's own CI stays red after the fixes: the newest commit
/// of verified bumps comes out and the earlier ones stay, one at a time,
/// and the pull request is never emptied.
#[test]
fn red_ci_takes_out_the_newest_commit_of_bumps_and_keeps_the_rest() {
    let mut run = DepRun::new(ten_bumps(), "base", 3.0);
    for (bumps, head) in [(vec![0, 1, 2], "h1"), (vec![3, 4], "h2"), (vec![5], "h3")] {
        run.groups = vec![Group {
            bumps,
            green: true,
            ..Default::default()
        }];
        run.on_commit(0, head);
    }
    assert_eq!(run.commits.len(), 3);
    assert_eq!(run.applied, vec![0, 1, 2, 3, 4, 5]);
    // A green set with nothing to write is no commit of its own.
    run.groups = vec![Group {
        bumps: vec![6],
        green: true,
        ..Default::default()
    }];
    run.on_commit(0, "h3");
    assert_eq!(run.commits.len(), 3);

    let why = ci_deferral_reason("npm error ETARGET No matching version found for lib-05@2.1.5");
    let out = run.take_out_last_commit(&why).expect("three commits");
    assert_eq!(out.restore_to, "h2");
    assert_eq!(out.lines.len(), 1);
    assert!(out.lines[0].starts_with("`lib-05` 2.1.0 to 2.1.5 deferred: the pull request's CI failed with it applied (npm error ETARGET"), "{}", out.lines[0]);
    assert_eq!(run.applied, vec![0, 1, 2, 3, 4, 6]);
    assert_eq!(run.head, "h2");
    // It broke the checks: it doesn't come back by itself.
    let d = run.deferred.last().unwrap();
    assert_eq!(d.package, "lib-05");
    assert!(!d.later);
    assert!(run.report_md().contains("lib-05"), "{}", run.report_md());

    // Still red: the one before it. Then one commit is left, and it stays.
    let out = run.take_out_last_commit(&why).expect("two commits");
    assert_eq!(out.restore_to, "h1");
    assert_eq!(out.lines.len(), 2);
    assert_eq!(run.applied, vec![0, 1, 2, 6]);
    assert_eq!(run.take_out_last_commit(&why), None);
    assert_eq!(run.commits.len(), 1);
    assert_eq!(run.applied, vec![0, 1, 2, 6]);

    // A saved run from before commits were kept reads with none, and
    // nothing is taken out of it.
    let mut old: DepRun = serde_json::from_str("{\"applied\":[0,1]}").unwrap();
    assert!(old.commits.is_empty());
    assert_eq!(old.take_out_last_commit(&why), None);
    assert!(ci_deferral_reason("  ").contains("CI failed with it applied;"));
}

// ── Version choice: the patched release inside the major ────────────────

fn advised(
    id: &str,
    advisory: &str,
    pkg: &str,
    version: &str,
    fixed: &[&str],
) -> (Finding, FindingState) {
    let (mut f, st) = finding(id, pkg, version, fixed, Tier::P2, Verdict::Unknown);
    f.advisory.primary = advisory.into();
    (f, st)
}

#[test]
fn a_patched_release_inside_the_major_is_chosen_over_a_newer_major() {
    // The advisory lists a patched release on the installed line and on the
    // next major: the plan stays on the line, with the lowest one.
    let fs = vec![
        advised(
            "a",
            "GHSA-aaaa",
            "glob-walk",
            "7.2.0",
            &["8.1.1", "7.5.9", "7.5.4"],
        ),
        advised("b", "GHSA-bbbb", "yaml-lite", "2.3.0", &["4.0.1", "2.8.3"]),
    ];
    let p = plan(&pairs(&fs), &cfg());
    assert!(p.deferred.is_empty());
    let walk = bump(&p, "glob-walk");
    assert_eq!((walk.to.as_str(), walk.major), ("7.5.4", false));
    assert_eq!(walk.major_reason, "");
    let yaml = bump(&p, "yaml-lite");
    assert_eq!((yaml.to.as_str(), yaml.major), ("2.8.3", false));
    // The override the sandbox writes is scoped to the line and stays on it.
    let all: Vec<&PlanBump> = p.bumps.iter().collect();
    let spec = &specs(&all)[0];
    assert_eq!(
        spec.bumps,
        vec![
            ApplyBump {
                name: "glob-walk".into(),
                line: "7".into(),
                to: "7.5.4".into()
            },
            ApplyBump {
                name: "yaml-lite".into(),
                line: "2".into(),
                to: "2.8.3".into()
            },
        ]
    );
    for b in &spec.bumps {
        assert_eq!(version_line(&b.to), b.line, "{} leaves its line", b.name);
    }
    for (f, st) in &fs {
        assert!(!crosses_major(f, st, &cfg()), "{}", f.package);
    }
}

#[test]
fn a_major_is_crossed_only_with_the_advisory_that_has_no_patch_on_the_line() {
    let fs = vec![advised(
        "a",
        "GHSA-cccc",
        "tmpl-engine",
        "2.3.0",
        &["4.0.1", "3.9.0"],
    )];
    let p = plan(&pairs(&fs), &cfg());
    let b = bump(&p, "tmpl-engine");
    // The lowest patched release above the line, never the newest.
    assert_eq!((b.to.as_str(), b.major), ("3.9.0", true));
    assert_eq!(
        b.major_reason,
        "GHSA-cccc lists no patched release on the 2 line (patched releases it lists: 3.9.0, 4.0.1)"
    );
    assert!(crosses_major(&fs[0].0, &fs[0].1, &cfg()));
    // The pull request says it on the bump's line.
    let mut run = DepRun::new(p, "base", 3.0);
    sandbox(&[]).drive(&mut run);
    let report = run.report_md();
    assert!(
        report.contains("- `tmpl-engine` 2.3.0 to 3.9.0 (pnpm-lock.yaml; major version: GHSA-cccc lists no patched release on the 2 line"),
        "{report}"
    );
}

#[test]
fn one_advisory_without_a_patch_on_the_line_is_named_as_why_the_line_moves() {
    // Two advisories on the same copy: one patched on the line, one only on
    // the next major. The line has to move; the reason names the one.
    let fs = vec![
        advised("a", "GHSA-dddd", "glob-walk", "7.2.0", &["7.5.4", "8.1.1"]),
        advised("b", "GHSA-eeee", "glob-walk", "7.2.0", &["8.2.0"]),
    ];
    let p = plan(&pairs(&fs), &cfg());
    assert_eq!(p.bumps.len(), 1);
    let b = &p.bumps[0];
    assert_eq!((b.to.as_str(), b.major), ("8.2.0", true));
    assert!(b
        .major_reason
        .starts_with("GHSA-eeee lists no patched release on the 7 line"));
    assert!(!b.major_reason.contains("GHSA-dddd"));
}

#[test]
fn every_major_in_a_plan_records_why_and_no_same_line_bump_does() {
    let mut fs = vec![];
    for i in 0..12 {
        let fixed: &[&str] = if i % 3 == 0 {
            &["5.0.0"]
        } else {
            &["4.2.9", "5.0.0"]
        };
        fs.push(advised(
            &format!("f{i}"),
            &format!("GHSA-{i:04}"),
            &format!("lib-{i:02}"),
            "4.1.0",
            fixed,
        ));
    }
    let p = plan(&pairs(&fs), &cfg());
    assert_eq!(p.bumps.iter().filter(|b| b.major).count(), 4);
    for b in &p.bumps {
        assert_eq!(b.major, !b.major_reason.is_empty(), "{}", b.package);
        assert_eq!(b.major, version_line(&b.to) != b.line, "{}", b.package);
    }
}

// ── Runtime proof: the build and the start ──────────────────────────────

/// Eight same-line bumps and two majors, each major its own commit.
fn mixed_plan() -> DepPlan {
    let mut fs: Vec<_> = (0..8)
        .map(|i| {
            advised(
                &format!("f{i}"),
                &format!("GHSA-s{i:03}"),
                &format!("lib-{i:02}"),
                "2.1.0",
                &["2.1.5"],
            )
        })
        .collect();
    fs.push(advised(
        "m1",
        "GHSA-m001",
        "render-core",
        "3.4.0",
        &["4.0.2"],
    ));
    fs.push(advised("m2", "GHSA-m002", "route-kit", "1.9.0", &["2.0.0"]));
    plan(&pairs(&fs), &cfg())
}

#[test]
fn a_run_proves_the_build_and_the_start_after_its_batches() {
    let mut run = DepRun::new(mixed_plan(), "base", 3.0);
    let mut sb = sandbox(&[]);
    sb.proof_plan = Some(web_proof_plan());
    sb.drive(&mut run);
    assert_eq!(run.applied.len(), 10);
    // One build on the base branch, one on the branch.
    assert_eq!((sb.proof_runs, run.proof.runs), (2, 2));
    assert!(run.proof.done && run.proof.unresolved.is_empty() && run.proof.taken_out.is_empty());
    let report = run.report_md();
    assert!(report.contains("**Runtime proof (the repository's build and start, run in the sandbox with the bumps):**"), "{report}");
    assert!(report.contains("- Build `pnpm build` (from the CI workflow `ci.yml`, job `build`): passes with these bumps."), "{report}");
    assert!(report.contains("the application started in the sandbox and answered on port 38411. `GET /` 200, `GET /api/health` 200."), "{report}");
    assert!(
        report.contains("and 2 runtime proof build(s); model $0.00"),
        "{report}"
    );
}

#[test]
fn a_build_the_newest_bumps_break_takes_them_out_and_defers_them_with_the_cause() {
    let mut run = DepRun::new(mixed_plan(), "base", 3.0);
    // The checks pass with it; only the application's build shows it.
    let mut sb = sandbox(&[]);
    sb.proof_plan = Some(web_proof_plan());
    sb.build_breaks = vec!["route-kit".into()];
    sb.drive(&mut run);
    let applied: Vec<&str> = run
        .applied_bumps()
        .iter()
        .map(|b| b.package.as_str())
        .collect();
    assert!(!applied.contains(&"route-kit"), "{applied:?}");
    assert!(applied.contains(&"render-core"), "{applied:?}");
    assert_eq!(applied.len(), 9);
    // Base, the branch (red), the branch again without the newest set.
    assert_eq!(sb.proof_runs, 3);
    let d = run
        .deferred
        .iter()
        .find(|d| d.package == "route-kit")
        .expect("deferred");
    assert_eq!(d.cause, Cause::Broke);
    assert!(
        !d.later,
        "a bump that breaks the build is held out of later bundles"
    );
    assert_eq!(
        d.reason,
        "with it applied the build `pnpm build` fails (exit 1), which passes on the base branch; taken out so the bumps before it ship"
    );
    assert_eq!(run.fixed_ids().len(), 9);
    assert!(run.deferred_ids().iter().any(|(id, _)| id == "m2"));
    let report = run.report_md();
    assert!(report.contains("- Taken out by this proof: `route-kit` 1.9.0 to 2.0.0 deferred: with it applied the build `pnpm build` fails"), "{report}");
    assert!(report.contains("Build `pnpm build` (from the CI workflow `ci.yml`, job `build`): passes with these bumps."), "{report}");
    assert!(run.proof.unresolved.is_empty());
}

#[test]
fn a_build_that_fails_on_the_base_branch_too_is_never_blamed_on_a_bump() {
    let mut run = DepRun::new(mixed_plan(), "base", 3.0);
    let mut sb = sandbox(&[]);
    sb.proof_plan = Some(web_proof_plan());
    sb.base_build_fails = true;
    sb.drive(&mut run);
    assert_eq!(run.applied.len(), 10, "nothing is taken out");
    assert_eq!(sb.proof_runs, 2);
    let report = run.report_md();
    assert!(report.contains("fails on the base branch too"), "{report}");
    assert!(
        report.contains("proves nothing about the bumps"),
        "{report}"
    );
    assert!(
        !report.contains("passes with these bumps.\n- Start"),
        "{report}"
    );
}

#[test]
fn the_last_commit_is_never_taken_out_and_what_fails_is_said() {
    let fs = vec![advised("a", "GHSA-one1", "lib-00", "2.1.0", &["2.1.5"])];
    let mut run = DepRun::new(plan(&pairs(&fs), &cfg()), "base", 3.0);
    let mut sb = sandbox(&[]);
    sb.proof_plan = Some(web_proof_plan());
    sb.build_breaks = vec!["lib-00".into()];
    sb.drive(&mut run);
    assert_eq!(run.applied.len(), 1, "the pull request is never emptied");
    assert_eq!(
        run.proof.unresolved,
        vec!["the build `pnpm build` fails (exit 1)"]
    );
    let report = run.report_md();
    assert!(report.contains("Still failing with the bumps in this pull request: the build `pnpm build` fails (exit 1)."), "{report}");
    assert!(
        report.contains("fails with these bumps (exit 1); it passes on the base branch"),
        "{report}"
    );
    assert!(
        !report.contains("job `build`): passes with these bumps"),
        "{report}"
    );
}

#[test]
fn a_proof_that_did_not_run_is_said_never_claimed() {
    // Nothing to run.
    let mut run = DepRun::new(ten_bumps(), "base", 3.0);
    sandbox(&[]).drive(&mut run);
    assert!(run.proof.done && run.proof.runs == 0);
    let report = run.report_md();
    assert!(report.contains("- Not run: "), "{report}");
    assert!(
        report.contains("Nothing here claims the application builds or starts"),
        "{report}"
    );
    // No result from the sandbox, twice: no verdict, nothing taken out.
    let mut run = DepRun::new(mixed_plan(), "base", 3.0);
    let mut sb = sandbox(&[]);
    sb.proof_plan = Some(web_proof_plan());
    loop {
        match run.next_action() {
            Action::Proof { base: true } => {
                run.on_proof(true, proof_result(&web_proof_plan(), false))
            }
            Action::Proof { base: false } => run.on_proof(
                false,
                rp::ProofResult::no_result("the sandbox build timed out (CodeBuild TIMED_OUT)"),
            ),
            Action::Done => break,
            Action::ProofTakeOut { .. } => panic!("no result is never a verdict"),
            _ => {
                // The batches, as the worker drives them.
                let mut one = run.clone();
                step(&mut sb, &mut one);
                run = one;
            }
        }
    }
    assert_eq!(run.applied.len(), 10);
    assert_eq!(run.proof.runs, 3, "the base, then the branch twice");
    let report = run.report_md();
    assert!(
        report.contains("no result (the sandbox build timed out (CodeBuild TIMED_OUT))"),
        "{report}"
    );
    assert!(
        report.contains("Not proven here; CI on this pull request is the check."),
        "{report}"
    );
    assert!(
        !report.contains("job `build`): passes with these bumps"),
        "{report}"
    );
}

/// One action of a run, by the fake sandbox.
fn step(sb: &mut FakeSandbox, run: &mut DepRun) {
    match run.next_action() {
        Action::Base => run.on_base(&sb.result(&[])),
        Action::Probe(groups) => {
            let results: Vec<(usize, ProbeResult)> = groups
                .iter()
                .map(|&g| {
                    let mut on: Vec<&PlanBump> = run.applied_bumps();
                    on.extend(run.groups[g].bumps.iter().map(|&i| &run.plan.bumps[i]));
                    (g, sb.result(&on))
                })
                .collect();
            run.on_probes(&results);
        }
        Action::Commit(g) => {
            sb.commits += 1;
            let head = format!("sha{}", sb.commits);
            run.on_commit(g, &head);
        }
        Action::ProofPlan => run.on_proof_plan(sb.proof_plan.clone().unwrap_or_default()),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn a_proof_waits_for_a_fresh_step_once_and_the_run_reads_as_moving() {
    let mut run = DepRun::new(ten_bumps(), "base", 3.0);
    let mut sb = sandbox(&[]);
    sb.proof_plan = Some(web_proof_plan());
    while run.next_action() != (Action::Proof { base: true }) {
        step(&mut sb, &mut run);
    }
    let before = run.progress();
    run.on_proof_wait();
    assert_ne!(
        run.progress(),
        before,
        "a wait for a fresh step is progress"
    );
    assert!(run.proof.waited);
    // The state survives the hand-off as plain data.
    let json = serde_json::to_string(&run).unwrap();
    let mut back: DepRun = serde_json::from_str(&json).unwrap();
    assert_eq!(back, run);
    assert_eq!(back.next_action(), Action::Proof { base: true });
    back.on_proof(true, proof_result(&web_proof_plan(), false));
    assert!(
        !back.proof.waited,
        "the next build may wait for its own step"
    );
    // A state saved before this change resumes with the proof still to do.
    let old: DepRun = serde_json::from_str(r#"{"plan":{"bumps":[],"deferred":[]}}"#).unwrap();
    assert_eq!(old.proof, RuntimeProof::default());
}

// ── What stays open ─────────────────────────────────────────────────────

#[test]
fn the_report_counts_what_stays_open_by_reason() {
    let mut fs = vec![
        advised("a", "GHSA-fix1", "lib-00", "2.1.0", &["2.1.5"]),
        advised("b", "GHSA-fix2", "lib-01", "2.1.0", &["2.1.5"]),
        advised("m", "GHSA-brk1", "route-kit", "1.9.0", &["2.0.0"]),
    ];
    // Only a major fixes it and it isn't reachable: the rules leave it out.
    let mut skipped = advised("n", "GHSA-maj1", "old-parser", "4.11.3", &["5.5.0"]);
    skipped.0.verdict = Verdict::NotReachable;
    fs.push(skipped);
    let mut run = DepRun::new(plan(&pairs(&fs), &cfg()), "base", 3.0);
    let mut sb = sandbox(&[]);
    sb.proof_plan = Some(web_proof_plan());
    sb.build_breaks = vec!["route-kit".into()];
    sb.drive(&mut run);
    assert_eq!(run.fixed_ids(), vec!["a", "b"]);

    // The repository's other findings: no patched release, one a parent
    // holds, one another pull request fixes, one not due, one handled.
    let mut no_patch = advised("p", "GHSA-nop1", "legacy-crypto", "1.2.0", &[]).0;
    no_patch.fixed_version = None;
    let mut no_patch2 = advised("p2", "GHSA-nop2", "legacy-crypto", "1.2.0", &[]).0;
    no_patch2.fixed_version = None;
    no_patch2.status = Status::NoFix;
    let mut elsewhere = advised("o", "GHSA-oth1", "img-tool", "3.0.0", &["3.0.1"]).0;
    elsewhere.status = Status::PrOpen;
    let waiting = advised("w", "GHSA-wai1", "date-lib", "1.0.0", &["1.0.1"]).0;
    let mut done = advised("d", "GHSA-don1", "gone", "1.0.0", &["1.0.1"]).0;
    done.status = Status::Fixed;
    let mut all: Vec<Finding> = fs.iter().map(|(f, _)| f.clone()).collect();
    all.extend([no_patch, no_patch2, elsewhere, waiting, done]);
    // A bump the package manager could not install.
    run.deferred.push(Deferral {
        package: "held-lib".into(),
        from: "2.0.0".into(),
        to: "2.0.4".into(),
        finding_ids: vec!["h".into()],
        reason: "its version could not be installed".into(),
        cause: Cause::Parent,
        ..Default::default()
    });
    all.push(advised("h", "GHSA-par1", "held-lib", "2.0.0", &["2.0.4"]).0);
    let refs: Vec<&Finding> = all.iter().collect();
    let groups = remaining(&run, &refs);
    assert_eq!(
        groups,
        vec![
            OpenGroup {
                why: Open::NoPatch,
                advisories: 2,
                packages: vec!["legacy-crypto".into()]
            },
            OpenGroup {
                why: Open::Parent,
                advisories: 1,
                packages: vec!["held-lib".into()]
            },
            OpenGroup {
                why: Open::Broke,
                advisories: 1,
                packages: vec!["route-kit".into()]
            },
            OpenGroup {
                why: Open::MajorNotNeeded,
                advisories: 1,
                packages: vec!["old-parser".into()]
            },
            OpenGroup {
                why: Open::OtherPr,
                advisories: 1,
                packages: vec!["img-tool".into()]
            },
            OpenGroup {
                why: Open::NotInRun,
                advisories: 1,
                packages: vec!["date-lib".into()]
            },
        ]
    );
    assert_eq!(
        remaining_md(&groups),
        "**Advisories that stay open after this pull request: 7** (counted from the advisories CoderHelm tracks for this repository)\n\
         - 2: no patched release exists yet (`legacy-crypto`)\n\
         - 1: blocked by a parent package (it pins the version, or the fixed version could not be installed) (`held-lib`)\n\
         - 1: deferred because the build or the checks failed with the bump (`route-kit`)\n\
         - 1: only a major upgrade fixes it, and the advisory is not reachable or urgent (`old-parser`)\n\
         - 1: fixed by another open pull request (`img-tool`)\n\
         - 1: not part of this run (not due yet, still being triaged, or in another run) (`date-lib`)\n"
    );
    // Nothing left is said too.
    let fixed: Vec<&Finding> = all
        .iter()
        .filter(|f| run.fixed_ids().contains(&f.id))
        .collect();
    assert_eq!(
        remaining_md(&remaining(&run, &fixed)),
        "**Advisories that stay open after this pull request: 0** (counted from the advisories CoderHelm tracks for this repository)\n\
         - None: every open advisory CoderHelm tracks for this repository is fixed here.\n"
    );
}

#[test]
fn a_take_out_that_could_not_be_written_leaves_the_bumps_and_says_what_fails() {
    let mut run = DepRun::new(mixed_plan(), "base", 3.0);
    let mut sb = sandbox(&[]);
    sb.proof_plan = Some(web_proof_plan());
    loop {
        match run.next_action() {
            Action::Proof { base } => run.on_proof(base, proof_result(&web_proof_plan(), !base)),
            // GitHub refused the commit: the state never moved.
            Action::ProofTakeOut { .. } => run.on_proof_take_out_failed(),
            Action::Done => break,
            _ => step(&mut sb, &mut run),
        }
    }
    assert_eq!(run.applied.len(), 10);
    assert!(run.deferred.is_empty());
    assert_eq!(
        run.proof.unresolved,
        vec!["the build `pnpm build` fails (exit 1)"]
    );
}
