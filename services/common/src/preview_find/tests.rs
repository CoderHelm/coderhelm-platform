//! Replays of each route a preview's address is published on, with payloads
//! shaped like the code host's own listings and like real preview bots'
//! comments (names and hosts made up).

use super::*;

const HEAD: &str = "4f2c9e1ab7d3c5e8f0a1b2c3d4e5f60718293a4b";
/// The head commit's time: a comment edited after it speaks of it.
const HEAD_AT: &str = "2026-11-17T10:00:00Z";

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

fn fixture(text: &str) -> serde_json::Value {
    serde_json::from_str(text).expect("fixture is JSON")
}

fn deployment_list(text: &str) -> Vec<(serde_json::Value, Vec<serde_json::Value>)> {
    fixture(text)
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["deployment"].clone(),
                d["statuses"].as_array().cloned().unwrap_or_default(),
            )
        })
        .collect()
}

fn changed_web() -> Vec<String> {
    s(&["apps/web/src/app/signup/page.tsx"])
}

fn apps() -> Vec<String> {
    s(&["web", "admin"])
}

#[test]
fn a_hosts_deployments_give_the_changed_apps_preview_of_the_head() {
    let d = deployment_list(include_str!("fixtures/deployments.json"));
    let seen = deployments(&d, HEAD);
    // The older commit's deploy and the production deploy are never this
    // head's preview.
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!(seen.iter().all(|x| x.tied && x.ready == Ready::Up));
    // The newest status says where it stands, and its environment address
    // is the site (the target is a console page).
    let web = seen.iter().find(|x| x.name.contains("web")).unwrap();
    assert_eq!(
        web.url,
        "https://web-git-feature-signup-form-team.host.example"
    );
    // A monorepo: the app the change touches, never another app's.
    assert_eq!(
        resolve(&seen, &changed_web(), &apps()),
        Answer::Up(Found {
            url: "https://web-git-feature-signup-form-team.host.example".into(),
            source: Source::Deployment,
            name: "Preview – web".into(),
            tied: true,
        })
    );
    let admin_change = s(&["apps/admin/src/pages/users.tsx"]);
    match resolve(&seen, &admin_change, &apps()) {
        Answer::Up(f) => assert!(f.url.starts_with("https://admin-"), "{f:?}"),
        a => panic!("{a:?}"),
    }
}

#[test]
fn a_workflow_jobs_environment_still_building_is_waited_on() {
    let d = deployment_list(include_str!("fixtures/workflow_environment.json"));
    let seen = deployments(&d, HEAD);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].ready, Ready::Building);
    // Its log link is a run on the code host, never the preview.
    assert_eq!(seen[0].url, "");
    assert!(matches!(
        resolve(&seen, &changed_web(), &apps()),
        Answer::Building(w) if w.contains("pr-12")
    ));
}

#[test]
fn a_deploy_that_failed_or_was_taken_down_is_said_never_waited_on() {
    let mk = |state: &str| {
        vec![(
            serde_json::json!({"sha": HEAD, "environment": "preview/feature-x", "transient_environment": true}),
            vec![
                serde_json::json!({"id": 1, "state": state, "created_at": "2026-11-17T10:01:00Z"}),
            ],
        )]
    };
    for (state, word) in [
        ("failure", "failed"),
        ("error", "failed"),
        ("inactive", "taken down"),
    ] {
        let seen = deployments(&mk(state), HEAD);
        match resolve(&seen, &changed_web(), &[]) {
            Answer::Failed(w) => assert!(w.contains(word), "{w}"),
            a => panic!("{state}: {a:?}"),
        }
    }
}

#[test]
fn a_deploy_status_on_the_head_gives_the_preview_and_never_a_ci_log() {
    let seen = statuses(&fixture(include_str!("fixtures/statuses.json")), HEAD);
    // The CI build's status and the host's rule-check status don't read as a
    // deploy of the preview.
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert_eq!(
        seen[0].url,
        "https://deploy-preview-12--acme-web.static-host.example"
    );
    assert!(matches!(
        resolve(&seen, &changed_web(), &[]),
        Answer::Up(f) if f.source == Source::CommitStatus
    ));
    // Another commit's combined status says nothing of this head.
    assert!(statuses(
        &fixture(include_str!("fixtures/statuses.json")),
        "0aa11bb22cc"
    )
    .is_empty());
}

#[test]
fn a_preview_check_runs_output_carries_the_address() {
    let seen = check_runs(
        &fixture(include_str!("fixtures/check_runs.json")),
        HEAD,
        &[],
    );
    // The end-to-end check and the unit tests aren't the deploy.
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert_eq!(seen[0].url, "https://8c1d2e3f.web.edge-host.example");
    assert_eq!(seen[0].ready, Ready::Up);
}

#[test]
fn a_preview_job_named_by_the_workflows_counts_even_when_its_name_doesnt_say_preview() {
    let runs = serde_json::json!({"check_runs": [
        {"name": "Ship web", "head_sha": HEAD, "status": "completed", "conclusion": "success",
         "output": {"summary": "Live at https://web-12.review.your-domain.example", "text": null}}
    ]});
    assert!(check_runs(&runs, HEAD, &[]).is_empty());
    let covers = vec![crate::preview::Covering {
        check: "Ship web".into(),
        role: Some(crate::preview::Role::Preview),
        workflow: ".github/workflows/preview.yml".into(),
    }];
    let seen = check_runs(&runs, HEAD, &covers);
    assert_eq!(seen[0].url, "https://web-12.review.your-domain.example");
}

#[test]
fn a_bot_comment_edited_in_place_is_read_row_by_row_and_an_older_commits_is_not() {
    let list = fixture(include_str!("fixtures/comments.json"));
    let seen = comments(list.as_array().unwrap(), HEAD, HEAD_AT);
    // A person's comment and the comment that names an older commit give
    // nothing; the table gives one row per app, never its console links.
    assert_eq!(seen.len(), 2, "{seen:?}");
    let web = seen.iter().find(|x| x.name == "web").unwrap();
    assert_eq!(web.ready, Ready::Building);
    let admin = seen.iter().find(|x| x.name == "admin").unwrap();
    assert_eq!(admin.ready, Ready::Up);
    assert!(seen.iter().all(|x| x.tied && !is_console(&x.url)));
    // The change is in the web app, still building: wait for it, never take
    // the admin app's preview instead.
    assert!(matches!(
        resolve(&seen, &changed_web(), &apps()),
        Answer::Building(w) if w.contains("web")
    ));
    // Before the head's time, an unnamed comment speaks of an older push.
    let early = comments(list.as_array().unwrap(), HEAD, "2026-11-17T11:00:00Z");
    assert!(early.iter().all(|x| !x.tied));
}

#[test]
fn a_comment_naming_the_head_commit_gives_its_preview() {
    let list = fixture(include_str!("fixtures/comment_head.json"));
    let seen = comments(list.as_array().unwrap(), HEAD, HEAD_AT);
    assert!(
        seen.iter().all(|x| x.tied && x.ready == Ready::Up),
        "{seen:?}"
    );
    let urls: Vec<&str> = seen.iter().map(|x| x.url.as_str()).collect();
    assert!(urls.contains(&"https://deploy-preview-12--acme-web.static-host.example"));
    assert!(urls.contains(&"https://acme.pages.example/web/pr-preview/pr-12/"));
    assert!(!urls.iter().any(|u| u.contains("/deploys/")));
}

#[test]
fn a_description_link_counts_only_when_nothing_on_the_head_has_an_address() {
    let link = body(
        "## Preview\nPreview: https://pr-12.preview.your-domain.example\nDocs: https://docs.example.org/guide",
        HEAD,
    );
    assert_eq!(link.len(), 1);
    assert!(!link[0].tied);
    // A check on the head says the deploy passed but prints no address:
    // the link is used.
    let mut all = link.clone();
    all.push(Sighting {
        source: Source::CheckRun,
        name: "Deploy preview".into(),
        url: String::new(),
        ready: Ready::Up,
        tied: true,
    });
    assert!(matches!(
        resolve(&all, &[], &[]),
        Answer::Up(f) if f.source == Source::Body && !f.tied
    ));
    // The head's deploy still building: wait, the link may serve an older
    // build.
    all.push(Sighting {
        source: Source::Deployment,
        name: "pr-12".into(),
        url: String::new(),
        ready: Ready::Building,
        tied: true,
    });
    assert!(matches!(resolve(&all, &[], &[]), Answer::Building(_)));
}

#[test]
fn the_most_direct_source_wins_between_answers_on_the_head() {
    let mut all = deployments(
        &deployment_list(include_str!("fixtures/deployments.json")),
        HEAD,
    );
    all.extend(statuses(
        &fixture(include_str!("fixtures/statuses.json")),
        HEAD,
    ));
    all.extend(body(
        "Preview: https://pr-12.preview.your-domain.example",
        HEAD,
    ));
    assert!(matches!(
        resolve(&all, &changed_web(), &apps()),
        Answer::Up(f) if f.source == Source::Deployment
    ));
    assert_eq!(resolve(&[], &changed_web(), &apps()), Answer::Nothing);
}

#[test]
fn templates_fill_in_the_pull_request_as_hosts_name_previews() {
    assert_eq!(branch_slug("Feature/Signup_Form!!"), "feature-signup-form");
    assert_eq!(branch_slug("--a//b--"), "a-b");
    let v = Vars {
        pr: 12,
        branch: "feature/signup-form",
        sha: HEAD,
        owner: "acme",
        repo: "web",
    };
    assert_eq!(
        expand("https://pr-{pr}.preview.your-domain.example", v).as_deref(),
        Some("https://pr-12.preview.your-domain.example")
    );
    assert_eq!(
        expand("https://{repo}-git-{branch_slug}-team.host.example", v).as_deref(),
        Some("https://web-git-feature-signup-form-team.host.example")
    );
    // In a path the branch keeps its slashes; in a host it is a slug.
    assert_eq!(
        expand("https://previews.your-domain.example/{branch}/", v).as_deref(),
        Some("https://previews.your-domain.example/feature/signup-form/")
    );
    assert_eq!(
        expand("https://{short_sha}.web.edge-host.example", v).as_deref(),
        Some("https://4f2c9e1.web.edge-host.example")
    );
    // A host label longer than DNS allows is cut to 63 characters.
    let long = Vars {
        branch: "feature/a-very-long-branch-name-that-keeps-going-and-going-past-the-limit",
        ..v
    };
    let u = expand("https://{branch_slug}--site.static-host.example", long).unwrap();
    let label = u["https://".len()..].split('.').next().unwrap();
    assert!(label.len() <= 63 && !label.ends_with('-'), "{label}");
    // Nothing to fill a placeholder with: no address.
    assert_eq!(
        expand("https://pr-{pr}.x.example", Vars { pr: 0, ..v }),
        None
    );
}

#[test]
fn a_template_is_checked_as_the_team_types_it() {
    assert!(check_template("").is_ok());
    assert!(check_template("https://pr-{pr}.preview.your-domain.example").is_ok());
    assert!(check_template("http://pr-{pr}.x.example").is_err());
    assert!(check_template("https://pr-{number}.x.example").is_err());
    // A fixed address is a shared environment, not a pull request's own.
    assert!(check_template("https://staging.your-domain.example").is_err());
    assert!(check_template("https://x.example/$(id)/{pr}").is_err());
}

#[test]
fn addresses_are_read_from_text_with_what_labels_them() {
    assert_eq!(
        pick_url(
            "[Visit Preview](https://web-abc.host.example) and https://docs.example.org",
            false
        )
        .as_deref(),
        Some("https://web-abc.host.example")
    );
    assert_eq!(pick_url("see https://docs.example.org/guide", false), None);
    assert_eq!(
        pick_url("see https://docs.example.org/guide", true).as_deref(),
        Some("https://docs.example.org/guide")
    );
    assert!(is_console(
        "https://app.static-host.example/sites/x/deploys/1"
    ));
    assert!(is_console("https://ci.example.org/gh/acme/web/4411"));
    assert!(!is_console(
        "https://deploy-preview-12--acme-web.static-host.example"
    ));
    assert!(is_code_host("https://github.com/acme/web/actions/runs/1"));
    assert!(!site_like("https://img.shields.io/badge/x.svg"));
}

#[test]
fn the_app_a_preview_is_named_after_is_matched_to_the_changed_paths() {
    let tree = s(&[
        "apps/web/package.json",
        "apps/admin/package.json",
        "packages/ui/src/a.ts",
    ]);
    assert_eq!(apps_of(&tree), s(&["web", "admin", "ui"]));
    let a = apps_of(&tree);
    assert_eq!(app_score("Preview – web", &changed_web(), &a), 2);
    assert_eq!(app_score("Preview – admin", &changed_web(), &a), -1);
    assert_eq!(app_score("api-pr-12", &changed_web(), &a), -1);
    assert_eq!(app_score("Preview", &changed_web(), &a), 0);
}

#[test]
fn time_between_reads_rfc3339_and_a_missing_time_is_due() {
    assert_eq!(
        minutes_between("2026-11-17T10:00:00Z", "2026-11-17T10:45:30Z"),
        45
    );
    assert_eq!(minutes_between("", "2026-11-17T10:45:30Z"), 0);
    assert_eq!(seconds_between("", "2026-11-17T10:45:30Z"), i64::MAX);
}
