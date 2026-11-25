//! Every fixture here is invented: no name, host, key or id of a real
//! install appears in a test.

use super::*;
use serde_json::json;

/// A synthetic customer team, every kind of record filled in.
fn customer() -> TeamRecords {
    TeamRecords {
        orgs: vec!["Quorvex-Labs".into()],
        repos: vec![
            "Quorvex-Labs/quorvex-ledger".into(),
            "web-app".into(),
            "api".into(),
        ],
        jira_hosts: vec!["https://brindlewick.atlassian.net".into()],
        project_keys: vec!["QVX".into(), "DATA".into()],
        wiki_hosts: vec!["https://talvoran.atlassian.net/wiki".into()],
        wiki_spaces: vec!["Zantrel Handbook".into(), "Engineering".into()],
        workspaces: vec!["ostrinhq".into()],
        member_emails: vec!["dana@mervale-mail.example".into(), "lee@gmail.com".into()],
        aws_accounts: vec!["210987654329".into(), "abc".into()],
        custom_domains: vec!["helm.vantrelio.example".into()],
        names: vec!["Quorvex Robotics".into()],
        install_names: vec![],
    }
}

/// The repository's own organization (synthetic too).
fn own() -> TeamRecords {
    TeamRecords {
        orgs: vec!["example-oss".into()],
        repos: vec!["example-oss/helmkit".into()],
        ..Default::default()
    }
}

fn install() -> InstallRecords {
    InstallRecords {
        teams: vec![customer(), own()],
        install: TeamRecords::default(),
    }
}

fn terms() -> TermSet {
    derive(&install(), &["example-oss"])
}

fn hit(line: &str) -> Option<Kind> {
    find_in_line(line, &terms())
}

#[test]
fn every_kind_of_record_becomes_a_term() {
    let cases = [
        ("Fixes the sync for Quorvex Labs", Kind::OrgName),
        ("the quorvex-ledger deploy", Kind::RepoName),
        ("see brindlewick.atlassian.net for it", Kind::TicketHost),
        ("closes QVX-1234", Kind::TicketKey),
        ("page on talvoran.atlassian.net", Kind::WikiHost),
        ("from the Zantrel Handbook space", Kind::WikiSpace),
        ("posted in ostrinhq", Kind::ChatWorkspace),
        ("mail from mervale-mail.example", Kind::EmailDomain),
        ("role in 210987654329", Kind::CloudAccount),
        ("served on helm.vantrelio.example", Kind::CustomDomain),
        ("for Quorvex Robotics", Kind::TeamName),
    ];
    for (line, kind) in cases {
        assert_eq!(hit(line), Some(kind), "{line}");
    }
}

#[test]
fn matching_ignores_case_and_separators_and_finds_joined_forms() {
    for line in [
        "QUORVEX LABS",
        "quorvex_labs",
        "QuorvexLabs",
        "quorvexlabs",
        "quorvex.labs",
        "(Quorvex-Labs)",
        "aws account 2109-8765-4329",
        "QuorvexLedger",
    ] {
        assert!(hit(line).is_some(), "{line}");
    }
}

#[test]
fn matching_is_on_word_boundaries() {
    for line in [
        "quorvexlabsx",
        "xquorvexlabs",
        "2109876543291",
        "QVX1234",
        "QVX 1234",
    ] {
        assert_eq!(hit(line), None, "{line}");
    }
}

#[test]
fn generic_words_and_short_names_are_skipped_unless_a_key() {
    // A repository of common words, a three-letter one, a common space name,
    // a public mail host: none is a term.
    for line in [
        "the web app",
        "an api change",
        "Engineering notes",
        "gmail.com",
        "abc",
    ] {
        assert_eq!(hit(line), None, "{line}");
    }
    // A short project key and a common-word key count as ticket keys only.
    assert_eq!(hit("QVX-88"), Some(Kind::TicketKey));
    assert_eq!(hit("DATA-12"), Some(Kind::TicketKey));
    assert_eq!(hit("the DATA table"), None);
    assert_eq!(hit("QVX alone"), None);
    assert!(is_generic("web-app", Kind::RepoName));
    assert!(is_generic("api", Kind::OrgName));
    assert!(!is_generic("Quorvex Labs", Kind::OrgName));
    // A name of common words is distinctive as an organization's, not as a
    // repository's.
    assert!(!is_generic("Blue River", Kind::OrgName));
    assert!(is_generic("blue-river", Kind::RepoName));
}

#[test]
fn the_repositorys_own_organization_is_not_a_customer() {
    // Its own names, and the same name in another team's records.
    assert_eq!(hit("example-oss/helmkit"), None);
    let mut other = customer();
    other.repos.push("helmkit".into());
    let set = derive(
        &InstallRecords {
            teams: vec![other, own()],
            install: TeamRecords::default(),
        },
        &["Example-OSS"],
    );
    assert_eq!(find_in_line("helmkit", &set), None);
    // On the customer's own repository the customer's names are its own,
    // and the other team's are details.
    let theirs = derive(&install(), &["Quorvex-Labs"]);
    assert_eq!(find_in_line("Quorvex Labs", &theirs), None);
    assert_eq!(find_in_line("helmkit", &theirs), Some(Kind::RepoName));
    // The pull request's author naming their own account is not a leak.
    let by_them = derive(&install(), &["example-oss", "Quorvex-Labs"]);
    assert_eq!(find_in_line("Quorvex Labs", &by_them), None);
}

#[test]
fn the_installs_own_names_count_only_outside_its_teams() {
    let mut i = install();
    i.install.names.push("Corvanthe Group".into());
    assert_eq!(
        find_in_line("Corvanthe Group", &derive(&i, &["example-oss"])),
        None
    );
    assert_eq!(
        find_in_line("Corvanthe Group", &derive(&i, &["someone-else"])),
        Some(Kind::TeamName)
    );
}

#[test]
fn hosts_are_named_by_their_site_label() {
    assert_eq!(site_label("acme.atlassian.net").as_deref(), Some("acme"));
    assert_eq!(
        site_label("https://wiki.acme.co.uk/x").as_deref(),
        Some("acme")
    );
    assert_eq!(site_label("acme.com").as_deref(), Some("acme"));
    assert_eq!(site_label("localhost"), None);
    assert_eq!(hit("the brindlewick board"), Some(Kind::TicketHost));
}

/// What is shown in public and what Debug prints never carries a term, and
/// never names a customer kind.
#[test]
fn no_matched_text_or_kind_in_any_output() {
    let set = terms();
    let sources = vec![
        Source::Title("Sync for Quorvex Labs".into()),
        Source::Body("Line one\nCloses QVX-12 for ostrinhq".into()),
        Source::Commit {
            sha: "c0ffee1234567".into(),
            message: "Fix\n\nAccount 210987654329".into(),
        },
        Source::Added {
            path: "docs/runbook.md".into(),
            lines: vec![(9, "See helm.vantrelio.example".into())],
        },
    ];
    let hits = scan(&sources, &set);
    assert_eq!(hits.len(), 4);
    let v = verdict(&hits);
    assert_eq!(v.conclusion, "failure");
    let body = check_body("abc", &v).to_string().to_lowercase();
    let debug = format!("{set:?} {hits:?} {:?} {:?}", install(), customer()).to_lowercase();
    for needle in [
        "quorvex",
        "qvx",
        "210987654329",
        "vantrelio",
        "zantrel",
        "robotics",
        "brindlewick",
        "talvoran",
        "mervale",
        "ostrin",
    ] {
        assert!(!body.contains(needle), "{needle} in the check run");
        assert!(!debug.contains(needle), "{needle} in Debug");
    }
    for k in Kind::ALL {
        assert!(
            !body.contains(k.as_str()),
            "the check run names the kind {}",
            k.as_str()
        );
    }
    for place in [
        "pr title",
        "pr body line 2",
        "commit c0ffee1 message line 3",
        "docs/runbook.md:9",
    ] {
        assert!(body.contains(place), "{place}");
    }
    assert_eq!(v.annotations, vec![("docs/runbook.md".to_string(), 9)]);
    assert!(body.contains(&FINDING.to_lowercase()));
}

#[test]
fn a_clean_pull_request_passes() {
    let v = verdict(&scan(&[Source::Title("Retry the sync".into())], &terms()));
    assert_eq!(v.conclusion, "success");
    assert_eq!(check_body("abc", &v)["name"], CHECK_NAME);
}

#[test]
fn the_check_turns_green_when_the_text_is_fixed() {
    let set = terms();
    let before = vec![Source::Body("Done for Quorvex Labs".into())];
    let after = vec![Source::Body("Done for one install".into())];
    assert_eq!(verdict(&scan(&before, &set)).conclusion, "failure");
    assert_eq!(verdict(&scan(&after, &set)).conclusion, "success");
    // The merge gate reads the newest run on the commit: the edit's.
    let runs = vec![
        json!({"name": CHECK_NAME, "id": 1, "started_at": "2026-01-01T00:00:00Z", "status": "completed", "conclusion": "failure"}),
        json!({"name": CHECK_NAME, "id": 2, "started_at": "2026-01-01T00:05:00Z", "status": "completed", "conclusion": "success"}),
    ];
    assert_eq!(guard_state(&runs), GuardState::Passing);
    assert_eq!(guard_state(&runs[..1]), GuardState::Failing);
}

#[test]
fn the_merge_is_refused_while_the_guard_fails_runs_or_is_missing_on_a_public_repo() {
    let run = |status: &str, conclusion: serde_json::Value| {
        vec![
            json!({"name": CHECK_NAME, "id": 1, "started_at": "t", "status": status, "conclusion": conclusion}),
        ]
    };
    let failing = guard_state(&run("completed", json!("failure")));
    let running = guard_state(&run("in_progress", serde_json::Value::Null));
    let skipped = guard_state(&run("completed", json!("neutral")));
    let other = vec![json!({"name": "Lint", "status": "completed", "conclusion": "failure"})];
    assert_eq!(guard_state(&other), GuardState::Missing);
    assert!(merge_refusal(failing, true, Some(0)).is_some());
    assert!(merge_refusal(failing, false, Some(0)).is_some());
    assert!(merge_refusal(running, true, Some(0)).is_some());
    assert!(merge_refusal(GuardState::Missing, true, Some(0)).is_some());
    assert_eq!(merge_refusal(GuardState::Missing, false, Some(0)), None);
    assert_eq!(merge_refusal(skipped, true, Some(0)), None);
    assert_eq!(merge_refusal(GuardState::Passing, true, Some(0)), None);
}

#[test]
fn only_a_members_branch_of_the_repo_itself_is_checked() {
    let o = |head: &str, assoc: &str, login: &str, kind: &str| Origin {
        base_repo: "example-oss/helmkit".into(),
        head_repo: head.into(),
        author_login: login.into(),
        author_type: kind.into(),
        author_association: assoc.into(),
        author_can_write: false,
    };
    assert!(trusted(&o("example-oss/helmkit", "MEMBER", "dana", "User")));
    assert!(trusted(&o("example-oss/helmkit", "OWNER", "dana", "User")));
    assert!(trusted(&o(
        "example-oss/helmkit",
        "NONE",
        "coderhelm[bot]",
        "Bot"
    )));
    // A fork, a deleted fork, an outside collaborator, a look-alike login.
    assert!(!trusted(&o("someone/helmkit", "MEMBER", "dana", "User")));
    assert!(!trusted(&o("", "MEMBER", "dana", "User")));
    assert!(!trusted(&o(
        "example-oss/helmkit",
        "COLLABORATOR",
        "dana",
        "User"
    )));
    assert!(!trusted(&o(
        "example-oss/helmkit",
        "CONTRIBUTOR",
        "coderhelm-fan",
        "User"
    )));
    assert!(!trusted(&o(
        "example-oss/helmkit",
        "NONE",
        "evil[bot]",
        "Bot"
    )));
}

#[test]
fn the_skipped_output_is_fixed() {
    assert_eq!(skipped(), skipped());
    assert_eq!(skipped().conclusion, "neutral");
    assert!(!skipped().summary.contains("customer detail"));
}

#[test]
fn files_read_are_docs_and_test_and_incident_fixtures() {
    for p in [
        "docs/incidents/zz_case.md",
        "services/worker/src/passes/incidents/zz_case.rs",
        "services/gateway/src/routes/testdata/x.json",
        "web/__fixtures__/a.ts",
        "src/foo.test.ts",
        "scripts/test_thing.py",
        "services/worker/src/passes/description_goldens/one.md",
        "services/worker/src/passes/review_cases/a.json",
        "pkg/thing_test.go",
    ] {
        assert!(reads_file(p), "{p}");
    }
    for p in [
        "src/main.rs",
        "README.md",
        "services/worker/src/passes/review.rs",
    ] {
        assert!(!reads_file(p), "{p}");
    }
}

#[test]
fn added_lines_carry_their_new_numbers() {
    let patch = "@@ -3,2 +3,3 @@ ctx\n keep\n-old\n+new one\n+new two\n keep";
    assert_eq!(
        added_lines(patch),
        vec![(4, "new one".to_string()), (5, "new two".to_string())]
    );
}

#[test]
fn records_are_read_without_their_values_reaching_debug() {
    let d = format!("{:?}", customer());
    assert!(d.starts_with("TeamRecords {") && !d.contains("Quorvex"));
}

#[test]
fn record_helpers() {
    use super::records::*;
    assert_eq!(project_key_of("QVX-12").as_deref(), Some("QVX"));
    assert_eq!(project_key_of("ALERT-3DCB8D72"), None);
    assert_eq!(project_key_of("qvx-12"), None);
    // CoderHelm's own made-up ticket prefixes are no customer's project.
    for own in ["GH-12", "SEC-4", "ALERT-9", "LOG-2"] {
        assert_eq!(project_key_of(own), None, "{own}");
    }
    assert_eq!(
        arn_accounts("arn:aws:iam::210987654329:role/x and arn:aws:s3:::bucket"),
        vec!["210987654329".to_string()]
    );
    assert_eq!(
        teams_tenant("ostrinhq.webhook.office.com …x9Qa").as_deref(),
        Some("ostrinhq")
    );
    assert_eq!(teams_tenant("hooks.slack.com …x9Qa"), None);
}

#[test]
fn the_common_words_list_names_no_install() {
    // The list is a public word list plus software words: it has no
    // hostnames, keys or ids, only lowercase words.
    for w in COMMON_WORDS
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        assert!(
            w.chars()
                .all(|c| c.is_ascii_lowercase() || c == '-' || c.is_ascii_digit()),
            "{w}"
        );
        assert!(w.len() >= 3, "{w}");
    }
}

#[test]
fn the_hosted_side_checks_only_its_operators_repositories() {
    // Self-hosted: one company, every repository.
    assert!(checks_repo(true, "anyone", None));
    // Hosted: only the app owner's repositories; a customer's own public
    // repository is never checked against other customers' names.
    assert!(checks_repo(false, "example-oss", Some("Example-OSS")));
    assert!(!checks_repo(false, "Quorvex-Labs", Some("example-oss")));
    assert!(!checks_repo(false, "example-oss", None));
    assert!(!checks_repo(false, "example-oss", Some("")));
}

#[test]
fn a_guard_that_never_runs_stops_holding_merges_after_the_grace() {
    let late = Some(MISSING_GRACE_SECS + 1);
    assert_eq!(merge_refusal(GuardState::Missing, true, late), None);
    assert!(merge_refusal(GuardState::Missing, true, None).is_some());
    assert!(merge_refusal(GuardState::Missing, true, Some(60)).is_some());
    // A failing or running guard holds the merge however old the head is.
    assert!(merge_refusal(GuardState::Failing, true, late).is_some());
    assert!(merge_refusal(GuardState::Running, true, late).is_some());
}

#[test]
fn a_member_whose_membership_is_private_is_trusted_by_write_access() {
    // GitHub shows the app a private member as a contributor.
    let mut o = Origin {
        base_repo: "example-oss/helmkit".into(),
        head_repo: "example-oss/helmkit".into(),
        author_login: "dana".into(),
        author_type: "User".into(),
        author_association: "CONTRIBUTOR".into(),
        author_can_write: false,
    };
    assert!(!trusted(&o));
    o.author_can_write = true;
    assert!(trusted(&o));
    // Write access never trusts a fork.
    o.head_repo = "dana/helmkit".into();
    assert!(!trusted(&o));
}
