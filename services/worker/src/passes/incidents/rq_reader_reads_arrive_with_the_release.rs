//! rq. A self-hosted install's owner got a push alert: "AWS account <the
//! install's own account> needs 1 new read-only permission ... Until an
//! admin updates the CoderHelmLogReader role, the parts that need it are
//! skipped." The account was the install's own. `PermissionCheck`'s
//! `install_account` was only ever set in tests (the builder always wrote
//! false), so the daily sweep treated that account like any other: it
//! posted the alert and the AWS page offered the one-click update stack.
//! And nothing in the release brought the reads to that role: since every
//! team read is made as CoderHelmLogReader (rk), it fell behind each release.
//!
//! Guarantee: no push for new read-only permissions, ever (the sweep sends
//! nothing); another account shows them only as the AWS page's quiet item
//! with its one-click stack; the install's own account on a one-stack
//! install is marked by the check's own builder, shows nothing to do, and
//! gets the reads from the access stack's reader update after each update
//! of the install's stack (lambda/native-reader).

use common::aws_role_reads;
use common::live::aws::AwsTarget;
use common::live::aws_account::AwsAccount;
use common::live::permissions::{self, PermissionCheck, Simulated};

const OWN_ACCOUNT: &str = "111122223333";
const OTHER_ACCOUNT: &str = "999988887777";
/// The install's own gateway role (INSTALL_OWN_ROLE_ARN), in its account.
const INSTALL_ROLE: &str = "arn:aws:iam::111122223333:role/coderhelm-prod-ServiceRole-EXAMPLE";
const TEMPLATE: &str =
    "https://coderhelm-public.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader.yaml";
const NOW: &str = "2030-03-19T09:00:00Z";

fn reader(account: &str) -> AwsTarget {
    AwsTarget {
        role_arn: format!("arn:aws:iam::{account}:role/CoderHelmLogReader"),
        external_id: "team-private-id".into(),
        region: "us-east-1".into(),
        ..Default::default()
    }
}

/// An STS that answers nothing (a closed local port), as in rk.
fn dead_sts() -> aws_sdk_sts::Client {
    use aws_sdk_sts::config::{BehaviorVersion, Credentials, Region};
    aws_sdk_sts::Client::from_conf(
        aws_sdk_sts::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://127.0.0.1:9")
            .retry_config(aws_sdk_sts::config::retry::RetryConfig::disabled())
            .credentials_provider(Credentials::new("caller", "caller", None, None, "test"))
            .build(),
    )
}

/// What the live check found once the role could be taken on: this
/// release's `ecs:DescribeTasks` is missing, as in the alert (1 new).
fn behind(mut c: PermissionCheck) -> PermissionCheck {
    c.works = true;
    permissions::apply_simulation(
        &mut c,
        &[Simulated {
            action: "ecs:DescribeTasks".into(),
            allowed: false,
            boundary_allows: None,
            org_allows: None,
        }],
    );
    c
}

fn account_with(t: &AwsTarget, c: PermissionCheck) -> AwsAccount {
    AwsAccount {
        account: t.account().to_string(),
        role_arn: t.role_arn.clone(),
        external_id: t.external_id.clone(),
        region: t.region.clone(),
        status: "connected".into(),
        permissions: Some(c),
        ..Default::default()
    }
}

#[tokio::test]
async fn the_checks_own_builder_marks_the_install_account() {
    // The real path the sweep, the AWS page and a waiting run's re-check
    // take (`permissions::check` is `check_for` with this process's install
    // role): before, `install_account` came back false for the install's
    // own account whatever happened.
    let own = reader(OWN_ACCOUNT);
    let c = permissions::check_for(
        &dead_sts(),
        &own,
        "permissions-check",
        PermissionCheck::for_install(&own, NOW, Some(INSTALL_ROLE), true),
    )
    .await;
    assert!(!c.works, "the closed STS took the role on");
    assert!(
        c.install_account,
        "the install's own account, kept by the release"
    );
    // Another account, and an install that doesn't keep its reader (from
    // the CLI): updated by a person like any other.
    let other = reader(OTHER_ACCOUNT);
    let c = permissions::check_for(
        &dead_sts(),
        &other,
        "permissions-check",
        PermissionCheck::for_install(&other, NOW, Some(INSTALL_ROLE), true),
    )
    .await;
    assert!(!c.install_account);
    assert!(!PermissionCheck::for_install(&own, NOW, Some(INSTALL_ROLE), false).install_account);
    assert!(!PermissionCheck::for_install(&own, NOW, None, true).install_account);
}

#[test]
fn the_install_account_shows_nothing_to_do() {
    let own = reader(OWN_ACCOUNT);
    let c = behind(PermissionCheck::for_install(
        &own,
        NOW,
        Some(INSTALL_ROLE),
        true,
    ));
    assert_eq!(c.status(), "update_needed");
    // No update stack to launch, on the account or on any optional read.
    let v = c.view(false, TEMPLATE);
    assert!(v["update"].is_null(), "{v}");
    assert_eq!(v["install_account"], true);
    for a in v["abilities"].as_array().unwrap() {
        assert!(a["turn_on_url"].is_null(), "{a}");
    }
    let p = c.problem(false).expect("says what happens");
    assert!(p.fix.starts_with("Nothing to do"), "{}", p.fix);
    assert!(p.fix.contains("the release brings"));
    // And no setup item: the role's own reads are the release's job.
    assert!(aws_role_reads::role_update(&account_with(&own, c), None, 0, TEMPLATE).is_none());
}

#[test]
fn another_account_is_one_quiet_item_with_its_one_click_stack() {
    let other = reader(OTHER_ACCOUNT);
    let c = behind(PermissionCheck::for_install(
        &other,
        NOW,
        Some(INSTALL_ROLE),
        true,
    ));
    let v = c.view(false, TEMPLATE);
    let url = v["update"]["url"]
        .as_str()
        .expect("the one-click update stack");
    assert!(url.contains("coderhelm-log-reader-update.yaml"), "{url}");
    assert_eq!(v["update"]["sign_in_account"], OTHER_ACCOUNT);
    let item = aws_role_reads::role_update(&account_with(&other, c), None, 0, TEMPLATE)
        .expect("the AWS role reads item");
    assert!(item
        .reasons
        .iter()
        .any(|r| r.contains("older than this release")));
}

#[test]
fn the_daily_sweep_sends_no_message_of_any_kind() {
    let src = include_str!("../aws_permissions.rs");
    let code = &src[..src.find("#[cfg(test)]").unwrap()];
    for push in [
        "notify::",
        "deliver",
        "log_findings::channels",
        "aws_permissions_update",
        "AWS_PERMS_NOTICE",
    ] {
        assert!(!code.contains(push), "the sweep still pushes: {push}");
    }
    // It still checks and saves every account for the AWS page.
    assert!(code.contains("permissions::check(") && code.contains("save_permissions("));
}

#[test]
fn the_release_brings_the_reads_to_the_install_accounts_reader() {
    // The access stack's reader update, as the release ships it: after each
    // update of the install's stack it creates or updates
    // CoderHelmLogReaderUpdate-release from the running release's update
    // template (lambda/tests/test_native_reader.py and
    // infra/test/native-access.test.ts check it end to end).
    let keeper = include_str!("../../../../../lambda/native-reader/handler.py");
    assert!(keeper.contains(r#"STACK = "CoderHelmLogReaderUpdate-release""#));
    assert!(keeper.contains(r#"TEMPLATE = "coderhelm-log-reader-update.yaml""#));
    let stack = include_str!("../../../../../infra/lib/native/access-stack.ts");
    assert!(stack.contains(r#"status: ["UPDATE_COMPLETE"]"#));
    assert!(stack.contains("lambda/native-reader/handler.py"));
    // The services learn it from the one-stack install's environment.
    let env = include_str!("../../../../../infra/lib/service-env.ts");
    assert!(env.contains(r#"if (cfg.native) env.INSTALL_READER_KEPT = "1";"#));
    assert_eq!(common::live::aws::READER_KEPT_ENV, "INSTALL_READER_KEPT");
}
