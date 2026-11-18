//! rk. A self-hosted install runs in the same AWS account as the team's
//! resources. An AWS WAF alarm run's WAF tools were refused
//! `wafv2:ListWebACLs`: `live::aws::assume` read with the worker's own
//! Lambda role instead of the team's `CoderHelmLogReader`. The refusal
//! blamed the team's role and the run waited. The wait's re-check took on
//! the reader, was allowed and started the run again, which was refused
//! again and waited again: a loop.
//!
//! Guarantee: every team read is made as the team's role (`TeamSession`,
//! made only by `assume`); the re-check takes the same target with the same
//! call; a refusal of another principal names it and is CoderHelm's own
//! failure; a restarted run never waits again for what the re-check read.

use aws_sdk_dynamodb::types::AttributeValue;
use common::alarm_evidence as ae;
use common::aws_role_reads::Gap;
use common::live::aws::{self, AwsTarget};
use common::live::aws_account::AwsAccount;
use common::role_wait::{self, Ending, Waiting};
use std::collections::HashMap;

const ACCOUNT: &str = "111122223333";
const READER: &str = "arn:aws:iam::111122223333:role/CoderHelmLogReader";
const WORKER: &str = "arn:aws:iam::111122223333:role/coderhelm-prod-ServiceRole-EXAMPLE";

/// The worker's refusal as AWS words it (CloudTrail named the same role).
fn worker_refused() -> String {
    format!(
        "User: arn:aws:sts::{ACCOUNT}:assumed-role/coderhelm-prod-ServiceRole-EXAMPLE/\
         coderhelm-prod-worker is not authorized to perform: wafv2:ListWebACLs on resource: \
         arn:aws:wafv2:us-east-1:{ACCOUNT}:regional/webacl/*"
    )
}

fn reader() -> AwsTarget {
    AwsTarget {
        role_arn: READER.into(),
        external_id: "team-private-id".into(),
        region: "us-east-1".into(),
        ..Default::default()
    }
}

/// An STS that answers nothing (a closed local port): taking on a role
/// fails, so any session that comes back was never the team's role.
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

#[tokio::test]
async fn a_same_account_reader_is_never_swapped_for_the_workers_own_role() {
    // The Lambda's environment: its own role, with the install's read role
    // set (infra/lib/own-account-read.ts), in the reader's account.
    std::env::set_var(aws::OWN_ROLE_ENV, WORKER);
    std::env::set_var("AWS_ACCESS_KEY_ID", "WORKER-KEY");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "worker-secret");
    let got = aws::assume(&dead_sts(), &reader(), "run").await;
    std::env::remove_var(aws::OWN_ROLE_ENV);
    std::env::remove_var("AWS_ACCESS_KEY_ID");
    std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    // Before: the worker's keys came back without asking STS, and every
    // WAF read was made, and refused, as the worker's role.
    match got {
        Ok(s) => {
            assert_ne!(s.access_key_id, "WORKER-KEY", "read as the worker's role");
            assert_eq!(s.role_arn(), READER);
        }
        Err(e) => assert_eq!(e.step, "sign_in", "{e:?}"),
    }
}

#[test]
fn the_recheck_and_the_run_take_on_the_same_role() {
    let item: HashMap<String, AttributeValue> = [
        ("account_id", ACCOUNT),
        ("role_arn", READER),
        ("external_id", "team-private-id"),
        ("region", "us-east-1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
    .collect();
    let run = AwsAccount::from_item(&item).expect("account").target();
    let recheck = common::log_findings::reader_target(&item).expect("target");
    assert_eq!(run, recheck);
    assert_eq!(recheck.role_arn, READER);
}

#[test]
fn a_refusal_of_another_role_names_it_and_is_never_the_teams_item() {
    let msg = worker_refused();
    let (line, team_gap) = ae::failure_line(
        "find the web ACL",
        "wafv2:ListWebACLs",
        Some("AccessDeniedException"),
        Some(&msg),
        READER,
    );
    assert!(!team_gap, "the worker's refusal blamed the team's role");
    assert!(
        line.contains("coderhelm-prod-ServiceRole-EXAMPLE"),
        "{line}"
    );
    assert!(line.contains("CoderHelm's own failure"), "{line}");
    assert!(
        !line.contains("CoderHelm's AWS role doesn't allow"),
        "{line}"
    );
    // The team's own role refused: that is the team's setup item.
    let as_team = msg.replace("coderhelm-prod-ServiceRole-EXAMPLE", "CoderHelmLogReader");
    let (line, team_gap) = ae::failure_line(
        "find the web ACL",
        "wafv2:ListWebACLs",
        Some("AccessDeniedException"),
        Some(&as_team),
        READER,
    );
    assert!(team_gap);
    assert!(
        line.contains("doesn't allow wafv2:ListWebACLs yet"),
        "{line}"
    );
    assert_eq!(
        aws::refused_other_principal(&msg, READER).as_deref(),
        Some("coderhelm-prod-ServiceRole-EXAMPLE")
    );
    assert_eq!(aws::refused_other_principal(&msg, WORKER), None);
}

#[test]
fn a_restarted_run_refused_what_the_recheck_read_never_waits_again() {
    let gap = Gap {
        account: ACCOUNT.into(),
        role_arn: READER.into(),
        actions: vec!["wafv2:ListWebACLs".into()],
        needed_for: "an AWS WAF rule's requests".into(),
        seen_at: "2026-11-18T10:00:00Z".into(),
        ..Default::default()
    };
    // The first run waits for the role.
    assert!(matches!(
        role_wait::ending_after(&[], vec![gap.clone()], None),
        Ending::AwaitRole(_)
    ));
    // The re-check read it and started the run again; refused again, it is
    // CoderHelm's own failure, never a second wait.
    let read = Waiting::of("run-1", std::slice::from_ref(&gap), "2026-11-18T10:05:00Z");
    match role_wait::ending_after(&[], vec![gap.clone()], Some(&read)) {
        Ending::OwnFailure(a) => {
            assert_eq!(a, vec!["wafv2:ListWebACLs".to_string()]);
            let line = role_wait::own_failure_line(&a);
            assert!(line.starts_with("CoderHelm's own failure"), "{line}");
            assert!(!line.contains('\n'));
        }
        e => panic!("waited again: {e:?}"),
    }
    // A different read the re-check never looked at still waits.
    let other = Gap {
        actions: vec!["s3:GetObject".into()],
        ..gap
    };
    assert!(matches!(
        role_wait::ending_after(&[], vec![other], Some(&read)),
        Ending::AwaitRole(_)
    ));
    // A person's item still goes to the person.
    assert_eq!(
        role_wait::ending_after(&["a decision".into()], vec![], Some(&read)),
        Ending::Person
    );
}
