//! cw. A team's read-only AWS role is behind the release, and an alert
//! route sends it AWS WAF alarms. The WAF reads of the alarm runs were
//! first added to a CloudShell setup script (`coderhelm-read-waf`) with a
//! one-line update shown as a setup item: every team had to find and paste
//! a script for each new read, the script patched a role CloudFormation had
//! made, and it added the reads to the data-source role `CoderhelmReadOnly`
//! while the alarm runs read through `CoderHelmLogReader`. A release can't
//! add a read itself: CoderHelm's own roles may not change IAM.
//!
//! Guarantee: every read is declared in the role stack's one versioned
//! template, as named groups a stack parameter switches on (the WAF log
//! group `WafLogs`, `DatabaseClusters`, `Warehouses`). A WAF alarm run on
//! the old role reads what it may, records what it was refused and what it
//! found (the log bucket's KMS key, the workgroup's results location), and
//! asks nobody. The install's one item, "AWS role reads", is the release's
//! one-click update of that stack with those filled in, which an AWS admin
//! of the account creates. Data sources the old script made move onto the
//! stack's role only once it reads them end to end.

use common::alarm_evidence::{self as ae};
use common::aws_role_reads::{self, Gap};
use common::live::aws::AwsTarget;
use common::live::aws_account::AwsAccount;
use common::log_reader::{self, UPDATE_TEMPLATE};
use common::stop_judge::{self as sj, JudgeModel, Kind, Picture};
use serde_json::{json, Value};
use std::sync::Mutex;

const ACCOUNT: &str = "111122223333";
const KEY: &str = "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab";
const RELEASE_UPDATE: &str =
    "https://coderhelm-public.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader-update.yaml";

/// The ticket the alert route wrote for the alarm.
const TICKET: &str = "- Alarm: acme-crawler-a-signature-drift\n\
    - State: ALARM (was OK)\n\
    - Metric: AWS/WAFV2 / CountedRequests\n\
    - Dimensions: WebACL=acme-web-acl, Rule=CrawlerASignatureCount\n";

struct Recorded(Mutex<Vec<Value>>);

impl JudgeModel for Recorded {
    async fn answer(&self, _: &str, _: &str, _: &Value) -> Result<Value, String> {
        self.0.lock().unwrap().pop().ok_or_else(|| "no more".into())
    }
}

fn run<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(f)
}

/// The team's connected account: its role stack made from an older
/// release's template (one read of this release missing), nothing chosen.
fn old_role() -> AwsAccount {
    let mut a = AwsAccount {
        account: ACCOUNT.into(),
        role_arn: format!("arn:aws:iam::{ACCOUNT}:role/CoderHelmLogReader"),
        external_id: "0b5f1a5e-0000-4000-8000-000000000000".into(),
        region: "us-east-1".into(),
        ..Default::default()
    };
    let mut c = common::live::permissions::PermissionCheck::new(&a.target(), "t");
    c.works = true;
    c.missing = vec!["ecs:DescribeTasks".into()];
    a.permissions = Some(c);
    a
}

/// The `param_<Name>=<value>` pairs of a quick-create link.
fn params(url: &str) -> Vec<(String, String)> {
    url.split(['?', '&'])
        .filter_map(|p| p.strip_prefix("param_"))
        .filter_map(|p| p.split_once('='))
        .map(|(k, v)| (k.to_string(), urlencoding::decode(v).unwrap().into_owned()))
        .collect()
}

/// The update template's parameters, with the pattern each must match.
fn template_params() -> Vec<(String, Option<String>)> {
    let t: serde_yaml::Value = serde_yaml::from_str(UPDATE_TEMPLATE).expect("the template parses");
    t["Parameters"]
        .as_mapping()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().unwrap().to_string(),
                v["AllowedPattern"].as_str().map(str::to_string),
            )
        })
        .collect()
}

/// The link opens CloudFormation's quick-create of this release's update
/// template, and every value it fills is one the template accepts.
fn assert_works(url: &str) {
    assert!(url.starts_with(
        "https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/quickcreate?"
    ));
    assert!(url.contains(&format!(
        "templateURL={}",
        urlencoding::encode(RELEASE_UPDATE)
    )));
    let declared = template_params();
    for (k, v) in params(url) {
        let (_, pattern) = declared
            .iter()
            .find(|(n, _)| *n == k)
            .unwrap_or_else(|| panic!("{k} isn't a parameter of the update template"));
        if let Some(p) = pattern {
            assert!(
                regex::Regex::new(p).unwrap().is_match(&v),
                "{k}={v} fails {p}"
            );
        }
    }
}

#[test]
fn an_old_role_with_a_waf_alert_route_gets_one_working_update_and_no_question() {
    let alarm = ae::waf_alarm(TICKET).expect("a WAF alarm");
    // First run on the old role: WAF's own reads work, the log tables are
    // refused; the bucket's settings read gave its KMS key.
    let refused = vec!["glue:GetDatabases".to_string()];
    let mut got = ae::evidence(
        &alarm,
        vec!["Couldn't list the Glue databases: CoderHelm's AWS role doesn't allow glue:GetDatabases yet.".into()],
        refused.clone(),
    );
    got.logs_to_s3 = true;
    got.waf_log_key = KEY.into();
    assert!(got.text.contains("\"AWS role reads\"") && got.text.contains("asks nobody"));
    assert!(!got.text.contains("CloudShell"));
    // The worker records it with what it found.
    let gap = Gap {
        waf_log_results: got.waf_log_results.clone(),
        waf_log_key: got.waf_log_key.clone(),
        ..aws_role_reads::gap(
            ACCOUNT,
            &old_role().role_arn,
            &got.refused,
            "an AWS WAF alarm's evidence",
            "t",
        )
        .unwrap()
    };
    let worker = include_str!("../mod.rs");
    assert!(worker.contains("waf_log_key: got.waf_log_key.clone()"));
    assert!(worker.contains("remember_waf_logs(state, &msg.team_id, &account, &got)"));
    // One item: the role is behind the release and the WAF log group is needed.
    let u =
        aws_role_reads::role_update(&old_role(), Some(&gap), 0, RELEASE_UPDATE).expect("one item");
    assert_works(&u.url);
    let p = params(&u.url);
    assert!(p.contains(&("WafLogs".into(), "on".into())));
    assert!(p.contains(&("WafLogKey".into(), KEY.into())));
    assert!(p.contains(&("RoleName".into(), "CoderHelmLogReader".into())));
    assert!(u
        .reasons
        .iter()
        .any(|r| r.contains("older than this release")));
    assert!(u.adds.contains(&"AWS WAF logs in S3"));
    assert!(u.stack_name.starts_with(&log_reader::update_stack_name()));
    // The next run, with the group on but no results location yet, found
    // the workgroup's and was refused the query: the next link names it.
    let mut stored = old_role();
    stored.permissions = None;
    stored.waf_logs = true;
    stored.waf_log_key = KEY.into();
    let later = Gap {
        actions: vec!["athena:StartQueryExecution".into()],
        waf_log_results: log_reader::waf_log_results("s3://aws-waf-logs-acme/athena-results/")
            .unwrap(),
        ..gap.clone()
    };
    let u2 = aws_role_reads::role_update(&stored, Some(&later), 0, RELEASE_UPDATE).unwrap();
    assert_works(&u2.url);
    assert!(params(&u2.url).contains(&(
        "WafLogResults".into(),
        "aws-waf-logs-acme/athena-results/".into()
    )));
    // A later release's ordinary update keeps the group on.
    stored.waf_log_results = later.waf_log_results.clone();
    let plain = log_reader::update_url(RELEASE_UPDATE, "us-east-1", &stored.chosen());
    assert!(plain.contains("param_WafLogs=on") && plain.contains("param_WafLogResults="));
    // The run asks nobody: its access question becomes a proceed.
    let ask = json!({"verdict":"ask","assumption":"","person_only":"access",
        "question":"Can someone grant the team's read-only role Glue and Athena access? glue:GetDatabases was denied for CoderHelm's role.",
        "tried":["glue:GetDatabases was denied"],"options":["Grant access"],"default":"Grant access",
        "deadline_hours":4,"blocker":"","evidence":[],"category":"","location":""});
    let m = Recorded(Mutex::new(vec![ask.clone(), ask]));
    let mut pic = Picture::new(Kind::Ask, "plan", "the planner asks before it can plan");
    pic.ticket_body = got.text.clone();
    let j = run(sj::judge(&m, pic));
    assert!(j.proceeds(), "{:?}", j.verdict());
    assert!(j.run_question().is_none());
}

#[test]
fn no_script_writes_a_permission_and_the_script_role_moves_over() {
    // The pasted script only makes the database user and creates or updates
    // the role stack from this release's template.
    let s = common::live::aws::setup_script(
        "654654210434",
        "0b5f1a5e-0000-4000-8000-000000000000",
        "https://coderhelm-public.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader.yaml",
        "",
    );
    assert!(!s.contains("put-role-policy") && !s.contains("create-role"));
    assert!(s.contains("aws cloudformation update-stack") && s.contains("UsePreviousValue"));
    // A database source on the script-made role: the update link names its
    // cluster, and it moves only onto the account's stack role.
    let src = AwsTarget {
        role_arn: format!("arn:aws:iam::{ACCOUNT}:role/CoderhelmReadOnly"),
        region: "us-east-1".into(),
        cluster_arn: format!("arn:aws:rds:us-east-1:{ACCOUNT}:cluster:app-db"),
        secret_arn: format!(
            "arn:aws:secretsmanager:us-east-1:{ACCOUNT}:secret:coderhelm-readonly-app-db-AbCdEf"
        ),
        database: "app".into(),
        ..Default::default()
    };
    let mut a = old_role();
    a.permissions = None;
    a.database_clusters = vec![src.cluster_arn.clone()];
    let u = aws_role_reads::role_update(&a, None, 1, RELEASE_UPDATE)
        .expect("the move is the same item");
    assert_works(&u.url);
    assert!(u.reasons[0].contains("CoderhelmReadOnly"));
    let next = common::role_move::candidate(&src, &a).unwrap();
    assert_eq!(next.role_arn, a.role_arn);
    assert_eq!(next.cluster_arn, src.cluster_arn);
    // The sweep switches a source only after its own check passed.
    let sweep = include_str!("../aws_permissions.rs");
    let tried = sweep.find("role_move::try_move(").unwrap();
    let saved = sweep.find("store::put_source(").unwrap();
    assert!(tried < saved);
}
