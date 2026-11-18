//! cx. An AWS WAF signature-drift alarm (a Count rule counting a crawler's
//! requests that got past a Block rule) reached a team's terraform
//! repository. The account's role stack predated the template's WAF reads,
//! so the alarm's evidence was refused; CoderHelm refused the alarm's own
//! metric itself ("AWS/WAFV2 isn't a namespace Coderhelm reads"); five
//! New Relic lookups were labelled "no lookups were made"; and the run
//! ended `needs_outside_change` with "Needed from you: Nothing", asking a
//! person to run an Athena query.
//!
//! Guarantee: any namespace is read (the role is the boundary); the rule's
//! matches are read without Athena (sampled requests, the log objects in
//! the bucket); every lookup counts; and a run with nothing left for a
//! person waits for the install's "AWS role reads" item (`awaiting_setup`)
//! and goes on by itself to its pull request once the role reads.

use common::alarm_evidence as ae;
use common::aws_role_reads::{self, Gap};
use common::live::metrics;
use common::role_wait::{self, Ending, Waiting};
use common::run_status;
use common::waf_logs::{LogLocation, Tally};
use serde_json::json;

use crate::passes::fix_location::{self, FixDecision, Route};

const OLD_REFUSAL: &str = "isn't a namespace Coderhelm reads";
const OLD_LABEL: &str = "Not checked against the data (no lookups were made)";
const ACCOUNT: &str = "111122223333";
const ROLE: &str = "arn:aws:iam::111122223333:role/CoderHelmLogReader";

/// The ticket the alert route wrote, in the run's shape.
const TICKET: &str = "ALARM: example-crawler-signature-drift\n\n\
    - Alarm: example-crawler-signature-drift\n\
    - State: ALARM (was OK)\n\
    - Description: ExampleCrawlerCount (Count) counts listing loads ExampleCrawlerBlock didn't block.\n\
    - Metric: AWS/WAFV2 / CountedRequests\n\
    - Dimensions: WebACL=example-web-acl, Rule=ExampleCrawlerCount\n\
    - Statistic: SUM\n";

/// The decision the planner wrote, in the run's shape: data-backed, its
/// one person step the role's reads.
fn decision() -> FixDecision {
    serde_json::from_value(json!({
        "fix_location": "outside_code",
        "intent": "bug",
        "cause": "Since 10:00 UTC both the block rule's blocks and the count rule's counts rose: part of the crawler's traffic no longer matches the identity. Which attribute changed is only in the WAF request logs.",
        "evidence": {
            "basis": "data",
            "looked_up": [
                "T2 metrics_query AWS/WAFV2 CountedRequests: refused",
                "T3 NRQL aws.wafv2 hourly: count rule 277, 357, 422; block rule 234, 447, 330",
                "T4 aws_identity: neither role lists Athena"
            ],
            "code_rule": "The block rule blocks listing loads that match the crawler's identity; the count rule counts what it misses.",
            "data_shows": "T3: counts rose from 104-199 an hour to 277-422.",
            "producer": {"kind": "source", "where": "Requests to web ACL example-web-acl that match the count rule but not the block rule; recorded in the WAF logs (S3 aws-waf-logs-example), unread"},
            "scope": "About 150-300 requests an hour above the overnight residual"
        },
        "outside_change": {
            "system": "AWS Athena, workgroup waf_logs",
            "change": "Run the query on the WAF logs for the count rule's matches grouped by JA4 prefix, User-Agent, Accept-Language and cookie presence. Or grant CoderHelm's role Athena read so CoderHelm runs it.",
            "where": "An engineer with the Athena workgroup, or an AWS admin approving CoderHelm's role stack update",
            "verify": "The result names one identity that makes up most of the rule's matches",
            "steps": [
                {
                    "step": "Read the crawler's new identity from the WAF logs",
                    "by": "person",
                    "owner": "an engineer with Athena access",
                    "how": "Athena workgroup waf_logs",
                    "why_person": "CoderHelm's roles (CoderHelmLogReader, CoderhelmReadOnly) have no athena:StartQueryExecution or s3:GetObject on the WAF log bucket, and wafv2:GetSampledRequests is refused",
                    "grant": "Approve the 'AWS role reads' update of CoderHelm's role stack, adding athena, glue and s3 read on the WAF logs and wafv2:GetSampledRequests",
                    "kind": "other"
                },
                {
                    "step": "Edit the identity values in envs/prod/main.tf and open the PR with the numbers",
                    "by": "coderhelm",
                    "owner": "CoderHelm",
                    "how": "a CoderHelm run with the result",
                    "kind": "other"
                }
            ]
        }
    }))
    .expect("the run's decision")
}

fn gap(seen_at: &str) -> Gap {
    aws_role_reads::gap(
        ACCOUNT,
        ROLE,
        &[
            "wafv2:ListWebACLs".to_string(),
            "wafv2:GetSampledRequests".to_string(),
        ],
        "an AWS WAF alarm's evidence",
        seen_at,
    )
    .expect("a gap")
}

#[test]
fn the_waf_metric_is_read_and_every_lookup_counts() {
    let alarm = ae::waf_alarm(TICKET).expect("a WAF alarm");
    // T2 again: the alarm's own metric, both rules, read in us-east-1.
    let q = metrics::check_query(
        &[
            json!({"namespace":"AWS/WAFV2","metric":"CountedRequests","dimensions":{"WebACL":alarm.web_acl,"Rule":alarm.rule},"stat":"Sum"}),
            json!({"namespace":"AWS/WAFV2","metric":"BlockedRequests","dimensions":{"WebACL":alarm.web_acl,"Rule":"ExampleCrawlerBlock"},"stat":"Sum"}),
        ],
        Some(3600),
        Some("1d"),
        None,
        None,
        1_795_007_000,
    )
    .expect("AWS/WAFV2 is read");
    assert_eq!(q.region.as_deref(), Some("us-east-1"));
    let any = metrics::check_namespace("Custom/Anything").unwrap();
    assert_eq!(any, "Custom/Anything");
    let src = include_str!("../../../../common/src/live/metrics.rs");
    assert!(!src.contains(&format!("\"{{ns}} {OLD_REFUSAL}")));
    // AWS's own refusal is the role's setup item, never ours.
    let e = common::live::StepError::role_refused(&["cloudwatch:GetMetricData"]);
    assert!(!e.is_refusal() && e.message.contains(aws_role_reads::ITEM_LABEL));
    // Five monitoring lookups are lookups: the decision is not "unchecked".
    let d = decision();
    let made = fix_location::lookups_made(0, 5);
    assert!(!fix_location::needs_lookup(Some(&d), true, made, &[]));
    assert!(fix_location::needs_lookup(
        Some(&d),
        true,
        fix_location::lookups_made(0, 0),
        &[]
    ));
    let plan = include_str!("../plan.rs");
    assert!(plan.contains("ops.as_ref().map_or(0, |o| o.lookups().len())"));
    assert!(!fix_location::outside_report(&d, true).contains(OLD_LABEL));
}

#[test]
fn without_the_roles_waf_reads_the_run_waits_and_raises_the_item() {
    let d = decision();
    let report = fix_location::outside_report(&d, true);
    let left = common::person_gate::sink(&report);
    // The run's report: nothing for a person.
    assert!(left.human.is_empty(), "{:?}", left.human);
    // It met the role's refusals this run, so it waits; never an outside
    // change with nobody to make it.
    let started = "2026-11-18T12:57:29Z";
    let gaps = vec![gap("2026-11-18T12:57:36Z"), gap("2026-10-01T00:00:00Z")];
    let refused = role_wait::refused_this_run(&gaps, started);
    let Ending::AwaitRole(waiting_on) = role_wait::ending(&left.human, refused) else {
        panic!("the run waits for the role's reads");
    };
    let w = Waiting::of("run-1", &waiting_on, "2026-11-18T13:04:33Z");
    assert_eq!(w.accounts, vec![ACCOUNT]);
    let note = role_wait::waiting_note(&w);
    assert!(note.contains("\"AWS role reads\"") && note.contains("goes on by itself"));
    assert!(run_status::ALL.contains(&run_status::AWAITING_SETUP));
    // The one install-level item, with the role stack's update.
    let account = common::live::aws_account::AwsAccount {
        account: ACCOUNT.into(),
        role_arn: ROLE.into(),
        region: "us-east-1".into(),
        ..Default::default()
    };
    assert!(aws_role_reads::role_update(
        &account,
        Some(&waiting_on[0]),
        0,
        "https://b/launch/v1/coderhelm-log-reader-update.yaml"
    )
    .is_some());
    // Nothing for a person and nothing to wait for: plan again.
    assert_eq!(role_wait::ending(&[], vec![]), Ending::PlanAgain);
    // The pipeline decides this way, and the re-checks are armed and sent.
    let m = include_str!("../mod.rs");
    let body = &m[m.find("async fn finish_without_code(").unwrap()..];
    assert!(body.contains("common::role_wait::ending(&left.human"));
    assert!(body.contains("common::run_status::AWAITING_SETUP"));
    assert!(body.contains("role_wait::arm("));
    assert!(m.contains("NoCodeEnd::PlanAgain(why)"));
    assert!(include_str!("../../main.rs").contains("passes::role_wait::check(&state, &msg)"));
    assert!(include_str!("../aws_permissions.rs").contains("role_wait::wake(state, team_id, None)"));
}

#[test]
fn once_the_role_reads_the_run_reads_the_logs_and_plans_its_pr() {
    let alarm = ae::waf_alarm(TICKET).unwrap();
    let w = Waiting::of("run-1", &[gap("t")], "t");
    // A re-check refused nothing: the run goes on.
    assert!(role_wait::ready(&w, &[]));
    assert!(!role_wait::ready(&w, &["wafv2:GetSampledRequests".into()]));
    // The sampled requests and the log objects, read without Athena.
    let samples = vec![ae::sample(
        40,
        &[
            ("User-Agent".into(), "Mozilla/5.0 Chrome/131.0.0.0".into()),
            ("Accept-Language".into(), "en-US,en;q=0.9".into()),
        ],
        "GET",
        "/listings?sport=a",
        "US",
        &[],
    )];
    let loc = LogLocation::of(
        "arn:aws:wafv2:us-east-1:111122223333:global/webacl/example-web-acl/0abc",
        "arn:aws:s3:::aws-waf-logs-example",
    )
    .unwrap();
    assert!(loc.hour_prefixes(1_795_000_000, 1_795_007_000)[0]
        .starts_with("AWSLogs/111122223333/WAFLogs/cloudfront/example-web-acl/"));
    let mut t = Tally::default();
    for i in 0..3 {
        let line = json!({
            "timestamp": 1_795_000_000_000i64 + i,
            "terminatingRuleId": "Default_Action",
            "action": "ALLOW",
            "nonTerminatingMatchingRules": [{"ruleId": alarm.rule, "action": "COUNT"}],
            "httpRequest": {"clientIp": "198.51.100.7", "country": "US", "uri": "/listings",
                "headers": [{"name": "User-Agent", "value": "Mozilla/5.0 Chrome/131.0.0.0"},
                            {"name": "Accept-Language", "value": "en-US,en;q=0.9"}]},
            "ja4Fingerprint": "t13d1517h2_8daaf6152771_b6f405a00624"
        });
        t.add(&line.to_string(), &alarm.rule, 0, i64::MAX);
    }
    let mut lines = ae::summarize(&samples);
    lines.extend(t.lines(&alarm.rule));
    let got = ae::evidence(&alarm, lines, vec![]);
    assert!(got.refused.is_empty());
    assert!(got.text.contains("t13d1517h2_8daaf6152771"));
    assert!(!got.text.contains("198.51.100.7"));
    assert!(!got.text.contains(aws_role_reads::ITEM_LABEL));
    // With the data, the plan is a code change: the run opens its PR.
    let mut d = decision();
    d.fix_location = fix_location::FixLocation::Code;
    d.outside_change = None;
    d.evidence.producer.kind = "source".into();
    assert_eq!(
        fix_location::route_for_plan(Some(&d), "1. Update the identity in envs/prod/main.tf"),
        Route::Code
    );
    // The waiting run starts again from its ticket, with its sender, so the
    // alarm's evidence is read again.
    let src = include_str!("../role_wait.rs");
    assert!(src.contains("stop_judge::restart_message(item"));
    assert!(src.contains("ticket.sender = sender"));
}
