//! bo. An alert route sends a CloudWatch signature-drift alarm to a team's
//! terraform repository: a WAF Count rule seeing a crawler that got
//! past a Block rule by changing its JA4 or user agent, fixed by a pull
//! request that updates the block rule's match. The run ended `needs_input`
//! with a long question asking a person to grant the team's read-only role
//! WAF and Athena access, "Default if unanswered in 4 hours: Grant access".
//! `wafv2:ListWebACLs` and `GetSampledRequests` were denied: the role's
//! setup script had never added any WAF read. The web ACL logs to an
//! `aws-waf-logs-*` bucket with an Athena table, and nothing read it.
//!
//! Guarantee: the role's setup script adds `coderhelm-read-waf` (WAF reads,
//! Athena queries over the WAF log tables with results only under the
//! workgroup's results prefix). With those reads the run gathers the rule's
//! matches (sampled requests, and the logs in S3 by JA4, JA3, user agent,
//! accept language, path and cookie presence) before it plans, and goes on
//! to its pull request. Without them, the refusal is the install's one
//! setup item ("AWS role reads", with the one-line CloudShell update),
//! never a question or a wait on the run: the stop judge turns such an ask
//! or hold into a proceed, the hand-back list doesn't carry it, and an
//! access question never shows a default that can't happen.

use common::alarm_evidence::{self as ae, LogTable};
use common::aws_role_reads;
use common::stop_judge::{self as sj, JudgeModel, Kind, Picture};
use serde_json::{json, Value};
use std::sync::Mutex;

const OLD_DEFAULT: &str = "Default if unanswered in 4 hours: Grant access";

/// The ticket the alert route wrote for the alarm.
const TICKET: &str = "- Alarm: acme-crawler-a-signature-drift\n\
    - State: ALARM (was OK)\n\
    - Description: CrawlerASignatureCount counts crawler A requests BlockCrawlerASignature didn't block.\n\
    - Metric: AWS/WAFV2 / CountedRequests\n\
    - Dimensions: WebACL=acme-web-acl, Rule=CrawlerASignatureCount\n";

/// The question the run asked, in its words.
const ASKED: &str = "Can someone grant the team's read-only role WAF and Athena access? \
    wafv2:ListWebACLs and GetSampledRequests were denied for CoderHelm's role, origin telemetry \
    sees every user agent as CloudFront, and no connected source can read the WAF logs in the \
    aws-waf-logs bucket.";

/// Plays back the model's answers in order.
struct Recorded(Mutex<Vec<Value>>);

impl JudgeModel for Recorded {
    async fn answer(&self, _: &str, _: &str, _: &Value) -> Result<Value, String> {
        self.0.lock().unwrap().pop().ok_or_else(|| "no more".into())
    }
}

fn the_ask() -> Value {
    json!({"verdict":"ask","assumption":"","person_only":"access","question":ASKED,
        "tried":["wafv2:ListWebACLs was denied","GetSampledRequests was denied",
                 "origin logs see every user agent as CloudFront"],
        "options":["Grant access","Paste the crawler's new JA4 and user agent"],
        "default":"Grant access","deadline_hours":4,"blocker":"","evidence":[],
        "category":"","location":""})
}

fn run<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(f)
}

/// The Glue table over the web ACL's log bucket, as the team's terraform
/// defines it (WAF's documented columns, an hourly projected partition).
fn log_table() -> LogTable {
    let params: std::collections::HashMap<String, String> = [
        ("projection.enabled", "true"),
        ("projection.log_time.type", "date"),
        ("projection.log_time.format", "yyyy/MM/dd/HH"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    LogTable {
        database: "acme_waf".into(),
        table: "waf_logs".into(),
        columns: [
            "timestamp",
            "terminatingruleid",
            "httprequest",
            "ja3fingerprint",
            "ja4fingerprint",
            "nonterminatingmatchingrules",
            "log_time",
        ]
        .iter()
        .map(|c| c.to_string())
        .collect(),
        date_partitions: ae::date_partitions(&["log_time".to_string()], &params),
    }
}

#[test]
fn with_waf_reads_the_run_gathers_the_evidence_and_moves_to_a_pr() {
    let alarm = ae::waf_alarm(TICKET).expect("a WAF alarm");
    assert_eq!(alarm.rule, "CrawlerASignatureCount");
    // The logging configuration names the bucket; its Glue table is found.
    let bucket = ae::bucket_of("arn:aws:s3:::aws-waf-logs-acme").unwrap();
    assert!(ae::table_in_bucket(
        "s3://aws-waf-logs-acme/AWSLogs/",
        &bucket
    ));
    let end = 1_794_916_800;
    let q = ae::athena_query(&log_table(), &alarm.rule, end - 3 * 3600, end).unwrap();
    assert!(q.contains("\"log_time\" BETWEEN '2026/11/17/09' AND '2026/11/17/12'"));
    assert!(q.contains("r.ruleid = 'CrawlerASignatureCount'"));
    assert!(!q.contains("clientip"));
    // What the reads returned.
    let samples = vec![ae::sample(
        40,
        &[
            ("User-Agent".into(), "CrawlerA/3.1".into()),
            ("Accept-Language".into(), "en-US".into()),
            ("Cookie".into(), "session=private-value".into()),
        ],
        "GET",
        "/listings/42?page=2",
        "US",
        &[],
    )];
    let rows: Vec<Vec<Option<String>>> = [
        vec![
            "ja4",
            "ja3",
            "ua",
            "accept_language",
            "path",
            "has_cookie",
            "requests",
        ],
        vec![
            "t13d1516h2_8daaf6152771_e5627efa2ab1",
            "773906b0efdefa24a7f2b8eb6985bf37",
            "CrawlerA/3.1",
            "en-US",
            "/listings/42",
            "false",
            "812",
        ],
    ]
    .iter()
    .map(|r| r.iter().map(|v| Some(v.to_string())).collect())
    .collect();
    let mut lines = ae::summarize(&samples);
    lines.extend(ae::athena_lines(&rows));
    let got = ae::evidence(&alarm, lines, vec![]);
    assert!(got.refused.is_empty());
    assert!(aws_role_reads::gap("111122223333", "", &got.refused, "x", "t").is_none());
    let ticket = ae::with_evidence(TICKET, &got.text);
    assert!(ticket.contains(ae::HEADING));
    assert!(ticket.contains("JA4 t13d1516h2_8daaf6152771_e5627efa2ab1"));
    assert!(ticket.contains("CrawlerA/3.1"));
    assert!(!ticket.contains("private-value") && !ticket.contains("page=2"));
    assert!(!ticket.contains(aws_role_reads::ITEM_LABEL));
    // The planner reads it: the evidence joins the ticket before planning.
    let src = include_str!("../mod.rs");
    let read = src.find("alarm_evidence_for_run(state, msg)").unwrap();
    let plan = src[read..].find("plan::run(").unwrap();
    assert!(plan > 0);
    // Nothing for a person: the judge proceeds and the run builds its PR.
    let m = Recorded(Mutex::new(vec![json!({"verdict":"proceed",
        "assumption":"update BlockCrawlerASignature to match the JA4 and user agent the logs show",
        "question":"","person_only":"","tried":[],"options":[],"default":"","deadline_hours":0,
        "blocker":"","evidence":[],"category":"","location":""})]));
    let j = run(sj::judge(
        &m,
        Picture::new(Kind::Ask, "plan", "the planner asks"),
    ));
    assert!(j.proceeds());
}

#[test]
fn without_waf_reads_no_question_and_one_install_item() {
    let alarm = ae::waf_alarm(TICKET).unwrap();
    let refused = vec![
        "wafv2:ListWebACLs".to_string(),
        "wafv2:GetSampledRequests".to_string(),
    ];
    let got = ae::evidence(
        &alarm,
        vec![
            "Couldn't find the web ACL: CoderHelm's AWS role doesn't allow wafv2:ListWebACLs yet."
                .into(),
        ],
        refused.clone(),
    );
    // One short line in the run names the install's setup item.
    assert!(got.text.contains("\"AWS role reads\""));
    assert!(got.text.contains("asks nobody"));
    // The install-level item exists, with the one-line update for the role.
    let role = "arn:aws:iam::111122223333:role/CoderhelmReadOnly";
    let gap = aws_role_reads::gap(
        "111122223333",
        role,
        &got.refused,
        "an AWS WAF alarm's evidence",
        "t",
    )
    .expect("the install item");
    assert_eq!(gap.actions, refused);
    let line = common::live::aws::waf_update_line(&gap.role_arn).unwrap();
    assert!(line.ends_with("| base64 -d | bash"));
    let checks = include_str!("../../../../gateway/src/routes/setup_checks.rs");
    assert!(checks.contains("aws_role_reads(&gaps)"));
    assert!(checks.contains("common::aws_role_reads::list("));
    // The run's question becomes a proceed, asked twice or not.
    let m = Recorded(Mutex::new(vec![the_ask(), the_ask()]));
    let mut p = Picture::new(Kind::Ask, "plan", "the planner asks before it can plan");
    p.ticket_body = got.text.clone();
    let j = run(sj::judge(&m, p));
    assert!(j.proceeds(), "{:?}", j.verdict());
    assert!(j.run_question().is_none());
    assert!(common::handback::HandBack::of_judged(&j).is_none());
    assert!(crate::passes::supervisor::hand_back_for(&j, ASKED).is_none());
    assert_eq!(common::handback::human_only_in(ASKED), None);
    // Every place a question is turned into a wait takes the judge's verdict.
    let src = include_str!("../stop_judge.rs");
    assert!(src.contains("judged.question().filter(|q| q.has_default())"));
}

#[test]
fn an_access_question_never_shows_a_default_that_cant_happen() {
    // Access that isn't the AWS role's (a credential) is still asked, with
    // no default to take.
    let ask = json!({"verdict":"ask","assumption":"","person_only":"access",
        "question":"Add the staging API key to the sandbox secrets?","tried":["read the ticket"],
        "options":["Grant access","Skip the staging check"],"default":"Grant access",
        "deadline_hours":4,"blocker":"","evidence":[],"category":"","location":""});
    let m = Recorded(Mutex::new(vec![ask]));
    let j = run(sj::judge(&m, Picture::new(Kind::Ask, "plan", "asks")));
    let q = j.run_question().expect("still asked");
    assert!(!q.why.contains(OLD_DEFAULT));
    assert!(!q.why.contains("Default if unanswered"));
    assert!(q.why.contains("no default"));
    assert!(!j.question_markdown().unwrap().contains("I go ahead"));
}
