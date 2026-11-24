//! bq. A card said "1 new log finding: Seen again: Request failed with 503
//! in AUTH GUARD": two lines of an Axios 503 logged by an API's auth guard,
//! "the evidence does not show which upstream service returned the 503, nor
//! any effect on other services or endpoints", an AWS account id line, and a
//! "Review findings" link. The error happens most days; two lines was its
//! usual rate, and nothing had read the guard's code, which names the
//! service it calls. Each analysis also sent every line of a burst to the
//! model as its own evidence.
//!
//! Guarantee: before anyone is told, the sighting is judged against the
//! finding's own week (`common::log_triage`, the same cases as the
//! analyzer): at its usual rate it is noise, counted with no card and no
//! fix; rising is act; new or back after a quiet week is watch, said once.
//! A failed call's code is read (the class its lines name, found by its
//! definition): the card names the upstream it calls (host and setting),
//! its timeout and whether it retries, with the lines today, the tasks and
//! the week's rate, and the decision on the first line. No account id line.
//! A burst is folded into one entry before any model call, a known error is
//! counted without the model, and the team's hourly budget caps analyses
//! (the analyzer's tests: `lambda/tests/test_log_triage.py`).

use chrono::{DateTime, Duration, Utc};
use common::log_findings::{Counted, Finding, Severity};
use common::log_incident::{
    diagnose, incident_message, plan, Action, Change, CodeRef, Commit, Domain, Incident, Input,
    Look, Lookup, Plan,
};

const GROUP: &str = "/ecs/prod-harbor-api";
const REPO: &str = "acme/harbor-api";
const NOW: &str = "2030-03-14T15:10:00Z";
const LINE: &str = "[Nest] 1  - 3/14/2030, 2:26:14 PM   ERROR [AuthGuard] AxiosError: Request failed with status code 503";
/// What the card said.
const OLD_WORDS: [&str; 3] = [
    "The evidence does not show",
    "AWS account",
    "new log finding",
];

fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(NOW)
        .unwrap()
        .with_timezone(&Utc)
}

/// The guard as the team wrote it.
const GUARD: &str = r#"import { Injectable, CanActivate } from '@nestjs/common';
@Injectable()
export class AuthGuard implements CanActivate {
  constructor(private http: HttpService, private config: ConfigService) {}
  async canActivate(ctx) {
    const base = this.config.get<string>('IDENTITY_URL') ?? 'http://identity.harbor.internal';
    const res = await this.http.axiosRef.get(`${base}/v1/session`, { timeout: 5000 });
    return res.data.valid;
  }
}
"#;

struct Account;

#[async_trait::async_trait]
impl Look for Account {
    async fn domains(&self) -> Lookup<Vec<Domain>> {
        Ok(vec![])
    }
    async fn changes(&self, _r: &str) -> Lookup<Vec<Change>> {
        Ok(vec![])
    }
    async fn code(&self, query: &str) -> Vec<CodeRef> {
        if query == "class AuthGuard" {
            vec![CodeRef {
                repo: REPO.into(),
                path: "src/auth/auth.guard.ts".into(),
                fragment: "export class AuthGuard implements CanActivate {".into(),
            }]
        } else {
            vec![]
        }
    }
    async fn history(&self, _r: &str, _p: &str) -> Vec<Commit> {
        vec![]
    }
    async fn lines_since(&self, _f: &Finding, _s: &str) -> Option<u64> {
        None
    }
    async fn read(&self, repo: &str, path: &str) -> Option<String> {
        (repo == REPO && path == "src/auth/auth.guard.ts").then(|| GUARD.to_string())
    }
}

/// The finding with `today` lines today and `per_day` on each of the five
/// days before.
fn finding(today: u64, per_day: u64) -> Finding {
    let day = |n: i64| (now() - Duration::days(n)).format("%Y-%m-%d").to_string();
    let mut daily: Vec<Counted> = (1..=5)
        .map(|n| Counted {
            name: day(n),
            lines: per_day,
        })
        .collect();
    daily.push(Counted {
        name: day(0),
        lines: today,
    });
    Finding {
        rec_id: "01auth".into(),
        status: "pending".into(),
        severity: Severity::Warning,
        title: "Request failed with 503 in AUTH GUARD".into(),
        summary: "The sample shows an AxiosError with status 503 logged by the AUTH GUARD.".into(),
        source_log_group: GROUP.into(),
        source_account_id: "111122223333".into(),
        error_pattern: "Request failed with status code 503".into(),
        first_seen_at: "2030-03-09T09:00:00Z".into(),
        last_seen_at: "2030-03-14T14:26:14Z".into(),
        reopen_count: 3,
        daily_lines: daily,
        log_streams: vec![
            Counted {
                name: "api/api/a".into(),
                lines: 1,
            },
            Counted {
                name: "api/api/b".into(),
                lines: 1,
            },
        ],
        ..Default::default()
    }
}

async fn diag(f: &Finding) -> common::log_incident::Diagnosis {
    diagnose(
        &Account,
        &Input {
            finding: Some(f),
            samples: vec![LINE.to_string()],
        },
        now(),
    )
    .await
}

#[tokio::test]
async fn two_lines_at_the_usual_rate_are_counted_and_nobody_is_told() {
    let d = diag(&finding(2, 3)).await;
    assert!(d.usual, "{d:?}");
    assert_eq!(
        d.decision,
        "Noise: At its usual rate: 2 lines, about 2 a day on 5 of the last 7 days."
    );
    // The sweep counts it and posts nothing, starts nothing.
    assert_eq!(plan(&d), Plan::Code);
    let sweep = include_str!("../log_autofix.rs");
    assert!(sweep.contains("if inc.diagnosis.usual && plan(&inc.diagnosis) == Plan::Code {"));
    assert!(sweep.contains("&Skip::Usual(reason.clone())"));
    let analyzer = include_str!("../../../../../lambda/log-analyzer/handler.py");
    assert!(analyzer.contains("if cand.get(\"notify\"):"));
    assert!(!analyzer.contains("if is_new or cand.get(\"seen_again\"):"));
}

#[tokio::test]
async fn a_rise_names_the_upstream_from_the_code_with_the_numbers() {
    let f = finding(400, 3);
    let d = diag(&f).await;
    assert!(!d.usual);
    assert!(
        d.decision
            .starts_with("Act: Rising: 400 lines against about 2 a day"),
        "{}",
        d.decision
    );
    assert_eq!(d.upstream, "identity.harbor.internal");
    assert_eq!(
        d.facts[0],
        "AuthGuard (src/auth/auth.guard.ts in acme/harbor-api) calls identity.harbor.internal (IDENTITY_URL), with a 5000 timeout, and no retry."
    );
    assert!(
        d.facts.contains(
            &"400 lines today from 2 tasks; seen on 5 of them, about 2 a day over the last 7 days."
                .to_string()
        ),
        "{:?}",
        d.facts
    );
    assert!(
        d.steps[0].starts_with("Retry AuthGuard's call to identity.harbor.internal once"),
        "{:?}",
        d.steps
    );

    let inc = Incident {
        id: "inc1".into(),
        lead: f.rec_id.clone(),
        members: vec![f.rec_id.clone()],
        log_groups: vec![GROUP.into()],
        diagnosis: d,
        ..Default::default()
    };
    let card = incident_message(
        &inc,
        &[(f.rec_id.clone(), f.title.clone())],
        &Action::Started {
            repo: REPO.into(),
            why: "from the code that writes the error".into(),
        },
        "https://app.example/logs?incident=inc1",
    );
    let text = format!("{card:?}");
    assert!(
        text.contains("(\"Decision\", \"Act: Rising: 400 lines"),
        "{text}"
    );
    assert!(
        text.contains("(\"Upstream\", \"identity.harbor.internal\")"),
        "{text}"
    );
    assert!(
        text.contains("AuthGuard call to identity.harbor.internal answered 503"),
        "{text}"
    );
    for old in OLD_WORDS {
        assert!(!text.contains(old), "{old}: {text}");
    }
}

#[test]
fn the_analyzer_folds_bursts_and_says_it_plainly() {
    let analyzer = include_str!("../../../../../lambda/log-analyzer/handler.py");
    for call in [
        "evidence = triage.collapse(attribution.build_evidence(batches, scrub_secrets))",
        "evidence, known = count_known(",
        "elif not triage.take_model_call(aws_insights_table, team_id):",
    ] {
        assert!(
            analyzer.contains(call),
            "the analyzer no longer has: {call}"
        );
    }
    assert!(!analyzer.contains("CloudWatch Logs · AWS account"));
    assert!(!analyzer.contains("{title} in AWS account"));
    let tests = include_str!("../../../../../lambda/tests/test_log_triage.py");
    assert!(tests.contains("def test_ten_thousand_lines_are_one_model_call_and_one_finding"));
}
