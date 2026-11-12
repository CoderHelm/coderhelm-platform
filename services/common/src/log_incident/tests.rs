use std::collections::HashMap;
use std::sync::Mutex;

use super::*;

const ACCOUNT: &str = "111122223333";
const OLD_HOST: &str =
    "search-ledger-os-opt2-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com";
const NEW_ENDPOINT: &str =
    "vpc-ledger-os-opt-zyxwvutsrqponmlkjihgfedcba.us-east-1.es.amazonaws.com";

fn t(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

fn code(repo: &str, path: &str, fragment: &str) -> CodeRef {
    CodeRef {
        repo: repo.into(),
        path: path.into(),
        fragment: fragment.into(),
    }
}

/// A fake account and codebase, shaped like the real incident: an
/// OpenSearch domain replaced by a VPC-only one, a service whose setting
/// still named the old host, and a pipeline that writes the missing index.
struct Fake {
    domains: Lookup<Vec<Domain>>,
    changes: Lookup<Vec<Change>>,
    code: HashMap<String, Vec<CodeRef>>,
    history: Vec<Commit>,
    since: Option<u64>,
    asked: Mutex<Vec<String>>,
}

impl Fake {
    fn incident() -> Fake {
        let mut code_hits = HashMap::new();
        code_hits.insert(
            "OPENSEARCH".to_string(),
            vec![code(
                "acme/ledger-api",
                "src/config/search.config.ts",
                "node: process.env.OPENSEARCH_NODE,",
            )],
        );
        code_hits.insert(
            "OPENSEARCH_NODE".to_string(),
            vec![
                code(
                    "acme/ledger-api",
                    "src/config/search.config.ts",
                    "node: process.env.OPENSEARCH_NODE,",
                ),
                code(
                    "acme/infra-live",
                    "envs/prod/ledger-api.tf",
                    "OPENSEARCH_NODE = module.search.endpoint",
                ),
            ],
        );
        code_hits.insert(
            "ledger-os-opt".to_string(),
            vec![code(
                "acme/infra-live",
                "modules/search/main.tf",
                "domain_name = \"ledger-os-opt\"",
            )],
        );
        code_hits.insert(
            "class_index".to_string(),
            vec![
                code(
                    "acme/ledger-api",
                    "src/search/classes.service.ts",
                    "'class_index'",
                ),
                code(
                    "acme/data-jobs",
                    "dags/class_index_sync.py",
                    "INDEX = \"class_index\"",
                ),
            ],
        );
        Fake {
            domains: Ok(vec![
                Domain {
                    name: "ledger-os-opt".into(),
                    endpoint: NEW_ENDPOINT.into(),
                    vpc: true,
                    deleted: false,
                },
                Domain {
                    name: "staging-ledger-os-opt".into(),
                    endpoint: "vpc-staging-ledger-os-opt-x.us-east-1.es.amazonaws.com".into(),
                    vpc: true,
                    deleted: false,
                },
            ]),
            changes: Ok(vec![
                Change {
                    at: "2026-11-12T05:04:32Z".into(),
                    action: "CreateDomain".into(),
                    resource: "ledger-os-opt".into(),
                    by: "deploy-bot".into(),
                },
                Change {
                    at: "2026-11-12T05:18:20Z".into(),
                    action: "DeleteDomain".into(),
                    resource: "ledger-os-opt2".into(),
                    by: "deploy-bot".into(),
                },
            ]),
            code: code_hits,
            history: vec![Commit {
                repo: "acme/infra-live".into(),
                path: "modules/search/main.tf".into(),
                sha: "4f1c2d3e9a".into(),
                message: "Move the search domain into the VPC".into(),
                at: "2026-11-12T04:58:00Z".into(),
            }],
            since: Some(0),
            asked: Mutex::new(vec![]),
        }
    }
}

#[async_trait::async_trait]
impl Look for Fake {
    async fn domains(&self) -> Lookup<Vec<Domain>> {
        self.domains.clone()
    }
    async fn changes(&self, resource: &str) -> Lookup<Vec<Change>> {
        self.changes
            .clone()
            .map(|v| v.into_iter().filter(|c| c.resource == resource).collect())
    }
    async fn code(&self, query: &str) -> Vec<CodeRef> {
        self.asked.lock().unwrap().push(query.to_string());
        self.code.get(query).cloned().unwrap_or_default()
    }
    async fn history(&self, repo: &str, path: &str) -> Vec<Commit> {
        self.history
            .iter()
            .filter(|c| c.repo == repo && c.path == path)
            .cloned()
            .collect()
    }
    async fn lines_since(&self, _f: &Finding, _since: &str) -> Option<u64> {
        self.since
    }
}

fn finding(id: &str, group: &str, title: &str, pattern: &str, first: &str, last: &str) -> Finding {
    Finding {
        rec_id: id.into(),
        status: "pending".into(),
        severity: Severity::Critical,
        title: title.into(),
        error_pattern: pattern.into(),
        source_log_group: group.into(),
        source_account_id: ACCOUNT.into(),
        first_seen_at: first.into(),
        last_seen_at: last.into(),
        created_at: "2026-11-12T09:27:15Z".into(),
        ..Default::default()
    }
}

fn dns_finding() -> Finding {
    finding(
        "dns",
        "/ecs/prod-ledger-api",
        "OpenSearch endpoint DNS ENOTFOUND in /ecs/prod-ledger-api",
        &format!("ConnectionError: getaddrinfo ENOTFOUND {OLD_HOST}"),
        "2026-11-12T05:19:17.975Z",
        "2026-11-12T05:25:18.153Z",
    )
}

fn index_finding() -> Finding {
    finding(
        "idx",
        "/ecs/prod-ledger-api",
        "OpenSearch class_index index missing (404) in /ecs/prod-ledger-api",
        "search on class_index failed (attempt 1/4, status=404, error=ResponseError):",
        "2026-11-12T05:24:23.261Z",
        "2026-11-12T05:26:38.950Z",
    )
}

fn web_finding() -> Finding {
    let mut f = finding(
        "web",
        "/ecs/prod-storefront",
        "Facets failing on OpenSearch errors in /ecs/prod-storefront",
        "errors: [Array],",
        "2026-11-12T05:19:07.249Z",
        "2026-11-12T05:26:39.069Z",
    );
    f.severity = Severity::Warning;
    f
}

const NOW: &str = "2026-11-12T09:30:00Z";

async fn diag(look: &Fake, f: &Finding) -> Diagnosis {
    diagnose(
        look,
        &Input {
            finding: Some(f),
            samples: vec![],
        },
        t(NOW),
    )
    .await
}

#[test]
fn hostnames_name_their_aws_resource() {
    let h = aws_host(OLD_HOST).unwrap();
    assert_eq!(
        (h.service.as_str(), h.name.as_str(), h.vpc),
        ("opensearch", "ledger-os-opt2", false)
    );
    let h = aws_host(NEW_ENDPOINT).unwrap();
    assert_eq!((h.name.as_str(), h.vpc), ("ledger-os-opt", true));
    // Cut short in a log line, it still names the domain.
    assert_eq!(
        aws_host("search-ledger-os-opt2").unwrap().name,
        "ledger-os-opt2"
    );
    assert_eq!(
        aws_host("orders.c1x2y3.us-east-1.rds.amazonaws.com")
            .unwrap()
            .service,
        "rds"
    );
    assert_eq!(aws_host("api.example.com"), None);
    assert_eq!(aws_host("search-results.example.com"), None);
    assert_eq!(stem("ledger-os-opt2"), "ledger-os-opt");
    assert_eq!(stem("ledger-os-opt-v2"), "ledger-os-opt");
    assert_eq!(stem("ledger-os-opt"), "ledger-os-opt");
}

#[test]
fn the_lines_say_what_broke() {
    let s = signals(&[&format!("Error: getaddrinfo ENOTFOUND {OLD_HOST}")]);
    assert_eq!(s[0].kind, ErrorKind::DnsNotFound);
    assert_eq!(s[0].host, OLD_HOST);
    let s =
        signals(&["search on class_index failed (attempt 1/4, status=404, error=ResponseError):"]);
    assert_eq!(
        (s[0].kind, s[0].index.as_str()),
        (ErrorKind::IndexMissing, "class_index")
    );
    let s = signals(&[
        r#"ResponseError: index_not_found_exception: [index_not_found_exception] Reason: no such index [class_index]"#,
    ]);
    assert_eq!(s[0].index, "class_index");
    let s = signals(&["search on class_index returned 503, retrying in 143ms"]);
    assert!(s.iter().all(|x| x.kind != ErrorKind::IndexMissing));
    let s = signals(&["[OpenSearchService] TimeoutError: Request timed out"]);
    assert_eq!(s[0].kind, ErrorKind::Timeout);
    assert!(signals(&["TypeError: cannot read properties of undefined"]).is_empty());
    assert_eq!(
        setting_name("node: process.env.OPENSEARCH_NODE,", "opensearch").as_deref(),
        Some("OPENSEARCH_NODE")
    );
}

#[tokio::test]
async fn a_dns_failure_on_a_replaced_domain_is_a_setting_with_the_new_endpoint_named() {
    let look = Fake::incident();
    let d = diag(&look, &dns_finding()).await;
    // Not the API's code: the setting that still names the old domain.
    assert_eq!(d.place, Place::Infra, "{d:#?}");
    assert_eq!(d.service, "opensearch");
    assert_eq!(d.resource, "ledger-os-opt");
    assert_eq!(
        d.headline,
        "OpenSearch domain ledger-os-opt2 replaced by ledger-os-opt"
    );
    let fix = d.fix.clone().expect("a fix in the repo that sets it");
    assert_eq!(fix.repo, "acme/infra-live");
    assert_eq!(fix.path, "envs/prod/ledger-api.tf");
    assert!(fix.change.contains(NEW_ENDPOINT), "{}", fix.change);
    assert!(fix.change.contains("OPENSEARCH_NODE"), "{}", fix.change);
    assert!(
        d.summary.starts_with(
            "The OpenSearch domain ledger-os-opt2 was replaced by ledger-os-opt (VPC only) at Nov 12 05:18 UTC; /ecs/prod-ledger-api still reads the old hostname from envs/prod/ledger-api.tf in acme/infra-live"
        ),
        "{}",
        d.summary
    );
    assert!(d
        .facts
        .iter()
        .any(|f| f.contains("DeleteDomain on ledger-os-opt2")));
    assert!(d
        .facts
        .iter()
        .any(|f| f.contains("Move the search domain into the VPC")));
    assert!(d.facts.iter().any(|f| f.contains(
        "reads its endpoint from OPENSEARCH_NODE (src/config/search.config.ts in acme/ledger-api)"
    )));
    // It stopped hours ago and nothing came since.
    assert_eq!(d.recovered_at, "2026-11-12T05:25:18.153Z");
}

#[tokio::test]
async fn the_old_hostname_in_code_is_the_line_to_change() {
    let mut look = Fake::incident();
    look.code.insert(
        OLD_HOST.into(),
        vec![code(
            "acme/ledger-api",
            "config/production.json",
            &format!("\"node\": \"https://{OLD_HOST}\""),
        )],
    );
    let d = diag(&look, &dns_finding()).await;
    assert_eq!(d.place, Place::Config, "{d:#?}");
    let fix = d.fix.unwrap();
    assert_eq!(
        (fix.repo.as_str(), fix.path.as_str()),
        ("acme/ledger-api", "config/production.json")
    );
}

#[tokio::test]
async fn a_setting_outside_the_code_is_a_step_for_a_person_and_refusals_are_named() {
    let mut look = Fake::incident();
    look.code.remove("OPENSEARCH_NODE");
    look.changes = Err(Some("cloudtrail:LookupEvents".into()));
    look.since = Some(40);
    let d = diag(&look, &dns_finding()).await;
    assert_eq!(d.place, Place::Config);
    assert_eq!(d.fix, None);
    assert!(
        d.steps[0].starts_with("Set OPENSEARCH_NODE for /ecs/prod-ledger-api to"),
        "{:?}",
        d.steps
    );
    assert!(d.steps[0].contains(NEW_ENDPOINT));
    assert_eq!(d.permissions, vec!["cloudtrail:LookupEvents".to_string()]);
    // Still happening: not recovered.
    assert_eq!(d.recovered_at, "");
}

#[tokio::test]
async fn a_missing_index_after_a_new_domain_is_data_with_the_pipeline_named() {
    let look = Fake::incident();
    let d = diag(&look, &index_finding()).await;
    assert_eq!(d.place, Place::Data);
    assert_eq!(d.resource, "ledger-os-opt");
    assert_eq!(
        d.summary,
        "The new OpenSearch domain ledger-os-opt has no class_index index yet, so /ecs/prod-ledger-api gets 404s."
    );
    assert_eq!(
        d.steps,
        vec!["Rerun class_index_sync (dags/class_index_sync.py in acme/data-jobs) to rebuild the class_index index on ledger-os-opt.".to_string()]
    );
}

#[tokio::test]
async fn one_cause_across_findings_and_services_is_one_incident() {
    let look = Fake::incident();
    let items: Vec<(Finding, Diagnosis)> = {
        let mut v = Vec::new();
        for f in [index_finding(), dns_finding(), web_finding()] {
            let d = diag(&look, &f).await;
            v.push((f, d));
        }
        v
    };
    let incidents = group(&items);
    assert_eq!(incidents.len(), 1, "{incidents:#?}");
    let inc = &incidents[0];
    // The setting leads; the index and the storefront ride along.
    assert_eq!(inc.lead, "dns");
    assert_eq!(inc.members, vec!["dns", "idx", "web"]);
    assert_eq!(
        inc.log_groups,
        vec!["/ecs/prod-ledger-api", "/ecs/prod-storefront"]
    );
    assert_eq!(inc.first_seen, "2026-11-12T05:19:07.249Z");
    assert!(inc.diagnosis.summary.contains("replaced by ledger-os-opt"));
    assert!(inc
        .diagnosis
        .summary
        .contains("has no class_index index yet"));
    assert!(inc
        .diagnosis
        .steps
        .iter()
        .any(|s| s.starts_with("Rerun class_index_sync")));
    assert!(inc.id.starts_with("INC-"));
    // The same findings grouped again keep the id they were given.
    let mut again = items.clone();
    again[0].0.incident_id = inc.id.clone();
    assert_eq!(group(&again)[0].id, inc.id);
    let note = ticket_note(inc, &["OpenSearch class_index index missing".into()]);
    assert!(note.contains("Where the fix belongs: infrastructure as code"));
    assert!(note.contains("Also needed, outside this change: Rerun class_index_sync"));
}

#[tokio::test]
async fn different_causes_or_windows_stay_apart() {
    let look = Fake::incident();
    let code_bug = finding(
        "bug",
        "/ecs/prod-ledger-api",
        "TypeError in the invoice export",
        "TypeError: cannot read properties of undefined (reading 'total')",
        "2026-11-12T05:20:00Z",
        "2026-11-12T05:21:00Z",
    );
    let later = finding(
        "later",
        "/ecs/prod-ledger-api",
        "OpenSearch class_index index missing (404) in /ecs/prod-ledger-api",
        "search on class_index failed (attempt 1/4, status=404, error=ResponseError):",
        "2026-11-12T09:00:00Z",
        "2026-11-12T09:05:00Z",
    );
    let mut items = Vec::new();
    for f in [dns_finding(), code_bug, later] {
        let d = diag(&look, &f).await;
        items.push((f, d));
    }
    assert_eq!(items[1].1.place, Place::Code);
    let incidents = group(&items);
    assert_eq!(incidents.len(), 3);
}

#[tokio::test]
async fn two_findings_with_one_cause_make_one_card_that_says_what_was_done() {
    let look = Fake::incident();
    let mut items = Vec::new();
    for f in [dns_finding(), index_finding()] {
        let d = diag(&look, &f).await;
        items.push((f, d));
    }
    let incidents = group(&items);
    assert_eq!(incidents.len(), 1);
    let inc = &incidents[0];
    // It stopped hours ago: say so; the change stays ready.
    assert_eq!(plan(&inc.diagnosis), Plan::Recovered);
    let titles: Vec<(String, String)> = items
        .iter()
        .map(|(f, _)| (f.rec_id.clone(), f.title.clone()))
        .collect();
    let m = incident_message(
        inc,
        &titles,
        &Action::Recovered,
        "https://app.example/logs?incident=x",
    );
    let text = format!("{m:?}");
    assert!(
        m.summary.starts_with(
            "✅ Recovered on its own: OpenSearch domain ledger-os-opt2 replaced by ledger-os-opt"
        ),
        "{}",
        m.summary
    );
    assert!(text.contains("2 findings, one cause"));
    assert!(
        text.contains("stopped on its own at Nov 12 05:26 UTC"),
        "{text}"
    );
    assert!(text.contains("If it comes back, the change is ready"));
    assert!(!text.contains("Which repo"));

    // Still happening: one fix in the repo that sets the endpoint, and the
    // card (when its PR opens) carries the rest.
    let mut look = Fake::incident();
    look.since = Some(30);
    let mut items = Vec::new();
    for f in [dns_finding(), index_finding()] {
        let d = diag(&look, &f).await;
        items.push((f, d));
    }
    let inc = &group(&items)[0];
    match plan(&inc.diagnosis) {
        Plan::FixIn(p) => {
            assert_eq!(p.repo, "acme/infra-live");
            let s = Action::Started {
                repo: p.repo.clone(),
            }
            .sentence(&inc.diagnosis);
            assert!(
                s.starts_with(
                    "Started a fix in acme/infra-live: Set OPENSEARCH_NODE to vpc-ledger-os-opt-"
                ),
                "{s}"
            );
        }
        other => panic!("{other:?}"),
    }
    // The finding's stored incident becomes the fix ticket's note.
    let mut f = dns_finding();
    f.incident = inc.to_json(&titles, "").to_string();
    let note = stored_note(&f);
    assert!(
        note.contains("Where the fix belongs: infrastructure as code"),
        "{note}"
    );
    assert!(note.contains("The same cause also produced: OpenSearch class_index index missing"));
}

#[test]
fn what_is_left_for_a_person_is_a_listed_reason() {
    use crate::handback::HumanOnly;
    let d = Diagnosis {
        place: Place::Data,
        ..Default::default()
    };
    assert!(matches!(
        plan(&d),
        Plan::ForAPerson(HumanOnly::MissingAccess, _)
    ));
    let d = Diagnosis {
        place: Place::Config,
        ..Default::default()
    };
    assert!(matches!(
        plan(&d),
        Plan::ForAPerson(HumanOnly::Credential, _)
    ));
    let d = Diagnosis {
        place: Place::Aws,
        ..Default::default()
    };
    assert_eq!(plan(&d), Plan::Watching);
    // Code that stopped erroring is still the code's: the fix goes ahead.
    let d = Diagnosis {
        place: Place::Code,
        recovered_at: "2026-11-12T05:26:00Z".into(),
        ..Default::default()
    };
    assert_eq!(plan(&d), Plan::Code);
}
