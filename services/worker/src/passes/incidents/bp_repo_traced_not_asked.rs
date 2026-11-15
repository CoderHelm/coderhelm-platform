//! bp. A secrets finding on a production API's log group posted "Which repo
//! should this fix go to?" with the right repo as its best guess and two
//! others (the infrastructure repo and a command line tool) as "also
//! possible", and no fix started. Its reason ended "there is no direct AWS
//! pointer": the ECS task definition writing to the log group runs an image
//! from an ECR repository named for the app repo plus its environment word,
//! tagged with the app's name and with the commit it was built from, and the
//! app repo's workflow pushes that image. None of it was read as evidence:
//! the image repository had to match the repo's name exactly, a bare tag
//! value never counted, a hit in a workflow counted the same as one in
//! Terraform, the classes in the error's own lines were never looked for,
//! and the hedged answer was cached for a week, so even a smarter resolver
//! would have kept asking.
//!
//! Guarantee: the repo is traced the way an engineer would: the commit the
//! running image was built from is looked up in every repo's history; a
//! workflow or build file that ships the image counts as shipping it, while
//! Terraform that only defines it is context, not a rival; the image
//! repository's and tags' names count without their environment word; the
//! classes the error's lines name are looked for as definitions and the
//! line's text where it is written. Evidence that points at one repo starts
//! the fix there and saves the repo for the log group, and the run says which
//! repo and why in one line. Split or thin evidence still starts the fix on
//! the best guess, with one card that says so and offers "Wrong repo? Move
//! it"; only a log group nothing ties to any repo asks. An answer from an
//! older resolver, or one that wasn't sure, is never served from the cache
//! for long.

use std::collections::HashMap;

use common::log_incident::{incident_message, Action, Diagnosis, Incident, Place};
use common::log_repo::{
    choose, resolve, Anchor, AwsFacts, Cached, Choice, EcsFact, Env, ImageFact, Input, Kind,
    LogLine, Resolution, Target, RESOLVER,
};
use common::repo_read::{RepoInfo, SearchHit};

const GROUP: &str = "/ecs/prod-ledger-api";
const APP: &str = "acme/ledger-api";
const INFRA: &str = "acme/terraform-infra";
const CLI: &str = "acme/ops-cli";
const SHA: &str = "4c1e9a07b2d85f6e3a90c71d2b48e5f60a1c3d97";
const NOW: &str = "2026-11-12T10:19:20Z";
/// What the card said.
const OLD_WORDS: [&str; 4] = [
    "Which repo should this fix go to?",
    "The evidence is split between repos",
    "We detected",
    "no direct AWS pointer",
];

fn now() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(NOW)
        .unwrap()
        .with_timezone(&chrono::Utc)
}

/// The team's account as the read-only role sees it: the service's task
/// definition logs to the group and runs the image built from `SHA`.
fn account() -> AwsFacts {
    let image = format!("111122223333.dkr.ecr.us-east-1.amazonaws.com/prod-ledger-api:sha-{SHA}");
    AwsFacts {
        connected: true,
        ecs: vec![EcsFact {
            cluster: "arn:aws:ecs:us-east-1:111122223333:cluster/prod-ledger-api-ecs-cluster"
                .into(),
            service: "prod-ledger-api-ecs-service".into(),
            task_definition:
                "arn:aws:ecs:us-east-1:111122223333:task-definition/prod-ledger-api:270".into(),
            family: "prod-ledger-api".into(),
            images: vec![image.clone()],
            ..Default::default()
        }],
        images: vec![ImageFact {
            image,
            ecr_repo: "prod-ledger-api".into(),
            tags: [("App", "ledger-api"), ("Environment", "prod")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn hit(repo: &str, path: &str, fragment: &str) -> SearchHit {
    SearchHit {
        repo: repo.into(),
        path: path.into(),
        fragments: vec![fragment.into()],
    }
}

/// GitHub and the model, faked at the edge; the cache in memory.
struct Team {
    facts: AwsFacts,
    commits: HashMap<&'static str, &'static str>,
    hits: HashMap<String, Vec<SearchHit>>,
    cache: std::sync::Mutex<HashMap<String, Cached>>,
}

impl Team {
    fn new() -> Team {
        let mut hits = HashMap::new();
        // The image repository's name: Terraform defines it, the app's
        // workflow pushes to it.
        hits.insert(
            "prod-ledger-api".to_string(),
            vec![
                hit(INFRA, "modules/ecs/main.tf", "name = \"prod-ledger-api\""),
                hit(
                    APP,
                    ".github/workflows/deploy.yml",
                    "ECR_REPOSITORY: prod-ledger-api",
                ),
            ],
        );
        hits.insert(
            "/ecs/prod-ledger-api".to_string(),
            vec![hit(
                INFRA,
                "modules/ecs/logs.tf",
                "name = \"/ecs/prod-ledger-api\"",
            )],
        );
        hits.insert(
            "class SearchIndexService".to_string(),
            vec![hit(
                APP,
                "src/search/search-index.service.ts",
                "export class SearchIndexService {",
            )],
        );
        Team {
            facts: account(),
            commits: [(APP, SHA)].into_iter().collect(),
            hits,
            cache: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl Env for Team {
    async fn aws_facts(&self, _t: &Target) -> AwsFacts {
        self.facts.clone()
    }
    async fn anchor_changed(&self, _t: &Target, _a: &Anchor) -> bool {
        false
    }
    async fn search(&self, repos: &[String], query: &str) -> Result<Vec<SearchHit>, String> {
        Ok(self
            .hits
            .get(query)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|h| repos.contains(&h.repo))
            .collect())
    }
    async fn profile(&self, _r: &RepoInfo) -> Option<String> {
        None
    }
    async fn has_commit(&self, repo: &RepoInfo, sha: &str) -> Option<bool> {
        Some(
            self.commits
                .get(repo.name.as_str())
                .is_some_and(|c| c.starts_with(sha)),
        )
    }
    /// The model hedges as it did on the card.
    async fn judge(&self, _s: &str, _u: &str) -> Result<String, String> {
        Ok(serde_json::json!({
            "repo": APP,
            "confidence": "medium",
            "reason": "The log group's sibling is saved to this backend repo; there is no direct AWS pointer.",
            "evidence": [],
            "candidates": [
                {"repo": APP, "reason": "backend"},
                {"repo": INFRA, "reason": "infra"},
                {"repo": CLI, "reason": "cli"},
            ],
        })
        .to_string())
    }
    async fn load(&self, key: &str) -> Option<Cached> {
        self.cache.lock().unwrap().get(key).cloned()
    }
    async fn save(&self, key: &str, c: &Cached) {
        self.cache.lock().unwrap().insert(key.into(), c.clone());
    }
}

fn input() -> Input {
    let repo = |n: &str, role: &str| RepoInfo {
        role: Some(role.into()),
        ..RepoInfo::new(n, "main")
    };
    Input {
        target: Target {
            account: "111122223333".into(),
            log_group: GROUP.into(),
            about: "Secrets detected in CloudWatch Logs (2 instances)".into(),
            samples: vec![LogLine {
                log_group: GROUP.into(),
                message: "[Nest] 1  - 11/12/2026, 10:19:20 AM   ERROR [SearchIndexService] Failed to reach the search cluster with password=[REDACTED_PASSWORD]".into(),
            }],
            ..Default::default()
        },
        repos: vec![repo(APP, "backend"), repo(INFRA, "infra"), repo(CLI, "tools")],
        related: vec![("/ecs/prod-ledger-api-worker".into(), APP.into())],
        ..Default::default()
    }
}

fn incident() -> Incident {
    Incident {
        id: "inc1".into(),
        lead: "01sec".into(),
        members: vec!["01sec".into()],
        log_groups: vec![GROUP.into()],
        diagnosis: Diagnosis {
            place: Place::Code,
            headline: "Passwords written to the API's logs".into(),
            summary: format!(
                "{GROUP} has 2 passwords or tokens in plain text (REDACTED_PASSWORD)."
            ),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn the_card_that_asked_now_starts_the_fix_in_the_traced_repo() {
    // Before: the hedged answer, cached by the older resolver for a week.
    let team = Team::new();
    let inp = input();
    let before = Resolution {
        repo: Some(APP.into()),
        confidence: common::log_repo::Confidence::Medium,
        reason:
            "The log group's sibling is saved to this backend repo; there is no direct AWS pointer."
                .into(),
        resolved_at: NOW.into(),
        ..Default::default()
    };
    team.cache.lock().unwrap().insert(
        inp.target.cache_key(),
        Cached {
            resolution: before.clone(),
            anchor: Anchor::default(),
            repos_key: common::log_repo::repos_key(&inp.repos),
            at: NOW.into(),
            resolver: RESOLVER - 1,
        },
    );
    // Even that answer never stops at a question now: it starts on the guess.
    assert!(matches!(choose(&before), Choice::Guess { ref repo, .. } if repo == APP));

    // After: worked out again, from what the account and the code say.
    let r = resolve(&team, &inp, now()).await;
    assert!(!r.cached, "the older resolver's answer was served");
    assert_eq!(choose(&r), Choice::Sure(APP.into()), "{r:?}");
    let kinds: Vec<Kind> = r.decided_by.iter().map(|e| e.kind).collect();
    for k in [
        Kind::ImageCommit,
        Kind::Ships,
        Kind::CodeSymbol,
        Kind::Sibling,
    ] {
        assert!(kinds.contains(&k), "{k:?} missing from {kinds:?}");
    }
    // The infrastructure repo defines the resource; it never rivals the app.
    assert!(r.decided_by.iter().all(|e| e.repo == APP));

    // The run says which repo and why, in one line.
    let action = Action::Started {
        repo: APP.into(),
        why: r.why_line(),
    };
    let inc = incident();
    let said = action.sentence(&inc.diagnosis);
    assert_eq!(
        said,
        "Started a fix in acme/ledger-api (from the running image's commit and the build and deploy files that ship it). The pull request follows here when it opens."
    );
    for old in OLD_WORDS {
        assert!(!said.contains(old), "{said}");
    }
}

#[tokio::test]
async fn a_split_call_starts_on_the_guess_and_says_so_never_only_asks() {
    // Without the commit, the workflow or the sibling, and with the class
    // the lines name defined in two repos, the evidence is split and the
    // model hedges: the fix still starts.
    let mut team = Team::new();
    team.commits.clear();
    team.hits.clear();
    team.hits.insert(
        "class SearchIndexService".to_string(),
        vec![
            hit(
                APP,
                "src/search/search-index.service.ts",
                "export class SearchIndexService {",
            ),
            hit(CLI, "src/search.ts", "class SearchIndexService {"),
        ],
    );
    let mut inp = input();
    inp.related.clear();
    let r = resolve(&team, &inp, now()).await;
    let Choice::Guess { repo, others } = choose(&r) else {
        panic!("{r:?}");
    };
    assert_eq!(repo, APP);
    let card = incident_message(
        &incident(),
        &[(
            "01sec".into(),
            "Secrets detected in CloudWatch Logs (2 instances)".into(),
        )],
        &Action::Guessed {
            repo: repo.clone(),
            others,
            why: r.why_line(),
        },
        "https://app.example/logs?incident=inc1",
    );
    let text = format!("{card:?}");
    assert!(text.contains("Fixing in acme/ledger-api"), "{text}");
    assert!(
        text.contains("Started a fix in acme/ledger-api, the best guess"),
        "{text}"
    );
    assert!(text.contains("Wrong repo? Move it"), "{text}");
    for old in OLD_WORDS.iter().chain(&["Pick the repo"]) {
        assert!(!text.contains(old), "{old}: {text}");
    }
}

#[test]
fn the_sweep_starts_on_every_choice_but_none_and_the_analyzer_reads_plainly() {
    let sweep = include_str!("../log_autofix.rs");
    for call in [
        "(common::log_repo::choose(&r), r)",
        "Choice::Guess { repo, others } => {",
        "Some(Action::Guessed {",
        "Action::Guessed { .. } => Some(\"guess\"),",
    ] {
        assert!(sweep.contains(call), "the sweep no longer has: {call}");
    }
    // Only `Choice::Ask` builds the question.
    assert_eq!(
        sweep.matches("action = Some(Action::WhichRepo {").count(),
        1
    );
    let analyzer = include_str!("../../../../../lambda/log-analyzer/handler.py");
    assert!(!analyzer.contains("We detected"));
    assert!(analyzer.contains("\"samples\": g[\"samples\"]"));
    // The worker reads the lines that carry the value for such a finding.
    let f = common::log_findings::Finding {
        title: "Secrets detected in CloudWatch Logs (2 instances)".into(),
        ..Default::default()
    };
    assert!(common::log_findings::sample_filter(&f).contains("password"));
}
