//! The repo resolution eval: log-group-to-repo cases with made-up names
//! (`eval_cases.json`), each run through the whole resolution with fake AWS
//! and code data and two stand-ins for the model:
//!
//! - an evidence reader, which picks the repo the strongest numbered
//!   evidence names (what a careful model does), and must land on the
//!   expected repo at the expected confidence;
//! - a name matcher, which picks the repo whose name shares the most words
//!   with the log group and always claims high confidence (what a name
//!   rule does). Whatever it picks, the settled resolution must never be
//!   sure unless real evidence backs that repo: a name never decides.
//!
//! It also checks that the resolver's source holds no regular expression,
//! so no pattern match on names can make the decision.
//!
//! `cargo test -p common log_repo::tests::eval -- --nocapture` prints the
//! table.

use super::*;

#[derive(serde::Deserialize)]
struct Hit {
    query: String,
    repo: String,
    path: String,
    #[serde(default)]
    fragment: String,
}

#[derive(serde::Deserialize)]
struct Case {
    name: String,
    log_group: String,
    repos: Vec<String>,
    #[serde(default)]
    facts: AwsFacts,
    #[serde(default)]
    hits: Vec<Hit>,
    #[serde(default)]
    past: Vec<PastFix>,
    expect: Option<String>,
    min: String,
    max: String,
    decoy: String,
    #[serde(default)]
    permissions: Vec<String>,
    #[serde(default)]
    saved: Option<String>,
    #[serde(default)]
    samples: Vec<LogLine>,
}

fn rank_of(tag: &str) -> u8 {
    serde_json::from_value::<Kind>(json!(tag))
        .map(Kind::rank)
        .unwrap_or(0)
}

/// The evidence reader: the strongest evidence's repo; a tie at the top
/// between repos is low confidence.
fn reader(prompt: &str) -> Result<String, String> {
    let mut best: Vec<(u8, String, String)> = Vec::new();
    for l in prompt.lines().filter(|l| l.starts_with('E')) {
        let mut parts = l.splitn(3, ' ');
        let (Some(id), Some(tag), Some(rest)) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        let tag = tag.trim_matches(|c| c == '[' || c == ']');
        let Some(repo) = rest
            .strip_prefix('(')
            .and_then(|r| r.split_once(')'))
            .map(|(r, _)| r.to_string())
        else {
            continue;
        };
        best.push((rank_of(tag), repo, id.to_string()));
    }
    best.sort_by_key(|b| std::cmp::Reverse(b.0));
    let Some((top, repo, id)) = best.first().cloned() else {
        return Ok(json!({"repo": null, "confidence": "low", "reason": "Nothing points at a repo.", "evidence": []}).to_string());
    };
    let tie = best.iter().any(|(r, other, _)| *r == top && other != &repo);
    let confidence = match (top, tie) {
        (_, true) | (0, _) => "low",
        (1, _) => "medium",
        _ => "high",
    };
    Ok(json!({"repo": repo, "confidence": confidence, "reason": "The strongest evidence names it.", "evidence": [id]}).to_string())
}

/// What a name rule picks: most shared words between the log group's last
/// part and a repo's name.
fn name_pick(group: &str, repos: &[String]) -> String {
    let last = group.rsplit('/').next().unwrap_or("").to_lowercase();
    let words: Vec<&str> = last.split('-').collect();
    repos
        .iter()
        .max_by_key(|r| {
            let short = r.rsplit('/').next().unwrap_or("").to_lowercase();
            short.split('-').filter(|w| words.contains(w)).count()
        })
        .cloned()
        .unwrap_or_default()
}

/// The name matcher as a model: the name rule's pick, always "high",
/// never citing evidence.
fn name_matcher(repos: Vec<String>) -> impl Fn(&str) -> Result<String, String> {
    move |prompt: &str| {
        let group = prompt
            .lines()
            .find_map(|l| l.strip_prefix("Log group: "))
            .unwrap_or("")
            .to_string();
        let pick = name_pick(&group, &repos);
        Ok(json!({"repo": pick, "confidence": "high", "reason": "Its name matches the log group.", "evidence": []}).to_string())
    }
}

fn env_for(
    c: &Case,
    judge: impl Fn(&str) -> Result<String, String> + Send + Sync + 'static,
) -> Fake {
    let mut env = Fake::new(c.facts.clone(), judge);
    for h in &c.hits {
        env.hits
            .entry(h.query.clone())
            .or_default()
            .push(SearchHit {
                repo: h.repo.clone(),
                path: h.path.clone(),
                fragments: vec![h.fragment.clone()],
            });
    }
    env
}

#[tokio::test]
async fn the_eval_set() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("eval_cases.json")).unwrap();
    assert!(cases.len() >= 10);
    let now = t("2026-10-26T12:00:00Z");
    let mut failures = Vec::new();
    let mut rows = Vec::new();
    for c in &cases {
        let names: Vec<&str> = c.repos.iter().map(String::as_str).collect();
        let inp = Input {
            target: Target {
                account: "111122223333".into(),
                log_group: c.log_group.clone(),
                samples: c.samples.clone(),
                ..Default::default()
            },
            repos: repos(&names),
            saved: c.saved.clone(),
            past: c.past.clone(),
            ..Default::default()
        };
        let good = resolve(&env_for(c, reader), &inp, now).await;
        let naive = resolve(&env_for(c, name_matcher(c.repos.clone())), &inp, now).await;

        let (min, max) = (Confidence::parse(&c.min), Confidence::parse(&c.max));
        let mut ok = true;
        if let Some(want) = &c.expect {
            if good.repo.as_deref() != Some(want.as_str()) {
                failures.push(format!("{}: expected {want}, got {:?}", c.name, good.repo));
                ok = false;
            }
        }
        if good.confidence < min || good.confidence > max {
            failures.push(format!(
                "{}: confidence {:?} outside {:?}..{:?}",
                c.name, good.confidence, min, max
            ));
            ok = false;
        }
        // The name matcher must never come out sure on a name alone.
        let naive_backed = naive.decided_by.iter().any(|e| e.kind.is_data());
        if naive.is_sure() && !naive_backed {
            failures.push(format!(
                "{}: a name-only pick ({:?}) came out sure",
                c.name, naive.repo
            ));
            ok = false;
        }
        let raw = name_pick(&c.log_group, &c.repos);
        if raw != c.decoy {
            failures.push(format!(
                "{}: the case's decoy {} isn't what a name rule picks ({raw})",
                c.name, c.decoy
            ));
            ok = false;
        }
        // A fix run for this error already worked in a repo (its triage
        // chose it): nothing may pre-fill a different one.
        if let Some(ran) = c.past.iter().map(|p| &p.ran_in).find(|r| !r.is_empty()) {
            for (who, r) in [("reader", &good), ("name rule", &naive)] {
                if r.repo.as_deref() != Some(ran.as_str()) {
                    failures.push(format!(
                        "{}: {who} pre-filled {:?}, the run worked in {ran}",
                        c.name, r.repo
                    ));
                    ok = false;
                }
            }
        }
        if !c.permissions.is_empty() && good.permissions != c.permissions {
            failures.push(format!("{}: permissions {:?}", c.name, good.permissions));
            ok = false;
        }
        rows.push(format!(
            "{} | {:<55} | {:<22} {:<6} | name rule: {:<22} {:<6}",
            if ok { "PASS" } else { "FAIL" },
            c.name,
            good.repo.as_deref().unwrap_or("-"),
            format!("{:?}", good.confidence).to_lowercase(),
            naive.repo.as_deref().unwrap_or("-"),
            format!("{:?}", naive.confidence).to_lowercase(),
        ));
    }
    println!(
        "\nRepo resolution eval ({} cases)\n{}",
        cases.len(),
        rows.join("\n")
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn no_regular_expression_decides() {
    for (file, src) in [
        ("log_repo.rs", include_str!("../../log_repo.rs")),
        ("log_repo/live.rs", include_str!("../live.rs")),
    ] {
        for banned in ["regex", "Regex"] {
            assert!(
                !src.contains(banned),
                "{file} uses a regular expression; matching is data and model judgment"
            );
        }
    }
}
