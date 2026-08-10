//! Computed, explainable change-risk. Blast radius is the primary weight, per the
//! research: a transparent weighted rubric with a per-dimension breakdown beats an
//! opaque model guess, and *actionability* (the reasons) is what makes a risk flag
//! useful. Signals here are all computable in-process from the diff + the repo
//! snapshot search — no sandbox or git history needed for this tier. Categorical
//! risks (migrations / auth / secrets / payments) hard-override to HIGH.

use crate::clients::github::GitHubClient;
use serde_json::Value;

/// One scored dimension of the risk breakdown.
#[derive(Debug, Clone)]
pub struct RiskDim {
    pub name: &'static str,
    pub score: u8, // 0-100
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct RiskReport {
    pub score: u8,
    pub level: &'static str, // LOW | MEDIUM | HIGH
    pub dims: Vec<RiskDim>,
    /// Present when a categorical rule forced HIGH (e.g. a DB migration).
    pub hard: Option<String>,
}

impl RiskReport {
    /// Markdown block for the PR review body — the per-dimension breakdown is the
    /// point (the reasons, not just a number).
    pub fn markdown(&self) -> String {
        let bar = |s: u8| {
            let filled = (s as usize).div_ceil(10);
            format!("{}{}", "█".repeat(filled), "░".repeat(10 - filled))
        };
        let mut md = format!("#### 🎯 Risk: {} ({}/100)\n", self.level, self.score);
        if let Some(h) = &self.hard {
            md.push_str(&format!("> ⚠️ Forced HIGH — {h}\n"));
        }
        for d in &self.dims {
            md.push_str(&format!(
                "- `{}` {} {} — {}\n",
                bar(d.score),
                d.name,
                d.score,
                d.detail
            ));
        }
        md
    }
}

/// Normalized Shannon entropy (0..1) of change spread across files (Hassan): a
/// change thinly spread across many files is harder to get right.
pub fn entropy(churns: &[u64]) -> f64 {
    let total: u64 = churns.iter().sum();
    let n = churns.iter().filter(|&&c| c > 0).count();
    if total == 0 || n <= 1 {
        return 0.0;
    }
    let mut h = 0.0;
    for &c in churns {
        if c > 0 {
            let p = c as f64 / total as f64;
            h -= p * p.log2();
        }
    }
    h / (n as f64).log2()
}

fn is_test_path(p: &str) -> bool {
    let l = p.to_ascii_lowercase();
    l.contains("test")
        || l.contains("spec")
        || l.contains("__tests__")
        || l.contains(".test.")
        || l.contains("_test.")
        || l.contains(".spec.")
}

fn is_generated_path(p: &str) -> bool {
    let l = p.to_ascii_lowercase();
    l.ends_with("lock")
        || l.ends_with(".lock")
        || l.ends_with(".snap")
        || l.contains("/dist/")
        || l.contains("/build/")
        || l.contains("node_modules/")
        || l.ends_with(".min.js")
        || l.contains("generated")
}

/// Sensitive-change classification → (score 0-100, human detail, hard-override reason).
pub fn classify_sensitive(files: &[(String, String, String)]) -> (u8, String, Option<String>) {
    // files: (path, status, patch)
    let mut hits: Vec<&'static str> = vec![];
    let mut hard: Option<String> = None;
    let mut deletions = false;

    for (path, status, patch) in files {
        let l = path.to_ascii_lowercase();
        let hay = format!("{l}\n{}", patch.to_ascii_lowercase());
        let migration = l.contains("migration")
            || l.contains("/migrate")
            || l.ends_with("schema.sql")
            || l.ends_with(".prisma")
            || l.contains("alembic");
        let auth = [
            "auth",
            "login",
            "session",
            "password",
            "secret",
            "token",
            "jwt",
            "crypto",
            "oauth",
            "credential",
        ]
        .iter()
        .any(|k| hay.contains(k));
        let payment = [
            "payment", "charge", "stripe", "billing", "invoice", "refund", "checkout",
        ]
        .iter()
        .any(|k| hay.contains(k));
        let infra = l == "dockerfile"
            || l.ends_with("/dockerfile")
            || l.ends_with(".tf")
            || l.contains(".github/workflows")
            || l.contains("k8s")
            || l.contains("cdk")
            || l.ends_with("package.json");
        if migration {
            hits.push("migration/schema");
            hard.get_or_insert_with(|| "touches a DB migration/schema (hard to roll back)".into());
        }
        if auth {
            hits.push("auth/secrets");
            hard.get_or_insert_with(|| "touches auth/secrets".into());
        }
        if payment {
            hits.push("payments");
            hard.get_or_insert_with(|| "touches payment/billing code".into());
        }
        if infra {
            hits.push("infra/ci");
        }
        if status == "removed" {
            deletions = true;
        }
    }
    hits.sort_unstable();
    hits.dedup();
    let mut score = (hits.len() as u32 * 30).min(90) as u8;
    if deletions {
        score = score.saturating_add(10).min(100);
    }
    let detail = if hits.is_empty() && !deletions {
        "no sensitive areas touched".to_string()
    } else {
        let mut d = hits.join(", ");
        if deletions {
            if !d.is_empty() {
                d.push_str(", ");
            }
            d.push_str("file deletion");
        }
        d
    };
    (score, detail, hard)
}

/// A search token for a changed file (its module stem), used to grep for callers.
fn derive_symbol(path: &str) -> Option<String> {
    let base = path.rsplit('/').next().unwrap_or(path);
    let stem = base.split('.').next().unwrap_or(base);
    let generic = matches!(stem, "index" | "mod" | "main" | "__init__" | "lib");
    let token = if generic {
        // Use the parent directory name instead of a generic filename.
        path.trim_end_matches(base)
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or(stem)
            .to_string()
    } else {
        stem.to_string()
    };
    if token.len() >= 3 {
        Some(token)
    } else {
        None
    }
}

/// Weighted combine → (score 0-100, level). `hard` forces HIGH.
pub fn combine(blast: u8, sensitive: u8, coverage: u8, size: u8, hard: bool) -> (u8, &'static str) {
    let score = (0.35 * blast as f64
        + 0.25 * sensitive as f64
        + 0.20 * coverage as f64
        + 0.20 * size as f64)
        .round() as u8;
    let level = if hard || score >= 67 {
        "HIGH"
    } else if score >= 34 {
        "MEDIUM"
    } else {
        "LOW"
    };
    (score, level)
}

/// Compute the risk report for a PR. Bounded: at most 15 changed source files get
/// a blast-radius search so a huge PR can't run the search loop unbounded.
pub async fn assess(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    head_sha: &str,
    compare: &Value,
) -> RiskReport {
    let empty = vec![];
    let files_json = compare["files"].as_array().unwrap_or(&empty);

    let mut files: Vec<(String, String, String)> = vec![]; // (path, status, patch)
    let mut churns: Vec<u64> = vec![];
    let mut total_churn: u64 = 0;
    let mut src_changed = 0u32;
    let mut test_changed = 0u32;
    let mut source_paths: Vec<String> = vec![];

    for f in files_json {
        let path = f["filename"].as_str().unwrap_or("").to_string();
        let status = f["status"].as_str().unwrap_or("").to_string();
        let patch = f["patch"].as_str().unwrap_or("").to_string();
        let adds = f["additions"].as_u64().unwrap_or(0);
        let dels = f["deletions"].as_u64().unwrap_or(0);
        total_churn += adds + dels;
        churns.push(adds + dels);
        if is_test_path(&path) {
            test_changed += 1;
        } else if !is_generated_path(&path) {
            src_changed += 1;
            if status != "removed" {
                source_paths.push(path.clone());
            }
        }
        files.push((path, status, patch));
    }

    // Sensitive class.
    let (sensitive_score, sensitive_detail, hard) = classify_sensitive(&files);

    // Blast radius: count distinct files referencing each changed source file's
    // module token (a cheap caller proxy over the in-process snapshot search).
    let mut dependents_total = 0usize;
    let mut top: Vec<(String, usize)> = vec![];
    for path in source_paths.iter().take(15) {
        let Some(token) = derive_symbol(path) else {
            continue;
        };
        if let Ok(results) = github.search_code(owner, repo, head_sha, &token).await {
            let count = results.iter().filter(|r| r.path != *path).count().min(25); // cap per-file contribution
            dependents_total += count;
            if count > 0 {
                top.push((path.clone(), count));
            }
        }
    }
    top.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
    let blast_score = (dependents_total * 4).min(100) as u8;
    let blast_detail = if top.is_empty() {
        "no downstream references found".to_string()
    } else {
        let names: Vec<String> = top
            .iter()
            .take(3)
            .map(|(p, c)| format!("{} (~{c})", p.rsplit('/').next().unwrap_or(p)))
            .collect();
        format!(
            "~{dependents_total} dependent refs; hottest: {}",
            names.join(", ")
        )
    };

    // Test coverage heuristic (patch-level coverage needs the sandbox; this is the
    // cheap gate): source changed with no test files touched → high gap.
    let coverage_score: u8 = if src_changed == 0 {
        0
    } else if test_changed == 0 {
        80
    } else if test_changed < src_changed {
        40
    } else {
        10
    };
    let coverage_detail = format!("{src_changed} source / {test_changed} test file(s) changed");

    // Size + entropy.
    let ent = entropy(&churns);
    let churn_score = ((total_churn as f64).min(1200.0) / 1200.0 * 100.0) as u8;
    let size_score = (churn_score as f64 * 0.7 + ent * 100.0 * 0.3).round() as u8;
    let size_detail = format!(
        "{total_churn} lines across {} file(s), spread {:.0}%",
        files.len(),
        ent * 100.0
    );

    let (score, level) = combine(
        blast_score,
        sensitive_score,
        coverage_score,
        size_score,
        hard.is_some(),
    );

    RiskReport {
        score,
        level,
        hard,
        dims: vec![
            RiskDim {
                name: "blast radius",
                score: blast_score,
                detail: blast_detail,
            },
            RiskDim {
                name: "sensitive area",
                score: sensitive_score,
                detail: sensitive_detail,
            },
            RiskDim {
                name: "test coverage",
                score: coverage_score,
                detail: coverage_detail,
            },
            RiskDim {
                name: "size/spread",
                score: size_score,
                detail: size_detail,
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entropy_zero_for_single_file() {
        assert_eq!(entropy(&[100]), 0.0);
        assert_eq!(entropy(&[]), 0.0);
    }

    #[test]
    fn entropy_max_for_even_spread() {
        // Evenly spread across 4 files → normalized entropy ≈ 1.0.
        let e = entropy(&[10, 10, 10, 10]);
        assert!(e > 0.99, "expected ~1.0, got {e}");
    }

    #[test]
    fn migration_forces_hard_high() {
        let files = vec![(
            "db/migrations/001_add.sql".to_string(),
            "added".to_string(),
            "CREATE TABLE x".to_string(),
        )];
        let (_score, detail, hard) = classify_sensitive(&files);
        assert!(hard.is_some());
        assert!(detail.contains("migration"));
        let (_s, level) = combine(0, 30, 0, 0, hard.is_some());
        assert_eq!(level, "HIGH");
    }

    #[test]
    fn plain_change_is_not_sensitive() {
        let files = vec![(
            "src/util/format.ts".to_string(),
            "modified".to_string(),
            "return x + 1".to_string(),
        )];
        let (score, _detail, hard) = classify_sensitive(&files);
        assert_eq!(score, 0);
        assert!(hard.is_none());
    }

    #[test]
    fn combine_levels() {
        assert_eq!(combine(0, 0, 0, 0, false).1, "LOW");
        assert_eq!(combine(50, 50, 50, 50, false).1, "MEDIUM");
        assert_eq!(combine(90, 90, 90, 90, false).1, "HIGH");
        assert_eq!(combine(0, 0, 0, 0, true).1, "HIGH"); // hard override
    }

    #[test]
    fn derive_symbol_uses_parent_for_generic_names() {
        assert_eq!(derive_symbol("src/auth/index.ts").as_deref(), Some("auth"));
        assert_eq!(derive_symbol("src/parser.rs").as_deref(), Some("parser"));
        assert_eq!(derive_symbol("a/b.x"), None); // stem "b" too short
    }
}
