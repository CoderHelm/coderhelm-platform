//! Terraform checks in the sandbox. A Terraform repository's change opened
//! with nothing verified before the PR, because the sandbox knew only Node,
//! Rust, Go and Python. For a repository with `*.tf` files the sandbox now
//! checks formatting (`terraform fmt -recursive -check`) and validates each
//! root module (`terraform init -backend=false` then `terraform validate`),
//! with the Terraform version the repository pins. It never runs `plan` or
//! `apply`: both need cloud credentials the sandbox must not have.

use serde_yaml::Value;

use super::checks::{is_plain_path, CiCommand};

/// The version installed when the repository pins none.
pub const DEFAULT_VERSION: &str = "1.16.7";

/// The most root modules validated in one run: each `init` downloads
/// providers, and the sandbox has a time budget.
const MAX_ROOTS: usize = 12;

/// The most directories read when looking for root modules.
const MAX_DIRS_READ: usize = 40;

/// The format check the sandbox runs when CI's own isn't mirrored.
pub const FMT_CHECK: &str = "terraform fmt -recursive -check";

/// What the sandbox needs to check a Terraform repository.
#[derive(Debug, Clone, PartialEq)]
pub struct TfRepo {
    /// The root modules to validate (directories, `.` for the repository's
    /// root), in path order.
    pub roots: Vec<String>,
    /// The exact Terraform version to install.
    pub version: String,
}

impl TfRepo {
    /// One command that validates every root module, each on its own (one
    /// failing never stops the next), and fails when any failed. `init`
    /// runs without the backend, so no state or credentials are touched.
    pub fn validate_cmd(&self) -> String {
        let dirs: Vec<String> = self.roots.iter().map(|d| format!("'{d}'")).collect();
        format!(
            "export TF_IN_AUTOMATION=1 TF_PLUGIN_CACHE_DIR=/tmp/tf-plugin-cache; \
             mkdir -p /tmp/tf-plugin-cache; ch_tf=0; \
             for d in {}; do echo \"== terraform validate in $d\"; \
             ( cd \"$d\" && {{ terraform init -backend=false -input=false -no-color > /tmp/tf-init.log 2>&1 \
             || {{ cat /tmp/tf-init.log; exit 1; }}; }} && terraform validate -no-color ) || ch_tf=1; done; \
             [ $ch_tf -eq 0 ]",
            dirs.join(" ")
        )
    }
}

/// Whether a `.tf` file's text makes its directory a root module: it
/// configures a provider, a backend or Terraform Cloud. Read line by line
/// from the HCL block headers.
pub fn declares_root(text: &str) -> bool {
    text.lines().any(|l| {
        let l = l.trim_start();
        l.starts_with("provider \"") || l.starts_with("backend \"") || l.starts_with("cloud {")
    })
}

/// The directory of a repository path (`.` for a file at the root).
fn dir_of(path: &str) -> String {
    match path.rfind('/') {
        Some(i) => path[..i].to_string(),
        None => ".".to_string(),
    }
}

/// The directories holding `.tf` files, in path order, leaving out
/// provider caches (`.terraform/`).
pub fn tf_dirs(paths: &[String]) -> Vec<String> {
    let mut dirs: Vec<String> = paths
        .iter()
        .filter(|p| p.ends_with(".tf"))
        .filter(|p| !p.split('/').any(|seg| seg == ".terraform"))
        .map(|p| dir_of(p))
        .filter(|d| d == "." || is_plain_path(d))
        .collect();
    dirs.sort();
    dirs.dedup();
    dirs
}

/// An exact version (`1.9.8`) for a version setting: a pin
/// (`1.9.8`, `=1.9.8`, `v1.9.8`), a pessimistic constraint (`~> 1.6`, its
/// lowest match), a wildcard (`1.6.x`). A lower bound (`>= 1.5`) is met by
/// [`DEFAULT_VERSION`] unless it asks for more. `None` for `latest` or
/// anything unreadable.
pub fn exact_version(spec: &str) -> Option<String> {
    let first = spec
        .trim()
        .trim_matches(|c| c == '"' || c == '\'')
        .split(',')
        .next()?
        .trim();
    let lower_bound = first.starts_with('>');
    if first.starts_with('<') || first.starts_with("!=") {
        return None;
    }
    let bare = first.trim_start_matches(['>', '<', '=', '~', '^', ' ', 'v']);
    let mut parts: Vec<String> = bare
        .split('.')
        .map(|p| {
            if p == "x" || p == "*" {
                "0".into()
            } else {
                p.to_string()
            }
        })
        .collect();
    if parts.is_empty()
        || parts.len() > 3
        || parts
            .iter()
            .any(|p| p.is_empty() || !p.chars().all(|c| c.is_ascii_digit()))
    {
        return None;
    }
    while parts.len() < 3 {
        parts.push("0".into());
    }
    let version = parts.join(".");
    if lower_bound {
        let num = |v: &str| -> Vec<u64> { v.split('.').filter_map(|p| p.parse().ok()).collect() };
        if num(&version) <= num(DEFAULT_VERSION) {
            return None;
        }
    }
    Some(version)
}

/// Whether `v` is safe to hand the sandbox's install step: digits and dots.
pub fn is_exact(v: &str) -> bool {
    let parts: Vec<&str> = v.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.len() <= 4 && p.chars().all(|c| c.is_ascii_digit()))
}

/// The `terraform_version` a workflow's `hashicorp/setup-terraform` step
/// pins, if any.
pub fn version_from_workflows(texts: &[String]) -> Option<String> {
    for text in texts {
        let Ok(doc) = serde_yaml::from_str::<Value>(text) else {
            continue;
        };
        let Some(jobs) = doc.get("jobs").and_then(Value::as_mapping) else {
            continue;
        };
        for job in jobs.values() {
            let Some(steps) = job.get("steps").and_then(Value::as_sequence) else {
                continue;
            };
            for step in steps {
                let uses = step.get("uses").and_then(Value::as_str).unwrap_or("");
                if !uses.contains("setup-terraform") {
                    continue;
                }
                let spec = match step.get("with").and_then(|w| w.get("terraform_version")) {
                    Some(Value::String(s)) => s.clone(),
                    Some(Value::Number(n)) => n.to_string(),
                    _ => continue,
                };
                if let Some(v) = exact_version(&spec) {
                    return Some(v);
                }
            }
        }
    }
    None
}

/// The `required_version` a `.tf` file's `terraform` block sets, as written.
pub fn required_version(text: &str) -> Option<String> {
    text.lines().find_map(|l| {
        let rest = l.trim().strip_prefix("required_version")?;
        let value = rest.trim_start().strip_prefix('=')?.trim();
        Some(value.trim_matches('"').to_string())
    })
}

/// The version to install: the workflow's `setup-terraform` pin, then
/// `.terraform-version`, then a root module's `required_version`, then
/// [`DEFAULT_VERSION`].
pub fn pick_version(
    from_workflow: Option<String>,
    version_file: Option<&str>,
    required: Option<&str>,
) -> String {
    from_workflow
        .or_else(|| version_file.and_then(exact_version))
        .or_else(|| required.and_then(exact_version))
        .filter(|v| is_exact(v))
        .unwrap_or_else(|| DEFAULT_VERSION.to_string())
}

/// The repository's Terraform root modules and pinned version, or `None`
/// when it has no `.tf` files. Root modules are the directories whose files
/// configure a provider or backend, plus the directories CI runs Terraform
/// in (from `ci`); a repository whose `.tf` files declare neither is
/// validated at the directories that hold them, up to [`MAX_ROOTS`].
pub async fn detect(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
    ci: &[CiCommand],
) -> Option<TfRepo> {
    let paths: Vec<String> = github
        .get_tree(owner, repo, git_ref)
        .await
        .ok()?
        .into_iter()
        .filter(|e| e.entry_type == "blob")
        .map(|e| e.path)
        .collect();
    let dirs = tf_dirs(&paths);
    if dirs.is_empty() {
        return None;
    }
    let mut roots: Vec<String> = Vec::new();
    let mut required: Option<String> = None;
    for dir in dirs.iter().take(MAX_DIRS_READ) {
        let files = paths
            .iter()
            .filter(|p| p.ends_with(".tf") && dir_of(p) == *dir)
            .take(8);
        for f in files {
            let Ok(text) = github.read_file(owner, repo, f, git_ref).await else {
                continue;
            };
            if required.is_none() {
                required = required_version(&text);
            }
            if declares_root(&text) {
                roots.push(dir.clone());
                break;
            }
        }
    }
    // The directories CI runs Terraform in are root modules too.
    for c in ci.iter().filter(|c| c.tool.starts_with("terraform")) {
        if let Some(d) = c.dir.as_deref().map(|d| d.trim_end_matches('/')) {
            if dirs.iter().any(|x| x == d) && !roots.iter().any(|r| r == d) {
                roots.push(d.to_string());
            }
        }
    }
    if roots.is_empty() {
        roots = dirs.clone();
    }
    roots.sort();
    roots.dedup();
    roots.truncate(MAX_ROOTS);
    let texts: Vec<String> = super::checks::read_workflows(github, owner, repo, git_ref)
        .await
        .into_iter()
        .map(|w| w.text)
        .collect();
    let version_file = github
        .read_file(owner, repo, ".terraform-version", git_ref)
        .await
        .ok();
    Some(TfRepo {
        roots,
        version: pick_version(
            version_from_workflows(&texts),
            version_file.as_deref().map(str::trim),
            required.as_deref(),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_the_directories_with_terraform_files() {
        let paths: Vec<String> = [
            "README.md",
            "envs/prod/main.tf",
            "envs/prod/variables.tf",
            "envs/staging/main.tf",
            "modules/bucket/main.tf",
            "envs/prod/.terraform/modules/x/main.tf",
            "weird dir/main.tf",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            tf_dirs(&paths),
            vec!["envs/prod", "envs/staging", "modules/bucket"]
        );
        assert!(tf_dirs(&["main.tf".to_string()]) == vec!["."]);
        assert!(tf_dirs(&["src/main.rs".to_string()]).is_empty());
    }

    #[test]
    fn a_root_module_configures_a_provider_or_backend() {
        assert!(declares_root(
            "terraform {\n  backend \"s3\" {\n    bucket = \"x\"\n  }\n}\n"
        ));
        assert!(declares_root(
            "provider \"aws\" {\n  region = var.region\n}\n"
        ));
        assert!(!declares_root(
            "resource \"aws_s3_bucket\" \"b\" {\n  bucket = var.name\n}\n"
        ));
    }

    #[test]
    fn the_pinned_version_is_read_exactly() {
        assert_eq!(exact_version("1.9.8").as_deref(), Some("1.9.8"));
        assert_eq!(exact_version("v1.9.8").as_deref(), Some("1.9.8"));
        assert_eq!(exact_version("= 1.6.3").as_deref(), Some("1.6.3"));
        assert_eq!(exact_version("~> 1.6").as_deref(), Some("1.6.0"));
        assert_eq!(exact_version("~> 1.5.7").as_deref(), Some("1.5.7"));
        assert_eq!(exact_version("1.7.x").as_deref(), Some("1.7.0"));
        assert_eq!(exact_version(">= 1.3.0, < 2.0.0"), None);
        assert_eq!(exact_version(">= 9.0").as_deref(), Some("9.0.0"));
        assert_eq!(exact_version("latest"), None);
        assert_eq!(exact_version("1.9.8; rm -rf /"), None);
        assert!(is_exact("1.16.7"));
        assert!(!is_exact("1.16"));
        assert!(!is_exact("1.16.7 && x"));
    }

    #[test]
    fn the_version_follows_the_workflow_then_files_then_the_default() {
        let wf = "on: pull_request\njobs:\n  fmt:\n    steps:\n      - uses: hashicorp/setup-terraform@v3\n        with:\n          terraform_version: 1.9.8\n      - run: terraform fmt -recursive -check\n".to_string();
        assert_eq!(version_from_workflows(&[wf]).as_deref(), Some("1.9.8"));
        assert_eq!(
            pick_version(None, Some("1.7.5"), Some(">= 1.3")),
            "1.7.5".to_string()
        );
        assert_eq!(pick_version(None, None, Some("~> 1.8")), "1.8.0");
        assert_eq!(pick_version(None, None, Some(">= 1.3")), DEFAULT_VERSION);
        assert_eq!(pick_version(None, None, None), DEFAULT_VERSION);
        assert_eq!(
            required_version("terraform {\n  required_version = \"~> 1.8\"\n}\n").as_deref(),
            Some("~> 1.8")
        );
    }

    #[test]
    fn the_sandbox_validates_each_root_module_and_never_plans() {
        let tf = TfRepo {
            roots: vec!["envs/prod".into(), "envs/staging".into()],
            version: "1.9.8".into(),
        };
        let cmd = tf.validate_cmd();
        assert!(cmd.contains("for d in 'envs/prod' 'envs/staging'; do"));
        assert!(cmd.contains("terraform init -backend=false -input=false"));
        assert!(cmd.contains("terraform validate -no-color"));
        assert!(cmd.ends_with("[ $ch_tf -eq 0 ]"));
        for never in ["terraform plan", "terraform apply", "-backend=true"] {
            assert!(!cmd.contains(never), "{never}");
        }
        assert_eq!(
            super::super::checks::kinds_of(&cmd),
            vec![super::super::checks::CheckKind::Typecheck]
        );
        assert!(!FMT_CHECK.contains("plan"));
    }
}
