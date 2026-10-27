//! A repository's codegen environment: the PUBLIC values its code generators
//! need to run in the sandbox (a CMS project id and dataset, for example),
//! kept per repository at `SETTINGS#CODEGEN_ENV#<owner>/<repo>`.
//!
//! The sandbox holds no secrets, and these values cross into it: only values
//! that already ship in a client bundle belong here. Names and values that
//! look like credentials are refused ([`validate`]).
//!
//! When a generator fails because a value it reads is missing, the run stops
//! before its pull request and says exactly which names are missing and where
//! to set them ([`missing`], [`failure_reason`]), instead of opening the pull
//! request without the regenerated files.

use std::collections::BTreeSet;

/// The settings sort key for a repository's codegen environment.
pub fn sk(repo: &str) -> String {
    format!("SETTINGS#CODEGEN_ENV#{repo}")
}

/// Most variables a repository may keep.
pub const MAX_VARS: usize = 30;
/// Longest value kept.
pub const MAX_VALUE_CHARS: usize = 300;

/// Where a person sets these, in the words the dashboard uses.
pub const WHERE: &str = "Settings, Repositories, the repository's Codegen environment";

/// Name prefixes frameworks reserve for values shipped to the browser.
const PUBLIC_PREFIXES: &[&str] = &[
    "SANITY_STUDIO_",
    "NEXT_PUBLIC_",
    "PUBLIC_",
    "VITE_",
    "NUXT_PUBLIC_",
    "EXPO_PUBLIC_",
    "REACT_APP_",
    "GATSBY_",
    "STORYBOOK_",
];

/// Words in a name that mark a credential.
const SECRET_WORDS: &[&str] = &[
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASSWD",
    "PRIVATE",
    "CREDENTIAL",
    "API_KEY",
    "APIKEY",
    "ACCESS_KEY",
    "AUTH",
    "SESSION",
    "COOKIE",
    "SIGNING",
    "WEBHOOK",
];

/// Whether a variable name looks like a credential.
pub fn name_looks_secret(name: &str) -> bool {
    let n = name.to_ascii_uppercase();
    SECRET_WORDS.iter().any(|w| n.contains(w)) || n.ends_with("_KEY") || n == "KEY"
}

/// Whether a value looks like a credential (a known token shape or a key).
pub fn value_looks_secret(value: &str) -> bool {
    let v = value.trim();
    const PREFIXES: &[&str] = &[
        "ghp_",
        "gho_",
        "ghs_",
        "github_pat_",
        "glpat-",
        "xox",
        "sk-",
        "sk_live_",
        "sk_test_",
        "rk_live_",
        "AKIA",
        "ASIA",
        "AIza",
        "eyJ",
        "-----BEGIN",
    ];
    PREFIXES.iter().any(|p| v.starts_with(p)) || v.contains("PRIVATE KEY")
}

/// Whether a name is one a framework ships to the browser, or names the
/// kind of setting codegen reads (a project id, a dataset).
pub fn is_public_name(name: &str) -> bool {
    let n = name.to_ascii_uppercase();
    !name_looks_secret(&n)
        && (PUBLIC_PREFIXES.iter().any(|p| n.starts_with(p))
            || n.contains("PROJECT_ID")
            || n.contains("DATASET")
            || n.ends_with("_API_VERSION")
            || n.ends_with("_ENDPOINT")
            || n.ends_with("_URL"))
}

/// Refuses what doesn't belong: a bad name, a credential-looking name or
/// value, a multi-line or long value, too many variables.
pub fn validate(vars: &[(String, String)]) -> Result<(), String> {
    if vars.len() > MAX_VARS {
        return Err(format!("At most {MAX_VARS} variables."));
    }
    let mut seen = BTreeSet::new();
    for (name, value) in vars {
        let ok = !name.is_empty()
            && name.len() <= 100
            && name
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !ok {
            return Err(format!(
                "`{name}` isn't a variable name (letters, digits and _)."
            ));
        }
        if !seen.insert(name.clone()) {
            return Err(format!("`{name}` is listed twice."));
        }
        if name_looks_secret(name) {
            return Err(format!(
                "`{name}` looks like a secret. Only public values (ones that already ship to the browser) belong here."
            ));
        }
        if value_looks_secret(value) {
            return Err(format!(
                "The value of `{name}` looks like a token or key. Only public values belong here."
            ));
        }
        if value.contains('\n') || value.chars().count() > MAX_VALUE_CHARS {
            return Err(format!(
                "The value of `{name}` must be one line of at most {MAX_VALUE_CHARS} characters."
            ));
        }
    }
    Ok(())
}

/// The environment variable names a config file's code reads
/// (`process.env.NAME`, `process.env["NAME"]`, `import.meta.env.NAME`).
pub fn env_refs(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for marker in [
        "process.env.",
        "import.meta.env.",
        "process.env[",
        "Deno.env.get(",
    ] {
        let mut rest = text;
        while let Some(i) = rest.find(marker) {
            rest = &rest[i + marker.len()..];
            let name: String = rest
                .trim_start_matches(['"', '\'', '`'])
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if name.len() >= 2 {
                out.insert(name);
            }
        }
    }
    out
}

/// Names in a generator's failure output that look like public settings
/// (`SANITY_STUDIO_PROJECT_ID is not set`).
pub fn named_in_output(output: &str) -> BTreeSet<String> {
    output
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| {
            w.len() >= 4
                && w.chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                && w.contains('_')
                && is_public_name(w)
        })
        .map(String::from)
        .collect()
}

/// The public names a failed generator needs that the repository's codegen
/// environment doesn't set: what its configs read plus what its output
/// names, less the ones already set. Credential-looking names never appear
/// (they can't be set here).
pub fn missing(
    config_refs: &BTreeSet<String>,
    output: &str,
    set: &[(String, String)],
) -> Vec<String> {
    let have: BTreeSet<&str> = set
        .iter()
        .filter(|(_, v)| !v.trim().is_empty())
        .map(|(k, _)| k.as_str())
        .collect();
    let wanted: BTreeSet<String> = config_refs
        .iter()
        .filter(|n| is_public_name(n))
        .cloned()
        .chain(named_in_output(output))
        .collect();
    wanted
        .into_iter()
        .filter(|n| !have.contains(n.as_str()))
        .collect()
}

/// The run's status when codegen can't run for missing public values.
pub fn failure_reason(repo: &str, command: &str, missing: &[String]) -> String {
    let names: Vec<String> = missing.iter().map(|m| format!("`{m}`")).collect();
    format!(
        "Stopped before opening a pull request: regenerating the generated files failed because \
         `{command}` needs public values the sandbox doesn't have: {}. Set them in {WHERE} \
         ({repo}), public values only, never secrets; then retry.",
        names.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(p: &[(&str, &str)]) -> Vec<(String, String)> {
        p.iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    #[test]
    fn public_values_pass_secrets_are_refused() {
        assert!(validate(&v(&[
            ("SANITY_STUDIO_PROJECT_ID", "abc123xy"),
            ("SANITY_STUDIO_DATASET", "production")
        ]))
        .is_ok());
        for bad in [
            ("SANITY_AUTH_TOKEN", "x"),
            ("STRIPE_SECRET", "x"),
            ("DB_PASSWORD", "x"),
            ("GOOGLE_API_KEY", "x"),
            ("MY_KEY", "x"),
        ] {
            assert!(validate(&v(&[bad])).is_err(), "{bad:?}");
        }
        assert!(validate(&v(&[("NEXT_PUBLIC_X", "ghp_abcdefghijklmnop")])).is_err());
        assert!(validate(&v(&[("NEXT_PUBLIC_X", "-----BEGIN RSA PRIVATE KEY-----")])).is_err());
        assert!(validate(&v(&[("1BAD", "x")])).is_err());
        assert!(validate(&v(&[("A_B", "x"), ("A_B", "y")])).is_err());
        assert!(validate(&v(&[("A_B", "line\nbreak")])).is_err());
    }

    #[test]
    fn finds_what_a_failed_generator_needs() {
        let cli = "import {defineCliConfig} from 'sanity/cli'\nexport default defineCliConfig({api: {projectId: process.env.SANITY_STUDIO_PROJECT_ID, dataset: process.env['SANITY_STUDIO_DATASET']}, token: process.env.SANITY_AUTH_TOKEN})";
        let refs = env_refs(cli);
        assert!(refs.contains("SANITY_STUDIO_PROJECT_ID"));
        assert!(refs.contains("SANITY_STUDIO_DATASET"));
        assert!(refs.contains("SANITY_AUTH_TOKEN"));
        let out = "Error: Configuration must contain `projectId`";
        // The token is never asked for: it can't be set here.
        assert_eq!(
            missing(&refs, out, &[]),
            vec!["SANITY_STUDIO_DATASET", "SANITY_STUDIO_PROJECT_ID"]
        );
        // One already set: only the other is missing.
        assert_eq!(
            missing(&refs, out, &v(&[("SANITY_STUDIO_PROJECT_ID", "abc")])),
            vec!["SANITY_STUDIO_DATASET"]
        );
        // Named only in the output.
        assert_eq!(
            missing(
                &Default::default(),
                "NEXT_PUBLIC_GRAPHQL_URL is not defined",
                &[]
            ),
            vec!["NEXT_PUBLIC_GRAPHQL_URL"]
        );
        let why = failure_reason("acme/web", "pnpm run typegen", &missing(&refs, out, &[]));
        assert!(why.contains("`SANITY_STUDIO_PROJECT_ID`"));
        assert!(why.contains("Codegen environment"));
        assert!(why.contains("never secrets"));
        assert_eq!(sk("acme/web"), "SETTINGS#CODEGEN_ENV#acme/web");
    }

    #[test]
    fn the_worker_reads_the_same_key() {
        let src = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../worker/src/passes/mod.rs"),
        )
        .unwrap();
        assert!(src.contains("common::codegen_env::sk("));
    }
}
