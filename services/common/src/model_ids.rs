//! Model ids a team can have saved, and what a retired one runs as now.
//!
//! Anthropic retires model ids; a saved id that no longer exists makes every
//! call fail with `not_found_error`. A retired id is read as its current
//! replacement wherever a team's setting is loaded, so old settings keep
//! working, and the settings page says what changed.

/// A retired id and the id that replaces it.
const RETIRED: &[(&str, &str)] = &[
    ("claude-sonnet-4-20250514", "claude-sonnet-5-5"),
    ("claude-opus-4-20250514", "claude-opus-5-5"),
    ("claude-haiku-4-5-20250514", "claude-haiku-4-5"),
];

/// The id to call for a saved id: its replacement when it is retired.
pub fn current(id: &str) -> &str {
    RETIRED
        .iter()
        .find(|(old, _)| *old == id)
        .map(|(_, new)| *new)
        .unwrap_or(id)
}

/// Whether a saved id is retired.
pub fn is_retired(id: &str) -> bool {
    RETIRED.iter().any(|(old, _)| *old == id)
}

// ── The release's model manifest ──────────────────────────────────────────
//
// `release_models.json` (next to this crate's Cargo.toml) lists the models
// this release calls. The services read it from here, the release workflow
// turns it into each release's `models.json` (selfhost/release-models.mjs,
// with the Bedrock ids for every inference profile prefix), and the updater
// checks the next release's list before it applies it. The test at the
// bottom fails when code names a model id the manifest doesn't list.

/// The manifest's text, built into the binary.
pub const MANIFEST_JSON: &str = include_str!("../release_models.json");

/// The parsed manifest.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Manifest {
    pub schema: u32,
    /// Called by every install of this release (the default models): an
    /// update or a provider switch waits until each one answers.
    pub calls: Vec<String>,
    /// What a team may pick in Settings, AI models.
    pub selectable: Vec<String>,
    /// Models whose Bedrock id isn't just their name (Bedrock has no undated
    /// Haiku 4.5 id).
    #[serde(default)]
    pub bedrock_names: std::collections::BTreeMap<String, String>,
}

/// This release's manifest.
pub fn manifest() -> &'static Manifest {
    static M: std::sync::OnceLock<Manifest> = std::sync::OnceLock::new();
    M.get_or_init(|| serde_json::from_str(MANIFEST_JSON).expect("release_models.json is valid"))
}

/// The models this release calls on every install (the update check and a
/// provider switch probe each one).
pub fn release_calls() -> Vec<&'static str> {
    manifest().calls.iter().map(String::as_str).collect()
}

/// Whether a team may pick this model.
pub fn is_selectable(id: &str) -> bool {
    manifest().selectable.iter().any(|m| m == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_manifest_reads_and_its_calls_are_selectable() {
        let m = manifest();
        assert_eq!(m.schema, 1);
        assert!(!m.calls.is_empty());
        for c in &m.calls {
            assert!(is_selectable(c), "{c} is called but a team can't pick it");
        }
        for (_, new) in RETIRED {
            assert!(
                is_selectable(new),
                "{new} replaces a retired id but isn't listed"
            );
        }
    }

    /// Every model id written in the services, the Lambdas and the infra
    /// config is in the manifest: a release can't start calling a model the
    /// update check doesn't know about. Test code is left out (it names
    /// made-up and old ids on purpose); a family prefix (`claude-opus-5`,
    /// matched with `starts_with`) counts when a listed id starts with it.
    #[test]
    fn code_names_only_models_in_the_manifest() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut files = Vec::new();
        for (dir, ext) in [("services", "rs"), ("lambda", "py"), ("infra/lib", "ts")] {
            collect(&root.join(dir), ext, &mut files);
        }
        assert!(files.len() > 50, "found only {} source files", files.len());
        let m = manifest();
        let known = |id: &str| {
            m.calls.iter().chain(&m.selectable).any(|k| k == id)
                || RETIRED.iter().any(|(old, _)| *old == id)
                || m.calls
                    .iter()
                    .chain(&m.selectable)
                    .any(|k| k.starts_with(&format!("{id}-")))
        };
        let mut missing = Vec::new();
        for f in files {
            let src = std::fs::read_to_string(&f).unwrap_or_default();
            for id in model_literals(non_test_part(&src)) {
                if !known(&id) {
                    missing.push(format!("{}: {id}", f.display()));
                }
            }
        }
        assert!(
            missing.is_empty(),
            "model ids missing from services/common/release_models.json:\n{}",
            missing.join("\n")
        );
    }

    #[test]
    fn literals_and_test_code_are_told_apart() {
        let src = "let m = \"claude-opus-5-5\"; let b = 'claude-haiku-4-5';\n#[cfg(test)]\nmod tests { \"claude-imaginary-9\" }";
        assert_eq!(
            model_literals(non_test_part(src)),
            vec![
                "claude-opus-5-5".to_string(),
                "claude-haiku-4-5".to_string()
            ]
        );
        // Not a whole id inside its quotes: a Bedrock id, a sentence.
        assert!(
            model_literals("\"us.anthropic.claude-opus-5-5\" \"use claude-opus-5-5 here\"")
                .is_empty()
        );
    }

    /// The source before its test module.
    fn non_test_part(src: &str) -> &str {
        src.find("#[cfg(test)]\nmod ")
            .or_else(|| src.find("\nclass Test"))
            .map_or(src, |i| &src[..i])
    }

    /// Quoted strings that are exactly a model id (`claude-<family>-<n>...`).
    fn model_literals(src: &str) -> Vec<String> {
        let re = regex::Regex::new(r#"["']((claude-[a-z]+-[0-9][a-z0-9-]*))["']"#).unwrap();
        re.captures_iter(src).map(|c| c[1].to_string()).collect()
    }

    fn collect(dir: &std::path::Path, ext: &str, out: &mut Vec<std::path::PathBuf>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if p.is_dir() {
                if !matches!(
                    name,
                    "target" | "node_modules" | "tests" | "__pycache__" | "dist"
                ) {
                    collect(&p, ext, out);
                }
            } else if p.extension().and_then(|x| x.to_str()) == Some(ext)
                && !name.starts_with("test_")
                && !name.ends_with("_test.rs")
                && name != "tests.rs"
                && !name.ends_with(".test.ts")
            {
                out.push(p);
            }
        }
    }

    #[test]
    fn a_retired_id_runs_as_its_replacement() {
        assert_eq!(current("claude-sonnet-4-20250514"), "claude-sonnet-5-5");
        assert_eq!(current("claude-opus-4-20250514"), "claude-opus-5-5");
        assert!(is_retired("claude-sonnet-4-20250514"));
    }

    #[test]
    fn a_current_id_is_left_alone() {
        assert_eq!(current("claude-opus-4-8"), "claude-opus-4-8");
        assert!(!is_retired("claude-sonnet-5-5"));
    }
}
