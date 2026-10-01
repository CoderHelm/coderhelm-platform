//! Reviewer-chosen PR labels (opt-in per repo).
//!
//! Many repos drive CI from PR labels — e.g. `E2E:<area>` to run part of an e2e
//! suite, `CI:DEPLOY_STAGING` to deploy a PR to staging first. When a repo turns
//! this on, the reviewer picks those labels for each PR from what the PR changes.
//!
//! Who decides what:
//! - The repo owns the label set: candidates are the repo's CURRENT GitHub labels
//!   (name + description), re-read on every review, so labels the team adds,
//!   renames or deletes are picked up with no CoderHelm change.
//! - The model picks, from the diff, the repo's AGENTS.md / docs (it can read
//!   files such as a test map with its tools) and the per-repo guidance.
//! - This module enforces the per-repo config in code: only labels matching
//!   `allow` are ever added, and `requires` rules add companion labels (e.g. an
//!   area that only runs on staging pulls in the staging-deploy label).
//! - Labels are only ever ADDED (one call, so CI re-runs once); CoderHelm never
//!   removes a label.

use std::collections::HashSet;

/// One label as it exists in the repo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoLabel {
    pub name: String,
    pub description: String,
}

/// The model's pick.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct LabelPick {
    pub name: String,
    #[serde(default)]
    pub reason: String,
}

/// Per-repo label config (REVIEW_CONFIG#REPO item).
#[derive(Debug, Clone, Default)]
pub struct LabelRules {
    /// Labels CoderHelm may add: exact names or `PREFIX*`. Case-insensitive.
    pub allow: Vec<String>,
    /// `pattern -> companion, companion`: when a label matching `pattern` is
    /// added, the companions are added too.
    pub requires: Vec<(String, Vec<String>)>,
    /// Free-text guidance for the model (when to use which label).
    pub guide: String,
}

impl LabelRules {
    /// `allow`: comma/newline-separated. `requires`: one rule per line (or `;`),
    /// `pattern -> a, b` (also accepts `=>`).
    pub fn parse(allow: &str, requires: &str, guide: &str) -> Self {
        let split = |s: &str| -> Vec<String> {
            s.split([',', '\n'])
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        };
        let requires = requires
            .split(['\n', ';'])
            .filter_map(|line| {
                let line = line.trim();
                let (lhs, rhs) = line.split_once("->").or_else(|| line.split_once("=>"))?;
                let lhs = lhs.trim();
                let rhs = split(rhs);
                (!lhs.is_empty() && !rhs.is_empty()).then(|| (lhs.to_string(), rhs))
            })
            .collect();
        Self {
            allow: split(allow),
            requires,
            guide: guide.trim().to_string(),
        }
    }

    pub fn allowed(&self, label: &str) -> bool {
        self.allow.iter().any(|p| pattern_matches(p, label))
    }

    /// Allowed label families (`E2E:*`). A family's labels may be added before
    /// they exist in GitHub — adding one creates it — because repos often define
    /// them in a mapping file (e.g. one e2e area per label) and GitHub only
    /// creates a label the first time it is used.
    pub fn families(&self) -> Vec<String> {
        self.allow
            .iter()
            .filter(|p| p.len() > 1 && p.ends_with('*'))
            .cloned()
            .collect()
    }

    fn in_family(&self, label: &str) -> bool {
        self.families().iter().any(|p| pattern_matches(p, label))
    }
}

/// A name GitHub will accept as a new label, and that clearly extends its
/// family (non-empty after the prefix): `E2E:camps`, not `E2E:` or `E2E: x y`.
fn valid_new_label(name: &str, rules: &LabelRules) -> bool {
    let n = name.trim();
    if n.is_empty() || n.len() > 50 {
        return false;
    }
    if !n
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '-' | '_' | '.' | '/'))
    {
        return false;
    }
    rules.families().iter().any(|p| {
        let prefix = p.trim_end_matches('*');
        n.len() > prefix.len()
            && n.to_ascii_lowercase()
                .starts_with(&prefix.to_ascii_lowercase())
    })
}

/// `E2E:*` matches any label starting with `E2E:`; otherwise exact. Case-insensitive.
fn pattern_matches(pattern: &str, label: &str) -> bool {
    let (p, l) = (pattern.to_ascii_lowercase(), label.to_ascii_lowercase());
    match p.strip_suffix('*') {
        Some(prefix) => l.starts_with(prefix),
        None => p == l,
    }
}

/// The rules actually used for a review. With an explicit allow list in the
/// repo's CoderHelm settings, that list is used as-is. Without one, the repo's
/// own docs decide: a repo label is allowed when its docs (AGENTS.md, CLAUDE.md…)
/// name it — exactly (`CI:DEPLOY_STAGING`) or as a family (`E2E:<area>`,
/// `E2E:*`). So documenting a new label is all a team does to make it available.
/// Doc-derived lists never include production labels (name contains "prod");
/// those must be allowed explicitly in settings.
pub fn effective_rules(
    configured: &LabelRules,
    repo_labels: &[RepoLabel],
    docs: &str,
) -> LabelRules {
    if !configured.allow.is_empty() {
        return configured.clone();
    }
    let mut allow: Vec<String> = repo_labels
        .iter()
        .filter(|l| documented(&l.name, docs) && !is_production(&l.name))
        .map(|l| l.name.clone())
        .collect();
    // Families the docs describe (`E2E:<area>`, `E2E:*`) — usable even before
    // each member label exists in GitHub.
    for fam in doc_families(docs) {
        if !is_production(&fam) && !allow.iter().any(|a| a.eq_ignore_ascii_case(&fam)) {
            allow.push(fam);
        }
    }
    LabelRules {
        allow,
        ..configured.clone()
    }
}

/// The docs mention this label by name, or its family (`PREFIX:<…>` / `PREFIX:*`).
fn documented(name: &str, docs: &str) -> bool {
    if name.trim().is_empty() {
        return false;
    }
    let docs_l = docs.to_ascii_lowercase();
    let name_l = name.to_ascii_lowercase();
    if contains_token(&docs_l, &name_l) {
        return true;
    }
    match name_l.find(':') {
        Some(i) if i > 0 => {
            let prefix = &name_l[..=i];
            docs_l.contains(&format!("{prefix}<")) || docs_l.contains(&format!("{prefix}*"))
        }
        _ => false,
    }
}

/// Label families written in the docs as `PREFIX:<…>` or `PREFIX:*` → `PREFIX:*`.
fn doc_families(docs: &str) -> Vec<String> {
    let bytes = docs.as_bytes();
    let mut out: Vec<String> = Vec::new();
    for (i, w) in bytes.windows(2).enumerate() {
        if w[0] != b':' || !(w[1] == b'<' || w[1] == b'*') {
            continue;
        }
        let mut s = i;
        while s > 0 && (bytes[s - 1].is_ascii_alphanumeric() || matches!(bytes[s - 1], b'_' | b'-'))
        {
            s -= 1;
        }
        if s == i {
            continue;
        }
        let fam = format!("{}:*", &docs[s..i]);
        if !out.iter().any(|f| f.eq_ignore_ascii_case(&fam)) {
            out.push(fam);
        }
    }
    out
}

/// `needle` appears in `hay` not as part of a longer label-ish word
/// (so `CI:E2E` doesn't match inside `CI:E2E_NIGHTLY`).
fn contains_token(hay: &str, needle: &str) -> bool {
    let is_word = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | ':' | '/');
    hay.match_indices(needle).any(|(i, _)| {
        let before = hay[..i].chars().next_back();
        let after = hay[i + needle.len()..].chars().next();
        !before.is_some_and(is_word) && !after.is_some_and(is_word)
    })
}

fn is_production(name: &str) -> bool {
    name.to_ascii_lowercase().contains("prod")
}

/// Labels the model may choose from: the repo's labels that `allow` permits.
pub fn candidates(repo_labels: &[RepoLabel], rules: &LabelRules) -> Vec<RepoLabel> {
    repo_labels
        .iter()
        .filter(|l| rules.allowed(&l.name))
        .cloned()
        .collect()
}

/// Prompt section: the candidate labels (with the repo's own descriptions), the
/// labels already on the PR, the companion rules and the repo's guidance.
pub fn prompt_section(candidates: &[RepoLabel], current: &[String], rules: &LabelRules) -> String {
    let families = rules.families();
    if candidates.is_empty() && families.is_empty() {
        return String::new();
    }
    let mut s = String::from(
        "\n## PR labels to add\n\
         This repo drives CI from PR labels. From what this PR changes, pick the labels below \
         that it needs (read the repo's docs / mapping files with your tools when they define \
         which code a label covers). Pick ONLY from this list, add nothing that isn't needed, \
         and give a one-line reason naming the changed file(s). Report them in `labels`; \
         an empty list is a valid answer. If a label is needed, ADD it yourself — never \
         tell the author to add a label.\n\
         Be strict: a file path matching a label's area is NOT enough. Add a label only when \
         the diff can change the BEHAVIOR that label's tests exercise — logic, data flow, API / \
         payment calls, form validation, state, routing, feature flags, error handling. Purely \
         presentational changes do not qualify, even inside that area: copy/text, fonts, \
         colors, spacing, classNames/styles, icons, comments, renames with no behavior effect. \
         Be strictest with labels that deploy (e.g. staging): they cost a shared environment, \
         so add them only for a functional change that needs them. In the reason, name the \
         behavior that changed (e.g. \"join checkout now sends startDate to createAccount\"), \
         not just the file.\n\
         Also write `tests_note` for the PR author, ALWAYS (even when you add nothing): one or \
         two sentences saying which tests this PR's labels will run — the labels you add plus \
         those already on the PR — named the way the repo's mapping names them (areas / tags, \
         e.g. `@join`), and WHERE they run (the PR preview or staging, per the repo docs). If no \
         tests are needed, say so and why (e.g. \"No e2e: only copy and styles changed in \
         JoinHeader.tsx\").\n",
    );
    for l in candidates {
        if l.description.trim().is_empty() {
            s.push_str(&format!("- {}\n", l.name));
        } else {
            s.push_str(&format!("- {} — {}\n", l.name, l.description.trim()));
        }
    }
    if !families.is_empty() {
        s.push_str(&format!(
            "You may also add any label in these families, even if it isn't listed above yet \
             (adding it creates it): {}. Use ONLY names the repo's docs or mapping files define \
             (e.g. the area names in its test map) — never invent one.\n",
            families
                .iter()
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !current.is_empty() {
        s.push_str(&format!("Already on the PR: {}\n", current.join(", ")));
    }
    if !rules.requires.is_empty() {
        s.push_str("Added automatically with a label (you don't need to pick these):\n");
        for (pat, comps) in &rules.requires {
            s.push_str(&format!("- {pat} → {}\n", comps.join(", ")));
        }
    }
    if !rules.guide.is_empty() {
        s.push_str(&format!("Repo guidance: {}\n", rules.guide));
    }
    s
}

/// What to add, and what was refused (for the log / review body).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LabelDecision {
    /// (exact repo label name, reason), in order, deduplicated, none already on the PR.
    pub add: Vec<(String, String)>,
    /// (requested name, why it was refused)
    pub refused: Vec<(String, String)>,
}

/// A requested label → the name to add: the repo's existing label (exact
/// casing) when allowed, or a new label in an allowed family.
fn resolve(name: &str, repo_labels: &[RepoLabel], rules: &LabelRules) -> Result<String, String> {
    match repo_labels
        .iter()
        .find(|l| l.name.eq_ignore_ascii_case(name.trim()))
    {
        Some(l) if rules.allowed(&l.name) => Ok(l.name.clone()),
        Some(l) => Err(format!("{} is not in this repo's allowed labels", l.name)),
        None if rules.in_family(name.trim()) && valid_new_label(name, rules) => {
            Ok(name.trim().to_string())
        }
        None => Err("no such label in the repo".to_string()),
    }
}

/// Validate the model's picks against the repo's live labels and the config,
/// then apply the companion rules. Pure: no I/O.
pub fn decide(
    picks: &[LabelPick],
    repo_labels: &[RepoLabel],
    current: &[String],
    rules: &LabelRules,
) -> LabelDecision {
    let on_pr: HashSet<String> = current.iter().map(|c| c.to_ascii_lowercase()).collect();
    let mut out = LabelDecision::default();
    let mut chosen: Vec<(String, String)> = Vec::new();
    let push = |chosen: &mut Vec<(String, String)>, name: String, reason: String| {
        if !chosen.iter().any(|(n, _)| n.eq_ignore_ascii_case(&name)) {
            chosen.push((name, reason));
        }
    };

    for p in picks {
        match resolve(&p.name, repo_labels, rules) {
            Ok(name) => push(&mut chosen, name, p.reason.trim().to_string()),
            Err(why) => out.refused.push((p.name.clone(), why)),
        }
    }

    // Companion rules apply to every label that ends up on the PR — the ones
    // picked now and the ones already there.
    let triggers: Vec<String> = chosen
        .iter()
        .map(|(n, _)| n.clone())
        .chain(current.iter().cloned())
        .collect();
    for label in &triggers {
        for (pat, comps) in &rules.requires {
            if !pattern_matches(pat, label) {
                continue;
            }
            for c in comps {
                match resolve(c, repo_labels, rules) {
                    Ok(name) => push(&mut chosen, name, format!("required by {label}")),
                    Err(why) => out
                        .refused
                        .push((c.clone(), format!("required by {label}, but {why}"))),
                }
            }
        }
    }

    out.add = chosen
        .into_iter()
        .filter(|(n, _)| !on_pr.contains(&n.to_ascii_lowercase()))
        .collect();
    out
}

/// The review's "Tests" section, shown on every review when label picking is on:
/// the labels added (with why), then what will run and where — or why nothing is
/// needed — so the author never has to guess whether tests were considered.
pub fn markdown(enabled: bool, decision: &LabelDecision, tests_note: &str) -> String {
    if !enabled {
        return String::new();
    }
    let mut s = String::from("\n\n#### Tests\n");
    if !decision.add.is_empty() {
        s.push_str("Labels added:\n");
        for (name, reason) in &decision.add {
            if reason.is_empty() {
                s.push_str(&format!("- `{name}`\n"));
            } else {
                s.push_str(&format!("- `{name}` — {reason}\n"));
            }
        }
        s.push('\n');
    }
    let note = tests_note.trim();
    if !note.is_empty() {
        s.push_str(note);
        s.push('\n');
    } else if decision.add.is_empty() {
        s.push_str("No test labels needed for this change.\n");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo() -> Vec<RepoLabel> {
        [
            ("CI:E2E", "Run every e2e test"),
            ("E2E:join", "Online join; payments need staging"),
            ("E2E:classes", "Classes listing"),
            ("E2E:auto", "CI picks areas"),
            ("CI:DEPLOY_STAGING", "Deploy this PR to staging"),
            ("CI:DEPLOY_PRODUCTION", "Ship it"),
            ("ch-review", ""),
        ]
        .iter()
        .map(|(n, d)| RepoLabel {
            name: n.to_string(),
            description: d.to_string(),
        })
        .collect()
    }
    fn rules() -> LabelRules {
        LabelRules::parse(
            "E2E:*, CI:E2E\nCI:DEPLOY_STAGING",
            "E2E:join -> CI:DEPLOY_STAGING\nCI:E2E => CI:DEPLOY_STAGING",
            "Payment tests need staging.",
        )
    }
    fn pick(n: &str, r: &str) -> LabelPick {
        LabelPick {
            name: n.into(),
            reason: r.into(),
        }
    }

    #[test]
    fn parses_allow_and_requires() {
        let r = rules();
        assert_eq!(r.allow, vec!["E2E:*", "CI:E2E", "CI:DEPLOY_STAGING"]);
        assert_eq!(r.requires.len(), 2);
        assert_eq!(
            r.requires[0],
            ("E2E:join".into(), vec!["CI:DEPLOY_STAGING".into()])
        );
        assert!(r.allowed("e2e:classes"));
        assert!(!r.allowed("CI:DEPLOY_PRODUCTION"));
        assert!(LabelRules::parse("", "garbage line", "")
            .requires
            .is_empty());
    }

    #[test]
    fn candidates_are_live_repo_labels_filtered_by_allow() {
        let names: Vec<String> = candidates(&repo(), &rules())
            .into_iter()
            .map(|l| l.name)
            .collect();
        assert_eq!(
            names,
            vec![
                "CI:E2E",
                "E2E:join",
                "E2E:classes",
                "E2E:auto",
                "CI:DEPLOY_STAGING"
            ]
        );
    }

    #[test]
    fn staging_area_pulls_in_staging_label() {
        let d = decide(
            &[pick("e2e:JOIN", "online-join changed")],
            &repo(),
            &[],
            &rules(),
        );
        assert_eq!(
            d.add,
            vec![
                ("E2E:join".into(), "online-join changed".into()),
                ("CI:DEPLOY_STAGING".into(), "required by E2E:join".into()),
            ]
        );
        assert!(d.refused.is_empty());
    }

    #[test]
    fn refuses_unknown_and_disallowed_never_adds_them() {
        let d = decide(
            &[
                pick("CI:MADE_UP", "made up"),
                pick("CI:DEPLOY_PRODUCTION", "nope"),
                pick("E2E:classes", "FilterWrapper.tsx"),
            ],
            &repo(),
            &[],
            &rules(),
        );
        assert_eq!(
            d.add,
            vec![("E2E:classes".into(), "FilterWrapper.tsx".into())]
        );
        assert_eq!(d.refused.len(), 2);
    }

    #[test]
    fn skips_labels_already_on_pr_but_still_applies_their_rules() {
        // CI:E2E already there (added by a human) → its companion still lands.
        let d = decide(
            &[pick("E2E:classes", "x")],
            &repo(),
            &["CI:E2E".into()],
            &rules(),
        );
        assert_eq!(
            d.add,
            vec![
                ("E2E:classes".into(), "x".into()),
                ("CI:DEPLOY_STAGING".into(), "required by CI:E2E".into()),
            ]
        );
        let none = decide(
            &[pick("E2E:join", "x")],
            &repo(),
            &["E2E:join".into(), "CI:DEPLOY_STAGING".into()],
            &rules(),
        );
        assert!(none.add.is_empty());
    }

    #[test]
    fn companion_must_be_allowed_and_exist() {
        let r = LabelRules::parse("E2E:*", "E2E:join -> CI:DEPLOY_STAGING, CI:GONE", "");
        let d = decide(&[pick("E2E:join", "x")], &repo(), &[], &r);
        assert_eq!(d.add, vec![("E2E:join".into(), "x".into())]);
        assert_eq!(d.refused.len(), 2);
    }

    #[test]
    fn duplicates_collapse() {
        let d = decide(
            &[
                pick("E2E:join", "a"),
                pick("e2e:join", "b"),
                pick("CI:DEPLOY_STAGING", "c"),
            ],
            &repo(),
            &[],
            &rules(),
        );
        assert_eq!(
            d.add,
            vec![
                ("E2E:join".into(), "a".into()),
                ("CI:DEPLOY_STAGING".into(), "c".into()),
            ]
        );
    }

    #[test]
    fn empty_allow_uses_labels_the_repo_docs_name() {
        let docs = "| `CI:E2E` | every test |\n| `E2E:<area>` | that area |\n\
                    Payment tests (`E2E:join`) need staging: add `CI:DEPLOY_STAGING`.\n\
                    Never touch CI:DEPLOY_PRODUCTION from a review.";
        let r = effective_rules(&LabelRules::default(), &repo(), docs);
        assert_eq!(
            r.allow,
            vec![
                "CI:E2E",
                "E2E:join",
                "E2E:classes",
                "E2E:auto",
                "CI:DEPLOY_STAGING",
                "E2E:*"
            ]
        );
        // production is never doc-derived, even when the docs name it
        assert!(!r.allowed("CI:DEPLOY_PRODUCTION"));
        // undocumented labels stay out
        assert!(!r.allowed("ch-review"));
    }

    #[test]
    fn explicit_allow_wins_and_can_include_production() {
        let explicit = LabelRules::parse("CI:DEPLOY_PRODUCTION", "", "");
        let r = effective_rules(&explicit, &repo(), "E2E:<area>");
        assert_eq!(r.allow, vec!["CI:DEPLOY_PRODUCTION"]);
    }

    #[test]
    fn exact_names_must_be_whole_tokens() {
        assert!(documented("CI:E2E", "add `CI:E2E` to run all"));
        assert!(!documented("CI:E2E", "add CI:E2E_NIGHTLY instead"));
        assert!(documented("E2E:mobile", "labels: E2E:* for areas"));
        assert!(!documented("E2E:mobile", "nothing relevant"));
        assert!(!documented("ch-review", "use ch-reviewer"));
    }

    #[test]
    fn tests_section_always_present_when_enabled() {
        let added = LabelDecision {
            add: vec![
                ("E2E:join".into(), "checkout now sends startDate".into()),
                ("CI:DEPLOY_STAGING".into(), "required by E2E:join".into()),
            ],
            refused: vec![],
        };
        let s = markdown(true, &added, "Runs @join on staging after the deploy.");
        assert!(s.contains("#### Tests"));
        assert!(s.contains("- `E2E:join` — checkout now sends startDate"));
        assert!(s.contains("Runs @join on staging after the deploy."));

        let none = markdown(
            true,
            &LabelDecision::default(),
            "No e2e: only styles changed in JoinHeader.tsx.",
        );
        assert!(none.contains("#### Tests"));
        assert!(none.contains("only styles changed"));
        assert!(!none.contains("Labels added"));

        // model gave no note and added nothing → still an explicit line
        assert!(markdown(true, &LabelDecision::default(), " ").contains("No test labels needed"));
        // feature off → nothing
        assert_eq!(markdown(false, &added, "x"), "");
    }

    #[test]
    fn family_labels_can_be_added_before_they_exist() {
        // Only CI:E2E / CI:DEPLOY_STAGING exist in GitHub; E2E:camps doesn't yet.
        let existing: Vec<RepoLabel> = repo()
            .into_iter()
            .filter(|l| !l.name.starts_with("E2E:"))
            .collect();
        let d = decide(
            &[pick("E2E:camps", "Sold out / waitlist state")],
            &existing,
            &[],
            &rules(),
        );
        assert_eq!(
            d.add,
            vec![("E2E:camps".into(), "Sold out / waitlist state".into())]
        );
        // a new family member still has to be a sane label name
        let bad = decide(
            &[pick("E2E:", "x"), pick("E2E: two words", "x")],
            &existing,
            &[],
            &rules(),
        );
        assert!(bad.add.is_empty());
        assert_eq!(bad.refused.len(), 2);
        // outside any family: still refused
        let other = decide(&[pick("CI:NEW_THING", "x")], &existing, &[], &rules());
        assert!(other.add.is_empty());
    }

    #[test]
    fn docs_families_become_allowed_without_existing_labels() {
        let existing: Vec<RepoLabel> = repo()
            .into_iter()
            .filter(|l| !l.name.starts_with("E2E:"))
            .collect();
        let docs = "| `E2E:<area>` | that area's tests |\n| `CI:E2E` | all |\nPROD:<x> never";
        let r = effective_rules(&LabelRules::default(), &existing, docs);
        assert!(r.allow.contains(&"E2E:*".to_string()));
        assert!(!r.allow.iter().any(|a| a.to_lowercase().starts_with("prod")));
        assert_eq!(r.families(), vec!["E2E:*".to_string()]);
        let d = decide(&[pick("E2E:join", "x")], &existing, &[], &r);
        assert_eq!(d.add[0].0, "E2E:join");
    }

    #[test]
    fn prompt_offers_families_and_forbids_deferring_to_author() {
        let r = rules();
        let s = prompt_section(&[], &[], &r);
        assert!(s.contains("`E2E:*`"));
        assert!(
            s.contains("never \n         tell the author") || s.contains("never tell the author")
        );
    }

    #[test]
    fn prompt_lists_candidates_rules_and_guide() {
        let r = rules();
        let s = prompt_section(&candidates(&repo(), &r), &["ch-review".into()], &r);
        assert!(s.contains("- E2E:join — Online join; payments need staging"));
        assert!(!s.contains("DEPLOY_PRODUCTION"));
        assert!(s.contains("Already on the PR: ch-review"));
        assert!(s.contains("E2E:join → CI:DEPLOY_STAGING"));
        assert!(s.contains("Payment tests need staging."));
        // strict: path match alone isn't enough; cosmetic changes don't qualify
        assert!(s.contains("a file path matching a label's area is NOT enough"));
        assert!(s.contains("Purely presentational changes do not qualify"));
        assert!(s.contains("Also write `tests_note`"));
        // nothing to pick (no allowed labels, no families) → no section
        let none = LabelRules::parse("CI:E2E", "", "");
        assert_eq!(prompt_section(&[], &[], &none), "");
    }
}
