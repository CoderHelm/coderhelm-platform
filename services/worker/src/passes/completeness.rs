//! What a pull request covers besides its diff, enforced in code.
//!
//! A reviewed infrastructure PR copied an existing block without saying
//! which, covered only this repository's half of the job, changed two
//! environments while confirming one, led with a warning banner, and shipped
//! a path whose environment had been stripped to `//`. The rules here are the
//! parts of the fix that don't rest on a prompt:
//!
//! - the block a change mirrors, read from the coder's `mirrors.json` notes
//!   and rendered under Changes ([`parse_mirrors`], [`mirrors_section`]);
//! - the steps outside the PR, from the plan's end-to-end checklist
//!   ([`not_in_pr_section`], [`checklist_gaps`]), and the merge order of a
//!   multi-repo run ([`merge_order_section`]);
//! - the environments the diff changes, from its paths, each needing its own
//!   evidence line ([`changed_environments`], [`env_gaps`]);
//! - the order of the description's sections, with CoderHelm's own sections
//!   (between `<!-- coderhelm:... -->` markers) kept whole when a writer
//!   rewrites the rest ([`take_owned`], [`assemble`]);
//! - holes a template or Markdown left in the description
//!   ([`placeholder_holes`]).

use serde::{Deserialize, Serialize};

use super::fix_location::FixDecision;
use super::proof::Finding;

/// The fence the coder names the blocks it copied in.
pub const MIRRORS_FENCE: &str = "mirrors.json";

/// An existing block a change copied, and how the copy differs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mirror {
    /// The file the copied block is in.
    #[serde(default)]
    pub path: String,
    /// The block: a resource, function, key or entry name.
    #[serde(default)]
    pub name: String,
    /// Each intentional difference, with its reason.
    #[serde(default, deserialize_with = "super::fix_location::string_or_list")]
    pub differences: Vec<String>,
}

/// The blocks the coder's notes say the change mirrors: the last
/// ```mirrors.json block in them (a fix round's notes come after the first
/// pass's, so the latest account wins). Entries without a path or a name
/// are dropped; an unreadable block reads as none.
pub fn parse_mirrors(notes: &str) -> Vec<Mirror> {
    let marker = format!("```{MIRRORS_FENCE}");
    let Some(start) = notes.rfind(&marker) else {
        return Vec::new();
    };
    let Some(nl) = notes[start..].find('\n') else {
        return Vec::new();
    };
    let body_start = start + nl + 1;
    let end = notes[body_start..]
        .find("```")
        .map(|i| body_start + i)
        .unwrap_or(notes.len());
    let raw = notes[body_start..end].trim();
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        Many(Vec<Mirror>),
        Wrapped { mirrors: Vec<Mirror> },
        One(Mirror),
    }
    let list = match serde_json::from_str::<OneOrMany>(raw) {
        Ok(OneOrMany::Many(v)) | Ok(OneOrMany::Wrapped { mirrors: v }) => v,
        Ok(OneOrMany::One(m)) => vec![m],
        Err(e) => {
            tracing::warn!(error = %e, "The coder's mirrors.json block didn't read");
            Vec::new()
        }
    };
    let mut out: Vec<Mirror> = Vec::new();
    for m in list {
        let m = Mirror {
            path: plain(&m.path),
            name: plain(&m.name),
            differences: m
                .differences
                .iter()
                .map(|d| one_line(d))
                .filter(|d| !d.is_empty())
                .collect(),
        };
        if !m.path.is_empty() && !m.name.is_empty() && !out.contains(&m) {
            out.push(m);
        }
    }
    out
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// A name or path as written inside backticks: one line, no backticks.
fn plain(s: &str) -> String {
    one_line(&s.replace('`', ""))
}

/// The sections CoderHelm writes itself, each between its own markers so it
/// is found again, kept whole when the rest is rewritten, and put back in
/// its place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    /// The blocks the change copied, under Changes.
    Mirrors,
    /// The sibling PRs of a multi-repo run, in merge order.
    MergeOrder,
    /// The steps outside this PR, each with its owner.
    NotInPr,
    /// What is still open, at the bottom (#238's "Not yet verified").
    OpenItems,
}

impl Section {
    const ALL: [Section; 4] = [
        Section::Mirrors,
        Section::MergeOrder,
        Section::NotInPr,
        Section::OpenItems,
    ];

    /// Its start and end markers.
    pub fn markers(self) -> (&'static str, &'static str) {
        match self {
            Section::Mirrors => ("<!-- coderhelm:mirrors -->", "<!-- /coderhelm:mirrors -->"),
            Section::MergeOrder => (
                "<!-- coderhelm:merge-order -->",
                "<!-- /coderhelm:merge-order -->",
            ),
            Section::NotInPr => (
                "<!-- coderhelm:not-in-pr -->",
                "<!-- /coderhelm:not-in-pr -->",
            ),
            Section::OpenItems => (super::proof::UNVERIFIED_START, super::proof::UNVERIFIED_END),
        }
    }
}

/// Splits `body` into the section between `start` and `end` (markers
/// included, with the blank lines after it) and the rest. The section is
/// empty when either marker is missing.
pub fn split_section(body: &str, start: &str, end: &str) -> (String, String) {
    let Some(s) = body.find(start) else {
        return (String::new(), body.to_string());
    };
    let Some(e) = body[s..].find(end).map(|e| s + e) else {
        return (String::new(), body.to_string());
    };
    let mut close = e + end.len();
    while body[close..].starts_with('\n') {
        close += 1;
    }
    (
        body[s..close].to_string(),
        format!("{}{}", &body[..s], &body[close..]),
    )
}

/// CoderHelm's own sections of a description, as written.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Owned {
    pub mirrors: String,
    pub merge_order: String,
    pub not_in_pr: String,
    pub open_items: String,
}

impl Owned {
    fn slot(&mut self, s: Section) -> &mut String {
        match s {
            Section::Mirrors => &mut self.mirrors,
            Section::MergeOrder => &mut self.merge_order,
            Section::NotInPr => &mut self.not_in_pr,
            Section::OpenItems => &mut self.open_items,
        }
    }

    /// Sets one section (empty removes it).
    pub fn set(&mut self, s: Section, text: String) {
        *self.slot(s) = text;
    }
}

/// Takes CoderHelm's sections out of a description: what they say, and the
/// rest of the body without them.
pub fn take_owned(body: &str) -> (Owned, String) {
    let mut owned = Owned::default();
    let mut rest = body.to_string();
    for s in Section::ALL {
        let (start, end) = s.markers();
        let (section, without) = split_section(&rest, start, end);
        if !section.is_empty() {
            owned.set(s, section.trim_end().to_string());
            rest = without;
        }
    }
    (owned, rest)
}

/// Whether `line` is a section heading (`## Risk`, `**Risk**`,
/// `3. **Risk** — …`) whose name starts with `name`.
fn is_heading(line: &str, name: &str) -> bool {
    let t = line.trim();
    let marked =
        t.starts_with('#') || t.starts_with("**") || t.starts_with(|c: char| c.is_ascii_digit());
    if !marked {
        return false;
    }
    let t = t.trim_start_matches('#').trim_start();
    let t = t.trim_start_matches(|c: char| c.is_ascii_digit() || c == '.' || c == ')');
    let t = t.trim_start().trim_start_matches("**").trim_start();
    let lower = t.to_ascii_lowercase();
    lower.starts_with(name)
        && !lower[name.len()..]
            .chars()
            .next()
            .is_some_and(|c| c.is_alphanumeric())
}

/// The trailing lines of a description that are its footer (the `<sub>`
/// line naming consulted team agents, HTML comments), split off so Open
/// items go above them.
fn split_footer(body: &str) -> (String, String) {
    let lines: Vec<&str> = body.trim_end().lines().collect();
    let mut cut = lines.len();
    while cut > 0 {
        let l = lines[cut - 1].trim();
        let footer = l.is_empty()
            || l.starts_with("<sub>")
            || (l.starts_with("<!--")
                && !l.starts_with("<!-- coderhelm:")
                && !l.starts_with("<!-- /coderhelm:"));
        if !footer {
            break;
        }
        cut -= 1;
    }
    let footer = lines[cut..].join("\n");
    let footer = footer.trim();
    (
        lines[..cut].join("\n"),
        if footer.is_empty() {
            String::new()
        } else {
            format!("\n\n{footer}")
        },
    )
}

/// The Open items section in its current shape. A section written before it
/// moved to the bottom (a `> [!WARNING]` callout headed "Not yet verified")
/// is rewritten: the callout and quote marks go, the items and their CI
/// marks stay.
pub fn normalize_open_items(section: &str) -> String {
    let (start, end) = Section::OpenItems.markers();
    let inner = section
        .trim()
        .trim_start_matches(start)
        .trim_end_matches(end)
        .trim();
    let items: Vec<String> = inner
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .filter(|l| !l.trim_start_matches('>').trim().starts_with("[!"))
        .filter(|l| {
            let t = l.trim_start_matches('>').trim();
            t != "**Not yet verified**" && t != "## Open items"
        })
        .map(|l| l.trim_start_matches('>').trim().to_string())
        .collect();
    if !items
        .iter()
        .any(|l| l.starts_with("- ") || l.starts_with("* "))
    {
        return String::new();
    }
    format!("{start}\n## Open items\n{}\n{end}", items.join("\n"))
}

/// Puts a description together in its order: the writer's Summary and
/// Changes, then the blocks the change mirrors, the merge order and the
/// steps not in this PR (before the Risk heading, or before Verification
/// when there is no Risk), the writer's Risk and Verification, then Open
/// items at the bottom, above the footer. Empty sections are left out.
pub fn assemble(rest: &str, owned: &Owned) -> String {
    let (main, footer) = split_footer(rest);
    let main = main.trim();
    let middle: Vec<&str> = [&owned.mirrors, &owned.merge_order, &owned.not_in_pr]
        .into_iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    let mut body = if middle.is_empty() {
        main.to_string()
    } else {
        let middle = middle.join("\n\n");
        let lines: Vec<&str> = main.lines().collect();
        let at = lines
            .iter()
            .position(|l| is_heading(l, "risk"))
            .or_else(|| lines.iter().position(|l| is_heading(l, "verification")));
        match at {
            Some(i) => {
                let before = lines[..i].join("\n");
                let after = lines[i..].join("\n");
                format!("{}\n\n{middle}\n\n{after}", before.trim_end())
            }
            None if main.is_empty() => middle,
            None => format!("{main}\n\n{middle}"),
        }
    };
    let open = normalize_open_items(&owned.open_items);
    if !open.is_empty() {
        body = if body.is_empty() {
            open
        } else {
            format!("{body}\n\n{open}")
        };
    }
    format!("{body}{footer}")
}

/// `body` with one of CoderHelm's sections set to `text` (empty removes it),
/// every section in its place.
pub fn with_section(body: &str, section: Section, text: String) -> String {
    let (mut owned, rest) = take_owned(body);
    owned.set(section, text);
    assemble(&rest, &owned)
}

/// The blocks the change copied, as the Changes section names them. Empty
/// when it copied none.
pub fn mirrors_section(mirrors: &[Mirror]) -> String {
    if mirrors.is_empty() {
        return String::new();
    }
    let (start, end) = Section::Mirrors.markers();
    let mut out = format!("{start}\n**Copies an existing block**\n");
    for m in mirrors {
        let diff = if m.differences.is_empty() {
            "no differences".to_string()
        } else {
            format!("differences: {}", m.differences.join("; "))
        };
        out.push_str(&format!("* mirrors `{}` in `{}`; {diff}\n", m.name, m.path));
    }
    out.push_str(end);
    out
}

/// The section listing every step outside this PR, one bullet each with its
/// owner. Empty when the plan has none.
pub fn not_in_pr_section(steps: &[(String, String)]) -> String {
    if steps.is_empty() {
        return String::new();
    }
    let (start, end) = Section::NotInPr.markers();
    let mut out = format!("{start}\n## Not in this PR\n");
    for (step, owner) in steps {
        out.push_str(&format!("* {step} (owner: {owner})\n"));
    }
    out.push_str(end);
    out
}

/// One PR of a multi-repo run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sibling {
    /// `owner/name`.
    pub repo: String,
    pub number: u64,
}

/// The merge order a multi-repo run's PR carries: every PR of the run in the
/// order they merge, this one marked. Empty for a single PR.
pub fn merge_order_section(prs: &[Sibling], this: usize) -> String {
    if prs.len() < 2 {
        return String::new();
    }
    let (start, end) = Section::MergeOrder.markers();
    let mut out = format!("{start}\n## Merge order\n");
    for (i, p) in prs.iter().enumerate() {
        let me = if i == this { " (this PR)" } else { "" };
        out.push_str(&format!("{}. {}#{}{me}\n", i + 1, p.repo, p.number));
    }
    out.push_str(end);
    out
}

/// What the plan's checklist leaves for a person to settle, whatever the
/// judge says: a code plan with no checklist at all, and steps outside the
/// PR with no owner. Each is an Open item, never a fix round (the coder
/// can't change the plan).
pub fn checklist_gaps(d: Option<&FixDecision>, repo: &str) -> Vec<Finding> {
    let Some(d) = d else {
        return Vec::new();
    };
    if d.end_to_end.is_empty() {
        return vec![Finding {
            point: "end_to_end".into(),
            detail: "The plan has no end-to-end checklist, so nothing says what else production \
                     needs beyond this PR."
                .into(),
            settle: "List every step the ticket needs in production and who does it, and add the \
                     ones outside this PR under Not in this PR."
                .into(),
            ..Default::default()
        }];
    }
    d.end_to_end
        .iter()
        .filter(|s| !s.step.trim().is_empty() && !s.in_pr(repo))
        .filter(|s| s.owner.trim().is_empty() && s.repo.trim().is_empty())
        .map(|s| Finding {
            point: "end_to_end".into(),
            detail: format!("No owner is named for: {}", one_line(&s.step)),
            settle: "Name the team, person or vendor who does it.".into(),
            ..Default::default()
        })
        .collect()
}

/// Each environment name and the words that mean it.
const ENVIRONMENTS: &[(&str, &[&str])] = &[
    ("prod", &["prod", "production", "prd"]),
    ("staging", &["staging", "stage", "stg"]),
    ("dev", &["dev", "development"]),
    ("qa", &["qa"]),
    ("uat", &["uat"]),
    ("preprod", &["preprod"]),
    ("sandbox", &["sandbox"]),
];

/// Directories whose children are environments (`envs/prod/…`).
const ENV_DIRS: &[&str] = &[
    "envs",
    "env",
    "environments",
    "environment",
    "overlays",
    "stages",
];

/// File types that hold per-environment configuration.
const CONFIG_EXTS: &[&str] = &[
    "yaml",
    "yml",
    "json",
    "tfvars",
    "tf",
    "hcl",
    "toml",
    "ini",
    "env",
    "properties",
    "conf",
    "cfg",
];

fn canonical_env(word: &str) -> Option<&'static str> {
    let w = word.to_ascii_lowercase();
    ENVIRONMENTS
        .iter()
        .find(|(_, words)| words.contains(&w.as_str()))
        .map(|(name, _)| *name)
}

/// The environment one changed file belongs to, if any: the child of an
/// environments directory (`envs/<name>/`), a directory named after one
/// (`staging/`, `prod/`), or a configuration file named after one
/// (`values-prod.yaml`, `prod.tfvars`, `.env.staging`).
fn environment_of(path: &str) -> Option<String> {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    let last = parts.len().checked_sub(1)?;
    for (i, part) in parts.iter().enumerate() {
        let lower = part.to_ascii_lowercase();
        if ENV_DIRS.contains(&lower.as_str()) && i < last {
            let child = parts[i + 1];
            let child = if i + 1 == last {
                // `env/prod.tfvars`: the file's stem names it.
                child.split('.').next().unwrap_or(child)
            } else {
                child
            };
            let child = child.to_ascii_lowercase();
            if child.is_empty() {
                continue;
            }
            return Some(canonical_env(&child).map_or(child.clone(), str::to_string));
        }
        if i < last {
            if let Some(env) = canonical_env(&lower) {
                return Some(env.to_string());
            }
        }
    }
    let file = parts[last].to_ascii_lowercase();
    let ext = file.rsplit('.').next().unwrap_or("");
    let config = file.starts_with(".env") || CONFIG_EXTS.contains(&ext);
    if !config {
        return None;
    }
    file.split(['.', '-', '_'])
        .find_map(canonical_env)
        .map(str::to_string)
}

/// The environments a diff changes, from its file paths, in the order first
/// seen.
pub fn changed_environments(paths: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for p in paths {
        if let Some(env) = environment_of(p) {
            if !out.contains(&env) {
                out.push(env);
            }
        }
    }
    out
}

/// Whether `line` is one of the coder's evidence lines (`- Confirmed:` or a
/// line marking a fact unconfirmed).
fn is_evidence_line(line: &str) -> bool {
    let t = line
        .trim()
        .trim_start_matches(['>', '-', '*', ' '])
        .trim_start_matches("**")
        .to_ascii_lowercase();
    [
        "confirmed:",
        "unconfirmed:",
        "unverified:",
        "not confirmed:",
        "not verified:",
    ]
    .iter()
    .any(|m| t.starts_with(m))
}

/// Whether `line` names environment `env` (any word that means it, or the
/// name itself for an environment with its own name).
fn names_env(line: &str, env: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    match ENVIRONMENTS.iter().find(|(name, _)| *name == env) {
        Some((_, words)) => lower
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|w| words.contains(&w)),
        None => lower.contains(&env.to_ascii_lowercase()),
    }
}

/// When a diff changes more than one environment, each needs its own
/// evidence line: a `- Confirmed:` line from a read-only lookup naming it, or
/// one `- Unconfirmed:` line naming it and the access that is missing. A gap
/// for each environment no evidence line names.
pub fn env_gaps(envs: &[String], notes: &str) -> Vec<Finding> {
    if envs.len() < 2 {
        return Vec::new();
    }
    let lines: Vec<&str> = notes.lines().filter(|l| is_evidence_line(l)).collect();
    envs.iter()
        .filter(|env| !lines.iter().any(|l| names_env(l, env)))
        .map(|env| Finding {
            point: "environment".into(),
            detail: format!(
                "The diff changes the {env} environment, and no evidence line confirms it or \
                 says what access is missing."
            ),
            settle: format!(
                "Confirm {env} with a read-only lookup and write a `- Confirmed:` line naming it, \
                 or one `- Unconfirmed:` line naming {env} and the access that is missing."
            ),
            ..Default::default()
        })
        .collect()
}

/// HTML tags GitHub keeps in a description; any other `<word>` is stripped
/// when it renders, leaving a hole.
const KEPT_TAGS: &[&str] = &[
    "a",
    "b",
    "blockquote",
    "br",
    "code",
    "dd",
    "del",
    "details",
    "div",
    "dl",
    "dt",
    "em",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "i",
    "img",
    "ins",
    "kbd",
    "li",
    "mark",
    "ol",
    "p",
    "picture",
    "pre",
    "q",
    "s",
    "samp",
    "small",
    "source",
    "span",
    "strong",
    "sub",
    "summary",
    "sup",
    "table",
    "tbody",
    "td",
    "th",
    "thead",
    "tr",
    "u",
    "ul",
    "var",
];

/// The word of text around byte `at`, for naming where a hole is.
fn word_at(text: &str, at: usize) -> String {
    let start = text[..at]
        .rfind(char::is_whitespace)
        .map(|i| i + 1)
        .unwrap_or(0);
    let end = text[at..]
        .find(char::is_whitespace)
        .map(|i| at + i)
        .unwrap_or(text.len());
    text[start..end].to_string()
}

/// Whether `inner` (what is between `<` and `>`) reads as a placeholder
/// name: letters, digits, `_`, `-`, spaces.
fn placeholder_like(inner: &str) -> bool {
    !inner.trim().is_empty()
        && inner
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | ' '))
}

/// Holes in one stretch of text, inline code or not.
fn holes_in(text: &str, code: bool, out: &mut Vec<String>) {
    let mut push = |h: String| {
        if !out.contains(&h) {
            out.push(h);
        }
    };
    // `${{ matrix.env }}` is a CI workflow expression quoted as CI runs it,
    // not a template left unfilled.
    let template = text
        .match_indices("${")
        .map(|(i, _)| i)
        .find(|&i| !text[i + 2..].starts_with('{'));
    if let Some(i) = template {
        push(format!(
            "`{}`: a template variable left unfilled",
            word_at(text, i)
        ));
    }
    for empty in ["{}", "{ }"] {
        if let Some(i) = text.find(empty) {
            push(format!(
                "`{}`: an empty `{{}}` placeholder",
                word_at(text, i)
            ));
        }
    }
    if let Some(i) = text.find("<>") {
        push(format!("`{}`: an empty `<>` placeholder", word_at(text, i)));
    }
    let bytes = text.as_bytes();
    let mut from = 0;
    while let Some(off) = text[from..].find("//") {
        let i = from + off;
        from = i + 2;
        let prev = if i == 0 { None } else { Some(bytes[i - 1]) };
        let next = bytes.get(i + 2).copied();
        let hole = match prev {
            None => false,
            Some(p) => {
                !p.is_ascii_whitespace()
                    && !matches!(p, b':' | b'/' | b'(' | b'"' | b'\'' | b'`' | b'[')
                    && next != Some(b'/')
            }
        };
        if hole {
            push(format!(
                "`{}`: an empty path segment (`//`)",
                word_at(text, i)
            ));
            break;
        }
    }
    let mut from = 0;
    while let Some(off) = text[from..].find('<') {
        let i = from + off;
        from = i + 1;
        let Some(close) = text[i + 1..].find('>').map(|c| i + 1 + c) else {
            break;
        };
        let inner = &text[i + 1..close];
        if inner.starts_with('!') || inner.len() > 40 {
            continue;
        }
        let hole = if code {
            // Inside code a placeholder renders literally; only one standing
            // in for a path or name part counts (`/app/<env>/db`), never a
            // type parameter (`Vec<String>`).
            let prev = if i == 0 { None } else { Some(bytes[i - 1]) };
            matches!(prev, None | Some(b'/' | b'.' | b'-' | b'_' | b' ' | b'='))
                && inner
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || matches!(c, '_' | '-'))
                && !inner.is_empty()
        } else {
            let tag = inner
                .trim_start_matches('/')
                .split([' ', '/'])
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            placeholder_like(inner) && !KEPT_TAGS.contains(&tag.as_str())
        };
        if hole {
            push(format!(
                "`{}`: `<{inner}>` is a placeholder{}",
                word_at(text, i),
                if code { "" } else { " Markdown strips" }
            ));
        }
    }
}

/// The holes in a description where a value should be: a template variable
/// left as written (`${env}`), an empty `{}` or `<>`, an empty path segment
/// (`/thing//env`), empty inline code, and `<name>` placeholders (Markdown
/// strips them outside code, so they render as nothing). Fenced code blocks
/// and HTML comments are skipped. Each hole once, naming the text around it.
pub fn placeholder_holes(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut fenced = false;
    for raw in body.lines() {
        let t = raw.trim_start();
        if t.starts_with("```") || t.starts_with("~~~") {
            fenced = !fenced;
            continue;
        }
        if fenced {
            continue;
        }
        // HTML comments (CoderHelm's markers among them) render as nothing
        // on purpose.
        let mut line = raw.to_string();
        while let Some(s) = line.find("<!--") {
            match line[s..].find("-->") {
                Some(e) => line.replace_range(s..s + e + 3, ""),
                None => {
                    line.truncate(s);
                    break;
                }
            }
        }
        // Inline code spans: a run of N backticks to the next run of N.
        let chars: Vec<char> = line.chars().collect();
        let mut prose = String::new();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] != '`' {
                prose.push(chars[i]);
                i += 1;
                continue;
            }
            let mut n = 0;
            while i + n < chars.len() && chars[i + n] == '`' {
                n += 1;
            }
            let open_end = i + n;
            let mut j = open_end;
            let mut close = None;
            while j < chars.len() {
                if chars[j] == '`' {
                    let mut m = 0;
                    while j + m < chars.len() && chars[j + m] == '`' {
                        m += 1;
                    }
                    if m == n {
                        close = Some(j);
                        break;
                    }
                    j += m;
                } else {
                    j += 1;
                }
            }
            match close {
                Some(c) => {
                    let span: String = chars[open_end..c].iter().collect();
                    if span.trim().is_empty() {
                        let h =
                            "empty inline code (two backticks with nothing between)".to_string();
                        if !out.contains(&h) {
                            out.push(h);
                        }
                    } else {
                        holes_in(&span, true, &mut out);
                    }
                    prose.push(' ');
                    i = c + n;
                }
                None if n == 2 => {
                    // A lone pair of backticks: inline code whose value
                    // came through empty.
                    let h = "empty inline code (two backticks with nothing between)".to_string();
                    if !out.contains(&h) {
                        out.push(h);
                    }
                    prose.push(' ');
                    i = open_end;
                }
                None => {
                    // An unmatched run is literal text.
                    for _ in 0..n {
                        prose.push('`');
                    }
                    i = open_end;
                }
            }
        }
        holes_in(&prose, false, &mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::fix_location::{EndToEndStep, FixLocation};

    fn step(step: &str, by: &str, owner: &str, repo: &str) -> EndToEndStep {
        EndToEndStep {
            step: step.into(),
            by: by.into(),
            owner: owner.into(),
            repo: repo.into(),
        }
    }

    #[test]
    fn the_block_a_change_copies_comes_from_the_coders_notes() {
        let notes = "Added the reporting user.\n\nEvidence:\n- Confirmed: x\n\n```mirrors.json\n[{\"path\": \"envs/prod/users.tf\", \"name\": \"`analytics_reader`\", \"differences\": [\"read-only grant, because it only reads reports\"]}, {\"path\": \"\", \"name\": \"no path\"}]\n```";
        let m = parse_mirrors(notes);
        assert_eq!(
            m,
            vec![Mirror {
                path: "envs/prod/users.tf".into(),
                name: "analytics_reader".into(),
                differences: vec!["read-only grant, because it only reads reports".into()],
            }]
        );
        // The latest account wins; a single object and a plain string read too.
        let later = format!(
            "{notes}\n\nAfter a fix round:\n```mirrors.json\n{{\"path\": \"a.tf\", \"name\": \"b\", \"differences\": \"none needed\"}}\n```"
        );
        assert_eq!(parse_mirrors(&later)[0].differences, vec!["none needed"]);
        assert!(parse_mirrors("no block").is_empty());
        assert!(parse_mirrors("```mirrors.json\nnot json\n```").is_empty());
        assert!(parse_mirrors("```mirrors.json\n[]\n```").is_empty());

        let s = mirrors_section(&m);
        assert!(s.starts_with("<!-- coderhelm:mirrors -->\n**Copies an existing block**\n"));
        assert!(s.contains(
            "* mirrors `analytics_reader` in `envs/prod/users.tf`; differences: read-only grant, because it only reads reports"
        ));
        assert_eq!(mirrors_section(&[]), "");
    }

    #[test]
    fn steps_outside_the_pr_each_get_an_owner() {
        let d = FixDecision {
            fix_location: FixLocation::Code,
            end_to_end: vec![
                step("Create the database user", "this_pr", "", ""),
                step(
                    "Store its password in the secrets manager",
                    "this_pr",
                    "",
                    "",
                ),
                step(
                    "Allow the vendor's IP range on the database firewall",
                    "person",
                    "the network team",
                    "",
                ),
                step(
                    "Grant read on the reporting schema",
                    "other_repo",
                    "",
                    "acme/db-grants",
                ),
                step("Send the vendor the connection details", "vendor", "", ""),
            ],
            ..Default::default()
        };
        let steps = crate::passes::fix_location::not_in_pr_steps(Some(&d), "acme/infra");
        assert_eq!(
            steps,
            vec![
                (
                    "Allow the vendor's IP range on the database firewall".to_string(),
                    "the network team".to_string()
                ),
                (
                    "Grant read on the reporting schema".to_string(),
                    "a pull request in acme/db-grants".to_string()
                ),
                (
                    "Send the vendor the connection details".to_string(),
                    "not named yet".to_string()
                ),
            ]
        );
        let s = not_in_pr_section(&steps);
        assert!(s.contains("## Not in this PR\n* Allow the vendor's IP range on the database firewall (owner: the network team)\n"));
        assert_eq!(not_in_pr_section(&[]), "");
        // The step with nobody named is an Open item, never a fix round.
        let g = checklist_gaps(Some(&d), "acme/infra");
        assert_eq!(g.len(), 1, "{g:?}");
        assert!(g[0]
            .detail
            .contains("Send the vendor the connection details"));
        assert_eq!(g[0].settler(), crate::passes::proof::Settler::Owner);
        // A plan with no checklist is one too; no plan, nothing to hold.
        let none = checklist_gaps(Some(&FixDecision::default()), "acme/infra");
        assert!(none[0].detail.contains("no end-to-end checklist"));
        assert!(checklist_gaps(None, "acme/infra").is_empty());
    }

    #[test]
    fn a_two_repo_run_names_its_sibling_and_the_merge_order() {
        // Invented: an infra repo creates the queue, a pipeline repo reads it.
        let d = FixDecision {
            end_to_end: vec![
                step(
                    "Create the queue and its read policy",
                    "this_pr",
                    "",
                    "acme/infra",
                ),
                step(
                    "Read from the queue in the nightly job",
                    "other_repo",
                    "",
                    "acme/pipeline",
                ),
                step(
                    "Merge acme/infra first, then acme/pipeline",
                    "person",
                    "whoever merges",
                    "",
                ),
            ],
            ..Default::default()
        };
        let infra = crate::passes::fix_location::not_in_pr_steps(Some(&d), "acme/infra");
        assert_eq!(infra[0].0, "Read from the queue in the nightly job");
        let pipeline = crate::passes::fix_location::not_in_pr_steps(Some(&d), "acme/pipeline");
        assert_eq!(pipeline[0].0, "Create the queue and its read policy");
        assert_eq!(pipeline[0].1, "a pull request in acme/infra");

        let prs = [
            Sibling {
                repo: "acme/infra".into(),
                number: 12,
            },
            Sibling {
                repo: "acme/pipeline".into(),
                number: 34,
            },
        ];
        let first = merge_order_section(&prs, 0);
        assert!(first.contains("## Merge order\n1. acme/infra#12 (this PR)\n2. acme/pipeline#34\n"));
        let second = merge_order_section(&prs, 1);
        assert!(second.contains("1. acme/infra#12\n2. acme/pipeline#34 (this PR)\n"));
        assert_eq!(merge_order_section(&prs[..1], 0), "");

        // Set into a description that has one already: replaced, in place.
        let body = assemble(
            "Source ticket: OPS-1\n\n**Summary** x\n\n**Changes**\n* y\n\n**Risk** Low\n\n**Verification**\n1. z",
            &Owned { merge_order: merge_order_section(&prs, 0), ..Default::default() },
        );
        let (mut owned, rest) = take_owned(&body);
        owned.set(Section::MergeOrder, merge_order_section(&prs, 1));
        let again = assemble(&rest, &owned);
        assert_eq!(again.matches("## Merge order").count(), 1);
        assert!(again.contains("(this PR)\n<!-- /coderhelm:merge-order -->"));
        assert!(again.contains("acme/pipeline#34 (this PR)"));
    }

    #[test]
    fn the_description_is_put_together_in_its_order() {
        let writer = "Closes #9\n\n**Summary** The report needs a reader.\n\n**Changes**\n* **Users** add `analytics_reader`\n\n**Risk** **Low**: one new user.\n\n**Verification**\n1. Run the plan.";
        let owned = Owned {
            mirrors: mirrors_section(&[Mirror {
                path: "envs/prod/users.tf".into(),
                name: "billing_reader".into(),
                differences: vec![],
            }]),
            merge_order: merge_order_section(
                &[
                    Sibling { repo: "acme/infra".into(), number: 1 },
                    Sibling { repo: "acme/pipeline".into(), number: 2 },
                ],
                0,
            ),
            not_in_pr: not_in_pr_section(&[("Allow the IP".into(), "the network team".into())]),
            open_items: format!(
                "{}\n## Open items\n- **Waiting on CI:** tests did not run. Settles when this PR's CI passes. <!-- coderhelm:ci -->\n{}",
                crate::passes::proof::UNVERIFIED_START,
                crate::passes::proof::UNVERIFIED_END
            ),
        };
        let body = assemble(
            &format!("{writer}\n\n<sub>Consulted team agents: x</sub>"),
            &owned,
        );
        let at = |s: &str| {
            body.find(s)
                .unwrap_or_else(|| panic!("{s} missing:\n{body}"))
        };
        assert!(at("**Summary**") < at("**Changes**"));
        assert!(at("**Changes**") < at("**Copies an existing block**"));
        assert!(at("**Copies an existing block**") < at("## Merge order"));
        assert!(at("## Merge order") < at("## Not in this PR"));
        assert!(at("## Not in this PR") < at("**Risk**"));
        assert!(at("**Risk**") < at("**Verification**"));
        assert!(at("**Verification**") < at("## Open items"));
        assert!(at("## Open items") < at("<sub>Consulted"));
        assert!(!body.contains("[!WARNING]"));
        assert!(body.starts_with("Closes #9\n\n"));
        // Taken apart and put back together, nothing moves.
        let (o, rest) = take_owned(&body);
        assert_eq!(assemble(&rest, &o), body);
        // No Risk heading: before Verification; neither: at the end.
        let no_risk = assemble("**Changes**\n* a\n\n## Verification\n1. b", &owned);
        assert!(no_risk.find("## Not in this PR") < no_risk.find("## Verification"));
        let bare = assemble("Just text.", &owned);
        assert!(bare.starts_with("Just text.\n\n<!-- coderhelm:mirrors -->"));
        assert!(bare.ends_with("<!-- /coderhelm:unverified -->"));
        assert!(is_heading("3. **Risk** — Low", "risk"));
        assert!(!is_heading("## Risks", "risk"));
        assert!(!is_heading("Risk is low", "risk"));
    }

    #[test]
    fn a_top_warning_from_before_moves_to_the_bottom_as_open_items() {
        let old = "<!-- coderhelm:unverified -->\n> [!WARNING]\n> **Not yet verified**\n> - **Impact not measured:** not counted. Settle: count them.\n> - **Waiting on CI:** lint did not run <!-- coderhelm:ci -->\n<!-- /coderhelm:unverified -->\n\nCloses #4\n\n**Summary** x\n\n**Risk** Low";
        let (owned, rest) = take_owned(old);
        let body = assemble(&rest, &owned);
        assert!(body.starts_with("Closes #4"), "{body}");
        assert!(!body.contains("[!WARNING]"));
        assert!(!body.contains("> "));
        assert!(body.ends_with(
            "**Risk** Low\n\n<!-- coderhelm:unverified -->\n## Open items\n- **Impact not measured:** not counted. Settle: count them.\n- **Waiting on CI:** lint did not run <!-- coderhelm:ci -->\n<!-- /coderhelm:unverified -->"
        ));
        assert_eq!(
            normalize_open_items(
                "<!-- coderhelm:unverified -->\n> [!NOTE]\n<!-- /coderhelm:unverified -->"
            ),
            ""
        );
    }

    #[test]
    fn the_environments_a_diff_changes_come_from_its_paths() {
        let paths: Vec<String> = [
            "envs/prod/main.tf",
            "envs/staging/main.tf",
            "environments/us-east-canary/values.yaml",
            "deploy/staging/kustomization.yaml",
            "helm/values-production.yaml",
            "config/.env.dev",
            "src/test/helpers.rs",
            "src/devtools.rs",
            "README.md",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            changed_environments(&paths),
            vec!["prod", "staging", "us-east-canary", "dev"]
        );
        assert!(changed_environments(&["src/main.rs".to_string()]).is_empty());
        assert_eq!(
            changed_environments(&["env/prod.tfvars".to_string()]),
            vec!["prod"]
        );
    }

    #[test]
    fn every_changed_environment_needs_its_own_evidence_line() {
        let envs = vec!["prod".to_string(), "staging".to_string()];
        let notes = "Evidence:\n- Confirmed: the production secret path exists (L1 came back with an ARN)\n- The staging role was edited.";
        let g = env_gaps(&envs, notes);
        assert_eq!(g.len(), 1, "{g:?}");
        assert_eq!(g[0].point, "environment");
        assert!(g[0].detail.contains("staging"));
        assert_eq!(g[0].settler(), crate::passes::proof::Settler::Proof);
        let both =
            format!("{notes}\n- Unconfirmed: staging, no read access to the staging account");
        assert!(env_gaps(&envs, &both).is_empty());
        // One environment: nothing to cross-check.
        assert!(env_gaps(&envs[..1], "").is_empty());
        let custom = vec!["prod".to_string(), "us-east-canary".to_string()];
        assert_eq!(
            env_gaps(
                &custom,
                "- Confirmed: prod ok\n- Confirmed: us-east-canary ok"
            )
            .len(),
            0
        );
    }

    #[test]
    fn holes_a_template_or_markdown_left_are_found() {
        let body = "**Changes**\n* Secret at `/app//db-password`\n* Template `${env}` left\n* Empty {} here\n* Empty `` code\n* Path /reports/<env>/daily strips\n* Code path `/data/<region>/x`\n* Fine: https://example.com/a, `Vec<String>`, <sub>x</sub>, <!-- coderhelm:ci -->, `// comment`\n\n```sh\necho ${HOME}//x {}\n```\n";
        let h = placeholder_holes(body);
        let has = |s: &str| h.iter().any(|x| x.contains(s));
        assert!(has("`/app//db-password`: an empty path segment"), "{h:?}");
        assert!(has("`${env}`: a template variable"), "{h:?}");
        assert!(has("an empty `{}` placeholder"), "{h:?}");
        assert!(has("empty inline code"), "{h:?}");
        assert!(has("`<env>` is a placeholder Markdown strips"), "{h:?}");
        assert!(has("`<region>` is a placeholder"), "{h:?}");
        assert_eq!(h.len(), 6, "{h:?}");
        assert!(placeholder_holes("Closes #4\n\n**Summary** Secret at `/app/prod/db-password`, see https://x.io/y <details><summary>a</summary></details>").is_empty());
        assert!(placeholder_holes("x <>").iter().any(|x| x.contains("`<>`")));
    }
}
