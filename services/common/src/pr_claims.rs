//! A description checked against the facts and the diff, sentence by
//! sentence, before it is saved.
//!
//! Descriptions go wrong in a few known ways: they claim a change the diff
//! does not contain, a check nobody recorded, a count the records disagree
//! with, or a blanket word ("unchanged", "no impact", "fully fixed") with
//! nothing behind it. On a dependency pull request they also contradict the
//! lockfile: a package "not moved" that is, a lockfile "out of sync" that
//! installs frozen, a fixed advisory listed as open.
//!
//! [`enforce`] reads every sentence people or a model wrote (the prose and
//! the open items; never the sections CoderHelm renders from records, and
//! never code) and holds it against [`Facts`]:
//!
//! - a count that disagrees is rewritten with the recorded number, and a
//!   table cell that says "not moved" gets the versions the head resolves;
//! - any other sentence the facts do not bear out is removed, a blanket
//!   word clause by clause so the rest of its sentence stays.
//!
//! It runs on every rebuild of every pull request's description
//! (`worker::passes::description`), costs no model call, and is idempotent:
//! what it returns passes it unchanged.

use std::sync::OnceLock;

use regex::Regex;

use crate::pr_facts::{Facts, Family};

/// What kind of statement the facts did not bear out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A file said to be changed that the diff does not contain.
    ChangeNotInDiff,
    /// A check said to pass that no recorded result shows passing.
    CheckNotRecorded,
    /// A number that is not the recorded one.
    WrongCount,
    /// "unchanged", "no impact", "fully fixed" with no supporting fact.
    Unsupported,
    /// A package said to be not moved, or still open, that the head moved.
    NotMoved,
    /// The lockfile said to be out of sync when a frozen install passes (or
    /// in sync when it fails).
    LockfileSync,
    /// A fixed advisory listed as open, or an open one as fixed.
    AdvisoryState,
}

/// One statement corrected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Correction {
    pub kind: Kind,
    /// The sentence as it was written.
    pub said: String,
    /// The fact it disagrees with.
    pub why: String,
    /// What it was rewritten to; `None` when it was removed.
    pub wrote: Option<String>,
}

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).expect("a valid pattern"))
}

macro_rules! pattern {
    ($name:ident, $p:expr) => {
        fn $name() -> &'static Regex {
            static R: OnceLock<Regex> = OnceLock::new();
            re(&R, $p)
        }
    };
}

pattern!(
    change_verb,
    r"(?i)\b(adds?|added|updates?|updated|changes?|changed|removes?|removed|deletes?|deleted|renames?|renamed|modif(?:y|ies|ied)|creates?|created|rewrites?|rewrote|rewritten|refactors?|refactored|introduces?|introduced|edits?|edited|extracts?|extracted|replaces?|replaced)\b"
);
pattern!(
    change_negated,
    r"(?i)(\b(not|never|no longer|without|unchanged|untouched|instead of|rather than|left|leaves|keeps?|kept)\b|n't\b)"
);
pattern!(
    about_a_reference,
    r"(?i)\b(import|imports|imported|reference|references|link|links|path|paths|call|calls|usage|use of|uses of|pointing|points|mirrors?|copied from|copy of|like|as in)\b[^`]{0,24}$"
);
pattern!(
    pass_claim,
    r"(?i)\b((?:unit |integration |e2e |end-to-end )?tests?|test suite|suite|specs?|lint(?:ing|er)?|type[- ]?checks?|typecheck|type checking|build|compilation|ci|checks?|frozen install|install)\b[^.;:!?\n]{0,50}?\b(pass(?:es|ed|ing)?|(?:is|are|was|were|stays?|went) green|succeed(?:s|ed)?|(?:is|are|runs?|ran) clean)\b"
);
pattern!(
    not_a_claim,
    r"(?i)(\b(not|never|until|once|should|must|will|would|can|could|may|might|to pass|whether|if|when|after|before|so that|needs? to|expected|verify|confirm|ensure|make sure|check that|wait)\b|n't\b)"
);
pattern!(
    imperative,
    r"(?i)^\W*(run|re-?run|confirm|check|verify|ensure|open|visit|see|wait|merge|review|look|make sure|watch)\b"
);
pattern!(
    severity_count,
    r"(?i)\b(\d+)\s+(critical|high|moderate|medium|low)\b"
);
pattern!(
    remaining_count,
    r"(?i)\b(\d+)\s+(?:security\s+)?(?:advisor(?:y|ies)|vulnerabilit(?:y|ies))\s+(?:remains?|(?:is|are)\s+left|left|still\s+open|(?:is|are)\s+not\s+fixed|not\s+fixed)\b"
);
pattern!(
    fixed_count,
    r"(?i)\bfix(?:es|ed)?\s+(\d+)\s+(?:security\s+)?(?:advisor(?:y|ies)|vulnerabilit(?:y|ies))\b"
);
pattern!(
    major_count,
    r"(?i)\b(\d+)\s+major(?:\s+version)?\s+(?:bumps?|jumps?|updates?|upgrades?|versions?)\b"
);
pattern!(
    file_count,
    r"(?i)\b(\d+)\s+files?\s+(?:changed|touched|modified)\b"
);
pattern!(
    about_advisories,
    r"(?i)\b(advisor(?:y|ies)|vulnerabilit(?:y|ies)|cves?|findings?)\b"
);
pattern!(
    remaining_word,
    r"(?i)\b(remains?|remaining|left|still|open|not fixed|unfixed|outstanding)\b"
);
pattern!(fixed_word, r"(?i)\bfix(?:es|ed)?\b");
pattern!(
    blanket,
    r"(?i)\b(unchanged|no (?:functional |behaviou?r(?:al)? |breaking |visible |user[- ]facing |runtime )?(?:changes?|impact|effect|regressions?)|fully (?:fixed|resolved|compatible|backwards?[- ]compatible)|backwards?[- ]compatible|no risk|risk[- ]free|safe to merge|completely (?:fixed|safe)|zero (?:risk|impact))\b"
);
pattern!(
    fully_fixed,
    r"(?i)^(fully (?:fixed|resolved)|completely fixed)$"
);
pattern!(
    not_done,
    r"(?i)\b(not moved(?: on the head)?|did not move|didn't move|not (?:yet )?(?:bumped|updated|upgraded|fixed|patched)|still (?:open|vulnerable|affected|pinned)|still (?:on|at) [0-9v][\w.\-]*|remains? (?:open|vulnerable|unfixed)|unfixed|left open|could not be (?:bumped|updated|upgraded|fixed))\b"
);
pattern!(
    open_heading,
    r"(?i)\b(not fixed|still open|left open|unfixed|not updated|not moved|remaining|not resolved)\b"
);
pattern!(
    lock_out,
    r"(?i)\block ?files?\b[^.;!?]{0,60}?\b(out of sync|not in sync|out of date|stale|does(?: not|n't) match|needs? (?:to be )?(?:regenerat|updat|refresh)\w*|(?:was|is) not regenerated)"
);
pattern!(
    lock_in,
    r"(?i)\block ?files?\b[^.;!?]{0,40}?\b(in sync|up to date|matches)\b"
);
pattern!(
    advisory_id,
    r"(?i)\b(GHSA(?:-[0-9a-z]{4}){3}|CVE-\d{4}-\d{3,})\b"
);
pattern!(
    not_fixed_word,
    r"(?i)\b(not (?:yet )?(?:fixed|patched|resolved|addressed)|remains?|still (?:open|present|affects?|vulnerable)|unfixed|left open|no fix|no patched)\b"
);
pattern!(
    fixed_claim,
    r"(?i)\b(fix(?:es|ed)|resolv(?:es|ed)|patch(?:es|ed)|address(?:es|ed)|clos(?:es|ed))\b"
);

/// File extensions a path somebody names ends with.
const EXTENSIONS: &[&str] = &[
    "rs", "ts", "tsx", "js", "jsx", "mjs", "cjs", "py", "rb", "go", "java", "kt", "swift", "c",
    "h", "cpp", "hpp", "cs", "php", "json", "yaml", "yml", "toml", "lock", "md", "mdx", "html",
    "css", "scss", "sql", "sh", "tf", "hcl", "tfvars", "xml", "gradle", "graphql", "proto", "vue",
    "svelte", "txt", "cfg", "ini", "conf",
];

/// A token that names a file by its path: a directory and an extension.
fn path_like(t: &str) -> bool {
    if t.is_empty()
        || t.len() > 200
        || t.contains("://")
        || t.starts_with(['-', '$', '/', '@', '.'])
        || t.chars()
            .any(|c| c.is_whitespace() || "()=<>:*{}|,;\"'".contains(c))
    {
        return false;
    }
    let Some((_, last)) = t.rsplit_once('/') else {
        return false;
    };
    last.rsplit_once('.')
        .is_some_and(|(stem, ext)| !stem.is_empty() && EXTENSIONS.contains(&ext))
}

/// The text with everything between backticks blanked (same length, so a
/// match's offsets are the original's), and the tokens that were there.
fn unticked(s: &str) -> (String, Vec<(usize, String)>) {
    let mut out = String::with_capacity(s.len());
    let mut tokens = Vec::new();
    let mut inside = false;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        if c == '`' {
            if inside {
                tokens.push((start, s[start..i].to_string()));
            } else {
                start = i + 1;
            }
            inside = !inside;
            out.push('`');
        } else if inside {
            for _ in 0..c.len_utf8() {
                out.push(' ');
            }
        } else {
            out.push(c);
        }
    }
    (out, tokens)
}

/// Where the sentences of a line of prose start and end.
fn sentence_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut start = 0;
    let mut inside = false;
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'`' {
            inside = !inside;
        } else if !inside && matches!(c, b'.' | b'!' | b'?') && i + 1 < bytes.len() {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j] == b' ' {
                j += 1;
            }
            let next = text[j..].chars().next();
            let boundary =
                j > i + 1 && next.is_some_and(|n| n.is_uppercase() || matches!(n, '`' | '*' | '['));
            if boundary {
                spans.push((start, j));
                start = j;
                i = j;
                continue;
            }
        }
        i += 1;
    }
    if start < text.len() {
        spans.push((start, text.len()));
    }
    spans
}

/// The clause of `s` that holds byte `at`: what is between `;`, ` but ` or
/// the ends.
fn clause_at(s: &str, at: usize) -> &str {
    let before = &s[..at];
    let start = [
        before.rfind(';').map(|i| i + 1),
        before.rfind(" but ").map(|i| i + 5),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(0);
    let after = &s[at..];
    let end = [after.find(';'), after.find(" but ")]
        .into_iter()
        .flatten()
        .min()
        .map(|i| at + i)
        .unwrap_or(s.len());
    &s[start..end]
}

/// What a line is, for the check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Region {
    /// Written by a person or a model.
    Prose,
    /// The open items.
    Open,
    /// Rendered from records by code, a marker, a heading or code: not read.
    Skip,
}

/// The sections CoderHelm renders from records, by the start of their
/// heading.
const RECORD_HEADINGS: &[&str] = &[
    "advisory",
    "advisories",
    "affected",
    "reachability",
    "what to check",
    "proof",
    "closes",
    "for the reviewer",
];

struct Line<'a> {
    text: &'a str,
    region: Region,
    /// Under a Changes heading.
    changes: bool,
    /// Under a heading that says what follows is not fixed or still open.
    open_heading: bool,
}

/// The heading a line is, and whether text follows it on the same line
/// (`**Risk** — **Low**, one module.` as a numbered writer puts it).
fn heading_of(line: &str) -> Option<(String, bool)> {
    let t = line.trim();
    if t.starts_with('#') {
        return Some((
            t.trim_start_matches('#')
                .trim()
                .trim_matches('*')
                .trim()
                .to_lowercase(),
            false,
        ));
    }
    let digits = t.chars().take_while(|c| c.is_ascii_digit()).count();
    let t = if digits > 0 && (t[digits..].starts_with(". ") || t[digits..].starts_with(") ")) {
        t[digits + 2..].trim_start()
    } else {
        t
    };
    let inner = t.strip_prefix("**")?;
    let end = inner.find("**")?;
    let name = inner[..end].trim().trim_end_matches(':').to_lowercase();
    let rest = inner[end + 2..].trim();
    if name.is_empty() || name.split_whitespace().count() > 5 {
        return None;
    }
    if rest.is_empty() {
        Some((name, false))
    } else if rest.starts_with(['—', '–', '-', ':']) && digits > 0 {
        Some((name, true))
    } else {
        None
    }
}

fn regions(body: &str) -> Vec<Line<'_>> {
    let lines: Vec<&str> = body.lines().collect();
    let mut out = Vec::with_capacity(lines.len());
    let mut in_code = false;
    // The block of CoderHelm's a line is inside, and its closing line.
    let mut block: Option<(String, String)> = None;
    let mut record_heading = false;
    let mut open_items_heading = false;
    let mut changes = false;
    let mut open_heading = false;
    for (i, raw) in lines.iter().enumerate() {
        let t = raw.trim();
        let mut region = Region::Prose;
        if t.starts_with("```") || t.starts_with("~~~") {
            in_code = !in_code;
            region = Region::Skip;
        } else if in_code {
            region = Region::Skip;
        } else if let Some((name, close)) = &block {
            if t == close {
                block = None;
                region = Region::Skip;
            } else if name == "unverified" {
                region = Region::Open;
            } else {
                region = Region::Skip;
            }
        } else if t.starts_with("<!-- coderhelm:") && t.ends_with("-->") {
            let name = t["<!-- coderhelm:".len()..t.len() - 3].trim();
            let close = format!("<!-- /coderhelm:{name} -->");
            if !name.contains(' ') && lines[i + 1..].iter().any(|l| l.trim() == close) {
                block = Some((name.to_string(), close));
            }
            region = Region::Skip;
        }
        if region != Region::Skip || block.as_ref().is_some_and(|(n, _)| n == "unverified") {
            if let Some((h, text_follows)) = heading_of(raw) {
                if block.is_none() {
                    record_heading = RECORD_HEADINGS.iter().any(|r| h.starts_with(r));
                    open_items_heading = h.starts_with("open items");
                    changes = h.starts_with("changes") || h.starts_with("change");
                }
                open_heading = open_heading_of(&h);
                // Text on a heading's own line is read like any other.
                if !text_follows || record_heading {
                    region = Region::Skip;
                }
            } else if t.is_empty() || t.starts_with('<') {
                region = Region::Skip;
            } else if block.is_none() {
                if record_heading {
                    region = Region::Skip;
                } else if open_items_heading {
                    region = Region::Open;
                }
            }
        }
        out.push(Line {
            text: raw,
            region,
            changes: changes && block.is_none(),
            open_heading,
        });
    }
    out
}

fn open_heading_of(h: &str) -> bool {
    open_heading().is_match(h)
}

/// The list marker, quote mark or checkbox a line starts with.
fn marker_len(line: &str) -> usize {
    let t = line.trim_start();
    let lead = line.len() - t.len();
    let mut n = 0;
    let rest = t;
    if let Some(r) = rest.strip_prefix("> ") {
        n += rest.len() - r.len();
        return lead + n + marker_len(r);
    }
    for m in ["- ", "* ", "+ "] {
        if rest.starts_with(m) {
            n = m.len();
            break;
        }
    }
    if n == 0 {
        let digits = rest.chars().take_while(|c| c.is_ascii_digit()).count();
        if digits > 0 && (rest[digits..].starts_with(". ") || rest[digits..].starts_with(") ")) {
            n = digits + 2;
        }
    }
    let after = &rest[n..];
    for b in ["[ ] ", "[x] ", "[X] "] {
        if after.starts_with(b) {
            n += b.len();
            break;
        }
    }
    lead + n
}

/// Where a sentence sits.
struct Ctx<'a> {
    facts: &'a Facts,
    region: Region,
    changes: bool,
    open_heading: bool,
    /// The sentence starts its line (after the list marker).
    first: bool,
    /// The line is a table row.
    row: bool,
}

/// Where `package` is named in `s`: in backticks (with or without a
/// version), or as a word of its own.
fn mention(s: &str, bare: &str, tokens: &[(usize, String)], package: &str) -> Option<usize> {
    for (at, t) in tokens {
        let name = t
            .rsplit_once('@')
            .filter(|(n, _)| !n.is_empty())
            .map_or(t.as_str(), |(n, _)| n);
        if name.eq_ignore_ascii_case(package) || t.eq_ignore_ascii_case(package) {
            return Some(*at);
        }
    }
    if package.len() < 4 {
        return None;
    }
    let lower = bare.to_lowercase();
    let p = package.to_lowercase();
    let mut from = 0;
    while let Some(i) = lower[from..].find(&p) {
        let at = from + i;
        let edge = |c: Option<char>| {
            c.is_none_or(|c| !(c.is_alphanumeric() || c == '-' || c == '_' || c == '/'))
        };
        if edge(lower[..at].chars().next_back()) && edge(lower[at + p.len()..].chars().next()) {
            let _ = s;
            return Some(at);
        }
        from = at + p.len();
    }
    None
}

pattern!(
    counted_noun,
    r"^(\s+(?:critical|high|moderate|medium|low))?\s+(advisory remains|advisories remain|advisory is|advisories are)\b"
);

/// The sentence with the number at `at` replaced by `n`, and the words it
/// counts made to agree ("1 high advisory remains", "2 high advisories
/// remain").
fn replace_number(s: &str, at: (usize, usize), n: usize) -> String {
    let rest = &s[at.1..];
    let rest = match counted_noun().captures(rest) {
        Some(c) => {
            let m = c.get(2).expect("the noun");
            let agreed = match (m.as_str(), n == 1) {
                ("advisory remains" | "advisories remain", true) => "advisory remains",
                ("advisory remains" | "advisories remain", false) => "advisories remain",
                (_, true) => "advisory is",
                (_, false) => "advisories are",
            };
            format!("{}{agreed}{}", &rest[..m.start()], &rest[m.end()..])
        }
        None => rest.to_string(),
    };
    format!("{}{n}{rest}", &s[..at.0])
}

/// The numbers of a sentence, corrected to the recorded ones.
fn fix_counts(s: &str, cx: &Ctx<'_>, out: &mut Vec<Correction>) -> String {
    let mut s = s.to_string();
    // One correction at a time: offsets move when a number's width does.
    for _ in 0..8 {
        let (bare, _) = unticked(&s);
        let mut fix: Option<((usize, usize), usize, String)> = None;
        let f = cx.facts;
        if f.files_complete {
            if let Some(c) = file_count().captures(&bare) {
                let m = c.get(1).unwrap();
                if m.as_str().parse::<usize>().ok() != Some(f.files.len()) {
                    fix = Some((
                        (m.start(), m.end()),
                        f.files.len(),
                        format!(
                            "the diff of `{}` changes {} files",
                            f.short_head(),
                            f.files.len()
                        ),
                    ));
                }
            }
        }
        if let (None, Some(d)) = (&fix, &f.dependency) {
            let a = &d.advisories;
            let known = !a.fixed.is_empty() || !a.remaining.is_empty();
            if known && about_advisories().is_match(&bare) {
                for c in severity_count().captures_iter(&bare) {
                    let m = c.get(1).unwrap();
                    let clause = clause_at(&bare, m.start());
                    let clause = clause
                        .rsplit_once('(')
                        .filter(|(_, tail)| {
                            !tail.contains(')') || tail.find(')') > tail.find(m.as_str())
                        })
                        .map_or(clause, |(_, tail)| tail);
                    let remaining_claim = remaining_word().is_match(clause)
                        || (!fixed_word().is_match(clause) && remaining_word().is_match(&bare));
                    if !remaining_claim {
                        continue;
                    }
                    let sev = c[2].to_lowercase();
                    let recorded = a
                        .remaining_by_severity
                        .iter()
                        .filter(|(k, _)| {
                            let k = k.to_lowercase();
                            k == sev
                                || (matches!(sev.as_str(), "moderate" | "medium")
                                    && matches!(k.as_str(), "moderate" | "medium"))
                        })
                        .map(|(_, n)| *n)
                        .sum::<usize>();
                    if m.as_str().parse::<usize>().ok() != Some(recorded) {
                        fix = Some((
                            (m.start(), m.end()),
                            recorded,
                            format!(
                                "{recorded} {sev} {} left on `{}`",
                                if recorded == 1 {
                                    "advisory is"
                                } else {
                                    "advisories are"
                                },
                                f.short_head()
                            ),
                        ));
                        break;
                    }
                }
            }
            if fix.is_none() && known {
                if let Some(c) = remaining_count().captures(&bare) {
                    let m = c.get(1).unwrap();
                    if m.as_str().parse::<usize>().ok() != Some(a.remaining.len()) {
                        fix = Some((
                            (m.start(), m.end()),
                            a.remaining.len(),
                            format!("{} left on `{}`", a.remaining.len(), f.short_head()),
                        ));
                    }
                }
            }
            if fix.is_none() && known {
                if let Some(c) = fixed_count().captures(&bare) {
                    let m = c.get(1).unwrap();
                    if m.as_str().parse::<usize>().ok() != Some(a.fixed.len()) {
                        fix = Some((
                            (m.start(), m.end()),
                            a.fixed.len(),
                            format!("{} fixed on `{}`", a.fixed.len(), f.short_head()),
                        ));
                    }
                }
            }
            if fix.is_none() && !d.lines.is_empty() {
                if let Some(c) = major_count().captures(&bare) {
                    let m = c.get(1).unwrap();
                    let majors = d.major_lines().len();
                    if m.as_str().parse::<usize>().ok() != Some(majors) {
                        fix = Some((
                            (m.start(), m.end()),
                            majors,
                            format!("{majors} cross a major version on `{}`", f.short_head()),
                        ));
                    }
                }
            }
        }
        let Some((at, n, why)) = fix else {
            break;
        };
        let next = replace_number(&s, at, n);
        out.push(Correction {
            kind: Kind::WrongCount,
            said: s.trim().to_string(),
            why,
            wrote: Some(next.trim().to_string()),
        });
        s = next;
    }
    s
}

/// Why a sentence is not borne out, when it is not: `(kind, why)`.
fn refuted(s: &str, cx: &Ctx<'_>) -> Option<(Kind, String)> {
    let f = cx.facts;
    let (bare, tokens) = unticked(s);
    if let Some(d) = &f.dependency {
        // A package said to be not moved or still open that the head moved.
        for l in d.lines.iter().filter(|l| l.moved && l.remaining.is_empty()) {
            let Some(at) = mention(s, &bare, &tokens, &l.package) else {
                continue;
            };
            if not_done().is_match(clause_at(&bare, at)) || cx.open_heading {
                return Some((
                    Kind::NotMoved,
                    format!(
                        "the lockfile on `{}` resolves `{}` at {} (from {}) and none of its advisories is left",
                        f.short_head(),
                        l.package,
                        l.to,
                        l.from
                    ),
                ));
            }
        }
        match &d.frozen_install {
            Some((true, by)) if lock_out().is_match(&bare) => {
                return Some((
                    Kind::LockfileSync,
                    format!("a frozen install passes on `{}`: {by}", f.short_head()),
                ));
            }
            Some((false, by)) if lock_in().is_match(&bare) && !not_a_claim().is_match(&bare) => {
                return Some((
                    Kind::LockfileSync,
                    format!("a frozen install fails on `{}`: {by}", f.short_head()),
                ));
            }
            _ => {}
        }
        for m in advisory_id().find_iter(&bare) {
            let id = m.as_str();
            let is = |list: &[String]| list.iter().any(|x| x.eq_ignore_ascii_case(id));
            let clause = clause_at(&bare, m.start());
            if is(&d.advisories.fixed) && (not_fixed_word().is_match(clause) || cx.open_heading) {
                return Some((
                    Kind::AdvisoryState,
                    format!("{id} is fixed on `{}`", f.short_head()),
                ));
            }
            if is(&d.advisories.remaining)
                && fixed_claim().is_match(clause)
                && !not_fixed_word().is_match(clause)
                && !not_a_claim().is_match(clause)
                && !cx.open_heading
            {
                return Some((
                    Kind::AdvisoryState,
                    format!("{id} is not fixed on `{}`", f.short_head()),
                ));
            }
        }
    }
    if cx.region != Region::Prose || cx.row {
        return None;
    }
    // A file said to be changed that the diff does not contain.
    if f.files_complete && !f.files.is_empty() {
        let lead_token =
            cx.changes && cx.first && s.trim_start().trim_start_matches('*').starts_with('`');
        let claims = change_verb().is_match(&bare) && !change_negated().is_match(&bare);
        for (i, (at, t)) in tokens.iter().enumerate() {
            if !path_like(t) || f.has_file(t) {
                continue;
            }
            let is_lead = lead_token && i == 0;
            let before = &bare[..at.saturating_sub(1)];
            if is_lead || (claims && !about_a_reference().is_match(before)) {
                return Some((
                    Kind::ChangeNotInDiff,
                    format!("`{t}` is not in the diff of `{}`", f.short_head()),
                ));
            }
        }
    }
    // A check said to pass that no recorded result shows passing.
    if f.checks_read && !imperative().is_match(&bare) && !not_a_claim().is_match(&bare) {
        if let Some(c) = pass_claim().captures(&bare) {
            let subject = c[1].to_lowercase();
            let all = f.all_checks_passed();
            let backed = if matches!(subject.as_str(), "ci" | "check" | "checks") {
                all
            } else {
                let fam = match subject.as_str() {
                    "suite" | "test suite" | "spec" | "specs" => vec![Family::Tests],
                    "compilation" => vec![Family::Build],
                    "frozen install" | "install" => vec![Family::Install],
                    other => Family::of(other),
                };
                let passed = f.passed_families();
                all || fam.iter().any(|x| passed.contains(x))
            };
            if !backed {
                return Some((
                    Kind::CheckNotRecorded,
                    format!(
                        "no recorded check result on `{}` shows {} passing",
                        f.short_head(),
                        c[1].trim()
                    ),
                ));
            }
        }
    }
    None
}

/// Whether a blanket phrase in `s` has a fact behind it.
fn blanket_backed(
    phrase: &str,
    s: &str,
    bare: &str,
    tokens: &[(usize, String)],
    f: &Facts,
) -> bool {
    if fully_fixed().is_match(phrase) {
        return f.dependency.as_ref().is_some_and(|d| {
            !d.advisories.fixed.is_empty()
                && d.advisories.remaining.is_empty()
                && d.advisories.introduced.is_empty()
        });
    }
    // "`src/a.rs` is unchanged": the diff says so.
    let named: Vec<&str> = tokens
        .iter()
        .map(|(_, t)| t.as_str())
        .filter(|t| path_like(t))
        .collect();
    if f.files_complete && !named.is_empty() && named.iter().all(|t| !f.has_file(t)) {
        return true;
    }
    // Something recorded as the same before and after, named here.
    let lower = bare.to_lowercase();
    let names = |what: &str| {
        what.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| w.len() >= 4)
            .any(|w| lower.contains(w))
    };
    if f.unchanged.iter().any(|u| names(&u.what)) {
        return true;
    }
    if let Some(d) = &f.dependency {
        if !d.probes.is_empty()
            && d.probes.iter().all(|p| p.same())
            && d.probes.iter().any(|p| names(&p.what))
        {
            return true;
        }
    }
    let _ = s;
    false
}

/// `s` without the clauses that hold an unsupported blanket phrase. `None`
/// when nothing worth keeping is left.
fn without_blankets(s: &str, f: &Facts) -> Option<(String, Vec<String>)> {
    let (bare, tokens) = unticked(s);
    let unsupported: Vec<(usize, usize, String)> = blanket()
        .find_iter(&bare)
        .filter(|m| !blanket_backed(m.as_str(), s, &bare, &tokens, f))
        .map(|m| (m.start(), m.end(), m.as_str().to_string()))
        .collect();
    if unsupported.is_empty() {
        return Some((s.to_string(), vec![]));
    }
    // Clause boundaries: `, ` `; ` and the dashes people write, outside
    // backticks.
    let mut cuts = vec![0];
    for d in [", ", "; ", " — ", " – ", " - "] {
        let mut from = 0;
        while let Some(i) = bare[from..].find(d) {
            cuts.push(from + i);
            from += i + d.len();
        }
    }
    cuts.sort_unstable();
    cuts.dedup();
    cuts.push(s.len());
    let mut kept = String::new();
    for w in cuts.windows(2) {
        let (a, b) = (w[0], w[1]);
        if unsupported.iter().any(|(x, _, _)| *x >= a && *x < b) {
            continue;
        }
        let piece = &s[a..b];
        if kept.is_empty() {
            kept.push_str(piece.trim_start_matches([',', ';', ' ', '—', '–', '-']));
        } else {
            kept.push_str(piece);
        }
    }
    let phrases = unsupported.into_iter().map(|(_, _, p)| p).collect();
    let kept = kept
        .trim()
        .trim_end_matches([',', ';', ':', '—', '–', '-'])
        .trim_end()
        .to_string();
    let words = kept
        .split_whitespace()
        .filter(|w| w.chars().any(char::is_alphanumeric))
        .count();
    // A bold word that leads the sentence (a risk level) stays whatever
    // else goes.
    let bold_lead = s.trim_start().starts_with("**");
    if words >= 3 || (bold_lead && words >= 1 && kept.contains("**")) {
        let ended = if kept.ends_with(['.', '!', '?']) || !s.trim_end().ends_with('.') {
            kept
        } else {
            format!("{kept}.")
        };
        Some((ended, phrases))
    } else if phrases.is_empty() {
        Some((s.to_string(), phrases))
    } else {
        None.or(Some((String::new(), phrases)))
    }
}

/// One sentence, corrected: what stays of it (empty when it goes).
fn sentence(s: &str, cx: &Ctx<'_>, out: &mut Vec<Correction>) -> String {
    if s.trim().is_empty() {
        return s.to_string();
    }
    let trail = &s[s.trim_end().len()..];
    let s = fix_counts(s, cx, out);
    if let Some((kind, why)) = refuted(&s, cx) {
        out.push(Correction {
            kind,
            said: s.trim().to_string(),
            why,
            wrote: None,
        });
        return String::new();
    }
    if cx.row {
        return s;
    }
    match without_blankets(&s, cx.facts) {
        Some((kept, phrases)) if phrases.is_empty() => kept,
        Some((kept, phrases)) => {
            out.push(Correction {
                kind: Kind::Unsupported,
                said: s.trim().to_string(),
                why: format!(
                    "nothing recorded on `{}` supports \"{}\"",
                    cx.facts.short_head(),
                    phrases.join("\", \"")
                ),
                wrote: (!kept.is_empty()).then(|| kept.clone()),
            });
            if kept.is_empty() {
                kept
            } else {
                format!("{kept}{trail}")
            }
        }
        None => s,
    }
}

/// A table row: a "not moved" cell on a line the head moved gets the
/// versions the head resolves.
fn row(line: &str, cx: &Ctx<'_>, out: &mut Vec<Correction>) -> String {
    let Some(d) = &cx.facts.dependency else {
        return line.to_string();
    };
    let (bare, tokens) = unticked(line);
    for l in d.lines.iter().filter(|l| l.moved && l.remaining.is_empty()) {
        if mention(line, &bare, &tokens, &l.package).is_none() {
            continue;
        }
        if let Some(m) = not_done().find(&bare) {
            // The whole cell: "2.1.0, not moved on the head" becomes the
            // versions the head resolves.
            let start = bare[..m.start()].rfind('|').map_or(m.start(), |i| i + 1);
            let end = bare[m.end()..].find('|').map_or(m.end(), |i| m.end() + i);
            let wrote = format!(
                "{} {} to {}{} {}",
                &line[..start],
                l.from,
                l.to,
                if l.major { " (major)" } else { "" },
                &line[end..]
            );
            out.push(Correction {
                kind: Kind::NotMoved,
                said: line.trim().to_string(),
                why: format!(
                    "the lockfile on `{}` resolves `{}` at {} (from {})",
                    cx.facts.short_head(),
                    l.package,
                    l.to,
                    l.from
                ),
                wrote: Some(wrote.trim().to_string()),
            });
            return wrote;
        }
    }
    // Any other statement in a row is held to the same facts.
    sentence(line, cx, out)
}

fn run(body: &str, facts: &Facts) -> (String, Vec<Correction>) {
    let mut corrections = Vec::new();
    let mut kept: Vec<String> = Vec::new();
    for l in regions(body) {
        if l.region == Region::Skip {
            kept.push(l.text.to_string());
            continue;
        }
        let is_row = l.text.trim_start().starts_with('|');
        if is_row {
            let t = l.text.trim();
            let separator = t.chars().all(|c| matches!(c, '|' | '-' | ':' | ' '));
            if separator {
                kept.push(l.text.to_string());
                continue;
            }
            let cx = Ctx {
                facts,
                region: l.region,
                changes: l.changes,
                open_heading: l.open_heading,
                first: true,
                row: true,
            };
            let next = row(l.text, &cx, &mut corrections);
            if !next.trim().is_empty() {
                kept.push(next);
            }
            continue;
        }
        let m = marker_len(l.text);
        let (marker, text) = l.text.split_at(m);
        let mut line = String::new();
        for (i, (a, b)) in sentence_spans(text).into_iter().enumerate() {
            let cx = Ctx {
                facts,
                region: l.region,
                changes: l.changes,
                open_heading: l.open_heading,
                first: i == 0,
                row: false,
            };
            line.push_str(&sentence(&text[a..b], &cx, &mut corrections));
        }
        if line.trim().is_empty() {
            continue;
        }
        kept.push(format!("{marker}{}", line.trim_end()));
    }
    // A removed line never leaves a run of blank lines behind.
    let mut out: Vec<String> = Vec::with_capacity(kept.len());
    for l in kept {
        if l.trim().is_empty() && out.last().is_some_and(|p: &String| p.trim().is_empty()) {
            continue;
        }
        out.push(l);
    }
    let mut text = out.join("\n");
    if body.ends_with('\n') && !text.ends_with('\n') {
        text.push('\n');
    }
    (text, corrections)
}

/// What [`enforce`] would correct, without changing anything.
pub fn check(body: &str, facts: &Facts) -> Vec<Correction> {
    run(body, facts).1
}

/// `body` with every statement the facts do not bear out rewritten from
/// them or removed, and what was corrected. A body the facts bear out comes
/// back as it is.
pub fn enforce(body: &str, facts: &Facts) -> (String, Vec<Correction>) {
    let (text, corrections) = run(body, facts);
    if corrections.is_empty() {
        return (body.to_string(), corrections);
    }
    (text, corrections)
}

/// The text the check reads, for a stamp: when it is the same, and the head
/// is the same, the check has nothing new to say.
pub fn checked_text(body: &str) -> String {
    regions(body)
        .into_iter()
        .filter(|l| l.region != Region::Skip)
        .map(|l| l.text.trim())
        .collect::<Vec<_>>()
        .join("\n")
}

/// A short digest of [`checked_text`], for the description's stamp.
pub fn checked_digest(body: &str) -> String {
    use sha2::{Digest, Sha256};
    let text = checked_text(&crate::pr_layout::unfold(body));
    Sha256::digest(text.as_bytes())
        .iter()
        .take(6)
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pr_facts::{
        Advisories, CheckFact, DepLine, DependencyFacts, Evidenced, FileChange, Outcome, Probe,
    };

    fn file(path: &str) -> FileChange {
        FileChange {
            path: path.into(),
            status: "modified".into(),
            previous: String::new(),
            changed: 0,
        }
    }

    fn passed(name: &str) -> CheckFact {
        CheckFact {
            name: name.into(),
            outcome: Outcome::Passed,
            url: String::new(),
        }
    }

    fn facts() -> Facts {
        Facts {
            head: "abc1234def".into(),
            files: vec![file("src/export/poller.rs"), file("src/export/mod.rs")],
            files_complete: true,
            checks: vec![passed("unit tests")],
            checks_read: true,
            ..Default::default()
        }
    }

    fn dep_facts() -> Facts {
        let mut f = facts();
        f.files = vec![file("package.json"), file("package-lock.json")];
        f.dependency = Some(DependencyFacts {
            lines: vec![
                DepLine {
                    package: "glob-walk".into(),
                    from: "2.1.0".into(),
                    to: "2.1.4".into(),
                    moved: true,
                    major: false,
                    remaining: vec![],
                    fixed: vec![],
                },
                DepLine {
                    package: "tiny-yaml".into(),
                    from: "1.2.0".into(),
                    to: "1.2.0".into(),
                    moved: false,
                    major: false,
                    remaining: vec!["GHSA-aaaa-bbbb-cccc".into()],
                    fixed: vec![],
                },
            ],
            advisories: Advisories {
                fixed: vec!["GHSA-1111-2222-3333".into(), "CVE-2026-10001".into()],
                remaining: vec!["GHSA-aaaa-bbbb-cccc".into(), "GHSA-dddd-eeee-ffff".into()],
                introduced: vec![],
                remaining_by_severity: [("high".to_string(), 2usize)].into_iter().collect(),
            },
            frozen_install: Some((true, "`npm ci` passed in the sandbox".into())),
            ..Default::default()
        });
        f
    }

    #[test]
    fn a_change_the_diff_does_not_contain_is_removed() {
        let body = "## Summary\n\nRetries the poll three times.\n\n## Changes\n\n* `src/export/poller.rs`: the retry loop.\n* `src/export/writer.rs`: flushes after each batch.\n* Also updates `docs/guides/export.md` with the new flag.\n";
        let (out, c) = enforce(body, &facts());
        assert!(
            out.contains("`src/export/poller.rs`: the retry loop."),
            "{out}"
        );
        assert!(!out.contains("writer.rs"), "{out}");
        assert!(!out.contains("export.md"), "{out}");
        assert_eq!(c.len(), 2);
        assert!(c
            .iter()
            .all(|x| x.kind == Kind::ChangeNotInDiff && x.wrote.is_none()));
        assert!(c[0]
            .why
            .contains("`src/export/writer.rs` is not in the diff of `abc1234`"));
        // What it returns passes it unchanged.
        assert_eq!(enforce(&out, &facts()), (out.clone(), vec![]));
    }

    #[test]
    fn a_file_named_without_a_claim_of_change_is_left() {
        let body = "## Summary\n\nThe poller now reads its limit like `src/config/limits.rs` does. `src/export/api.rs` is left as it is.\n\n## Verification\n\n1. Run `cargo test` and confirm the tests pass.\n";
        let (out, c) = enforce(body, &facts());
        assert_eq!(out, body);
        assert!(c.is_empty(), "{c:?}");
    }

    #[test]
    fn a_check_nobody_recorded_is_not_claimed() {
        let mut f = facts();
        f.checks = vec![
            passed("lint"),
            CheckFact {
                name: "unit tests".into(),
                outcome: Outcome::Failed,
                url: String::new(),
            },
        ];
        let body = "## Verification\n\nAll tests pass and lint is clean. The build was not run.\n";
        let (out, c) = enforce(body, &f);
        assert_eq!(c.len(), 1, "{c:?}");
        assert_eq!(c[0].kind, Kind::CheckNotRecorded);
        assert!(!out.contains("tests pass"), "{out}");
        assert!(out.contains("The build was not run."), "{out}");
        // With the suite recorded as passed, the same sentence stands.
        let (out, c) = enforce(body, &facts());
        assert!(c.is_empty(), "{c:?}");
        assert_eq!(out, body);
        // Nothing is judged when the head's checks could not be read.
        f.checks_read = false;
        assert!(check(body, &f).is_empty());
    }

    #[test]
    fn a_blanket_word_needs_a_fact() {
        let body = "## Risk\n\n**Low**, contract unchanged, one module.\n\nNo impact on other callers. `src/export/api.rs` is unchanged.\n";
        let (out, c) = enforce(body, &facts());
        assert!(out.contains("**Low**, one module."), "{out}");
        assert!(!out.contains("contract unchanged"), "{out}");
        assert!(!out.contains("No impact"), "{out}");
        // The diff itself backs this one.
        assert!(out.contains("`src/export/api.rs` is unchanged."), "{out}");
        assert_eq!(c.iter().filter(|x| x.kind == Kind::Unsupported).count(), 2);
        // A recorded before and after backs the sentence about it.
        let mut f = facts();
        f.unchanged = vec![Evidenced {
            what: "the export contract".into(),
            evidence: "the response schema test passed before and after".into(),
        }];
        let (out, _) = enforce(body, &f);
        assert!(out.contains("contract unchanged"), "{out}");
        assert_eq!(enforce(&out, &f).0, out);
    }

    #[test]
    fn fully_fixed_is_said_only_when_nothing_is_left() {
        let body = "## Change\n\nThe advisories are fully fixed.\n";
        let (out, c) = enforce(body, &dep_facts());
        assert!(!out.contains("fully fixed"), "{out}");
        assert_eq!(c[0].kind, Kind::Unsupported);
        let mut f = dep_facts();
        f.dependency.as_mut().unwrap().advisories.remaining.clear();
        assert!(check(body, &f).is_empty());
    }

    #[test]
    fn a_dependency_description_never_contradicts_the_lockfile() {
        let body = "## Summary\n\nFixes 2 advisories. 1 high advisory remains.\n\n| Package | Version |\n| --- | --- |\n| `glob-walk` | not moved on the head |\n| `tiny-yaml` | not moved on the head |\n\n## Change\n\nGHSA-1111-2222-3333 is not fixed yet. GHSA-aaaa-bbbb-cccc is fixed by the override.\n\n<!-- coderhelm:unverified -->\n## Open items\n- The lockfile is out of sync with `package.json`; regenerate it.\n- `glob-walk` is still open.\n- `tiny-yaml` has no patched release.\n<!-- /coderhelm:unverified -->\n";
        let (out, c) = enforce(body, &dep_facts());
        let kinds: Vec<Kind> = c.iter().map(|x| x.kind).collect();
        assert!(out.contains("2 high advisories remain."), "{out}");
        assert!(out.contains("| `glob-walk` | 2.1.0 to 2.1.4 |"), "{out}");
        // A line that really did not move keeps saying so.
        assert!(
            out.contains("| `tiny-yaml` | not moved on the head |"),
            "{out}"
        );
        assert!(!out.contains("GHSA-1111-2222-3333 is not fixed"), "{out}");
        assert!(!out.contains("GHSA-aaaa-bbbb-cccc is fixed"), "{out}");
        assert!(!out.contains("out of sync"), "{out}");
        assert!(!out.contains("`glob-walk` is still open"), "{out}");
        assert!(
            out.contains("- `tiny-yaml` has no patched release."),
            "{out}"
        );
        assert_eq!(
            kinds,
            vec![
                Kind::WrongCount,
                Kind::NotMoved,
                Kind::AdvisoryState,
                Kind::AdvisoryState,
                Kind::LockfileSync,
                Kind::NotMoved,
            ]
        );
        assert!(c[4].why.contains("`npm ci` passed in the sandbox"));
        assert_eq!(enforce(&out, &dep_facts()), (out.clone(), vec![]));
    }

    #[test]
    fn items_under_a_not_fixed_heading_are_held_to_the_head() {
        let body = "## Change\n\n### Not fixed\n\n- `glob-walk`\n- GHSA-1111-2222-3333\n- GHSA-dddd-eeee-ffff\n";
        let (out, c) = enforce(body, &dep_facts());
        assert_eq!(c.len(), 2, "{c:?}");
        assert!(!out.contains("glob-walk"), "{out}");
        assert!(out.contains("- GHSA-dddd-eeee-ffff"), "{out}");
    }

    #[test]
    fn counts_follow_the_records() {
        let mut f = dep_facts();
        f.dependency.as_mut().unwrap().lines[0].major = true;
        let body = "Fixes 5 advisories with 3 major version bumps; 4 files changed.\n";
        let (out, c) = enforce(body, &f);
        assert_eq!(
            out,
            "Fixes 2 advisories with 1 major version bumps; 2 files changed.\n"
        );
        assert_eq!(c.len(), 3);
        assert!(c
            .iter()
            .all(|x| x.kind == Kind::WrongCount && x.wrote.is_some()));
    }

    #[test]
    fn records_code_and_the_lead_are_never_read() {
        let body = "<!-- coderhelm:lead -->\nNo impact, unchanged.\n<!-- /coderhelm:lead -->\n\n## Proof\n\nAll tests pass, no impact.\n\n```\nunchanged; tests pass\n```\n\n<!-- coderhelm:settled -->\n**Settled**\n- lint passes, unchanged\n<!-- /coderhelm:settled -->\n\n<!-- coderhelm:security -->\n";
        let mut f = facts();
        f.checks.clear();
        let (out, c) = enforce(body, &f);
        assert_eq!(out, body);
        assert!(c.is_empty(), "{c:?}");
        assert_eq!(checked_text(body), "");
    }

    #[test]
    fn a_probe_backs_a_behaviour_claim_only_when_it_is_the_same() {
        let mut f = dep_facts();
        let body = "## Change\n\nThe parse output is unchanged.\n";
        assert_eq!(check(body, &f).len(), 1);
        f.dependency.as_mut().unwrap().probes = vec![Probe {
            what: "parse output of the sample config".into(),
            before: "sha256:1f".into(),
            after: "sha256:1f".into(),
        }];
        assert!(check(body, &f).is_empty());
        f.dependency.as_mut().unwrap().probes[0].after = "sha256:2a".into();
        assert_eq!(check(body, &f).len(), 1);
    }

    #[test]
    fn the_checked_text_changes_only_with_what_is_read() {
        let a = "## Summary\n\nRetries the poll.\n\n<!-- coderhelm:settled -->\n- one\n<!-- /coderhelm:settled -->\n";
        let b = "## Summary\n\nRetries the poll.\n\n<!-- coderhelm:settled -->\n- two\n<!-- /coderhelm:settled -->\n";
        let c = "## Summary\n\nRetries the poll twice.\n";
        assert_eq!(checked_text(a), checked_text(b));
        assert_ne!(checked_text(a), checked_text(c));
    }
}
