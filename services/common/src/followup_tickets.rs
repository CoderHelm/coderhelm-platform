//! Follow-up tickets a run files from the ticket it worked on.
//!
//! An audit ticket's deliverable is often "the list, then tickets for the
//! fixes". The run writes the drafts (the plan's `followup_tickets`, or the
//! numbered list under a "Follow-up tickets" heading in proposal.md for a
//! plan written before that field), and CoderHelm files them itself, with the
//! team's own connection:
//!
//! - in the source ticket's Jira project, through the Coderhelm app's
//!   create-ticket trigger, each linked to the source ("relates to"),
//!   typed by the project and labelled `coderhelm-followup` and
//!   `coderhelm:followup:<source>` ([`labels`]), never with the team's
//!   trigger label, so a follow-up never starts a run by itself;
//! - for a team with no Jira (a GitHub issue), as issues in the repository
//!   when the GitHub App may write issues;
//! - with neither, the drafts go into the ticket comment and the run
//!   completes. Filing never waits on a person.
//!
//! Each draft is filed once per source ticket ([`draft_id`]): the worker
//! keeps what it filed, and the app finds a ticket it already filed for the
//! same draft, so a retried run never files twice. A draft worded
//! differently is still one topic: before filing, the follow-ups already
//! filed for the source (CoderHelm's records and the tracker's label search,
//! [`Existing`]) are read, and a draft whose topic one of them covers
//! ([`topic`]: the normalized title and the key files and components it
//! names, [`marks`]) is not filed again ([`Filed::Covered`]). One comment on
//! the source ticket lists what was filed ([`comment`]). A refusal is said
//! as exactly what was refused ([`refusal`]): a missing project permission
//! by its name, never "connect Jira".
//!
//! Follow-ups never cascade: a run on a follow-up files none of its own
//! ([`depth_refusal`]), one source files at most [`DAILY_CAP_PER_SOURCE`] a
//! day, and the gateway starts no run on a follow-up from its label or
//! assignee ([`held_comment`]); a person starts it from CoderHelm.

use serde::{Deserialize, Serialize};

/// The tool's name, in the team's agent permissions and in the plan.
pub const TOOL: &str = "file_followup_tickets";

/// The longest summary filed (Jira takes 255 characters).
pub const MAX_SUMMARY: usize = 120;
/// The most drafts one run files.
pub const MAX_DRAFTS: usize = 20;

/// One follow-up ticket as the run drafted it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Draft {
    pub summary: String,
    #[serde(default)]
    pub description: String,
}

/// The id of a draft for one source ticket: stable across runs, so a retry
/// finds what an earlier run filed. Summaries are compared as the app
/// compares them (case, spacing and a trailing dot aside).
pub fn draft_id(source: &str, d: &Draft) -> String {
    let s = normal_summary(&d.summary);
    crate::content_hash(&format!("{}|{s}", source.trim().to_ascii_uppercase()))
        .chars()
        .take(24)
        .collect()
}

/// A summary as two drafts are compared.
pub fn normal_summary(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
        .trim_end_matches(['.', ' '])
        .to_string()
}

/// Words that say nothing about a topic.
const STOP_WORDS: &[&str] = &[
    "a", "an", "the", "and", "or", "to", "of", "on", "in", "for", "with", "when", "is", "are",
    "be", "it", "its", "as", "at", "by", "from", "that", "this", "these", "those", "into", "not",
    "no", "any", "all", "only", "via", "per", "so", "then", "than", "instead", "where", "which",
];

/// Whether a token reads as a code identifier in mixed case
/// (`imageWithAlt`, `ContactCard`): a lowercase letter followed by an
/// uppercase one.
fn mixed_case(t: &str) -> bool {
    let c: Vec<char> = t.chars().collect();
    c.windows(2)
        .any(|w| w[0].is_ascii_lowercase() && w[1].is_ascii_uppercase())
}

/// A file name's stem (`Hero` of `apps/web/Hero.tsx:85`), when `t` names a
/// file: a dot followed by a short extension of letters.
fn file_stem(t: &str) -> Option<String> {
    let t = t.split(':').next().unwrap_or(t);
    let base = t.rsplit('/').next().unwrap_or(t);
    let (stem, ext) = base.rsplit_once('.')?;
    let ext_ok = (1..=5).contains(&ext.len()) && ext.chars().all(|c| c.is_ascii_alphabetic());
    (ext_ok && !stem.is_empty() && stem.chars().any(|c| c.is_ascii_alphabetic()))
        .then(|| stem.to_ascii_lowercase())
}

/// The key files and components a text names: file names (their stems),
/// code identifiers in mixed case, and names in backticks that read as
/// code (`slot-promo`, `image_with_alt`), all lowercased. Two drafts about the same component share these whatever
/// their wording.
pub fn marks(text: &str) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    let ticked: Vec<&str> = text.split('`').skip(1).step_by(2).collect();
    for t in ticked {
        let t = t.trim();
        if t.is_empty() || t.contains(' ') {
            continue;
        }
        if let Some(s) = file_stem(t) {
            out.insert(s);
        } else if t.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            && t.chars().any(|c| c.is_ascii_alphabetic())
            && t.len() > 2
            // A plain word in backticks (`alt`) is an attribute, not a part.
            && (mixed_case(t) || t.contains('_') || t.contains('-'))
        {
            out.insert(t.to_ascii_lowercase());
        }
    }
    for raw in text.split(|c: char| c.is_whitespace() || ",;()[]{}\"'`".contains(c)) {
        let t = raw.trim_matches(|c: char| !c.is_ascii_alphanumeric());
        if t.is_empty() {
            continue;
        }
        if let Some(s) = file_stem(raw.trim_matches(|c: char| ",.;:!?".contains(c))) {
            out.insert(s);
        } else if mixed_case(t) && t.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            out.insert(t.to_ascii_lowercase());
        }
    }
    out
}

/// A title's words as two topics compare them: lowercased, split at
/// anything not a letter or digit, without stop words, numbers or the
/// [`marks`] (compared on their own), plurals made singular.
pub fn topic_words(text: &str) -> std::collections::BTreeSet<String> {
    let skip = marks(text);
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(|w| w.to_ascii_lowercase())
        .filter(|w| !skip.contains(w))
        .filter(|w| !STOP_WORDS.contains(&w.as_str()))
        .filter(|w| !w.chars().all(|c| c.is_ascii_digit()))
        .map(|w| {
            if w.len() > 3 && w.ends_with('s') && !w.ends_with("ss") {
                w[..w.len() - 1].to_string()
            } else {
                w
            }
        })
        .collect()
}

/// A follow-up already filed for the source ticket: CoderHelm's own record
/// of it, or what the tracker's label search returned.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Existing {
    pub key: String,
    pub url: String,
    pub summary: String,
    /// The key files and components its description names ([`marks`]).
    pub marks: Vec<String>,
}

/// The follow-ups filed for a source, one per key: CoderHelm's records
/// first, a tracker's answer filling in what a record lacks (an older
/// record kept no summary).
pub fn merge_existing(records: Vec<Existing>, tracker: Vec<Existing>) -> Vec<Existing> {
    let mut out: Vec<Existing> = Vec::new();
    for e in records.into_iter().chain(tracker) {
        if e.key.trim().is_empty() {
            continue;
        }
        match out.iter_mut().find(|o| o.key == e.key) {
            Some(o) => {
                if o.summary.trim().is_empty() {
                    o.summary = e.summary;
                }
                if o.url.trim().is_empty() {
                    o.url = e.url;
                }
                for m in e.marks {
                    if !o.marks.contains(&m) {
                        o.marks.push(m);
                    }
                }
            }
            None => out.push(e),
        }
    }
    out
}

/// The follow-ups a [`comment`] CoderHelm posted lists (`KEY: summary
/// (url) [note]` lines), for a record kept before records held summaries.
pub fn titles_from_comment(text: &str) -> Vec<Existing> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        let Some((key, rest)) = line.split_once(": ") else {
            continue;
        };
        if project_of(key).is_none() {
            continue;
        }
        let mut summary = rest.to_string();
        let mut url = String::new();
        if let Some(i) = summary.find(" [") {
            summary.truncate(i);
        }
        if let Some(i) = summary.rfind(" (http") {
            url = summary[i + 2..].trim_end_matches(')').to_string();
            summary.truncate(i);
        }
        if summary.starts_with("covered by ") {
            continue;
        }
        out.push(Existing {
            key: key.to_string(),
            url,
            summary: summary.trim().to_string(),
            marks: Vec::new(),
        });
    }
    out
}

/// How a draft's topic stands against one filed follow-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topic {
    /// The same work: the draft is not filed again.
    Same,
    /// Close enough that a small model decides ([`same_topic_prompt`]).
    Maybe,
    /// Different work.
    New,
}

/// The two numbers [`topic`] reads: how much of the titles' words the two
/// share (of all their words), and how much of the smaller set of key
/// files and components they share (0 when either names none).
pub fn overlap(draft: &Draft, e: &Existing) -> (f32, f32) {
    let a = topic_words(&draft.summary);
    let b = topic_words(&e.summary);
    let union = a.union(&b).count();
    let words = if union == 0 {
        0.0
    } else {
        a.intersection(&b).count() as f32 / union as f32
    };
    let ma = marks(&format!("{}\n{}", draft.summary, draft.description));
    let mut mb: std::collections::BTreeSet<String> = marks(&e.summary);
    mb.extend(e.marks.iter().map(|m| m.to_ascii_lowercase()));
    let small = ma.len().min(mb.len());
    let files = if small == 0 {
        0.0
    } else {
        ma.intersection(&mb).count() as f32 / small as f32
    };
    (words, files)
}

/// Whether `e` covers `draft`'s topic. The same topic: most of the title's
/// words, or the same key files and components with some of the words. An
/// exact summary is always the same.
pub fn topic(draft: &Draft, e: &Existing) -> Topic {
    if normal_summary(&draft.summary) == normal_summary(&e.summary) {
        return Topic::Same;
    }
    let (words, files) = overlap(draft, e);
    if words >= 0.5 || (files >= 0.5 && words >= 0.15) {
        Topic::Same
    } else if words >= 0.25 || files > 0.0 {
        Topic::Maybe
    } else {
        Topic::New
    }
}

/// The most filed follow-ups the small check weighs for one draft.
pub const MAX_CANDIDATES: usize = 3;

/// Where a draft stands against the follow-ups filed for its source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Near<'a> {
    /// One of them is the same topic.
    Same(&'a Existing),
    /// These may be (closest first, at most [`MAX_CANDIDATES`]): the small
    /// check decides.
    Maybe(Vec<&'a Existing>),
    /// None is near: the draft is new work.
    New,
}

/// A draft against the follow-ups filed for its source ([`topic`]).
pub fn near<'a>(draft: &Draft, existing: &'a [Existing]) -> Near<'a> {
    let mut maybe: Vec<(&Existing, f32)> = Vec::new();
    for e in existing {
        match topic(draft, e) {
            Topic::Same => return Near::Same(e),
            Topic::Maybe => {
                let (w, f) = overlap(draft, e);
                maybe.push((e, w + f));
            }
            Topic::New => {}
        }
    }
    if maybe.is_empty() {
        return Near::New;
    }
    maybe.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    Near::Maybe(
        maybe
            .into_iter()
            .take(MAX_CANDIDATES)
            .map(|(e, _)| e)
            .collect(),
    )
}

/// The files and components a draft names that the follow-up covering it
/// doesn't: the scope it grew by, said on that ticket instead of a new one.
pub fn added_scope(draft: &Draft, e: &Existing) -> Vec<String> {
    let mut known = marks(&e.summary);
    known.extend(e.marks.iter().map(|m| m.to_ascii_lowercase()));
    marks(&format!("{}\n{}", draft.summary, draft.description))
        .into_iter()
        .filter(|m| !known.contains(m))
        .collect()
}

/// The system prompt of the small check on drafts [`topic`] can't settle.
pub const SAME_TOPIC_SYSTEM: &str = "You compare new follow-up ticket drafts with the tickets \
     already filed from the same source ticket. For each draft, name the filed ticket that \
     already covers its work (the same fix, even worded differently or a little narrower or \
     wider), or \"none\" when the draft is other work. Answer only with a JSON object.";

/// The question for the small check: each draft numbered with the filed
/// tickets it may be, the answer `{"1": "<KEY>" | "none", ...}`.
pub fn same_topic_prompt(asks: &[(&Draft, Vec<&Existing>)]) -> String {
    let mut s = String::from(
        "For each draft, which filed ticket (if any) already covers the same work?\n\n",
    );
    for (i, (d, filed)) in asks.iter().enumerate() {
        let body = crate::truncate_str(
            &d.description
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" "),
            500,
        )
        .to_string();
        s.push_str(&format!(
            "{}. Draft: {}\n   Draft details: {}\n",
            i + 1,
            d.summary.trim(),
            if body.is_empty() {
                "none"
            } else {
                body.as_str()
            },
        ));
        for e in filed {
            s.push_str(&format!("   Filed {}: {}\n", e.key, e.summary.trim()));
        }
        s.push('\n');
    }
    s.push_str("Answer as {\"1\": \"ABC-12\", \"2\": \"none\", ...}.");
    s
}

/// The small check's answer: for each draft, the key of the filed ticket
/// that covers it (`None`: new work). A draft it didn't answer, or answered
/// with a key it wasn't shown, is covered by its closest candidate: a draft
/// is never filed beside a ticket that may already cover it; the comment
/// lists it with that ticket, for a person to file if it is other work.
pub fn read_same_topic(
    answer: Option<&str>,
    asks: &[(&Draft, Vec<&Existing>)],
) -> Vec<Option<String>> {
    let v: serde_json::Value = answer
        .and_then(|t| {
            let start = t.find('{')?;
            let end = t.rfind('}')?;
            serde_json::from_str(t.get(start..=end)?).ok()
        })
        .unwrap_or_default();
    asks.iter()
        .enumerate()
        .map(|(i, (_, filed))| {
            let first = filed.first().map(|e| e.key.clone());
            match v.get((i + 1).to_string()).and_then(|a| a.as_str()) {
                Some(a) if a.trim().eq_ignore_ascii_case("none") => None,
                Some(a) => filed
                    .iter()
                    .find(|e| e.key.eq_ignore_ascii_case(a.trim()))
                    .map(|e| e.key.clone())
                    .or(first),
                None => first,
            }
        })
        .collect()
}

/// The comment on a filed follow-up whose topic a later draft covers with
/// more scope: what the draft adds, and from where.
pub fn scope_comment(draft: &Draft, added: &[String], source: &str) -> String {
    let body = draft
        .description
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "More scope for this follow-up of {source}, from a later reading of that ticket's \
         findings (not filed as a separate ticket): {}{}\n\nAlso names: {}",
        draft.summary.trim(),
        if body.is_empty() || normal_summary(&body) == normal_summary(&draft.summary) {
            String::new()
        } else {
            format!(". {body}")
        },
        added.join(", ")
    )
}

/// The Jira project of a ticket key (`ABC` of `ABC-12`).
pub fn project_of(key: &str) -> Option<&str> {
    let (p, n) = key.trim().split_once('-')?;
    let ok = !p.is_empty()
        && p.chars().next().is_some_and(|c| c.is_ascii_uppercase())
        && p.chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        && !n.is_empty()
        && n.chars().all(|c| c.is_ascii_digit());
    ok.then_some(p)
}

/// The label that marks a ticket as a follow-up CoderHelm filed. It is never
/// a trigger: a ticket carrying it starts no run by itself.
pub const MARKER_LABEL: &str = "coderhelm-followup";

/// The start of the label naming a follow-up's source
/// (`coderhelm:followup:ABC-12`).
pub const SOURCE_LABEL_PREFIX: &str = "coderhelm:followup:";

/// The most follow-ups CoderHelm files from one source ticket in one day
/// (UTC), across every run on it. A run past it files nothing more and says
/// so in its comment.
pub const DAILY_CAP_PER_SOURCE: u32 = 10;

/// The label every follow-up of `source` carries.
pub fn source_label(source: &str) -> String {
    format!("{SOURCE_LABEL_PREFIX}{}", source.trim())
}

/// Whether `label` is one of CoderHelm's follow-up labels (the marker or a
/// source label), case and spacing aside.
pub fn is_followup_label(label: &str) -> bool {
    let l = label.trim().to_ascii_lowercase();
    l == MARKER_LABEL || l.starts_with(SOURCE_LABEL_PREFIX)
}

/// Whether `label` is the team's trigger label: the label itself (case
/// ignored), or the label with a repository pinned after it
/// (`coderhelm:acme/web`). A follow-up label is never a trigger, whatever
/// the team's trigger is.
pub fn is_trigger_label(label: &str, trigger_label: &str) -> bool {
    let trigger = trigger_label.trim().to_ascii_lowercase();
    let label = label.trim().to_ascii_lowercase();
    !trigger.is_empty()
        && !is_followup_label(&label)
        && (label == trigger || label.starts_with(&format!("{trigger}:")))
}

/// The labels a follow-up carries: the marker and the source's label, and
/// never one the team's trigger (`trigger_label`) matches, so filing a
/// follow-up never starts a run on it. No repository label either: on the
/// default trigger, `coderhelm:<repo>` is itself a trigger.
pub fn labels(source: &str, trigger_label: &str) -> Vec<String> {
    [MARKER_LABEL.to_string(), source_label(source)]
        .into_iter()
        .filter(|l| !is_trigger_label(l, trigger_label))
        .filter(|l| !l.trim().eq_ignore_ascii_case(trigger_label.trim()))
        .collect()
}

/// The source of a follow-up, from its labels (`coderhelm:followup:ABC-12`
/// gives `ABC-12`); `Some("")` when only the marker is there; `None` when
/// the ticket carries neither.
pub fn source_of_labels<S: AsRef<str>>(labels: &[S]) -> Option<String> {
    let mut marked = false;
    for l in labels {
        let l = l.as_ref().trim();
        if l.len() > SOURCE_LABEL_PREFIX.len()
            && l[..SOURCE_LABEL_PREFIX.len()].eq_ignore_ascii_case(SOURCE_LABEL_PREFIX)
        {
            return Some(l[SOURCE_LABEL_PREFIX.len()..].to_ascii_uppercase());
        }
        if l.eq_ignore_ascii_case(MARKER_LABEL) {
            marked = true;
        }
    }
    marked.then(String::new)
}

/// The sentence every filed follow-up's description ends with
/// ([`description`]), so a run on the ticket knows it is one even when its
/// labels were not sent (an older Jira app, a retry from the run record).
const FILED_BY: &str = ", filed by CoderHelm from that ticket's findings.";

/// Whether a ticket's text is a follow-up CoderHelm filed ([`description`]).
pub fn is_followup_body(body: &str) -> bool {
    body.contains("A follow-up of ") && body.contains(FILED_BY)
}

/// The source a follow-up's text names (`A follow-up of ABC-12 (...)`):
/// `None` when the text is no follow-up's, `Some("")` when it names no key.
pub fn source_of_body(body: &str) -> Option<String> {
    if !is_followup_body(body) {
        return None;
    }
    let after = body.rsplit("A follow-up of ").next().unwrap_or("");
    let word: String = after
        .chars()
        .take_while(|c| !c.is_whitespace() && *c != ',' && *c != '(')
        .collect();
    Some(if project_of(&word).is_some() || word.starts_with('#') {
        word
    } else {
        String::new()
    })
}

/// The settings record naming a filed follow-up by its own key, so the
/// gateway and the worker tell a follow-up from any other ticket.
pub fn of_record_sk(key: &str) -> String {
    format!("FOLLOWUP_OF#{}", key.trim().to_ascii_uppercase())
}

/// The settings record counting the follow-ups filed from `source` on
/// `day` (`YYYY-MM-DD`, UTC).
pub fn day_record_sk(source: &str, day: &str) -> String {
    format!("FOLLOWUP_DAY#{}#{day}", source.trim().to_ascii_uppercase())
}

/// The settings record counting the duplicate follow-ups closed for
/// `source` on `day` (at most [`DAILY_CAP_PER_SOURCE`]).
pub fn close_day_record_sk(source: &str, day: &str) -> String {
    format!(
        "FOLLOWUP_CLOSE_DAY#{}#{day}",
        source.trim().to_ascii_uppercase()
    )
}

/// Why a run on a follow-up files none of its own (depth one).
pub fn depth_refusal(source: &str) -> String {
    if source.trim().is_empty() {
        "this ticket is itself a follow-up CoderHelm filed, and a follow-up never files follow-ups of its own".into()
    } else {
        format!(
            "this ticket is itself a follow-up CoderHelm filed from {}, and a follow-up never files follow-ups of its own",
            source.trim()
        )
    }
}

/// Why a draft was not filed: the source's daily limit is reached.
pub fn daily_cap_refusal(source: &str) -> String {
    format!(
        "CoderHelm files at most {DAILY_CAP_PER_SOURCE} follow-ups a day from {}, and that many are filed today",
        source.trim()
    )
}

/// The one comment on a follow-up ticket that got the trigger label or was
/// assigned to CoderHelm: no run starts on its own, and how a person starts
/// one.
pub fn held_comment(source: &str) -> String {
    let from = if source.trim().is_empty() {
        String::new()
    } else {
        format!(" from {}", source.trim())
    };
    format!(
        "This ticket is a follow-up CoderHelm filed{from}, so CoderHelm doesn't start a run on it from its label or assignee. \
         To have CoderHelm work it, start it in CoderHelm: Start a run with this ticket's key (the dashboard, or start_run in a connected app)."
    )
}

fn strip_marks(s: &str) -> String {
    s.replace("**", "").replace('`', "")
}

/// The first sentence of an item, plain, at most [`MAX_SUMMARY`] characters
/// (cut at a word).
pub fn summary_of(item: &str) -> String {
    let text = item.split_whitespace().collect::<Vec<_>>().join(" ");
    // A bold lead ("**Title**: the rest") is the title.
    if let Some(rest) = text.strip_prefix("**") {
        if let Some((title, _)) = rest.split_once("**") {
            let t = title.trim().trim_end_matches(':').trim();
            if !t.is_empty() {
                return clip(&strip_marks(t));
            }
        }
    }
    let plain = strip_marks(&text);
    let first = match plain.find(". ") {
        Some(i) => &plain[..i],
        None => plain.trim_end_matches('.'),
    };
    clip(first.trim())
}

fn clip(s: &str) -> String {
    if s.chars().count() <= MAX_SUMMARY {
        return s.to_string();
    }
    let cut: String = s.chars().take(MAX_SUMMARY).collect();
    match cut.rfind(' ') {
        Some(i) if i > MAX_SUMMARY / 2 => format!("{}...", &cut[..i]),
        _ => format!("{cut}..."),
    }
}

/// Whether a heading names the follow-up tickets to file.
fn is_followup_heading(line: &str) -> bool {
    let l = line.trim_start_matches('#').trim().to_lowercase();
    let l = l
        .replace("follow-up", "followup")
        .replace("follow up", "followup");
    (l.contains("followup") && (l.contains("ticket") || l.contains("issue")))
        || l.contains("tickets to file")
        || l.contains("issues to file")
}

/// A list item's text, when `line` starts one (`1. `, `1) `, `- `, `* `).
fn item_start(line: &str) -> Option<&str> {
    let t = line.trim_start();
    if let Some(rest) = t.strip_prefix("- ").or_else(|| t.strip_prefix("* ")) {
        return Some(rest);
    }
    let digits = t.chars().take_while(|c| c.is_ascii_digit()).count();
    if digits == 0 {
        return None;
    }
    let rest = &t[digits..];
    rest.strip_prefix(". ").or_else(|| rest.strip_prefix(") "))
}

/// The drafts under proposal.md's follow-up heading: each list item (with
/// its indented continuation lines) is one draft. Empty without the heading.
pub fn from_proposal(proposal: &str) -> Vec<Draft> {
    let mut out: Vec<String> = Vec::new();
    let mut inside = false;
    for line in proposal.lines() {
        if line.trim_start().starts_with('#') {
            if inside && !out.is_empty() {
                break;
            }
            inside = is_followup_heading(line);
            continue;
        }
        if !inside {
            continue;
        }
        if let Some(text) = item_start(line).filter(|_| {
            // A nested item belongs to the item above it.
            line.len() - line.trim_start().len() < 2 || out.is_empty()
        }) {
            out.push(text.trim().to_string());
        } else if let Some(last) = out.last_mut() {
            if !line.trim().is_empty() {
                last.push('\n');
                last.push_str(line.trim());
            }
        }
    }
    out.into_iter()
        .filter(|t| !t.trim().is_empty())
        .take(MAX_DRAFTS)
        .map(|t| Draft {
            summary: summary_of(&t),
            description: t,
        })
        .filter(|d| !d.summary.is_empty())
        .collect()
}

/// The drafts a run files: the plan's own list, else proposal.md's.
pub fn drafts(planned: &[Draft], proposal: &str) -> Vec<Draft> {
    let mut v: Vec<Draft> = planned
        .iter()
        .filter(|d| !d.summary.trim().is_empty())
        .take(MAX_DRAFTS)
        .map(|d| Draft {
            summary: clip(&strip_marks(d.summary.trim())),
            description: d.description.trim().to_string(),
        })
        .collect();
    if v.is_empty() {
        v = from_proposal(proposal);
    }
    // One of each: two items with the same summary are one ticket.
    let mut seen = std::collections::HashSet::new();
    v.retain(|d| seen.insert(normal_summary(&d.summary)));
    v
}

/// The description a filed ticket carries: the draft, and where it came from.
pub fn description(d: &Draft, source: &str, source_title: &str, run_url: &str) -> String {
    let mut s = d.description.trim().to_string();
    if s.is_empty() {
        s = d.summary.trim().to_string();
    }
    let from = if source_title.trim().is_empty() {
        source.to_string()
    } else {
        format!("{source} ({})", source_title.trim())
    };
    s.push_str(&format!("\n\nA follow-up of {from}{FILED_BY}"));
    if !run_url.trim().is_empty() {
        s.push_str(&format!(" The run: {}", run_url.trim()));
    }
    s
}

/// Where the follow-ups go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tracker {
    /// The source ticket's Jira project, through the Coderhelm app.
    Jira { project: String, site: String },
    /// Issues in the repository, through the GitHub App.
    Github { repo: String },
    /// Nowhere CoderHelm may file: the drafts go in the comment, and why.
    Comment(String),
}

/// Where a run files: Jira when the source is a Jira ticket and the app's
/// create-ticket trigger is registered; else GitHub issues when the App may
/// write issues (`issues_write`: `None` when unknown, tried then); else the
/// comment, with why.
pub fn tracker(
    source: &str,
    jira_create: bool,
    site: &str,
    repo: &str,
    issues_write: Option<bool>,
) -> Tracker {
    if jira_create {
        if let Some(p) = project_of(source) {
            return Tracker::Jira {
                project: p.to_string(),
                site: site.trim_end_matches('/').to_string(),
            };
        }
    }
    if issues_write != Some(false) && !repo.trim().is_empty() {
        return Tracker::Github {
            repo: repo.trim().to_string(),
        };
    }
    let why = if repo.trim().is_empty() {
        "this team has no Jira project and no repository CoderHelm files issues in".to_string()
    } else {
        format!(
            "this team has no Jira project to file in, and CoderHelm's GitHub App can't open \
             issues in {repo} (the installation doesn't grant Issues: write)"
        )
    };
    Tracker::Comment(why)
}

/// Whether the link to the source was made.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Linked {
    Yes,
    No(String),
}

/// What became of one draft.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Filed {
    /// Filed now, or found filed by an earlier run (`existing`).
    Ticket {
        key: String,
        url: String,
        existing: bool,
        linked: Linked,
    },
    /// Not filed: a follow-up already filed for the source covers the
    /// draft's topic (`title` is that ticket's summary).
    Covered {
        key: String,
        url: String,
        title: String,
    },
    /// Not filed, and exactly why.
    NotFiled(String),
}

impl Filed {
    pub fn key(&self) -> Option<&str> {
        match self {
            Filed::Ticket { key, .. } | Filed::Covered { key, .. } => Some(key),
            Filed::NotFiled(_) => None,
        }
    }

    /// Whether this run created the ticket (not found, not covered).
    pub fn is_new(&self) -> bool {
        matches!(
            self,
            Filed::Ticket {
                existing: false,
                ..
            }
        )
    }
}

/// The words for an app's refusal of a follow-up: the status and its JSON
/// answer (`error`, `missing`, `words`, `details`). A missing project
/// permission is named with where a Jira admin grants it; nothing here ever
/// says Jira isn't connected.
pub fn refusal(status: u16, body: &serde_json::Value, site: &str, project: &str) -> String {
    let on = if site.trim().is_empty() {
        "the Jira site".to_string()
    } else {
        site.trim_end_matches('/').to_string()
    };
    let error = body["error"].as_str().unwrap_or("");
    let details = body["details"]
        .as_str()
        .map(|d| d.split_whitespace().collect::<Vec<_>>().join(" "))
        .unwrap_or_default();
    let details = crate::truncate_str(&details, 300);
    match error {
        "missing_permission" => {
            let words = body["words"]
                .as_str()
                .filter(|w| !w.is_empty())
                .unwrap_or("Create Issues");
            format!(
                "the Coderhelm app on {on} lacks the Jira project permission {words} in \
                 {project}. A Jira admin grants it in {project}'s project settings, Permissions, \
                 to the atlassian-addons-project-access role (the role Jira gives its apps)"
            )
        }
        "unauthorized" => format!(
            "the Coderhelm app on {on} didn't accept CoderHelm's shared secret; the app \
             registers again by itself on its next event"
        ),
        "no_issue_type" => format!(
            "{project} has no issue type a ticket can be filed with (only sub-tasks or epics)"
        ),
        "permissions_unreadable" | "issue_types_unreadable" | "create_failed" => {
            let what = match error {
                "permissions_unreadable" => "read its permissions in",
                "issue_types_unreadable" => "read the issue types of",
                _ => "create the ticket in",
            };
            if details.is_empty() {
                format!("the Coderhelm app on {on} couldn't {what} {project} (HTTP {status})")
            } else {
                format!(
                    "the Coderhelm app on {on} couldn't {what} {project} (HTTP {status}): {details}"
                )
            }
        }
        "" if details.is_empty() => format!("the Jira app answered HTTP {status}"),
        e => format!(
            "the Jira app answered HTTP {status}: {}",
            if details.is_empty() { e } else { details }
        ),
    }
}

/// The one comment on the source ticket: the tickets filed (or found filed),
/// what wasn't filed and why, and the drafts themselves when none could be
/// filed. Paragraphs, one per ticket, for Jira's plain comment layout.
pub fn comment(drafts: &[Draft], filed: &[Filed], tracker: &Tracker) -> String {
    let made: Vec<(&Draft, &Filed)> = drafts
        .iter()
        .zip(filed.iter())
        .filter(|(_, f)| matches!(f, Filed::Ticket { .. }))
        .collect();
    let covered: Vec<(&Draft, &Filed)> = drafts
        .iter()
        .zip(filed.iter())
        .filter(|(_, f)| matches!(f, Filed::Covered { .. }))
        .collect();
    let mut parts: Vec<String> = Vec::new();
    if !made.is_empty() {
        let n = made.len();
        parts.push(format!(
            "Filed {} follow-up {} from this ticket's findings:",
            n,
            if n == 1 { "ticket" } else { "tickets" }
        ));
        for (d, f) in &made {
            if let Filed::Ticket {
                key,
                url,
                existing,
                linked,
            } = f
            {
                let mut line = format!("{key}: {}", d.summary.trim());
                if !url.is_empty() {
                    line.push_str(&format!(" ({url})"));
                }
                if *existing {
                    line.push_str(" [filed earlier]");
                }
                if let Linked::No(why) = linked {
                    line.push_str(&format!(" [not linked to this ticket: {why}]"));
                }
                parts.push(line);
            }
        }
    }
    if !covered.is_empty() {
        parts.push(
            "Not filed again, because a follow-up already filed from this ticket covers the same \
             work:"
                .to_string(),
        );
        for (d, f) in &covered {
            if let Filed::Covered { key, url, title } = f {
                let mut line = format!("{}: covered by {key}", d.summary.trim());
                if !title.trim().is_empty() {
                    line.push_str(&format!(" ({})", title.trim()));
                }
                if !url.is_empty() {
                    line.push_str(&format!(" {url}"));
                }
                parts.push(line);
            }
        }
    }
    let missed: Vec<(&Draft, &str)> = drafts
        .iter()
        .zip(filed.iter())
        .filter_map(|(d, f)| match f {
            Filed::NotFiled(why) => Some((d, why.as_str())),
            Filed::Ticket { .. } | Filed::Covered { .. } => None,
        })
        .collect();
    let unfiled_drafts: Vec<&Draft> = match tracker {
        Tracker::Comment(_) => drafts.iter().collect(),
        _ => missed.iter().map(|(d, _)| *d).collect(),
    };
    if let Tracker::Comment(why) = tracker {
        parts.push(format!(
            "The follow-up tickets, as drafted. They weren't filed because {why}:"
        ));
    } else if !missed.is_empty() {
        let mut whys: Vec<&str> = missed.iter().map(|(_, w)| *w).collect();
        whys.dedup();
        parts.push(format!(
            "Not filed, because {}. The drafts:",
            whys.join("; ")
        ));
    }
    for (i, d) in unfiled_drafts.iter().enumerate() {
        let body = d
            .description
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if body.is_empty() || normal_summary(&body) == normal_summary(&d.summary) {
            parts.push(format!("{}. {}", i + 1, d.summary.trim()));
        } else {
            parts.push(format!("{}. {}: {body}", i + 1, d.summary.trim()));
        }
    }
    parts.join("\n\n")
}

/// Whether a step of the plan is the filing of follow-up tickets: it names
/// the tool, or its words say to file (create, open, raise) the follow-up
/// tickets or issues.
pub fn is_filing_step(tool: &str, text: &str) -> bool {
    if tool.trim() == TOOL {
        return true;
    }
    let t = text
        .to_lowercase()
        .replace("follow-up", "followup")
        .replace("follow up", "followup");
    let acts = ["file", "create", "open", "raise", "log "]
        .iter()
        .any(|v| t.contains(v));
    acts && t.contains("followup") && (t.contains("ticket") || t.contains("issue"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROPOSAL: &str = "# Audit\n\n## Answer\n1. not a draft\n\n## Follow-up tickets to file (additive only)\n1. Record in the ticket that `Hero.tsx` and `ContactCard.tsx` are not rendered. There is no code change.\n2. Add alt-required validation to `imageWithAlt` (when an asset is set). Then run typegen.\n3. **Coach images**: require alt, and fill the 3 missing ones.\n   - with setIfMissing\n\n## Scope\nAn audit only.\n";

    #[test]
    fn the_drafts_are_the_list_under_the_follow_up_heading() {
        let d = from_proposal(PROPOSAL);
        assert_eq!(d.len(), 3, "{d:?}");
        assert_eq!(
            d[0].summary,
            "Record in the ticket that Hero.tsx and ContactCard.tsx are not rendered"
        );
        assert_eq!(
            d[1].summary,
            "Add alt-required validation to imageWithAlt (when an asset is set)"
        );
        assert_eq!(d[2].summary, "Coach images");
        assert!(d[2].description.contains("with setIfMissing"));
        assert!(from_proposal("# Plan\n1. a step\n").is_empty());
    }

    #[test]
    fn the_plan_s_own_drafts_win_and_repeats_are_one() {
        let planned = vec![
            Draft {
                summary: "Fix the alt".into(),
                description: "x".into(),
            },
            Draft {
                summary: "fix the  alt.".into(),
                description: "y".into(),
            },
        ];
        let d = drafts(&planned, PROPOSAL);
        assert_eq!(d.len(), 1);
        assert_eq!(drafts(&[], PROPOSAL).len(), 3);
    }

    #[test]
    fn a_draft_keeps_its_id_across_runs() {
        let a = Draft {
            summary: "Fix the alt.".into(),
            ..Default::default()
        };
        let b = Draft {
            summary: "fix  the alt".into(),
            description: "other words".into(),
        };
        assert_eq!(draft_id("ABC-1", &a), draft_id("abc-1", &b));
        assert_ne!(draft_id("ABC-1", &a), draft_id("ABC-2", &a));
    }

    #[test]
    fn the_tracker_is_the_source_project_then_issues_then_the_comment() {
        assert_eq!(
            tracker(
                "ABC-12",
                true,
                "https://x.atlassian.net/",
                "o/r",
                Some(true)
            ),
            Tracker::Jira {
                project: "ABC".into(),
                site: "https://x.atlassian.net".into()
            }
        );
        assert_eq!(
            tracker("#12", false, "", "o/r", None),
            Tracker::Github { repo: "o/r".into() }
        );
        let Tracker::Comment(why) = tracker("#12", false, "", "o/r", Some(false)) else {
            panic!("no way to file");
        };
        assert!(why.contains("Issues: write"), "{why}");
        assert!(!why.to_lowercase().contains("connect"), "{why}");
    }

    #[test]
    fn a_missing_permission_is_named_never_connect_jira() {
        let body = serde_json::json!({"error": "missing_permission", "missing": ["CREATE_ISSUES"], "words": "Create Issues"});
        let w = refusal(403, &body, "https://x.atlassian.net", "ABC");
        assert!(
            w.contains("lacks the Jira project permission Create Issues in ABC"),
            "{w}"
        );
        assert!(!w.to_lowercase().contains("connect jira"), "{w}");
        let w = refusal(
            400,
            &serde_json::json!({"error": "create_failed", "details": "Field 'x' is required"}),
            "",
            "ABC",
        );
        assert!(w.contains("Field 'x' is required"), "{w}");
    }

    #[test]
    fn the_comment_lists_the_new_keys_and_the_drafts_not_filed() {
        let d = from_proposal(PROPOSAL);
        let filed = vec![
            Filed::Ticket {
                key: "ABC-20".into(),
                url: "https://x.atlassian.net/browse/ABC-20".into(),
                existing: false,
                linked: Linked::Yes,
            },
            Filed::Ticket {
                key: "ABC-21".into(),
                url: String::new(),
                existing: true,
                linked: Linked::Yes,
            },
            Filed::NotFiled("the Jira app answered HTTP 500".into()),
        ];
        let t = Tracker::Jira {
            project: "ABC".into(),
            site: String::new(),
        };
        let c = comment(&d, &filed, &t);
        assert!(c.starts_with("Filed 2 follow-up tickets"), "{c}");
        assert!(c.contains("ABC-20: Record in the ticket"), "{c}");
        assert!(c.contains("ABC-21: Add alt-required validation to imageWithAlt (when an asset is set) [filed earlier]"), "{c}");
        assert!(
            c.contains("Not filed, because the Jira app answered HTTP 500"),
            "{c}"
        );
        assert!(c.contains("1. Coach images"), "{c}");
        let none = comment(&d, &[], &Tracker::Comment("no tracker".into()));
        assert!(
            none.contains("They weren't filed because no tracker"),
            "{none}"
        );
        assert!(none.contains("3. Coach images"), "{none}");
    }

    #[test]
    fn filing_steps_are_known_by_tool_or_words() {
        assert!(is_filing_step(TOOL, ""));
        assert!(is_filing_step("", "File the 8 follow-up tickets"));
        assert!(is_filing_step(
            "",
            "Follow-up tickets filed? create them in Jira"
        ));
        assert!(!is_filing_step("", "Post the audit table on the ticket"));
        assert!(!is_filing_step("", "Follow-up: open the settings page"));
    }

    fn filed(key: &str, summary: &str) -> Existing {
        Existing {
            key: key.into(),
            url: String::new(),
            summary: summary.into(),
            marks: Vec::new(),
        }
    }

    fn draft(summary: &str) -> Draft {
        Draft {
            summary: summary.into(),
            description: String::new(),
        }
    }

    #[test]
    fn marks_are_files_components_and_ticked_names() {
        let m = marks("Fix `apps/web/Hero.tsx:85` and query.ts:5; extend imageWithAlt and ContactCard, not `alt`.");
        for k in ["hero", "query", "imagewithalt", "contactcard"] {
            assert!(m.contains(k), "{k}: {m:?}");
        }
        assert!(!m.contains("alt"), "{m:?}");
        assert!(!m.contains("fix"));
        let w = topic_words("Replace placeholder and whitespace alt text with data-derived alt");
        assert!(w.contains("placeholder") && w.contains("derived") && !w.contains("and"));
        assert!(topic_words("Coach images").contains("image"));
    }

    #[test]
    fn a_reworded_draft_is_the_topic_already_filed() {
        let earlier = vec![
            filed("ABC-92", "Stop using image filenames as alt text in imageFragmentFields"),
            filed("ABC-93", "Require alt text on imageWithAlt and add alt to mediaAsset images"),
            filed("ABC-94", "Replace generic placeholder alt text with data-derived or empty alt"),
            filed("ABC-95", "Coach images: fall back to the coach name for alt; empty when the name is adjacent"),
            filed("ABC-96", "Fix alt on the unused Hero, ContactCard, FeatureSection and CoachesCarousel components"),
            filed("ABC-97", "Confirm hardcoded alt=\"\" images are decorative"),
        ];
        let same = |s: &str| match near(&draft(s), &earlier) {
            Near::Same(e) => Some(e.key.clone()),
            _ => None,
        };
        assert_eq!(
            same("Remove unused Hero, FeatureSection and ContactCard prototype components"),
            Some("ABC-96".into())
        );
        assert_eq!(
            same("Replace placeholder and whitespace alt text with data-derived or decorative alt"),
            Some("ABC-94".into())
        );
        assert_eq!(
            same("Studio: warn when image alt is missing on imageWithAlt and coach image"),
            Some("ABC-93".into())
        );
        // The same projection in other words: near, for the small check,
        // with the ticket it is among the candidates.
        let Near::Maybe(c) = near(
            &draft("Image alt falls back to the asset filename in GROQ projections"),
            &earlier,
        ) else {
            panic!("near");
        };
        assert!(
            c.len() <= MAX_CANDIDATES && c.iter().any(|e| e.key == "ABC-92"),
            "{c:?}"
        );
        // Other work stays new.
        assert_eq!(
            near(&draft("Content-image alt: apply the same missing-alt warning to block alt fields and title fallbacks on the web"), &earlier),
            Near::New
        );
        assert_eq!(
            near(&draft("Add a sitemap entry for the slots page"), &earlier),
            Near::New
        );
        // An exact summary is always the same, case and dots aside.
        assert_eq!(
            topic(
                &draft("confirm hardcoded alt=\"\" images are decorative."),
                &earlier[5]
            ),
            Topic::Same
        );
    }

    #[test]
    fn grown_scope_is_what_the_draft_names_beyond_the_ticket() {
        let e = Existing {
            marks: vec!["imagewithalt".into()],
            ..filed("ABC-93", "Require alt text on imageWithAlt")
        };
        let d = Draft {
            summary: "Warn when alt is missing on imageWithAlt".into(),
            description: "Also the coach image in `coach.ts`.".into(),
        };
        assert_eq!(added_scope(&d, &e), vec!["coach".to_string()]);
        let c = scope_comment(&d, &added_scope(&d, &e), "ABC-89");
        assert!(
            c.contains("follow-up of ABC-89") && c.contains("Also names: coach"),
            "{c}"
        );
    }

    #[test]
    fn the_small_check_never_files_beside_a_ticket_it_did_not_clear() {
        let d = draft("Fall back to the coach name");
        let a = filed("ABC-95", "Coach images: fall back to the name");
        let b = filed("ABC-96", "Something near");
        let asks = vec![
            (&d, vec![&a, &b]),
            (&d, vec![&b, &a]),
            (&d, vec![&a]),
            (&d, vec![&b]),
        ];
        assert_eq!(
            read_same_topic(
                Some("ok {\"1\": \"none\", \"2\": \"ABC-95\", \"3\": \"ABC-7\"}"),
                &asks
            ),
            vec![
                None,
                Some("ABC-95".into()),
                Some("ABC-95".into()),
                Some("ABC-96".into())
            ],
            "unanswered or unknown: the closest candidate covers it"
        );
        assert_eq!(
            read_same_topic(None, &asks[..1]),
            vec![Some("ABC-95".into())]
        );
        let p = same_topic_prompt(&asks[..1]);
        assert!(p.contains("1. Draft: Fall back to the coach name") && p.contains("Filed ABC-95"));
    }

    #[test]
    fn the_posted_comment_names_what_was_filed() {
        let c = "Filed 2 follow-up tickets from this ticket's findings:\n\nABC-98: Remove unused prototypes (https://x.atlassian.net/browse/ABC-98)\n\nABC-99: Studio: warn when alt is missing [filed earlier]\n\nNot filed again, because ...:\n\nOther draft: covered by ABC-96";
        let t = titles_from_comment(c);
        assert_eq!(t.len(), 2, "{t:?}");
        assert_eq!(
            (t[0].key.as_str(), t[0].summary.as_str(), t[0].url.as_str()),
            (
                "ABC-98",
                "Remove unused prototypes",
                "https://x.atlassian.net/browse/ABC-98"
            )
        );
        assert_eq!(t[1].summary, "Studio: warn when alt is missing");
    }

    #[test]
    fn records_and_the_tracker_merge_by_key() {
        let rec = Existing {
            url: "u".into(),
            ..filed("ABC-1", "")
        };
        let found = vec![filed("ABC-1", "From Jira"), filed("ABC-2", "Other")];
        let m = merge_existing(vec![rec], found);
        assert_eq!(m.len(), 2);
        assert_eq!(
            (m[0].summary.as_str(), m[0].url.as_str()),
            ("From Jira", "u")
        );
    }

    #[test]
    fn a_covered_draft_is_listed_with_its_ticket_not_filed() {
        let d = vec![draft("Remove unused prototypes"), draft("New work")];
        let f = vec![
            Filed::Covered {
                key: "ABC-96".into(),
                url: String::new(),
                title: "Fix alt on the unused components".into(),
            },
            Filed::Ticket {
                key: "ABC-98".into(),
                url: String::new(),
                existing: false,
                linked: Linked::Yes,
            },
        ];
        let c = comment(
            &d,
            &f,
            &Tracker::Jira {
                project: "ABC".into(),
                site: String::new(),
            },
        );
        assert!(c.starts_with("Filed 1 follow-up ticket"), "{c}");
        assert!(
            c.contains(
                "Remove unused prototypes: covered by ABC-96 (Fix alt on the unused components)"
            ),
            "{c}"
        );
        assert!(f[1].is_new() && !f[0].is_new());
        assert_eq!(f[0].key(), Some("ABC-96"));
    }

    #[test]
    fn project_and_labels() {
        assert_eq!(project_of("ABC-12"), Some("ABC"));
        assert_eq!(project_of("A1_B-3"), Some("A1_B"));
        assert_eq!(project_of("#12"), None);
        assert_eq!(project_of("abc-1"), None);
        assert_eq!(
            labels("ABC-12", "coderhelm"),
            vec!["coderhelm-followup", "coderhelm:followup:ABC-12"]
        );
    }

    #[test]
    fn followup_labels_never_carry_the_trigger() {
        for trigger in ["coderhelm", "CoderHelm", "ai", "coderhelm-followup", "x:y"] {
            let l = labels("ABC-12", trigger);
            assert!(
                l.iter().all(|x| !is_trigger_label(x, trigger)),
                "{trigger}: {l:?}"
            );
            assert!(l.contains(&"coderhelm:followup:ABC-12".to_string()));
        }
        // A team whose trigger is the marker itself gets the source label only.
        assert_eq!(
            labels("ABC-1", "coderhelm-followup"),
            vec!["coderhelm:followup:ABC-1"]
        );
        // The follow-up labels are never a trigger, even on the default.
        assert!(!is_trigger_label("coderhelm:followup:ABC-1", "coderhelm"));
        assert!(!is_trigger_label("coderhelm-followup", "coderhelm"));
        assert!(is_trigger_label("coderhelm", "coderhelm"));
        assert!(is_trigger_label("coderhelm:acme/web", "coderhelm"));
        assert!(!is_trigger_label("coderhelmish", "coderhelm"));
    }

    #[test]
    fn a_followup_is_known_by_its_labels_or_text() {
        assert_eq!(
            source_of_labels(&["coderhelm", "coderhelm:followup:abc-10"]),
            Some("ABC-10".into())
        );
        assert_eq!(
            source_of_labels(&["coderhelm-followup"]),
            Some(String::new())
        );
        assert_eq!(source_of_labels(&["coderhelm", "coderhelm:acme/web"]), None);
        let body = description(
            &Draft {
                summary: "Fix alt".into(),
                description: "Add alt text.".into(),
            },
            "ABC-1",
            "Audit",
            "https://app/runs/detail?id=r",
        );
        assert!(is_followup_body(&body), "{body}");
        assert_eq!(source_of_body(&body), Some("ABC-1".into()));
        assert!(!is_followup_body("Add alt text to the images."));
        assert_eq!(source_of_body("Add alt text to the images."), None);
        assert_eq!(of_record_sk("abc-9"), "FOLLOWUP_OF#ABC-9");
        assert_eq!(
            day_record_sk("abc-1", "2026-11-24"),
            "FOLLOWUP_DAY#ABC-1#2026-11-24"
        );
        assert!(held_comment("ABC-1").contains("from ABC-1"));
        assert!(depth_refusal("ABC-1").contains("never files follow-ups"));
    }
}
