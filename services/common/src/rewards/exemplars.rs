//! Past work that earned (or lost) reward, shown to agents as examples.
//!
//! When a review comment is acted on, or a CoderHelm pull request merges as
//! written, it becomes a "good" example for that repository; one dismissed,
//! closed or reverted becomes an "avoid" example. Each keeps the sum of the
//! scores of its outcomes and the embedding of its text (metered Titan, made
//! once when the example is first written). When an agent starts on similar
//! work, [`select`] ranks the repository's examples by similarity to the new
//! work, size of reward and same file area and finding category, and hands
//! over the best few of each polarity.
//!
//! Each example also keeps how it reads on the Learning page: a one-line
//! title, a short cleaned summary ([`super::example_text`]), the reason it
//! is an example, and the last runs and reviews it was given to.

use super::example_text;
use super::RewardKind;
use serde::Serialize;
use sha2::{Digest, Sha256};

/// Sort-key prefix of an example.
pub const PREFIX: &str = "REWARDEX#";
/// Total score at or above which an example is a good one.
pub const GOOD_AT: f64 = 1.0;
/// Total score at or below which an example is one to avoid.
pub const AVOID_AT: f64 = -1.0;
/// Most examples of each polarity given to one prompt.
pub const MAX_EACH: usize = 3;
/// Most text kept per example.
pub const MAX_TEXT: usize = 700;
/// Most recent uses kept per example.
pub const MAX_USES_KEPT: usize = 5;
/// Where the body of an example was cut before titles were stored.
pub const LEGACY_FIX_BODY: usize = 500;
pub const LEGACY_FINDING_BODY: usize = 400;

pub const KIND_FINDING: &str = "finding";
pub const KIND_FIX: &str = "fix";

pub fn sk(repo: &str, id: &str) -> String {
    format!("{PREFIX}{repo}#{id}")
}

pub fn repo_prefix(repo: &str) -> String {
    format!("{PREFIX}{repo}#")
}

/// A stable id: the same thread or pull request is one example.
pub fn id_for(repo: &str, pr: u64, what: &str) -> String {
    let d = Sha256::digest(format!("{repo}#{pr}#{what}").as_bytes());
    d.iter().take(10).map(|b| format!("{b:02x}")).collect()
}

/// One time an example was given to an agent.
#[derive(Debug, Clone, Default, PartialEq, Serialize, serde::Deserialize)]
pub struct Use {
    pub at: String,
    /// `reviewer` or `coder`.
    pub agent: String,
    /// The coding run it was given to; empty for a review.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    /// The pull request reviewed; 0 for a coding run.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub pr: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Exemplar {
    pub id: String,
    pub repo: String,
    /// [`KIND_FINDING`] or [`KIND_FIX`].
    pub kind: String,
    pub category: String,
    pub area: String,
    /// What agents are given and what is embedded: `title: plain summary`.
    pub text: String,
    /// One clean line (empty on examples kept before titles were).
    pub title: String,
    /// Short cleaned markdown.
    pub summary: String,
    /// Why it is an example, from its latest outcome.
    pub reason: String,
    /// The run that made the change (code changes only).
    pub run_id: String,
    pub last_used_at: String,
    /// The latest uses, oldest first, at most [`MAX_USES_KEPT`].
    pub used_by: Vec<Use>,
    /// Sum of its outcomes' scores.
    pub reward: f64,
    pub events: u64,
    /// Prompts it was put in.
    pub uses: u64,
    pub pr: u64,
    pub created_at: String,
    #[serde(skip)]
    pub embedding: Vec<f32>,
}

/// How an example reads on the page.
#[derive(Debug, Clone, PartialEq)]
pub struct Display {
    pub title: String,
    pub summary: String,
    pub reason: String,
}

impl Display {
    /// `title: summary` as one plain line (the title alone when the summary
    /// adds nothing).
    pub fn line(&self) -> String {
        let rest = example_text::plain(&self.summary);
        if rest.is_empty() || rest == self.title {
            self.title.clone()
        } else {
            format!("{}: {rest}", self.title)
        }
    }
}

impl Exemplar {
    /// A new example from a title and a body as written on GitHub: the
    /// title and summary are cleaned once, here, and `text` is made from
    /// them.
    pub fn with_text(mut self, raw_title: &str, body: &str) -> Self {
        self.title = example_text::title(raw_title, body);
        self.summary = example_text::summary(body);
        let d = Display {
            title: self.title.clone(),
            summary: self.summary.clone(),
            reason: String::new(),
        };
        self.text = example_text::cut_words(&d.line(), MAX_TEXT);
        self
    }

    /// Title, summary and reason; an example kept before they were stored
    /// gets them from its text and its kind now.
    pub fn display(&self) -> Display {
        let (title, summary) = if self.title.is_empty() {
            let cap = if self.kind == KIND_FIX {
                LEGACY_FIX_BODY
            } else {
                LEGACY_FINDING_BODY
            };
            example_text::legacy_split(&self.text, cap)
        } else {
            (self.title.clone(), self.summary.clone())
        };
        let reason = if self.reason.is_empty() {
            fallback_reason(&self.kind, self.polarity())
        } else {
            self.reason.clone()
        };
        Display {
            title,
            summary,
            reason,
        }
    }

    /// The uses to keep after one more: the latest [`MAX_USES_KEPT`].
    pub fn uses_after(&self, next: Use) -> Vec<Use> {
        let mut v = self.used_by.clone();
        v.push(next);
        let drop = v.len().saturating_sub(MAX_USES_KEPT);
        v.drain(..drop);
        v
    }

    pub fn polarity(&self) -> Option<&'static str> {
        if self.reward >= GOOD_AT {
            Some("good")
        } else if self.reward <= AVOID_AT {
            Some("avoid")
        } else {
            None
        }
    }
}

/// Why an example kept before reasons were stored is one.
pub fn fallback_reason(kind: &str, polarity: Option<&str>) -> String {
    match (kind, polarity) {
        (KIND_FIX, Some("good")) => "Merged, with a good outcome",
        (KIND_FIX, _) => "Closed, rewritten or reverted",
        (_, Some("good")) => "The team acted on this comment",
        _ => "The team dismissed it or left it open",
    }
    .to_string()
}

/// Why a code change is an example, from its pull request's outcome events
/// (`kind`, `value`): how CI went, then how it merged or closed.
pub fn fix_reason(events: &[(RewardKind, f64)]) -> String {
    let mut parts: Vec<String> = vec![];
    for (k, v) in events {
        match k {
            RewardKind::CiFirstTry => parts.push("CI passed on the first try".into()),
            RewardKind::CiFixRounds if *v >= 1.0 => {
                let n = v.round() as u64;
                parts.push(format!(
                    "CI needed {n} fix round{}",
                    if n == 1 { "" } else { "s" }
                ))
            }
            _ => {}
        }
    }
    for (k, v) in events {
        match k {
            RewardKind::PrMergedUnchanged => parts.push("merged without changes".into()),
            RewardKind::PrMergedEdited => {
                let pct = (v.clamp(0.0, 1.0) * 100.0).round() as u64;
                parts.push(if pct > 0 {
                    format!("merged after people changed {pct}% of it")
                } else {
                    "merged after people edited it".into()
                })
            }
            RewardKind::PrClosedUnmerged => parts.push("closed without merging".into()),
            RewardKind::RereviewRequested => parts.push("a re-review was asked for".into()),
            _ => {}
        }
    }
    let mut s = parts.join(", ");
    if let Some(c) = s.get(..1) {
        s = format!("{}{}", c.to_uppercase(), &s[1..]);
    }
    s
}

/// Why a review comment is an example.
pub fn finding_reason(kind: RewardKind) -> String {
    match kind {
        RewardKind::FindingAccepted => "The author changed the code it pointed at",
        RewardKind::FindingDismissed => "The team pushed back and the comment was judged wrong",
        RewardKind::FindingIgnored => "Still open when the pull request closed",
        _ => "",
    }
    .to_string()
}

/// The embedding as stored (little-endian f32).
pub fn pack(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

pub fn unpack(b: &[u8]) -> Vec<f32> {
    b.chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

pub fn cosine(a: &[f32], b: &[f32]) -> f64 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let (mut dot, mut na, mut nb) = (0f64, 0f64, 0f64);
    for (x, y) in a.iter().zip(b) {
        dot += (*x as f64) * (*y as f64);
        na += (*x as f64).powi(2);
        nb += (*y as f64).powi(2);
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// What the agent is about to work on.
#[derive(Debug, Clone, Default)]
pub struct Query<'a> {
    pub kind: &'a str,
    pub areas: &'a [String],
    pub categories: &'a [String],
    pub embedding: Option<&'a [f32]>,
}

fn rank(e: &Exemplar, q: &Query<'_>) -> f64 {
    let sim = q.embedding.map(|v| cosine(v, &e.embedding)).unwrap_or(0.0);
    let size = (e.reward.abs() / 3.0).min(1.0);
    let area = if !e.area.is_empty() && q.areas.iter().any(|a| a == &e.area) {
        1.0
    } else {
        0.0
    };
    let cat = if !e.category.is_empty()
        && q.categories
            .iter()
            .any(|c| c.eq_ignore_ascii_case(&e.category))
    {
        1.0
    } else {
        0.0
    };
    0.5 * sim + 0.3 * size + 0.2 * area + 0.1 * cat
}

/// The best good and avoid examples for `q`, at most [`MAX_EACH`] each.
pub fn select(cands: &[Exemplar], q: &Query<'_>) -> (Vec<Exemplar>, Vec<Exemplar>) {
    let mut good: Vec<(f64, &Exemplar)> = vec![];
    let mut avoid: Vec<(f64, &Exemplar)> = vec![];
    for e in cands.iter().filter(|e| e.kind == q.kind) {
        match e.polarity() {
            Some("good") => good.push((rank(e, q), e)),
            Some("avoid") => avoid.push((rank(e, q), e)),
            _ => {}
        }
    }
    let top = |mut v: Vec<(f64, &Exemplar)>| -> Vec<Exemplar> {
        v.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.id.cmp(&b.1.id))
        });
        v.into_iter()
            .take(MAX_EACH)
            .map(|(_, e)| e.clone())
            .collect()
    };
    (top(good), top(avoid))
}

/// The prompt block for selected examples; empty when there are none.
pub fn prompt_block(kind: &str, good: &[Exemplar], avoid: &[Exemplar]) -> String {
    if good.is_empty() && avoid.is_empty() {
        return String::new();
    }
    let (title, good_intro, avoid_intro) = if kind == KIND_FINDING {
        (
            "Examples from earlier reviews of this repository",
            "Comments the team acted on (write findings like these):",
            "Comments the team dismissed or left open (do not raise findings like these):",
        )
    } else {
        (
            "Examples from earlier changes to this repository",
            "Changes the team merged as written (follow their approach):",
            "Changes the team closed, rewrote or reverted (avoid what they did):",
        )
    };
    let line = |e: &Exemplar| {
        let text = e.display().line();
        let mut tag = String::new();
        if !e.category.is_empty() {
            tag.push_str(&format!("[{}] ", e.category));
        }
        if !e.area.is_empty() {
            tag.push_str(&format!("`{}` ", e.area));
        }
        format!("- {tag}{}", example_text::cut_words(&text, 400))
    };
    let mut out = format!("\n\n## {title}\n");
    if !good.is_empty() {
        out.push_str(good_intro);
        out.push('\n');
        out.push_str(&good.iter().map(line).collect::<Vec<_>>().join("\n"));
        out.push('\n');
    }
    if !avoid.is_empty() {
        out.push_str(avoid_intro);
        out.push('\n');
        out.push_str(&avoid.iter().map(line).collect::<Vec<_>>().join("\n"));
        out.push('\n');
    }
    out.push_str("Security and correctness come first whatever these examples show.");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ex(id: &str, kind: &str, reward: f64, area: &str, cat: &str, emb: Vec<f32>) -> Exemplar {
        Exemplar {
            id: id.into(),
            repo: "acme/web".into(),
            kind: kind.into(),
            category: cat.into(),
            area: area.into(),
            text: format!("text {id}"),
            reward,
            events: 1,
            embedding: emb,
            ..Default::default()
        }
    }

    #[test]
    fn polarity_needs_real_reward() {
        assert_eq!(ex("a", "fix", 1.5, "", "", vec![]).polarity(), Some("good"));
        assert_eq!(
            ex("a", "fix", -2.0, "", "", vec![]).polarity(),
            Some("avoid")
        );
        assert_eq!(ex("a", "fix", 0.5, "", "", vec![]).polarity(), None);
    }

    #[test]
    fn selection_prefers_similar_same_area_high_reward() {
        let q_emb = vec![1.0, 0.0];
        let cands = vec![
            ex(
                "near",
                KIND_FINDING,
                1.0,
                "src/api",
                "tests",
                vec![1.0, 0.0],
            ),
            ex("far", KIND_FINDING, 1.0, "docs", "style", vec![0.0, 1.0]),
            ex("big", KIND_FINDING, 3.0, "docs", "style", vec![0.0, 1.0]),
            ex(
                "weak",
                KIND_FINDING,
                0.5,
                "src/api",
                "tests",
                vec![1.0, 0.0],
            ),
            ex(
                "bad",
                KIND_FINDING,
                -2.0,
                "src/api",
                "style",
                vec![1.0, 0.0],
            ),
            ex("fix", KIND_FIX, 3.0, "src/api", "", vec![1.0, 0.0]),
            ex("x1", KIND_FINDING, 1.0, "", "", vec![]),
            ex("x2", KIND_FINDING, 1.0, "", "", vec![]),
        ];
        let areas = vec!["src/api".to_string()];
        let cats = vec!["tests".to_string()];
        let q = Query {
            kind: KIND_FINDING,
            areas: &areas,
            categories: &cats,
            embedding: Some(&q_emb),
        };
        let (good, avoid) = select(&cands, &q);
        let ids: Vec<&str> = good.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids[0], "near");
        assert_eq!(good.len(), MAX_EACH);
        assert!(!ids.contains(&"weak"));
        assert!(!ids.contains(&"fix"));
        assert_eq!(avoid.len(), 1);
        assert_eq!(avoid[0].id, "bad");
        let b = prompt_block(KIND_FINDING, &good, &avoid);
        assert!(b.contains("Comments the team acted on"));
        assert!(b.contains("[style] `src/api` text bad"));
        assert!(prompt_block(KIND_FIX, &[], &[]).is_empty());
    }

    #[test]
    fn new_examples_store_clean_text() {
        let e = Exemplar {
            kind: KIND_FIX.into(),
            ..Default::default()
        }
        .with_text(
            "fix(api): retry payments",
            "Source ticket: OPS-1\n\n<!-- coderhelm:ci -->**Summary** Retry once.\n\n- Keep the key",
        );
        assert_eq!(e.title, "fix(api): retry payments");
        assert_eq!(e.summary, "Retry once.\n\n- Keep the key");
        assert_eq!(e.text, "fix(api): retry payments: Retry once. Keep the key");
        assert_eq!(e.display().title, e.title);
    }

    #[test]
    fn old_examples_read_the_same_way() {
        let e = Exemplar {
            kind: KIND_FINDING.into(),
            text: "Missing test: <!-- x -->The **retry** path has no test.".into(),
            reward: -2.0,
            ..Default::default()
        };
        let d = e.display();
        assert_eq!(d.title, "Missing test");
        assert_eq!(d.summary, "The **retry** path has no test.");
        assert_eq!(d.reason, "The team dismissed it or left it open");
        let b = prompt_block(KIND_FINDING, &[], &[e]);
        assert!(
            b.contains("- Missing test: The retry path has no test."),
            "{b}"
        );
    }

    #[test]
    fn reasons_say_what_happened() {
        assert_eq!(
            fix_reason(&[
                (RewardKind::PrMergedUnchanged, 1.0),
                (RewardKind::CiFirstTry, 1.0)
            ]),
            "CI passed on the first try, merged without changes"
        );
        assert_eq!(
            fix_reason(&[
                (RewardKind::CiFixRounds, 3.0),
                (RewardKind::PrMergedEdited, 0.4)
            ]),
            "CI needed 3 fix rounds, merged after people changed 40% of it"
        );
        assert_eq!(
            fix_reason(&[(RewardKind::PrClosedUnmerged, 1.0)]),
            "Closed without merging"
        );
        assert_eq!(fix_reason(&[]), "");
        assert!(finding_reason(RewardKind::FindingIgnored).starts_with("Still open"));
    }

    #[test]
    fn keeps_the_latest_uses() {
        let mut e = Exemplar::default();
        for i in 0..7 {
            e.used_by = e.uses_after(Use {
                at: format!("t{i}"),
                agent: "coder".into(),
                run_id: format!("r{i}"),
                pr: 0,
            });
        }
        assert_eq!(e.used_by.len(), MAX_USES_KEPT);
        assert_eq!(e.used_by[0].at, "t2");
        assert_eq!(e.used_by.last().unwrap().at, "t6");
    }

    #[test]
    fn embeddings_pack_and_compare() {
        let v = vec![0.5f32, -1.25, 3.0];
        assert_eq!(unpack(&pack(&v)), v);
        assert!((cosine(&v, &v) - 1.0).abs() < 1e-9);
        assert_eq!(cosine(&v, &[1.0]), 0.0);
        assert_eq!(cosine(&[0.0, 0.0], &[0.0, 0.0]), 0.0);
        assert_eq!(id_for("a/b", 1, "T1"), id_for("a/b", 1, "T1"));
        assert_ne!(id_for("a/b", 1, "T1"), id_for("a/b", 2, "T1"));
    }
}
