//! Citations for brain answers.
//!
//! Every source an answer may cite is registered as it is read (a
//! knowledge passage, an attached file, a connection's or data source's
//! result, a memory, a file of code) and gets a short id (`S1`, `S2`, ...)
//! that the model writes in brackets. Citations come only from this
//! registry, never from the answer's prose: a bracket naming no registered
//! source is removed, and an answer that cites nothing is replaced by a
//! plain statement that the sources don't cover the question.

use serde::{Deserialize, Serialize};

use super::search::Hit;

/// The answer given when nothing in the chosen sources supports one.
pub const NO_SOURCE_ANSWER: &str =
    "Nothing in what you chose to search answers this, so I won't guess. \
Try searching more places, adding the document to Knowledge, or attaching it to your question.";

/// Longest passage kept on a citation.
const PASSAGE_CHARS: usize = 1_600;

/// One source an answer can cite.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SourceRef {
    /// `S1`, `S2`, ...
    pub id: String,
    /// `knowledge`, `upload`, `connection`, `data_source`, `code`, `memory`, `skill`.
    pub kind: String,
    /// The file, page, tool or item's name.
    pub title: String,
    /// Where in it ("Page 3", a path, a repo).
    #[serde(default)]
    pub loc: String,
    /// The source's date: a document's own date, or the time a live source
    /// was read.
    #[serde(default)]
    pub as_of: String,
    /// The text the answer rests on.
    #[serde(default)]
    pub passage: String,
    /// A page in the dashboard, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub href: Option<String>,
    /// From a source of truth.
    #[serde(default)]
    pub authoritative: bool,
    /// The knowledge base it came from.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub base: String,
    /// The original page outside Coderhelm (a synced Notion, Confluence or
    /// Drive page), which the citation links to first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The connector a synced passage came from (`notion`, `confluence`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source_kind: String,
}

/// The sources read while answering one question.
#[derive(Debug, Default, Clone)]
pub struct Registry {
    refs: Vec<SourceRef>,
}

impl Registry {
    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    pub fn len(&self) -> usize {
        self.refs.len()
    }

    pub fn get(&self, id: &str) -> Option<&SourceRef> {
        self.refs.iter().find(|r| r.id == id)
    }

    /// Registers a source and returns its id. The same passage read twice
    /// keeps its first id.
    pub fn add(&mut self, mut r: SourceRef) -> String {
        if let Some(same) = self.refs.iter().find(|x| {
            x.kind == r.kind && x.title == r.title && x.loc == r.loc && x.passage == r.passage
        }) {
            return same.id.clone();
        }
        r.id = format!("S{}", self.refs.len() + 1);
        r.passage = crate::truncate_str(&r.passage, PASSAGE_CHARS).to_string();
        let id = r.id.clone();
        self.refs.push(r);
        id
    }

    /// Registers knowledge passages; returns their ids in order.
    pub fn add_hits(&mut self, hits: &[Hit]) -> Vec<String> {
        hits.iter()
            .map(|h| {
                self.add(SourceRef {
                    kind: "knowledge".into(),
                    title: h.file_name.clone(),
                    loc: h.loc.clone(),
                    as_of: h.as_of.clone(),
                    passage: h.text.clone(),
                    href: Some(format!("/knowledge/base/?id={}", h.kb_id)),
                    authoritative: h.authoritative,
                    base: h.kb_name.clone(),
                    url: original_url(&h.url),
                    source_kind: h.source_kind.clone(),
                    ..Default::default()
                })
            })
            .collect()
    }

    /// The sources `ids` as the block the model reads: each with its id,
    /// title, place, date and text.
    pub fn render(&self, ids: &[String]) -> String {
        let mut out = String::new();
        for id in ids {
            let Some(r) = self.get(id) else { continue };
            out.push_str(&format!(
                "<source id=\"{}\" kind=\"{}\" title=\"{}\"",
                r.id,
                r.kind,
                attr(&r.title)
            ));
            if !r.loc.is_empty() {
                out.push_str(&format!(" where=\"{}\"", attr(&r.loc)));
            }
            if !r.as_of.is_empty() {
                out.push_str(&format!(" as_of=\"{}\"", attr(&r.as_of)));
            }
            if r.authoritative {
                out.push_str(" source_of_truth=\"yes\"");
            }
            out.push_str(">\n");
            out.push_str(r.passage.trim());
            out.push_str("\n</source>\n");
        }
        out
    }
}

/// A synced item's address, only when it is a plain https link (never a
/// `javascript:` or other scheme a page could be tricked into opening).
pub fn original_url(raw: &str) -> Option<String> {
    let u = reqwest::Url::parse(raw.trim()).ok()?;
    (u.scheme() == "https" && u.host_str().is_some()).then(|| u.to_string())
}

fn attr(s: &str) -> String {
    s.replace('"', "'").replace(['\n', '\r'], " ")
}

/// A finished answer.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Final {
    pub text: String,
    /// The sources cited, in the order first cited.
    pub citations: Vec<SourceRef>,
    /// False when the answer was replaced because nothing supported it.
    pub grounded: bool,
}

/// The ids in one bracket: `[S1]`, `[S1, S3]`, `[S2][S4]` all work.
fn bracket_ids(inner: &str) -> Option<Vec<String>> {
    let ids: Vec<String> = inner
        .split([',', ';'])
        .map(|p| p.trim().to_string())
        .collect();
    let ok = !ids.is_empty()
        && ids.iter().all(|id| {
            id.len() >= 2 && id.starts_with('S') && id[1..].bytes().all(|b| b.is_ascii_digit())
        });
    ok.then_some(ids)
}

/// The answer as shown: brackets naming no registered source removed, the
/// sources it cites listed, and the no-source answer when it cites nothing.
pub fn finalize(text: &str, reg: &Registry) -> Final {
    let mut out = String::with_capacity(text.len());
    let mut cited: Vec<String> = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find('[') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let Some(close) = after.find(']').filter(|c| *c <= 40) else {
            out.push('[');
            rest = after;
            continue;
        };
        match bracket_ids(&after[..close]) {
            Some(ids) => {
                let known: Vec<String> =
                    ids.into_iter().filter(|id| reg.get(id).is_some()).collect();
                if !known.is_empty() {
                    out.push_str(&format!("[{}]", known.join(", ")));
                    for id in known {
                        if !cited.contains(&id) {
                            cited.push(id);
                        }
                    }
                }
                rest = &after[close + 1..];
            }
            None => {
                out.push('[');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    if cited.is_empty() {
        return Final {
            text: NO_SOURCE_ANSWER.to_string(),
            citations: Vec::new(),
            grounded: false,
        };
    }
    // A removed bracket can leave "word ." behind.
    let text = out.replace(" .", ".").replace(" ,", ",");
    Final {
        text: text.trim().to_string(),
        citations: cited.iter().filter_map(|id| reg.get(id).cloned()).collect(),
        grounded: true,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn only_https_originals_are_linked() {
        assert_eq!(
            super::original_url("https://x.atlassian.net/wiki/spaces/ENG/pages/1"),
            Some("https://x.atlassian.net/wiki/spaces/ENG/pages/1".into())
        );
        assert_eq!(super::original_url("javascript:alert(1)"), None);
        assert_eq!(super::original_url("http://example.com"), None);
        assert_eq!(super::original_url(""), None);
    }

    use super::*;

    fn reg_with(n: usize) -> Registry {
        let mut r = Registry::default();
        for i in 0..n {
            r.add(SourceRef {
                kind: "knowledge".into(),
                title: format!("Doc {i}.pdf"),
                loc: format!("Page {i}"),
                as_of: "2026-05-01".into(),
                passage: format!("passage {i}"),
                ..Default::default()
            });
        }
        r
    }

    #[test]
    fn ids_are_short_and_stable() {
        let mut r = reg_with(2);
        assert_eq!(r.len(), 2);
        assert!(r.get("S1").is_some() && r.get("S2").is_some());
        // The same passage again keeps its id.
        let again = r.add(SourceRef {
            kind: "knowledge".into(),
            title: "Doc 0.pdf".into(),
            loc: "Page 0".into(),
            passage: "passage 0".into(),
            ..Default::default()
        });
        assert_eq!(again, "S1");
        assert_eq!(r.len(), 2);
        let block = r.render(&["S2".into(), "S9".into()]);
        assert!(block.contains("id=\"S2\"") && block.contains("as_of=\"2026-05-01\""));
        assert!(!block.contains("S9"));
    }

    #[test]
    fn citations_come_only_from_the_registry() {
        let r = reg_with(2);
        let f = finalize(
            "Leave is 16 weeks [S1]. Also see [S2, S7] and [S9]. Not a cite: [link] and [S].",
            &r,
        );
        assert!(f.grounded);
        assert_eq!(
            f.text,
            "Leave is 16 weeks [S1]. Also see [S2] and. Not a cite: [link] and [S]."
        );
        let ids: Vec<&str> = f.citations.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, ["S1", "S2"]);
        assert_eq!(f.citations[0].title, "Doc 0.pdf");
    }

    #[test]
    fn the_no_source_answer_replaces_an_uncited_one() {
        // Nothing was found at all.
        let f = finalize("Parental leave is probably 12 weeks.", &Registry::default());
        assert!(!f.grounded);
        assert_eq!(f.text, NO_SOURCE_ANSWER);
        assert!(f.citations.is_empty());
        // Sources were read but the answer cites none (or only made-up ones).
        let f = finalize("I think it's 12 weeks [S5].", &reg_with(1));
        assert!(!f.grounded);
        assert_eq!(f.text, NO_SOURCE_ANSWER);
    }

    #[test]
    fn hits_become_knowledge_sources() {
        let mut r = Registry::default();
        let ids = r.add_hits(&[Hit {
            kb_id: "kb1".into(),
            kb_name: "HR".into(),
            file_id: "f".into(),
            file_name: "Leave.pdf".into(),
            loc: "Page 2".into(),
            as_of: "2026-05-01".into(),
            text: "x".repeat(5_000),
            score: 0.9,
            authoritative: true,
            ord: 4,
            url: "https://www.notion.so/Leave-policy-0a1b".into(),
            source_kind: "notion".into(),
        }]);
        assert_eq!(ids, ["S1"]);
        let s = r.get("S1").unwrap();
        assert_eq!(s.kind, "knowledge");
        assert_eq!(s.base, "HR");
        assert!(s.authoritative);
        assert_eq!(
            s.url.as_deref(),
            Some("https://www.notion.so/Leave-policy-0a1b")
        );
        assert_eq!(s.source_kind, "notion");
        assert_eq!(s.href.as_deref(), Some("/knowledge/base/?id=kb1"));
        assert!(s.passage.len() <= 1_600);
    }
}
