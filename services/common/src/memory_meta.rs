//! What a repo memory records about itself, kept in its tags so the worker
//! (which writes memories and recalls them) and the gateway (which shows and
//! edits them) read the same thing: where it came from, whether a person pinned
//! it, and what it is about. Usage is counted separately (see [`usage_sk`]),
//! because a recall shouldn't rewrite the memory store.

/// A pinned memory reaches the agents on every ticket, not only when relevant.
pub const PINNED: &str = "pinned";

const RUN: &str = "run:";
const TICKET: &str = "ticket:";
const TITLE: &str = "title:";
const BY: &str = "by:";
const SOURCE: &str = "source:";
const EDITED_BY: &str = "edited-by:";

/// The topics a memory can be about, as tag, label for people, and the order
/// they are shown in.
pub const TOPICS: &[(&str, &str)] = &[
    ("code-style", "Code style"),
    ("architecture", "How the code is built"),
    ("procedure", "How things are done here"),
    ("anti-pattern", "Things to avoid"),
    ("correction", "Corrections"),
    ("learned", "Other things it learned"),
];

/// Where a memory came from.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct Origin {
    /// `run` (learned while doing a ticket) or `person` (someone added it).
    pub source: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub ticket_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub title: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub added_by: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub edited_by: String,
}

/// Everything the tags say, plus the ones that mean nothing here.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct Meta {
    pub topic: String,
    pub pinned: bool,
    pub origin: Origin,
}

fn clean(v: &str, max: usize) -> String {
    let one_line: String = v.split_whitespace().collect::<Vec<_>>().join(" ");
    crate::truncate_str(&one_line, max).to_string()
}

/// Tags for a memory learned while doing a ticket.
pub fn run_tags(topic: &str, run_id: &str, ticket_id: &str, title: &str) -> Vec<String> {
    let mut t = vec![topic_tag(topic).to_string(), format!("{SOURCE}run")];
    if !run_id.is_empty() {
        t.push(format!("{RUN}{run_id}"));
    }
    if !ticket_id.is_empty() {
        t.push(format!("{TICKET}{}", clean(ticket_id, 60)));
    }
    if !title.is_empty() {
        t.push(format!("{TITLE}{}", clean(title, 120)));
    }
    t
}

/// Tags for a memory a person added.
pub fn person_tags(topic: &str, by: &str, pinned: bool) -> Vec<String> {
    let mut t = vec![
        topic_tag(topic).to_string(),
        format!("{SOURCE}person"),
        format!("{BY}{}", clean(by, 120)),
    ];
    if pinned {
        t.push(PINNED.to_string());
    }
    t
}

/// A known topic tag, or `learned`.
pub fn topic_tag(topic: &str) -> &'static str {
    TOPICS
        .iter()
        .find(|(t, _)| *t == topic)
        .map(|(t, _)| *t)
        .unwrap_or("learned")
}

pub fn topic_label(topic: &str) -> &'static str {
    TOPICS
        .iter()
        .find(|(t, _)| *t == topic)
        .map(|(_, l)| *l)
        .unwrap_or("Other things it learned")
}

/// Reads what the tags say. Memories written before provenance existed come
/// back as learned from a run with no details, which is what they were.
pub fn parse(tags: &[String]) -> Meta {
    let mut m = Meta {
        topic: "learned".into(),
        origin: Origin {
            source: "run".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    for t in tags {
        if t == PINNED {
            m.pinned = true;
        } else if let Some(v) = t.strip_prefix(RUN) {
            m.origin.run_id = v.into();
        } else if let Some(v) = t.strip_prefix(TICKET) {
            m.origin.ticket_id = v.into();
        } else if let Some(v) = t.strip_prefix(TITLE) {
            m.origin.title = v.into();
        } else if let Some(v) = t.strip_prefix(BY) {
            m.origin.added_by = v.into();
        } else if let Some(v) = t.strip_prefix(EDITED_BY) {
            m.origin.edited_by = v.into();
        } else if let Some(v) = t.strip_prefix(SOURCE) {
            m.origin.source = v.into();
        } else if TOPICS.iter().any(|(k, _)| k == t) {
            m.topic = t.clone();
        }
    }
    m
}

/// The tags after a person changes a memory: topic and pin replaced, who edited
/// it recorded, where it came from kept.
pub fn retag(
    tags: &[String],
    topic: Option<&str>,
    pinned: Option<bool>,
    edited_by: &str,
) -> Vec<String> {
    let current = parse(tags);
    let mut out: Vec<String> = tags
        .iter()
        .filter(|t| {
            *t != PINNED
                && !t.starts_with(EDITED_BY)
                && !(topic.is_some() && TOPICS.iter().any(|(k, _)| k == *t))
        })
        .cloned()
        .collect();
    if let Some(t) = topic {
        out.insert(0, topic_tag(t).to_string());
    }
    if pinned.unwrap_or(current.pinned) {
        out.push(PINNED.into());
    }
    if !edited_by.is_empty() {
        out.push(format!("{EDITED_BY}{}", clean(edited_by, 120)));
    }
    out
}

/// Where a memory's use is counted: one item per memory, in the settings
/// table under the team.
pub fn usage_sk(owner: &str, name: &str, memory_id: &str) -> String {
    format!("MEMUSE#{owner}/{name}#{memory_id}")
}

pub fn usage_prefix(owner: &str, name: &str) -> String {
    format!("MEMUSE#{owner}/{name}#")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_memory_says_where_it_came_from() {
        let tags = run_tags("code-style", "01RUN", "PROJ-12", "Fix   the\nlogin  page");
        let m = parse(&tags);
        assert_eq!(
            m,
            Meta {
                topic: "code-style".into(),
                pinned: false,
                origin: Origin {
                    source: "run".into(),
                    run_id: "01RUN".into(),
                    ticket_id: "PROJ-12".into(),
                    title: "Fix the login page".into(),
                    ..Default::default()
                },
            }
        );
    }

    #[test]
    fn a_person_can_add_pin_and_edit() {
        let tags = person_tags("procedure", "a@example.com", true);
        let m = parse(&tags);
        assert!(m.pinned);
        assert_eq!(m.origin.source, "person");
        assert_eq!(m.origin.added_by, "a@example.com");
        let unpinned = retag(&tags, Some("anti-pattern"), Some(false), "b@example.com");
        let m = parse(&unpinned);
        assert!(!m.pinned);
        assert_eq!(m.topic, "anti-pattern");
        assert_eq!(m.origin.added_by, "a@example.com");
        assert_eq!(m.origin.edited_by, "b@example.com");
        // only one topic and one editor survive repeated edits
        let again = retag(&unpinned, Some("procedure"), None, "c@example.com");
        assert_eq!(
            again
                .iter()
                .filter(|t| TOPICS.iter().any(|(k, _)| k == *t))
                .count(),
            1
        );
        assert_eq!(again.iter().filter(|t| t.starts_with(EDITED_BY)).count(), 1);
    }

    #[test]
    fn old_memories_read_as_learned_from_a_run() {
        let m = parse(&["anti-pattern".into(), "repo:acme/web".into()]);
        assert_eq!(m.topic, "anti-pattern");
        assert_eq!(m.origin.source, "run");
        assert!(m.origin.run_id.is_empty());
        assert_eq!(topic_tag("nonsense"), "learned");
        assert_eq!(topic_label("procedure"), "How things are done here");
    }
}
