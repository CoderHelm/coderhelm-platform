//! A repository's pull request template, as the structure of the
//! description.
//!
//! Before, a repository's template was given to the model that wrote the
//! first draft and nothing else: the description CoderHelm posted opened
//! with its own fixed layout (Summary, Needed from you, Review focus, Risk,
//! Verification) and the template's sections, as the model had filled them,
//! went into the collapsed "Implementation notes". A rewrite after the head
//! moved never saw the template at all.
//!
//! Now the template's headings are the visible structure, in its order and
//! at its levels, and CoderHelm's content is filled into them by what each
//! heading asks for ([`Slot`]): the summary into the description section,
//! the check table into the testing section, the risk and the way back into
//! the risk section, the screenshots into the screenshots section, the
//! ticket into the related section; a checklist keeps the template's boxes;
//! any other section keeps what the writer put under that heading. What a
//! person must do and the review focus stay in sight after the first
//! section. Everything here is pure; `pr_body::render_templated` lays the
//! blocks out.

/// The hidden mark of a visible part laid into a template.
pub const MARK: &str = "<!-- coderhelm:template -->";

/// One section of a template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    /// `##` and the like, as the template writes it.
    pub level: String,
    pub title: String,
    /// The template's own lines under the heading, comments taken out.
    pub lines: Vec<String>,
}

impl Section {
    pub fn heading(&self) -> String {
        format!("{} {}", self.level, self.title)
    }
}

/// What a section asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    Summary,
    Changes,
    Testing,
    Risk,
    Screens,
    Ticket,
    Checklist,
    Review,
    /// Anything else: the writer's words under that heading.
    Other,
}

fn has_any(t: &str, words: &[&str]) -> bool {
    let w: Vec<&str> = t
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .collect();
    words.iter().any(|x| {
        if x.contains(' ') {
            t.contains(x)
        } else {
            w.iter().any(|y| y == x || y.strip_suffix('s') == Some(x))
        }
    })
}

/// What a heading asks for.
pub fn slot(s: &Section) -> Slot {
    let t = s.title.to_lowercase();
    let boxes = s.lines.iter().any(|l| is_box(l));
    if has_any(&t, &["checklist", "check list"]) || (boxes && has_any(&t, &["before", "author"])) {
        return Slot::Checklist;
    }
    if has_any(
        &t,
        &[
            "screenshot",
            "screen",
            "recording",
            "video",
            "visual",
            "demo",
        ],
    ) {
        return Slot::Screens;
    }
    if has_any(
        &t,
        &[
            "test",
            "testing",
            "tested",
            "verification",
            "verify",
            "qa",
            "validation",
            "how to check",
        ],
    ) {
        return Slot::Testing;
    }
    if has_any(&t, &["risk", "rollback", "roll back", "impact", "rollout"]) {
        return Slot::Risk;
    }
    if has_any(
        &t,
        &[
            "ticket",
            "issue",
            "issues",
            "related",
            "link",
            "jira",
            "reference",
            "closes",
            "fixes",
        ],
    ) {
        return Slot::Ticket;
    }
    if has_any(&t, &["reviewer", "review"]) {
        return Slot::Review;
    }
    if has_any(&t, &["change", "changed", "implementation", "detail"]) {
        return Slot::Changes;
    }
    if has_any(
        &t,
        &[
            "summary",
            "description",
            "what",
            "overview",
            "context",
            "why",
            "purpose",
            "background",
            "motivation",
            "about",
        ],
    ) {
        return Slot::Summary;
    }
    if boxes {
        return Slot::Checklist;
    }
    Slot::Other
}

fn is_box(l: &str) -> bool {
    let t = l.trim_start();
    t.starts_with("- [ ]") || t.starts_with("- [x]") || t.starts_with("* [ ]")
}

fn without_comments(text: &str) -> String {
    let mut t = text.to_string();
    while let Some(a) = t.find("<!--") {
        match t[a..].find("-->") {
            Some(b) => t.replace_range(a..a + b + 3, ""),
            None => {
                t.truncate(a);
                break;
            }
        }
    }
    t
}

fn heading_of(line: &str) -> Option<(String, String)> {
    let t = line.trim();
    if !t.starts_with('#') {
        return None;
    }
    let hashes = t.chars().take_while(|c| *c == '#').count();
    if hashes > 4 {
        return None;
    }
    let title = t[hashes..].trim().trim_matches('*').trim();
    (!title.is_empty() && t[hashes..].starts_with(' '))
        .then(|| ("#".repeat(hashes), title.to_string()))
}

/// The sections of a template, in its order. Empty when it has no heading
/// (a template of prose alone gives no structure to fill).
pub fn sections(template: &str) -> Vec<Section> {
    let text = without_comments(template);
    let mut out: Vec<Section> = Vec::new();
    let mut in_code = false;
    for l in text.lines() {
        if l.trim_start().starts_with("```") {
            in_code = !in_code;
        }
        if !in_code {
            if let Some((level, title)) = heading_of(l) {
                out.push(Section {
                    level,
                    title,
                    lines: Vec::new(),
                });
                continue;
            }
        }
        if let Some(s) = out.last_mut() {
            if !l.trim().is_empty() || !s.lines.is_empty() {
                s.lines.push(l.trim_end().to_string());
            }
        }
    }
    for s in &mut out {
        while s.lines.last().is_some_and(|l| l.trim().is_empty()) {
            s.lines.pop();
        }
    }
    out
}

/// The sections a writer wrote, by heading: `(title, text under it)`.
pub fn written_sections(text: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    let mut in_code = false;
    for l in text.lines() {
        if l.trim_start().starts_with("```") {
            in_code = !in_code;
        }
        if !in_code {
            if let Some((_, title)) = heading_of(l) {
                out.push((title, Vec::new()));
                continue;
            }
        }
        if let Some((_, lines)) = out.last_mut() {
            lines.push(l.to_string());
        }
    }
    out.into_iter()
        .map(|(t, l)| (t, l.join("\n").trim().to_string()))
        .collect()
}

/// The checklist lines of a template section, as the template has them.
pub fn boxes(s: &Section) -> Vec<String> {
    s.lines.iter().filter(|l| is_box(l)).cloned().collect()
}

/// The template's headings a visible part lacks, in the template's order.
pub fn missing(template: &str, visible: &str) -> Vec<String> {
    let have: Vec<String> = visible
        .lines()
        .filter_map(heading_of)
        .map(|(_, t)| t.to_lowercase())
        .collect();
    sections(template)
        .into_iter()
        .filter(|s| !have.contains(&s.title.to_lowercase()))
        .map(|s| s.heading())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: &str = "## Description\n<!-- What does this change and why? -->\n\n## Type of change\n- [ ] Bug fix\n- [ ] New feature\n\n## How has this been tested?\n\n## Screenshots\n\n## Checklist\n- [ ] I have read the contributing guide\n- [ ] Tests pass locally\n";

    #[test]
    fn a_templates_sections_are_read_in_order_with_what_each_asks_for() {
        let s = sections(T);
        let got: Vec<(String, Slot)> = s.iter().map(|x| (x.title.clone(), slot(x))).collect();
        assert_eq!(
            got,
            vec![
                ("Description".into(), Slot::Summary),
                ("Type of change".into(), Slot::Changes),
                ("How has this been tested?".into(), Slot::Testing),
                ("Screenshots".into(), Slot::Screens),
                ("Checklist".into(), Slot::Checklist),
            ]
        );
        assert!(s[0].lines.is_empty(), "comments are not content");
        assert_eq!(boxes(&s[4]).len(), 2);
        assert!(sections("Just say what it does.").is_empty());
    }

    #[test]
    fn missing_headings_are_named() {
        let v = "## Description\nx\n## Checklist\n- [ ] a";
        assert_eq!(
            missing(T, v),
            vec![
                "## Type of change".to_string(),
                "## How has this been tested?".to_string(),
                "## Screenshots".to_string()
            ]
        );
    }
}
