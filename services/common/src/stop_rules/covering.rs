//! Rule 4 when the covering pull request is CoderHelm's own: one pull
//! request for the work, not two.
//!
//! A run ended as covered by an open pull request ([`super::existing_work`])
//! used to stop there, even when that pull request was CoderHelm's own and
//! lacked part of what the ticket asks: the run's work stayed on a branch
//! and the ticket's earlier pull request stayed open beside it. Now:
//!
//! - CoderHelm's own open pull request ([`crate::own_pr::judge`]) that lacks
//!   something for the ticket: the run continues in it. What the run built
//!   that it doesn't have goes onto its branch, and its description gets a
//!   section for this ticket ([`section`]) naming what was carried over and
//!   what is still open (the unfinished tasks, and what the covered check
//!   itself said it lacks). The run ends linked to it ([`Outcome::Continued`]).
//! - It already has everything: the run ends as before, saying so plainly
//!   ([`complete_note`], [`Outcome::Complete`]).
//! - A person's pull request is never pushed to, edited or closed: the run
//!   ends linked to it and says that person's pull request covers the ticket
//!   ([`person_note`], [`Outcome::Person`]).
//! - It merged or closed meanwhile: the run takes its normal path.
//!
//! The ticket's earlier pull request that CoderHelm opened, now redundant,
//! is marked superseded in its own description ([`mark_superseded`]) and on
//! the run page, which offers to close it. No team setting lets CoderHelm
//! close a pull request by itself, so it closes only on a person's click.

use serde::{Deserialize, Serialize};

use super::existing_work::Covered;

/// How a covered run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// CoderHelm's own pull request lacked something: the run continued in
    /// it and ended linked to it.
    Continued,
    /// The pull request already had everything the ticket asks for.
    Complete,
    /// A person's pull request covers the ticket; CoderHelm didn't touch it.
    Person,
    /// CoderHelm's own pull request, but its run was still working on it:
    /// nothing was written.
    Busy,
    /// CoderHelm's own pull request, but the work couldn't be carried over
    /// without losing some of it: the run's work stays on its branch.
    NotCarried,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Continued => "continued",
            Outcome::Complete => "complete",
            Outcome::Person => "person",
            Outcome::Busy => "busy",
            Outcome::NotCarried => "not_carried",
        }
    }
}

/// What the run put on the covering pull request's branch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Carried {
    /// The branch it went onto (the covering pull request's).
    pub branch: String,
    /// The branch's head after.
    #[serde(default)]
    pub head: String,
    /// The run's commits it brought.
    #[serde(default)]
    pub commits: usize,
    /// The files they change.
    #[serde(default)]
    pub files: Vec<String>,
    /// The run's change was rebuilt on the branch (the two had diverged)
    /// rather than added on top of it.
    #[serde(default)]
    pub rebuilt: bool,
}

/// The ticket's earlier pull request CoderHelm opened, made redundant.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Superseded {
    pub repo: String,
    pub number: u64,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub title: String,
    /// A person closed it from the run page.
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub closed_by: String,
}

/// What a covered run records (`covered_by` on the run): the pull request,
/// why it covers the ticket, and what the run did about it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub repo: String,
    pub number: u64,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub signals: Vec<String>,
    /// It is CoderHelm's own pull request ([`crate::own_pr::judge`]).
    #[serde(default)]
    pub own: bool,
    /// [`Outcome::as_str`]; empty on records from before.
    #[serde(default)]
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carried: Option<Carried>,
    /// What is still open for this ticket in that pull request.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_items: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded: Option<Superseded>,
    /// What the run page says, in one paragraph.
    #[serde(default)]
    pub note: String,
}

impl Record {
    pub fn new(c: &Covered, own: bool, outcome: Outcome) -> Record {
        Record {
            repo: c.pr.repo.clone(),
            number: c.pr.number,
            url: c.pr.url.clone(),
            title: c.pr.title.clone(),
            author: c.pr.author.clone(),
            reason: c.reason.clone(),
            signals: c.signals.lines(),
            own,
            outcome: outcome.as_str().to_string(),
            ..Default::default()
        }
    }
}

/// Most open items one section lists.
const MAX_OPEN_ITEMS: usize = 20;

/// What is still open for the ticket: the run's unfinished tasks, then what
/// the covered check said the pull request lacks, each once.
pub fn open_items(unfinished: &[String], missing: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for i in unfinished.iter().chain(missing) {
        let i = i.trim().trim_end_matches('.').trim();
        if i.is_empty() || out.iter().any(|o| o.eq_ignore_ascii_case(i)) {
            continue;
        }
        out.push(i.to_string());
        if out.len() == MAX_OPEN_ITEMS {
            break;
        }
    }
    out
}

/// Nothing to carry over: the pull request already has the run's work and
/// nothing is open for the ticket.
pub fn complete(carried: Option<&Carried>, open: &[String]) -> bool {
    carried.is_none_or(|c| c.commits == 0) && open.is_empty()
}

fn marks(ticket: &str) -> (String, String) {
    (
        format!("<!-- coderhelm:carries {ticket} -->"),
        format!("<!-- /coderhelm:carries {ticket} -->"),
    )
}

/// The covering pull request's section for this ticket. `ticket_line` links
/// the ticket (`Closes #N` for a GitHub issue, `Source ticket: KEY`).
pub fn section(
    ticket: &str,
    ticket_line: &str,
    carried: Option<&Carried>,
    open: &[String],
) -> String {
    let (start, end) = marks(ticket);
    let mut out = format!(
        "{start}\n## Also carries {ticket}\n\n{ticket_line}\n\nThis pull request also does what \
         {ticket} asks: CoderHelm found it already covered the ticket and continued here instead \
         of opening a second pull request.\n"
    );
    match carried.filter(|c| c.commits > 0) {
        Some(c) => {
            out.push_str(&format!(
                "\nCarried over from {ticket}'s run: {} commit{}{}, changing {}.\n",
                c.commits,
                if c.commits == 1 { "" } else { "s" },
                if c.rebuilt {
                    ", rebuilt on this branch"
                } else {
                    ""
                },
                files_list(&c.files)
            ));
        }
        None => out.push_str("\nNothing had to be carried over: the change was already here.\n"),
    }
    if open.is_empty() {
        out.push_str(&format!("\nNothing is open for {ticket}.\n"));
    } else {
        out.push_str(&format!("\n### Still open for {ticket}\n\n"));
        for i in open {
            out.push_str(&format!("- [ ] {i}\n"));
        }
    }
    out.push_str(&end);
    out
}

fn files_list(files: &[String]) -> String {
    match files.len() {
        0 => "no files".to_string(),
        n if n <= 6 => files
            .iter()
            .map(|f| format!("`{f}`"))
            .collect::<Vec<_>>()
            .join(", "),
        n => format!(
            "{} and {} more",
            files[..5]
                .iter()
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", "),
            n - 5
        ),
    }
}

/// `body` with this ticket's section in place: the earlier one replaced,
/// else added at the end. The rest of the description is kept as it is.
pub fn put_section(body: &str, ticket: &str, section: &str) -> String {
    let (start, end) = marks(ticket);
    if let (Some(a), Some(b)) = (body.find(&start), body.find(&end)) {
        if a < b {
            return format!("{}{section}{}", &body[..a], &body[b + end.len()..]);
        }
    }
    let body = body.trim_end();
    if body.is_empty() {
        section.to_string()
    } else {
        format!("{body}\n\n{section}")
    }
}

const SUPERSEDED_MARK: &str = "<!-- coderhelm:superseded -->";

/// The line the redundant pull request's description starts with.
pub fn superseded_line(covering: u64) -> String {
    format!("**Superseded by #{covering}, which carries this ticket's change.**")
}

/// `body` starting with the superseded line, once (a later mark replaces an
/// earlier one).
pub fn mark_superseded(body: &str, covering: u64) -> String {
    let rest = match body.split_once(SUPERSEDED_MARK) {
        Some((_, r)) => r.trim_start(),
        None => body,
    };
    format!(
        "{line}\n{SUPERSEDED_MARK}\n\n{rest}",
        line = superseded_line(covering)
    )
}

/// The comment CoderHelm leaves on the redundant pull request when a person
/// closes it from the run page.
pub fn close_comment(covering: u64, by: &str) -> String {
    let by = by.trim();
    format!(
        "Closed{}: superseded by #{covering}, which carries this ticket's change. The branch is \
         kept.",
        if by.is_empty() {
            String::new()
        } else {
            format!(" by {by} from CoderHelm")
        }
    )
}

/// The run page's note when the run continued in its own pull request.
pub fn continued_note(number: u64, carried: Option<&Carried>, open: &[String]) -> String {
    let what = match carried.filter(|c| c.commits > 0) {
        Some(c) => format!(
            "carried this run's {} commit{} over to its branch `{}`{}",
            c.commits,
            if c.commits == 1 { "" } else { "s" },
            c.branch,
            if c.rebuilt { " (rebuilt on it)" } else { "" }
        ),
        None => "found the change already on its branch".to_string(),
    };
    let open = if open.is_empty() {
        "Nothing is open for this ticket.".to_string()
    } else {
        format!(
            "Still open for this ticket, listed in its description: {}.",
            open.join("; ")
        )
    };
    format!(
        "Continued in #{number}, CoderHelm's own open pull request that already covered this \
         ticket: CoderHelm {what}, added this ticket to its description and opened no second \
         pull request. {open}"
    )
}

/// The run page's note when the covering pull request already has it all.
pub fn complete_note(number: u64) -> String {
    format!(
        "#{number} already has everything this ticket asks for. CoderHelm built no second fix \
         and linked it on the ticket."
    )
}

/// The run page's note when a person's pull request covers the ticket.
pub fn person_note(number: u64, author: &str) -> String {
    let who = if author.trim().is_empty() {
        "a person".to_string()
    } else {
        author.trim().to_string()
    };
    format!(
        "{who}'s pull request #{number} covers this ticket. CoderHelm never pushes to, edits or \
         closes a pull request it didn't create, so it built no second fix and linked #{number} \
         on the ticket. If the ticket needs a separate change, say so on the ticket, then retry."
    )
}

/// The run page's note when the run couldn't carry its work over.
pub fn not_carried_note(number: u64, why: &str, branch: &str) -> String {
    format!(
        "#{number} is CoderHelm's own pull request and covers this ticket, but this run's work \
         couldn't be carried over to it: {}. Nothing was pushed to #{number}; the work is kept on \
         branch `{branch}`. Retry to try again.",
        why.trim().trim_end_matches('.')
    )
}

/// The note on the ticket. `markdown` for GitHub, plain for Jira.
pub fn ticket_note(record: &Record, markdown: bool) -> String {
    let link = match (markdown, record.url.is_empty()) {
        (true, false) => format!("[#{}]({})", record.number, record.url),
        (false, false) => format!("#{} ({})", record.number, record.url),
        _ => format!("{}#{}", record.repo, record.number),
    };
    match record.outcome.as_str() {
        "continued" => format!(
            "This ticket's change is in {link}, CoderHelm's open pull request that already \
             covered it: CoderHelm carried its work over there instead of opening a second pull \
             request.{}",
            if record.open_items.is_empty() {
                String::new()
            } else {
                format!(" Still open: {}.", record.open_items.join("; "))
            }
        ),
        "complete" => format!(
            "{link} already has everything this ticket asks for. CoderHelm didn't build a second \
             fix."
        ),
        "person" => format!(
            "Already being fixed in {link}{}: {}. CoderHelm doesn't change a person's pull \
             request and didn't build a second fix. If this ticket needs a separate change, say \
             so on the ticket and run it again.",
            if record.author.is_empty() {
                String::new()
            } else {
                format!(" by {}", record.author)
            },
            record.reason.trim_end_matches('.')
        ),
        _ => format!(
            "Already being fixed in {link}: {}. CoderHelm didn't build a second fix.",
            record.reason.trim_end_matches('.')
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stop_rules::existing_work::{OpenPr, Signals};

    fn covered() -> Covered {
        Covered {
            pr: OpenPr {
                repo: "acme/web".into(),
                number: 52,
                title: "Ship the change".into(),
                url: "https://github.com/acme/web/pull/52".into(),
                author: "coderhelm[bot]".into(),
                ..Default::default()
            },
            signals: Signals {
                ticket_key: true,
                ..Default::default()
            },
            reason: "It does the ticket's change.".into(),
            missing: vec!["Register the two helper files.".into()],
        }
    }

    fn carried(n: usize) -> Carried {
        Carried {
            branch: "coderhelm/abc-2".into(),
            head: "t1".into(),
            commits: n,
            files: vec!["src/a.ts".into(), "src/b.ts".into()],
            rebuilt: false,
        }
    }

    #[test]
    fn open_items_are_the_unfinished_tasks_then_what_the_check_named() {
        let items = open_items(
            &["Add the settings page.".into(), " ".into()],
            &[
                "Register the two helper files.".into(),
                "add the settings page".into(),
            ],
        );
        assert_eq!(
            items,
            vec!["Add the settings page", "Register the two helper files"]
        );
    }

    #[test]
    fn complete_means_nothing_carried_and_nothing_open() {
        assert!(complete(None, &[]));
        assert!(complete(Some(&carried(0)), &[]));
        assert!(!complete(Some(&carried(2)), &[]));
        assert!(!complete(None, &["x".into()]));
    }

    #[test]
    fn the_section_names_the_ticket_what_came_over_and_what_is_open() {
        let s = section(
            "ABC-1",
            "Source ticket: ABC-1",
            Some(&carried(3)),
            &["Register the two helper files".into()],
        );
        assert!(s.contains("## Also carries ABC-1"));
        assert!(s.contains("Source ticket: ABC-1"));
        assert!(s.contains("3 commits, changing `src/a.ts`, `src/b.ts`"));
        assert!(s.contains("- [ ] Register the two helper files"));
        let none = section("ABC-1", "Closes #7", None, &[]);
        assert!(none.contains("Nothing had to be carried over"));
        assert!(none.contains("Nothing is open for ABC-1"));
    }

    #[test]
    fn the_section_replaces_its_earlier_self_and_keeps_the_rest() {
        let body = "## Summary\nShips it.";
        let first = put_section(
            body,
            "ABC-1",
            &section("ABC-1", "x", Some(&carried(1)), &[]),
        );
        assert!(first.starts_with("## Summary\nShips it.\n\n<!-- coderhelm:carries ABC-1 -->"));
        let second = put_section(
            &first,
            "ABC-1",
            &section("ABC-1", "x", Some(&carried(4)), &[]),
        );
        assert_eq!(second.matches("## Also carries ABC-1").count(), 1);
        assert!(second.contains("4 commits"));
        assert!(!second.contains("1 commit,"));
        assert!(second.starts_with("## Summary\nShips it."));
        // Another ticket's section is its own.
        let both = put_section(&second, "ABC-9", &section("ABC-9", "x", None, &[]));
        assert!(both.contains("## Also carries ABC-1") && both.contains("## Also carries ABC-9"));
    }

    #[test]
    fn the_superseded_line_goes_first_once() {
        let body = "Source ticket: ABC-1\n\n## Summary";
        let once = mark_superseded(body, 52);
        assert!(once.starts_with("**Superseded by #52, which carries this ticket's change.**"));
        assert!(once.ends_with("Source ticket: ABC-1\n\n## Summary"));
        let twice = mark_superseded(&once, 60);
        assert_eq!(twice.matches("Superseded by").count(), 1);
        assert!(twice.starts_with("**Superseded by #60"));
        assert!(twice.ends_with("Source ticket: ABC-1\n\n## Summary"));
    }

    #[test]
    fn the_notes_say_plainly_what_happened() {
        let c = covered();
        assert!(complete_note(52).starts_with("#52 already has everything this ticket asks for"));
        let p = person_note(41, "octocat");
        assert!(p.starts_with("octocat's pull request #41 covers this ticket"));
        assert!(p.contains("never pushes to, edits or closes a pull request it didn't create"));
        let n = continued_note(52, Some(&carried(2)), &["Register the helpers".into()]);
        assert!(n.starts_with("Continued in #52"));
        assert!(n.contains("carried this run's 2 commits over to its branch `coderhelm/abc-2`"));
        assert!(n.contains("Still open for this ticket"));
        let mut r = Record::new(&c, true, Outcome::Continued);
        r.open_items = vec!["Register the helpers".into()];
        assert!(ticket_note(&r, true)
            .starts_with("This ticket's change is in [#52](https://github.com/acme/web/pull/52)"));
        r.outcome = "complete".into();
        assert!(ticket_note(&r, false).contains("already has everything"));
        r.outcome = "person".into();
        r.author = "octocat".into();
        assert!(ticket_note(&r, false).contains("doesn't change a person's pull request"));
        assert!(
            close_comment(52, "Ana").starts_with("Closed by Ana from CoderHelm: superseded by #52")
        );
    }

    #[test]
    fn a_record_round_trips_and_old_records_read() {
        let mut r = Record::new(&covered(), true, Outcome::Continued);
        r.carried = Some(carried(2));
        r.superseded = Some(Superseded {
            repo: "acme/web".into(),
            number: 41,
            ..Default::default()
        });
        let back: Record = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back, r);
        let old: Record = serde_json::from_str(
            r#"{"repo":"acme/web","number":41,"url":"u","title":"t","author":"octocat","reason":"r","signals":[]}"#,
        )
        .unwrap();
        assert!(!old.own && old.outcome.is_empty() && old.superseded.is_none());
    }
}
