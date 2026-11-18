//! One ticket, one run.
//!
//! A ticket reached CoderHelm twice within seconds: once from its tracker,
//! and once as a GitHub issue that CoderHelm itself opened for it from a plan
//! a person approved. The two runs carried different ids (the tracker's key
//! and `GH-<n>`), so the per-id guards saw two tickets, and the same work got
//! two runs and two pull requests.
//!
//! Every run of a ticket now starts through one claim, keyed by the ticket's
//! canonical key ([`canonical`]): the ticket it is for, whatever carried it
//! in. An issue CoderHelm opens for a ticket names that ticket in a marker in
//! its body ([`marker`]) and in the stored link from the issue to its plan
//! task, so its run resolves to the same key as the ticket's own. The claim
//! is one conditional write ([`claim_sk`]): a second trigger finds it held by
//! a live run ([`decide`]) and is dropped in favour of that run, whatever the
//! source, whatever the order.

use crate::messages::TicketSource;

const MARKER_OPEN: &str = "<!-- coderhelm:for-ticket ";
const MARKER_CLOSE: &str = " -->";

/// The line an issue CoderHelm opens for another ticket carries, so its run
/// is that ticket's run.
pub fn marker(key: &str) -> String {
    format!("{MARKER_OPEN}{}{MARKER_CLOSE}", key.trim())
}

/// Whether `s` is a tracker key: letters, then digits or letters, a dash and
/// a number (`ABC-12`, `GH-41`).
pub fn is_key(s: &str) -> bool {
    let Some((head, num)) = s.split_once('-') else {
        return false;
    };
    let mut chars = head.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !num.is_empty()
        && num.chars().all(|c| c.is_ascii_digit())
}

/// The ticket a body's marker names, uppercased.
pub fn marked_ticket(body: &str) -> Option<String> {
    let at = body.find(MARKER_OPEN)?;
    let rest = &body[at + MARKER_OPEN.len()..];
    let key = rest.split(MARKER_CLOSE).next()?.trim();
    is_key(key).then(|| key.to_ascii_uppercase())
}

/// The ticket a plan task was written for: a marker in its description, or
/// the tracker key its title leads with (`ABC-12: Title`, `[ABC-12] Title`,
/// `ABC-12 Title`), the way a plan made from a ticket is titled.
pub fn named_ticket(title: &str, description: &str) -> Option<String> {
    if let Some(k) = marked_ticket(description) {
        return Some(k);
    }
    let t = title.trim().trim_start_matches('[');
    let word: String = t
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect();
    let after = t[word.len()..].chars().next();
    let ends = matches!(after, None | Some(':' | ']' | ' ' | '\u{a0}'));
    (is_key(&word) && ends).then(|| word.to_ascii_uppercase())
}

/// The key of a ticket named by `key`, seen from `repo` (`owner/name`): a
/// GitHub issue number is only unique within its repository.
fn key_in(key: &str, repo: &str) -> String {
    let key = key.trim().to_ascii_uppercase();
    match key.strip_prefix("GH-") {
        Some(n) if !repo.is_empty() => format!("github:{}#{n}", repo.to_ascii_lowercase()),
        _ => format!("ticket:{key}"),
    }
}

/// The canonical key of the ticket a run is for: the ticket a marker or a
/// stored link (`linked`) names, else the ticket that carried it in. Two
/// triggers for one ticket get one key, from any source.
pub fn canonical(
    source: &TicketSource,
    ticket_id: &str,
    repo: &str,
    body: &str,
    linked: Option<&str>,
) -> String {
    if let Some(k) = linked
        .map(str::trim)
        .filter(|k| is_key(k))
        .map(str::to_string)
        .or_else(|| marked_ticket(body))
    {
        return key_in(&k, repo);
    }
    match source {
        TicketSource::Alert => format!("alert:{}", ticket_id.trim()),
        _ if is_key(ticket_id.trim()) => key_in(ticket_id, repo),
        _ => format!("ticket:{}", ticket_id.trim()),
    }
}

/// The claim's sort key on the teams table.
pub fn claim_sk(canonical: &str) -> String {
    format!("TICKET_RUN#{canonical}")
}

/// A claim not yet backed by its run's record is a run still starting: it
/// holds for this long.
pub const STARTING_SECS: i64 = 300;

/// Who holds a ticket's claim, as the run table shows them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    pub run_id: String,
    /// The holder run's status; `None` when it has no record yet.
    pub status: Option<String>,
    /// Hours since the holder run's record last changed.
    pub idle_hours: i64,
    /// Seconds since the claim was taken.
    pub claim_secs: i64,
}

impl Holder {
    /// Whether the holder is still working the ticket: the same bounds as
    /// the run start's duplicate check, so a run that died can't hold a
    /// ticket forever.
    pub fn live(&self) -> bool {
        match self.status.as_deref() {
            None => self.claim_secs < STARTING_SECS,
            Some("running" | "queued") => self.idle_hours < 2,
            Some("awaiting_ci") => self.idle_hours < 26,
            Some(_) => false,
        }
    }
}

/// What a run starting does about a ticket's claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// Nobody held it: this run took it.
    Take,
    /// A live run holds it: this trigger is that run's ticket, and no
    /// second run starts.
    Held(String),
    /// A run that ended holds it: this run takes it over from that run id.
    TakeOver(String),
}

/// The decision for run `me` given the claim's holder.
pub fn decide(me: &str, holder: Option<&Holder>) -> Claim {
    match holder {
        None => Claim::Take,
        Some(h) if h.run_id == me => Claim::Take,
        Some(h) if h.live() => Claim::Held(h.run_id.clone()),
        Some(h) => Claim::TakeOver(h.run_id.clone()),
    }
}

/// The note on the live run when a second trigger for its ticket arrives.
pub fn held_note(ticket_id: &str, source: &TicketSource) -> String {
    let from = match source {
        TicketSource::Github => "a GitHub issue",
        TicketSource::Jira => "the tracker",
        TicketSource::Alert => "an alert",
    };
    format!(
        "The same ticket arrived again from {from} (`{ticket_id}`): this run is its one run, so no \
         second run or pull request was started."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_markers() {
        assert!(is_key("ABC-12") && is_key("GH-41") && is_key("abc-1"));
        assert!(!is_key("ABC") && !is_key("-12") && !is_key("ABC-") && !is_key("1A-2"));
        let body = format!("Do it.\n\n{}", marker("abc-12"));
        assert_eq!(marked_ticket(&body).as_deref(), Some("ABC-12"));
        assert_eq!(marked_ticket("no marker"), None);
    }

    #[test]
    fn a_plan_task_names_the_ticket_its_title_leads_with() {
        for t in ["ABC-12: Make it work", "[ABC-12] Make it", "ABC-12 make it"] {
            assert_eq!(named_ticket(t, "").as_deref(), Some("ABC-12"), "{t}");
        }
        assert_eq!(named_ticket("Make ABC-12 work", ""), None);
        assert_eq!(named_ticket("ABC-12x: no", ""), None);
        assert_eq!(
            named_ticket("Title", &marker("XY-3")).as_deref(),
            Some("XY-3")
        );
    }

    #[test]
    fn one_key_for_one_ticket_from_any_source() {
        let jira = canonical(&TicketSource::Jira, "ABC-12", "o/r", "", None);
        let issue = canonical(
            &TicketSource::Github,
            "GH-7",
            "o/r",
            &marker("ABC-12"),
            None,
        );
        let linked = canonical(&TicketSource::Github, "GH-7", "o/r", "", Some("abc-12"));
        assert_eq!(jira, "ticket:ABC-12");
        assert_eq!(issue, jira);
        assert_eq!(linked, jira);
        // GitHub numbers are per repository.
        assert_ne!(
            canonical(&TicketSource::Github, "GH-7", "o/a", "", None),
            canonical(&TicketSource::Github, "GH-7", "o/b", "", None)
        );
        assert_eq!(
            canonical(&TicketSource::Github, "GH-7", "O/R", "", None),
            "github:o/r#7"
        );
        assert_eq!(
            canonical(&TicketSource::Alert, "alarm-x", "o/r", "", None),
            "alert:alarm-x"
        );
    }

    #[test]
    fn a_live_holder_keeps_the_ticket_and_an_ended_one_hands_it_over() {
        let h = |status: Option<&str>, idle_hours, claim_secs| Holder {
            run_id: "R1".into(),
            status: status.map(str::to_string),
            idle_hours,
            claim_secs,
        };
        assert_eq!(decide("R2", None), Claim::Take);
        assert_eq!(decide("R1", Some(&h(Some("running"), 0, 9))), Claim::Take);
        for live in [
            h(Some("running"), 1, 9),
            h(Some("queued"), 0, 9),
            h(Some("awaiting_ci"), 25, 9),
            h(None, 0, 30),
        ] {
            assert_eq!(
                decide("R2", Some(&live)),
                Claim::Held("R1".into()),
                "{live:?}"
            );
        }
        for ended in [
            h(Some("failed"), 0, 9),
            h(Some("completed"), 0, 9),
            h(Some("running"), 3, 9),
            h(Some("awaiting_ci"), 27, 9),
            h(None, 0, STARTING_SECS + 1),
        ] {
            assert_eq!(
                decide("R2", Some(&ended)),
                Claim::TakeOver("R1".into()),
                "{ended:?}"
            );
        }
    }
}
