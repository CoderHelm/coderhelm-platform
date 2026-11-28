//! A knowledge base's sync health as Chat reads it: when its sources last
//! synced and whether one is failing, retrying or overdue now.
//!
//! A wiki source's sign-in server refused for most of a day, and every sync
//! of it stopped. Chat answered a question about that wiki from the copy
//! synced before, and its setup check listed the base by name only: nothing
//! in the answer said the source was failing or how old the copy was. The
//! Knowledge page showed both.
//!
//! [`of_base`] reads the same source records the Knowledge page reads, and
//! [`Health::line`] says it in one line. Chat puts that line on every
//! passage from the base (`answer::SourceRef::health`), on the knowledge
//! search's result, and in the setup check, so a search over a failing
//! source says so in the answer's sources.

use super::sources::{shown_status, Source, Status};

/// One source in trouble.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Trouble {
    /// The source's name on the Knowledge page.
    pub source: String,
    /// Since when (RFC 3339), when known.
    pub since: String,
    /// What the source's last sync said, and its fix.
    pub message: String,
    pub fix: String,
}

/// A base's sync health.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Health {
    pub kb_id: String,
    pub name: String,
    /// It has synced sources (not only uploads, not searched live).
    pub synced: bool,
    /// The newest successful sync of any of its sources (RFC 3339); empty
    /// when none has finished.
    pub last_good: String,
    /// Sources whose last sync stopped on something a person must fix.
    pub failing: Vec<Trouble>,
    /// Sources whose syncs stop for a passing reason and are tried again.
    pub retrying: Vec<Trouble>,
    /// Sources whose last good sync is older than three of their own
    /// sync intervals: their copy is behind their schedule.
    pub overdue: Vec<Trouble>,
    /// Sources a person paused.
    pub paused: Vec<String>,
}

fn secs_of(t: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(t)
        .ok()
        .map(|d| d.timestamp())
}

fn name_of(s: &Source) -> String {
    if s.name.trim().is_empty() {
        s.kind_key().to_string()
    } else {
        s.name.trim().to_string()
    }
}

/// The base's health from its sources at `now` (unix seconds).
pub fn of_base(kb_id: &str, name: &str, sources: &[Source], now: i64) -> Health {
    let mut h = Health {
        kb_id: kb_id.to_string(),
        name: name.to_string(),
        synced: !sources.is_empty(),
        ..Default::default()
    };
    for s in sources {
        let when = secs_of(&s.last_success_at);
        if when.is_some_and(|t| secs_of(&h.last_good).is_none_or(|g| t > g)) {
            h.last_good = s.last_success_at.clone();
        }
        if s.paused {
            h.paused.push(name_of(s));
            continue;
        }
        let status = shown_status(s, now);
        if status == Status::Error {
            let e = s.error.clone().unwrap_or_default();
            h.failing.push(Trouble {
                source: name_of(s),
                since: [
                    s.stopped_since.as_str(),
                    e.at.as_str(),
                    s.last_sync_at.as_str(),
                ]
                .into_iter()
                .find(|t| !t.is_empty())
                .unwrap_or("")
                .to_string(),
                message: e.message,
                fix: e.fix,
            });
            continue;
        }
        if let Some(e) = s.retrying.as_ref().filter(|_| s.stops > 0) {
            h.retrying.push(Trouble {
                source: name_of(s),
                since: if s.stopped_since.is_empty() {
                    e.at.clone()
                } else {
                    s.stopped_since.clone()
                },
                message: e.message.clone(),
                fix: e.fix.clone(),
            });
            continue;
        }
        let behind = when.is_some_and(|t| now - t > 3 * s.interval_mins() * 60);
        if behind && status != Status::Syncing {
            h.overdue.push(Trouble {
                source: name_of(s),
                since: s.last_success_at.clone(),
                ..Default::default()
            });
        }
    }
    h
}

impl Health {
    /// Something about the base makes its copy suspect now.
    pub fn in_trouble(&self) -> bool {
        !self.failing.is_empty() || !self.retrying.is_empty() || !self.overdue.is_empty()
    }

    /// One line: when it last synced, and each source in trouble with since
    /// when and what it said. Empty for a base with no synced sources.
    pub fn line(&self) -> String {
        if !self.synced {
            return String::new();
        }
        let mut parts = vec![if self.last_good.is_empty() {
            "no sync has finished yet".to_string()
        } else {
            format!("last good sync {}", self.last_good)
        }];
        let since = |t: &Trouble| {
            if t.since.is_empty() {
                String::new()
            } else {
                format!(" since {}", t.since)
            }
        };
        let said = |t: &Trouble| {
            [t.message.trim(), t.fix.trim()]
                .into_iter()
                .filter(|w| !w.is_empty())
                .collect::<Vec<_>>()
                .join(" ")
        };
        for t in &self.failing {
            parts.push(format!(
                "source {} failing{}: {}",
                t.source,
                since(t),
                said(t)
            ));
        }
        for t in &self.retrying {
            parts.push(format!(
                "source {} retrying after failed syncs{}: {}",
                t.source,
                since(t),
                said(t)
            ));
        }
        for t in &self.overdue {
            parts.push(format!(
                "source {} is behind its sync schedule (last good sync {})",
                t.source, t.since
            ));
        }
        for p in &self.paused {
            parts.push(format!("source {p} is paused"));
        }
        let lead = if self.in_trouble() {
            "NOT CURRENT"
        } else {
            "current"
        };
        format!("{lead}: {}", parts.join("; "))
    }

    /// What the answer says first when it read this base while in trouble.
    pub fn warning(&self) -> Option<String> {
        self.in_trouble().then(|| {
            format!(
                "Knowledge base {} is not current ({}). Say so in the answer's first line, with \
                 since when, before anything it holds.",
                self.name,
                self.line()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::sources::{SourceError, Status};
    use super::*;

    const NOW: &str = "2026-03-10T12:00:00Z";

    fn now() -> i64 {
        secs_of(NOW).unwrap()
    }

    fn src(name: &str, last_good: &str) -> Source {
        Source {
            id: format!("src-{name}"),
            kb_id: "kb1".into(),
            name: name.into(),
            status: Status::Ready,
            last_success_at: last_good.into(),
            last_sync_at: last_good.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_healthy_base_says_when_it_last_synced() {
        let h = of_base(
            "kb1",
            "Northwind wiki",
            &[src("Wiki", "2026-03-10T11:55:00Z")],
            now(),
        );
        assert!(!h.in_trouble());
        assert_eq!(h.line(), "current: last good sync 2026-03-10T11:55:00Z");
        assert!(h.warning().is_none());
    }

    #[test]
    fn a_failing_source_says_since_when_and_its_fix() {
        let mut s = src("Wiki", "2026-03-09T22:00:00Z");
        s.status = Status::Error;
        s.stopped_since = "2026-03-09T22:30:00Z".into();
        s.error = Some(SourceError {
            message: "The sign-in was refused.".into(),
            fix: "Reconnect to sign in again.".into(),
            action: "reconnect".into(),
            at: "2026-03-10T11:50:00Z".into(),
        });
        let h = of_base("kb1", "Northwind wiki", &[s], now());
        assert!(h.in_trouble());
        let l = h.line();
        assert!(
            l.starts_with("NOT CURRENT: last good sync 2026-03-09T22:00:00Z"),
            "{l}"
        );
        assert!(
            l.contains("source Wiki failing since 2026-03-09T22:30:00Z"),
            "{l}"
        );
        assert!(l.contains("Reconnect to sign in again."), "{l}");
        assert!(h.warning().unwrap().contains("first line"));
    }

    #[test]
    fn a_retrying_source_is_not_current_either() {
        let mut s = src("Wiki", "2026-03-09T22:00:00Z");
        s.stops = 4;
        s.stopped_since = "2026-03-09T23:00:00Z".into();
        s.retrying = Some(SourceError {
            message: "The sign-in server was briefly unavailable.".into(),
            action: "retry".into(),
            ..Default::default()
        });
        let h = of_base("kb1", "Northwind wiki", &[s], now());
        assert_eq!(h.retrying.len(), 1);
        assert!(h
            .line()
            .contains("retrying after failed syncs since 2026-03-09T23:00:00Z"));
    }

    #[test]
    fn a_source_far_behind_its_own_schedule_is_overdue() {
        let h = of_base(
            "kb1",
            "Northwind wiki",
            &[src("Wiki", "2026-03-08T12:00:00Z")],
            now(),
        );
        assert_eq!(h.overdue.len(), 1);
        assert!(h.line().contains("behind its sync schedule"));
        // Paused is said, never overdue.
        let mut p = src("Wiki", "2026-03-01T12:00:00Z");
        p.paused = true;
        let h = of_base("kb1", "Northwind wiki", &[p], now());
        assert!(!h.in_trouble());
        assert!(h.line().contains("source Wiki is paused"));
    }

    #[test]
    fn uploads_only_have_nothing_to_say() {
        let h = of_base("kb1", "Harbor uploads", &[], now());
        assert_eq!(h.line(), "");
        assert!(!h.in_trouble());
    }
}
