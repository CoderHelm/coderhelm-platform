//! The hosted side's check that every `{name}.coderhelm.com` still points
//! only where CoderHelm put it, and still reaches its own install.
//!
//! Every 15 minutes the hosted worker reads the zone and, for each claimed
//! name, compares the records with what CoderHelm stored, then asks for an
//! ownership proof twice: once through the install's recorded CloudFront
//! address and once through the name. This module decides; the worker
//! (worker/src/passes/hybrid_name_check.rs) reads and writes.
//!
//! What it guards against: a name left pointing at a CloudFront address its
//! install no longer owns (a stack deleted without leaving hybrid, a
//! distribution replaced), and any record under a claimed name that CoderHelm
//! didn't write. Such a name is parked: it goes back to the hosted "being set
//! up" page. The name stays reserved for its install; nothing is deleted
//! that would let someone else take it.
//!
//! A network failure (DNS, connection, timeout) doesn't count as a wrong
//! answer: only an answer that is wrong does, [`PARK_AFTER`] times in a row.
//! A name whose address hasn't answered at all for a whole day
//! ([`UNREACHABLE_PARK_AFTER`] runs) is parked too: by then it isn't a blip,
//! the CloudFront address is gone.

use super::names::{self, NameRecord};

/// Wrong answers in a row before a name is parked (15 minutes apart: about
/// 45 minutes).
pub const PARK_AFTER: u32 = 3;

/// Runs in a row with no answer at all before a name is parked (a day).
pub const UNREACHABLE_PARK_AFTER: u32 = 96;

/// One record as Route 53 lists it, reduced to what the check compares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneRecord {
    /// Lowercase, no trailing dot.
    pub name: String,
    /// `CNAME`, `A`, `AAAA`, `TXT`, ...
    pub rtype: String,
    /// The first value of a plain record (lowercase, no trailing dot).
    pub value: Option<String>,
    /// An alias record's target (lowercase, no trailing dot).
    pub alias: Option<String>,
}

fn bare(s: &str) -> String {
    s.trim().trim_end_matches('.').to_ascii_lowercase()
}

impl ZoneRecord {
    pub fn new(name: &str, rtype: &str, value: Option<&str>, alias: Option<&str>) -> Self {
        Self {
            name: bare(name),
            rtype: rtype.to_ascii_uppercase(),
            value: value.map(bare),
            alias: alias.map(bare),
        }
    }

    /// A type the hosted side may write (and so may also remove).
    pub fn writable(&self) -> bool {
        matches!(self.rtype.as_str(), "CNAME" | "A" | "AAAA")
    }

    fn is_fallback(&self, fallback: &str) -> bool {
        matches!(self.rtype.as_str(), "A" | "AAAA")
            && self.alias.as_deref() == Some(bare(fallback).as_str())
    }
}

/// The claimed name a record sits under: `<name>.<zone>` or
/// `_<token>.<name>.<zone>`. None for the zone apex, the wildcard, anything
/// deeper, or outside the zone.
pub fn owner(record: &str, zone: &str) -> Option<String> {
    let r = bare(record);
    let rest = r.strip_suffix(&format!(".{}", bare(zone)))?;
    let labels: Vec<&str> = rest.split('.').collect();
    let label = match labels.as_slice() {
        [n] => *n,
        [t, n] if t.starts_with('_') && !t.starts_with("_acme-challenge") => *n,
        _ => return None,
    };
    // Only a label a name can be (Route 53 lists the wildcard as `\052`).
    let name_like = !label.is_empty()
        && label
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !name_like {
        return None;
    }
    Some(label.to_string())
}

/// What the zone holds for one claimed name, against what CoderHelm stored.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Records {
    /// Records under the name that CoderHelm didn't write and may remove
    /// (CNAME, A, AAAA): indexes into the listed records.
    pub foreign: Vec<usize>,
    /// Records under the name of a type CoderHelm can't change (TXT, MX,
    /// ...): reported to CoderHelm's operators, never touched.
    pub untouchable: Vec<usize>,
    /// The address CNAME's index, when it points where CoderHelm stored.
    pub address: Option<usize>,
    /// The fallback alias records' indexes (A and AAAA).
    pub fallback: Vec<usize>,
    /// The address record points at a CloudFront address other than the
    /// install's recorded one.
    pub wrong_target: bool,
}

impl Records {
    /// Nothing under the name that CoderHelm didn't write.
    pub fn clean(&self) -> bool {
        self.foreign.is_empty() && !self.wrong_target
    }
}

/// Sorts the zone's records under `name` into what CoderHelm wrote and what
/// it didn't. `stored` and `pending` are the name's stored record list and
/// whether the fallback alias is meant to be in place; `cloudfront` is the
/// install's recorded CloudFront address.
pub fn sort_records(
    name: &str,
    zone: &str,
    stored: &[NameRecord],
    pending: bool,
    cloudfront: &str,
    fallback: &str,
    listed: &[ZoneRecord],
) -> Records {
    let host = names::hostname(name);
    let mut out = Records::default();
    for (i, r) in listed.iter().enumerate() {
        if owner(&r.name, zone).as_deref() != Some(name) {
            continue;
        }
        if !r.writable() {
            out.untouchable.push(i);
            continue;
        }
        if r.name == host && r.is_fallback(fallback) {
            if pending {
                out.fallback.push(i);
            } else {
                out.foreign.push(i);
            }
            continue;
        }
        let ours = r.rtype == "CNAME"
            && stored.iter().any(|s| {
                bare(&s.name) == r.name && r.value.as_deref() == Some(bare(&s.value).as_str())
            });
        if !ours {
            out.foreign.push(i);
            continue;
        }
        if r.name == host {
            if r.value.as_deref() == Some(bare(cloudfront).as_str()) {
                out.address = Some(i);
            } else {
                out.wrong_target = true;
                out.foreign.push(i);
            }
        }
    }
    out
}

/// How an ownership proof went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proof {
    /// The install answered with its own id and a right proof.
    Proven,
    /// Something answered, and it wasn't this install's right proof (another
    /// install, a wrong proof, an HTTP error page): counts toward parking.
    Wrong,
    /// No answer (DNS, connection, timeout): never counts.
    Unreachable,
}

/// Why a name is parked (stored, shown to the install's admins and in hosted
/// admin, in plain words).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkReason {
    /// A record under the name that CoderHelm didn't write.
    ForeignRecord,
    /// The name's install is gone, revoked or no longer verified.
    NoInstall,
    /// The recorded CloudFront address no longer answers for the install.
    OriginGone,
    /// The name, after working, no longer reaches its install.
    NameGone,
}

impl ParkReason {
    pub fn code(self) -> &'static str {
        match self {
            Self::ForeignRecord => "foreign_record",
            Self::NoInstall => "no_install",
            Self::OriginGone => "origin_gone",
            Self::NameGone => "name_gone",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::ForeignRecord => "A DNS record under this address wasn't written by CoderHelm, so CoderHelm put the address back on its holding page.",
            Self::NoInstall => "This address's install is no longer registered with CoderHelm, so CoderHelm put the address back on its holding page.",
            Self::OriginGone => "The CloudFront address this name pointed at stopped answering as this install, so CoderHelm put the address back on its holding page. Settings, System, Domain sets it up again.",
            Self::NameGone => "This address stopped reaching this install, so CoderHelm put it back on its holding page. Settings, System, Domain sets it up again.",
        }
    }
}

/// The name's check history, as stored on its reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct History {
    /// Wrong answers in a row.
    pub failures: u32,
    /// The name has answered as its install at least once.
    pub proven_before: bool,
    /// Runs in a row where the address didn't answer at all.
    pub unreachable: u32,
}

/// What one run decides for one name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub history: History,
    /// Park now, and why.
    pub park: Option<ParkReason>,
    /// A short state for the admin views: `ok`, `setting_up`, `waiting`,
    /// `failing`, `unreachable`, `parked`.
    pub state: &'static str,
}

/// Decides for one claimed name. `install_ok` is false when the name's
/// install is missing, revoked or unverified. `origin` is the proof through
/// the recorded CloudFront address and `via_name` through the name; both are
/// None while the name is still on the fallback (nothing to prove yet).
pub fn decide(
    prev: History,
    install_ok: bool,
    records: &Records,
    origin: Option<Proof>,
    via_name: Option<Proof>,
) -> Decision {
    let park = |reason, history| Decision {
        history,
        park: Some(reason),
        state: "parked",
    };
    let reset = History {
        failures: 0,
        proven_before: prev.proven_before,
        unreachable: 0,
    };
    if !install_ok {
        return park(ParkReason::NoInstall, reset);
    }
    if !records.clean() {
        // Written outside CoderHelm: back to what CoderHelm knows, now.
        return park(ParkReason::ForeignRecord, reset);
    }
    let (Some(origin), Some(via_name)) = (origin, via_name) else {
        return Decision {
            history: reset,
            park: None,
            state: "setting_up",
        };
    };
    let strike = |reason: ParkReason| {
        let history = History {
            failures: prev.failures + 1,
            proven_before: prev.proven_before,
            unreachable: 0,
        };
        if history.failures >= PARK_AFTER {
            park(
                reason,
                History {
                    failures: 0,
                    ..history
                },
            )
        } else {
            Decision {
                history,
                park: None,
                state: "failing",
            }
        }
    };
    match (origin, via_name) {
        (Proof::Proven, Proof::Proven) => Decision {
            history: History {
                failures: 0,
                proven_before: true,
                unreachable: 0,
            },
            park: None,
            state: "ok",
        },
        // The address the name points at answers as someone else, or as nobody.
        (Proof::Wrong, _) => strike(ParkReason::OriginGone),
        // The install is there, the name doesn't reach it: dangerous only
        // once it worked (before that, the install hasn't moved onto the
        // name yet; the domain step shows it).
        (Proof::Proven, Proof::Wrong) if prev.proven_before => strike(ParkReason::NameGone),
        (Proof::Proven, Proof::Wrong) => Decision {
            history: reset,
            park: None,
            state: "waiting",
        },
        // No answer at all: not a wrong answer, and no reset. Only a whole
        // day of it parks the name (the address is gone, not blipping).
        (Proof::Unreachable, _) | (_, Proof::Unreachable) => {
            let history = History {
                unreachable: prev.unreachable + 1,
                ..prev
            };
            if history.unreachable >= UNREACHABLE_PARK_AFTER {
                park(ParkReason::OriginGone, reset)
            } else {
                Decision {
                    history,
                    park: None,
                    state: "unreachable",
                }
            }
        }
    }
}

/// What parking changes in the zone: the listed records to delete (the
/// address and anything CoderHelm didn't write) and whether to add the
/// fallback alias (A and AAAA). Every deletion is under the name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkPlan {
    pub delete: Vec<usize>,
    pub add_fallback: bool,
}

pub fn plan_park(name: &str, zone: &str, records: &Records, listed: &[ZoneRecord]) -> ParkPlan {
    let mut delete: Vec<usize> = records.foreign.clone();
    if let Some(a) = records.address {
        delete.push(a);
    }
    delete.sort_unstable();
    delete.dedup();
    delete.retain(|i| {
        listed
            .get(*i)
            .is_some_and(|r| r.writable() && owner(&r.name, zone).as_deref() == Some(name))
    });
    let host = names::hostname(name);
    // The holding page answers through the zone's wildcard too, but an
    // explicit alias keeps the name's record CoderHelm's own.
    let add_fallback = records.fallback.is_empty()
        || delete
            .iter()
            .any(|i| listed[*i].name == host && listed[*i].rtype != "CNAME");
    ParkPlan {
        delete,
        add_fallback,
    }
}

/// The stored record list once parked: the address goes, the certificate's
/// validation record stays (ACM renews through it).
pub fn stored_after_park(stored: &[NameRecord]) -> Vec<NameRecord> {
    stored
        .iter()
        .filter(|r| r.purpose != "address")
        .cloned()
        .collect()
}

/// Records in the zone that sit under no claimed name and match a shape the
/// hosted side writes (an address CNAME to CloudFront, an ACM validation
/// CNAME, the fallback alias): left behind by a name released without its
/// records. Returns their indexes. Anything else under an unclaimed name is
/// reported, never removed (see [`unknown`]).
pub fn orphans(
    zone: &str,
    claimed: &dyn Fn(&str) -> bool,
    fallback: &str,
    listed: &[ZoneRecord],
) -> Vec<usize> {
    let reserved = names::reserved();
    listed
        .iter()
        .enumerate()
        .filter(|(_, r)| {
            let Some(label) = owner(&r.name, zone) else {
                return false;
            };
            if reserved.iter().any(|n| n == &label) || claimed(&label) {
                return false;
            }
            let host = format!("{label}.{}", bare(zone));
            match r.rtype.as_str() {
                "CNAME" if r.name == host => {
                    r.value.as_deref().is_some_and(names::is_cloudfront_domain)
                }
                "CNAME" => r
                    .value
                    .as_deref()
                    .is_some_and(|v| v.ends_with(".acm-validations.aws")),
                "A" | "AAAA" => r.name == host && r.is_fallback(fallback),
                _ => false,
            }
        })
        .map(|(i, _)| i)
        .collect()
}

/// Records under unclaimed, unreserved names that don't match a shape the
/// hosted side writes: reported to CoderHelm's operators only.
pub fn unknown(
    zone: &str,
    claimed: &dyn Fn(&str) -> bool,
    fallback: &str,
    listed: &[ZoneRecord],
) -> Vec<usize> {
    let gone = orphans(zone, claimed, fallback, listed);
    let reserved = names::reserved();
    listed
        .iter()
        .enumerate()
        .filter(|(i, r)| {
            !gone.contains(i)
                && owner(&r.name, zone)
                    .is_some_and(|l| !reserved.iter().any(|n| n == &l) && !claimed(&l))
        })
        .map(|(i, _)| i)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZONE: &str = "coderhelm.com";
    const FALLBACK: &str = "dfallback1234.cloudfront.net";
    const CF: &str = "d1abcdef234567.cloudfront.net";
    const CERT_NAME: &str = "_0123456789abcdef0123456789abcdef.acme.coderhelm.com";
    const CERT_VALUE: &str = "_fedcba9876543210fedcba9876543210.xlfgrmvvlj.acm-validations.aws";

    fn stored() -> Vec<NameRecord> {
        vec![
            NameRecord {
                purpose: "certificate".into(),
                name: CERT_NAME.into(),
                rtype: "CNAME".into(),
                value: CERT_VALUE.into(),
            },
            NameRecord {
                purpose: "address".into(),
                name: "acme.coderhelm.com".into(),
                rtype: "CNAME".into(),
                value: CF.into(),
            },
        ]
    }

    fn live() -> Vec<ZoneRecord> {
        vec![
            ZoneRecord::new("coderhelm.com.", "A", None, Some("dsite.cloudfront.net.")),
            ZoneRecord::new(
                "app.coderhelm.com.",
                "CNAME",
                Some("dapp.cloudfront.net."),
                None,
            ),
            ZoneRecord::new(CERT_NAME, "CNAME", Some(CERT_VALUE), None),
            ZoneRecord::new(
                "acme.coderhelm.com.",
                "CNAME",
                Some(&format!("{CF}.")),
                None,
            ),
        ]
    }

    fn sort(listed: &[ZoneRecord], pending: bool, cf: &str) -> Records {
        sort_records("acme", ZONE, &stored(), pending, cf, FALLBACK, listed)
    }

    #[test]
    fn owner_is_the_name_or_its_token_records_only() {
        assert_eq!(owner("acme.coderhelm.com.", ZONE).as_deref(), Some("acme"));
        assert_eq!(owner(CERT_NAME, ZONE).as_deref(), Some("acme"));
        assert_eq!(owner("coderhelm.com", ZONE), None);
        assert_eq!(owner("\\052.coderhelm.com", ZONE), None);
        assert_eq!(owner("*.coderhelm.com", ZONE), None);
        assert_eq!(owner("a.b.coderhelm.com", ZONE), None);
        assert_eq!(owner("_acme-challenge.acme.coderhelm.com", ZONE), None);
        assert_eq!(owner("acme.example.org", ZONE), None);
    }

    #[test]
    fn a_matching_name_passes_and_resets() {
        let r = sort(&live(), false, CF);
        assert!(r.clean());
        assert_eq!(r.address, Some(3));
        let prev = History {
            failures: 2,
            proven_before: false,
            unreachable: 5,
        };
        let d = decide(prev, true, &r, Some(Proof::Proven), Some(Proof::Proven));
        assert_eq!(d.park, None);
        assert_eq!(d.state, "ok");
        assert_eq!(
            d.history,
            History {
                failures: 0,
                proven_before: true,
                unreachable: 0
            }
        );
    }

    #[test]
    fn a_wrong_target_is_parked_after_three_wrong_answers() {
        let r = sort(&live(), false, CF);
        let mut h = History {
            failures: 0,
            proven_before: true,
            unreachable: 0,
        };
        for n in 1..PARK_AFTER {
            let d = decide(h, true, &r, Some(Proof::Wrong), Some(Proof::Wrong));
            assert_eq!(d.park, None, "not after {n}");
            assert_eq!(d.history.failures, n);
            h = d.history;
        }
        let d = decide(h, true, &r, Some(Proof::Wrong), Some(Proof::Proven));
        assert_eq!(d.park, Some(ParkReason::OriginGone));
        assert_eq!(d.history.failures, 0);
    }

    #[test]
    fn network_failures_dont_count_as_wrong_answers() {
        let r = sort(&live(), false, CF);
        let h = History {
            failures: 2,
            proven_before: true,
            unreachable: 0,
        };
        for (o, n) in [
            (Proof::Unreachable, Proof::Unreachable),
            (Proof::Proven, Proof::Unreachable),
            (Proof::Unreachable, Proof::Proven),
        ] {
            let d = decide(h, true, &r, Some(o), Some(n));
            assert_eq!(d.park, None);
            assert_eq!(d.history.failures, 2, "neither counted nor reset");
            assert_eq!(d.history.unreachable, 1);
            assert_eq!(d.state, "unreachable");
        }
    }

    #[test]
    fn only_a_whole_day_without_any_answer_parks() {
        let r = sort(&live(), false, CF);
        let mut h = History::default();
        for _ in 1..UNREACHABLE_PARK_AFTER {
            let d = decide(
                h,
                true,
                &r,
                Some(Proof::Unreachable),
                Some(Proof::Unreachable),
            );
            assert_eq!(d.park, None);
            h = d.history;
        }
        let d = decide(
            h,
            true,
            &r,
            Some(Proof::Unreachable),
            Some(Proof::Unreachable),
        );
        assert_eq!(d.park, Some(ParkReason::OriginGone));
        // One answer in between starts the day over.
        let h = History {
            unreachable: UNREACHABLE_PARK_AFTER - 1,
            ..History::default()
        };
        let d = decide(h, true, &r, Some(Proof::Proven), Some(Proof::Proven));
        assert_eq!(d.history.unreachable, 0);
    }

    #[test]
    fn a_name_not_moved_onto_yet_waits_instead_of_failing() {
        let r = sort(&live(), false, CF);
        let h = History {
            failures: 0,
            proven_before: false,
            unreachable: 0,
        };
        for _ in 0..10 {
            let d = decide(h, true, &r, Some(Proof::Proven), Some(Proof::Wrong));
            assert_eq!(d.park, None);
            assert_eq!(d.state, "waiting");
        }
        // Once it worked, the same answer counts.
        let h = History {
            failures: PARK_AFTER - 1,
            proven_before: true,
            unreachable: 0,
        };
        let d = decide(h, true, &r, Some(Proof::Proven), Some(Proof::Wrong));
        assert_eq!(d.park, Some(ParkReason::NameGone));
    }

    #[test]
    fn a_record_coderhelm_did_not_write_parks_at_once() {
        let mut listed = live();
        listed.push(ZoneRecord::new(
            "acme.coderhelm.com",
            "A",
            Some("203.0.113.9"),
            None,
        ));
        let r = sort(&listed, false, CF);
        assert!(!r.clean());
        assert_eq!(r.foreign, vec![4]);
        let d = decide(
            History::default(),
            true,
            &r,
            Some(Proof::Proven),
            Some(Proof::Proven),
        );
        assert_eq!(d.park, Some(ParkReason::ForeignRecord));
        let p = plan_park("acme", ZONE, &r, &listed);
        assert_eq!(p.delete, vec![3, 4]);
        assert!(p.add_fallback);
    }

    #[test]
    fn an_address_changed_outside_coderhelm_is_foreign() {
        let mut listed = live();
        listed[3] = ZoneRecord::new(
            "acme.coderhelm.com",
            "CNAME",
            Some("devil9876543.cloudfront.net"),
            None,
        );
        let r = sort(&listed, false, CF);
        assert!(!r.clean());
        assert_eq!(r.address, None);
        assert_eq!(r.foreign, vec![3]);
    }

    #[test]
    fn a_recorded_cloudfront_that_moved_is_a_wrong_target() {
        // The stored record points at CF, but the install now reports another distribution.
        let r = sort(&live(), false, "dnewdist12345.cloudfront.net");
        assert!(r.wrong_target);
        assert!(!r.clean());
    }

    #[test]
    fn other_record_types_are_reported_not_touched() {
        let mut listed = live();
        listed.push(ZoneRecord::new(
            "acme.coderhelm.com",
            "TXT",
            Some("\"hello\""),
            None,
        ));
        let r = sort(&listed, false, CF);
        assert!(r.clean());
        assert_eq!(r.untouchable, vec![4]);
        let p = plan_park("acme", ZONE, &r, &listed);
        assert!(!p.delete.contains(&4));
    }

    #[test]
    fn a_missing_install_parks_and_keeps_the_certificate_record() {
        let r = sort(&live(), false, CF);
        let d = decide(History::default(), false, &r, None, None);
        assert_eq!(d.park, Some(ParkReason::NoInstall));
        let p = plan_park("acme", ZONE, &r, &live());
        assert_eq!(
            p.delete,
            vec![3],
            "the address only; the reservation and certificate record stay"
        );
        let kept = stored_after_park(&stored());
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].purpose, "certificate");
    }

    #[test]
    fn a_name_on_the_fallback_is_setting_up() {
        let listed = vec![
            ZoneRecord::new("acme.coderhelm.com", "A", None, Some(FALLBACK)),
            ZoneRecord::new("acme.coderhelm.com", "AAAA", None, Some(FALLBACK)),
        ];
        let r = sort_records("acme", ZONE, &[], true, CF, FALLBACK, &listed);
        assert!(r.clean());
        assert_eq!(r.fallback, vec![0, 1]);
        let d = decide(History::default(), true, &r, None, None);
        assert_eq!(d.park, None);
        assert_eq!(d.state, "setting_up");
        // The same alias when the name isn't meant to be pending is foreign.
        let r = sort_records("acme", ZONE, &[], false, CF, FALLBACK, &listed);
        assert_eq!(r.foreign, vec![0, 1]);
    }

    #[test]
    fn plans_never_touch_other_names() {
        let mut listed = live();
        listed.push(ZoneRecord::new(
            "other.coderhelm.com",
            "A",
            Some("203.0.113.9"),
            None,
        ));
        let mut r = sort(&listed, false, CF);
        // Even if an index outside the name slipped in, the plan drops it.
        r.foreign.push(1);
        r.foreign.push(4);
        let p = plan_park("acme", ZONE, &r, &listed);
        assert_eq!(p.delete, vec![3]);
    }

    #[test]
    fn orphans_are_only_shapes_coderhelm_writes_under_unclaimed_names() {
        let listed = vec![
            ZoneRecord::new(
                "gone.coderhelm.com",
                "CNAME",
                Some("d9876543210.cloudfront.net"),
                None,
            ),
            ZoneRecord::new(
                "_0123456789abcdef0123456789abcdef.gone.coderhelm.com",
                "CNAME",
                Some(CERT_VALUE),
                None,
            ),
            ZoneRecord::new("gone2.coderhelm.com", "A", None, Some(FALLBACK)),
            ZoneRecord::new(
                "app.coderhelm.com",
                "CNAME",
                Some("dapp1234567.cloudfront.net"),
                None,
            ),
            ZoneRecord::new("acme.coderhelm.com", "CNAME", Some(CF), None),
            ZoneRecord::new("mystery.coderhelm.com", "A", Some("203.0.113.9"), None),
            ZoneRecord::new("coderhelm.com", "MX", Some("1 mx.example.org"), None),
            ZoneRecord::new("*.coderhelm.com", "A", None, Some(FALLBACK)),
        ];
        let claimed = |n: &str| n == "acme";
        assert_eq!(orphans(ZONE, &claimed, FALLBACK, &listed), vec![0, 1, 2]);
        assert_eq!(unknown(ZONE, &claimed, FALLBACK, &listed), vec![5]);
    }

    #[test]
    fn reasons_have_codes_and_plain_messages() {
        for r in [
            ParkReason::ForeignRecord,
            ParkReason::NoInstall,
            ParkReason::OriginGone,
            ParkReason::NameGone,
        ] {
            assert!(!r.code().is_empty());
            assert!(r.message().ends_with('.'));
            assert!(!r.message().contains('→'));
        }
    }
}
