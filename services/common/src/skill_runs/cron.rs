//! A schedule's cron line: the five fields people write (`23 7 * * 1` is
//! Mondays at 07:23), read in the team's time zone.
//!
//! - [`parse`] checks a line and keeps each field's values.
//! - [`Cron::eventbridge`] is the same schedule as EventBridge Scheduler's
//!   `cron(...)` expression, which runs it in the time zone given next to it
//!   (so a 07:23 schedule stays 07:23 across daylight saving).
//! - [`Cron::next_after`] is the next time it fires, for the dashboard.
//!
//! Supported in each field: `*`, a number, `a-b`, `*/n`, `a-b/n` and lists of
//! those; month and weekday names (`JAN`, `MON`). EventBridge can't run a
//! schedule that names both a day of the month and a weekday, so neither
//! does this.

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use chrono_tz::Tz;

/// One field: every value it allows, and whether it was `*`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    values: Vec<u32>,
    any: bool,
    /// The field as written, upper-cased, for EventBridge.
    text: String,
}

/// A checked cron line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cron {
    minute: Field,
    hour: Field,
    day: Field,
    month: Field,
    /// 0 = Sunday .. 6 = Saturday (7 is read as Sunday).
    weekday: Field,
}

const MONTHS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const DAYS: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

fn name_value(s: &str, names: &[&str], first: u32) -> Option<u32> {
    names
        .iter()
        .position(|n| n.eq_ignore_ascii_case(s))
        .map(|i| i as u32 + first)
}

fn value(s: &str, lo: u32, hi: u32, names: &[&str], first: u32) -> Result<u32, String> {
    let v = match s.parse::<u32>() {
        Ok(v) => v,
        Err(_) => name_value(s, names, first).ok_or_else(|| format!("'{s}' isn't a value"))?,
    };
    if v < lo || v > hi {
        return Err(format!("{v} is outside {lo} to {hi}"));
    }
    Ok(v)
}

fn field(raw: &str, lo: u32, hi: u32, names: &[&str], first: u32) -> Result<Field, String> {
    let mut values = Vec::new();
    for part in raw.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => {
                let s: u32 = s
                    .parse()
                    .map_err(|_| format!("'{part}' has a step that isn't a number"))?;
                if s == 0 {
                    return Err(format!("'{part}' has a step of 0"));
                }
                (r, s)
            }
            None => (part, 1),
        };
        let (a, b) = if range == "*" {
            (lo, hi)
        } else if let Some((a, b)) = range.split_once('-') {
            (
                value(a, lo, hi, names, first)?,
                value(b, lo, hi, names, first)?,
            )
        } else {
            let v = value(range, lo, hi, names, first)?;
            (v, if part.contains('/') { hi } else { v })
        };
        if a > b {
            return Err(format!("'{part}' runs backwards"));
        }
        let mut v = a;
        while v <= b {
            values.push(v);
            v += step;
        }
    }
    values.sort_unstable();
    values.dedup();
    Ok(Field {
        values,
        any: raw == "*",
        text: raw.to_ascii_uppercase(),
    })
}

/// Reads a five-field cron line, or says what is wrong with it.
pub fn parse(line: &str) -> Result<Cron, String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() != 5 {
        return Err(
            "Write five fields: minute hour day-of-month month weekday, like 23 7 * * MON.".into(),
        );
    }
    let at = |i: usize, what: &str, r: Result<Field, String>| {
        r.map_err(|e| format!("The {what} field ('{}'): {e}.", parts[i]))
    };
    let minute = at(0, "minute", field(parts[0], 0, 59, &[], 0))?;
    let hour = at(1, "hour", field(parts[1], 0, 23, &[], 0))?;
    let day = at(2, "day-of-month", field(parts[2], 1, 31, &[], 1))?;
    let month = at(3, "month", field(parts[3], 1, 12, &MONTHS, 1))?;
    let mut weekday = at(4, "weekday", field(parts[4], 0, 7, &DAYS, 0))?;
    if weekday.values.contains(&7) {
        weekday.values.retain(|v| *v != 7);
        if !weekday.values.contains(&0) {
            weekday.values.insert(0, 0);
        }
    }
    if !day.any && !weekday.any {
        return Err(
            "Name a day of the month or a weekday, not both (leave the other as *).".into(),
        );
    }
    if hour.any && minute.values.windows(2).any(|w| w[1] - w[0] < 5) {
        return Err("A schedule runs at most every 5 minutes.".into());
    }
    Ok(Cron {
        minute,
        hour,
        day,
        month,
        weekday,
    })
}

/// A weekday field in EventBridge's numbering (1 = Sunday .. 7 = Saturday),
/// written with names so nothing is off by one.
fn eventbridge_weekday(f: &Field) -> String {
    let names: Vec<&str> = f.values.iter().map(|v| DAYS[*v as usize % 7]).collect();
    names.join(",")
}

impl Cron {
    /// EventBridge Scheduler's `cron(minutes hours day month weekday year)`.
    pub fn eventbridge(&self) -> String {
        let (day, weekday) = if self.weekday.any {
            (self.day.text.clone(), "?".to_string())
        } else {
            ("?".to_string(), eventbridge_weekday(&self.weekday))
        };
        format!(
            "cron({} {} {} {} {} *)",
            self.minute.text, self.hour.text, day, self.month.text, weekday
        )
    }

    fn matches_day(&self, d: chrono::NaiveDate) -> bool {
        self.month.values.contains(&d.month())
            && self.day.values.contains(&d.day())
            && self
                .weekday
                .values
                .contains(&d.weekday().num_days_from_sunday())
    }

    /// The first time after `after` the schedule fires, in `tz`. `None` when
    /// it never does within a year (31 February).
    pub fn next_after(&self, tz: Tz, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let local = after.with_timezone(&tz).naive_local();
        let mut day = local.date();
        for _ in 0..400 {
            if self.matches_day(day) {
                for h in &self.hour.values {
                    for m in &self.minute.values {
                        let naive = day.and_hms_opt(*h, *m, 0)?;
                        // A time skipped by daylight saving doesn't fire.
                        let Some(t) = tz.from_local_datetime(&naive).earliest() else {
                            continue;
                        };
                        let t = t.with_timezone(&Utc);
                        if t > after {
                            return Some(t);
                        }
                    }
                }
            }
            day += Duration::days(1);
        }
        None
    }
}

/// A time zone people pick (`America/New_York`, `UTC`), or why it isn't one.
pub fn time_zone(name: &str) -> Result<Tz, String> {
    name.trim()
        .parse::<Tz>()
        .map_err(|_| format!("'{name}' isn't a time zone; use a name like America/New_York."))
}

/// `t` as the team reads it: `Mon 30 Nov 07:23 EST`.
pub fn local_label(t: DateTime<Utc>, tz: Tz) -> String {
    let l = t.with_timezone(&tz);
    format!(
        "{} {:02}:{:02} {}",
        l.format("%a %-d %b"),
        l.hour(),
        l.minute(),
        l.format("%Z")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_weekly_line_becomes_the_same_eventbridge_schedule() {
        let c = parse("23 7 * * 1").unwrap();
        assert_eq!(c.eventbridge(), "cron(23 7 ? * MON *)");
        assert_eq!(
            parse("0 6 1 * *").unwrap().eventbridge(),
            "cron(0 6 1 * ? *)"
        );
        assert_eq!(
            parse("30 9 * * mon-fri").unwrap().eventbridge(),
            "cron(30 9 ? * MON,TUE,WED,THU,FRI *)"
        );
        // 0 and 7 are both Sunday.
        assert_eq!(
            parse("0 0 * * 7").unwrap().eventbridge(),
            "cron(0 0 ? * SUN *)"
        );
        assert_eq!(
            parse("0 0 * * 0,7").unwrap().eventbridge(),
            "cron(0 0 ? * SUN *)"
        );
    }

    #[test]
    fn lines_people_get_wrong_are_refused_with_why() {
        for (bad, says) in [
            ("* * * *", "five fields"),
            ("60 * * * *", "minute"),
            ("0 24 * * *", "hour"),
            ("0 0 1 * MON", "not both"),
            ("* * * * *", "every 5 minutes"),
            ("*/2 * * * *", "every 5 minutes"),
            ("0 0 * * 8", "weekday"),
            ("5-1 * * * *", "backwards"),
            ("*/0 * * * *", "step of 0"),
        ] {
            let e = parse(bad).unwrap_err();
            assert!(e.contains(says), "{bad}: {e}");
        }
        assert!(parse("*/15 * * * *").is_ok());
    }

    #[test]
    fn the_next_run_is_read_in_the_teams_time_zone_across_daylight_saving() {
        let ny = time_zone("America/New_York").unwrap();
        let c = parse("23 7 * * MON").unwrap();
        // Tuesday 24 November 2026, 15:00 UTC: next is Monday 30 November,
        // 07:23 EST = 12:23 UTC.
        let now = Utc.with_ymd_and_hms(2026, 11, 24, 15, 0, 0).unwrap();
        let next = c.next_after(ny, now).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 11, 30, 12, 23, 0).unwrap());
        assert_eq!(local_label(next, ny), "Mon 30 Nov 07:23 EST");
        // In summer the same 07:23 is 11:23 UTC.
        let june = Utc.with_ymd_and_hms(2026, 6, 2, 0, 0, 0).unwrap();
        assert_eq!(
            c.next_after(ny, june).unwrap(),
            Utc.with_ymd_and_hms(2026, 6, 8, 11, 23, 0).unwrap()
        );
        assert!(parse("0 0 31 2 *").unwrap().next_after(ny, now).is_none());
        assert!(time_zone("Mars/Base").is_err());
    }
}
