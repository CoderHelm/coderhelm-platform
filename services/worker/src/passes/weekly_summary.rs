//! The weekly summary email: what Coderhelm did for a team in the seven whole
//! UTC days before the send (Monday to Sunday for the Monday send).
//!
//! A schedule drops `{"type":"weekly_summary"}` on the ticket queue every
//! Monday; it sends nothing unless `WEEKLY_SUMMARY_ENABLED=true`. Each active
//! member whose weekly summary preference is on (missing = on) gets one email,
//! built for their role: data-source lookups only for admins and owners, alerts
//! only for members and up, the same split as the dashboard's endpoints.
//! A team can limit the scheduled send to a rollout list of addresses
//! (`SETTINGS#WEEKLY_SUMMARY_ROLLOUT`, attribute `only_emails`).
//!
//! `{"type":"weekly_summary_test","team_id":..,"to":..}` renders the real
//! summary for one team as an admin would see it and sends it to that one
//! address only, whether or not the schedule is on.
//!
//! Every email carries a visible unsubscribe link and the one-click
//! unsubscribe headers (RFC 8058), signed per person (see
//! `common::unsubscribe`).

use std::collections::HashMap;
use std::fmt::Write as _;

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message, MessageHeader};
use chrono::{DateTime, Datelike, Duration, SecondsFormat, TimeZone, Utc};
use common::unsubscribe;
use tracing::{info, warn};

use crate::WorkerState;

type Item = HashMap<String, AttributeValue>;
type BoxErr = Box<dyn std::error::Error + Send + Sync>;

const APP: &str = "https://app.coderhelm.com";
const MAX_PAGES: usize = 40;
/// Runs older than this can't have merged in the window in any way we report.
const RUN_LOOKBACK_DAYS: i64 = 30;
/// Merged PRs listed by name; the rest are counted.
const TOP_MERGED: usize = 5;
/// PRs and runs waiting on a person, listed by name; the rest are counted.
const TOP_ATTENTION: usize = 5;
/// How long the once-per-week send marker blocks a second send.
const SENT_MARKER_SECS: u64 = 14 * 86_400;
/// Mail apps read this address for the `mailto:` unsubscribe; requests that
/// land there are handled by hand.
const UNSUBSCRIBE_MAILTO: &str =
    "mailto:support@coderhelm.com?subject=Unsubscribe%20from%20weekly%20summaries";

/// Send one test email for a team to one address.
pub use common::messages::WeeklySummaryTestMessage;

// ── What goes in it ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MergedPr {
    pub title: String,
    pub url: String,
    pub repo: String,
}

/// Why something is waiting on a person.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Waiting {
    /// The run stopped to ask a question.
    Input,
    /// The PR is open and done on Coderhelm's side.
    Review,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attention {
    pub title: String,
    pub repo: String,
    /// The PR when there is one, otherwise the run's page.
    pub url: String,
    pub why: Waiting,
}

/// Tokens split the same way everywhere: new (uncached) input, read from
/// cache, written to cache, and output.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Tokens {
    pub input: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub output: u64,
}

impl Tokens {
    pub fn total(&self) -> u64 {
        self.input + self.cache_read + self.cache_write + self.output
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunStats {
    /// Runs created in the week.
    pub started: u64,
    /// Of the runs created in the week, the ones that finished their work.
    pub completed: u64,
    /// PRs merged in the week (one per PR), whenever their run started.
    pub merged: u64,
    pub top_merged: Vec<MergedPr>,
    /// CI fixes Coderhelm pushed in the week, and on how many PRs.
    pub ci_fix_pushes: u64,
    pub ci_fix_prs: u64,
    /// PRs Coderhelm handed to a person in the week because it could not get
    /// CI green.
    pub ci_handed_off: u64,
    /// Waiting on a person, most recent first, at most `TOP_ATTENTION`.
    pub attention: Vec<Attention>,
    pub attention_total: u64,
    /// Of `attention_total`: PRs ready for review.
    pub waiting_review: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReviewStats {
    /// Reviews posted on PRs (answers to questions are not reviews).
    pub posted: u64,
    /// Distinct PRs those reviews were on.
    pub prs: u64,
    pub approve: u64,
    pub request_changes: u64,
    pub comment: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AlertStats {
    pub received: u64,
    pub recovered: u64,
    pub runs_started: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LookupStats {
    pub total: u64,
    pub failed: u64,
    pub refused: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Usage {
    /// "October 2026"
    pub month: String,
    /// The month's counters as of the send.
    pub month_tokens: Tokens,
    /// The week's days from the same counters; `None` when part of the week
    /// is from before every model call was counted per day.
    pub week_tokens: Option<Tokens>,
    /// The team's limits (none set = no limit); each is shown by the rule it
    /// is enforced with.
    pub limits: common::token_limits::TokenLimits,
}

/// One limit's line: the tokens it counts, then "of the … limit used · N%".
#[derive(Debug, Clone, PartialEq)]
pub struct LimitLine {
    pub used: u64,
    pub rest: String,
    pub pct: u64,
}

impl Usage {
    /// Each set limit against the month's counters, by the same rule the
    /// limit is enforced with.
    pub fn limit_lines(&self) -> Vec<LimitLine> {
        use common::token_limits::LimitKind;
        let t = &self.month_tokens;
        let totals = common::usage::UsageTotals {
            tokens_in_uncached: t.input,
            cache_read: t.cache_read,
            cache_write: t.cache_write,
            tokens_out: t.output,
        };
        self.limits
            .uses(&totals)
            .into_iter()
            .map(|u| {
                let pct = (u.used as f64 / u.max as f64 * 100.0).round() as u64;
                let rest = match u.kind {
                    LimitKind::Legacy { counts_cache } => {
                        let rule = if counts_cache {
                            ""
                        } else {
                            " (the limit counts new input and output, not cache)"
                        };
                        format!("of the {} token limit used · {pct}%{rule}", tokens(u.max))
                    }
                    LimitKind::NewTokens => format!(
                        "of the {} new tokens limit used · {pct}% (new input and output)",
                        tokens(u.max)
                    ),
                    kind => format!(
                        "of the {} {} limit used · {pct}%",
                        tokens(u.max),
                        kind.label()
                    ),
                };
                LimitLine {
                    used: u.used,
                    rest,
                    pct,
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    pub team_name: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub runs: RunStats,
    pub reviews: ReviewStats,
    pub alerts: AlertStats,
    pub skill_reads: u64,
    pub agent_asks: u64,
    pub lookups: LookupStats,
    pub usage: Usage,
}

/// What a recipient may see, from their role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Access {
    Viewer,
    Member,
    Admin,
}

impl Access {
    pub fn from_role(role: &str) -> Access {
        match role {
            "owner" | "admin" => Access::Admin,
            // Records without a role are members (as on the team page).
            "member" | "billing" | "" => Access::Member,
            _ => Access::Viewer,
        }
    }
}

pub struct Rendered {
    pub subject: String,
    pub html: String,
    pub text: String,
    /// False when nothing happened that this person can see.
    pub has_data: bool,
}

// ── Pure: rows → numbers ──────────────────────────────────────────────────

fn s(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn n(i: &Item, k: &str) -> f64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}

fn b(i: &Item, k: &str) -> Option<bool> {
    i.get(k).and_then(|v| v.as_bool().ok()).copied()
}

/// The string entries of a list attribute.
fn strings(i: &Item, k: &str) -> Vec<String> {
    i.get(k)
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default()
}

fn within(ts: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
    DateTime::parse_from_rfc3339(ts)
        .map(|t| {
            let t = t.with_timezone(&Utc);
            t >= start && t < end
        })
        .unwrap_or(false)
}

/// The summary's week: the seven whole UTC days before the day it is sent.
pub fn week_before(now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let end = now
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .map(|t| t.and_utc())
        .unwrap_or(now);
    (end - Duration::days(7), end)
}

/// One PR across every run that worked on it.
struct PrRuns<'a> {
    runs: Vec<&'a Item>,
}

impl PrRuns<'_> {
    fn merged_run(&self) -> Option<&Item> {
        self.runs
            .iter()
            .copied()
            .filter(|r| s(r, "status") == "merged" || s(r, "pr_outcome") == "merged")
            .max_by_key(|r| s(r, "merged_at"))
    }
    fn closed(&self) -> bool {
        self.runs.iter().any(|r| s(r, "pr_outcome") == "closed")
    }
    fn latest(&self) -> &Item {
        self.runs
            .iter()
            .copied()
            .max_by_key(|r| s(r, "created_at"))
            .expect("a PR has at least one run")
    }
}

/// The key a PR is counted under: its link, or the run when there is none.
fn pr_key(r: &Item) -> String {
    let url = s(r, "pr_url");
    if url.is_empty() {
        format!("run:{}", s(r, "run_id"))
    } else {
        url
    }
}

/// Runs from the last 30 days → the week's numbers.
///
/// * started / completed: runs created in the week, and those of them that
///   finished their work (completed or merged);
/// * merged: PRs (one per link, however many runs worked on it) whose merge
///   time is in the week; a merge recorded without a time counts when its run
///   was created in the week;
/// * waiting on a person, as of the send: a run that asked a question and is
///   still its ticket's latest run, and a PR whose latest run finished, that no
///   run recorded as merged or closed (one entry per PR);
/// * CI: fixes pushed in the week (`ci_fixes_at`), the PRs they were on, and
///   PRs handed to a person in the week (`handed_off_at`).
pub fn run_stats(rows: &[Item], start: DateTime<Utc>, end: DateTime<Utc>) -> RunStats {
    let mut out = RunStats::default();
    let mut prs: HashMap<String, PrRuns> = HashMap::new();
    let mut latest_of_ticket: HashMap<(String, String), String> = HashMap::new();
    for r in rows {
        let created = s(r, "created_at");
        if n(r, "pr_number") > 0.0 || !s(r, "pr_url").is_empty() {
            prs.entry(pr_key(r))
                .or_insert_with(|| PrRuns { runs: Vec::new() })
                .runs
                .push(r);
        }
        let ticket = s(r, "ticket_id");
        if !ticket.is_empty() {
            let e = latest_of_ticket.entry((s(r, "repo"), ticket)).or_default();
            if created > *e {
                *e = created.clone();
            }
        }
        if within(&created, start, end) {
            out.started += 1;
            if matches!(s(r, "status").as_str(), "completed" | "merged") {
                out.completed += 1;
            }
        }
    }

    let mut merged: Vec<(String, MergedPr)> = Vec::new();
    let mut waiting: Vec<(String, Attention)> = Vec::new();
    let mut fix_prs = std::collections::HashSet::new();
    let mut handed = std::collections::HashSet::new();
    for (key, pr) in &prs {
        if let Some(m) = pr.merged_run() {
            let merged_at = s(m, "merged_at");
            let created = s(m, "created_at");
            let in_week = if merged_at.is_empty() {
                within(&created, start, end)
            } else {
                within(&merged_at, start, end)
            };
            if in_week {
                out.merged += 1;
                let url = s(m, "pr_url");
                if !url.is_empty() {
                    let when = if merged_at.is_empty() {
                        created
                    } else {
                        merged_at
                    };
                    merged.push((
                        when,
                        MergedPr {
                            title: s(m, "title"),
                            url,
                            repo: s(m, "repo"),
                        },
                    ));
                }
            }
        } else if !pr.closed() {
            let last = pr.latest();
            if s(last, "status") == "completed" && n(last, "pr_number") > 0.0 {
                out.waiting_review += 1;
                waiting.push((
                    s(last, "created_at"),
                    Attention {
                        title: s(last, "title"),
                        repo: s(last, "repo"),
                        url: s(last, "pr_url"),
                        why: Waiting::Review,
                    },
                ));
            }
        }
        for r in &pr.runs {
            let fixes = strings(r, "ci_fixes_at")
                .iter()
                .filter(|t| within(t, start, end))
                .count() as u64;
            if fixes > 0 {
                out.ci_fix_pushes += fixes;
                fix_prs.insert(key.clone());
            }
            if within(&s(r, "handed_off_at"), start, end) {
                handed.insert(key.clone());
            }
        }
    }
    out.ci_fix_prs = fix_prs.len() as u64;
    out.ci_handed_off = handed.len() as u64;

    for r in rows {
        if s(r, "status") != "needs_input" {
            continue;
        }
        let created = s(r, "created_at");
        let ticket = s(r, "ticket_id");
        let superseded = !ticket.is_empty()
            && latest_of_ticket
                .get(&(s(r, "repo"), ticket))
                .is_some_and(|latest| *latest > created);
        if superseded {
            continue;
        }
        let url = match s(r, "pr_url") {
            u if u.is_empty() => format!("{APP}/runs/detail/?id={}", s(r, "run_id")),
            u => u,
        };
        waiting.push((
            created,
            Attention {
                title: s(r, "title"),
                repo: s(r, "repo"),
                url,
                why: Waiting::Input,
            },
        ));
    }

    // Most recent first; ties by link so the order never depends on the map.
    merged.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.url.cmp(&b.1.url)));
    out.top_merged = merged
        .into_iter()
        .take(TOP_MERGED)
        .map(|(_, p)| p)
        .collect();
    waiting.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.url.cmp(&b.1.url)));
    out.attention_total = waiting.len() as u64;
    out.attention = waiting
        .into_iter()
        .take(TOP_ATTENTION)
        .map(|(_, a)| a)
        .collect();
    out
}

/// Review stats records (`RSTAT#`) → reviews posted, the PRs they were on,
/// and the verdict split. Answers to questions and re-reviews that posted
/// nothing are not reviews.
pub fn review_stats(rows: &[Item]) -> ReviewStats {
    let mut out = ReviewStats::default();
    let mut prs = std::collections::HashSet::new();
    for r in rows {
        let verdict = s(r, "verdict");
        if !b(r, "posted").unwrap_or(true) || verdict == "QUESTION" {
            continue;
        }
        out.posted += 1;
        prs.insert((s(r, "repo"), n(r, "pr_number") as u64));
        match verdict.as_str() {
            "APPROVE" => out.approve += 1,
            "REQUEST_CHANGES" => out.request_changes += 1,
            "COMMENT" => out.comment += 1,
            _ => {}
        }
    }
    out.prs = prs.len() as u64;
    out
}

/// Review records (`REVIEW#`) from before every review also wrote a stats
/// record, in the week, that have no stats record → stats rows for them.
/// Those reviews did not record whether they were posted: counted as posted.
pub fn reviews_missing_stats(
    reviews: &[Item],
    stats: &[Item],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Vec<Item> {
    let have: std::collections::HashSet<String> = stats.iter().map(|i| s(i, "sk")).collect();
    reviews
        .iter()
        .filter(|r| within(&s(r, "created_at"), start, end))
        .filter(|r| {
            common::review_stats::stat_sk_from_review_sk(&s(r, "sk"))
                .is_some_and(|sk| !have.contains(&sk))
        })
        .map(|r| {
            let mut row = r.clone();
            row.insert("posted".into(), AttributeValue::Bool(true));
            row
        })
        .collect()
}

/// Alert records (`ALERT_EVENT#`) → received, recovered and runs started.
pub fn alert_stats(rows: &[Item]) -> AlertStats {
    let mut out = AlertStats::default();
    for r in rows {
        let outcome = s(r, "outcome");
        let recovery =
            s(r, "kind") == "recovery" || outcome == "recovered" || outcome == "no_open_alert";
        if recovery {
            out.recovered += 1;
        } else if outcome != "not_alarm" {
            out.received += 1;
            if outcome == "run_started" {
                out.runs_started += 1;
            }
        }
    }
    out
}

// ── Pure: numbers → email ─────────────────────────────────────────────────
//
// Built the way mail apps need it: tables for layout, every style inline
// (the <style> blocks only add phone spacing and guards on top), a 600px
// column that shrinks on phones, system fonts, no images (the mark is text,
// so nothing breaks with images off), a VML button for Outlook on Windows,
// and a hidden preview line.
//
// Dark only, like the dashboard: every layer (body, page table, card, tiles)
// carries its dark color both as a `bgcolor` attribute and inline, so it
// stays dark where <style> is stripped, and the page says it is already dark
// (`color-scheme: dark`) so apps don't invert it. Text colors pass AA
// contrast on the card and tile colors.

/// The dashboard's dark zinc palette, used inline everywhere.
mod c {
    pub const PAGE: &str = "#09090b";
    pub const CARD: &str = "#18181b";
    pub const TILE: &str = "#202024";
    pub const BORDER: &str = "#27272a";
    pub const T1: &str = "#f4f4f5";
    pub const T2: &str = "#a1a1aa";
    pub const T3: &str = "#8f8f99";
    pub const LINK: &str = "#60a5fa";
    pub const BLUE: &str = "#3b82f6";
    pub const BUTTON: &str = "#2563eb";
    pub const TRACK: &str = "#2e2e33";
    pub const GREEN: &str = "#22c55e";
    pub const AMBER: &str = "#f59e0b";
    pub const RED: &str = "#ef4444";
    pub const GRAY: &str = "#71717a";
    /// Status chips: (background, text).
    pub const CHIP_REVIEW: (&str, &str) = ("#172554", "#bfdbfe");
    pub const CHIP_INPUT: (&str, &str) = ("#422006", "#fde68a");
}

const FONT: &str = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif";
const MONO: &str = "ui-monospace,SFMono-Regular,Menlo,Consolas,'Liberation Mono',monospace";

/// Keeps the dark values where mail apps try to restyle: Gmail (reached
/// through its `u + .body` wrapper) and Outlook.com's dark mode, which marks
/// elements with `data-ogsc` (text) and `data-ogsb` (backgrounds).
const GUARD_CSS: &str = "u + .body .bg-page{background:#09090b !important}\
u + .body .bg-card{background:#18181b !important}\
u + .body .bg-tile{background:#202024 !important}\
[data-ogsc] .t1{color:#f4f4f5 !important}\
[data-ogsc] .t2{color:#a1a1aa !important}\
[data-ogsc] .t3{color:#8f8f99 !important}\
[data-ogsc] .lk{color:#60a5fa !important}\
[data-ogsb] .bg-page{background:#09090b !important}\
[data-ogsb] .bg-card{background:#18181b !important}\
[data-ogsb] .bg-tile{background:#202024 !important}";

/// Phones: tighter padding, a smaller headline. Resets for links that mail
/// apps restyle on their own.
const BASE_CSS: &str = ":root{color-scheme:dark;supported-color-schemes:dark}\
body{margin:0;padding:0;background:#09090b;-webkit-text-size-adjust:100%}\
a[x-apple-data-detectors]{color:inherit !important;text-decoration:none !important;font:inherit !important}\
u + .body a{color:inherit}\
@media (max-width:620px){\
.px{padding-left:20px !important;padding-right:20px !important}\
.h1{font-size:24px !important;line-height:32px !important}\
.tile{padding:14px !important}\
.num{font-size:28px !important;line-height:34px !important}\
}";

fn esc(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// 1234 → "1,234".
fn num(v: u64) -> String {
    let digits = v.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// 1_234_567 → "1.2M", 20_000_000 → "20M", 45_600 → "46K".
fn tokens(v: u64) -> String {
    let short = |x: f64, unit: &str| {
        let s = format!("{x:.1}");
        format!("{}{unit}", s.strip_suffix(".0").unwrap_or(&s))
    };
    match v {
        0..=9_999 => num(v),
        10_000..=999_499 => format!("{:.0}K", v as f64 / 1_000.0),
        999_500..=999_949_999 => short(v as f64 / 1_000_000.0, "M"),
        _ => short(v as f64 / 1_000_000_000.0, "B"),
    }
}

fn plural(v: u64, one: &str, many: &str) -> String {
    format!("{} {}", num(v), if v == 1 { one } else { many })
}

/// "Oct 12 – 18, 2026", "Sep 28 – Oct 4, 2026".
fn period(start: DateTime<Utc>, end: DateTime<Utc>) -> String {
    let last = end - Duration::seconds(1);
    if start.year() != last.year() {
        format!(
            "{} – {}",
            start.format("%b %-d, %Y"),
            last.format("%b %-d, %Y")
        )
    } else if start.month() != last.month() {
        format!("{} – {}", start.format("%b %-d"), last.format("%b %-d, %Y"))
    } else {
        format!("{} – {}", start.format("%b %-d"), last.format("%-d, %Y"))
    }
}

/// The number at the end of a PR link: ".../pull/41" → 41.
fn pr_number(url: &str) -> Option<u64> {
    url.trim_end_matches('/')
        .rsplit_once("/pull/")
        .and_then(|(_, n)| n.parse().ok())
}

/// "example/api · #41"
fn pr_meta(repo: &str, url: &str) -> String {
    match (repo.is_empty(), pr_number(url)) {
        (false, Some(n)) => format!("{repo} · #{n}"),
        (false, None) => repo.to_string(),
        (true, Some(n)) => format!("#{n}"),
        (true, None) => String::new(),
    }
}

/// Joins the last two words so a line never ends on one word alone.
fn no_orphan(html: &str) -> String {
    match html.rsplit_once(' ') {
        Some((head, tail)) if !head.is_empty() => format!("{head}&nbsp;{tail}"),
        _ => html.to_string(),
    }
}

fn title_or(title: &str, fallback: &str) -> String {
    let t = title.trim();
    if t.is_empty() {
        fallback.to_string()
    } else {
        t.to_string()
    }
}

/// One of the big numbers at the top.
struct Card {
    value: String,
    label: String,
    note: Option<String>,
}

fn card(v: u64, one: &str, many: &str, note: Option<String>) -> Card {
    Card {
        value: num(v),
        label: (if v == 1 { one } else { many }).to_string(),
        note,
    }
}

/// The week's most telling numbers, most telling first, at most four, only
/// ones that are not zero.
fn cards(sm: &Summary, access: Access) -> Vec<Card> {
    let r = &sm.runs;
    let v = &sm.reviews;
    let a = &sm.alerts;
    let mut out = Vec::new();
    if r.merged > 0 {
        let note =
            (r.waiting_review > 0).then(|| format!("{} waiting on review", num(r.waiting_review)));
        out.push(card(r.merged, "PR merged", "PRs merged", note));
    }
    if r.completed > 0 {
        let note = (r.started > r.completed).then(|| format!("of {} started", num(r.started)));
        out.push(card(r.completed, "run completed", "runs completed", note));
    }
    if v.prs > 0 {
        let note = Some(plural(v.posted, "review posted", "reviews posted"));
        out.push(card(v.prs, "PR reviewed", "PRs reviewed", note));
    }
    if access >= Access::Member && a.received > 0 {
        let note = (a.recovered > 0).then(|| format!("{} recovered", num(a.recovered)));
        out.push(card(a.received, "alert received", "alerts received", note));
    }
    if r.ci_fix_pushes > 0 {
        let note = Some(format!("on {}", plural(r.ci_fix_prs, "PR", "PRs")));
        out.push(card(
            r.ci_fix_pushes,
            "CI fix pushed",
            "CI fixes pushed",
            note,
        ));
    }
    if sm.skill_reads > 0 {
        out.push(card(sm.skill_reads, "skill read", "skill reads", None));
    }
    if access >= Access::Admin && sm.lookups.total > 0 {
        out.push(card(sm.lookups.total, "data lookup", "data lookups", None));
    }
    // Tokens have their own section; a card only when nothing else happened.
    let week = sm.usage.week_tokens.unwrap_or_default();
    if out.is_empty() && week.total() > 0 {
        out.push(Card {
            value: tokens(week.total()),
            label: "tokens used".into(),
            note: None,
        });
    }
    out.truncate(4);
    out
}

/// "71.4M tokens · 1.1M new input · 61.4M read from cache · 8.8M written to
/// cache · 1.0M output": the split used everywhere tokens are shown.
fn token_split(t: &Tokens) -> String {
    format!(
        "{} new input · {} read from cache · {} written to cache · {} output",
        tokens(t.input),
        tokens(t.cache_read),
        tokens(t.cache_write),
        tokens(t.output)
    )
}

/// The token split as a small table: this week and the month so far, in
/// the same order as everywhere else (total, new input, read from cache,
/// written to cache, output).
fn token_table(week: Option<&Tokens>, month: &Tokens, month_name: &str) -> String {
    let th = |text: &str, first: bool| {
        let (align, pad) = if first {
            ("left", "0 0 8px")
        } else {
            ("right", "0 0 8px 12px")
        };
        format!(
            r#"<th scope="col" align="{align}" class="t3" style="padding:{pad};font-size:13px;line-height:18px;font-weight:600;color:{t3};white-space:nowrap">{}</th>"#,
            esc(text),
            t3 = c::T3
        )
    };
    let mut rows = format!(
        "<tr>{}{}{}</tr>",
        th("Tokens", true),
        if week.is_some() {
            th("This week", false)
        } else {
            String::new()
        },
        th(&format!("{month_name} so far"), false)
    );
    let w = week.copied().unwrap_or_default();
    for (i, (name, wv, m)) in [
        ("Total", w.total(), month.total()),
        ("New input", w.input, month.input),
        ("Read from cache", w.cache_read, month.cache_read),
        ("Written to cache", w.cache_write, month.cache_write),
        ("Output", w.output, month.output),
    ]
    .into_iter()
    .enumerate()
    {
        let (class, color, weight) = if i == 0 {
            ("t1", c::T1, 600)
        } else {
            ("t2", c::T2, 400)
        };
        let cell = |v: String, first: bool| {
            let align = if first { "left" } else { "right" };
            let pad = if first { "0" } else { "0 0 0 12px" };
            format!(
                r#"<td align="{align}" class="{class} ln" style="padding:{pad};border-top:1px solid {line};font-size:14px;line-height:34px;font-weight:{weight};color:{color};font-variant-numeric:tabular-nums;white-space:nowrap">{v}</td>"#,
                line = c::BORDER
            )
        };
        let _ = write!(
            rows,
            "<tr>{}{}{}</tr>",
            cell(esc(name), true),
            if week.is_some() {
                cell(tokens(wv), false)
            } else {
                String::new()
            },
            cell(tokens(m), false)
        );
    }
    format!(
        r#"<table class="tokens" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;margin-top:20px;border-collapse:collapse">{rows}</table>"#
    )
}

/// A row in "The rest of the week": a name, a short line, and a link.
struct Detail {
    name: &'static str,
    line: String,
    /// Extra HTML under the line (the review bar), already escaped.
    extra: String,
    link_label: &'static str,
    link: String,
}

fn bar_cells(parts: &[(u64, &str, &str)]) -> String {
    let total: u64 = parts.iter().map(|p| p.0).sum();
    if total == 0 {
        return String::new();
    }
    // Whole percents that add up to 100; anything above zero gets at least 2.
    let mut widths: Vec<u64> = parts
        .iter()
        .map(|p| {
            if p.0 == 0 {
                0
            } else {
                ((p.0 as f64 / total as f64 * 100.0).round() as u64).max(2)
            }
        })
        .collect();
    let sum: u64 = widths.iter().sum();
    if let Some(i) = (0..widths.len()).max_by_key(|&i| widths[i]) {
        widths[i] = (widths[i] + 100).saturating_sub(sum);
    }
    let mut cells = String::new();
    for (w, (_, color, class)) in widths.iter().zip(parts) {
        if *w == 0 {
            continue;
        }
        let _ = write!(
            cells,
            r#"<td width="{w}%" height="8" bgcolor="{color}" class="{class}" style="width:{w}%;height:8px;background:{color};font-size:0;line-height:0;mso-line-height-rule:exactly">&nbsp;</td>"#
        );
    }
    format!(
        r#"<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="trk" bgcolor="{track}" style="width:100%;border-collapse:separate;background:{track};border-radius:4px;overflow:hidden;table-layout:fixed"><tr>{cells}</tr></table>"#,
        track = c::TRACK
    )
}

/// How full the month is against the limit: a filled share and the rest.
fn meter(pct: u64, color: &str) -> String {
    let fill = pct.clamp(1, 100);
    let rest = if fill < 100 {
        format!(
            r#"<td width="{w}%" height="8" style="width:{w}%;height:8px;font-size:0;line-height:0;mso-line-height-rule:exactly">&nbsp;</td>"#,
            w = 100 - fill
        )
    } else {
        String::new()
    };
    format!(
        r#"<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="trk" bgcolor="{track}" style="width:100%;border-collapse:separate;background:{track};border-radius:4px;overflow:hidden;table-layout:fixed"><tr><td width="{fill}%" height="8" bgcolor="{color}" style="width:{fill}%;height:8px;background:{color};border-radius:4px;font-size:0;line-height:0;mso-line-height-rule:exactly">&nbsp;</td>{rest}</tr></table>"#,
        track = c::TRACK
    )
}

fn details(sm: &Summary, access: Access) -> Vec<Detail> {
    let mut out = Vec::new();
    let v = &sm.reviews;
    if v.posted > 0 {
        let mut legend = Vec::new();
        for (n, word, color, class) in [
            (v.approve, "approved", c::GREEN, "t1"),
            (v.request_changes, "changes requested", c::AMBER, "t1"),
            (v.comment, "commented", c::GRAY, "t1"),
        ] {
            if n > 0 {
                legend.push(format!(
                    r#"<span style="display:inline-block;margin:0 14px 0 0;white-space:nowrap"><span style="color:{color}">&#9679;</span>&nbsp;<span class="{class}" style="color:{t1}">{}</span> {word}</span>"#,
                    num(n),
                    t1 = c::T1,
                ));
            }
        }
        let bar = bar_cells(&[
            (v.approve, c::GREEN, ""),
            (v.request_changes, c::AMBER, ""),
            (v.comment, c::GRAY, ""),
        ]);
        out.push(Detail {
            name: "Reviews",
            line: format!(
                "{} on {}",
                plural(v.posted, "review posted", "reviews posted"),
                plural(v.prs, "PR", "PRs")
            ),
            extra: format!(
                r#"<div style="margin:12px 0 8px">{bar}</div><div class="t2" style="font-size:14px;line-height:22px;color:{t2}">{}</div>"#,
                legend.join(""),
                t2 = c::T2
            ),
            link_label: "View",
            link: format!("{APP}/reviewer/"),
        });
    }
    let r = &sm.runs;
    if r.ci_fix_pushes + r.ci_handed_off > 0 {
        let mut parts = Vec::new();
        if r.ci_fix_pushes > 0 {
            parts.push(format!(
                "{} on {}",
                plural(r.ci_fix_pushes, "fix pushed", "fixes pushed"),
                plural(r.ci_fix_prs, "PR", "PRs")
            ));
        }
        if r.ci_handed_off > 0 {
            parts.push(format!(
                "{} handed to a person",
                plural(r.ci_handed_off, "PR", "PRs")
            ));
        }
        out.push(Detail {
            name: "CI",
            line: parts.join(" · "),
            extra: String::new(),
            link_label: "View",
            link: format!("{APP}/analytics/"),
        });
    }
    let a = &sm.alerts;
    if access >= Access::Member && a.received + a.recovered > 0 {
        let mut line = format!(
            "{} received · {} recovered",
            num(a.received),
            num(a.recovered)
        );
        if a.runs_started > 0 {
            let _ = write!(
                line,
                " · {}",
                plural(a.runs_started, "run started", "runs started")
            );
        }
        out.push(Detail {
            name: "Alerts",
            line,
            extra: String::new(),
            link_label: "View",
            link: format!("{APP}/alerts/"),
        });
    }
    if sm.skill_reads + sm.agent_asks > 0 {
        out.push(Detail {
            name: "Skills",
            line: format!(
                "{} · {}",
                plural(sm.skill_reads, "skill read", "skill reads"),
                plural(sm.agent_asks, "agent ask", "agent asks")
            ),
            extra: String::new(),
            link_label: "View",
            link: format!("{APP}/skills/?tab=activity&range=7d"),
        });
    }
    let l = &sm.lookups;
    if access >= Access::Admin && l.total > 0 {
        let problems = l.failed + l.refused;
        let (link_label, link) = if problems > 0 {
            (
                "See problems",
                format!("{APP}/data-sources/activity/?outcome=problems&range=7d"),
            )
        } else {
            ("View", format!("{APP}/data-sources/activity/?range=7d"))
        };
        out.push(Detail {
            name: "Data lookups",
            line: format!(
                "{} · {} failed · {} refused",
                plural(l.total, "lookup", "lookups"),
                num(l.failed),
                num(l.refused)
            ),
            extra: String::new(),
            link_label,
            link,
        });
    }
    out
}

fn waiting_label(w: Waiting) -> &'static str {
    match w {
        Waiting::Input => "Needs your answer",
        Waiting::Review => "Ready for review",
    }
}

/// The section label above each block.
fn h2(text: &str) -> String {
    format!(
        r#"<h2 class="t3" style="margin:0 0 4px;font-family:{FONT};font-size:13px;line-height:20px;font-weight:600;letter-spacing:0.06em;text-transform:uppercase;color:{t3};mso-line-height-rule:exactly">{}</h2>"#,
        esc(text),
        t3 = c::T3
    )
}

fn more_link(label: &str, url: &str) -> String {
    format!(
        r#"<p style="margin:16px 0 0;font-size:15px;line-height:22px"><a href="{}" class="lk" style="color:{lk};font-weight:600;text-decoration:none">{}&nbsp;&rarr;</a></p>"#,
        esc(url),
        esc(label),
        lk = c::LINK
    )
}

/// A block of the card with space and a hairline above it.
fn block(inner: &str) -> String {
    format!(
        r#"<tr><td class="px" style="padding:0 40px"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td class="ln" style="padding:28px 0 0;border-top:1px solid {line}">{inner}</td></tr></table></td></tr><tr><td height="28" style="height:28px;font-size:0;line-height:0">&nbsp;</td></tr>"#,
        line = c::BORDER
    )
}

/// One linked PR or run: the title, then a quiet line under it.
fn list_row(
    title: &str,
    url: &str,
    meta: &str,
    chip: Option<(&str, &str, &str, &str)>,
    first: bool,
) -> String {
    let border = if first {
        String::new()
    } else {
        format!("border-top:1px solid {};", c::BORDER)
    };
    let chip = chip
        .map(|(text, bg, fg, class)| {
            format!(
                r#"<span class="{class}" style="display:inline-block;padding:0 8px;border-radius:999px;background:{bg};color:{fg};font-size:12px;line-height:20px;font-weight:600;white-space:nowrap">{}</span>&nbsp;&nbsp;"#,
                esc(text)
            )
        })
        .unwrap_or_default();
    format!(
        r#"<tr><td class="ln" style="padding:12px 0;{border}"><a href="{url}" class="t1" style="color:{t1};font-size:16px;line-height:24px;font-weight:600;text-decoration:none;word-break:break-word">{title}</a><div class="t3" style="margin-top:4px;font-size:14px;line-height:20px;color:{t3}">{chip}{meta}</div></td></tr>"#,
        url = esc(url),
        title = esc(title),
        meta = esc(meta),
        t1 = c::T1,
        t3 = c::T3,
    )
}

fn list(rows: &[String]) -> String {
    format!(
        r#"<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:4px">{}</table>"#,
        rows.join("")
    )
}

fn stat_tile(cd: &Card, colspan: bool) -> String {
    let span = if colspan {
        r#" colspan="3""#
    } else {
        r#" width="48%""#
    };
    let note = cd
        .note
        .as_ref()
        .map(|n| {
            format!(
                r#"<div class="t3" style="margin-top:2px;font-size:13px;line-height:18px;color:{t3}">{}</div>"#,
                esc(n),
                t3 = c::T3
            )
        })
        .unwrap_or_default();
    format!(
        r#"<td{span} valign="top" class="bg-tile tile" bgcolor="{tile}" style="padding:20px;background:{tile};border:1px solid {border};border-radius:12px"><div class="t1 num" style="font-size:32px;line-height:38px;font-weight:700;letter-spacing:-0.02em;color:{t1};font-variant-numeric:tabular-nums;mso-line-height-rule:exactly">{value}</div><div class="t2" style="margin-top:6px;font-size:15px;line-height:20px;font-weight:500;color:{t2}">{label}</div>{note}</td>"#,
        tile = c::TILE,
        border = c::BORDER,
        t1 = c::T1,
        t2 = c::T2,
        value = esc(&cd.value),
        label = esc(&cd.label),
    )
}

fn stat_grid(cs: &[Card]) -> String {
    let gap = r#"<td width="4%" style="width:4%;font-size:0;line-height:0">&nbsp;</td>"#;
    let mut rows = Vec::new();
    for pair in cs.chunks(2) {
        match pair {
            [a, b] => rows.push(format!(
                "<tr>{}{gap}{}</tr>",
                stat_tile(a, false),
                stat_tile(b, false)
            )),
            [a] => rows.push(format!("<tr>{}</tr>", stat_tile(a, true))),
            _ => {}
        }
    }
    format!(
        r#"<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;border-collapse:separate">{}</table>"#,
        rows.join(r#"<tr><td colspan="3" height="12" style="height:12px;font-size:0;line-height:0">&nbsp;</td></tr>"#)
    )
}

/// A button Outlook on Windows draws too (VML), with the link everyone
/// else gets.
fn button(label: &str, url: &str) -> String {
    let url = esc(url);
    let label = esc(label);
    format!(
        r#"<!--[if mso]><v:roundrect xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word" href="{url}" style="height:48px;v-text-anchor:middle;width:200px;" arcsize="17%" stroke="f" fillcolor="{bg}"><w:anchorlock/><center style="color:#fafafa;font-family:Arial,sans-serif;font-size:16px;font-weight:bold;">{label}</center></v:roundrect><![endif]--><!--[if !mso]><!--><a href="{url}" style="display:inline-block;background:{bg};color:#fafafa;font-family:{FONT};font-size:16px;line-height:48px;font-weight:600;text-align:center;text-decoration:none;padding:0 28px;border-radius:8px;white-space:nowrap;-webkit-text-size-adjust:none">{label}</a><!--<![endif]-->"#,
        bg = c::BUTTON
    )
}

/// The hidden line mail apps show next to the subject, padded so no body
/// text leaks in after it.
fn preheader(text: &str) -> String {
    let filler = "&#8199;&#65279;&#847; ".repeat(90);
    format!(
        r#"<div style="display:none;max-height:0;max-width:0;overflow:hidden;mso-hide:all;font-size:1px;line-height:1px;opacity:0" aria-hidden="true">{}{filler}</div>"#,
        esc(text)
    )
}

pub fn render(sm: &Summary, access: Access, unsubscribe_url: &str) -> Rendered {
    let r = &sm.runs;
    let u = &sm.usage;
    let cs = cards(sm, access);
    let ds = details(sm, access);
    let month_total = u.month_tokens.total();
    let has_usage = u.week_tokens.is_some_and(|w| w.total() > 0) || month_total > 0;
    let has_data = !cs.is_empty() || !r.attention.is_empty() || !ds.is_empty() || has_usage;
    let when = period(sm.start, sm.end);
    let team = if sm.team_name.trim().is_empty() {
        "your team".to_string()
    } else {
        sm.team_name.trim().to_string()
    };
    let fact = |cd: &Card| format!("{} {}", cd.value, cd.label);
    let (subject, headline) = match cs.as_slice() {
        [] => (
            "Your Coderhelm week: a quiet one".to_string(),
            "A quiet week on Coderhelm".to_string(),
        ),
        [a] => (
            format!("Your Coderhelm week: {}", fact(a)),
            format!("Your week on Coderhelm: {}", fact(a)),
        ),
        [a, b, ..] => (
            format!("Your Coderhelm week: {} and {}", fact(a), fact(b)),
            format!("Your week on Coderhelm: {}", fact(a)),
        ),
    };
    let intro = if has_data {
        format!("Here is what Coderhelm did for {team} this week.")
    } else {
        format!("Nothing happened in Coderhelm for {team} from {when}.")
    };
    let preview = if r.attention_total > 0 {
        format!(
            "{} waiting on you. {team}, {when}.",
            plural(r.attention_total, "thing is", "things are")
        )
    } else {
        let rest: Vec<String> = cs.iter().skip(2).map(fact).collect();
        if rest.is_empty() {
            format!("{team}, {when}.")
        } else {
            format!("Also {}. {team}, {when}.", rest.join(", "))
        }
    };

    // Usage lines, shared by both parts.
    let month = &u.month;
    // Each limit is shown by the rule it is enforced with: new input +
    // output, cache read, cache write, or (the old single limit) all four
    // kinds or new input + output only.
    let limit_lines = u.limit_lines();
    let usage_head = if limit_lines.is_empty() {
        format!("{} tokens · no monthly limit set", tokens(month_total))
    } else {
        limit_lines
            .iter()
            .map(|l| format!("{} {}", tokens(l.used), l.rest))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let month_line = format!(
        "{} tokens in {month} so far · {}",
        tokens(month_total),
        token_split(&u.month_tokens)
    );
    let week_line = u
        .week_tokens
        .map(|w| {
            format!(
                "\n{} tokens this week · {}",
                tokens(w.total()),
                token_split(&w)
            )
        })
        .unwrap_or_default();

    // ── Plain text ──
    let mut text = format!("{headline}\n{team} · {when}\n");
    if !has_data {
        text.push_str("\nNothing happened in Coderhelm this week.\n");
    }
    if !cs.is_empty() {
        text.push('\n');
        for cd in &cs {
            let _ = write!(text, "  {} {}", cd.value, cd.label);
            if let Some(n) = &cd.note {
                let _ = write!(text, " ({n})");
            }
            text.push('\n');
        }
    }
    if !r.attention.is_empty() {
        text.push_str("\nNEEDS YOUR ATTENTION · LAST 30 DAYS\n");
        for a in &r.attention {
            let meta = pr_meta(&a.repo, &a.url);
            let _ = writeln!(
                text,
                "- {} ({}{})\n  {}",
                title_or(&a.title, "Untitled run"),
                waiting_label(a.why).to_lowercase(),
                if meta.is_empty() {
                    String::new()
                } else {
                    format!(", {meta}")
                },
                a.url
            );
        }
        let more = r.attention_total.saturating_sub(r.attention.len() as u64);
        if more > 0 {
            let _ = writeln!(text, "  and {} more: {APP}/", num(more));
        }
    }
    if !r.top_merged.is_empty() {
        text.push_str("\nMERGED THIS WEEK\n");
        for p in &r.top_merged {
            let meta = pr_meta(&p.repo, &p.url);
            let _ = writeln!(
                text,
                "- {}{}\n  {}",
                title_or(&p.title, &p.url),
                if meta.is_empty() {
                    String::new()
                } else {
                    format!(" ({meta})")
                },
                p.url
            );
        }
        let more = r.merged.saturating_sub(r.top_merged.len() as u64);
        if more > 0 {
            let _ = writeln!(text, "  and {} more: {APP}/", num(more));
        }
    }
    if !ds.is_empty() {
        text.push_str("\nTHE REST OF THE WEEK\n");
        for d in &ds {
            let mut line = d.line.clone();
            if d.name == "Reviews" {
                let v = &sm.reviews;
                let _ = write!(
                    line,
                    " ({} approved, {} changes requested, {} commented)",
                    num(v.approve),
                    num(v.request_changes),
                    num(v.comment)
                );
            }
            let _ = writeln!(text, "- {}: {line}\n  {}", d.name, d.link);
        }
    }
    if has_usage {
        let _ = writeln!(
            text,
            "\nTOKEN USAGE\n{usage_head}\n{month_line}{week_line}\nToken limit: {APP}/settings/budget/"
        );
    }
    let _ = write!(
        text,
        "\nOpen Coderhelm: {APP}/\n\n--\nYou get this every Monday because weekly summaries are on \
         for your Coderhelm account.\nManage notifications: {APP}/settings/notifications/\n\
         Unsubscribe from weekly summaries: {unsubscribe_url}\nCoderhelm · https://coderhelm.com\n"
    );

    // ── HTML ──
    let mut rows = String::new();
    // Headline and intro.
    let _ = write!(
        rows,
        r#"<tr><td class="px" style="padding:40px 40px 28px"><h1 class="t1 h1" style="margin:0;font-family:{FONT};font-size:28px;line-height:36px;font-weight:700;letter-spacing:-0.02em;color:{t1};mso-line-height-rule:exactly">{}</h1><p class="t2" style="margin:8px 0 0;font-size:16px;line-height:24px;color:{t2}">{}</p></td></tr>"#,
        match cs.first() {
            // The number and what it counts stay on one line.
            Some(a) => format!(
                "Your week on Coderhelm: {}",
                esc(&fact(a)).replace(' ', "&nbsp;")
            ),
            None => no_orphan(&esc(&headline)),
        },
        no_orphan(&esc(&intro)),
        t1 = c::T1,
        t2 = c::T2,
    );
    if !cs.is_empty() {
        let _ = write!(
            rows,
            r#"<tr><td class="px" style="padding:0 40px 28px">{}</td></tr>"#,
            stat_grid(&cs)
        );
    }
    if !r.attention.is_empty() {
        let items: Vec<String> = r
            .attention
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let chip = match a.why {
                    Waiting::Input => (
                        "Needs your answer",
                        c::CHIP_INPUT.0,
                        c::CHIP_INPUT.1,
                        "chip-i",
                    ),
                    Waiting::Review => (
                        "Ready for review",
                        c::CHIP_REVIEW.0,
                        c::CHIP_REVIEW.1,
                        "chip-r",
                    ),
                };
                list_row(
                    &title_or(&a.title, "Untitled run"),
                    &a.url,
                    &pr_meta(&a.repo, &a.url),
                    Some(chip),
                    i == 0,
                )
            })
            .collect();
        let more = r.attention_total.saturating_sub(r.attention.len() as u64);
        let tail = if more > 0 {
            more_link(&format!("and {} more", num(more)), &format!("{APP}/"))
        } else {
            String::new()
        };
        rows.push_str(&block(&format!(
            "{}{}{tail}",
            h2("Needs your attention · last 30 days"),
            list(&items)
        )));
    }
    if !r.top_merged.is_empty() {
        let items: Vec<String> = r
            .top_merged
            .iter()
            .enumerate()
            .map(|(i, p)| {
                list_row(
                    &title_or(&p.title, &p.url),
                    &p.url,
                    &pr_meta(&p.repo, &p.url),
                    None,
                    i == 0,
                )
            })
            .collect();
        let more = r.merged.saturating_sub(r.top_merged.len() as u64);
        let tail = if more > 0 {
            more_link(&format!("and {} more", num(more)), &format!("{APP}/"))
        } else {
            more_link("See all runs", &format!("{APP}/"))
        };
        rows.push_str(&block(&format!(
            "{}{}{tail}",
            h2("Merged this week"),
            list(&items)
        )));
    }
    if !ds.is_empty() {
        let mut inner = String::new();
        for (i, d) in ds.iter().enumerate() {
            let border = if i == 0 {
                String::new()
            } else {
                format!("border-top:1px solid {};", c::BORDER)
            };
            let _ = write!(
                inner,
                r#"<tr><td class="ln" style="padding:14px 0;{border}"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td valign="top" class="t1" style="font-size:16px;line-height:24px;font-weight:600;color:{t1}">{name}</td><td valign="top" align="right" style="font-size:14px;line-height:24px;white-space:nowrap;padding-left:12px"><a href="{link}" class="lk" style="color:{lk};font-weight:600;text-decoration:none">{label}&nbsp;&rarr;</a></td></tr><tr><td colspan="2" class="t2" style="padding-top:2px;font-size:15px;line-height:22px;color:{t2}">{line}{extra}</td></tr></table></td></tr>"#,
                name = esc(d.name),
                link = esc(&d.link),
                label = esc(d.link_label),
                line = esc(&d.line),
                extra = d.extra,
                t1 = c::T1,
                t2 = c::T2,
                lk = c::LINK,
            );
        }
        rows.push_str(&block(&format!(
            r#"{}<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:4px">{inner}</table>"#,
            h2("The rest of the week")
        )));
    }
    if has_usage {
        let line = |big: &str, small: &str| {
            format!(
                r#"<p style="margin:4px 0 0;font-size:16px;line-height:28px"><span class="t1" style="font-size:22px;font-weight:700;color:{t1};font-variant-numeric:tabular-nums">{big}</span> <span class="t2" style="color:{t2}">{small}</span></p>"#,
                big = esc(big),
                small = esc(small),
                t1 = c::T1,
                t2 = c::T2,
            )
        };
        let lines = if limit_lines.is_empty() {
            line(&tokens(month_total), "tokens · no monthly limit set")
        } else {
            limit_lines
                .iter()
                .map(|l| {
                    let color = match l.pct {
                        0..=79 => c::BLUE,
                        80..=99 => c::AMBER,
                        _ => c::RED,
                    };
                    format!(
                        r#"{}<div style="margin:12px 0 0">{}</div>"#,
                        line(&tokens(l.used), &l.rest),
                        meter(l.pct, color)
                    )
                })
                .collect::<String>()
        };
        rows.push_str(&block(&format!(
            r#"{h}{lines}{split}{more}"#,
            h = h2("Token usage"),
            split = token_table(u.week_tokens.as_ref(), &u.month_tokens, month),
            more = more_link("Token limit", &format!("{APP}/settings/budget/")),
        )));
    }
    let _ = write!(
        rows,
        r#"<tr><td class="px" style="padding:4px 40px 40px">{}</td></tr>"#,
        button("Open Coderhelm", &format!("{APP}/"))
    );

    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en" dir="ltr" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="x-apple-disable-message-reformatting">
<meta name="format-detection" content="telephone=no,date=no,address=no,email=no,url=no">
<meta name="color-scheme" content="dark">
<meta name="supported-color-schemes" content="dark">
<title>{title}</title>
<!--[if mso]><noscript><xml><o:OfficeDocumentSettings><o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml></noscript><style>td,th,div,p,a,h1,h2{{font-family:Arial,sans-serif;mso-line-height-rule:exactly}}</style><![endif]-->
<style>{base}</style>
<style>{guard}</style>
</head>
<body class="body bg-page" bgcolor="{page}" xml:lang="en" style="color-scheme:dark;margin:0;padding:0;background:{page};word-spacing:normal">
{pre}
<div role="article" aria-roledescription="email" aria-label="{title}" lang="en" dir="ltr" class="bg-page" style="background:{page};font-family:{FONT};font-size:16px">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="bg-page" bgcolor="{page}" style="width:100%;background:{page}"><tr><td align="center" style="padding:32px 16px 40px">
<!--[if mso]><table role="presentation" width="600" align="center" cellpadding="0" cellspacing="0" border="0"><tr><td><![endif]-->
<div style="max-width:600px;margin:0 auto">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
<td style="padding:0 4px 20px;font-family:{MONO};font-size:18px;line-height:24px;font-weight:700;white-space:nowrap"><span class="t1" style="color:{t1};letter-spacing:-0.08em">/</span><span style="color:{blue};letter-spacing:-0.08em">/</span><span class="t1" style="color:{t1};letter-spacing:-0.08em">/</span>&nbsp;<span class="t1" style="color:{t1};letter-spacing:-0.02em">Coderhelm</span></td>
<td align="right" class="t3" style="padding:0 4px 20px;font-size:13px;line-height:24px;color:{t3}">{when}</td>
</tr></table>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="bg-card" bgcolor="{card}" style="width:100%;background:{card};border:1px solid {border};border-radius:16px;border-collapse:separate">
{rows}
</table>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td align="center" class="t3" style="padding:28px 24px 0;font-size:13px;line-height:20px;color:{t3}">
<p style="margin:0">You get this every Monday because weekly summaries are on for your Coderhelm account.</p>
<p style="margin:8px 0 0"><a href="{app}/settings/notifications/" class="t2" style="color:{t2};text-decoration:underline;white-space:nowrap">Manage notifications</a> &nbsp;·&nbsp; <a href="{unsub}" class="t2" style="color:{t2};text-decoration:underline;white-space:nowrap">Unsubscribe from weekly summaries</a></p>
<p style="margin:16px 0 0">Coderhelm&nbsp;·&nbsp;<a href="https://coderhelm.com" class="t3" style="color:{t3};text-decoration:none">coderhelm.com</a>&nbsp;·&nbsp;<a href="https://coderhelm.com/privacy" class="t3" style="color:{t3};text-decoration:none">Privacy</a></p>
</td></tr></table>
</div>
<!--[if mso]></td></tr></table><![endif]-->
</td></tr></table>
</div>
</body>
</html>"#,
        title = esc(&subject),
        base = BASE_CSS,
        guard = GUARD_CSS,
        pre = preheader(&preview),
        page = c::PAGE,
        card = c::CARD,
        border = c::BORDER,
        t1 = c::T1,
        t2 = c::T2,
        t3 = c::T3,
        blue = c::BLUE,
        when = esc(&when),
        app = APP,
        unsub = esc(unsubscribe_url),
    );
    Rendered {
        subject,
        html,
        text,
        has_data,
    }
}

// ── Loading ───────────────────────────────────────────────────────────────

fn av(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn ulid_at(t: DateTime<Utc>, high: bool) -> String {
    let random = if high { (1u128 << 80) - 1 } else { 0 };
    ulid::Ulid::from_parts(t.timestamp_millis().max(0) as u64, random).to_string()
}

/// Where a summary is read from: the tables, and a client allowed to read
/// them. Built from the worker's state, or from a plain client in the
/// read-only live check.
pub struct Source<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub settings_table: &'a str,
    pub runs_table: &'a str,
    pub analytics_table: &'a str,
}

impl<'a> Source<'a> {
    pub fn of(state: &'a WorkerState) -> Self {
        Source {
            dynamo: &state.dynamo,
            settings_table: &state.config.settings_table_name,
            runs_table: &state.config.runs_table_name,
            analytics_table: &state.config.analytics_table_name,
        }
    }
}

async fn settings_range(
    src: &Source<'_>,
    team: &str,
    lo: &str,
    hi: &str,
    projection: &str,
) -> Result<Vec<Item>, BoxErr> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..MAX_PAGES {
        let mut req = src
            .dynamo
            .query()
            .table_name(src.settings_table)
            .key_condition_expression("pk = :pk AND sk BETWEEN :lo AND :hi")
            .expression_attribute_values(":pk", av(team))
            .expression_attribute_values(":lo", av(lo))
            .expression_attribute_values(":hi", av(hi))
            .projection_expression(projection)
            .set_exclusive_start_key(start.take());
        // `action` is a reserved word.
        if projection.contains("#a") {
            req = req.expression_attribute_names("#a", "action");
        }
        let resp = req.send().await?;
        out.extend(resp.items().iter().cloned());
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    Ok(out)
}

async fn load_runs(src: &Source<'_>, team: &str, end: DateTime<Utc>) -> Result<Vec<Item>, BoxErr> {
    let lo = ulid_at(end - Duration::days(RUN_LOOKBACK_DAYS), false);
    let hi = ulid_at(end - Duration::milliseconds(1), true);
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..MAX_PAGES {
        let resp = src
            .dynamo
            .query()
            .table_name(src.runs_table)
            .key_condition_expression("team_id = :t AND run_id BETWEEN :lo AND :hi")
            .expression_attribute_values(":t", av(team))
            .expression_attribute_values(":lo", av(&lo))
            .expression_attribute_values(":hi", av(&hi))
            .projection_expression(
                "run_id, created_at, #s, repo, title, ticket_id, pr_number, pr_url, \
                 pr_outcome, merged_at, ci_fixes_at, handed_off_at",
            )
            .expression_attribute_names("#s", "status")
            .set_exclusive_start_key(start.take())
            .send()
            .await?;
        out.extend(resp.items().iter().cloned());
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    Ok(out)
}

/// The first whole UTC day every model call was counted per day
/// (`DAY#` analytics items, from the deploy of per-call accounting on
/// 2026-10-19). A week that starts earlier has no complete week count.
fn per_day_counting_from() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 20, 0, 0, 0)
        .single()
        .unwrap_or_else(Utc::now)
}

/// From this time on every review also writes its stats record. Earlier
/// reviews got theirs from a one-time copy that dropped some writes, so a
/// week that starts earlier also reads the review records themselves.
fn review_stats_complete_from() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 17, 4, 0, 0)
        .single()
        .unwrap_or_else(Utc::now)
}

fn tokens_of(item: Option<&Item>) -> Tokens {
    let t = common::usage::UsageTotals::from_item(item);
    Tokens {
        input: t.tokens_in_uncached,
        cache_read: t.cache_read,
        cache_write: t.cache_write,
        output: t.tokens_out,
    }
}

async fn analytics_item(
    src: &Source<'_>,
    team: &str,
    period: &str,
) -> Result<Option<Item>, BoxErr> {
    Ok(src
        .dynamo
        .get_item()
        .table_name(src.analytics_table)
        .key("team_id", av(team))
        .key("period", av(period))
        .send()
        .await?
        .item()
        .cloned())
}

/// The month so far (as of `now`), the week's days, and the limit, all from
/// the same counters the limit is enforced with.
async fn load_usage(
    src: &Source<'_>,
    team: &str,
    start: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Usage, BoxErr> {
    let month = analytics_item(src, team, &now.format("%Y-%m").to_string()).await?;
    let limit = src
        .dynamo
        .get_item()
        .table_name(src.settings_table)
        .key("pk", av(team))
        .key("sk", av(common::token_limits::SETTINGS_SK))
        .send()
        .await?;
    let week_tokens = if start >= per_day_counting_from() {
        let mut w = Tokens::default();
        for d in 0..7 {
            let day = (start + Duration::days(d)).format("%Y-%m-%d");
            let t = tokens_of(
                analytics_item(src, team, &format!("DAY#{day}"))
                    .await?
                    .as_ref(),
            );
            w.input += t.input;
            w.cache_read += t.cache_read;
            w.cache_write += t.cache_write;
            w.output += t.output;
        }
        Some(w)
    } else {
        None
    };
    Ok(Usage {
        month: now.format("%B %Y").to_string(),
        month_tokens: tokens_of(month.as_ref()),
        week_tokens,
        limits: common::token_limits::TokenLimits::from_item(limit.item()),
    })
}

/// Review records, keys and the fields a stats row needs (bodies are not
/// projected away by DynamoDB's read cost, but they are not kept).
async fn load_review_records(src: &Source<'_>, team: &str) -> Result<Vec<Item>, BoxErr> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let resp = src
            .dynamo
            .query()
            .table_name(src.settings_table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", av(team))
            .expression_attribute_values(":p", av("REVIEW#"))
            .projection_expression("sk, created_at, verdict, repo, pr_number")
            .set_exclusive_start_key(start.take())
            .send()
            .await?;
        out.extend(resp.items().iter().cloned());
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => return Ok(out),
        }
    }
}

/// Everything in a team's summary for the week before the day of `now`.
pub async fn load(
    state: &WorkerState,
    team_id: &str,
    team_name: &str,
    now: DateTime<Utc>,
) -> Result<Summary, BoxErr> {
    load_from(&Source::of(state), team_id, team_name, now).await
}

pub async fn load_from(
    src: &Source<'_>,
    team_id: &str,
    team_name: &str,
    now: DateTime<Utc>,
) -> Result<Summary, BoxErr> {
    let (start, end) = week_before(now);
    let iso = |t: DateTime<Utc>| t.to_rfc3339_opts(SecondsFormat::Millis, true);
    let last = end - Duration::milliseconds(1);
    // Stats keys hold the review's own timestamp (nanoseconds), so the range
    // ends just before the first key of `end`.
    let rstat_lo = format!("{}{}", common::review_stats::PREFIX, iso(start));
    let rstat_hi = format!("{}{}", common::review_stats::PREFIX, iso(end));
    let alert_lo = format!("ALERT_EVENT#{}", ulid_at(start, false));
    let alert_hi = format!("ALERT_EVENT#{}", ulid_at(last, true));
    let use_lo = format!("SKILLUSE#{}", ulid_at(start, false));
    let use_hi = format!("SKILLUSE#{}", ulid_at(last, true));
    let log_lo = format!("DSLOG#{}", iso(start));
    let log_hi = format!("DSLOG#{}", iso(end));
    let (runs, reviews, alerts, access, lookups, usage) = tokio::join!(
        load_runs(src, team_id, end),
        settings_range(
            src,
            team_id,
            &rstat_lo,
            &rstat_hi,
            "sk, verdict, posted, repo, pr_number"
        ),
        settings_range(src, team_id, &alert_lo, &alert_hi, "kind, outcome"),
        settings_range(src, team_id, &use_lo, &use_hi, "#a, outcome"),
        settings_range(src, team_id, &log_lo, &log_hi, "outcome"),
        load_usage(src, team_id, start, now),
    );
    let mut reviews = reviews?;
    // `rstat_hi` is exclusive in effect: a key at exactly `end` sorts after it
    // only when it has more digits; drop anything at or after `end`.
    reviews.retain(|r| {
        r.get("sk").and_then(|v| v.as_s().ok()).is_some_and(|sk| {
            within(
                sk.trim_start_matches(common::review_stats::PREFIX)
                    .split('#')
                    .next()
                    .unwrap_or(""),
                start,
                end,
            )
        })
    });
    let complete_from = review_stats_complete_from();
    if start < complete_from {
        let records = load_review_records(src, team_id).await?;
        let gap_end = end.min(complete_from);
        let missing = reviews_missing_stats(&records, &reviews, start, gap_end);
        reviews.extend(missing);
    }
    let mut skills = common::skills::AccessCounts::default();
    for i in access? {
        skills.add(&s(&i, "action"), &s(&i, "outcome"));
    }
    let mut looked = LookupStats::default();
    for i in lookups? {
        looked.total += 1;
        match s(&i, "outcome").as_str() {
            "ok" => {}
            "refused" => looked.refused += 1,
            _ => looked.failed += 1,
        }
    }
    Ok(Summary {
        team_name: team_name.to_string(),
        start,
        end,
        runs: run_stats(&runs?, start, end),
        reviews: review_stats(&reviews),
        alerts: alert_stats(&alerts?),
        skill_reads: skills.reads.max(0) as u64,
        agent_asks: skills.asks.max(0) as u64,
        lookups: looked,
        usage: usage?,
    })
}

async fn team_name(state: &WorkerState, team_id: &str) -> String {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", av(team_id))
        .key("sk", av("META"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().map(|i| s(i, "github_org")))
        .unwrap_or_default()
}

struct Member {
    user_id: String,
    email: String,
    role: String,
}

/// Everyone on the team with an email who has accepted their invite.
async fn members(state: &WorkerState, team_id: &str) -> Result<Vec<Member>, BoxErr> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..MAX_PAGES {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", av(team_id))
            .expression_attribute_values(":p", av("USER#"))
            .set_exclusive_start_key(start.take())
            .send()
            .await?;
        for i in resp.items() {
            let email = s(i, "email");
            if email.is_empty() || s(i, "status") == "invited" {
                continue;
            }
            out.push(Member {
                user_id: s(i, "sk"),
                email,
                role: s(i, "role"),
            });
        }
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    Ok(out)
}

/// The person's weekly summary preference; on unless they turned it off.
async fn wants_summary(state: &WorkerState, team_id: &str, user_id: &str) -> Result<bool, BoxErr> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", av(team_id))
        .key("sk", av(&format!("NOTIFICATIONS#{user_id}")))
        .send()
        .await?;
    Ok(item
        .item()
        .and_then(|i| b(i, "email_weekly_summary"))
        .unwrap_or(true))
}

fn unsubscribe_link(state: &WorkerState, team_id: &str, user_id: &str) -> Option<String> {
    if state.secrets.jwt_secret.is_empty() {
        return None;
    }
    let exp = Utc::now().timestamp() + unsubscribe::VALID_SECS;
    let token = unsubscribe::sign(
        &state.secrets.jwt_secret,
        team_id,
        user_id,
        unsubscribe::WEEKLY_SUMMARY,
        exp,
    );
    Some(unsubscribe::url(&token))
}

/// The headers that let mail apps offer their own unsubscribe button.
pub fn unsubscribe_headers(link: &str) -> [(&'static str, String); 2] {
    [
        (
            "List-Unsubscribe",
            format!("<{link}>, <{UNSUBSCRIBE_MAILTO}>"),
        ),
        (
            "List-Unsubscribe-Post",
            "List-Unsubscribe=One-Click".to_string(),
        ),
    ]
}

async fn send(state: &WorkerState, to: &str, email: &Rendered, link: &str) -> Result<(), BoxErr> {
    let content = |v: &str| Content::builder().data(v).charset("UTF-8").build();
    let mut message = Message::builder().subject(content(&email.subject)?).body(
        Body::builder()
            .html(content(&email.html)?)
            .text(content(&email.text)?)
            .build(),
    );
    for (name, value) in unsubscribe_headers(link) {
        message = message.headers(MessageHeader::builder().name(name).value(value).build()?);
    }
    state
        .ses
        .send_email()
        .from_email_address(&state.config.ses_from_address)
        .destination(Destination::builder().to_addresses(to).build())
        .content(EmailContent::builder().simple(message.build()).build())
        .send()
        .await?;
    Ok(())
}

/// "2026-W43": the once-per-week marker's key.
fn week_key(t: DateTime<Utc>) -> String {
    let w = t.iso_week();
    format!("{}-W{:02}", w.year(), w.week())
}

/// Send one team's summaries. Returns how many went out.
async fn send_team(
    state: &WorkerState,
    team_id: &str,
    now: DateTime<Utc>,
) -> Result<usize, BoxErr> {
    let people = members(state, team_id).await?;
    if people.is_empty() {
        return Ok(0);
    }
    // Read before anything is sent: if it can't be read, send nothing.
    let listed_only = state.config.weekly_summary_listed_teams_only;
    let only = rollout_for(listed_only, rollout(state, team_id).await?);
    if !team_in_rollout(listed_only, only.as_ref()) {
        return Ok(0);
    }
    let name = team_name(state, team_id).await;
    let summary = load(state, team_id, &name, now).await?;
    let week = week_key(now);
    let mut sent = 0;
    for p in people {
        if !on_rollout(only.as_ref(), &p.email) {
            continue;
        }
        if !should_send(
            only.as_ref(),
            &p.email,
            wants_summary(state, team_id, &p.user_id).await?,
        ) {
            continue;
        }
        let Some(link) = unsubscribe_link(state, team_id, &p.user_id) else {
            warn!("No signing secret: not sending weekly summaries without an unsubscribe link");
            return Ok(sent);
        };
        let email = render(&summary, Access::from_role(&p.role), &link);
        if !email.has_data {
            continue;
        }
        let marker = format!("WEEKLY_SUMMARY#{week}#{}", p.user_id);
        let claim = common::claim::claim(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            &marker,
            SENT_MARKER_SECS,
        )
        .await;
        if claim != common::claim::Claim::Won {
            // Already sent this week, or we can't tell: never send twice.
            continue;
        }
        match send(state, &p.email, &email, &link).await {
            Ok(()) => sent += 1,
            Err(e) => warn!(team_id, error = %e, "Weekly summary not sent to one member"),
        }
    }
    info!(
        team_id,
        recipients = sent,
        rollout_list = only.as_ref().map(|l| l.len()),
        "Weekly summary sent for a team"
    );
    Ok(sent)
}

/// Where a team's rollout list lives: `only_emails`, a string set (or a
/// list of strings) of addresses. While it exists, the scheduled summary goes
/// only to members on it; without it, to everyone as usual.
const ROLLOUT_SK: &str = "SETTINGS#WEEKLY_SUMMARY_ROLLOUT";

fn norm_email(v: &str) -> String {
    v.trim().to_lowercase()
}

/// The `only_emails` addresses, normalized. `None` when the team has no list.
fn rollout_from(item: Option<&Item>) -> Option<std::collections::HashSet<String>> {
    let v = item?.get("only_emails")?;
    let mut out = std::collections::HashSet::new();
    if let Ok(ss) = v.as_ss() {
        out.extend(ss.iter().map(|e| norm_email(e)));
    } else if let Ok(l) = v.as_l() {
        out.extend(
            l.iter()
                .filter_map(|e| e.as_s().ok())
                .map(|e| norm_email(e)),
        );
    } else if let Ok(s) = v.as_s() {
        out.extend(s.split(',').map(norm_email));
    }
    out.remove("");
    // A list that is there but unreadable sends to nobody, never to everyone.
    Some(out)
}

async fn rollout(
    state: &WorkerState,
    team_id: &str,
) -> Result<Option<std::collections::HashSet<String>>, BoxErr> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", av(team_id))
        .key("sk", av(ROLLOUT_SK))
        .send()
        .await?;
    Ok(rollout_from(item.item()))
}

/// Whether a team gets the scheduled summary at all: while the rollout is
/// limited to listed teams, a team without a list gets nothing.
fn team_in_rollout(listed_only: bool, only: Option<&std::collections::HashSet<String>>) -> bool {
    !listed_only || only.is_some()
}

/// The list that limits who gets the summary: only while the rollout is
/// limited to listed teams. Once it goes to everyone, a team's old list no
/// longer holds anyone back; each person's own setting still does.
fn rollout_for(
    listed_only: bool,
    only: Option<std::collections::HashSet<String>>,
) -> Option<std::collections::HashSet<String>> {
    if listed_only {
        only
    } else {
        None
    }
}

fn on_rollout(only: Option<&std::collections::HashSet<String>>, email: &str) -> bool {
    only.is_none_or(|l| l.contains(&norm_email(email)))
}

/// Whether a member gets this week's scheduled summary.
fn should_send(
    only: Option<&std::collections::HashSet<String>>,
    email: &str,
    pref_on: bool,
) -> bool {
    pref_on && on_rollout(only, email)
}

/// The Monday schedule: every active team, if the summary is switched on.
pub async fn run(state: &WorkerState) -> Result<(), BoxErr> {
    if !state.config.weekly_summary_enabled {
        info!("Weekly summary is switched off (WEEKLY_SUMMARY_ENABLED); sending nothing");
        return Ok(());
    }
    let now = Utc::now();
    let mut start: Option<Item> = None;
    let (mut teams, mut emails) = (0usize, 0usize);
    for _ in 0..MAX_PAGES {
        let resp = state
            .dynamo
            .scan()
            .table_name(&state.config.table_name)
            .filter_expression("sk = :meta")
            .expression_attribute_values(":meta", av("META"))
            .projection_expression("pk, #st")
            .expression_attribute_names("#st", "status")
            .set_exclusive_start_key(start.take())
            .send()
            .await?;
        for i in resp.items() {
            let team_id = s(i, "pk");
            if team_id.is_empty() || s(i, "status") == "suspended" {
                continue;
            }
            teams += 1;
            match send_team(state, &team_id, now).await {
                Ok(n) => emails += n,
                Err(e) => warn!(team_id = %team_id, error = %e, "Weekly summary failed for a team"),
            }
        }
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    info!(teams, emails, "Weekly summaries sent");
    Ok(())
}

/// One test email: the team's real summary, as an admin sees it, to `to` only.
pub async fn run_test(state: &WorkerState, msg: WeeklySummaryTestMessage) -> Result<(), BoxErr> {
    let to = msg.to.trim();
    if msg.team_id.is_empty() || !to.contains('@') || to.contains([',', ';', '\n', '\r', ' ']) {
        return Err("weekly_summary_test needs a team_id and one email address in `to`".into());
    }
    let now = Utc::now();
    let name = team_name(state, &msg.team_id).await;
    let summary = load(state, &msg.team_id, &name, now).await?;
    // A real link when the address belongs to someone on the team; otherwise
    // a test link that changes nothing.
    let member = members(state, &msg.team_id)
        .await?
        .into_iter()
        .find(|m| m.email.eq_ignore_ascii_case(to));
    let link = member
        .as_ref()
        .and_then(|m| unsubscribe_link(state, &msg.team_id, &m.user_id))
        .unwrap_or_else(|| unsubscribe::url(unsubscribe::TEST_TOKEN));
    let mut email = render(&summary, Access::Admin, &link);
    email.subject = format!("[Test] {}", email.subject);
    send(state, to, &email, &link).await?;
    info!(
        team_id = %msg.team_id,
        member = member.is_some(),
        has_data = email.has_data,
        "Weekly summary test sent"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| {
                let val = if let Some(num) = v.strip_prefix('#') {
                    AttributeValue::N(num.to_string())
                } else if *v == "true" || *v == "false" {
                    AttributeValue::Bool(*v == "true")
                } else {
                    AttributeValue::S(v.to_string())
                };
                (k.to_string(), val)
            })
            .collect()
    }

    fn week() -> (DateTime<Utc>, DateTime<Utc>) {
        let end = Utc.with_ymd_and_hms(2026, 10, 19, 0, 0, 0).unwrap();
        (end - Duration::days(7), end)
    }

    fn pr(title: &str, repo: &str, n: u64) -> MergedPr {
        MergedPr {
            title: title.into(),
            url: format!("https://github.com/{repo}/pull/{n}"),
            repo: repo.into(),
        }
    }

    fn sample() -> Summary {
        let (start, end) = week();
        Summary {
            team_name: "Example Team".into(),
            start,
            end,
            runs: RunStats {
                started: 16,
                completed: 12,
                merged: 14,
                top_merged: vec![
                    pr("Add retry to the webhook sender", "example/api", 41),
                    pr("Fix <script> escaping & quotes", "example/web", 9),
                    pr("Cache the pricing page query", "example/web", 12),
                    pr("Move session cleanup to a nightly job", "example/api", 44),
                    pr(
                        "Bump the image library to the patched release",
                        "example/mobile",
                        230,
                    ),
                ],
                ci_fix_pushes: 4,
                ci_fix_prs: 3,
                ci_handed_off: 1,
                attention: vec![
                    Attention {
                        title: "Show billing errors on the settings page".into(),
                        repo: "example/web".into(),
                        url: "https://github.com/example/web/pull/15".into(),
                        why: Waiting::Review,
                    },
                    Attention {
                        title: "Which region should the new queue live in?".into(),
                        repo: "example/api".into(),
                        url: format!("{APP}/runs/detail/?id=01JABC"),
                        why: Waiting::Input,
                    },
                ],
                attention_total: 2,
                waiting_review: 1,
            },
            reviews: ReviewStats {
                posted: 9,
                prs: 6,
                approve: 5,
                request_changes: 3,
                comment: 1,
            },
            alerts: AlertStats {
                received: 6,
                recovered: 4,
                runs_started: 2,
            },
            skill_reads: 31,
            agent_asks: 5,
            lookups: LookupStats {
                total: 48,
                failed: 2,
                refused: 1,
            },
            usage: Usage {
                month: "October".into(),
                month_tokens: Tokens {
                    input: 2_000_000,
                    cache_read: 100_000_000,
                    cache_write: 14_000_000,
                    output: 2_000_000,
                },
                week_tokens: Some(Tokens {
                    input: 1_100_000,
                    cache_read: 61_400_000,
                    cache_write: 8_800_000,
                    output: 1_000_000,
                }),
                limits: legacy_limit(200_000_000, true),
            },
        }
    }

    /// A week with almost nothing in it.
    fn sparse() -> Summary {
        let (start, end) = week();
        Summary {
            team_name: "Quiet Team".into(),
            start,
            end,
            runs: RunStats {
                started: 1,
                completed: 1,
                merged: 1,
                top_merged: vec![pr("Fix a typo in the README", "example/docs", 3)],
                ..Default::default()
            },
            reviews: ReviewStats::default(),
            alerts: AlertStats::default(),
            skill_reads: 0,
            agent_asks: 0,
            lookups: LookupStats::default(),
            usage: Usage {
                month: "October".into(),
                month_tokens: Tokens {
                    input: 40_000,
                    cache_read: 300_000,
                    cache_write: 50_000,
                    output: 10_000,
                },
                week_tokens: None,
                limits: Default::default(),
            },
        }
    }

    const LINK: &str = "https://api.coderhelm.com/email/unsubscribe?t=abc.def";

    #[test]
    fn the_week_is_the_seven_whole_days_before_the_send_and_labelled_so() {
        let sent = Utc.with_ymd_and_hms(2026, 10, 19, 13, 0, 0).unwrap();
        let (start, end) = week_before(sent);
        assert_eq!(start, Utc.with_ymd_and_hms(2026, 10, 12, 0, 0, 0).unwrap());
        assert_eq!(end, Utc.with_ymd_and_hms(2026, 10, 19, 0, 0, 0).unwrap());
        // Monday to Sunday: the label names exactly the days counted.
        assert_eq!(period(start, end), "Oct 12 – 18, 2026");
        assert_eq!(week_before(end), (end - Duration::days(7), end));
    }

    #[test]
    fn merges_count_once_per_pr_when_they_merged() {
        let (start, end) = week();
        let rows = vec![
            // Started this week, merged this week.
            item(&[
                ("run_id", "r1"),
                ("created_at", "2026-10-14T10:00:00Z"),
                ("status", "merged"),
                ("pr_number", "#5"),
                ("pr_url", "https://github.com/x/y/pull/5"),
                ("title", "Newer"),
                ("merged_at", "2026-10-16T10:00:00Z"),
            ]),
            // An earlier run on the same PR: the PR counts once.
            item(&[
                ("run_id", "r0"),
                ("created_at", "2026-10-13T10:00:00Z"),
                ("status", "completed"),
                ("pr_number", "#5"),
                ("pr_url", "https://github.com/x/y/pull/5"),
            ]),
            // Started weeks ago, merged this week.
            item(&[
                ("created_at", "2026-09-30T10:00:00Z"),
                ("status", "completed"),
                ("pr_outcome", "merged"),
                ("pr_number", "#4"),
                ("pr_url", "https://github.com/x/y/pull/4"),
                ("title", "Older"),
                ("merged_at", "2026-10-13T10:00:00Z"),
            ]),
            // Merged last week: not this week's.
            item(&[
                ("created_at", "2026-10-01T10:00:00Z"),
                ("status", "merged"),
                ("pr_number", "#3"),
                ("pr_url", "https://github.com/x/y/pull/3"),
                ("merged_at", "2026-10-05T10:00:00Z"),
            ]),
            item(&[("created_at", "2026-10-17T10:00:00Z"), ("status", "failed")]),
            item(&[
                ("created_at", "2026-10-17T11:00:00Z"),
                ("status", "retried"),
            ]),
        ];
        let r = run_stats(&rows, start, end);
        assert_eq!(r.merged, 2);
        assert_eq!(r.top_merged.len(), 2);
        assert_eq!(r.top_merged[0].title, "Newer");
        assert_eq!(r.top_merged[1].title, "Older");
        // Created this week: r1, r0, the failed and the retried run.
        assert_eq!((r.started, r.completed), (4, 2));
    }

    #[test]
    fn a_closed_run_is_neither_finished_work_nor_waiting_on_anyone() {
        let (start, end) = week();
        let rows = vec![
            // A person closed the run and kept its PR open.
            item(&[
                ("run_id", "c1"),
                ("created_at", "2026-10-14T10:00:00Z"),
                ("status", "closed"),
                ("pr_number", "#7"),
                ("pr_url", "https://github.com/x/y/pull/7"),
                ("ticket_id", "T-7"),
                ("repo", "x/y"),
            ]),
            // An earlier run on the same PR finished; the close is the latest.
            item(&[
                ("run_id", "c0"),
                ("created_at", "2026-10-13T10:00:00Z"),
                ("status", "completed"),
                ("pr_number", "#7"),
                ("pr_url", "https://github.com/x/y/pull/7"),
                ("ticket_id", "T-7"),
                ("repo", "x/y"),
            ]),
            // A question a person closed instead of answering.
            item(&[
                ("run_id", "c2"),
                ("created_at", "2026-10-15T10:00:00Z"),
                ("status", "closed"),
                ("ticket_id", "T-8"),
                ("repo", "x/y"),
            ]),
        ];
        let r = run_stats(&rows, start, end);
        assert_eq!((r.started, r.completed), (3, 1));
        assert_eq!((r.attention_total, r.waiting_review), (0, 0));
    }

    #[test]
    fn waiting_on_a_person_is_one_entry_per_pr_and_only_the_latest_question() {
        let (start, end) = week();
        let rows = vec![
            // Two runs on one open PR: one entry, from the latest run.
            item(&[
                ("run_id", "a1"),
                ("created_at", "2026-10-14T10:00:00Z"),
                ("status", "completed"),
                ("pr_number", "#6"),
                ("pr_url", "https://github.com/x/y/pull/6"),
                ("title", "First try"),
            ]),
            item(&[
                ("run_id", "a2"),
                ("created_at", "2026-10-15T10:00:00Z"),
                ("status", "completed"),
                ("pr_number", "#6"),
                ("pr_url", "https://github.com/x/y/pull/6"),
                ("title", "Waiting"),
            ]),
            // A PR another run merged is not waiting, whatever this run says.
            item(&[
                ("created_at", "2026-10-14T09:00:00Z"),
                ("status", "completed"),
                ("pr_number", "#8"),
                ("pr_url", "https://github.com/x/y/pull/8"),
            ]),
            item(&[
                ("created_at", "2026-10-14T12:00:00Z"),
                ("status", "merged"),
                ("pr_number", "#8"),
                ("pr_url", "https://github.com/x/y/pull/8"),
                ("merged_at", "2026-10-01T00:00:00Z"),
            ]),
            // Closed without merging: not waiting.
            item(&[
                ("created_at", "2026-10-15T11:00:00Z"),
                ("status", "completed"),
                ("pr_outcome", "closed"),
                ("pr_number", "#7"),
                ("pr_url", "https://github.com/x/y/pull/7"),
            ]),
            // A question two weeks ago, still the ticket's latest run.
            item(&[
                ("run_id", "r9"),
                ("created_at", "2026-10-03T10:00:00Z"),
                ("status", "needs_input"),
                ("ticket_id", "T-1"),
                ("repo", "x/y"),
                ("title", "Asks"),
            ]),
            // A question a later run of the same ticket replaced.
            item(&[
                ("run_id", "q1"),
                ("created_at", "2026-10-04T10:00:00Z"),
                ("status", "needs_input"),
                ("ticket_id", "T-2"),
                ("repo", "x/y"),
            ]),
            item(&[
                ("run_id", "q2"),
                ("created_at", "2026-10-05T10:00:00Z"),
                ("status", "cancelled"),
                ("ticket_id", "T-2"),
                ("repo", "x/y"),
            ]),
        ];
        let r = run_stats(&rows, start, end);
        assert_eq!((r.attention_total, r.waiting_review), (2, 1));
        assert_eq!(r.attention[0].title, "Waiting");
        assert_eq!(r.attention[0].why, Waiting::Review);
        assert_eq!(r.attention[0].url, "https://github.com/x/y/pull/6");
        assert_eq!(r.attention[1].why, Waiting::Input);
        assert_eq!(
            r.attention[1].url,
            "https://app.coderhelm.com/runs/detail/?id=r9"
        );
    }

    #[test]
    fn ci_fixes_and_hand_offs_count_when_they_happened() {
        let (start, end) = week();
        let list = |v: &[&str]| {
            AttributeValue::L(v.iter().map(|s| AttributeValue::S(s.to_string())).collect())
        };
        let mut old_run = item(&[
            ("created_at", "2026-09-30T10:00:00Z"),
            ("status", "completed"),
            ("pr_number", "#4"),
            ("pr_url", "https://github.com/x/y/pull/4"),
            ("handed_off_at", "2026-10-15T00:00:00Z"),
        ]);
        // Two fixes this week, one before it.
        old_run.insert(
            "ci_fixes_at".into(),
            list(&[
                "2026-10-01T00:00:00Z",
                "2026-10-13T00:00:00Z",
                "2026-10-14T00:00:00Z",
            ]),
        );
        let mut new_run = item(&[
            ("created_at", "2026-10-14T10:00:00Z"),
            ("status", "completed"),
            ("pr_number", "#5"),
            ("pr_url", "https://github.com/x/y/pull/5"),
            ("handed_off_at", "2026-10-21T00:00:00Z"),
            // The old counters carry no time and are not counted.
            ("ci_fix_commits", "#9"),
            ("ci_last", "failed"),
            ("ci_fix_attempts", "#3"),
        ]);
        new_run.insert("ci_fixes_at".into(), list(&["2026-10-15T00:00:00Z"]));
        let r = run_stats(&[old_run, new_run], start, end);
        assert_eq!((r.ci_fix_pushes, r.ci_fix_prs, r.ci_handed_off), (3, 2, 1));
    }

    #[test]
    fn reviews_count_posted_reviews_and_the_prs_they_were_on() {
        let reviews = vec![
            item(&[
                ("verdict", "APPROVE"),
                ("posted", "true"),
                ("repo", "a/b"),
                ("pr_number", "#1"),
            ]),
            item(&[
                ("verdict", "REQUEST_CHANGES"),
                ("repo", "a/b"),
                ("pr_number", "#1"),
            ]),
            item(&[
                ("verdict", "COMMENT"),
                ("posted", "true"),
                ("repo", "a/c"),
                ("pr_number", "#1"),
            ]),
            item(&[
                ("verdict", "APPROVE"),
                ("posted", "false"),
                ("repo", "a/d"),
                ("pr_number", "#9"),
            ]),
            item(&[
                ("verdict", "QUESTION"),
                ("posted", "true"),
                ("repo", "a/e"),
                ("pr_number", "#9"),
            ]),
        ];
        assert_eq!(
            review_stats(&reviews),
            ReviewStats {
                posted: 3,
                prs: 2,
                approve: 1,
                request_changes: 1,
                comment: 1
            }
        );
    }

    #[test]
    fn reviews_without_a_stats_record_are_filled_in_from_the_review_records() {
        let (start, end) = week();
        let review = |sk: &str, created: &str| {
            item(&[
                ("sk", sk),
                ("created_at", created),
                ("verdict", "APPROVE"),
                ("repo", "a/b"),
                ("pr_number", "#7"),
            ])
        };
        let records = vec![
            review(
                "REVIEW#a/b#000007#2026-10-14T00:00:00Z",
                "2026-10-14T00:00:00Z",
            ),
            review(
                "REVIEW#a/b#000007#2026-10-15T00:00:00Z",
                "2026-10-15T00:00:00Z",
            ),
            // Outside the week.
            review(
                "REVIEW#a/b#000007#2026-10-01T00:00:00Z",
                "2026-10-01T00:00:00Z",
            ),
        ];
        let stats = vec![item(&[
            ("sk", "RSTAT#2026-10-14T00:00:00Z#a/b#000007"),
            ("verdict", "APPROVE"),
            ("posted", "false"),
        ])];
        let missing = reviews_missing_stats(&records, &stats, start, end);
        assert_eq!(missing.len(), 1);
        assert_eq!(
            missing[0]["created_at"],
            AttributeValue::S("2026-10-15T00:00:00Z".into())
        );
        assert_eq!(missing[0]["posted"], AttributeValue::Bool(true));
        // The one with a stats record keeps its own posted flag.
        let mut all = stats.clone();
        all.extend(missing);
        assert_eq!(review_stats(&all).posted, 1);
    }

    #[test]
    fn alerts_received_leave_out_recoveries_and_non_alarms() {
        let alerts = vec![
            item(&[("kind", "cloudwatch_alarm"), ("outcome", "run_started")]),
            item(&[("kind", "message"), ("outcome", "notified")]),
            item(&[
                ("kind", "cloudwatch_alarm"),
                ("outcome", "recovered"),
                ("backfilled", "true"),
            ]),
            item(&[("kind", "recovery"), ("outcome", "recovered")]),
            item(&[("kind", "recovery"), ("outcome", "no_open_alert")]),
            item(&[("kind", "cloudwatch_alarm"), ("outcome", "not_alarm")]),
        ];
        assert_eq!(
            alert_stats(&alerts),
            AlertStats {
                received: 2,
                recovered: 3,
                runs_started: 1
            }
        );
    }

    #[test]
    fn the_limit_is_shown_by_the_rule_it_is_enforced_with() {
        let mut sm = sample();
        // Without cache: 2M new input + 2M output of 200M.
        sm.usage.limits = legacy_limit(200_000_000, false);
        let e = render(&sm, Access::Admin, LINK);
        assert!(
            e.text.contains(
                "4M of the 200M token limit used · 2% (the limit counts new input and output, not cache)"
            ),
            "{}",
            e.text
        );
        // The month still shows all four kinds.
        assert!(e.text.contains("118M tokens in October so far"));
        sm.usage.limits = legacy_limit(200_000_000, true);
        let e = render(&sm, Access::Admin, LINK);
        assert!(e.text.contains("118M of the 200M token limit used · 59%\n"));
    }

    fn legacy_limit(max: u64, counts_cache: bool) -> common::token_limits::TokenLimits {
        common::token_limits::TokenLimits {
            limits: vec![common::token_limits::Limit {
                kind: common::token_limits::LimitKind::Legacy { counts_cache },
                max,
            }],
            legacy: true,
        }
    }

    #[test]
    fn each_set_limit_gets_its_own_line_and_meter() {
        use common::token_limits::{Limit, LimitKind, TokenLimits};
        let mut sm = sample();
        sm.usage.limits = TokenLimits {
            limits: vec![
                Limit {
                    kind: LimitKind::NewTokens,
                    max: 20_000_000,
                },
                Limit {
                    kind: LimitKind::CacheWrite,
                    max: 14_000_000,
                },
            ],
            legacy: false,
        };
        let e = render(&sm, Access::Admin, LINK);
        // 2M new input + 2M output of 20M; 14M cache write of 14M.
        assert!(
            e.text.contains(
                "TOKEN USAGE\n4M of the 20M new tokens limit used · 20% (new input and output)\n\
                 14M of the 14M cache write tokens limit used · 100%\n"
            ),
            "{}",
            e.text
        );
        assert!(
            !e.text.contains("cache read tokens limit"),
            "unset limits are not shown"
        );
        assert!(e.html.contains("of the 20M new tokens limit used · 20%"));
        assert!(e
            .html
            .contains("of the 14M cache write tokens limit used · 100%"));
        assert_eq!(e.html.matches("Token usage").count(), 1);
    }

    #[test]
    fn a_week_without_complete_day_counts_shows_the_month_only() {
        let mut sm = sample();
        sm.usage.week_tokens = None;
        let e = render(&sm, Access::Admin, LINK);
        assert!(!e.text.contains("tokens this week"));
        assert!(!e.html.contains("This week"));
        assert!(e.html.contains("October so far"));
        sm.usage.week_tokens = Some(Tokens::default());
        let e = render(&sm, Access::Admin, LINK);
        assert!(e.html.contains("This week"));
    }

    #[test]
    fn cards_say_what_they_count() {
        let e = render(&sample(), Access::Admin, LINK);
        // Distinct PRs on the card, reviews posted in its note.
        assert!(
            e.text.contains("  6 PRs reviewed (9 reviews posted)"),
            "{}",
            e.text
        );
        assert!(e.text.contains("Reviews: 9 reviews posted on 6 PRs"));
        // Waiting on review counts every waiting PR, not the listed ones.
        assert!(e.text.contains("14 PRs merged (1 waiting on review)"));
        let member = render(&sample(), Access::Member, LINK);
        assert!(
            member.text.contains("6 alerts received (4 recovered)"),
            "{}",
            member.text
        );
        assert!(e
            .text
            .contains("CI: 4 fixes pushed on 3 PRs · 1 PR handed to a person"));
    }

    #[test]
    fn no_money_anywhere_in_the_email() {
        for sm in [sample(), sparse()] {
            for access in [Access::Admin, Access::Member, Access::Viewer] {
                let e = render(&sm, access, LINK);
                for (part, body) in [
                    ("subject", &e.subject),
                    ("html", &e.html),
                    ("text", &e.text),
                ] {
                    let lower = body.to_lowercase();
                    assert!(!body.contains('$'), "{part} has a $");
                    for word in ["cost", "spend", "spent", "usd", "price", "dollar"] {
                        assert!(!lower.contains(word), "{part} says {word}");
                    }
                }
            }
        }
    }

    #[test]
    fn lookups_are_for_admins_and_alerts_for_members() {
        let sm = sample();
        let admin = render(&sm, Access::Admin, LINK);
        let member = render(&sm, Access::Member, LINK);
        let viewer = render(&sm, Access::Viewer, LINK);
        for e in [&admin.html, &admin.text] {
            assert!(e.contains("Data lookups"));
            assert!(
                e.contains("outcome=problems&amp;range=7d")
                    || e.contains("outcome=problems&range=7d")
            );
        }
        for e in [&member.html, &member.text, &viewer.html, &viewer.text] {
            assert!(!e.to_lowercase().contains("lookup"), "{e}");
        }
        assert!(member.html.contains(">Alerts<"));
        assert!(member.html.contains("alerts received"));
        assert!(!viewer.html.contains(">Alerts<"));
        assert!(!viewer.html.contains("alerts received"));
        assert!(viewer.html.contains("Merged this week"));
    }

    #[test]
    fn empty_sections_are_left_out() {
        let (start, end) = week();
        let sm = Summary {
            team_name: "Quiet".into(),
            start,
            end,
            runs: RunStats::default(),
            reviews: ReviewStats {
                posted: 1,
                prs: 1,
                approve: 1,
                ..Default::default()
            },
            alerts: AlertStats::default(),
            skill_reads: 0,
            agent_asks: 0,
            lookups: LookupStats::default(),
            usage: Usage::default(),
        };
        let e = render(&sm, Access::Admin, LINK);
        assert!(e.has_data);
        assert!(e.html.contains(">Reviews<"));
        assert_eq!(e.subject, "Your Coderhelm week: 1 PR reviewed");
        for gone in [
            "Needs your attention",
            "Merged this week",
            ">CI<",
            ">Alerts<",
            ">Skills<",
            "Data lookups",
            "Token usage",
        ] {
            assert!(!e.html.contains(gone), "{gone}");
        }
        let nothing = Summary {
            reviews: ReviewStats::default(),
            ..sm
        };
        let e = render(&nothing, Access::Admin, LINK);
        assert!(!e.has_data);
        assert!(e.text.contains("Nothing happened"));
    }

    #[test]
    fn every_email_links_each_section_and_can_be_unsubscribed() {
        let e = render(&sample(), Access::Admin, LINK);
        for link in [
            "https://app.coderhelm.com/\"",
            "https://app.coderhelm.com/reviewer/",
            "https://app.coderhelm.com/analytics/",
            "https://app.coderhelm.com/alerts/",
            "https://app.coderhelm.com/skills/?tab=activity&amp;range=7d",
            "https://app.coderhelm.com/settings/budget/",
            "https://app.coderhelm.com/settings/notifications/",
            "https://app.coderhelm.com/runs/detail/?id=01JABC",
            "https://github.com/example/api/pull/41",
            "https://github.com/example/web/pull/15",
        ] {
            assert!(e.html.contains(link), "{link}");
        }
        assert!(e.html.contains("Unsubscribe from weekly summaries"));
        assert!(e.html.contains(&format!("href=\"{LINK}\"")));
        assert!(e
            .text
            .contains(&format!("Unsubscribe from weekly summaries: {LINK}")));
        assert_eq!(
            e.subject,
            "Your Coderhelm week: 14 PRs merged and 12 runs completed"
        );
        assert!(e
            .html
            .contains("Your week on Coderhelm: 14&nbsp;PRs&nbsp;merged"));
        assert!(e.html.contains("Oct 12 – 18, 2026"));
        // Five merged PRs by name, the rest counted.
        assert!(e.html.contains("and 9 more"));
        assert!(e.html.contains("example/api · #41"));
        // Titles are escaped, not markup.
        assert!(e.html.contains("Fix &lt;script&gt; escaping &amp; quotes"));
        assert!(!e.html.contains("<script>"));
        // Tokens against the limit, split the same way everywhere.
        assert!(e.text.contains("118M of the 200M token limit used · 59%"));
        assert!(e.html.contains("of the 200M token limit used · 59%"));
        let split = "72.3M tokens this week · 1.1M new input · 61.4M read from cache · \
                     8.8M written to cache · 1M output";
        assert!(e.text.contains(split), "{}", e.text);
        assert!(e.html.contains("Read from cache") && e.html.contains("61.4M"));
        // Waiting on a person.
        assert!(e.html.contains("Ready for review"));
        assert!(e.html.contains("Needs your answer"));
        assert!(
            e.text.contains("2 things are waiting on you")
                || e.html.contains("2 things are waiting on you")
        );
    }

    #[test]
    fn the_html_is_built_for_mail_apps() {
        for sm in [sample(), sparse()] {
            let e = render(&sm, Access::Admin, LINK);
            let h = &e.html;
            // Gmail clips messages over about 100KB.
            assert!(h.len() < 60_000, "{} bytes", h.len());
            assert!(h.contains(r#"<html lang="en""#));
            assert!(h.contains(r#"name="color-scheme" content="dark""#));
            assert!(h.contains(r#"name="supported-color-schemes""#));
            assert!(h.contains("u + .body .bg-page"));
            // Dark only, even where styles are stripped.
            assert!(h.contains(r##"<body class="body bg-page" bgcolor="#09090b""##));
            assert!(h.contains(r##"class="bg-card" bgcolor="#18181b""##));
            assert!(!h.contains("light dark") && !h.contains("background:#f4f4f5"));
            assert!(h.contains("[data-ogsc]"));
            assert!(h.contains("v:roundrect"));
            assert!(h.contains(r#"aria-hidden="true""#));
            assert!(h.contains("<h1"));
            // Every table is for layout except the token split, and nothing needs an image.
            assert_eq!(
                h.matches("<table").count(),
                h.matches(r#"<table role="presentation""#).count()
                    + h.matches(r#"<table class="tokens""#).count()
            );
            assert!(!h.contains("<img"));
            assert!(!h.contains("background-image"));
            // No pure white or black backgrounds to fight forced dark mode.
            assert!(!h.contains("background:#ffffff") && !h.contains("background:#000000"));
            // The plain text part stands on its own.
            assert!(!e.text.contains("<td") && !e.text.contains("<a "));
            assert!(e.text.contains("https://app.coderhelm.com/"));
        }
    }

    #[test]
    fn listed_teams_only_skips_teams_without_a_list() {
        let list: std::collections::HashSet<String> =
            ["person@example.com".to_string()].into_iter().collect();
        // Limited rollout: only teams with a list get anything.
        assert!(!team_in_rollout(true, None));
        assert!(team_in_rollout(true, Some(&list)));
        // Open rollout: every team, as before.
        assert!(team_in_rollout(false, None));
        assert!(team_in_rollout(false, Some(&list)));
        // Sent to everyone: an old list no longer limits who gets it.
        assert_eq!(rollout_for(false, Some(list.clone())), None);
        assert_eq!(rollout_for(true, Some(list.clone())), Some(list.clone()));
    }

    #[test]
    fn a_rollout_list_limits_who_gets_the_scheduled_summary() {
        // No list: everyone whose preference is on.
        let none = rollout_from(None);
        assert!(should_send(none.as_ref(), "anyone@example.com", true));
        assert!(!should_send(none.as_ref(), "anyone@example.com", false));
        let no_field = item(&[("pk", "team-1")]);
        assert_eq!(rollout_from(Some(&no_field)), None);

        // A list: only the people on it, matched without case or spaces,
        // and still only if their preference is on.
        let mut listed = Item::new();
        listed.insert(
            "only_emails".into(),
            AttributeValue::Ss(vec![" Owner@Example.com ".into()]),
        );
        let only = rollout_from(Some(&listed));
        assert!(should_send(only.as_ref(), "owner@example.COM", true));
        assert!(!should_send(only.as_ref(), "owner@example.com", false));
        assert!(!should_send(only.as_ref(), "member@example.com", true));

        // A list of strings works the same way.
        let mut as_list = Item::new();
        as_list.insert(
            "only_emails".into(),
            AttributeValue::L(vec![AttributeValue::S("owner@example.com".into())]),
        );
        let only = rollout_from(Some(&as_list));
        assert!(should_send(only.as_ref(), "Owner@example.com", true));
        assert!(!should_send(only.as_ref(), "member@example.com", true));

        // A list that is there but unreadable sends to nobody.
        let mut odd = Item::new();
        odd.insert("only_emails".into(), AttributeValue::N("1".into()));
        let only = rollout_from(Some(&odd));
        assert!(!should_send(only.as_ref(), "owner@example.com", true));
    }

    #[test]
    fn one_click_headers_follow_rfc_8058() {
        let [list, post] = unsubscribe_headers(LINK);
        assert_eq!(list.0, "List-Unsubscribe");
        assert!(list.1.starts_with(&format!("<{LINK}>, <mailto:")));
        assert_eq!(
            post,
            (
                "List-Unsubscribe-Post",
                "List-Unsubscribe=One-Click".to_string()
            )
        );
    }

    #[test]
    fn the_queue_messages_parse() {
        use crate::models::WorkerMessage;
        let m: WorkerMessage = serde_json::from_str(r#"{"type":"weekly_summary"}"#).unwrap();
        assert!(matches!(m, WorkerMessage::WeeklySummary));
        let m: WorkerMessage = serde_json::from_str(
            r#"{"type":"weekly_summary_test","team_id":"team-1","to":"someone@example.com"}"#,
        )
        .unwrap();
        let WorkerMessage::WeeklySummaryTest(t) = m else {
            panic!("not a test message");
        };
        assert_eq!(
            (t.team_id.as_str(), t.to.as_str()),
            ("team-1", "someone@example.com")
        );
    }

    #[test]
    fn numbers_read_well() {
        assert_eq!(num(0), "0");
        assert_eq!(num(1234567), "1,234,567");
        assert_eq!(tokens(9_999), "9,999");
        assert_eq!(tokens(45_600), "46K");
        assert_eq!(tokens(999_600), "1M");
        assert_eq!(tokens(2_350_000), "2.4M");
        assert_eq!(tokens(20_000_000), "20M");
        assert_eq!(tokens(1_250_000_000), "1.2B");
        assert_eq!(pr_number("https://github.com/a/b/pull/41"), Some(41));
        assert_eq!(
            pr_number("https://app.coderhelm.com/runs/detail/?id=x"),
            None
        );
        let (start, end) = week();
        assert_eq!(period(start, end), "Oct 12 – 18, 2026");
        assert_eq!(
            period(start - Duration::days(14), end - Duration::days(14)),
            "Sep 28 – Oct 4, 2026"
        );
        assert_eq!(week_key(week().1), "2026-W43");
        assert_eq!(Access::from_role("owner"), Access::Admin);
        assert_eq!(Access::from_role(""), Access::Member);
        assert_eq!(Access::from_role("viewer"), Access::Viewer);
    }

    /// `WEEKLY_SUMMARY_PREVIEW=/path/dir cargo test -p worker weekly_summary_preview -- --ignored`
    /// writes sample emails as HTML and text, to look at in a browser.
    #[test]
    #[ignore]
    fn weekly_summary_preview() {
        let dir = std::env::var("WEEKLY_SUMMARY_PREVIEW").expect("WEEKLY_SUMMARY_PREVIEW dir");
        for (name, sm, access) in [
            ("full", sample(), Access::Admin),
            ("member", sample(), Access::Member),
            ("sparse", sparse(), Access::Admin),
        ] {
            let e = render(&sm, access, LINK);
            std::fs::write(format!("{dir}/weekly-summary-{name}.html"), &e.html).unwrap();
            std::fs::write(
                format!("{dir}/weekly-summary-{name}.txt"),
                format!("Subject: {}\n\n{}", e.subject, e.text),
            )
            .unwrap();
        }
    }

    /// Renders one real team's summary from live tables, read only, as an
    /// admin would get it (nothing is sent):
    /// `WEEKLY_SUMMARY_LIVE_TEAM=TEAM#.. WEEKLY_SUMMARY_LIVE_OUT=/dir
    /// [WEEKLY_SUMMARY_LIVE_AT=2026-10-19T13:00:00Z] [WEEKLY_SUMMARY_LIVE_STAGE=prod]
    /// cargo test -p worker weekly_summary_live -- --ignored`.
    /// Writes `email.txt`, `email.html` and `summary.txt` for an independent
    /// check of every figure against the raw records.
    #[test]
    #[ignore]
    fn weekly_summary_live() {
        let team = std::env::var("WEEKLY_SUMMARY_LIVE_TEAM").expect("WEEKLY_SUMMARY_LIVE_TEAM");
        let dir = std::env::var("WEEKLY_SUMMARY_LIVE_OUT").expect("WEEKLY_SUMMARY_LIVE_OUT dir");
        let stage = std::env::var("WEEKLY_SUMMARY_LIVE_STAGE").unwrap_or_else(|_| "prod".into());
        let at = std::env::var("WEEKLY_SUMMARY_LIVE_AT")
            .ok()
            .map(|v| {
                DateTime::parse_from_rfc3339(&v)
                    .expect("WEEKLY_SUMMARY_LIVE_AT is RFC 3339")
                    .with_timezone(&Utc)
            })
            .unwrap_or_else(Utc::now);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let dynamo = aws_sdk_dynamodb::Client::new(&cfg);
            let (settings, runs, analytics) = (
                format!("coderhelm-{stage}-settings"),
                format!("coderhelm-{stage}-runs"),
                format!("coderhelm-{stage}-analytics"),
            );
            let src = Source {
                dynamo: &dynamo,
                settings_table: &settings,
                runs_table: &runs,
                analytics_table: &analytics,
            };
            let sm = load_from(&src, &team, "a team", at).await.unwrap();
            let e = render(&sm, Access::Admin, LINK);
            std::fs::write(format!("{dir}/email.html"), &e.html).unwrap();
            std::fs::write(
                format!("{dir}/email.txt"),
                format!("Subject: {}\n\n{}", e.subject, e.text),
            )
            .unwrap();
            std::fs::write(format!("{dir}/summary.txt"), format!("{sm:#?}")).unwrap();
        });
    }
}
