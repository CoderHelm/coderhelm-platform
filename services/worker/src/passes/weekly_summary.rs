//! The weekly summary email: what Coderhelm did for a team in the last 7 days.
//!
//! A schedule drops `{"type":"weekly_summary"}` on the ticket queue every
//! Monday; it sends nothing unless `WEEKLY_SUMMARY_ENABLED=true`. Each active
//! member whose weekly summary preference is on (missing = on) gets one email,
//! built for their role: data-source lookups only for admins and owners, alerts
//! only for members and up, the same split as the dashboard's endpoints.
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
use chrono::{DateTime, Datelike, Duration, SecondsFormat, Utc};
use common::unsubscribe;
use serde::{Deserialize, Serialize};
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
/// How long the once-per-week send marker blocks a second send.
const SENT_MARKER_SECS: u64 = 14 * 86_400;
/// Mail apps read this address for the `mailto:` unsubscribe; requests that
/// land there are handled by hand.
const UNSUBSCRIBE_MAILTO: &str =
    "mailto:support@coderhelm.com?subject=Unsubscribe%20from%20weekly%20summaries";

/// Send one test email for a team to one address.
#[derive(Serialize, Deserialize, Debug)]
pub struct WeeklySummaryTestMessage {
    pub team_id: String,
    pub to: String,
}

// ── What goes in it ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MergedPr {
    pub title: String,
    pub url: String,
    pub repo: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunStats {
    pub started: u64,
    pub merged: u64,
    pub open: u64,
    pub failed: u64,
    pub top_merged: Vec<MergedPr>,
    /// Commits CoderHelm pushed to fix CI, and on how many runs.
    pub ci_fix_pushes: u64,
    pub ci_fix_runs: u64,
    pub tokens: u64,
    pub cost_usd: f64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReviewStats {
    pub total: u64,
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
    pub month_tokens: u64,
    pub month_cost_usd: f64,
    /// 0 = no limit.
    pub limit_tokens: u64,
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

fn within(ts: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
    DateTime::parse_from_rfc3339(ts)
        .map(|t| {
            let t = t.with_timezone(&Utc);
            t >= start && t < end
        })
        .unwrap_or(false)
}

/// Runs from the last 30 days → the week's numbers. Started, open, failed,
/// CI fixes and tokens count runs started in the week; merged counts PRs
/// merged in the week, whenever their run started.
pub fn run_stats(rows: &[Item], start: DateTime<Utc>, end: DateTime<Utc>) -> RunStats {
    let mut out = RunStats::default();
    let mut merged: Vec<(String, MergedPr)> = Vec::new();
    for r in rows {
        let created = s(r, "created_at");
        let in_week = within(&created, start, end);
        let status = s(r, "status");
        let outcome = s(r, "pr_outcome");
        let is_merged = status == "merged" || outcome == "merged";
        let merged_at = s(r, "merged_at");
        let has_pr = n(r, "pr_number") > 0.0;
        if is_merged && (within(&merged_at, start, end) || (merged_at.is_empty() && in_week)) {
            out.merged += 1;
            let url = s(r, "pr_url");
            if !url.is_empty() {
                let when = if merged_at.is_empty() {
                    created.clone()
                } else {
                    merged_at.clone()
                };
                merged.push((
                    when,
                    MergedPr {
                        title: s(r, "title"),
                        url,
                        repo: s(r, "repo"),
                    },
                ));
            }
        }
        if !in_week {
            continue;
        }
        out.started += 1;
        if status == "failed" {
            out.failed += 1;
        } else if has_pr && !is_merged && outcome != "closed" && status != "cancelled" {
            out.open += 1;
        }
        let fixes = n(r, "ci_fix_commits") as u64;
        if fixes > 0 {
            out.ci_fix_pushes += fixes;
            out.ci_fix_runs += 1;
        }
        out.tokens += (n(r, "tokens_in") + n(r, "tokens_out")) as u64;
        out.cost_usd += n(r, "cost_usd");
    }
    // Most recent merges first.
    merged.sort_by(|a, b| b.0.cmp(&a.0));
    out.top_merged = merged
        .into_iter()
        .take(TOP_MERGED)
        .map(|(_, p)| p)
        .collect();
    out
}

/// Review stats records (`RSTAT#`) → verdict split. Questions answered and
/// re-reviews that posted nothing are not reviews.
pub fn review_stats(rows: &[Item]) -> ReviewStats {
    let mut out = ReviewStats::default();
    for r in rows {
        let verdict = s(r, "verdict");
        if !b(r, "posted").unwrap_or(true) || verdict == "QUESTION" {
            continue;
        }
        out.total += 1;
        match verdict.as_str() {
            "APPROVE" => out.approve += 1,
            "REQUEST_CHANGES" => out.request_changes += 1,
            "COMMENT" => out.comment += 1,
            _ => {}
        }
    }
    out
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

/// 1_234_567 → "1.2M".
fn tokens(v: u64) -> String {
    match v {
        0..=9_999 => num(v),
        10_000..=999_999 => format!("{:.0}K", v as f64 / 1_000.0),
        _ => format!("{:.1}M", v as f64 / 1_000_000.0),
    }
}

fn plural(v: u64, one: &str, many: &str) -> String {
    format!("{} {}", num(v), if v == 1 { one } else { many })
}

fn period(start: DateTime<Utc>, end: DateTime<Utc>) -> String {
    let last = end - Duration::seconds(1);
    if start.year() == last.year() {
        format!("{} – {}", start.format("%b %-d"), last.format("%b %-d, %Y"))
    } else {
        format!(
            "{} – {}",
            start.format("%b %-d, %Y"),
            last.format("%b %-d, %Y")
        )
    }
}

/// One stat in a section: a number and what it counts.
struct Stat {
    value: String,
    label: String,
    tone: Tone,
}

#[derive(Clone, Copy)]
enum Tone {
    Plain,
    Good,
    Bad,
}

impl Tone {
    fn color(self) -> &'static str {
        match self {
            Tone::Plain => "#ffffff",
            Tone::Good => "#4ade80",
            Tone::Bad => "#f87171",
        }
    }
}

/// A count with its label in the right number: "1 review", "2 reviews".
fn counted(v: u64, one: &str, many: &str, tone: Tone) -> Stat {
    stat(num(v), if v == 1 { one } else { many }, tone)
}

fn stat(value: impl Into<String>, label: impl Into<String>, tone: Tone) -> Stat {
    Stat {
        value: value.into(),
        label: label.into(),
        tone,
    }
}

struct Section {
    title: &'static str,
    stats: Vec<Stat>,
    /// Extra lines under the numbers, already plain text; links render in HTML.
    lines: Vec<(String, Option<String>)>,
    link_label: &'static str,
    link: String,
}

fn sections(sm: &Summary, access: Access) -> Vec<Section> {
    let mut out = Vec::new();
    let r = &sm.runs;
    if r.started + r.merged + r.open + r.failed > 0 {
        let mut stats = vec![
            stat(num(r.started), "started", Tone::Plain),
            stat(num(r.merged), "merged", Tone::Good),
            counted(r.open, "PR open", "PRs open", Tone::Plain),
        ];
        if r.failed > 0 {
            stats.push(stat(num(r.failed), "failed", Tone::Bad));
        }
        let mut lines: Vec<(String, Option<String>)> = r
            .top_merged
            .iter()
            .map(|p| {
                let title = if p.title.trim().is_empty() {
                    p.url.clone()
                } else {
                    p.title.trim().to_string()
                };
                let label = if p.repo.is_empty() {
                    title
                } else {
                    format!("{title} ({})", p.repo)
                };
                (label, Some(p.url.clone()))
            })
            .collect();
        let more = r.merged.saturating_sub(r.top_merged.len() as u64);
        if more > 0 && !lines.is_empty() {
            lines.push((format!("and {} more merged", num(more)), None));
        }
        out.push(Section {
            title: "Runs",
            stats,
            lines,
            link_label: "See all runs",
            link: format!("{APP}/"),
        });
    }
    let v = &sm.reviews;
    if v.total > 0 {
        out.push(Section {
            title: "PR reviews",
            stats: vec![
                counted(v.total, "review", "reviews", Tone::Plain),
                stat(num(v.approve), "approved", Tone::Good),
                stat(num(v.request_changes), "changes asked", Tone::Bad),
                stat(num(v.comment), "commented", Tone::Plain),
            ],
            lines: vec![],
            link_label: "See reviews",
            link: format!("{APP}/reviewer/"),
        });
    }
    if r.ci_fix_pushes > 0 {
        out.push(Section {
            title: "CI fixes",
            stats: vec![
                counted(r.ci_fix_pushes, "fix pushed", "fixes pushed", Tone::Plain),
                counted(r.ci_fix_runs, "run fixed", "runs fixed", Tone::Plain),
            ],
            lines: vec![],
            link_label: "See analytics",
            link: format!("{APP}/analytics/"),
        });
    }
    let a = &sm.alerts;
    if access >= Access::Member && a.received + a.recovered > 0 {
        let mut stats = vec![
            stat(num(a.received), "received", Tone::Plain),
            stat(num(a.recovered), "recovered", Tone::Good),
        ];
        if a.runs_started > 0 {
            stats.push(counted(
                a.runs_started,
                "run started",
                "runs started",
                Tone::Plain,
            ));
        }
        out.push(Section {
            title: "Alerts",
            stats,
            lines: vec![],
            link_label: "See alerts",
            link: format!("{APP}/alerts/"),
        });
    }
    if sm.skill_reads + sm.agent_asks > 0 {
        out.push(Section {
            title: "Skills and agents",
            stats: vec![
                counted(sm.skill_reads, "skill read", "skill reads", Tone::Plain),
                counted(sm.agent_asks, "agent ask", "agent asks", Tone::Plain),
            ],
            lines: vec![],
            link_label: "See skill activity",
            link: format!("{APP}/skills/?tab=activity&range=7d"),
        });
    }
    let l = &sm.lookups;
    if access >= Access::Admin && l.total > 0 {
        let problems = l.failed + l.refused;
        let mut stats = vec![counted(l.total, "lookup", "lookups", Tone::Plain)];
        stats.push(stat(
            num(l.failed),
            "failed",
            if l.failed > 0 { Tone::Bad } else { Tone::Plain },
        ));
        stats.push(stat(num(l.refused), "refused", Tone::Plain));
        let lines = if problems > 0 {
            vec![(
                format!(
                    "Look at the {} that went wrong",
                    plural(problems, "lookup", "lookups")
                ),
                Some(format!(
                    "{APP}/data-sources/activity/?outcome=problems&range=7d"
                )),
            )]
        } else {
            vec![]
        };
        out.push(Section {
            title: "Data source lookups",
            stats,
            lines,
            link_label: "See lookups",
            link: format!("{APP}/data-sources/activity/?range=7d"),
        });
    }
    let u = &sm.usage;
    if r.tokens > 0 || u.month_tokens > 0 {
        let mut stats = vec![stat(tokens(r.tokens), "tokens this week", Tone::Plain)];
        if r.cost_usd >= 0.01 {
            stats.push(stat(
                format!("${:.2}", r.cost_usd),
                "cost this week",
                Tone::Plain,
            ));
        }
        let month_line = if u.limit_tokens > 0 {
            let pct = (u.month_tokens as f64 / u.limit_tokens as f64 * 100.0).round() as u64;
            stats.push(stat(
                format!("{pct}%"),
                format!("of the {} limit", u.month),
                if pct >= 80 { Tone::Bad } else { Tone::Plain },
            ));
            format!(
                "{} of {} tokens used in {}",
                tokens(u.month_tokens),
                tokens(u.limit_tokens),
                u.month
            )
        } else {
            format!(
                "{} tokens used in {}, no monthly limit set",
                tokens(u.month_tokens),
                u.month
            )
        };
        out.push(Section {
            title: "Usage",
            stats,
            lines: vec![(month_line, None)],
            link_label: "See the token limit",
            link: format!("{APP}/settings/budget/"),
        });
    }
    out
}

pub fn render(sm: &Summary, access: Access, unsubscribe_url: &str) -> Rendered {
    let secs = sections(sm, access);
    let has_data = !secs.is_empty();
    let when = period(sm.start, sm.end);
    let team = if sm.team_name.trim().is_empty() {
        "your team".to_string()
    } else {
        sm.team_name.trim().to_string()
    };
    let subject = format!("Your Coderhelm week: {team}, {when}");

    // Plain text.
    let mut text = format!("Coderhelm weekly summary for {team}\n{when}\n");
    if !has_data {
        text.push_str("\nNothing happened in Coderhelm this week.\n");
    }
    for sec in &secs {
        let _ = write!(text, "\n{}\n", sec.title.to_uppercase());
        let parts: Vec<String> = sec
            .stats
            .iter()
            .map(|s| format!("{} {}", s.value, s.label))
            .collect();
        let _ = writeln!(text, "{}", parts.join(" · "));
        for (line, link) in &sec.lines {
            match link {
                Some(l) => {
                    let _ = writeln!(text, "- {line}: {l}");
                }
                None => {
                    let _ = writeln!(text, "- {line}");
                }
            }
        }
        let _ = writeln!(text, "{}: {}", sec.link_label, sec.link);
    }
    let _ = write!(
        text,
        "\n--\nYou get this every Monday because the weekly summary is on for your \
         Coderhelm account.\nUnsubscribe from weekly summaries: {unsubscribe_url}\n\
         Email settings: {APP}/settings/notifications/\n"
    );

    // HTML: inline styles and tables, so it holds up in every mail app.
    let mut body = String::new();
    let _ = write!(
        body,
        "<h1 style=\"color:#fff;font-size:20px;line-height:1.3;margin:0 0 4px\">Your week in Coderhelm</h1>\
         <p style=\"color:#888;font-size:14px;margin:0 0 8px\">{} · {}</p>",
        esc(&team),
        esc(&when)
    );
    if !has_data {
        body.push_str(
            "<p style=\"color:#ccc;font-size:15px;line-height:1.6;margin:24px 0 0\">\
             Nothing happened in Coderhelm this week.</p>",
        );
    }
    for sec in &secs {
        let _ = write!(
            body,
            "<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" \
             style=\"margin-top:24px;border-top:1px solid #222\"><tr><td style=\"padding-top:20px\">\
             <h2 style=\"color:#fff;font-size:15px;margin:0 0 12px;font-weight:600\">{}</h2>",
            esc(sec.title)
        );
        // Stats as inline blocks: side by side on a wide screen, wrapping on a phone.
        for st in &sec.stats {
            let _ = write!(
                body,
                "<div style=\"display:inline-block;vertical-align:top;min-width:96px;margin:0 16px 12px 0\">\
                 <div style=\"color:{};font-size:22px;font-weight:700;line-height:1.2\">{}</div>\
                 <div style=\"color:#888;font-size:13px;line-height:1.4\">{}</div></div>",
                st.tone.color(),
                esc(&st.value),
                esc(&st.label)
            );
        }
        if !sec.lines.is_empty() {
            body.push_str("<ul style=\"margin:4px 0 0;padding-left:18px\">");
            for (line, link) in &sec.lines {
                match link {
                    Some(l) => {
                        let _ = write!(
                            body,
                            "<li style=\"color:#ccc;font-size:14px;line-height:1.6\"><a href=\"{}\" style=\"color:#60a5fa;text-decoration:none\">{}</a></li>",
                            esc(l),
                            esc(line)
                        );
                    }
                    None => {
                        let _ = write!(
                            body,
                            "<li style=\"color:#ccc;font-size:14px;line-height:1.6\">{}</li>",
                            esc(line)
                        );
                    }
                }
            }
            body.push_str("</ul>");
        }
        let _ = write!(
            body,
            "<p style=\"margin:12px 0 0\"><a href=\"{}\" style=\"color:#60a5fa;font-size:14px;text-decoration:none\">{} &rarr;</a></p>\
             </td></tr></table>",
            esc(&sec.link),
            esc(sec.link_label)
        );
    }
    let html = format!(
        "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
         <meta name=\"color-scheme\" content=\"dark\"><title>{title}</title></head>\
         <body style=\"margin:0;padding:0;background:#0a0a0a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif\">\
         <table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" style=\"background:#0a0a0a\"><tr><td align=\"center\" style=\"padding:32px 12px\">\
         <table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" style=\"max-width:600px;background:#111;border-radius:8px;border:1px solid #222\">\
         <tr><td style=\"padding:32px 28px 24px\">\
         <img src=\"https://coderhelm.com/favicon.svg\" width=\"32\" height=\"32\" alt=\"Coderhelm\" style=\"display:block;margin-bottom:24px\">\
         {body}\
         <a href=\"{app}/\" style=\"display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;font-size:14px;margin-top:28px\">Open Coderhelm</a>\
         <p style=\"color:#666;font-size:12px;line-height:1.6;margin-top:32px;padding-top:16px;border-top:1px solid #222\">\
         You get this every Monday because the weekly summary is on for your Coderhelm account.<br>\
         <a href=\"{unsub}\" style=\"color:#888\">Unsubscribe from weekly summaries</a> · \
         <a href=\"{app}/settings/notifications/\" style=\"color:#888\">Email settings</a> · \
         <a href=\"https://coderhelm.com/privacy\" style=\"color:#888\">Privacy</a></p>\
         </td></tr></table></td></tr></table></body></html>",
        title = esc(&subject),
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

async fn settings_range(
    state: &WorkerState,
    team: &str,
    lo: &str,
    hi: &str,
    projection: &str,
) -> Result<Vec<Item>, BoxErr> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..MAX_PAGES {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND sk BETWEEN :lo AND :hi")
            .expression_attribute_values(":pk", av(team))
            .expression_attribute_values(":lo", av(lo))
            .expression_attribute_values(":hi", av(hi))
            .projection_expression(projection)
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

async fn load_runs(
    state: &WorkerState,
    team: &str,
    end: DateTime<Utc>,
) -> Result<Vec<Item>, BoxErr> {
    let lo = ulid_at(end - Duration::days(RUN_LOOKBACK_DAYS), false);
    let hi = ulid_at(end, true);
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..MAX_PAGES {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .key_condition_expression("team_id = :t AND run_id BETWEEN :lo AND :hi")
            .expression_attribute_values(":t", av(team))
            .expression_attribute_values(":lo", av(&lo))
            .expression_attribute_values(":hi", av(&hi))
            .projection_expression(
                "created_at, #s, repo, title, cost_usd, tokens_in, tokens_out, pr_number, \
                 pr_url, ci_fix_commits, pr_outcome, merged_at",
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

async fn load_usage(state: &WorkerState, team: &str, end: DateTime<Utc>) -> Result<Usage, BoxErr> {
    let period = end.format("%Y-%m").to_string();
    let (month, limit) = tokio::join!(
        state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", av(team))
            .key("period", av(&period))
            .send(),
        state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", av(team))
            .key("sk", av("SETTINGS#TOKEN_LIMIT"))
            .send()
    );
    let month = month?;
    let limit = limit?;
    let m = month.item();
    Ok(Usage {
        month: end.format("%B %Y").to_string(),
        month_tokens: m
            .map(|i| (n(i, "total_tokens_in") + n(i, "total_tokens_out")) as u64)
            .unwrap_or(0),
        month_cost_usd: m.map(|i| n(i, "total_cost_usd")).unwrap_or(0.0),
        limit_tokens: limit.item().map(|i| n(i, "max_tokens") as u64).unwrap_or(0),
    })
}

/// Everything in a team's summary for the 7 days before `end`.
pub async fn load(
    state: &WorkerState,
    team_id: &str,
    team_name: &str,
    end: DateTime<Utc>,
) -> Result<Summary, BoxErr> {
    let start = end - Duration::days(7);
    let iso = |t: DateTime<Utc>| t.to_rfc3339_opts(SecondsFormat::Millis, true);
    let table = state.config.settings_table_name.as_str();
    let skills = common::skills::SkillStore {
        dynamo: &state.dynamo,
        table,
        team: team_id,
    };
    let since = iso(start);
    let rstat_lo = format!("{}{}", common::review_stats::PREFIX, iso(start));
    let rstat_hi = format!("{}{}~", common::review_stats::PREFIX, iso(end));
    let alert_lo = format!("ALERT_EVENT#{}", ulid_at(start, false));
    let alert_hi = format!("ALERT_EVENT#{}", ulid_at(end, true));
    let (runs, reviews, alerts, access, lookups, usage) = tokio::join!(
        load_runs(state, team_id, end),
        settings_range(state, team_id, &rstat_lo, &rstat_hi, "verdict, posted"),
        settings_range(state, team_id, &alert_lo, &alert_hi, "kind, outcome"),
        skills.access_counts(&since),
        common::live::store::count_activity(&state.dynamo, table, team_id, &since),
        load_usage(state, team_id, end),
    );
    let runs = runs?;
    let access = access.map_err(|e| format!("skill activity: {e}"))?;
    let (ok, refused, failed) = lookups.map_err(|e| format!("lookups: {e}"))?;
    Ok(Summary {
        team_name: team_name.to_string(),
        start,
        end,
        runs: run_stats(&runs, start, end),
        reviews: review_stats(&reviews?),
        alerts: alert_stats(&alerts?),
        skill_reads: access.reads.max(0) as u64,
        agent_asks: access.asks.max(0) as u64,
        lookups: LookupStats {
            total: (ok + refused + failed).max(0) as u64,
            failed: failed.max(0) as u64,
            refused: refused.max(0) as u64,
        },
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
    let name = team_name(state, team_id).await;
    let summary = load(state, team_id, &name, now).await?;
    let week = week_key(now);
    let mut sent = 0;
    for p in people {
        if !wants_summary(state, team_id, &p.user_id).await? {
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
    Ok(sent)
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
        let end = Utc.with_ymd_and_hms(2026, 10, 19, 13, 0, 0).unwrap();
        (end - Duration::days(7), end)
    }

    fn sample() -> Summary {
        let (start, end) = week();
        Summary {
            team_name: "Example Team".into(),
            start,
            end,
            runs: RunStats {
                started: 12,
                merged: 7,
                open: 3,
                failed: 2,
                top_merged: vec![
                    MergedPr {
                        title: "Add retry to the webhook sender".into(),
                        url: "https://github.com/example/api/pull/41".into(),
                        repo: "example/api".into(),
                    },
                    MergedPr {
                        title: "Fix <script> escaping & quotes".into(),
                        url: "https://github.com/example/web/pull/9".into(),
                        repo: "example/web".into(),
                    },
                ],
                ci_fix_pushes: 4,
                ci_fix_runs: 3,
                tokens: 2_350_000,
                cost_usd: 18.4,
            },
            reviews: ReviewStats {
                total: 9,
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
                month: "October 2026".into(),
                month_tokens: 8_200_000,
                month_cost_usd: 61.0,
                limit_tokens: 10_000_000,
            },
        }
    }

    const LINK: &str = "https://api.coderhelm.com/email/unsubscribe?t=abc.def";

    #[test]
    fn runs_count_the_week_and_merges_count_when_they_merged() {
        let (start, end) = week();
        let rows = vec![
            // Started this week, merged this week.
            item(&[
                ("created_at", "2026-10-14T10:00:00Z"),
                ("status", "merged"),
                ("pr_number", "#5"),
                ("pr_url", "https://github.com/x/y/pull/5"),
                ("title", "Newer"),
                ("merged_at", "2026-10-16T10:00:00Z"),
                ("tokens_in", "#1000"),
                ("tokens_out", "#500"),
                ("cost_usd", "#1.5"),
                ("ci_fix_commits", "#2"),
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
                ("tokens_in", "#99999"),
            ]),
            // Open PR this week.
            item(&[
                ("created_at", "2026-10-15T10:00:00Z"),
                ("status", "completed"),
                ("pr_number", "#6"),
            ]),
            // Closed without merging: neither open nor merged.
            item(&[
                ("created_at", "2026-10-15T11:00:00Z"),
                ("status", "completed"),
                ("pr_outcome", "closed"),
                ("pr_number", "#7"),
            ]),
            item(&[("created_at", "2026-10-17T10:00:00Z"), ("status", "failed")]),
            // Merged last week: not this week's.
            item(&[
                ("created_at", "2026-10-01T10:00:00Z"),
                ("status", "merged"),
                ("pr_number", "#3"),
                ("merged_at", "2026-10-05T10:00:00Z"),
            ]),
        ];
        let r = run_stats(&rows, start, end);
        assert_eq!((r.started, r.merged, r.open, r.failed), (4, 2, 1, 1));
        assert_eq!(r.top_merged.len(), 2);
        assert_eq!(r.top_merged[0].title, "Newer");
        assert_eq!((r.ci_fix_pushes, r.ci_fix_runs), (2, 1));
        assert_eq!(r.tokens, 1500);
        assert!((r.cost_usd - 1.5).abs() < 1e-9);
    }

    #[test]
    fn reviews_and_alerts_are_split_the_way_the_pages_show_them() {
        let reviews = vec![
            item(&[("verdict", "APPROVE"), ("posted", "true")]),
            item(&[("verdict", "REQUEST_CHANGES")]),
            item(&[("verdict", "COMMENT"), ("posted", "true")]),
            item(&[("verdict", "APPROVE"), ("posted", "false")]),
            item(&[("verdict", "QUESTION"), ("posted", "true")]),
        ];
        assert_eq!(
            review_stats(&reviews),
            ReviewStats {
                total: 3,
                approve: 1,
                request_changes: 1,
                comment: 1
            }
        );
        let alerts = vec![
            item(&[("kind", "cloudwatch_alarm"), ("outcome", "run_started")]),
            item(&[("kind", "cloudwatch_alarm"), ("outcome", "notified")]),
            item(&[("kind", "cloudwatch_alarm"), ("outcome", "recovered")]),
            item(&[("kind", "recovery"), ("outcome", "no_open_alert")]),
            item(&[("kind", "eventbridge"), ("outcome", "not_alarm")]),
        ];
        assert_eq!(
            alert_stats(&alerts),
            AlertStats {
                received: 2,
                recovered: 2,
                runs_started: 1
            }
        );
    }

    #[test]
    fn lookups_are_for_admins_and_alerts_for_members() {
        let sm = sample();
        let admin = render(&sm, Access::Admin, LINK);
        let member = render(&sm, Access::Member, LINK);
        let viewer = render(&sm, Access::Viewer, LINK);
        for e in [&admin.html, &admin.text] {
            assert!(e.contains("Data source lookups") || e.contains("DATA SOURCE LOOKUPS"));
            assert!(
                e.contains("outcome=problems&amp;range=7d")
                    || e.contains("outcome=problems&range=7d")
            );
        }
        for e in [&member.html, &member.text, &viewer.html, &viewer.text] {
            assert!(!e.to_lowercase().contains("lookups"), "{e}");
        }
        assert!(member.html.contains(">Alerts<"));
        assert!(!viewer.html.contains(">Alerts<"));
        assert!(viewer.html.contains(">Runs<"));
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
                total: 1,
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
        assert!(e.html.contains("PR reviews"));
        for gone in [
            ">Runs<",
            "CI fixes",
            ">Alerts<",
            "Skills and agents",
            "lookups",
            ">Usage<",
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
            "https://app.coderhelm.com/data-sources/activity/?range=7d",
            "https://app.coderhelm.com/settings/budget/",
            "https://github.com/example/api/pull/41",
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
            "Your Coderhelm week: Example Team, Oct 12 – Oct 19, 2026"
        );
        // Titles are escaped, not markup.
        assert!(e.html.contains("Fix &lt;script&gt; escaping &amp; quotes"));
        assert!(!e.html.contains("<script>"));
        // Usage against the limit.
        assert!(e.text.contains("8.2M of 10.0M tokens used in October 2026"));
        assert!(e.html.contains("82%"));
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
        assert_eq!(tokens(2_350_000), "2.4M");
        assert_eq!(week_key(week().1), "2026-W43");
        assert_eq!(Access::from_role("owner"), Access::Admin);
        assert_eq!(Access::from_role(""), Access::Member);
        assert_eq!(Access::from_role("viewer"), Access::Viewer);
    }

    /// `WEEKLY_SUMMARY_PREVIEW=/path/dir cargo test -p worker weekly_summary_preview -- --ignored`
    /// writes the sample email as HTML and text, to look at in a browser.
    #[test]
    #[ignore]
    fn weekly_summary_preview() {
        let dir = std::env::var("WEEKLY_SUMMARY_PREVIEW").expect("WEEKLY_SUMMARY_PREVIEW dir");
        let e = render(&sample(), Access::Admin, LINK);
        std::fs::write(format!("{dir}/weekly-summary.html"), &e.html).unwrap();
        std::fs::write(format!("{dir}/weekly-summary.txt"), &e.text).unwrap();
        let m = render(&sample(), Access::Member, LINK);
        std::fs::write(format!("{dir}/weekly-summary-member.html"), &m.html).unwrap();
    }
}
