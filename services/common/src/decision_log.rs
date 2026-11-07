//! What the stop judge decided, and what people did afterwards.
//!
//! Every judged decision ([`crate::stop_judge::Judged`]) is kept on the
//! settings table under the team, with the picture it saw and the model's
//! answers, so a wrong one can be replayed. When a person acts on it the
//! outcome is added ([`Outcome`]): answered with the default or otherwise,
//! dismissed the review, merged with the flagged lines unchanged, cancelled
//! the run, retried it, overrode the stop, or fixed what was flagged.
//!
//! Analytics reads them as two rates ([`summarize`]): unnecessary stops (a
//! question answered with the default, a stop overridden or retried as it
//! was) and nuisance blocks (a review dismissed, overridden, or merged with
//! the lines it flagged unchanged). A confirmed-wrong decision exports as a
//! replay fixture ([`fixture`]), so it becomes a case the suite holds.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::stop_judge::{Group, Judged, Kind, Picture, Verdict};

/// The record type on the settings table.
pub const RECORD_TYPE: &str = "decision";
/// Every decision's sort key starts with this.
pub const SK_PREFIX: &str = "DECISION#";

/// Longest stored picture and replies, so an item stays far under 400 KB.
const MAX_PICTURE_BYTES: usize = 60_000;
const MAX_REPLIES_BYTES: usize = 12_000;

/// What a decision was about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subject {
    Run { run_id: String },
    Pr { repo: String, number: u64 },
}

impl Subject {
    /// The sort-key prefix of this subject's decisions.
    pub fn prefix(&self) -> String {
        match self {
            Subject::Run { run_id } => format!("{SK_PREFIX}RUN#{run_id}#"),
            Subject::Pr { repo, number } => format!("{SK_PREFIX}PR#{repo}#{number:0>6}#"),
        }
    }

    pub fn sk(&self, at: &str) -> String {
        format!("{}{at}", self.prefix())
    }
}

/// What a person did after a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Answered the question with the recommended default.
    AnsweredDefault,
    /// Answered with something else.
    AnsweredOther,
    /// Dismissed CoderHelm's request for changes.
    DismissedReview,
    /// Merged with the flagged lines unchanged since the block.
    MergedFlaggedUnchanged,
    /// Changed the flagged lines, then merged or pushed.
    Fixed,
    /// Cancelled or closed the run.
    CancelledRun,
    /// Retried the run without changing anything.
    Retried,
    /// Went ahead over the stop or block (merged over it, told the run to
    /// continue without answering).
    Overrode,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::AnsweredDefault => "answered_default",
            Outcome::AnsweredOther => "answered_other",
            Outcome::DismissedReview => "dismissed_review",
            Outcome::MergedFlaggedUnchanged => "merged_flagged_unchanged",
            Outcome::Fixed => "fixed",
            Outcome::CancelledRun => "cancelled_run",
            Outcome::Retried => "retried",
            Outcome::Overrode => "overrode",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        serde_json::from_value(Value::String(s.to_string())).ok()
    }

    /// Whether this outcome shows the decision was wrong (`Some(true)`),
    /// right (`Some(false)`), or says nothing about it (`None`).
    pub fn confirms_wrong(self, group: Group) -> Option<bool> {
        match (group, self) {
            (Group::Stop, Outcome::AnsweredDefault | Outcome::Retried | Outcome::Overrode) => {
                Some(true)
            }
            (Group::Stop, Outcome::AnsweredOther) => Some(false),
            (
                Group::Block,
                Outcome::DismissedReview | Outcome::MergedFlaggedUnchanged | Outcome::Overrode,
            ) => Some(true),
            (Group::Block, Outcome::Fixed) => Some(false),
            _ => None,
        }
    }
}

/// One decision as stored.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub sk: String,
    pub kind: String,
    pub step: String,
    pub verdict: String,
    /// `model`, `model_reasked`, `rule:...`, `fallback:...`.
    pub source: String,
    /// The category analytics groups by: the block's category, the kind of
    /// fact a question asks for, or the step for a stop.
    pub category: String,
    /// The step's reason for proposing it.
    pub reason: String,
    /// The blocker, the question, or the assumption.
    pub detail: String,
    /// The default answer of a question.
    #[serde(default)]
    pub default_answer: String,
    /// The location a block flagged.
    #[serde(default)]
    pub location: String,
    /// The pull request head the decision was made on.
    #[serde(default)]
    pub head_sha: String,
    #[serde(default)]
    pub run_id: String,
    #[serde(default)]
    pub repo: String,
    #[serde(default)]
    pub pr_number: u64,
    pub created_at: String,
    #[serde(default)]
    pub outcome: Option<Outcome>,
    #[serde(default)]
    pub outcome_at: String,
    /// The picture the judge saw (JSON, bounded).
    #[serde(default)]
    pub picture: String,
    /// The model's answers (JSON array, bounded).
    #[serde(default)]
    pub replies: String,
}

impl Record {
    /// Built from a judged decision and the picture it saw.
    pub fn new(subject: &Subject, judged: &Judged, picture: &Picture, head_sha: &str) -> Self {
        let created_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let (category, detail, default_answer, location) = match judged.verdict() {
            Verdict::Proceed { assumption } => (
                judged.step().to_string(),
                assumption.clone(),
                "".into(),
                "".into(),
            ),
            Verdict::Ask(q) => (
                serde_json::to_value(q.person_only)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default(),
                q.question.clone(),
                q.default.clone(),
                String::new(),
            ),
            Verdict::Hold(h) => (
                h.category
                    .map(|c| c.as_str().to_string())
                    .unwrap_or_else(|| judged.step().to_string()),
                h.blocker.clone(),
                String::new(),
                h.location.clone(),
            ),
        };
        let source = match judged.source() {
            crate::stop_judge::Source::Model => "model".to_string(),
            crate::stop_judge::Source::ModelReasked => "model_reasked".to_string(),
            crate::stop_judge::Source::Rule(r) => format!("rule:{r}"),
            crate::stop_judge::Source::Fallback(r) => {
                format!("fallback:{}", crate::truncate_str(r, 400))
            }
        };
        let (run_id, repo, pr_number) = match subject {
            Subject::Run { run_id } => (run_id.clone(), String::new(), 0),
            Subject::Pr { repo, number } => (String::new(), repo.clone(), *number),
        };
        Record {
            sk: subject.sk(&created_at),
            kind: judged.kind().as_str().to_string(),
            step: judged.step().to_string(),
            verdict: judged.verdict().as_str().to_string(),
            source,
            category,
            reason: crate::truncate_str(&picture.reason, 2_000).to_string(),
            detail: crate::truncate_str(&detail, 2_000).to_string(),
            default_answer,
            location,
            head_sha: head_sha.to_string(),
            run_id,
            repo,
            pr_number,
            created_at,
            outcome: None,
            outcome_at: String::new(),
            picture: crate::head_tail_str(
                &serde_json::to_string(picture).unwrap_or_default(),
                MAX_PICTURE_BYTES,
            ),
            replies: crate::head_tail_str(
                &serde_json::to_string(judged.replies()).unwrap_or_default(),
                MAX_REPLIES_BYTES,
            ),
        }
    }

    fn kind(&self) -> Option<Kind> {
        serde_json::from_value(Value::String(self.kind.clone())).ok()
    }

    pub fn group(&self) -> Group {
        self.kind().map(Kind::group).unwrap_or(Group::Stop)
    }

    /// A stop or a block actually happened (the judge asked or held). A
    /// proceed costs no one anything, so it is never a nuisance.
    pub fn stopped(&self) -> bool {
        self.verdict == "ask" || self.verdict == "hold"
    }

    /// The outcome shows this decision was wrong.
    pub fn confirmed_wrong(&self) -> bool {
        self.stopped()
            && self
                .outcome
                .and_then(|o| o.confirms_wrong(self.group()))
                .unwrap_or(false)
    }

    pub fn to_item(&self, team_id: &str) -> HashMap<String, AttributeValue> {
        let s = |v: &str| AttributeValue::S(v.to_string());
        let mut item = HashMap::new();
        item.insert("pk".into(), s(team_id));
        item.insert("sk".into(), s(&self.sk));
        item.insert("record_type".into(), s(RECORD_TYPE));
        for (k, v) in [
            ("kind", &self.kind),
            ("step", &self.step),
            ("verdict", &self.verdict),
            ("source", &self.source),
            ("category", &self.category),
            ("reason", &self.reason),
            ("detail", &self.detail),
            ("default_answer", &self.default_answer),
            ("location", &self.location),
            ("head_sha", &self.head_sha),
            ("run_id", &self.run_id),
            ("repo", &self.repo),
            ("created_at", &self.created_at),
            ("picture", &self.picture),
            ("replies", &self.replies),
        ] {
            if !v.is_empty() {
                item.insert(k.into(), s(v));
            }
        }
        if self.pr_number > 0 {
            item.insert(
                "pr_number".into(),
                AttributeValue::N(self.pr_number.to_string()),
            );
        }
        if let Some(o) = self.outcome {
            item.insert("outcome".into(), s(o.as_str()));
            item.insert("outcome_at".into(), s(&self.outcome_at));
        }
        item
    }

    pub fn from_item(item: &HashMap<String, AttributeValue>) -> Self {
        let s = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        Record {
            sk: s("sk"),
            kind: s("kind"),
            step: s("step"),
            verdict: s("verdict"),
            source: s("source"),
            category: s("category"),
            reason: s("reason"),
            detail: s("detail"),
            default_answer: s("default_answer"),
            location: s("location"),
            head_sha: s("head_sha"),
            run_id: s("run_id"),
            repo: s("repo"),
            pr_number: item
                .get("pr_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0),
            created_at: s("created_at"),
            outcome: Outcome::parse(&s("outcome")),
            outcome_at: s("outcome_at"),
            picture: s("picture"),
            replies: s("replies"),
        }
    }
}

/// Keeps a decision. Best effort: a failed write never changes the decision.
pub async fn record(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    rec: &Record,
) -> bool {
    if table.is_empty() {
        return false;
    }
    match dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(rec.to_item(team_id)))
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            tracing::warn!(team_id, sk = %rec.sk, error = %e, "Could not keep the stop judge's decision");
            false
        }
    }
}

/// A subject's decisions, oldest first.
pub async fn for_subject(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    subject: &Subject,
) -> Vec<Record> {
    query_prefix(dynamo, table, team_id, &subject.prefix(), None, 5).await
}

/// Decisions under a sort-key prefix, oldest first, newer than `since`
/// when given.
pub async fn query_prefix(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    prefix: &str,
    since: Option<&str>,
    max_pages: usize,
) -> Vec<Record> {
    let mut out = Vec::new();
    if table.is_empty() {
        return out;
    }
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    for _ in 0..max_pages {
        let mut q = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.into()))
            .expression_attribute_values(":p", AttributeValue::S(prefix.into()))
            .set_exclusive_start_key(start.take());
        if let Some(since) = since {
            q = q
                .filter_expression("created_at >= :since")
                .expression_attribute_values(":since", AttributeValue::S(since.into()));
        }
        let Ok(page) = q.send().await else {
            break;
        };
        out.extend(page.items().iter().map(Record::from_item));
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out
}

/// Which open decisions an outcome lands on: the subject's stops and blocks
/// that have no outcome yet. `pick` narrows them (by kind, by head).
pub fn pending(records: &[Record], group: Group) -> Vec<&Record> {
    records
        .iter()
        .filter(|r| r.stopped() && r.outcome.is_none() && r.group() == group)
        .collect()
}

/// Sets an outcome on one decision, only if it has none (a later act never
/// rewrites the first one).
pub async fn set_outcome(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    sk: &str,
    outcome: Outcome,
) -> bool {
    if table.is_empty() {
        return false;
    }
    dynamo
        .update_item()
        .table_name(table)
        .key("pk", AttributeValue::S(team_id.into()))
        .key("sk", AttributeValue::S(sk.into()))
        .update_expression("SET outcome = :o, outcome_at = :t")
        .condition_expression("attribute_exists(sk) AND attribute_not_exists(outcome)")
        .expression_attribute_values(":o", AttributeValue::S(outcome.as_str().into()))
        .expression_attribute_values(":t", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .is_ok()
}

/// Lands one outcome on a subject's open decisions of `group`; returns how
/// many it set. `choose` picks the outcome per decision (a merge is
/// "unchanged" for one block and "fixed" for another).
pub async fn land(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    subject: &Subject,
    group: Group,
    choose: impl Fn(&Record) -> Option<Outcome>,
) -> usize {
    let records = for_subject(dynamo, table, team_id, subject).await;
    let mut n = 0;
    for r in pending(&records, group) {
        if let Some(o) = choose(r) {
            if set_outcome(dynamo, table, team_id, &r.sk, o).await {
                n += 1;
            }
        }
    }
    n
}

/// Whether a person's answer took the recommended default.
pub fn answered_default(default: &str, answer: &str) -> bool {
    let norm = |s: &str| {
        s.to_ascii_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let (d, a) = (norm(default), norm(answer));
    !d.is_empty()
        && (a == d
            || a.starts_with(&d)
            || matches!(
                a.as_str(),
                "yes" | "ok" | "default" | "go ahead" | "sounds good" | "recommended"
            ))
}

/// The outcome of a merge for a block: unchanged when the flagged file was
/// not touched after the block's head (or the head is the same), else fixed.
/// `changed_since` is the files changed between the block's head and the
/// merge, when known.
pub fn merge_outcome(rec: &Record, merge_head: &str, changed_since: Option<&[String]>) -> Outcome {
    if !rec.head_sha.is_empty() && rec.head_sha == merge_head {
        return Outcome::MergedFlaggedUnchanged;
    }
    let path = rec
        .location
        .split([':', '#', ' '])
        .next()
        .unwrap_or("")
        .trim();
    match changed_since {
        Some(files) if !path.is_empty() => {
            if files.iter().any(|f| f == path) {
                Outcome::Fixed
            } else {
                Outcome::MergedFlaggedUnchanged
            }
        }
        // New commits and nothing to compare: the change was looked at.
        _ => Outcome::Fixed,
    }
}

// ── Where people's acts land ───────────────────────────────────────────────

/// A person answered a run's questions: each open ask on the run is answered
/// with its default or otherwise; an open hold was answered (the person gave
/// what it named).
pub async fn on_answers(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    run_id: &str,
    answers: &[String],
) -> usize {
    let subject = Subject::Run {
        run_id: run_id.to_string(),
    };
    land(dynamo, table, team_id, &subject, Group::Stop, |r| {
        Some(if r.verdict == "ask" {
            if answers
                .iter()
                .any(|a| answered_default(&r.default_answer, a))
            {
                Outcome::AnsweredDefault
            } else {
                Outcome::AnsweredOther
            }
        } else {
            Outcome::AnsweredOther
        })
    })
    .await
}

/// A person ended or restarted a run: its open stops get that outcome.
pub async fn on_run_acted(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    run_id: &str,
    outcome: Outcome,
) -> usize {
    let subject = Subject::Run {
        run_id: run_id.to_string(),
    };
    land(dynamo, table, team_id, &subject, Group::Stop, |_| {
        Some(outcome)
    })
    .await
}

/// A person dismissed CoderHelm's request for changes on a pull request.
pub async fn on_review_dismissed(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    repo: &str,
    pr: u64,
) -> usize {
    let subject = Subject::Pr {
        repo: repo.to_string(),
        number: pr,
    };
    land(dynamo, table, team_id, &subject, Group::Block, |_| {
        Some(Outcome::DismissedReview)
    })
    .await
}

/// A pull request merged: each open block on it was either left as it was
/// (merged with the flagged lines unchanged) or acted on (fixed).
pub async fn on_merged(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    repo: &str,
    pr: u64,
    merge_head: &str,
    changed_since: Option<&[String]>,
) -> usize {
    let subject = Subject::Pr {
        repo: repo.to_string(),
        number: pr,
    };
    land(dynamo, table, team_id, &subject, Group::Block, |r| {
        Some(merge_outcome(r, merge_head, changed_since))
    })
    .await
}

/// A person approved the pull request while CoderHelm's block stood: each
/// open block on it was overridden.
pub async fn on_approved_over_block(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    repo: &str,
    pr: u64,
) -> usize {
    let subject = Subject::Pr {
        repo: repo.to_string(),
        number: pr,
    };
    land(dynamo, table, team_id, &subject, Group::Block, |_| {
        Some(Outcome::Overrode)
    })
    .await
}

/// The pull request heads the open blocks on it were decided on (so a merge
/// can compare each head with the merged one).
pub async fn open_block_heads(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    repo: &str,
    pr: u64,
) -> Vec<String> {
    let subject = Subject::Pr {
        repo: repo.to_string(),
        number: pr,
    };
    let records = for_subject(dynamo, table, team_id, &subject).await;
    let mut heads: Vec<String> = pending(&records, Group::Block)
        .iter()
        .map(|r| r.head_sha.clone())
        .filter(|h| !h.is_empty())
        .collect();
    heads.sort();
    heads.dedup();
    heads
}

/// A merge's outcome for one block, given the files changed since each
/// block's head (`changed_by_head`, from a compare of that head with the
/// merged one). A block with no compare falls back to [`merge_outcome`]
/// without one.
pub fn merge_outcome_by_head(
    rec: &Record,
    merge_head: &str,
    changed_by_head: &HashMap<String, Vec<String>>,
) -> Outcome {
    merge_outcome(
        rec,
        merge_head,
        changed_by_head.get(&rec.head_sha).map(|v| v.as_slice()),
    )
}

/// A pull request merged, with the files changed since each open block's
/// head: a block whose flagged file was never touched merged unchanged
/// (it wasn't needed), one whose file changed was fixed.
pub async fn on_merged_by_head(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    repo: &str,
    pr: u64,
    merge_head: &str,
    changed_by_head: &HashMap<String, Vec<String>>,
) -> usize {
    let subject = Subject::Pr {
        repo: repo.to_string(),
        number: pr,
    };
    land(dynamo, table, team_id, &subject, Group::Block, |r| {
        Some(merge_outcome_by_head(r, merge_head, changed_by_head))
    })
    .await
}

/// The judge's memory of the outcome log: earlier stops or blocks of `kind`
/// that people's acts showed wrong, newest first, the same step first, one
/// line each (`category`: what it said, and what people did).
pub fn overturned_lines(records: &[Record], kind: &str, step: &str) -> Vec<String> {
    let mut wrong: Vec<&Record> = records
        .iter()
        .filter(|r| r.kind == kind && r.confirmed_wrong())
        .collect();
    wrong.sort_by(|a, b| {
        (b.step == step)
            .cmp(&(a.step == step))
            .then(b.created_at.cmp(&a.created_at))
    });
    let mut out: Vec<String> = Vec::new();
    for r in wrong {
        let line = format!(
            "{} by the {} step ({}): \"{}\"; people {}",
            r.verdict,
            r.step,
            if r.category.is_empty() {
                "no category"
            } else {
                r.category.as_str()
            },
            crate::truncate_str(r.detail.trim(), 200),
            match r.outcome {
                Some(Outcome::AnsweredDefault) => "answered with the default it offered",
                Some(Outcome::Retried) => "retried the run as it was",
                Some(Outcome::Overrode) => "went ahead over it",
                Some(Outcome::DismissedReview) => "dismissed the review",
                Some(Outcome::MergedFlaggedUnchanged) => "merged with the flagged lines unchanged",
                _ => "overturned it",
            }
        );
        if !out.contains(&line) {
            out.push(line);
        }
        if out.len() >= crate::stop_judge::MAX_OVERTURNED {
            break;
        }
    }
    out
}

/// Decisions newer than `since`, without the stored picture and answers
/// (for counts and the judge's memory: a few kilobytes, not megabytes).
pub async fn since_light(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    since: &str,
    max_pages: usize,
) -> Vec<Record> {
    let mut out = Vec::new();
    if table.is_empty() {
        return out;
    }
    let fields = [
        "sk",
        "kind",
        "step",
        "verdict",
        "category",
        "detail",
        "outcome",
        "outcome_at",
        "created_at",
        "head_sha",
        "location",
        "repo",
        "pr_number",
        "run_id",
    ];
    let names: Vec<String> = (0..fields.len()).map(|i| format!("#f{i}")).collect();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    for _ in 0..max_pages.max(1) {
        let mut q = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .filter_expression("#f8 >= :since")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.into()))
            .expression_attribute_values(":p", AttributeValue::S(SK_PREFIX.into()))
            .expression_attribute_values(":since", AttributeValue::S(since.into()))
            .projection_expression(names.join(", "))
            .set_exclusive_start_key(start.take());
        for (alias, name) in names.iter().zip(fields) {
            q = q.expression_attribute_names(alias, name);
        }
        let Ok(page) = q.send().await else {
            break;
        };
        out.extend(page.items().iter().map(Record::from_item));
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out
}

/// Decisions newer than `since` (RFC 3339), across the team.
pub async fn since(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    since: &str,
) -> Vec<Record> {
    query_prefix(dynamo, table, team_id, SK_PREFIX, Some(since), 20).await
}

// ── Analytics ──────────────────────────────────────────────────────────────

/// One example of a confirmed-wrong decision, for the view.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Example {
    pub sk: String,
    pub kind: String,
    pub step: String,
    pub category: String,
    pub detail: String,
    pub outcome: String,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub repo: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub pr_number: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// A rate of wrong stops or blocks.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Rate {
    /// Stops (or blocks) the judge let happen.
    pub total: u64,
    /// Of those, the ones a person's act settled either way.
    pub with_outcome: u64,
    /// Of those, confirmed wrong.
    pub wrong: u64,
    /// `wrong / with_outcome` (0 when nothing has an outcome yet).
    pub rate: f64,
    /// Wrong ones by category.
    pub by_category: Vec<(String, u64)>,
    /// The newest wrong ones.
    pub examples: Vec<Example>,
}

/// Both views and the decisions the judge turned around.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    pub unnecessary_stops: Rate,
    pub nuisance_blocks: Rate,
    /// Decisions the judge made in the window, stops and proceeds alike.
    pub decisions: u64,
    /// Of those, proceeds (the step proposed a stop, the work went on).
    pub proceeded: u64,
}

const MAX_EXAMPLES: usize = 10;

fn rate(records: &[&Record]) -> Rate {
    let total = records.len() as u64;
    let with_outcome: Vec<&&Record> = records
        .iter()
        .filter(|r| {
            r.outcome
                .and_then(|o| o.confirms_wrong(r.group()))
                .is_some()
        })
        .collect();
    let mut wrong: Vec<&&Record> = with_outcome
        .iter()
        .copied()
        .filter(|r| r.confirmed_wrong())
        .collect();
    wrong.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    let mut cats: HashMap<String, u64> = HashMap::new();
    for r in &wrong {
        *cats.entry(r.category.clone()).or_default() += 1;
    }
    let mut by_category: Vec<(String, u64)> = cats.into_iter().collect();
    by_category.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let w = wrong.len() as u64;
    let n = with_outcome.len() as u64;
    Rate {
        total,
        with_outcome: n,
        wrong: w,
        rate: if n == 0 { 0.0 } else { w as f64 / n as f64 },
        by_category,
        examples: wrong
            .iter()
            .take(MAX_EXAMPLES)
            .map(|r| Example {
                sk: r.sk.clone(),
                kind: r.kind.clone(),
                step: r.step.clone(),
                category: r.category.clone(),
                detail: crate::truncate_str(&r.detail, 300).to_string(),
                outcome: r
                    .outcome
                    .map(|o| o.as_str().to_string())
                    .unwrap_or_default(),
                created_at: r.created_at.clone(),
                run_id: r.run_id.clone(),
                repo: r.repo.clone(),
                pr_number: r.pr_number,
            })
            .collect(),
    }
}

/// The two views over a window's decisions.
pub fn summarize(records: &[Record]) -> Summary {
    let stops: Vec<&Record> = records
        .iter()
        .filter(|r| r.stopped() && r.group() == Group::Stop)
        .collect();
    let blocks: Vec<&Record> = records
        .iter()
        .filter(|r| r.stopped() && r.group() == Group::Block)
        .collect();
    Summary {
        unnecessary_stops: rate(&stops),
        nuisance_blocks: rate(&blocks),
        decisions: records.len() as u64,
        proceeded: records.iter().filter(|r| r.verdict == "proceed").count() as u64,
    }
}

/// The weekly email's line, or `None` when there was nothing to judge.
pub fn weekly_line(s: &Summary) -> Option<String> {
    let st = &s.unnecessary_stops;
    let bl = &s.nuisance_blocks;
    if st.total == 0 && bl.total == 0 {
        return None;
    }
    let pct = |r: &Rate| {
        if r.with_outcome == 0 {
            "no outcomes yet".to_string()
        } else {
            format!(
                "{:.0}% ({} of {} with an outcome)",
                r.rate * 100.0,
                r.wrong,
                r.with_outcome
            )
        }
    };
    Some(format!(
        "Stops and blocks: {} stops, unnecessary {}; {} blocks, nuisance {}.",
        st.total,
        pct(st),
        bl.total,
        pct(bl)
    ))
}

// ── Replay fixtures ────────────────────────────────────────────────────────

/// A confirmed-wrong decision as a replay fixture: the picture it saw, the
/// model's recorded answers, and the decision people's act says was right.
/// Names are the team's own; scrub them before committing the file.
pub fn fixture(rec: &Record) -> Value {
    let picture: Value = serde_json::from_str(&rec.picture).unwrap_or(Value::Null);
    let replies: Value = serde_json::from_str(&rec.replies).unwrap_or(json!([]));
    let expect = match rec.group() {
        Group::Block => "proceed",
        Group::Stop => "proceed",
    };
    json!({
        "name": format!("{}-{}", rec.step, rec.created_at.replace([':', '.'], "-")),
        "about": format!(
            "Exported: the judge chose {} on {}; the outcome was {} ({}). Scrub names, then \
             record three trials (REPLAY_RECORD=1) once the decision is right.",
            rec.verdict,
            rec.kind,
            rec.outcome.map(|o| o.as_str()).unwrap_or("none"),
            crate::truncate_str(&rec.detail, 200)
        ),
        "case": "stop_judge",
        "k": 1,
        "input": { "picture": picture },
        "replies": [replies],
        "expect": { "verdict": expect, "never_ask": true }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stop_judge::{self, Evidence, JudgeModel};

    struct One(Value);
    impl JudgeModel for One {
        async fn answer(&self, _: &str, _: &str, _: &Value) -> Result<Value, String> {
            Ok(self.0.clone())
        }
    }

    fn judged(kind: Kind, reply: Value) -> (Judged, Picture) {
        let mut p = Picture::new(kind, "review", "a finding");
        p.evidence = vec![Evidence {
            source: "ci".into(),
            finding: "fails".into(),
            location: "src/a.rs:3".into(),
            checked: true,
            ..Default::default()
        }];
        let j = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(stop_judge::judge(&One(reply), p.clone()));
        (j, p)
    }

    fn hold() -> Value {
        json!({"verdict":"hold","assumption":"","question":"","person_only":"","tried":[],
               "options":[],"default":"","deadline_hours":0,"blocker":"tests fail",
               "evidence":["E1"],"category":"correctness","location":"src/a.rs:3"})
    }

    #[test]
    fn a_record_round_trips_through_the_item() {
        let (j, p) = judged(Kind::Block, hold());
        let subject = Subject::Pr {
            repo: "acme/site".into(),
            number: 7,
        };
        let mut r = Record::new(&subject, &j, &p, "abc123");
        r.outcome = Some(Outcome::DismissedReview);
        r.outcome_at = "2026-11-06T00:00:00Z".into();
        let back = Record::from_item(&r.to_item("TEAM#1"));
        assert_eq!(back, r);
        assert!(back.sk.starts_with("DECISION#PR#acme/site#000007#"));
        assert_eq!(back.category, "correctness");
        assert!(back.confirmed_wrong());
    }

    #[test]
    fn outcomes_say_wrong_right_or_nothing() {
        use Outcome::*;
        assert_eq!(AnsweredDefault.confirms_wrong(Group::Stop), Some(true));
        assert_eq!(AnsweredOther.confirms_wrong(Group::Stop), Some(false));
        assert_eq!(CancelledRun.confirms_wrong(Group::Stop), None);
        assert_eq!(DismissedReview.confirms_wrong(Group::Block), Some(true));
        assert_eq!(
            MergedFlaggedUnchanged.confirms_wrong(Group::Block),
            Some(true)
        );
        assert_eq!(Fixed.confirms_wrong(Group::Block), Some(false));
        for o in [
            AnsweredDefault,
            AnsweredOther,
            DismissedReview,
            MergedFlaggedUnchanged,
            Fixed,
            CancelledRun,
            Retried,
            Overrode,
        ] {
            assert_eq!(Outcome::parse(o.as_str()), Some(o));
        }
    }

    #[test]
    fn the_summary_counts_rates_categories_and_examples() {
        let (j, p) = judged(Kind::Block, hold());
        let pr = Subject::Pr {
            repo: "acme/site".into(),
            number: 1,
        };
        let mut a = Record::new(&pr, &j, &p, "h");
        a.outcome = Some(Outcome::MergedFlaggedUnchanged);
        let mut b = Record::new(&pr, &j, &p, "h");
        b.outcome = Some(Outcome::Fixed);
        let c = Record::new(&pr, &j, &p, "h");
        let s = summarize(&[a, b, c]);
        assert_eq!(s.nuisance_blocks.total, 3);
        assert_eq!(s.nuisance_blocks.with_outcome, 2);
        assert_eq!(s.nuisance_blocks.wrong, 1);
        assert!((s.nuisance_blocks.rate - 0.5).abs() < 1e-9);
        assert_eq!(
            s.nuisance_blocks.by_category,
            vec![("correctness".into(), 1)]
        );
        assert_eq!(s.nuisance_blocks.examples.len(), 1);
        assert_eq!(s.unnecessary_stops.total, 0);
        assert!(weekly_line(&s).unwrap().contains("nuisance 50% (1 of 2"));
        assert!(weekly_line(&Summary::default()).is_none());
    }

    #[test]
    fn a_proceed_is_never_a_nuisance() {
        let (j, p) = judged(
            Kind::Ask,
            json!({"verdict":"proceed","assumption":"monthly","question":"","person_only":"",
                   "tried":[],"options":[],"default":"","deadline_hours":0,"blocker":"",
                   "evidence":[],"category":"","location":""}),
        );
        let mut r = Record::new(
            &Subject::Run {
                run_id: "r1".into(),
            },
            &j,
            &p,
            "",
        );
        r.outcome = Some(Outcome::Overrode);
        assert!(!r.confirmed_wrong());
        let s = summarize(&[r]);
        assert_eq!(s.unnecessary_stops.total, 0);
        assert_eq!(s.proceeded, 1);
    }

    #[test]
    fn a_merge_is_unchanged_or_fixed_by_the_flagged_file() {
        let (j, p) = judged(Kind::Block, hold());
        let r = Record::new(
            &Subject::Pr {
                repo: "a/b".into(),
                number: 1,
            },
            &j,
            &p,
            "h1",
        );
        assert_eq!(
            merge_outcome(&r, "h1", None),
            Outcome::MergedFlaggedUnchanged
        );
        let other = vec!["README.md".to_string()];
        assert_eq!(
            merge_outcome(&r, "h2", Some(&other)),
            Outcome::MergedFlaggedUnchanged
        );
        let touched = vec!["src/a.rs".to_string()];
        assert_eq!(merge_outcome(&r, "h2", Some(&touched)), Outcome::Fixed);
    }

    /// The merge webhook used to pass no compare, so every block on a PR
    /// with any later commit read as "fixed" and none as a nuisance.
    #[test]
    fn a_merge_compares_each_blocks_own_head() {
        let (j, p) = judged(Kind::Block, hold());
        let pr = Subject::Pr {
            repo: "a/b".into(),
            number: 1,
        };
        let r = Record::new(&pr, &j, &p, "h1");
        let mut by_head = HashMap::new();
        by_head.insert("h1".to_string(), vec!["README.md".to_string()]);
        assert_eq!(
            merge_outcome_by_head(&r, "h9", &by_head),
            Outcome::MergedFlaggedUnchanged
        );
        by_head.insert("h1".to_string(), vec!["src/a.rs".to_string()]);
        assert_eq!(merge_outcome_by_head(&r, "h9", &by_head), Outcome::Fixed);
    }

    #[test]
    fn overturned_decisions_become_the_judges_memory() {
        let (j, p) = judged(Kind::Block, hold());
        let pr = Subject::Pr {
            repo: "a/b".into(),
            number: 1,
        };
        let mut wrong = Record::new(&pr, &j, &p, "h1");
        wrong.outcome = Some(Outcome::Overrode);
        wrong.created_at = "2026-11-02T00:00:00Z".into();
        let mut right = Record::new(&pr, &j, &p, "h1");
        right.outcome = Some(Outcome::Fixed);
        let open = Record::new(&pr, &j, &p, "h1");
        let mut other_step = wrong.clone();
        other_step.step = "own_review".into();
        other_step.created_at = "2026-11-05T00:00:00Z".into();
        other_step.detail = "another".into();
        let lines = overturned_lines(&[wrong.clone(), right, open, other_step], "block", "review");
        assert_eq!(lines.len(), 2, "{lines:?}");
        // The same step first, even when older.
        assert!(
            lines[0].contains("by the review step (correctness)"),
            "{lines:?}"
        );
        assert!(lines[0].contains("people went ahead over it"));
        assert!(lines[1].contains("own_review"));
        assert!(overturned_lines(&[wrong], "ask", "review").is_empty());
    }

    #[test]
    fn answers_that_take_the_default() {
        assert!(answered_default("Monthly totals", "monthly totals"));
        assert!(answered_default("Monthly totals", "Monthly totals, please"));
        assert!(answered_default("Monthly totals", "yes"));
        assert!(!answered_default("Monthly totals", "Yearly"));
        assert!(!answered_default("", "yes"));
    }

    #[test]
    fn a_wrong_decision_exports_as_a_fixture_the_suite_reads() {
        let (j, p) = judged(Kind::Block, hold());
        let mut r = Record::new(
            &Subject::Pr {
                repo: "a/b".into(),
                number: 1,
            },
            &j,
            &p,
            "h",
        );
        r.outcome = Some(Outcome::DismissedReview);
        let f = fixture(&r);
        assert_eq!(f["case"], "stop_judge");
        assert_eq!(f["expect"]["verdict"], "proceed");
        assert_eq!(f["input"]["picture"]["step"], "review");
        assert_eq!(f["replies"][0][0]["verdict"], "hold");
        let back: Picture = serde_json::from_value(f["input"]["picture"].clone()).unwrap();
        assert_eq!(back.kind, Some(Kind::Block));
    }
}
