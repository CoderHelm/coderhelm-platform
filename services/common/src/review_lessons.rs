//! Lessons the reviewer learns from people's pushback.
//!
//! When a person disputes one of CoderHelm's review findings (a reply under
//! its comment, a PR comment, dismissing its review) or closes, approves or
//! merges the PR while its "changes requested" stands, the gateway marks the PR's discussion as
//! having news ([`mark_event`]) and queues one judgment after a quiet period.
//! Only the job for the latest event judges ([`debounce`]), so one discussion
//! yields one judgment. The worker's model decides whether the reviewer was
//! wrong and, when it was, writes one general lesson.
//!
//! Lessons live on the settings table (pk = team id, sk = [`lesson_sk`]),
//! scoped to one repository or to the whole team ([`TEAM_SCOPE`]). They are
//! recalled into every review of that repository ([`recall_block`]), and the
//! team can list and delete them from the dashboard's API.

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use std::collections::HashMap;

/// Sort-key prefix of every lesson.
pub const LESSON_SK_PREFIX: &str = "REVIEWLESSON#";
/// Sort-key prefix of a PR's pending-judgment record.
pub const PENDING_SK_PREFIX: &str = "REVIEWLESSONPENDING#";
/// The scope of a lesson that holds for every repository of the team.
pub const TEAM_SCOPE: &str = "*";
/// Quiet period after the last event before the discussion is judged.
pub const DEBOUNCE_SECS: i32 = 600;
/// Most lessons kept per scope; past it the oldest go.
pub const MAX_LESSONS_PER_SCOPE: usize = 40;
/// Most lessons put in front of the reviewer.
pub const MAX_RECALLED: usize = 25;
/// How long a pending record is kept after its last write.
pub const PENDING_TTL_SECS: i64 = 14 * 86_400;

/// The pending record's states.
pub const STATUS_PENDING: &str = "pending";
pub const STATUS_JUDGING: &str = "judging";
pub const STATUS_JUDGED: &str = "judged";

/// What started a judgment.
pub const TRIGGER_THREAD_REPLY: &str = "thread_reply";
pub const TRIGGER_PR_COMMENT: &str = "pr_comment";
pub const TRIGGER_REVIEW_BODY: &str = "review_body";
pub const TRIGGER_DISMISSED: &str = "dismissed";
pub const TRIGGER_CLOSED: &str = "closed_unmerged";
/// A person approved the PR while CoderHelm's latest verdict requested
/// changes: the strongest sign its block was wrong.
pub const TRIGGER_APPROVED_OVER_BLOCK: &str = "approved_over_block";
/// A person merged the PR while CoderHelm's latest verdict requested changes.
pub const TRIGGER_MERGED_OVER_BLOCK: &str = "merged_over_block";

/// The sort-key prefix of one scope's lessons (`owner/name` or `*`).
pub fn scope_prefix(scope: &str) -> String {
    format!("{LESSON_SK_PREFIX}{scope}#")
}

/// A lesson's sort key.
pub fn lesson_sk(scope: &str, id: &str) -> String {
    format!("{}{id}", scope_prefix(scope))
}

/// A PR's pending-judgment record key.
pub fn pending_sk(owner: &str, name: &str, pr: u64) -> String {
    format!("{PENDING_SK_PREFIX}{owner}/{name}#{pr:06}")
}

/// Is `scope` a valid lesson scope: the team, or `owner/name`?
pub fn valid_scope(scope: &str) -> bool {
    if scope == TEAM_SCOPE {
        return true;
    }
    let mut parts = scope.split('/');
    let ok = |p: Option<&str>| {
        p.is_some_and(|p| {
            !p.is_empty()
                && p != "."
                && p != ".."
                && p.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        })
    };
    ok(parts.next()) && ok(parts.next()) && parts.next().is_none()
}

/// Is `sk` a lesson's sort key (and nothing else on the settings table)?
pub fn is_lesson_sk(sk: &str) -> bool {
    let Some(rest) = sk.strip_prefix(LESSON_SK_PREFIX) else {
        return false;
    };
    let Some((scope, id)) = rest.rsplit_once('#') else {
        return false;
    };
    valid_scope(scope)
        && !id.is_empty()
        && id.len() <= 64
        && id.chars().all(|c| c.is_ascii_alphanumeric())
}

/// One lesson the reviewer learned from the team's pushback.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ReviewLesson {
    pub sk: String,
    pub id: String,
    /// `owner/name`, or [`TEAM_SCOPE`].
    pub scope: String,
    pub lesson: String,
    /// The finding category it applies to (`correctness`, `style`, …).
    pub category: String,
    /// Why the reviewer was wrong, in the judge's words.
    pub why: String,
    /// Where it was learned.
    pub source_repo: String,
    pub source_pr: u64,
    pub trigger: String,
    pub created_at: String,
    /// The same lesson was learned again (from another discussion).
    pub seen: u64,
    pub last_seen_at: String,
}

type Item = HashMap<String, AttributeValue>;

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn n(item: &Item, k: &str) -> u64 {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

impl ReviewLesson {
    pub fn from_item(item: &Item) -> Option<Self> {
        let sk = s(item, "sk");
        if !is_lesson_sk(&sk) {
            return None;
        }
        let lesson = s(item, "lesson");
        if lesson.trim().is_empty() {
            return None;
        }
        Some(Self {
            id: sk.rsplit('#').next().unwrap_or("").to_string(),
            scope: s(item, "scope"),
            lesson,
            category: s(item, "category"),
            why: s(item, "why"),
            source_repo: s(item, "source_repo"),
            source_pr: n(item, "source_pr"),
            trigger: s(item, "trigger"),
            created_at: s(item, "created_at"),
            seen: n(item, "seen").max(1),
            last_seen_at: s(item, "last_seen_at"),
            sk,
        })
    }

    pub fn to_item(&self, team_id: &str) -> Item {
        let mut i = Item::new();
        let mut put = |k: &str, v: AttributeValue| {
            i.insert(k.to_string(), v);
        };
        put("pk", AttributeValue::S(team_id.to_string()));
        put("sk", AttributeValue::S(self.sk.clone()));
        put("record_type", AttributeValue::S("review_lesson".into()));
        put("scope", AttributeValue::S(self.scope.clone()));
        put("lesson", AttributeValue::S(self.lesson.clone()));
        put("category", AttributeValue::S(self.category.clone()));
        put("why", AttributeValue::S(self.why.clone()));
        put("source_repo", AttributeValue::S(self.source_repo.clone()));
        put("source_pr", AttributeValue::N(self.source_pr.to_string()));
        put("trigger", AttributeValue::S(self.trigger.clone()));
        put("created_at", AttributeValue::S(self.created_at.clone()));
        put("seen", AttributeValue::N(self.seen.max(1).to_string()));
        put("last_seen_at", AttributeValue::S(self.last_seen_at.clone()));
        i
    }
}

/// What the reconciler decided about a new lesson against the ones kept.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
pub struct Reconcile {
    /// The id of a kept lesson that already says the same thing.
    #[serde(default)]
    pub duplicate_of: Option<String>,
    /// Ids of kept lessons the new one contradicts or replaces.
    #[serde(default)]
    pub supersedes: Vec<String>,
}

/// What to write for a new lesson.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorePlan {
    /// The new lesson, unless it repeats a kept one.
    pub put: Option<ReviewLesson>,
    /// The kept lesson that repeats it: counted as seen again.
    pub seen_again: Option<String>,
    /// Sort keys removed: superseded lessons, and the oldest past the cap.
    pub delete: Vec<String>,
}

/// Pure: how a new lesson lands among the kept ones of the same scopes.
/// A duplicate is never stored twice; a contradicted lesson goes; a scope
/// keeps at most `cap` lessons, dropping its oldest.
pub fn plan_store(
    existing: &[ReviewLesson],
    new: ReviewLesson,
    decision: &Reconcile,
    cap: usize,
) -> StorePlan {
    let known = |id: &str| existing.iter().find(|l| l.id == id);
    if let Some(d) = decision.duplicate_of.as_deref().and_then(known) {
        return StorePlan {
            put: None,
            seen_again: Some(d.sk.clone()),
            delete: vec![],
        };
    }
    let mut delete: Vec<String> = decision
        .supersedes
        .iter()
        .filter_map(|id| known(id))
        .map(|l| l.sk.clone())
        .collect();
    // The cap counts the new lesson's own scope.
    let mut same_scope: Vec<&ReviewLesson> = existing
        .iter()
        .filter(|l| l.scope == new.scope && !delete.contains(&l.sk))
        .collect();
    if same_scope.len() + 1 > cap {
        same_scope.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        let over = same_scope.len() + 1 - cap;
        delete.extend(same_scope.iter().take(over).map(|l| l.sk.clone()));
    }
    StorePlan {
        put: Some(new),
        seen_again: None,
        delete,
    }
}

/// Pure: the block every review of a repository is given, newest first,
/// repository lessons ahead of team-wide ones. Empty when there are none.
pub fn recall_block(lessons: &[ReviewLesson]) -> String {
    if lessons.is_empty() {
        return String::new();
    }
    let mut sorted: Vec<&ReviewLesson> = lessons.iter().collect();
    sorted.sort_by(|a, b| {
        (a.scope == TEAM_SCOPE)
            .cmp(&(b.scope == TEAM_SCOPE))
            .then(b.created_at.cmp(&a.created_at))
    });
    let mut out = String::from(
        "\n\n## Lessons from this team's own corrections of your reviews\n\
         People on this team pushed back on earlier reviews by you, and each lesson below was \
         learned from that. They are the team's decisions about its own code: they win over your \
         default judgement. Do not raise, and never block on, a finding a lesson says is not a \
         problem here; at most say it as a note when the lesson allows that. The one exception is \
         a real security or data-loss defect, which you still report.\n",
    );
    for l in sorted.into_iter().take(MAX_RECALLED) {
        let cat = if l.category.trim().is_empty() {
            String::new()
        } else {
            format!("[{}] ", l.category.trim())
        };
        let scope = if l.scope == TEAM_SCOPE {
            "all repositories"
        } else {
            "this repository"
        };
        out.push_str(&format!(
            "- {cat}{} ({scope})\n",
            crate::truncate_str(l.lesson.trim(), 600)
        ));
    }
    out
}

/// Should a queued judgment run now?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Debounce {
    /// This job is for the discussion's latest event: judge it.
    Run,
    /// A later event queued its own job; that one judges.
    Superseded,
    /// Already judged (or being judged) since the last event.
    Done,
}

/// Pure: the debounce decision for a job queued at `msg_event_at` (ms), given
/// the PR's pending record.
pub fn debounce(status: &str, last_event_at: i64, msg_event_at: i64) -> Debounce {
    if status != STATUS_PENDING {
        return Debounce::Done;
    }
    if last_event_at != msg_event_at {
        return Debounce::Superseded;
    }
    Debounce::Run
}

/// Records news on a PR's discussion: the record goes (back) to pending with
/// this event's time, which the queued job carries. Returns that time (ms).
pub async fn mark_event(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    owner: &str,
    name: &str,
    pr: u64,
    trigger: &str,
) -> Result<i64, String> {
    let now = chrono::Utc::now();
    let at = now.timestamp_millis();
    dynamo
        .update_item()
        .table_name(table)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(pending_sk(owner, name, pr)))
        .update_expression(
            "SET #st = :p, last_event_at = :t, last_trigger = :tr, repo = :r, pr_number = :n, \
             #ttl = :ttl ADD triggers :one",
        )
        .expression_attribute_names("#st", "status")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":p", AttributeValue::S(STATUS_PENDING.into()))
        .expression_attribute_values(":t", AttributeValue::N(at.to_string()))
        .expression_attribute_values(":tr", AttributeValue::S(trigger.to_string()))
        .expression_attribute_values(":r", AttributeValue::S(format!("{owner}/{name}")))
        .expression_attribute_values(":n", AttributeValue::N(pr.to_string()))
        .expression_attribute_values(
            ":ttl",
            AttributeValue::N((now.timestamp() + PENDING_TTL_SECS).to_string()),
        )
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(at)
}

/// One scope's lessons (`owner/name` or [`TEAM_SCOPE`]).
pub async fn list_scope(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    scope: &str,
) -> Result<Vec<ReviewLesson>, String> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..5 {
        let res = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
            .expression_attribute_values(":p", AttributeValue::S(scope_prefix(scope)))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| e.to_string())?;
        out.extend(res.items().iter().filter_map(ReviewLesson::from_item));
        match res.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

/// Every lesson a review of `owner/name` is given: the repository's and the
/// team's.
pub async fn for_repo(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    owner: &str,
    name: &str,
) -> Result<Vec<ReviewLesson>, String> {
    let mut all = list_scope(dynamo, table, team_id, &format!("{owner}/{name}")).await?;
    all.extend(list_scope(dynamo, table, team_id, TEAM_SCOPE).await?);
    Ok(all)
}

/// Every lesson of the team (all scopes).
pub async fn list_all(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
) -> Result<Vec<ReviewLesson>, String> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..10 {
        let res = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
            .expression_attribute_values(":p", AttributeValue::S(LESSON_SK_PREFIX.into()))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| e.to_string())?;
        out.extend(res.items().iter().filter_map(ReviewLesson::from_item));
        match res.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lesson(scope: &str, id: &str, created_at: &str, text: &str) -> ReviewLesson {
        ReviewLesson {
            sk: lesson_sk(scope, id),
            id: id.into(),
            scope: scope.into(),
            lesson: text.into(),
            category: "correctness".into(),
            created_at: created_at.into(),
            seen: 1,
            ..Default::default()
        }
    }

    #[test]
    fn keys_hold_the_scope_and_refuse_anything_else() {
        assert_eq!(
            lesson_sk("acme/web", "01ABC"),
            "REVIEWLESSON#acme/web#01ABC"
        );
        assert_eq!(lesson_sk(TEAM_SCOPE, "01ABC"), "REVIEWLESSON#*#01ABC");
        assert_eq!(
            pending_sk("acme", "web", 42),
            "REVIEWLESSONPENDING#acme/web#000042"
        );
        assert!(is_lesson_sk("REVIEWLESSON#acme/web#01ABC"));
        assert!(is_lesson_sk("REVIEWLESSON#*#01ABC"));
        assert!(!is_lesson_sk("REVIEW#acme/web#000042#2026"));
        assert!(!is_lesson_sk("REVIEWLESSON#acme#01ABC"));
        assert!(!is_lesson_sk("REVIEWLESSON#../x#01ABC"));
        assert!(!is_lesson_sk("REVIEWLESSON#acme/web#"));
        assert!(!is_lesson_sk("REVIEWLESSONPENDING#acme/web#000042"));
    }

    #[test]
    fn a_lesson_round_trips_through_its_item() {
        let mut l = lesson("acme/web", "01ABC", "2026-10-01T00:00:00Z", "Do X.");
        l.source_repo = "acme/web".into();
        l.source_pr = 7;
        let back = ReviewLesson::from_item(&l.to_item("TEAM#1")).unwrap();
        assert_eq!(back, l);
    }

    #[test]
    fn a_duplicate_is_counted_not_stored_twice() {
        let kept = vec![lesson("acme/web", "A", "2026-01-01", "Old.")];
        let plan = plan_store(
            &kept,
            lesson("acme/web", "B", "2026-02-01", "Same."),
            &Reconcile {
                duplicate_of: Some("A".into()),
                supersedes: vec![],
            },
            40,
        );
        assert_eq!(plan.put, None);
        assert_eq!(plan.seen_again.as_deref(), Some("REVIEWLESSON#acme/web#A"));
        assert!(plan.delete.is_empty());
    }

    #[test]
    fn a_newer_lesson_replaces_the_one_it_contradicts() {
        let kept = vec![
            lesson("acme/web", "A", "2026-01-01", "Block X."),
            lesson(TEAM_SCOPE, "T", "2026-01-01", "Other."),
        ];
        let plan = plan_store(
            &kept,
            lesson("acme/web", "B", "2026-02-01", "Don't block X."),
            &Reconcile {
                duplicate_of: None,
                supersedes: vec!["A".into(), "unknown".into()],
            },
            40,
        );
        assert_eq!(plan.put.unwrap().id, "B");
        assert_eq!(plan.delete, vec!["REVIEWLESSON#acme/web#A".to_string()]);
    }

    #[test]
    fn an_unknown_duplicate_id_does_not_drop_the_lesson() {
        let plan = plan_store(
            &[],
            lesson("acme/web", "B", "2026-02-01", "New."),
            &Reconcile {
                duplicate_of: Some("nope".into()),
                supersedes: vec![],
            },
            40,
        );
        assert!(plan.put.is_some());
    }

    #[test]
    fn the_cap_drops_the_oldest_of_the_same_scope() {
        let kept = vec![
            lesson("acme/web", "C", "2026-03-01", "c"),
            lesson("acme/web", "A", "2026-01-01", "a"),
            lesson("acme/web", "B", "2026-02-01", "b"),
            lesson(TEAM_SCOPE, "T", "2025-01-01", "t"),
        ];
        let plan = plan_store(
            &kept,
            lesson("acme/web", "D", "2026-04-01", "d"),
            &Reconcile::default(),
            3,
        );
        assert_eq!(plan.delete, vec!["REVIEWLESSON#acme/web#A".to_string()]);
    }

    #[test]
    fn the_recall_block_says_lessons_win_except_safety() {
        assert_eq!(recall_block(&[]), "");
        let b = recall_block(&[
            lesson(TEAM_SCOPE, "T", "2026-05-01", "Team rule."),
            lesson("acme/web", "A", "2026-01-01", "Repo rule."),
        ]);
        assert!(b.contains("team's own corrections"));
        assert!(b.contains("win over your default judgement"));
        assert!(b.contains("security or data-loss"));
        let repo = b.find("Repo rule.").unwrap();
        let team = b.find("Team rule.").unwrap();
        assert!(repo < team, "repository lessons come first");
        assert!(b.contains("[correctness] Repo rule. (this repository)"));
        assert!(b.contains("(all repositories)"));
    }

    #[test]
    fn only_the_latest_event_judges() {
        assert_eq!(debounce(STATUS_PENDING, 5, 5), Debounce::Run);
        assert_eq!(debounce(STATUS_PENDING, 9, 5), Debounce::Superseded);
        assert_eq!(debounce(STATUS_JUDGED, 5, 5), Debounce::Done);
        assert_eq!(debounce(STATUS_JUDGING, 5, 5), Debounce::Done);
    }
}
