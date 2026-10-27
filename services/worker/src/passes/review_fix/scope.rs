//! A review thread asks for one change; the fix changes only that. Code
//! next to it that looks alike but that the thread did not name stays as it
//! is. After the agent's fix a judge reads each hunk against the threads and
//! says whether a thread asked for it; the ones none asked for are taken
//! back out before the push.

use serde_json::{json, Value};

use super::staged::NumberedHunk;

/// What the judge is told.
pub const SYSTEM: &str = "You check that a fix for pull request review comments changes only \
what the comments ask for. You get the review comments (each thread's request, with the file \
and line it is on) and the fix as numbered hunks. For every hunk, say whether a comment asked \
for it. \"requested\": the hunk does what a comment asks, or is needed for it to work (an \
import, a test for the requested change, a call site the requested change forces to change, a \
fix for a CI failure listed with the comments). \"out_of_scope\": nothing in the comments asks \
for it: a similar line elsewhere the comment did not name, the same change applied to \
neighbouring code, a refactor, a rename, reformatting, or a cleanup. When a comment names one \
place, changes to other places that look the same are out of scope unless the comment says to \
change all of them. Name the comment number a requested hunk serves. Give every hunk id exactly \
once.";

/// The judge's answer schema.
pub fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "hunks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "verdict": {"type": "string", "enum": ["requested", "out_of_scope"]},
                        "comment": {"type": "integer", "description": "The comment number a requested hunk serves; 0 when none"},
                        "reason": {"type": "string", "description": "One short sentence"}
                    },
                    "required": ["id", "verdict", "comment", "reason"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["hunks"],
        "additionalProperties": false
    })
}

/// One request a review thread makes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub path: String,
    pub line: Option<u64>,
    pub body: String,
    /// Earlier comments of the same thread, for context.
    pub context: Vec<String>,
}

/// The judge's prompt.
pub fn prompt(review_body: &str, requests: &[Request], ci_failures: bool, hunks: &str) -> String {
    let mut out = String::new();
    if !review_body.trim().is_empty() {
        out.push_str(&format!(
            "## Review summary (comment 0)\n{}\n\n",
            review_body.trim()
        ));
    }
    out.push_str("## Review comments\n");
    for (i, r) in requests.iter().enumerate() {
        let at = match r.line {
            Some(l) if !r.path.is_empty() => format!("`{}` line {l}", r.path),
            _ if !r.path.is_empty() => format!("`{}`", r.path),
            _ => "the pull request".to_string(),
        };
        out.push_str(&format!("### Comment {} on {at}\n", i + 1));
        for c in &r.context {
            out.push_str(&format!("> {}\n", c.trim()));
        }
        out.push_str(r.body.trim());
        out.push_str("\n\n");
    }
    if ci_failures {
        out.push_str(
            "CI failures were listed with the comments too: a hunk that fixes a failing check is \
             requested.\n\n",
        );
    }
    out.push_str("## The fix, as numbered hunks\n");
    out.push_str(hunks);
    out
}

/// One hunk's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub id: usize,
    pub requested: bool,
    pub comment: usize,
    pub reason: String,
}

/// The judge's answer read: the verdicts for known ids, and the ids it
/// left out. None when the answer is not the schema's shape.
pub fn parse(answer: &Value, ids: &[usize]) -> Option<(Vec<Verdict>, Vec<usize>)> {
    let arr = answer.get("hunks")?.as_array()?;
    let mut verdicts: Vec<Verdict> = Vec::new();
    for h in arr {
        let id = h.get("id")?.as_u64()? as usize;
        if !ids.contains(&id) || verdicts.iter().any(|v| v.id == id) {
            continue;
        }
        let requested = match h.get("verdict")?.as_str()? {
            "requested" => true,
            "out_of_scope" => false,
            _ => return None,
        };
        verdicts.push(Verdict {
            id,
            requested,
            comment: h.get("comment").and_then(|c| c.as_u64()).unwrap_or(0) as usize,
            reason: h
                .get("reason")
                .and_then(|r| r.as_str())
                .unwrap_or("")
                .trim()
                .to_string(),
        });
    }
    let missing = ids
        .iter()
        .filter(|id| !verdicts.iter().any(|v| v.id == **id))
        .copied()
        .collect();
    Some((verdicts, missing))
}

/// The ids to take back out.
pub fn out_of_scope(verdicts: &[Verdict]) -> Vec<usize> {
    verdicts
        .iter()
        .filter(|v| !v.requested)
        .map(|v| v.id)
        .collect()
}

/// What the scope check found, for the evidence: one line per hunk taken
/// out, with the file and the judge's reason.
pub fn reverted_lines(verdicts: &[Verdict], hunks: &[NumberedHunk]) -> Vec<String> {
    verdicts
        .iter()
        .filter(|v| !v.requested)
        .filter_map(|v| {
            let h = hunks.iter().find(|h| h.id == v.id)?;
            Some(format!(
                "`{}` line {}: {}",
                h.hunk.path,
                h.hunk.old_start + 1,
                if v.reason.is_empty() {
                    "no review comment asked for it"
                } else {
                    &v.reason
                }
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdicts_read_with_unknown_and_duplicate_ids_dropped_and_missing_named() {
        let answer = json!({"hunks": [
            {"id": 1, "verdict": "requested", "comment": 1, "reason": "the asked rename"},
            {"id": 2, "verdict": "out_of_scope", "comment": 0, "reason": "same pattern in another function"},
            {"id": 2, "verdict": "requested", "comment": 1, "reason": "dup"},
            {"id": 9, "verdict": "requested", "comment": 1, "reason": "unknown"}
        ]});
        let (v, missing) = parse(&answer, &[1, 2, 3]).unwrap();
        assert_eq!(v.len(), 2);
        assert!(v[0].requested);
        assert!(!v[1].requested);
        assert_eq!(missing, vec![3]);
        assert_eq!(out_of_scope(&v), vec![2]);
    }

    #[test]
    fn a_wrong_shape_is_no_answer() {
        assert!(parse(&json!({"x": 1}), &[1]).is_none());
        assert!(parse(
            &json!({"hunks": [{"id": 1, "verdict": "maybe", "comment": 0, "reason": ""}]}),
            &[1]
        )
        .is_none());
    }

    #[test]
    fn the_prompt_numbers_comments_and_carries_the_hunks() {
        let p = prompt(
            "",
            &[Request {
                path: "jobs/sync.py".into(),
                line: Some(42),
                body: "Make this log DEBUG".into(),
                context: vec!["[alice]: why INFO here?".into()],
            }],
            false,
            "[hunk 1]\n@@ jobs/sync.py -42,1 +42,1 @@\n",
        );
        assert!(p.contains("### Comment 1 on `jobs/sync.py` line 42"));
        assert!(p.contains("> [alice]: why INFO here?"));
        assert!(p.contains("[hunk 1]"));
        assert!(!p.contains("CI failures"));
    }

    #[test]
    fn the_schema_requires_every_field() {
        let s = schema();
        let req = s["properties"]["hunks"]["items"]["required"]
            .as_array()
            .unwrap();
        assert_eq!(req.len(), 4);
    }
}
