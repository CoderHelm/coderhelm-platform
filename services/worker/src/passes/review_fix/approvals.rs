//! A push to a pull request that had approvals can dismiss them (a branch
//! rule that dismisses stale reviews). The person who asked for the change
//! is told, naming who has to approve again, so the PR doesn't sit waiting
//! on an approval nobody knows is gone.

use serde_json::Value;

/// Each reviewer's latest decisive review state, from the PR's reviews
/// (oldest first, as GitHub lists them). Comments don't change a decision.
pub fn latest_states(reviews: &Value) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for r in reviews.as_array().into_iter().flatten() {
        let Some(login) = r["user"]["login"].as_str() else {
            continue;
        };
        let state = r["state"].as_str().unwrap_or("").to_ascii_uppercase();
        if !matches!(
            state.as_str(),
            "APPROVED" | "CHANGES_REQUESTED" | "DISMISSED"
        ) {
            continue;
        }
        match out.iter_mut().find(|(l, _)| l == login) {
            Some((_, s)) => *s = state,
            None => out.push((login.to_string(), state)),
        }
    }
    out
}

/// The reviewers whose approval stood before the push.
pub fn approvers(reviews: &Value) -> Vec<String> {
    latest_states(reviews)
        .into_iter()
        .filter(|(_, s)| s == "APPROVED")
        .map(|(l, _)| l)
        .collect()
}

/// The approvers from before the push whose approval no longer stands.
pub fn dismissed(before: &[String], after: &Value) -> Vec<String> {
    let now = latest_states(after);
    before
        .iter()
        .filter(|l| {
            !now.iter()
                .any(|(who, s)| who.eq_ignore_ascii_case(l) && s == "APPROVED")
        })
        .cloned()
        .collect()
}

/// What the person who asked is told.
pub fn notice(pr_url: &str, dismissed: &[String]) -> String {
    let who = dismissed
        .iter()
        .map(|l| format!("@{l}"))
        .collect::<Vec<_>>()
        .join(", ");
    let verb = if dismissed.len() == 1 {
        "needs"
    } else {
        "need"
    };
    format!(
        "The review fix was pushed to {pr_url} and the push dismissed the earlier approval \
         (stale review dismissal). {who} {verb} to approve again before it can merge. Nothing \
         was merged."
    )
}

/// The same notice for a channel where `@` would ping the wrong system.
pub fn notice_plain(pr_url: &str, dismissed: &[String]) -> String {
    notice(pr_url, dismissed).replace('@', "")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn review(login: &str, state: &str) -> Value {
        json!({"user": {"login": login}, "state": state})
    }

    #[test]
    fn the_latest_decision_per_reviewer_counts_and_comments_do_not() {
        let reviews = json!([
            review("alice", "CHANGES_REQUESTED"),
            review("alice", "APPROVED"),
            review("alice", "COMMENTED"),
            review("bob", "APPROVED"),
            review("carol", "APPROVED"),
            review("carol", "CHANGES_REQUESTED"),
        ]);
        assert_eq!(approvers(&reviews), vec!["alice", "bob"]);
    }

    #[test]
    fn approvals_gone_after_the_push_are_named() {
        let before = vec!["alice".to_string(), "bob".to_string()];
        let after = json!([review("alice", "DISMISSED"), review("bob", "APPROVED"),]);
        assert_eq!(dismissed(&before, &after), vec!["alice"]);
        // An approval GitHub no longer lists at all is gone too.
        assert_eq!(dismissed(&before, &json!([])), vec!["alice", "bob"]);
        assert!(dismissed(&[], &after).is_empty());
    }

    #[test]
    fn the_notice_names_who_must_approve_again() {
        let n = notice(
            "https://github.com/o/r/pull/7",
            &["alice".into(), "bob".into()],
        );
        assert!(n.contains("@alice, @bob need to approve again"));
        assert!(n.contains("Nothing was merged"));
        assert!(notice_plain("u", &["alice".into()]).contains("alice needs to approve again"));
    }
}
