//! Who may ask CoderHelm for work from GitHub. A comment or review starts a
//! run, a review or a fix pass only when its author has standing in the
//! repository; anyone can comment on a public repository, and their words
//! must not become instructions.

use serde_json::Value;

/// GitHub's `author_association` values for people with standing in the
/// repository: its owner, a member of the owning organization, or an invited
/// collaborator. Everyone else (CONTRIBUTOR, FIRST_TIMER, NONE, ...) is an
/// outsider as far as starting work goes.
pub fn association_may_start_work(association: &str) -> bool {
    matches!(association, "OWNER" | "MEMBER" | "COLLABORATOR")
}

/// Standing GitHub states on a comment or review object itself: a bot
/// account (an app the repository's owners installed, such as CI), or a
/// person whose association with the repository may start work.
pub fn shown_by_github(item: &Value) -> bool {
    item["user"]["type"].as_str() == Some("Bot")
        || association_may_start_work(item["author_association"].as_str().unwrap_or(""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_people_with_standing_start_work() {
        for a in ["OWNER", "MEMBER", "COLLABORATOR"] {
            assert!(association_may_start_work(a), "{a}");
        }
        for a in [
            "CONTRIBUTOR",
            "FIRST_TIME_CONTRIBUTOR",
            "FIRST_TIMER",
            "MANNEQUIN",
            "NONE",
            "",
            "member",
        ] {
            assert!(!association_may_start_work(a), "{a}");
        }
    }

    #[test]
    fn standing_is_read_from_the_comment_or_review() {
        let item = |assoc: &str, kind: &str| json!({"author_association": assoc, "user": {"type": kind, "login": "x", "id": 1}});
        assert!(shown_by_github(&item("MEMBER", "User")));
        assert!(shown_by_github(&item("COLLABORATOR", "User")));
        // CI and other installed apps keep working whatever their association.
        assert!(shown_by_github(&item("NONE", "Bot")));
        assert!(!shown_by_github(&item("CONTRIBUTOR", "User")));
        assert!(!shown_by_github(&item("NONE", "User")));
        assert!(!shown_by_github(&json!({})));
    }
}
