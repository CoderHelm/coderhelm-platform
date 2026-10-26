//! A self-hosted install's GitHub comments and PR text link to the install:
//! the progress comment on an issue, the "consulted agents" line in PR bodies
//! and reviews, and the merge-gate controls. Rendered as an install at
//! `https://coderhelm.example.com`; nothing may name a coderhelm.com host
//! other than the public docs. (The weekly summary is checked next to its
//! fixture in `weekly_summary`.)

use common::public_urls::{
    hosted_mentions, override_for_this_thread, pretend_self_hosted, EXAMPLE_SELF_HOST,
};

#[test]
fn github_comments_link_to_the_install() {
    pretend_self_hosted();
    let consulted = vec![super::skills::Consulted {
        name: "Payments expert".into(),
        repo: "acme/web".into(),
        path: ".coderhelm/agents/payments.md".into(),
        asked: 2,
        answered: 1,
    }];
    let out = vec![
        super::format_progress("triage", "01RUN"),
        super::format_progress("implement", "01RUN"),
        super::skills::consulted_markdown(&consulted, super::skills::app_url("prod")),
        common::merge_gate::control_url(
            super::skills::app_url("prod"),
            "acme",
            "web",
            7,
            common::merge_gate::Control::Rearm,
        ),
    ];
    override_for_this_thread(None);
    for o in &out {
        assert_eq!(hosted_mentions(o), Vec::<String>::new(), "{o}");
        assert!(o.contains(EXAMPLE_SELF_HOST), "{o}");
    }
}
