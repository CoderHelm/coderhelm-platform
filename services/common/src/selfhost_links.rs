//! A self-hosted install's links point at the install. Every link builder and
//! message in this crate that reaches a person (unsubscribe and approval
//! links, review cards, knowledge-source alerts, log fix cards, merge-gate
//! controls, the CI quarantine call) is rendered as an install at
//! `https://coderhelm.example.com`, and nothing it produces may name a
//! coderhelm.com host other than the public docs. Approval cards are checked
//! next to their fixture in `agent_policy::notify`.

use crate::knowledge::sources::{alert, signin_error, Kind, SignIn, Source};
use crate::notify::{ChannelKind, Message};
use crate::public_urls::{
    app_url, hosted_mentions, override_for_this_thread, pretend_self_hosted, EXAMPLE_SELF_HOST,
};

fn rendered(m: &Message) -> Vec<String> {
    ChannelKind::ALL
        .iter()
        .map(|k| k.render(m).to_string())
        .chain(m.buttons.iter().map(|b| b.url.clone()))
        .collect()
}

#[test]
fn every_link_and_message_points_at_the_install() {
    pretend_self_hosted();
    let app = app_url("prod");
    assert_eq!(app, EXAMPLE_SELF_HOST);

    let mut out: Vec<(&str, String)> = vec![
        ("unsubscribe link", crate::unsubscribe::base_url()),
        ("plugin approval link", crate::plugins::signed::url("tok")),
        ("CI quarantine call", crate::e2e::ci_setup::quarantine_url()),
        (
            "merge gate control",
            crate::merge_gate::control_url(app, "acme", "web", 7, crate::merge_gate::Control::Stop),
        ),
    ];

    let artifact = crate::plugins::drafts::Artifact {
        id: "art1".into(),
        plugin_name: "marketing".into(),
        kind: "social".into(),
        title: "Launch".into(),
        version: 3,
        ..Default::default()
    };
    let card = crate::plugins::chat::review_card(
        &artifact,
        "We shipped.",
        crate::plugins::chat::dashboard("prod"),
    );
    out.extend(
        rendered(&card)
            .into_iter()
            .map(|s| ("plugin review card", s)),
    );

    let si = SignIn {
        email: "ada@example.org".into(),
        name: "Ada".into(),
        ..Default::default()
    };
    let source = Source {
        id: "s1".into(),
        kb_id: "kb1".into(),
        kind: Some(Kind::Sanity),
        name: "Help center".into(),
        error: Some(signin_error(&si, Kind::Sanity, String::new(), false)),
        signin: Some(si),
        ..Default::default()
    };
    let err = source.error.clone().unwrap();
    out.extend(
        rendered(&alert::signin_message(app, &source, &err))
            .into_iter()
            .map(|s| ("knowledge sign-in card", s)),
    );
    let (subject, body) = alert::signin_email(app, &source, &err);
    out.push(("knowledge sign-in email", format!("{subject}\n{body}")));

    let fix = crate::alert_notify::fix_message(
        "CPU high",
        "acme/web",
        7,
        "Fix the loop",
        "https://github.com/acme/web/pull/7",
        &format!("{app}/alerts/detail?id=a1"),
    );
    out.extend(rendered(&fix).into_iter().map(|s| ("alert fix card", s)));

    let finding = crate::log_findings::Finding {
        rec_id: "f1".into(),
        ..Default::default()
    };
    let url = format!("{app}/logs?finding=f1");
    let repo_check =
        crate::log_findings::repo_check_message(&finding, None, "two candidates", &[], &url);
    out.extend(
        rendered(&repo_check)
            .into_iter()
            .map(|s| ("log repo card", s)),
    );
    let ticket = crate::log_findings::TicketRecord::default();
    let log_fix = crate::log_findings::fix_message(
        &ticket,
        "acme/web",
        7,
        "Fix the loop",
        "https://github.com/acme/web/pull/7",
        &url,
    );
    out.extend(rendered(&log_fix).into_iter().map(|s| ("log fix card", s)));

    override_for_this_thread(None);
    for (what, o) in &out {
        assert_eq!(hosted_mentions(o), Vec::<String>::new(), "{what}: {o}");
    }
    for (what, o) in out.iter().take(4) {
        assert!(o.starts_with(EXAMPLE_SELF_HOST), "{what}: {o}");
    }
}

#[test]
fn an_install_without_its_address_never_falls_back_to_the_hosted_one() {
    override_for_this_thread(Some(crate::public_urls::Override {
        urls: None,
        site: None,
        self_hosted: true,
    }));
    let out = [
        crate::unsubscribe::base_url(),
        crate::plugins::signed::url("tok"),
        crate::e2e::ci_setup::quarantine_url(),
        crate::agent_policy::dashboard_url().to_string(),
    ];
    override_for_this_thread(None);
    for o in &out {
        assert_eq!(hosted_mentions(o), Vec::<String>::new(), "{o}");
    }
}
