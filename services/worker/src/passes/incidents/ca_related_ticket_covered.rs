//! ca. A ticket's pull request was closed by a person who expected a
//! linked ticket's pull request to cover it. The pull request that was
//! kept never read the linked ticket: when it was called ready, the
//! drawer's Register button still pointed to the old address.
//!
//! Guarantee: the Jira app sends the ticket's issue links and the tickets
//! its text names, with their summaries and descriptions; the gateway
//! keeps them on the run's message, the planner reads their acceptance
//! criteria as part of the change, and the run keeps them. Before the pull
//! request is called ready, each address a pull request closed in favor of
//! this one moved is checked on the head's files, each link criterion is
//! checked on the preview page, and anything else is listed as not covered
//! with why, in the description's "Related tickets" section.

use crate::passes::related::closed_in_favor;
use common::related_tickets::{
    check_page, check_source, criteria, fold, from_payload, link_expectation, moved_links,
    plan_note, section, with_section, Verdict, MARKER,
};
use serde_json::json;

const DRAWER: &str = "src/components/nav/register-drawer.tsx";

/// The closed pull request's change: the drawer button to the new page.
const CLOSED_PATCH: &str = "@@ -18,7 +18,7 @@ export function RegisterDrawer() {
   return (
     <Drawer>
-      <Button href=\"https://www.example.com/old-camps/register\">Register</Button>
+      <Button href=\"/venue-a/summer-camps/register\">Register</Button>
     </Drawer>
   );
";

#[test]
fn the_linked_ticket_reaches_the_plan() {
    let event = json!({"related": [{
        "key": "ABC-98",
        "relation": "relates to",
        "summary": "Drawer Register button goes to the new registration page",
        "status": "Closed",
        "description": "Acceptance criteria\n- The drawer's \"Register\" button links to /venue-a/summer-camps/register\n"
    }]});
    let related = from_payload(&event);
    assert_eq!(related.len(), 1);
    let c = fold(&related);
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].from, "ABC-98");
    let note = plan_note(&related);
    assert!(note.contains("ABC-98 (relates to)"), "{note}");
    assert!(note.contains("links to /venue-a/summer-camps/register"));
    assert_eq!(
        criteria("Acceptance criteria\n- one\n- two\n\nNotes\n- not one"),
        ["one", "two"]
    );
}

#[test]
fn the_head_still_pointing_at_the_old_address_is_not_covered() {
    let timeline = json!([{"event": "cross-referenced", "source": {"issue": {
        "number": 1531, "title": "Drawer Register button to the new page", "state": "closed",
        "pull_request": {"merged_at": null}, "repository": {"full_name": "acme/site"}}}}]);
    assert_eq!(closed_in_favor(&timeline, "acme/site")[0].0, 1531);
    let moves = moved_links(DRAWER, CLOSED_PATCH);
    assert_eq!(moves.len(), 1);
    let head = "<Button href=\"https://www.example.com/old-camps/register\">Register</Button>";
    let v = check_source(&moves[0], Some(head));
    match &v {
        Verdict::NotCovered(why) => assert!(why.contains("still uses"), "{why}"),
        other => panic!("{other:?}"),
    }
    let fixed = "<Button href=\"/venue-a/summer-camps/register\">Register</Button>";
    assert!(matches!(
        check_source(&moves[0], Some(fixed)),
        Verdict::Covered(_)
    ));
}

#[test]
fn a_link_criterion_is_checked_on_the_preview_and_the_rest_is_listed() {
    let e = link_expectation(
        "The drawer's \"Register\" button links to /venue-a/summer-camps/register",
    )
    .unwrap();
    let old = r#"<div role="dialog"><a href="https://www.example.com/old-camps/register">Register</a></div>"#;
    assert!(matches!(
        check_page("the preview", old, &e),
        Some(Verdict::NotCovered(_))
    ));
    let c = fold(&from_payload(
        &json!({"related": [{"key": "ABC-98", "description":
        "Acceptance criteria\n- The drawer's \"Register\" button links to /venue-a/summer-camps/register"}]}),
    ));
    let s = section(&[(
        c[0].clone(),
        Verdict::NotCovered("not checked: no preview".into()),
    )]);
    assert!(s.starts_with(MARKER));
    assert!(
        s.contains("**not covered**: not checked: no preview"),
        "{s}"
    );
    let body = with_section("## Summary\nx\n", &s);
    assert_eq!(with_section(&body, &s).matches(MARKER).count(), 1);
}

#[test]
fn ready_checks_them_and_the_jira_app_sends_them() {
    let ready = include_str!("../ready.rs");
    assert!(ready.contains("related_section(ctx, &pr, f.preview.as_ref())"));
    let passes = include_str!("../mod.rs");
    assert!(passes.contains("related::keep(state, &msg.team_id, run_id, &msg.related)"));
    let plan = include_str!("../plan.rs");
    assert_eq!(
        plan.matches("related_tickets::plan_note(&msg.related)")
            .count(),
        2
    );
    let gateway = include_str!("../../../../gateway/src/routes/jira_webhook.rs");
    assert!(gateway.contains("related: common::related_tickets::from_payload(payload)"));
    let app = include_str!("../../../../../coderhelm-jira/src/index.js");
    assert!(app.contains("issuelinks") && app.contains("relatedTickets(issue.key, fields"));
}
