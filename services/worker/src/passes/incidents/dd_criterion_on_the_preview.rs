//! dd. A ticket asked that a page's answers be in the HTML the server
//! sends, so they show with scripts turned off and crawlers read them. The
//! run rewrote the block as markup that needs no script, wrote unit tests
//! that render the component on its own, and pushed. Its pull request had
//! nothing that fetched the preview's page as the server sends it: the
//! page loads the block on the client only, so the served HTML still had
//! none of the answers. The preview's own check found it ("the text is not
//! in the server HTML") after the push. The run that opened the pull
//! request came from an issue with an empty body, so the two-way proof had
//! no criterion to prove at all and the ready decision read "nothing to
//! prove".
//!
//! Guarantee: the criteria to prove come from the ticket, else from the
//! plan's criteria map (`common::e2e_proof::criteria_or_map`), so an empty
//! ticket body never means nothing to prove. A criterion about what the
//! server sends (its page source, the page with scripts off, what a
//! crawler reads: `Criterion::about_served`) is also proven on the served
//! page itself: the page is fetched as the server sends it on the preview
//! and on staging (`e2e_proof::served_legs`, a plain GET through
//! `web_check::fetch_page`, no script run) and each side is judged against
//! the criterion. Not shown on the preview is coder work whatever the
//! browser test says, so it goes back as a fix round and the ready
//! decision waits for it (`common::e2e_proof::outcome`, `standing_of`);
//! shown on the preview and missing on staging is proven; a side that
//! couldn't be fetched or judged is unverified with why, never a pass.

use crate::passes::e2e_proof::{
    apply_served, page_url, served_prompt, standing_of, ServedCheck, Standing,
};
use common::ci_repro::{Leg, Record};
use common::e2e_proof::{self as ep, Outcome, Proof, Served};

const TITLE: &str = "Make the questions block work without JavaScript";
/// The issue the run started from: no body at all.
const BODY: &str = "";
const HEAD: &str = "5e84f9390fe6";

/// The plan's criteria map: the only place the run's criteria were written.
const DESIGN: &str = "## Approach\nRender the block with native markup.\n\n## Criteria map\n- Every answer, including those behind Show more, is in the server HTML | page: /acme/classes | e2e: e2e/tests/site/questions.spec.ts › every answer is served | asserts: all answers are in the page source\n- One item opens at a time | page: /acme/classes | e2e: e2e/tests/site/questions.spec.ts › one open | asserts: opening one closes the other\n";

/// The preview's page as its server sent it: the block is loaded on the
/// client only, so the served HTML has the section and no answers.
const PREVIEW_HTML: &str = "<html><head><script>window.__chunks=['questions-block']</script><style>.x{}</style></head><body><main><section id=\"questions\"><h2>Questions</h2><!--$?--><template id=\"B:0\"></template><div class=\"loading\"></div><!--/$--></section></main><script src=\"/_next/static/chunks/questions.js\"></script></body></html>";

fn browser_green() -> Leg {
    Leg {
        ran: true,
        passed: 1,
        ..Default::default()
    }
}

fn served(url: &str, shown: Option<bool>, quote: &str) -> Served {
    Served {
        url: url.into(),
        fetched: true,
        status: 200,
        shown,
        quote: quote.into(),
        ..Default::default()
    }
}

fn proof(criterion: &str, change: Option<Served>, base: Option<Served>) -> Proof {
    Proof {
        criterion: criterion.into(),
        page: "/acme/classes".into(),
        test: "e2e/tests/site/questions.spec.ts › every answer is served".into(),
        record: Record {
            head: HEAD.into(),
            check: "two-way proof".into(),
            // The browser runs the page's scripts: its test passes on both.
            base: browser_green(),
            change: browser_green(),
            ..Default::default()
        },
        served_change: change,
        served_base: base,
    }
}

#[test]
fn an_empty_ticket_body_still_has_the_plans_criteria_to_prove() {
    let map = ep::parse_map(DESIGN);
    // The ticket text alone gives nothing to prove.
    assert!(ep::criteria(TITLE, BODY, &[])
        .iter()
        .all(|c| !c.needs_e2e()));
    let criteria = ep::criteria_or_map(TITLE, BODY, &[], &map);
    assert_eq!(criteria.len(), 2, "{criteria:?}");
    assert!(criteria.iter().all(|c| c.needs_e2e()));
    assert_eq!(criteria[0].page.as_deref(), Some("/acme/classes"));
    // The served-page criterion is known as one; the click one is not.
    assert!(criteria[0].about_served());
    assert!(!criteria[1].about_served());
    // A ticket that has its own criteria keeps them.
    let own = ep::criteria_or_map(
        TITLE,
        "Expected\nThe answers show on /acme/classes with JavaScript off",
        &[],
        &map,
    );
    assert_eq!(own.len(), 1);
    assert!(own[0].text.contains("JavaScript off"));
}

#[test]
fn the_served_page_is_read_without_its_scripts() {
    let view = ep::served_view(PREVIEW_HTML);
    assert!(view.contains("<h2>Questions</h2>"), "{view}");
    assert!(!view.contains("__chunks"), "{view}");
    assert!(!view.contains(".x{}"), "{view}");
    // Structured data is what the server sends for crawlers: kept.
    let ld = ep::served_view(
        "<head><script type=\"application/ld+json\">{\"@type\":\"FAQPage\"}</script></head>",
    );
    assert!(ld.contains("FAQPage"), "{ld}");
    assert_eq!(
        page_url("https://pr-12.preview.example.dev/", "/acme/classes"),
        "https://pr-12.preview.example.dev/acme/classes"
    );
}

#[test]
fn the_judgment_reads_each_side_and_an_unanswered_check_is_never_a_pass() {
    let criterion = "Every answer, including those behind Show more, is in the server HTML";
    let checks = vec![
        ServedCheck {
            criterion: criterion.into(),
            side: "preview",
            url: "https://pr-12.preview.example.dev/acme/classes".into(),
            view: ep::served_view(PREVIEW_HTML),
        },
        ServedCheck {
            criterion: criterion.into(),
            side: "staging",
            url: "https://staging.example.dev/acme/classes".into(),
            view: ep::served_view(PREVIEW_HTML),
        },
    ];
    let prompt = served_prompt(&checks);
    assert!(prompt.contains("<page id=\"0\""), "{prompt}");
    assert!(prompt.contains("0. page 0 (preview)"), "{prompt}");
    assert!(prompt.contains("1. page 1 (staging)"), "{prompt}");
    let v = serde_json::json!({"verdicts": [
        {"check": 0, "shown": "no", "quote": "section#questions holds only a loading placeholder"}
    ]});
    let got = apply_served(&v, 2);
    assert_eq!(got[0].0, Some(false));
    assert!(got[0].1.contains("placeholder"));
    assert_eq!(
        got[1].0, None,
        "a check the judgment skipped stays unjudged"
    );
}

#[test]
fn missing_from_the_served_page_is_coder_work_though_the_browser_test_passes() {
    let criterion = "Every answer, including those behind Show more, is in the server HTML";
    let p = proof(
        criterion,
        Some(served(
            "https://pr-12.preview.example.dev/acme/classes",
            Some(false),
            "section#questions holds only a loading placeholder",
        )),
        Some(served(
            "https://staging.example.dev/acme/classes",
            Some(false),
            "",
        )),
    );
    // On main the browser legs alone decided: green on both, "doesn't test
    // the ticket", and nothing said the server sends none of it.
    match ep::outcome(&p) {
        Outcome::CoderWork(w) => {
            assert!(w.contains("server HTML"), "{w}");
            assert!(w.contains("pr-12.preview.example.dev"), "{w}");
        }
        o => panic!("{o:?}"),
    }
    let items = ep::coder_items(std::slice::from_ref(&p), HEAD);
    assert_eq!(items.len(), 1);
    assert!(items[0].contains("server renders it"), "{}", items[0]);
    assert!(items[0].contains("page source"), "{}", items[0]);
    // The ready decision waits on the fix round, never ready.
    assert_eq!(standing_of(&[p], HEAD), Standing::Fixing(1));
}

#[test]
fn shown_on_the_preview_and_missing_on_staging_is_proven_and_a_failed_fetch_is_unverified() {
    let criterion = "Every answer, including those behind Show more, is in the server HTML";
    let proven = proof(
        criterion,
        Some(served(
            "https://pr-12.preview.example.dev/acme/classes",
            Some(true),
            "<details><summary>Can I book online?</summary>Yes",
        )),
        Some(served(
            "https://staging.example.dev/acme/classes",
            Some(false),
            "answers are empty hidden divs",
        )),
    );
    assert!(matches!(ep::outcome(&proven), Outcome::Proven(_)));
    assert_eq!(standing_of(&[proven], HEAD), Standing::Done(vec![]));

    // A preview that answers with an error page is coder work.
    let mut down = served("https://pr-12.preview.example.dev/acme/classes", None, "");
    down.status = 500;
    let p = proof(criterion, Some(down), None);
    assert!(ep::outcome(&p).is_coder_work(), "{:?}", ep::outcome(&p));

    // Not judged and no browser test: unverified with why, never a pass.
    let unjudged = Served {
        url: "https://pr-12.preview.example.dev/acme/classes".into(),
        note: "the judgment didn't answer".into(),
        ..Default::default()
    };
    let mut p = proof(criterion, Some(unjudged), None);
    p.test.clear();
    match standing_of(&[p], HEAD) {
        Standing::Done(notes) => {
            assert_eq!(notes.len(), 1);
            assert!(notes[0].contains("couldn't be judged"), "{notes:?}");
        }
        s => panic!("{s:?}"),
    }
}

/// The pull request watch's proof runs the served-page legs, and its
/// criteria come through the plan's map when the ticket gives none.
#[test]
fn the_proof_fetches_the_served_page_and_falls_back_to_the_plan() {
    let src = include_str!("../e2e_proof.rs");
    let prove = &src[src.find("pub(crate) async fn prove(").unwrap()..];
    let prove = &prove[..prove.find("\n}\n").unwrap()];
    assert!(
        prove.contains("served_legs("),
        "prove doesn't fetch the served page"
    );
    assert!(prove.contains("served_change"));
    let criteria = &src[src.find("async fn criteria_of(").unwrap()..];
    let criteria = &criteria[..criteria.find("\n}\n").unwrap()];
    assert!(criteria.contains("criteria_or_map("));
    assert!(criteria.contains("design_of("));
    let legs = &src[src.find("async fn served_legs(").unwrap()..];
    assert!(legs.contains("about_served()"));
    assert!(legs.contains("fetch_served("));
    assert!(legs.contains("judge_served("));
}
