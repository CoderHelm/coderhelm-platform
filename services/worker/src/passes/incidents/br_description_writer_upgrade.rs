//! br. The description writer was upgraded (a final review in the one save,
//! the repository's template as the structure, a running check never a
//! result), and a pull request opened the day before kept its old text:
//! several check rows "Not run: still running when this was written", "Look
//! first" and "Roll back" said twice, filler and a raw page id. Its head had
//! not moved, its checked text was what CoderHelm wrote, and no check showed
//! as running in the new form, so its stamp said it was current and nothing
//! ever built it again.
//!
//! What let it through: the stamp named the head, the digest of the checked
//! text and running checks, never the writer that built it, so an upgrade
//! changed nothing for a description already written; and the pull request
//! watch rebuilt a description only on its way to another act.
//!
//! Guarantee: the stamp names the writer version (`description::WRITER`,
//! raised with every change to what a description says or how it is laid
//! out), and a description built by another writer, or by one from before
//! the version, is not current. The pull request watch's sweep catches up
//! open pull requests CoderHelm opened, whatever the age of their run: a few
//! per team each sweep, inside its GitHub budget, each rebuilt once and
//! marked on its run. A description still wholly CoderHelm's is built
//! again from the facts of the head alone (then the final review in the
//! save): nothing of the old text is kept, not even in a fold, except a
//! line that closes an issue. A description with no CoderHelm stamp, or
//! whose checked text a person changed, is left as it is.

use super::fixture::*;
use crate::passes::description::{self, CatchUp};
use crate::passes::description_facts::{self, Inputs};
use crate::passes::pr_risk;
use crate::passes::pr_watch::{self, Candidate};
use crate::passes::reviewer_guide::{Check, State};

const HEAD: &str = "5ab6c7d8e9f00112233445566778899aabbccdde";
const PAGE_ID: &str = "7e1d2c3b-4a59-4687-b0a1-c2d3e4f5a6b7";

/// The description as the writer before the upgrade left it (names made
/// up): its lead with the checks frozen as running, the written prose, and
/// the older reviewer block beside the lead.
fn written_before() -> String {
    format!(
        "<!-- coderhelm:lead -->
<!-- coderhelm:body v1 ticket=HARBOR-12 head=5ab6c7d -->
Adds a length warning to the banner block's caption in the Studio (HARBOR-12).<!-- ch:summary-end -->

### Review focus

- Approach: decide whether this does what HARBOR-12 asks. CoderHelm's conclusion: nothing is out of the ordinary.
- Read first: `apps/studio/blocks/banner-validation.ts` (the largest change to code).
- See it: Studio: Home page ({PAGE_ID}), banner caption field.

**Rollback:** Revert this pull request; nothing outside the code changes with it.

### Verification

On 5ab6c7d (head of this PR):

| Check | Result |
|---|---|
| Lint (CI) | Not run: still running when this was written |
| Unit tests (CI) | Not run: still running when this was written |
| Type check (CI) | Not run: still running when this was written |
| Build (CI) | Not run: still running when this was written |
| Test Coverage Report (CI) | Not run: still running when this was written |

<!-- /coderhelm:lead -->

## Summary

Adds a length warning to the banner block's caption in the Studio.

## Changes

* `apps/studio/blocks/banner-validation.ts`: the length rule.

<!-- coderhelm:reviewer-guide -->
**For the reviewer**
* **Look first:** `apps/studio/blocks/banner-validation.ts`, the largest change to code.
* **Roll back:** revert this pull request; nothing outside the code changes with it.
<!-- /coderhelm:reviewer-guide -->
"
    )
}

/// Stamped as the writer before the version stamped it: head and digest,
/// nothing else.
fn stamped_before(body: &str) -> String {
    format!(
        "{}\n\n{}{} {} -->",
        body.trim_end(),
        description::STAMP_START,
        &HEAD[..12],
        description::checked_digest(body)
    )
}

/// The catch-up's rebuild on the same head, as `description::catch_up_run`
/// runs it: only a description wholly CoderHelm's is rebuilt, from the
/// facts of the head alone (the checks have passed) with nothing of its
/// old text kept, then stamped and written as the save writes.
fn rebuilt(body: &str) -> String {
    if description::catch_up(body, HEAD) != CatchUp::Rebuild {
        return body.to_string();
    }
    let body = &description::fresh_base(body);
    let files = vec![(
        "apps/studio/blocks/banner-validation.ts".to_string(),
        "added".to_string(),
        format!("@@ -1 +1,18 @@\n{}", "+line\n".repeat(18)),
    )];
    let floor = pr_risk::floor(&files, &std::collections::HashMap::new());
    let checks: Vec<Check> = ["Lint", "Unit tests", "Type check", "Build"]
        .iter()
        .map(|n| Check {
            name: n.to_string(),
            url: format!("https://ci.example.com/{}", n.replace(' ', "-")),
            state: State::Passed,
        })
        .collect();
    let facts = description_facts::facts(&Inputs {
        head: HEAD,
        body,
        files: &files,
        floor: &floor,
        checks: Some(&checks),
        why: None,
        dependency: None,
        outside: None,
        run_url: "",
        notes: vec![],
        evidence: vec![],
        repo_url: "https://github.com/acme/site",
    });
    let shaped =
        description_facts::shape_for(body, "Warn on long banner captions", facts, None, None);
    let next = description::with_stamp(&shaped.body, HEAD);
    // What `description::save` writes.
    let reviewed = common::pr_review::review(&description::clean(&next));
    common::pr_layout::layout(&common::person_gate::sink(&reviewed.body).text)
}

/// Before: everything the old stamp named still held on the unchanged
/// head, so it was current and left as it was. After: the missing writer
/// version makes it stale, the catch-up rebuilds it once, and the rebuilt
/// description is current and never picked again.
#[test]
fn an_old_layout_description_on_an_unchanged_head_is_rebuilt_once() {
    let old = stamped_before(&written_before());
    // Before: same head, same checked text, no running check in the stamp:
    // the three things the stamp used to name all say current.
    assert_eq!(description::stamp_of(&old), Some(&HEAD[..12]));
    assert!(old.contains(&description::checked_digest(&old)));
    assert!(!old.contains("checks-running"));
    assert!(!description::shows_running(&old));
    // After: the writer that built it is not this one.
    assert!(!description::is_current(&old, HEAD));
    assert_eq!(description::catch_up(&old, HEAD), CatchUp::Rebuild);

    let new = rebuilt(&old);
    let end = Ending {
        status: "completed".into(),
        pr: Some(12),
        draft: false,
        words: vec![new.clone()],
    };
    end.never_says("still running when this was written");
    end.never_says("nothing is out of the ordinary");
    end.never_says(PAGE_ID);
    end.never_says("Look first");
    assert!(end.says("| Lint (CI) | Passed |"), "{new}");
    assert_eq!(
        new.matches("evert this pull request; nothing outside")
            .count(),
        1,
        "the way back once: {new}"
    );
    // Rebuilt once: current on the head, nothing for the next sweep.
    assert!(description::is_current(&new, HEAD), "{new}");
    assert_eq!(description::catch_up(&new, HEAD), CatchUp::Current);
    assert_eq!(rebuilt(&new), new, "a second catch-up changes nothing");
}

/// An older description, from before the lead and the reviewer block had
/// markers: nothing tells its old lead from its prose. Wholly CoderHelm's
/// (the digest still matches), it is built again from the facts and no
/// old line survives anywhere, not even in a fold; the line that closes
/// its issue is kept.
#[test]
fn an_old_description_without_markers_keeps_no_old_line() {
    let unmarked = format!(
        "Adds a length warning to the banner block's caption in the Studio (HARBOR-12).

### Review focus

- Approach: decide whether this does what HARBOR-12 asks. CoderHelm's conclusion: nothing is out of the ordinary.
- Read first: `apps/studio/blocks/banner-validation.ts` (the largest change to code).
- See it: Studio: Home page ({PAGE_ID}), banner caption field.

**For the reviewer**
* **Look first:** `apps/studio/blocks/banner-validation.ts`, the largest change to code.
* **Roll back:** revert this pull request; nothing outside the code changes with it.
* **Not verified on `5ab6c7d`:** `Lint` (still running); `Studio build` (did not run).

**Rollback:** Revert this pull request; nothing outside the code changes with it.

### Verification

| Check | Result |
|---|---|
| Lint (CI) | Not run: still running when this was written |
| Unit tests (CI) | Not run: still running when this was written |
| Type check (CI) | Not run: still running when this was written |
| Build (CI) | Not run: still running when this was written |
| Test Coverage Report (CI) | Not run: still running when this was written |

Closes #12
"
    );
    assert!(!unmarked.contains("<!-- coderhelm:lead -->"));
    assert!(!unmarked.contains("<!-- coderhelm:reviewer-guide -->"));
    let old = stamped_before(&unmarked);
    assert_eq!(description::catch_up(&old, HEAD), CatchUp::Rebuild);
    let new = rebuilt(&old);
    for line in [
        "still running when this was written",
        "Look first",
        "Roll back",
        "For the reviewer",
        "nothing is out of the ordinary",
        "(still running)",
        "Test Coverage Report",
        PAGE_ID,
    ] {
        assert!(!new.contains(line), "{line:?} is still in: {new}");
    }
    assert!(new.contains("| Lint (CI) | Passed |"), "{new}");
    assert!(new.contains("Closes #12"), "the issue still closes: {new}");
    assert!(description::is_current(&new, HEAD), "{new}");
    assert_eq!(rebuilt(&new), new, "rebuilt once");
    // The catch-up runs this path, and the build drops the old body.
    let src = include_str!("../description.rs");
    let run = fn_src(src, "\npub async fn catch_up_run(");
    assert!(run.contains("false, true, None"), "{run}");
    let build = fn_src(src, "\nasync fn build(");
    assert!(build.contains("None if fresh => (fresh_base(&body), None)"));
    assert!(build.contains("fresh && catch_up(&body, &head) != CatchUp::Rebuild"));
}

/// The catch-up never writes over a person: a description with no stamp,
/// or whose checked text a person changed, is left.
#[test]
fn a_persons_description_is_left_as_it_is() {
    assert_eq!(
        description::catch_up(&written_before(), HEAD),
        CatchUp::Leave,
        "no CoderHelm stamp"
    );
    let edited = stamped_before(&written_before()).replace(
        "Adds a length warning",
        "Adds a length warning (we agreed 120 characters on the call)",
    );
    assert_eq!(description::catch_up(&edited, HEAD), CatchUp::Leave);
}

/// The sweep reaches every open pull request CoderHelm opened, a few per
/// team each time, and the rebuild runs through the one rebuild.
#[test]
fn the_watch_sweep_catches_up_a_few_per_team() {
    let run = |team: &str, n: u32| Candidate {
        team_id: team.into(),
        run_id: format!("run-{team}-{n}"),
        status: "completed".into(),
        pr: n as u64 + 1,
        installation_id: 1,
        updated_at: format!("2026-10-{:02}", n + 1),
        ..Default::default()
    };
    let mut all: Vec<Candidate> = (0..10).map(|n| run("t1", n)).collect();
    all.push(run("t2", 0));
    let picks = pr_watch::catch_up_picks(all, pr_watch::CATCH_UP_PER_TEAM, pr_watch::CATCH_UP_MAX);
    assert_eq!(
        picks.iter().filter(|c| c.team_id == "t1").count(),
        pr_watch::CATCH_UP_PER_TEAM
    );
    assert!(picks.iter().any(|c| c.team_id == "t2"));

    let src = include_str!("../pr_watch.rs");
    let sweep = fn_src(src, "\npub async fn sweep(");
    assert!(
        sweep.contains("catch_up(state, &mut budget"),
        "inside the sweep's budget"
    );
    let one = fn_src(src, "\nasync fn catch_up_one(");
    assert!(one.contains("description::catch_up("));
    assert!(
        one.contains("description::catch_up_run("),
        "the rebuild from the facts alone"
    );
    let all = fn_src(src, "\nasync fn catch_up(");
    assert!(all.contains("budget\n            .allows(") || all.contains("budget.allows("));
    // No age window: an old run's open pull request is reached too.
    assert!(!all.contains(":cut"));
}

/// A security fix's pull request from before the upgrade: its old body has
/// no lead or reviewer-block markers and its check rows froze as running.
/// Its sections are computed from the lockfile on the head
/// (`security_fixes::at_head`), which in an ordinary rebuild keeps the rest
/// of the body. In the catch-up, wholly CoderHelm's, it is built from the
/// sections computed now and the facts alone: no old line survives, and
/// the line that closes its issue is kept.
#[test]
fn an_old_security_description_keeps_no_old_line() {
    use crate::passes::security_fixes;
    use common::dep_head::{self, LockAt};
    use common::security_fixes::{self as sec, Advisory, Bump, Finding, FindingState, FixUnit};

    let unit = FixUnit {
        repo: format!("{OWNER}/{REPO}"),
        ecosystem: "npm".into(),
        bundle: true,
        bumps: vec![Bump {
            package: "xml-kit".into(),
            from: "1.4.4".into(),
            to: "1.4.9".into(),
            major: false,
            manifest: "pnpm-lock.yaml".into(),
        }],
        ..Default::default()
    };
    let finding = Finding {
        id: "f-1".into(),
        package: "xml-kit".into(),
        version: "1.4.4".into(),
        fixed_version: Some("1.4.9".into()),
        manifest: "pnpm-lock.yaml".into(),
        advisory: Advisory {
            primary: "GHSA-xxxx-0011".into(),
            ids: vec!["GHSA-xxxx-0011".into()],
            ..Default::default()
        },
        ..Default::default()
    };
    let state = FindingState::default();
    let copies = |v: &str| Some(vec![("xml-kit".to_string(), v.to_string())]);
    let head = dep_head::resolve(
        &unit,
        HEAD,
        &[LockAt {
            path: "pnpm-lock.yaml".into(),
            base: copies("1.4.4"),
            head: copies("1.4.9"),
            why: String::new(),
        }],
    );
    let check = common::upgrade_notes::WhatToCheck::default();
    let pairs = [(&finding, &state)];

    let old_body = format!(
        "{marker}
Bumps xml-kit to fix GHSA-xxxx-0011.

**For the reviewer**
* **Look first:** `pnpm-lock.yaml`, the only change.
* **Roll back:** revert this pull request; nothing outside the code changes with it.

**Rollback:** Revert this pull request; nothing outside the code changes with it.

### Verification

| Check | Result |
|---|---|
| Lint (CI) | Not run: still running when this was written |
| Unit tests (CI) | Not run: still running when this was written |
| Audit (CI) | Not run: still running when this was written |

Fixes #34
",
        marker = sec::MARKER
    );
    assert!(!old_body.contains("<!-- coderhelm:lead -->"));
    let old = stamped_before(&old_body);
    assert_eq!(description::catch_up(&old, HEAD), CatchUp::Rebuild);

    // What `at_head` gives the build in the catch-up: the sections computed
    // now, and only the closing line of the old body.
    let computed = sec::pr_body_at_head(
        &unit,
        &pairs,
        "Bumped.",
        &check,
        &sec::BodyLinks::default(),
        &head,
    );
    // Before: the ordinary rebuild keeps the rest of the old body.
    let kept = security_fixes::rebuilt_body(&description::flat(&old), &computed);
    assert!(kept.contains("Look first") && kept.contains("still running when this was written"));
    let base = security_fixes::fresh_body(&computed, &description::flat(&old));
    let dependency = common::pr_dependency::facts(&unit, &pairs, &head, &check);
    let files = vec![(
        "pnpm-lock.yaml".to_string(),
        "modified".to_string(),
        "@@ -1 +1 @@\n-a\n+b\n".to_string(),
    )];
    let floor = pr_risk::floor(&files, &std::collections::HashMap::new());
    let checks: Vec<Check> = ["Lint", "Unit tests"]
        .iter()
        .map(|n| Check {
            name: n.to_string(),
            url: format!("https://ci.example.com/{}", n.replace(' ', "-")),
            state: State::Passed,
        })
        .collect();
    let facts = description_facts::facts(&Inputs {
        head: HEAD,
        body: &base,
        files: &files,
        floor: &floor,
        checks: Some(&checks),
        why: None,
        dependency: Some(dependency),
        outside: None,
        run_url: "",
        notes: vec![],
        evidence: vec![],
        repo_url: "https://github.com/acme/site",
    });
    let shaped = description_facts::shape_for(&base, "Fix GHSA-xxxx-0011", facts, None, None);
    let next = description::with_stamp(&shaped.body, HEAD);
    let reviewed = common::pr_review::review(&description::clean(&next));
    let new = common::pr_layout::layout(&common::person_gate::sink(&reviewed.body).text);

    for line in [
        "still running when this was written",
        "Look first",
        "Roll back",
        "For the reviewer",
        "| Audit (CI)",
    ] {
        assert!(!new.contains(line), "{line:?} is still in: {new}");
    }
    assert!(new.contains("xml-kit"), "the computed sections: {new}");
    assert!(new.contains("Fixes #34"), "the issue still closes: {new}");
    assert!(description::is_current(&new, HEAD), "{new}");
    assert_eq!(description::catch_up(&new, HEAD), CatchUp::Current);

    // The build passes the catch-up on to the security sections.
    let src = include_str!("../description.rs");
    let build = fn_src(src, "\nasync fn build(");
    let at = &build[build.find("security_fixes::at_head(").unwrap()..];
    assert!(at[..at.find(".await").unwrap()].contains("fresh,"), "{at}");
    let sec_src = include_str!("../security_fixes.rs");
    let at_head = fn_src(sec_src, "\npub(crate) async fn at_head(");
    assert!(at_head.contains("if fresh_only {"));
    assert!(at_head.contains("fresh_body(&fresh, body)"));
}
