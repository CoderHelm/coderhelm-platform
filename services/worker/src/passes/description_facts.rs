//! A description built from the facts of its head: the pure half of
//! `description`.
//!
//! [`facts`] turns what the pipeline read (the diff, the head's check
//! results, the run's ticket, a dependency update's lockfile facts, the
//! steps outside the code) into `common::pr_facts::Facts`. [`shape`] is the
//! one path every description takes to its final text:
//!
//! 1. every sentence a person or a model wrote is held against the facts
//!    (`pr_claims::enforce`);
//! 2. the reader pass fills the lead's summary and check, as far as its
//!    reply can be verified (`pr_reader`), else what the description
//!    already said, else the records;
//! 3. the lead is rendered from the facts (`pr_facts::lead`), the steps and
//!    evidence are rendered collapsed (`pr_steps::evidence_block`);
//! 4. the visible part is the lead and what exists only when something is
//!    wrong; everything else is collapsed (`pr_layout::layout`), and the
//!    whole fits GitHub's limit with a link for what is left out.
//!
//! No network, no model call: `description::build` gathers the inputs and
//! the quality eval (`description_eval`) runs the same function on recorded
//! cases.

use common::pr_claims::{self, Correction};
use common::pr_facts::{
    CheckFact, Closer, DependencyFacts, Facts, Family, FileChange, OpenItem, Outcome, Risk,
    Unverified, Why, LEAD_END, LEAD_START,
};
use common::pr_reader::{self, Slots};
use common::pr_steps::{self, Checked, EVIDENCE_END, EVIDENCE_START};

use super::fix_location::OutsideChange;
use super::pr_risk::{Floor, Level};
use super::reviewer_guide::{Check, State};

/// Why an open item nobody is asked to close counts as not verified.
const OPEN_ITEM: &str = "nothing has checked it yet";

/// GitHub lists at most this many files of a comparison.
pub const COMPARE_FILE_LIMIT: usize = 300;

/// Everything read for one head.
pub struct Inputs<'a> {
    pub head: &'a str,
    pub body: &'a str,
    /// The diff's files: path, status, patch.
    pub files: &'a [(String, String, String)],
    pub floor: &'a Floor,
    /// The head's checks; `None` when they could not be read.
    pub checks: Option<&'a [Check]>,
    pub why: Option<Why>,
    pub dependency: Option<DependencyFacts>,
    /// The work outside the code, from the run's decision.
    pub outside: Option<&'a OutsideChange>,
    pub run_url: &'a str,
    /// Said once on the pull request (a key that could not be made).
    pub notes: Vec<String>,
    /// Raw evidence beyond the outside steps' (what the title's form was
    /// read from).
    pub evidence: Vec<String>,
    /// The repository's address, for links to files at the head.
    pub repo_url: &'a str,
}

/// Lines added and removed in a patch.
fn changed_lines(patch: &str) -> usize {
    patch
        .lines()
        .filter(|l| {
            (l.starts_with('+') && !l.starts_with("+++"))
                || (l.starts_with('-') && !l.starts_with("---"))
        })
        .count()
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn heading(line: &str) -> Option<String> {
    let t = line.trim();
    if t.starts_with('#') {
        return Some(
            t.trim_start_matches('#')
                .trim()
                .trim_matches('*')
                .trim()
                .to_lowercase(),
        );
    }
    let t = t.trim_start_matches(|c: char| c.is_ascii_digit() || c == '.' || c == ')' || c == ' ');
    if t.starts_with("**") {
        let inner = t.trim_start_matches("**");
        if let Some(end) = inner.find("**") {
            let rest = inner[end + 2..].trim();
            // `**Summary**` alone, or `**Summary** — text` as a writer numbers it.
            if rest.is_empty() || rest.starts_with(['—', '-', ':', '–']) {
                return Some(inner[..end].trim().trim_end_matches(':').to_lowercase());
            }
        }
    }
    None
}

/// The lines of the first section whose heading starts with one of
/// `names`, outside CoderHelm's own blocks.
fn section<'a>(body: &'a str, names: &[&str]) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut inside = false;
    for l in body.lines() {
        if let Some(h) = heading(l) {
            if inside {
                break;
            }
            inside = names.iter().any(|n| h.starts_with(n));
            if inside {
                // `**Summary** — text` carries its text on the heading line.
                if let Some((_, rest)) = l.split_once("** ") {
                    let rest = rest.trim_start_matches(['—', '-', ':', '–', ' ']);
                    if !rest.is_empty() {
                        out.push(rest);
                    }
                }
            }
            continue;
        }
        if inside {
            if l.trim().starts_with("<!--") {
                break;
            }
            out.push(l);
        }
    }
    out
}

/// What the description itself says the change is: the first sentences of
/// its Summary.
pub fn summary_of(body: &str) -> String {
    // A template's "Description" is its summary.
    let lines = match section(body, &["summary"]) {
        l if l.iter().any(|x| !x.trim().is_empty()) => l,
        _ => section(body, &["description"]),
    };
    let para: Vec<&str> = lines
        .iter()
        .map(|l| l.trim())
        .skip_while(|l| l.is_empty())
        .take_while(|l| !l.is_empty() && !l.starts_with('|') && !l.starts_with("- "))
        .collect();
    let text = one_line(&para.join(" "));
    let mut out = String::new();
    for (n, s) in text.split_inclusive(". ").enumerate() {
        if n >= 2 || out.chars().count() + s.chars().count() > pr_reader::MAX_SUMMARY_CHARS {
            break;
        }
        out.push_str(s);
    }
    out.trim().to_string()
}

/// The one command the description gives to check the change.
pub fn check_of(body: &str) -> String {
    for l in section(
        body,
        &["verification", "how to check", "test plan", "testing"],
    ) {
        let mut ticks = l.split('`');
        ticks.next();
        while let Some(inside) = ticks.next() {
            if pr_steps::is_check_command(inside) {
                return format!("`{}`", inside.trim());
            }
            ticks.next();
        }
    }
    String::new()
}

/// The questions a description puts to a person (`completeness`): each is
/// something only they can close.
fn questions_of(body: &str) -> Vec<OpenItem> {
    let Some(at) = body.find("## Questions only a person can answer") else {
        return Vec::new();
    };
    body[at..]
        .lines()
        .skip(1)
        .map(str::trim)
        .take_while(|l| !l.is_empty() && !l.starts_with("<!--") && !l.starts_with('#'))
        .filter_map(|l| {
            let digits = l.chars().take_while(|c| c.is_ascii_digit()).count();
            (digits > 0 && l[digits..].starts_with(". ")).then(|| one_line(&l[digits + 2..]))
        })
        .filter_map(|q| {
            // A question reaches a person only when the gate finds it theirs
            // (as a yes/no on CoderHelm's recommendation); the rest are
            // decided and noted ([`question_notes`]). It stays a question,
            // so the layout renders the gate's item for it
            // (`pr_facts::needed_items`).
            match common::person_gate::line_outcome(common::person_gate::Kind::Question, &q) {
                common::person_gate::Outcome::Human(_) => Some(OpenItem {
                    text: format!("Answer: {q}"),
                    closer: Closer::Person,
                }),
                _ => None,
            }
        })
        .collect()
}

/// The sign-offs the settle stage folded into the merge decision
/// (`settle::SIGNOFF_START`).
pub fn signoffs(body: &str) -> Vec<String> {
    let (start, end) = (super::settle::SIGNOFF_START, super::settle::SIGNOFF_END);
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(a) = rest.find(start) {
        let after = &rest[a + start.len()..];
        let Some(b) = after.find(end) else {
            break;
        };
        let note = one_line(&after[..b]);
        if !note.is_empty() && !out.contains(&note) {
            out.push(note);
        }
        rest = &after[b..];
    }
    out
}

/// The decisions and settled notes for the questions a description put to
/// a person that the gate found CoderHelm's to decide: one line each, for
/// the collapsed evidence.
pub fn question_notes(body: &str) -> Vec<String> {
    let Some(at) = body.find("## Questions only a person can answer") else {
        return Vec::new();
    };
    let asks: Vec<common::person_gate::Ask> = body[at..]
        .lines()
        .skip(1)
        .map(str::trim)
        .take_while(|l| !l.is_empty() && !l.starts_with("<!--") && !l.starts_with('#'))
        .filter_map(|l| {
            let digits = l.chars().take_while(|c| c.is_ascii_digit()).count();
            (digits > 0 && l[digits..].starts_with(". ")).then(|| {
                common::person_gate::Ask::new(common::person_gate::Kind::Question, &l[digits + 2..])
            })
        })
        .collect();
    common::person_gate::gate(asks, &common::person_gate::Context::default()).evidence_lines()
}

/// The open items a description lists, each with who closes it, and the
/// questions it puts to a person.
pub fn open_of(body: &str) -> Vec<OpenItem> {
    let mut out = listed_open(body);
    out.extend(questions_of(body));
    out
}

fn listed_open(body: &str) -> Vec<OpenItem> {
    let (start, end) = (super::proof::UNVERIFIED_START, super::proof::UNVERIFIED_END);
    let Some(a) = body.find(start) else {
        return Vec::new();
    };
    let Some(b) = body[a..].find(end) else {
        return Vec::new();
    };
    body[a..a + b]
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("- ") || l.starts_with("* "))
        .map(|l| {
            let ci = l.contains("<!-- coderhelm:ci -->");
            let text: String = {
                let mut t = l[2..].to_string();
                while let (Some(x), Some(y)) = (t.find("<!--"), t.find("-->")) {
                    if y < x {
                        break;
                    }
                    t.replace_range(x..y + 3, "");
                }
                one_line(&t)
            };
            // Who closes it is the gate's call, never a word list: an item
            // reaches a person only for a listed reason.
            let person = matches!(
                common::person_gate::line_outcome(common::person_gate::Kind::OpenItem, &text),
                common::person_gate::Outcome::Human(_)
            );
            OpenItem {
                text,
                closer: if ci {
                    Closer::Ci
                } else if person {
                    Closer::Person
                } else {
                    Closer::CoderHelm
                },
            }
        })
        .collect()
}

fn stated_level(body: &str) -> Option<Level> {
    let text = section(body, &["risk"]).join(" ");
    [
        ("**High**", Level::High),
        ("**Medium**", Level::Medium),
        ("**Low**", Level::Low),
    ]
    .into_iter()
    .find(|(w, _)| text.contains(w))
    .map(|(_, l)| l)
}

fn rank(l: Level) -> u8 {
    match l {
        Level::Low => 0,
        Level::Medium => 1,
        Level::High => 2,
    }
}

/// The facts of one head, from what was read. The lead's summary and check
/// are filled by [`shape`].
pub fn facts(i: &Inputs<'_>) -> Facts {
    let checks: Vec<CheckFact> = i
        .checks
        .unwrap_or(&[])
        .iter()
        .map(|c| CheckFact {
            name: c.name.clone(),
            outcome: match c.state {
                State::Passed => Outcome::Passed,
                State::Failed => Outcome::Failed,
                State::Running => Outcome::Running,
                State::DidNotRun => Outcome::DidNotRun,
            },
            url: c.url.clone(),
        })
        .collect();
    let level = match stated_level(i.body) {
        Some(s) if rank(s) > rank(i.floor.level) => s,
        _ => i.floor.level,
    };
    let open = open_of(i.body);
    let mut f = Facts {
        head: i.head.to_string(),
        why: i.why.clone(),
        files: i
            .files
            .iter()
            .map(|(path, status, patch)| FileChange {
                path: path.clone(),
                status: status.clone(),
                previous: String::new(),
                changed: changed_lines(patch),
            })
            .collect(),
        files_complete: i.files.len() < COMPARE_FILE_LIMIT,
        checks_read: i.checks.is_some(),
        risk: (!i.files.is_empty()).then(|| Risk {
            level: level.word().to_string(),
            why: i
                .floor
                .reasons
                .first()
                .map(|r| one_line(r))
                .unwrap_or_default(),
            look_first: super::reviewer_guide::look_first(i.files, i.floor),
            rollback: super::reviewer_guide::rollback(i.files),
        }),
        // What is open and nobody is asked to close is not verified yet.
        unverified: open
            .iter()
            .filter(|o| o.closer == Closer::CoderHelm)
            .map(|o| Unverified {
                what: o.text.trim_end_matches('.').to_string(),
                why: OPEN_ITEM.to_string(),
            })
            .collect(),
        open,
        dependency: i.dependency.clone(),
        run_url: i.run_url.to_string(),
        notes: i.notes.clone(),
        evidence: i
            .evidence
            .iter()
            .cloned()
            .chain(question_notes(i.body))
            .collect(),
        review_notes: signoffs(i.body),
        repo_url: i.repo_url.trim_end_matches('/').to_string(),
        ..Default::default()
    };
    // A frozen install a check recorded on this head.
    if let Some(d) = f.dependency.as_mut().filter(|d| d.frozen_install.is_none()) {
        if let Some(c) = checks.iter().find(|c| {
            Family::of(&c.name).contains(&Family::Install) && c.outcome != Outcome::Running
        }) {
            match c.outcome {
                Outcome::Passed => {
                    d.frozen_install = Some((true, format!("`{}` passed", c.name)));
                }
                Outcome::Failed => {
                    d.frozen_install = Some((false, format!("`{}` failed", c.name)));
                }
                _ => {}
            }
        }
    }
    if let Some(c) = i.outside {
        f.steps = super::outside_work::steps_now(c, true);
        f.steps.extend(super::outside_work::person_checks(c));
        // A look at a page belongs to the reviewer's merge decision.
        f.review_notes
            .extend(f.steps.iter().filter_map(|s| s.for_review()));
        f.checked = super::outside_work::checked_now(c);
        // A test the note wants run is CI's to run, and CoderHelm's to read.
        // "none, because ... . The unit test x must pass": only what is to
        // run is read.
        let named = c.confirm.as_ref().map(|x| one_line(&x.e2e)).map(|e| {
            if e.to_lowercase().starts_with("none") {
                e.split_once(". ")
                    .map(|(_, rest)| rest.to_string())
                    .unwrap_or_default()
            } else {
                e
            }
        });
        if let Some(e2e) = named
            .map(|e| e.trim_end_matches('.').to_string())
            .filter(|e| !e.is_empty())
        {
            let all = !checks.is_empty()
                && checks
                    .iter()
                    .filter(|c| c.outcome != Outcome::DidNotRun)
                    .all(|c| c.outcome == Outcome::Passed);
            if all {
                f.checked.push(Checked {
                    what: format!("the tests the change names ({e2e}) run in CI"),
                    result: "every check passed".to_string(),
                    when: format!("on `{}`", f.short_head()),
                });
            } else {
                f.unverified.push(Unverified {
                    what: format!("the tests the change names ({e2e})"),
                    why: "CI has not passed on this head".to_string(),
                });
            }
        }
        let mut raw = super::outside_work::raw_evidence(c);
        raw.extend(f.evidence.clone());
        f.evidence = raw;
    }
    f.checks = checks;
    f
}

/// The head's preview in the facts: its state, its address, the commands
/// that check the changed area, and each check that covers the change and
/// didn't run, as something not verified yet (never as evidence).
pub fn with_preview(f: &mut Facts, preview: Option<&common::preview::State>) {
    let Some(p) = preview else {
        return;
    };
    f.preview = Some(p.clone());
    if p.url.starts_with("https://") {
        f.preview_url = p.url.clone();
    }
    if !p.how_to_check.trim().is_empty() {
        f.how_to_check = p.how_to_check.clone();
    }
    for g in common::pr_facts::preview_gaps(f) {
        f.unverified.push(Unverified {
            what: format!("`{g}` on this head"),
            why: "it covers this change and hasn't run yet; CoderHelm starts it on the preview and reads its result".into(),
        });
    }
    if p.e2e_required && !p.e2e_covered {
        f.unverified.push(Unverified {
            what: "an end-to-end spec for the change a person sees".into(),
            why: "the repository has an end-to-end suite and the change adds no spec to it; CoderHelm writes one".into(),
        });
    }
    scope_to_change(f);
}

/// The backticked check commands a line names.
fn commands_in(text: &str) -> Vec<String> {
    text.split('`')
        .skip(1)
        .step_by(2)
        .filter(|c| common::ci_cover::is_check_command(c))
        .map(str::to_string)
        .collect()
}

/// Holds "How to check" and Open items to the change: a check command
/// aimed at a folder the change doesn't touch is dropped (the CI job that
/// covers the change, or "no CI job covers this", is said instead), and a
/// failed end-to-end check is said from its own report, never "couldn't be
/// read" while it has one (`common::ci_cover`, `common::e2e::failure_read`).
pub fn scope_to_change(f: &mut Facts) {
    let Some(p) = f.preview.clone() else {
        return;
    };
    let changed: Vec<String> = f.files.iter().map(|x| x.path.clone()).collect();
    let fits = |t: &str| {
        commands_in(t)
            .iter()
            .all(|c| common::ci_cover::command_fits(c, &changed))
    };
    let before = f.open.len() + f.unverified.len();
    f.open.retain(|o| fits(&o.text));
    f.unverified.retain(|u| fits(&u.what) && fits(&u.why));
    if !p.failures.is_empty() {
        let unread = |t: &str| common::e2e::failure_read::says_unreadable(t);
        f.open.retain(|o| !unread(&o.text));
        f.unverified.retain(|u| !unread(&u.what) && !unread(&u.why));
    }
    let dropped = before != f.open.len() + f.unverified.len();
    if !fits(&f.how_to_check) {
        f.how_to_check = match p.ci_jobs.first() {
            Some(j) => {
                let cmd = j.commands.first().cloned().unwrap_or_default();
                if j.dir.is_empty() {
                    format!("`{cmd}`")
                } else {
                    format!("`cd {} && {cmd}`", j.dir)
                }
            }
            None => String::new(),
        };
    }
    let ci = common::ci_cover::line(&p.ci_jobs);
    if !f.evidence.contains(&ci) {
        f.evidence.push(ci.clone());
    }
    if dropped && p.ci_jobs.is_empty() && !f.notes.contains(&format!("{ci}.")) {
        f.notes.push(format!("{ci}."));
    }
    for r in &p.failures {
        let line = r.line();
        if !f.evidence.contains(&line) {
            f.evidence.push(line.clone());
        }
        let what = format!("`{}` on this head", r.check);
        if matches!(r.cause, common::e2e::failure_read::Cause::ThisChange(_))
            && !f.unverified.iter().any(|u| u.what == what)
        {
            f.unverified.push(Unverified {
                what,
                why: "it fails because of this change (its report says why); CoderHelm fixes it like any failing check".into(),
            });
        }
    }
}

/// A description in its final shape, the facts it shows and what the check
/// corrected on the way.
#[derive(Debug, Clone)]
pub struct Shaped {
    pub body: String,
    pub facts: Facts,
    pub corrections: Vec<Correction>,
}

/// The written text of `body` for the reader pass: no lead, no folds, none
/// of CoderHelm's own blocks.
pub fn written(body: &str) -> String {
    let flat = common::pr_layout::strip_generated(body);
    let (_, rest) = super::completeness::take_owned(&flat);
    rest
}

/// `body` (as it stands, in any earlier shape) in its final shape for
/// `facts`. `reply` is the reader pass's answer when a model was asked.
/// Shaping twice gives the same text. The eval and the incident cases
/// shape with no template; the rebuild calls [`shape_for`].
#[cfg(test)]
pub fn shape(body: &str, title: &str, facts: Facts, reply: Option<&str>) -> Shaped {
    shape_for(body, title, facts, reply, None)
}

/// [`shape`], laid into the repository's pull request `template` when it
/// has one: its headings are the visible structure and CoderHelm's content
/// fills them (`pr_body::render_templated`); the written sections that went
/// into sight are not said again below.
pub fn shape_for(
    body: &str,
    title: &str,
    mut facts: Facts,
    reply: Option<&str>,
    template: Option<&str>,
) -> Shaped {
    let prior = Slots::from_body(body, &facts);
    // What a person ticked stays ticked.
    let ticked = common::pr_body::ticked_ids(body);
    let flat = common::pr_layout::strip_generated(body);
    // 1. Every written sentence against the facts.
    let (checked, corrections) = pr_claims::enforce(&flat, &facts);
    // What is open is what the checked text still lists: an item the facts
    // refuted is gone from the lead as well.
    let open = open_of(&checked);
    facts.unverified.retain(|u| u.why != OPEN_ITEM);
    facts.unverified.extend(
        open.iter()
            .filter(|o| o.closer == Closer::CoderHelm)
            .map(|o| Unverified {
                what: o.text.trim_end_matches('.').to_string(),
                why: OPEN_ITEM.to_string(),
            }),
    );
    facts.open = open;
    // The risk's reason, from the checked text, when the diff gave none.
    if let Some(r) = facts.risk.as_mut().filter(|r| r.why.is_empty()) {
        let said = one_line(&section(&checked, &["risk"]).join(" "));
        let said = said
            .trim_start_matches(|c: char| !c.is_alphanumeric() && c != '*')
            .trim_start_matches("**Low**")
            .trim_start_matches("**Medium**")
            .trim_start_matches("**High**")
            .trim_start_matches([',', ':', '.', ' ', '—', '-', '–']);
        let first = said.split(". ").next().unwrap_or("").trim_end_matches('.');
        if first.split_whitespace().count() >= 2 && first.chars().count() <= 140 {
            r.why = first.to_string();
        }
    }
    // 2. The summary and the check: a verified reply, what the description
    //    already carried, then the records.
    let text = written(&checked);
    let slots = reply
        .map(|r| pr_reader::accept(r, &facts, &text))
        .unwrap_or_default();
    let first = |a: &str, b: &str, c: String| {
        if !a.is_empty() {
            a.to_string()
        } else if !b.is_empty() {
            b.to_string()
        } else {
            c
        }
    };
    let from_title = {
        let t = common::pr_conventions::bare(title).2;
        let mut c = t.chars();
        let t = match c.next() {
            Some(f) => f.to_uppercase().chain(c).collect::<String>(),
            None => t,
        };
        if t.is_empty() || t.ends_with('.') {
            t
        } else {
            format!("{t}.")
        }
    };
    let own = summary_of(&checked);
    // A summary the facts arrive with (written for this head) comes after a
    // verified reply and before what the description carried.
    let given = std::mem::take(&mut facts.summary);
    let earlier = first(&given, &prior.summary, String::new());
    facts.summary = first(
        &slots.summary,
        &earlier,
        if own.is_empty() { from_title } else { own },
    );
    // The commands read from the change itself (its specs and tests) come
    // before any written one: a bare suite command never stands for them.
    let computed = std::mem::take(&mut facts.how_to_check);
    facts.how_to_check = if computed.trim().is_empty() {
        first(&slots.check, &prior.check, check_of(&checked))
    } else {
        computed
    };
    // Open items were read again from the text: held to the change again.
    scope_to_change(&mut facts);
    let checked = pr_reader::without_sections(&checked, &slots.drop);
    // 3. The visible part, the generated blocks below it and the steps and
    //    evidence, from the facts; the written text goes in the first fold.
    let b = common::pr_facts::body_of(&facts, &ticked);
    let (r, checked) = match template.filter(|t| !common::pr_template::sections(t).is_empty()) {
        Some(t) => {
            let (r, used) = common::pr_body::render_templated(&b, t, &written(&checked));
            (r, pr_reader::without_sections(&checked, &used))
        }
        None => (common::pr_body::render(&b), checked),
    };
    // The head is named once, in the Verification lead-in.
    let short = format!("on `{}`", facts.short_head());
    let own_checks: Vec<Checked> = facts
        .checked
        .iter()
        .map(|c| Checked {
            when: if c.when.trim() == short {
                String::new()
            } else {
                c.when.clone()
            },
            ..c.clone()
        })
        .collect();
    let evidence = pr_steps::evidence_block(&facts.steps, &own_checks, &facts.evidence);
    let mut out = format!("{LEAD_START}\n{}\n\n{LEAD_END}\n\n", r.visible);
    out.push_str(checked.trim());
    for (start, inner, end) in [
        (
            common::pr_body::NOTES_TOP_START,
            r.notes_top.as_str(),
            common::pr_body::NOTES_TOP_END,
        ),
        (
            common::pr_body::CHECKS_START,
            r.evidence.as_str(),
            common::pr_body::CHECKS_END,
        ),
        (EVIDENCE_START, "", EVIDENCE_END),
        (
            common::pr_body::BUILT_START,
            r.built.as_str(),
            common::pr_body::BUILT_END,
        ),
        (
            common::pr_body::FOOTER_START,
            r.footer.as_str(),
            common::pr_body::FOOTER_END,
        ),
    ] {
        let b = if start == EVIDENCE_START {
            evidence.clone()
        } else {
            common::pr_body::block(start, inner, end)
        };
        if !b.is_empty() {
            out.push_str("\n\n");
            out.push_str(&b);
        }
    }
    // 4. What is in sight, what is collapsed, and the size limit.
    let laid = common::pr_layout::layout(out.trim());
    let body = common::pr_body_fit::fit_to(
        &laid,
        common::pr_body::BODY_MAX,
        Some(facts.run_url.as_str()).filter(|u| !u.trim().is_empty()),
    );
    Shaped {
        body,
        facts,
        corrections,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn files() -> Vec<(String, String, String)> {
        vec![(
            "src/export/poller.rs".to_string(),
            "modified".to_string(),
            "@@ -1 +1,3 @@\n+retry\n".to_string(),
        )]
    }

    const BODY: &str = "Source ticket: OPS-41\n\n1. **Summary** — The export poller retries a timed-out poll three times before it gives up.\n\n2. **Changes**\n\n* `src/export/poller.rs`: the retry loop.\n* `src/export/writer.rs`: flushes after each batch.\n\n3. **Risk** — **Low**, contract unchanged.\n\n4. **Verification**\n\n1. Run `cargo test -p export`.\n\n<!-- coderhelm:unverified -->\n## Open items\n- The export against the live bucket was not run.\n- Lint has not run yet. <!-- coderhelm:ci -->\n<!-- /coderhelm:unverified -->\n\n<sub>Consulted: the platform agent</sub>";

    fn inputs<'a>(
        files: &'a [(String, String, String)],
        floor: &'a Floor,
        checks: &'a [Check],
    ) -> Inputs<'a> {
        Inputs {
            head: "abc1234def",
            body: BODY,
            files,
            floor,
            checks: Some(checks),
            why: Some(Why {
                reference: "OPS-41".into(),
                title: "Export poller gives up on the first timeout".into(),
                url: String::new(),
            }),
            dependency: None,
            outside: None,
            run_url: "https://app.example.com/runs/detail?id=run-1",
            notes: vec![],
            evidence: vec![],
            repo_url: "https://github.com/acme/site",
        }
    }

    #[test]
    fn an_ordinary_description_is_shaped_from_its_facts() {
        let files = files();
        let floor = super::super::pr_risk::floor(&files, &HashMap::new());
        let checks = vec![Check {
            name: "unit tests".into(),
            url: "https://ci.example.com/1".into(),
            state: State::Passed,
        }];
        let f = facts(&inputs(&files, &floor, &checks));
        assert_eq!(f.open.len(), 2);
        assert_eq!(f.open[1].closer, Closer::Ci);
        let s = shape(BODY, "OPS-41: Export poller gives up", f.clone(), None);
        // The phantom file and the unsupported word were corrected.
        assert_eq!(s.corrections.len(), 2, "{:?}", s.corrections);
        assert!(!s.body.contains("writer.rs"), "{}", s.body);
        assert!(!s.body.contains("contract unchanged"), "{}", s.body);
        let seen = common::pr_layout::visible(&s.body);
        assert!(
            seen.starts_with("### Problem\nExport poller gives up on the first timeout (OPS-41)."),
            "{seen}"
        );
        assert!(
            seen.contains(
                "### Changes\nThe export poller retries a timed-out poll three times before it gives up."
            ),
            "{seen}"
        );
        assert!(seen.contains("| unit tests (CI) | Passed |"), "{seen}");
        assert!(
            seen.contains(
                "| The export against the live bucket | Not run: nothing has checked it yet |"
            ),
            "{seen}"
        );
        assert!(seen.contains("**Risk:** Low. "), "{seen}");
        // Nothing is needed: the section is not there.
        assert!(!seen.contains("Needed from you"), "{seen}");
        // How to check is in Verification; the ticket line and the
        // walkthrough are below.
        assert!(
            seen.contains("How to check: `cargo test -p export`."),
            "{seen}"
        );
        for below in ["Source ticket: OPS-41", "the retry loop"] {
            assert!(
                s.body.contains(below) && !seen.contains(below),
                "{below}: {}",
                s.body
            );
        }
        assert_eq!(common::pr_layout::rubric(&s.body, &s.facts), vec![]);
        // Shaping what was shaped changes nothing.
        let again = shape(&s.body, "OPS-41: Export poller gives up", f, None);
        assert_eq!(again.body, s.body);
        assert!(again.corrections.is_empty());
    }

    #[test]
    fn only_a_question_a_person_alone_can_settle_is_needed_from_them() {
        let body = "## Summary\n\nAdds the card block.\n\n<!-- coderhelm:not-in-pr -->\n## Questions only a person can answer\n1. Should the card link open in a new tab?\n<!-- /coderhelm:not-in-pr -->\n";
        // A reversible choice is decided and noted, never asked.
        assert!(open_of(body).is_empty());
        assert_eq!(question_notes(body).len(), 1);
        assert!(
            question_notes(body)[0].starts_with("Chose "),
            "{:?}",
            question_notes(body)
        );
        // One that reaches customers and nothing settles is a yes/no.
        let refund = body.replace(
            "Should the card link open in a new tab?",
            "Should cancelled bookings be refunded to customers?",
        );
        let open = open_of(&refund);
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].closer, Closer::Person);
        assert_eq!(
            open[0].text,
            "Answer: Should cancelled bookings be refunded to customers?"
        );
        let asked = shape(
            &refund,
            "GH-41: Card block",
            Facts {
                head: "abc1234".into(),
                ..Default::default()
            },
            None,
        );
        assert!(
            common::pr_layout::visible(&asked.body).contains(
                "### Needed from you\n- [ ] <!-- ch:should-cancelled-bookings-be-refunded-to --> **Ticket's owner**, before merge: Yes or no: Should cancelled bookings be refunded to customers? If nobody answers: going ahead as built."
            ),
            "{}",
            asked.body
        );
        let s = shape(
            body,
            "GH-41: Card block",
            Facts {
                head: "abc1234".into(),
                ..Default::default()
            },
            None,
        );
        assert!(
            !common::pr_layout::visible(&s.body).contains("Needed from you"),
            "{}",
            s.body
        );
    }

    #[test]
    fn the_reader_pass_fills_the_lead_only_with_what_it_can_verify() {
        let files = files();
        let floor = super::super::pr_risk::floor(&files, &HashMap::new());
        let f = facts(&inputs(&files, &floor, &[]));
        let reply = r#"{"summary": "Retries a timed-out export poll three times. All tests pass.", "check": "cargo test -p export", "drop": []}"#;
        let s = shape(BODY, "t", f.clone(), Some(reply));
        assert!(
            s.body.contains(
                "\nRetries a timed-out export poll three times.<!-- ch:summary-end -->\n"
            ),
            "{}",
            s.body
        );
        assert!(!s.body.contains("All tests pass"), "{}", s.body);
        // An invented command is refused; the description's own stays.
        let s = shape(
            BODY,
            "t",
            f,
            Some(r#"{"summary": "x", "check": "make ship"}"#),
        );
        assert!(
            s.body.contains("How to check: `cargo test -p export`."),
            "{}",
            s.body
        );
        assert!(
            s.body
                .contains("The export poller retries a timed-out poll"),
            "{}",
            s.body
        );
    }
}
