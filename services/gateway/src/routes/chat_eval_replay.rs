//! Chat's replay evals: does Chat understand what a person asks, and answer
//! that? The cases (`chat_eval_cases.json`) are real conversations, made
//! anonymous and moved onto the made-up marina team of `chat_eval`, and
//! variants that cover every kind of question: an orientation, what Chat can
//! do, what needs me, a status, a how-to, a lookup, a judgement, a
//! correction of Chat itself, and small talk. Some carry the thread before
//! the question (a correction needs the answer it corrects).
//!
//! Each case runs through the real answer loop, prepared exactly as
//! production prepares a question ([`prepare`], `brain::answer`), over the
//! team's code, work, knowledge, logs and skills, and over the team's
//! setup ([`team_setup`]). A model judge grades the answer once against a
//! rubric ([`JUDGE_SYSTEM`]): it understood the question, answered that
//! first, checked every claim in this answer, fits the question's size, and
//! has the right tone.
//!
//! `offline_*` tests run on every build with a scripted model. The live run
//! asks the real model and writes one JSON line per case:
//!
//! ```text
//! CHAT_EVAL_API_KEY=... CHAT_EVAL_OUT=/path/replay.jsonl \
//!   cargo test -p gateway live_replay_eval -- --ignored --nocapture
//! ```

use common::knowledge::answer::{Registry, SourceRef};
use common::repo_read::{MemHost, RepoInfo};
use serde::Deserialize;
use serde_json::{json, Value};

use super::brain::{mention_note, system_prompt, Message};
use super::chat_agent::testing::{CodeBox, Recorder};
use super::chat_agent::{self, Answered, Ask, Called, MustLook, Sink, Toolbox, LIMITS};
use super::chat_eval::{fixture, work_store, WorkBox, WorkStore, TEAM};
use super::chat_work as work;

/// One question to replay.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplayCase {
    pub name: String,
    /// `real` (from a real conversation, anonymous) or `variant`.
    pub origin: String,
    /// The kind of question it is (`common::chat_intent::Intent::key`).
    pub intent: String,
    /// The thread before the question, as `(role, text)`.
    #[serde(default)]
    pub history: Vec<(String, String)>,
    pub question: String,
    /// What the person wanted from the answer, for the judge.
    pub wants: String,
}

pub fn cases() -> Vec<ReplayCase> {
    serde_json::from_str(include_str!("chat_eval_cases.json")).expect("chat_eval_cases.json")
}

pub fn history_of(c: &ReplayCase) -> Vec<Message> {
    c.history
        .iter()
        .enumerate()
        .map(|(i, (role, content))| Message {
            seq: i as u32,
            role: role.clone(),
            content: content.clone(),
            ..Default::default()
        })
        .collect()
}

// ── The team ────────────────────────────────────────────────────────────────

/// The team's infrastructure repository.
pub const INFRA: &str = "northwind/harbor-infra";

const INFRA_FILES: &[(&str, &str)] = &[
    (
        "README.md",
        "# Harbor infra\n\nTerraform for the Harbor apps: `envs/prod` and `envs/staging`, \
         modules in `modules/`.\n",
    ),
    (
        "envs/prod/waf.tf",
        "# Crawler B7: the team's name for one bot that crawls the slip listing pages. It looks\n\
         # like ordinary Chrome on Windows, sends no cookie, comes through residential proxies and\n\
         # changes its Chrome version often, so fingerprint rules miss it. It is matched by the\n\
         # shape of its requests: a listing page filtered for 4 or more marinas, with no cookie\n\
         # and no Referer. Counted only for now; switch to block once the counts agree.\n\
         module \"waf\" {\n  source = \"../../modules/waf\"\n  signature_b7 = {\n    \
         action = \"count\"\n    paths = [\"/slips\", \"/moorings\"]\n    min_marinas = 4\n  }\n}\n",
    ),
    (
        "envs/prod/alarms.tf",
        "# harbor-web tasks ran out of local disk once: the Next.js fetch cache under\n\
         # .next/cache/fetch-cache grows with every fetch and nothing clears it, so a task that\n\
         # stays up long enough fills its disk and every write fails with ENOSPC. The disk size\n\
         # was left at Fargate's default on purpose. This alarm is the safety net.\n\
         resource \"aws_cloudwatch_metric_alarm\" \"web_enospc\" {\n  alarm_name = \
         \"prod-harbor-web-enospc\"\n  threshold  = 1\n  period     = 300\n}\n",
    ),
];

/// The team's repositories, with the infrastructure one.
pub fn replay_fixture() -> (MemHost, Vec<RepoInfo>) {
    let (host, mut repos) = fixture();
    let host = host.with_repo(INFRA, "5d0c11aa90", INFRA_FILES);
    repos.push(RepoInfo {
        role: Some("infra".into()),
        ..RepoInfo::new(INFRA, "main")
    });
    (host, repos)
}

/// The team's knowledge passages: `(file, page, text)`.
pub const KNOWLEDGE: &[(&str, &str, &str)] = &[
    (
        "Booking policy.pdf",
        "Page 2",
        "Cancelling a slip booking: cancel from My bookings in the Harbor app, or ask the harbor \
         office to cancel it for you. Cancel at least 48 hours before arrival to get the full \
         amount back; a later cancellation gets half back. A no-show gets nothing back.",
    ),
    (
        "Booking policy.pdf",
        "Page 3",
        "Moving a booking to another date: one free change with at least 24 hours' notice, to a \
         date within one year of the original. A later change counts as a cancellation and a new \
         booking.",
    ),
    (
        "Event bookings.pdf",
        "Page 1",
        "Party and event bookings: cancel more than 5 days before the event and only the deposit \
         is kept; 5 days or less before, the deposit and the full balance are kept.",
    ),
];

const KB_NAME: &str = "Member handbook";
pub const KNOWLEDGE_TOOL: &str = "search_knowledge";
pub const LOGS_TOOL: &str = "logs_search";
pub const SKILL_TOOL: &str = "read_skill";

fn words(text: &str) -> Vec<String> {
    work::words_of(text)
        .into_iter()
        .filter(|w| w.chars().count() >= 4)
        .collect()
}

/// The passages sharing at least two words with `query` (a booking and a
/// bookings are one word).
pub fn search_knowledge(query: &str) -> Vec<(&'static str, &'static str, &'static str)> {
    let q = words(query);
    let stem = |w: &str| w.trim_end_matches('s').to_string();
    KNOWLEDGE
        .iter()
        .filter(|(_, _, text)| {
            let t: Vec<String> = words(text).iter().map(|w| stem(w)).collect();
            q.iter().filter(|w| t.contains(&stem(w))).count() >= 2
        })
        .copied()
        .collect()
}

fn add_passages(reg: &mut Registry, found: &[(&str, &str, &str)]) -> Vec<String> {
    found
        .iter()
        .map(|(title, loc, text)| {
            reg.add(SourceRef {
                kind: "knowledge".into(),
                title: title.to_string(),
                loc: loc.to_string(),
                as_of: "2026-09-01".into(),
                passage: text.to_string(),
                base: KB_NAME.into(),
                href: Some("/knowledge/base/?id=kb1".into()),
                ..Default::default()
            })
        })
        .collect()
}

const LOG_LINES: &str = "Searched harbor-web's log group over the last 14 days.\n\
26,599 matching lines, all from task 7f3a91c0 (started 2026-11-15 17:54 UTC), the first at 2026-11-18 04:44 UTC and the latest a minute ago.\n\
2026-11-18T04:44:10Z [7f3a91c0] Failed to update prerender cache: ENOSPC: no space left on device, open '/app/.next/cache/fetch-cache/3f9a...'\n\
2026-11-18T09:12:51Z [7f3a91c0] ENOSPC: no space left on device, write\n\
Other tasks in the window ran a few hours to a day before they were replaced; none logged ENOSPC.";

const SKILLS: &str = "The team's skills: skill:harbor-frontend (how the web app is laid out and its conventions), \
skill:code-standards (review rules), skill:e2e-test (runs the Playwright end-to-end suite from a terminal: \
`npx playwright test`), skill:sanity-deploy (deploys the CMS from a terminal).";

/// The team's setup, as `coderhelm_setup` would find it: GitHub and Jira
/// connected, the runs of the work store counted.
pub fn team_setup(store: &WorkStore) -> super::chat_setup::Setup {
    use super::chat_setup::{github_check, jira_check, Check, Connection, Counted, Setup};
    let items: Vec<super::chat_work_runs::Item> = store
        .runs
        .iter()
        .filter(|(t, _)| t == TEAM)
        .map(|(_, i)| i.clone())
        .collect();
    let runs = super::chat_work_runs::tally_of(&items, "2026-10-20T00:00:00Z", 30, false);
    Setup {
        checked_at: "2026-11-19 14:00 UTC".into(),
        role: "admin".into(),
        can_change_integrations: true,
        github_linked: true,
        connections: vec![
            Connection {
                name: "GitHub".into(),
                check: github_check(Some(42), true, 3),
            },
            Connection {
                name: "Jira".into(),
                check: jira_check(&super::chat_work_tickets::Jira {
                    app: Some(super::chat_work_tickets::JiraApp {
                        issues_url: "https://example.invalid/issues".into(),
                        secret: "x".into(),
                        site: "https://northwind.atlassian.net".into(),
                    }),
                    off: String::new(),
                }),
            },
            Connection {
                name: "AWS account 111122223333".into(),
                check: Check::Ok(
                    "CloudWatch logs and metrics read through a read-only role".into(),
                ),
            },
        ],
        repos: vec![
            super::chat_eval::API.into(),
            super::chat_eval::WEB.into(),
            INFRA.into(),
        ],
        knowledge: vec![KB_NAME.into()],
        runs: Some(Ok(runs)),
        records: vec![
            Counted {
                what: "pull request reviews",
                n: Ok(12),
                at_least: false,
            },
            Counted {
                what: "open security findings",
                n: Ok(1),
                at_least: false,
            },
            Counted {
                what: "open infrastructure findings",
                n: Ok(2),
                at_least: false,
            },
        ],
        reads: vec![
            "Files and pages".into(),
            "Code".into(),
            "Logs and data".into(),
            "Team know-how".into(),
            "Tickets, pull requests and runs".into(),
        ],
    }
}

/// Every tool the team's Chat has: code, work, knowledge, logs, skills,
/// and the side's own tools ([`side_tools`]).
pub struct ReplayBox<'a> {
    pub code: CodeBox<'a>,
    pub work: WorkBox<'a>,
    pub defs: Vec<Value>,
    pub store: &'a WorkStore,
}

impl<'a> ReplayBox<'a> {
    pub fn new(host: &'a MemHost, repos: &[RepoInfo], store: &'a WorkStore) -> Self {
        let code = CodeBox::new(host, repos.to_vec());
        let all: Vec<&'static str> = work::KINDS.iter().map(|k| k.key).collect();
        let work = WorkBox::new(store, TEAM, all);
        let mut defs = code.defs.clone();
        defs.push(json!({
            "name": KNOWLEDGE_TOOL,
            "description": "Search the team's knowledge bases again with different words. Returns passages as <source> blocks you can cite.",
            "input_schema": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}
        }));
        defs.push(json!({
            "name": LOGS_TOOL,
            "description": "Search the team's CloudWatch logs (read only) for words or an error, over the last days.",
            "input_schema": {"type": "object", "properties": {
                "query": {"type": "string"}, "source": {"type": "string"}, "hours": {"type": "integer"}
            }, "required": ["query"]}
        }));
        defs.push(json!({
            "name": SKILL_TOOL,
            "description": "Read the team's skills (written guidance and helpers) by name, or list them with no name.",
            "input_schema": {"type": "object", "properties": {"name": {"type": "string"}}}
        }));
        defs.extend(work.defs.iter().cloned());
        defs.extend(side_tools());
        ReplayBox {
            code,
            work,
            defs,
            store,
        }
    }
}

fn one(reg: &mut Registry, kind: &str, title: &str, passage: &str) -> String {
    let id = reg.add(SourceRef {
        kind: kind.into(),
        title: title.into(),
        as_of: "2026-11-19 14:00 UTC".into(),
        passage: passage.into(),
        ..Default::default()
    });
    reg.render(&[id])
}

#[async_trait::async_trait]
impl Toolbox for ReplayBox<'_> {
    fn defs(&self) -> &[Value] {
        &self.defs
    }
    async fn call(
        &mut self,
        reg: &mut Registry,
        sink: &dyn Sink,
        name: &str,
        input: &Value,
    ) -> Called {
        let done = |text: String, label: &str, summary: &str, kind: &str| Called::Done {
            text,
            label: label.into(),
            summary: summary.into(),
            is_error: false,
            kind: kind.into(),
        };
        match name {
            n if super::chat_code::is_code_tool(n) => self.code.call(reg, sink, name, input).await,
            n if work::is_work_tool(n) => self.work.call(reg, sink, name, input).await,
            KNOWLEDGE_TOOL => {
                let found = search_knowledge(input["query"].as_str().unwrap_or(""));
                if found.is_empty() {
                    return done(
                        "No passages matched.".into(),
                        "Searched knowledge",
                        "No passages matched",
                        "knowledge",
                    );
                }
                let ids = add_passages(reg, &found);
                done(
                    reg.render(&ids),
                    "Searched knowledge",
                    "passages",
                    "knowledge",
                )
            }
            LOGS_TOOL => {
                let q = input["query"].as_str().unwrap_or("").to_lowercase();
                if ["enospc", "space", "disk", "cache"]
                    .iter()
                    .any(|w| q.contains(w))
                {
                    let text = one(
                        reg,
                        "data_source",
                        "Searched the logs in harbor-web",
                        LOG_LINES,
                    );
                    done(text, "Searched the logs", "26,599 lines", "data_sources")
                } else {
                    done(
                        "No results found.".into(),
                        "Searched the logs",
                        "No results",
                        "data_sources",
                    )
                }
            }
            SKILL_TOOL => {
                let text = one(reg, "skill", "The team's skills", SKILLS);
                done(text, "Read the team's skills", "Read", "skills")
            }
            super::chat_setup::SETUP => {
                let (text, summary) = super::chat_setup::render(&team_setup(self.store), reg);
                done(
                    text,
                    "Checked Coderhelm's setup for the team",
                    &summary,
                    "work",
                )
            }
            _ => done(format!("Unknown tool: {name}"), name, "Unknown", ""),
        }
    }
}

// ── Preparing a question, as production does ────────────────────────────────

/// The tools this side of the comparison has beyond the team's sources.
pub fn side_tools() -> Vec<Value> {
    vec![super::chat_setup::def()]
}

/// What `brain::answer` does before the loop: read the question with the
/// thread, order the tools, write the notes, decide where it must look and
/// whether an uncited reply stands. Returns `(notes, must_look, uncited_ok)`.
pub fn prepare(
    question: &str,
    history: &[Message],
    tb: &mut ReplayBox<'_>,
    repos: &[RepoInfo],
) -> (String, Option<MustLook>, bool) {
    let understood = super::chat_understand::read(question, history);
    tb.work.needs_shape = understood.needs_shape();
    understood.order(&mut tb.defs, question);
    let offered: Vec<&str> = tb.defs.iter().filter_map(|t| t["name"].as_str()).collect();
    let mut notes = mention_note(repos, question);
    notes.push_str(&understood.notes(&offered));
    notes.push_str(&format!(
        "\nThe team's own tools for this question:\n{SKILLS}\n"
    ));
    (
        notes,
        understood.must_look(&offered, question),
        understood.uncited_ok(),
    )
}

/// The model's input, as `brain::build_messages` writes it: the thread,
/// then the sources found first, the notes and the question.
pub fn messages(history: &[Message], sources: &str, notes: &str, question: &str) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    for m in history {
        let role = if m.role == "assistant" {
            "assistant"
        } else {
            "user"
        };
        if out.last().is_some_and(|l| l["role"] == role) {
            continue;
        }
        out.push(json!({ "role": role, "content": [{"type": "text", "text": m.content}] }));
    }
    if out.last().is_some_and(|l| l["role"] == "user") {
        out.pop();
    }
    if out.first().is_some_and(|f| f["role"] == "assistant") {
        out.remove(0);
    }
    let block = if sources.trim().is_empty() {
        "(no knowledge passages matched this question)".to_string()
    } else {
        sources.to_string()
    };
    out.push(json!({ "role": "user", "content": [{"type": "text", "text":
        format!("<sources>\n{block}</sources>\n{notes}\nQuestion: {question}")
    }]}));
    out
}

/// One case through the real loop. Returns the answer and every source read.
pub async fn replay(
    model: &dyn chat_agent::Model,
    host: &MemHost,
    repos: &[RepoInfo],
    store: &WorkStore,
    case: &ReplayCase,
) -> (Answered, Registry) {
    let history = history_of(case);
    let mut tb = ReplayBox::new(host, repos, store);
    let mut reg = Registry::default();
    // Knowledge first, in code, as production retrieves it.
    let first = add_passages(
        &mut reg,
        &search_knowledge(&super::brain::retrieval_query(&case.question, &history)),
    );
    let (notes, must_look, uncited_ok) = prepare(&case.question, &history, &mut tb, repos);
    let sink = Recorder::default();
    let msgs = messages(&history, &reg.render(&first), &notes, &case.question);
    let out = chat_agent::run(
        model,
        &mut tb,
        &sink,
        &mut reg,
        Ask {
            system: system_prompt(repos, true),
            messages: msgs,
            question: &case.question,
            limits: LIMITS,
            must_look,
            uncited_ok,
        },
    )
    .await
    .unwrap_or_else(|_| panic!("{}: the model couldn't be reached", case.name));
    (out, reg)
}

// ── The judge ───────────────────────────────────────────────────────────────

pub const JUDGE_SYSTEM: &str = "You grade one answer written by a software team's Chat assistant (Coderhelm's Chat). You see the thread before the question, the question, what the person actually wanted (written by the eval's author), everything Chat read while answering (its sources this turn), and the answer the person saw. Grade five things, each true or false:\n\
1. understood: the answer shows Chat understood what the person actually wanted, not a different or template question.\n\
2. answered_first: the first sentence or two answer that question directly; anything else comes after.\n\
3. claims_checked: every claim about connections, setup, counts, state, people or facts in the answer is supported by what Chat read this turn (its sources); nothing is asserted that the sources don't support or that contradicts them; a failed check is described as that check, never as a broader claim like \"X isn't connected\". An answer that says plainly what it couldn't check is fine. A greeting with no claims is true.\n\
4. length_right: the length and structure fit the question: a greeting or a yes-or-no gets a short answer, an orientation is brief and scannable, a diagnosis may be longer; no padding, no unrequested lists or dumps.\n\
5. tone_right: plain, direct and respectful; a mistake is admitted plainly when the person corrects it; no defensiveness, no salesmanship, and no handing work back that the assistant's tools could do. A canned line that doesn't engage with the question is false.\n\
Reply with JSON only: {\"understood\": bool, \"answered_first\": bool, \"claims_checked\": bool, \"length_right\": bool, \"tone_right\": bool, \"why\": \"one or two sentences on any false\"}.";

/// The criteria, in the order they are reported.
pub const CRITERIA: [&str; 5] = [
    "understood",
    "answered_first",
    "claims_checked",
    "length_right",
    "tone_right",
];

pub fn judge_body(model_id: &str, case: &ReplayCase, answer: &str, reg: &Registry) -> Value {
    let thread: String = case
        .history
        .iter()
        .map(|(r, t)| format!("{r}: {}\n", common::truncate_str(t, 1500)))
        .collect();
    let read: String = (1..=reg.len())
        .filter_map(|n| reg.get(&format!("S{n}")))
        .map(|s| {
            format!(
                "[{}] {} ({}): {}\n",
                s.id,
                s.title,
                s.kind,
                common::truncate_str(&s.passage, 900)
            )
        })
        .collect();
    json!({
        "model": model_id,
        "max_tokens": 400,
        "system": JUDGE_SYSTEM,
        "messages": [{"role": "user", "content": [{"type": "text", "text": format!(
            "Thread before the question:\n{}\nQuestion:\n{}\n\nWhat the person wanted:\n{}\n\nWhat Chat read this turn:\n{}\nThe answer the person saw:\n{}",
            if thread.is_empty() { "(none)\n".to_string() } else { thread },
            case.question,
            case.wants,
            if read.is_empty() { "(nothing)\n".to_string() } else { read },
            answer
        )}]}],
    })
}

/// The judge's grades, by [`CRITERIA`], and why.
pub fn read_verdict(reply: &str) -> Option<([bool; 5], String)> {
    let start = reply.find('{')?;
    let end = reply.rfind('}')?;
    let v: Value = serde_json::from_str(reply.get(start..=end)?).ok()?;
    let mut out = [false; 5];
    for (i, k) in CRITERIA.iter().enumerate() {
        out[i] = v[*k].as_bool()?;
    }
    Some((out, v["why"].as_str().unwrap_or("").to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::chat_agent::testing::{calls, say, Scripted, Step};
    use crate::routes::chat_setup::SETUP;
    use crate::routes::chat_work_needs::{ANSWER_SHAPE, NOT_THE_LIST};

    #[test]
    fn every_case_reads_as_the_kind_of_question_it_is() {
        let all = cases();
        assert!(all.len() >= 40, "{}", all.len());
        assert!(all.iter().filter(|c| c.origin == "real").count() >= 8);
        let mut wrong = Vec::new();
        for c in &all {
            let read = super::super::chat_understand::read(&c.question, &history_of(c));
            if read.intent().key() != c.intent {
                wrong.push(format!(
                    "{}: read as {}, is {}",
                    c.name,
                    read.intent().key(),
                    c.intent
                ));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
        // Every kind of question has cases.
        for k in [
            "orientation",
            "capability",
            "needs_me",
            "status",
            "how_to",
            "lookup",
            "judgement",
            "correction",
            "small_talk",
        ] {
            assert!(all.iter().filter(|c| c.intent == k).count() >= 4, "{k}");
        }
    }

    /// The real conversations were made anonymous: every link and
    /// repository in a case is the made-up team's, and no case names a
    /// person by address.
    #[test]
    fn every_case_is_about_the_made_up_team_only() {
        let text = include_str!("chat_eval_cases.json");
        assert!(!text.contains('@'), "an address in a case");
        for (i, _) in text.match_indices("github.com/") {
            assert!(
                text[i..].starts_with("github.com/northwind/"),
                "{}",
                &text[i..i + 40]
            );
        }
        for (i, _) in text.match_indices(".atlassian.net") {
            assert!(
                text[..i].ends_with("northwind"),
                "{}",
                &text[i.saturating_sub(30)..i]
            );
        }
        let repos = [
            super::super::chat_eval::API,
            super::super::chat_eval::WEB,
            INFRA,
        ];
        for c in cases() {
            for h in super::super::chat_hints::read(&c.question).repos {
                assert!(repos.contains(&h.as_str()), "{}: {h}", c.name);
            }
        }
    }

    fn case(name: &str) -> ReplayCase {
        cases().into_iter().find(|c| c.name == name).unwrap()
    }

    /// The incident, replayed: the newcomer's question reached for the
    /// to-do list and wrote it in its fixed shape. Now the list comes back
    /// without its shape, the answer must look the setup up, and it does.
    #[tokio::test]
    async fn offline_the_newcomer_gets_an_orientation_not_the_to_do_list() {
        let c = case("real: a new leader asks what could be useful to them");
        let (host, repos) = replay_fixture();
        let store = work_store();
        let steps: Vec<Step> = vec![
            // As in the incident: the list first, and an answer from it.
            Box::new(|_| calls("n", work::NEEDS_ME, json!({}))),
            Box::new(|b| {
                let r = super::super::chat_agent::testing::last_result(b);
                assert!(r.contains(NOT_THE_LIST) && !r.contains(ANSWER_SHAPE), "{r}");
                say("Welcome. 4 things need you now: ABC-1 is waiting on your answer [S1].")
            }),
            // Sent back: it didn't look at the setup.
            Box::new(|b| {
                let sent = b["messages"].to_string();
                assert!(
                    sent.contains("you didn't look there") && sent.contains(SETUP),
                    "{sent}"
                );
                calls("s", SETUP, json!({}))
            }),
            Box::new(|b| {
                let r = super::super::chat_agent::testing::last_result(b);
                let team = super::super::chat_agent::testing::id_for(&r, "Coderhelm for this team")
                    .unwrap();
                let what =
                    super::super::chat_agent::testing::id_for(&r, "What Coderhelm does").unwrap();
                say(&format!("Coderhelm already works for your team: 3 runs in the last 30 days across harbor-web and harbor-api [{team}]. For a CTO: ask “Where are we on the checkout redesign?” for status, “Which critical findings are open?” for risk [{what}]."))
            }),
        ];
        let model = Scripted::new(steps, vec![r#"{"hands_back": false}"#]);
        let (out, _) = replay(&model, &host, &repos, &store, &c).await;
        assert!(out.rechecked);
        assert_eq!(out.calls, [work::NEEDS_ME, SETUP]);
        assert!(
            out.fin
                .text
                .starts_with("Coderhelm already works for your team"),
            "{}",
            out.fin.text
        );
        assert!(out
            .fin
            .citations
            .iter()
            .any(|s| s.title == "Coderhelm for this team"));
        let bodies = model.bodies.lock().unwrap();
        // The first turn offered the setup first and said how the question reads.
        assert_eq!(bodies[0]["tools"][0]["name"], SETUP);
        let first = bodies[0]["messages"].to_string();
        assert!(
            first.contains("an orientation") && first.contains("the cto"),
            "{first}"
        );
        assert!(!first.contains("Call work_needs_me first"), "{first}");
    }

    /// The follow-up, replayed: a correction is never answered from the
    /// earlier answer's words.
    #[tokio::test]
    async fn offline_a_correction_checks_again_before_it_answers() {
        let c = case("real: the leader questions a claim that Jira isn't connected");
        let (host, repos) = replay_fixture();
        let store = work_store();
        let steps: Vec<Step> = vec![
            Box::new(|_| say("Jira isn't connected to Coderhelm, as I said.")),
            Box::new(|_| calls("s", SETUP, json!({}))),
            Box::new(|_| {
                say("You're right, and I was wrong: Jira is connected through the Jira app, and Chat can look your tickets up [S1].")
            }),
        ];
        let model = Scripted::new(steps, vec![]);
        let (out, _) = replay(&model, &host, &repos, &store, &c).await;
        assert!(out.rechecked);
        assert_eq!(out.calls, [SETUP]);
        assert!(out.fin.text.starts_with("You're right, and I was wrong"));
        let bodies = model.bodies.lock().unwrap();
        let first = bodies[0]["messages"].to_string();
        assert!(
            first.contains("What they question in your last answer"),
            "{first}"
        );
        assert!(
            first.contains("Jira isn't connected to Coderhelm, so your Jira tickets"),
            "{first}"
        );
    }

    /// Small talk is answered as written; anything else uncited looks once
    /// with its tools before the plain no-source line replaces it.
    #[tokio::test]
    async fn offline_small_talk_stands_and_an_uncited_answer_looks_once() {
        let (host, repos) = replay_fixture();
        let store = work_store();
        let hi = case("small talk: hi");
        let model = Scripted::new(
            vec![Box::new(|_| say("Hi! What can I help you with?"))],
            vec![],
        );
        let (out, _) = replay(&model, &host, &repos, &store, &hi).await;
        assert_eq!(out.fin.text, "Hi! What can I help you with?");
        assert!(!out.rechecked && out.calls.is_empty());
        let logs = case("capability: production logs");
        let model = Scripted::new(
            vec![
                Box::new(|_| say("Yes, I can read your logs.")),
                Box::new(|b| {
                    let sent = b["messages"].to_string();
                    assert!(sent.contains("cites nothing read in this answer"), "{sent}");
                    calls("s", SETUP, json!({}))
                }),
                Box::new(|_| {
                    say("Yes: an AWS account is connected, and Chat reads its CloudWatch logs [S1].")
                }),
            ],
            vec![r#"{"hands_back": false}"#],
        );
        let (out, _) = replay(&model, &host, &repos, &store, &logs).await;
        assert!(out.rechecked && out.fin.grounded);
        assert!(out.fin.text.starts_with("Yes: an AWS account is connected"));
    }

    #[test]
    fn the_judge_reads_five_grades_and_sees_what_chat_read() {
        let v = read_verdict("ok {\"understood\": true, \"answered_first\": false, \"claims_checked\": true, \"length_right\": true, \"tone_right\": true, \"why\": \"led with a list\"}").unwrap();
        assert_eq!(v.0, [true, false, true, true, true]);
        assert_eq!(v.1, "led with a list");
        assert!(read_verdict("{\"understood\": true}").is_none());
        let mut reg = Registry::default();
        add_passages(&mut reg, &search_knowledge("How can I cancel a booking?"));
        let b = judge_body(
            "m",
            &case("real: how to cancel a booking (from a knowledge page)"),
            "a",
            &reg,
        );
        let text = b["messages"][0]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("[S1] Booking policy.pdf"), "{text}");
        assert!(text.contains("What the person wanted"));
    }
}

/// The replay set against the real model, judged once per case. Ignored
/// unless asked for.
#[tokio::test]
#[ignore = "calls the real model: set CHAT_EVAL_API_KEY and run with --ignored"]
async fn live_replay_eval() {
    use futures::StreamExt;
    let key = std::env::var("CHAT_EVAL_API_KEY").expect("set CHAT_EVAL_API_KEY");
    let model_id = std::env::var("CHAT_EVAL_MODEL").unwrap_or_else(|_| "claude-sonnet-5-5".into());
    let judge_id = std::env::var("CHAT_EVAL_JUDGE_MODEL").unwrap_or_else(|_| model_id.clone());
    let out_path = std::env::var("CHAT_EVAL_OUT").ok();
    let (meter, _) = common::usage::UsageMeter::memory("TEAM#eval");
    let model = chat_agent::ApiModel {
        http: reqwest::Client::new(),
        api_key: key.clone(),
        meter: meter.clone(),
        model_id: model_id.clone(),
        check_model_id: model_id.clone(),
        kind_of: crate::routes::brain::kind_of_tool,
    };
    let judge = chat_agent::ApiModel {
        http: reqwest::Client::new(),
        api_key: key,
        meter,
        model_id: judge_id.clone(),
        check_model_id: judge_id.clone(),
        kind_of: crate::routes::brain::kind_of_tool,
    };
    let (host, repos) = replay_fixture();
    let store = work_store();
    let all = cases();
    let rows: Vec<Value> = futures::stream::iter(all.iter())
        .map(|c| {
            let (model, judge, host, repos, store) = (&model, &judge, &host, &repos, &store);
            let judge_id = judge_id.clone();
            async move {
                let (out, reg) = replay(model, host, repos, store, c).await;
                let verdict = chat_agent::Model::ask(
                    judge,
                    judge_body(&judge_id, c, &out.fin.text, &reg),
                )
                .await
                .and_then(|r| read_verdict(&r));
                let (grades, why) = verdict.unwrap_or(([false; 5], "the judge's reply couldn't be read".into()));
                println!("\n=== {} ===\nQ: {}\ncalls: {:?}\nA: {}\ngrades: {grades:?} {why}", c.name, c.question, out.calls, out.fin.text);
                json!({
                    "name": c.name, "origin": c.origin, "intent": c.intent,
                    "calls": out.calls, "rechecked": out.rechecked, "answer": out.fin.text,
                    "grades": CRITERIA.iter().zip(grades).map(|(k, g)| (k.to_string(), json!(g))).collect::<serde_json::Map<_, _>>(),
                    "all": grades.iter().all(|g| *g), "why": why,
                })
            }
        })
        .buffer_unordered(6)
        .collect()
        .await;
    if let Some(p) = out_path {
        let lines: String = rows.iter().map(|r| format!("{r}\n")).collect();
        std::fs::write(&p, lines).expect("write CHAT_EVAL_OUT");
    }
    let n = rows.len() as f64;
    println!("\ncases: {}", rows.len());
    for k in CRITERIA.iter().copied().chain(["all"]) {
        let pass = rows
            .iter()
            .filter(|r| {
                if k == "all" {
                    r["all"] == true
                } else {
                    r["grades"][k] == true
                }
            })
            .count();
        println!(
            "{k}: {pass}/{} ({:.0}%)",
            rows.len(),
            100.0 * pass as f64 / n
        );
    }
}
