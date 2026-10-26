//! Chat's answer-quality evals: questions about a made-up team's code, with
//! pass criteria checked in code.
//!
//! The fixture is two small repositories of an invented marina booking app
//! (`northwind/harbor-api`, `northwind/harbor-web`). Each case asks one of
//! the questions people ask Chat ("How is harbor-api put together?",
//! "Where are refunds handled?", "What does src/jobs/expireHolds.ts do?") and
//! passes only when the answer:
//! - used the code tools that fit (the overview, reading files, search),
//! - is grounded in what it read: it states the case's facts from the files,
//! - cites real paths of the fixture (and the ones the case needs),
//! - never hands the person work its tools could do (the answer check, run
//!   with the model itself, says so).
//!
//! `offline_*` tests run the real answer loop with a scripted model, so the
//! tools, sources, citations and the hand-back retry are checked on every
//! build. `live_chat_eval` runs the same cases against the real model; it is
//! ignored unless asked for, and needs a model key:
//!
//! ```text
//! CHAT_EVAL_API_KEY=... cargo test -p gateway live_chat_eval -- --ignored --nocapture
//! ```

use super::brain::system_prompt;
use super::chat_agent::testing::{CodeBox, Recorder};
use super::chat_agent::{self, Answered, Ask, LIMITS};
use super::chat_code::{LIST_FILES, OVERVIEW, READ_FILE, SEARCH_CODE};
use common::knowledge::answer::Registry;
use common::repo_read::{MemHost, RepoInfo};
use serde_json::json;

pub const API: &str = "northwind/harbor-api";
pub const WEB: &str = "northwind/harbor-web";

const API_FILES: &[(&str, &str)] = &[
    (
        "README.md",
        "# Harbor API\n\nThe booking service behind the Harbor marina app. Boaters book slips \
         (berths) by the night; the API holds a slip while the boater pays, confirms the booking \
         when the card payment clears, and refunds cancelled bookings.\n\n## Running it\n\n\
         `npm install`, then `npm run dev`. It needs a Postgres database (`DATABASE_URL`).\n\n\
         ## Layout\n\nRoutes live in `src/routes`, business rules in `src/services`, the \
         database in `src/db`, background jobs in `src/jobs`.\n",
    ),
    (
        "AGENTS.md",
        "# Notes for agents\n\n- Keep routes thin: validate with zod, then call a service.\n\
         - Money is in cents, always integers.\n- Every refund writes an audit row.\n",
    ),
    (
        "package.json",
        "{\n  \"name\": \"harbor-api\",\n  \"version\": \"2.3.0\",\n  \"scripts\": {\n    \
         \"dev\": \"tsx watch src/server.ts\",\n    \"test\": \"vitest run\",\n    \
         \"jobs\": \"tsx src/jobs/runner.ts\"\n  },\n  \"dependencies\": {\n    \
         \"express\": \"^4.19.2\",\n    \"zod\": \"^3.23.8\",\n    \"pg\": \"^8.12.0\",\n    \
         \"node-cron\": \"^3.0.3\"\n  }\n}\n",
    ),
    (
        "src/server.ts",
        "import express from \"express\";\nimport { routes } from \"./routes\";\n\
         import { errorHandler } from \"./middleware/errors\";\nimport { requireUser } from \
         \"./middleware/auth\";\n\nexport function createServer() {\n  const app = express();\n  \
         app.use(express.json());\n  app.get(\"/health\", (_req, res) => res.json({ ok: true }));\n  \
         app.use(\"/api\", requireUser, routes);\n  app.use(errorHandler);\n  return app;\n}\n\n\
         if (require.main === module) {\n  createServer().listen(process.env.PORT ?? 4000);\n}\n",
    ),
    (
        "src/routes/index.ts",
        "import { Router } from \"express\";\nimport { bookings } from \"./bookings\";\n\
         import { refunds } from \"./refunds\";\nimport { slips } from \"./slips\";\n\n\
         export const routes = Router();\nroutes.use(\"/bookings\", bookings);\n\
         routes.use(\"/refunds\", refunds);\nroutes.use(\"/slips\", slips);\n",
    ),
    (
        "src/routes/bookings.ts",
        "import { Router } from \"express\";\nimport { z } from \"zod\";\n\
         import { holdSlip, confirmBooking } from \"../services/bookingService\";\n\n\
         export const bookings = Router();\n\nconst Hold = z.object({ slipId: z.string(), \
         nights: z.number().int().min(1).max(30) });\n\nbookings.post(\"/\", async (req, res) => {\n  \
         const body = Hold.parse(req.body);\n  res.status(201).json(await holdSlip(req.user.id, \
         body.slipId, body.nights));\n});\n\nbookings.post(\"/:id/confirm\", async (req, res) => {\n  \
         res.json(await confirmBooking(req.params.id));\n});\n",
    ),
    (
        "src/routes/refunds.ts",
        "import { Router } from \"express\";\nimport { z } from \"zod\";\n\
         import { issueRefund } from \"../services/refundService\";\n\n\
         export const refunds = Router();\n\nconst Ask = z.object({ bookingId: z.string(), reason: \
         z.string().max(500) });\n\nrefunds.post(\"/\", async (req, res) => {\n  const body = \
         Ask.parse(req.body);\n  res.status(201).json(await issueRefund(body.bookingId, req.user.id, \
         body.reason));\n});\n",
    ),
    (
        "src/routes/slips.ts",
        "import { Router } from \"express\";\nimport { db } from \"../db/pool\";\n\n\
         export const slips = Router();\n\nslips.get(\"/\", async (_req, res) => {\n  \
         res.json((await db.query(\"select * from slips where active\")).rows);\n});\n",
    ),
    (
        "src/services/bookingService.ts",
        "import { db } from \"../db/pool\";\nimport { charge } from \"../payments/cardGateway\";\n\n\
         /** Holds a slip for 30 minutes while the boater pays. */\nexport async function \
         holdSlip(userId: string, slipId: string, nights: number) {\n  const price = await \
         nightlyPrice(slipId);\n  return db.one(\n    \"insert into bookings (user_id, slip_id, \
         nights, amount_cents, status, hold_expires_at) values ($1, $2, $3, $4, 'held', now() + \
         interval '30 minutes') returning *\",\n    [userId, slipId, nights, price * nights],\n  \
         );\n}\n\nexport async function confirmBooking(id: string) {\n  const b = await \
         db.one(\"select * from bookings where id = $1\", [id]);\n  await charge(b.user_id, \
         b.amount_cents, `booking ${id}`);\n  return db.one(\"update bookings set status = \
         'confirmed' where id = $1 returning *\", [id]);\n}\n\nasync function nightlyPrice(slipId: \
         string) {\n  return (await db.one(\"select price_cents from slips where id = $1\", \
         [slipId])).price_cents;\n}\n",
    ),
    (
        "src/services/refundService.ts",
        "import { db } from \"../db/pool\";\nimport { refund } from \"../payments/cardGateway\";\n\
         import { audit } from \"./audit\";\n\n/** Cancellations at least 48 hours before arrival \
         get the full amount back; later ones get half. */\nexport const FULL_REFUND_HOURS = 48;\n\n\
         export async function issueRefund(bookingId: string, userId: string, reason: string) {\n  \
         const b = await db.one(\"select * from bookings where id = $1 and user_id = $2\", \
         [bookingId, userId]);\n  if (b.status !== \"confirmed\") {\n    throw new \
         Error(\"Only confirmed bookings can be refunded\");\n  }\n  const hoursLeft = \
         (b.arrives_at.getTime() - Date.now()) / 3_600_000;\n  const amount = hoursLeft >= \
         FULL_REFUND_HOURS ? b.amount_cents : Math.floor(b.amount_cents / 2);\n  await \
         refund(b.payment_id, amount);\n  await db.query(\"update bookings set status = 'refunded' \
         where id = $1\", [bookingId]);\n  await audit(\"refund\", { bookingId, userId, amount, \
         reason });\n  return { bookingId, amount_cents: amount };\n}\n",
    ),
    (
        "src/services/audit.ts",
        "import { db } from \"../db/pool\";\n\nexport async function audit(action: string, data: \
         object) {\n  await db.query(\"insert into audit_log (action, data) values ($1, $2)\", \
         [action, data]);\n}\n",
    ),
    (
        "src/payments/cardGateway.ts",
        "const BASE = process.env.CARD_GATEWAY_URL!;\n\nexport async function charge(customer: \
         string, cents: number, memo: string) {\n  return post(\"/charges\", { customer, cents, memo \
         });\n}\n\nexport async function refund(paymentId: string, cents: number) {\n  return \
         post(\"/refunds\", { paymentId, cents });\n}\n\nasync function post(path: string, body: \
         object) {\n  const r = await fetch(BASE + path, { method: \"POST\", body: \
         JSON.stringify(body) });\n  if (!r.ok) throw new Error(`card gateway ${r.status}`);\n  \
         return r.json();\n}\n",
    ),
    (
        "src/jobs/runner.ts",
        "import cron from \"node-cron\";\nimport { expireHolds } from \"./expireHolds\";\n\n\
         cron.schedule(\"*/5 * * * *\", expireHolds);\n",
    ),
    (
        "src/jobs/expireHolds.ts",
        "import { db } from \"../db/pool\";\n\n/**\n * Frees slips whose hold ran out: a booking \
         stays 'held' for 30 minutes while the\n * boater pays (see holdSlip). Runs every five \
         minutes from runner.ts.\n */\nexport async function expireHolds() {\n  const { rowCount } = \
         await db.query(\n    \"update bookings set status = 'expired' where status = 'held' and \
         hold_expires_at < now()\",\n  );\n  if (rowCount) console.log(`expired ${rowCount} \
         holds`);\n}\n",
    ),
    (
        "src/db/pool.ts",
        "import { Pool } from \"pg\";\n\nconst pool = new Pool({ connectionString: \
         process.env.DATABASE_URL });\n\nexport const db = {\n  query: (sql: string, args: \
         unknown[] = []) => pool.query(sql, args),\n  one: async (sql: string, args: unknown[] = \
         []) => (await pool.query(sql, args)).rows[0],\n};\n",
    ),
    (
        "src/middleware/auth.ts",
        "export function requireUser(req, res, next) {\n  if (!req.headers.authorization) return \
         res.status(401).end();\n  req.user = { id: req.headers[\"x-user-id\"] };\n  next();\n}\n",
    ),
    (
        "src/middleware/errors.ts",
        "export function errorHandler(err, _req, res, _next) {\n  res.status(400).json({ error: \
         err.message });\n}\n",
    ),
    (
        "test/refunds.test.ts",
        "import { describe, it, expect } from \"vitest\";\nimport { FULL_REFUND_HOURS } from \
         \"../src/services/refundService\";\n\ndescribe(\"refunds\", () => {\n  it(\"gives a full \
         refund two days out\", () => {\n    expect(FULL_REFUND_HOURS).toBe(48);\n  });\n});\n",
    ),
];

const WEB_FILES: &[(&str, &str)] = &[
    (
        "README.md",
        "# Harbor web\n\nThe boater-facing Next.js site. It calls harbor-api for slips, bookings \
         and refunds.\n",
    ),
    (
        "package.json",
        "{\n  \"name\": \"harbor-web\",\n  \"dependencies\": { \"next\": \"15.0.0\", \"react\": \
         \"19.0.0\" }\n}\n",
    ),
    (
        "app/page.tsx",
        "export default function Home() {\n  return <h1>Find a slip</h1>;\n}\n",
    ),
    (
        "app/bookings/[id]/cancel.tsx",
        "export async function cancel(id: string) {\n  await fetch(`/api/refunds`, { method: \
         \"POST\", body: JSON.stringify({ bookingId: id, reason: \"changed plans\" }) });\n}\n",
    ),
];

/// The fixture: two repositories of a made-up team.
pub fn fixture() -> (MemHost, Vec<RepoInfo>) {
    let host = MemHost::default()
        .with_repo(API, "4a7c1e9d2b", API_FILES)
        .with_repo(WEB, "9e1f00ab37", WEB_FILES);
    let repos = vec![
        RepoInfo {
            role: Some("backend".into()),
            ..RepoInfo::new(API, "main")
        },
        RepoInfo {
            role: Some("web".into()),
            ..RepoInfo::new(WEB, "main")
        },
    ];
    (host, repos)
}

/// One question and what a good answer to it does.
pub struct Case {
    pub name: &'static str,
    pub question: &'static str,
    /// At least one of these tools is called.
    pub uses_any: &'static [&'static str],
    /// Every one of these files is cited.
    pub cites: &'static [&'static str],
    /// Facts from the files the answer states (any one spelling of each).
    pub facts: &'static [&'static [&'static str]],
}

pub const CASES: &[Case] = &[
    Case {
        name: "how a repository is put together",
        question: "How is harbor-api put together?",
        uses_any: &[OVERVIEW],
        cites: &["src/server.ts"],
        facts: &[
            &["src/routes", "routes/index.ts", "routes"],
            &["src/services", "services"],
            &["postgres", "src/db", "pool.ts"],
            &["expireholds", "src/jobs", "jobs"],
        ],
    },
    Case {
        name: "where something is handled",
        question: "Where are refunds handled, and how much does a boater get back?",
        uses_any: &[SEARCH_CODE, LIST_FILES, READ_FILE],
        cites: &["src/services/refundService.ts"],
        facts: &[&["issuerefund"], &["48"], &["half", "50%", "/ 2"]],
    },
    Case {
        name: "what a file does",
        question: "What does src/jobs/expireHolds.ts do?",
        uses_any: &[READ_FILE],
        cites: &["src/jobs/expireHolds.ts"],
        facts: &[
            &["30 minutes", "30-minute", "30 min"],
            &["five minutes", "5 minutes", "*/5"],
            &["expired"],
        ],
    },
];

/// Why an answer failed its case (empty when it passed).
pub fn grade(case: &Case, out: &Answered, host: &MemHost) -> Vec<String> {
    let mut why = Vec::new();
    if !case
        .uses_any
        .iter()
        .any(|t| out.calls.iter().any(|c| c == t))
    {
        why.push(format!(
            "used none of {:?} (called {:?})",
            case.uses_any, out.calls
        ));
    }
    if !out.fin.grounded {
        why.push("the answer cites nothing".into());
    }
    let real = |repo: &str, path: &str| {
        // A file, or a folder (a listing's source) that holds files.
        host.repos.get(repo).is_some_and(|(_, files)| {
            path.is_empty()
                || files.contains_key(path)
                || files.keys().any(|f| f.starts_with(&format!("{path}/")))
        })
    };
    for c in &out.fin.citations {
        if let Some(code) = &c.code {
            if !real(&code.repo, &code.path) {
                why.push(format!(
                    "cites {} in {}, which isn't there",
                    code.path, code.repo
                ));
            }
        }
    }
    for path in case.cites {
        if !out
            .fin
            .citations
            .iter()
            .any(|c| c.code.as_ref().is_some_and(|x| x.path == *path))
        {
            why.push(format!("doesn't cite {path}"));
        }
    }
    let text = out.fin.text.to_ascii_lowercase();
    for fact in case.facts {
        if !fact.iter().any(|f| text.contains(&f.to_ascii_lowercase())) {
            why.push(format!("doesn't say {}", fact.join(" / ")));
        }
    }
    why
}

/// The first user turn for a question, as Chat builds it (no knowledge).
fn opening(repos: &[RepoInfo], question: &str) -> Vec<serde_json::Value> {
    let notes = super::brain::mention_note(repos, question);
    vec![json!({"role": "user", "content": [{"type": "text", "text":
        format!("<sources>\n(no knowledge passages matched this question)</sources>\n{notes}\nQuestion: {question}")
    }]})]
}

/// Runs one case through the real answer loop.
pub async fn ask(
    model: &dyn chat_agent::Model,
    host: &MemHost,
    repos: &[RepoInfo],
    case: &Case,
) -> (Answered, Recorder) {
    let mut tb = CodeBox::new(host, repos.to_vec());
    let sink = Recorder::default();
    let mut reg = Registry::default();
    let out = chat_agent::run(
        model,
        &mut tb,
        &sink,
        &mut reg,
        Ask {
            system: system_prompt(repos, true),
            messages: opening(repos, case.question),
            question: case.question,
            limits: LIMITS,
        },
    )
    .await
    .unwrap_or_else(|_| panic!("{}: the model couldn't be reached", case.name));
    (out, sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::chat_agent::testing::{calls, id_for, last_result, say, Scripted, Step};

    /// The way a good answer goes, played by a scripted model: the loop,
    /// tools, sources and graders are real.
    fn script(case: &Case) -> Vec<Step> {
        match case.name {
            "how a repository is put together" => vec![
                Box::new(|_| calls("a", OVERVIEW, json!({"repo": "harbor-api"}))),
                Box::new(|_| {
                    calls(
                        "b",
                        READ_FILE,
                        json!({"repo": "harbor-api", "path": "src/server.ts"}),
                    )
                }),
                Box::new(|b| {
                    let r = last_result(b);
                    let s = id_for(&r, "src/server.ts").unwrap();
                    say(&format!(
                        "harbor-api is an Express service: `src/server.ts` mounts `src/routes` under /api [{s}]. \
                         Routes call `src/services`, which use Postgres through `src/db/pool.ts`, and \
                         `src/jobs` runs expireHolds every five minutes [{s}]."
                    ))
                }),
            ],
            "where something is handled" => vec![
                Box::new(|_| calls("a", SEARCH_CODE, json!({"query": "refund"}))),
                Box::new(|_| {
                    calls(
                        "b",
                        READ_FILE,
                        json!({"repo": "harbor-api", "path": "src/services/refundService.ts"}),
                    )
                }),
                Box::new(|b| {
                    let s = id_for(&last_result(b), "src/services/refundService.ts").unwrap();
                    say(&format!(
                        "Refunds are handled by `issueRefund` [{s}]. Cancelling 48 hours or more ahead returns \
                         the full amount; later, half [{s}]."
                    ))
                }),
            ],
            _ => vec![
                Box::new(|_| {
                    calls(
                        "a",
                        READ_FILE,
                        json!({"repo": "harbor-api", "path": "src/jobs/expireHolds.ts"}),
                    )
                }),
                Box::new(|b| {
                    let s = id_for(&last_result(b), "src/jobs/expireHolds.ts").unwrap();
                    say(&format!(
                        "It frees slips whose 30 minutes hold ran out, marking those bookings expired [{s}]. \
                         It runs every five minutes [{s}]."
                    ))
                }),
            ],
        }
    }

    #[tokio::test]
    async fn offline_every_case_passes_with_a_good_trajectory() {
        let (host, repos) = fixture();
        for case in CASES {
            let model = Scripted::new(script(case), vec![r#"{"hands_back": false}"#]);
            let (out, sink) = ask(&model, &host, &repos, case).await;
            let why = grade(case, &out, &host);
            assert!(why.is_empty(), "{}: {why:?}\n{}", case.name, out.fin.text);
            // Each source is a real file, named by its path, linked at its commit.
            for c in &out.fin.citations {
                let code = c.code.as_ref().unwrap();
                assert_eq!(c.title, code.path);
                assert!(c
                    .url
                    .as_deref()
                    .unwrap()
                    .contains(&format!("/blob/{}/", code.commit)));
                assert!(!c.title.contains("Code graph"));
            }
            assert!(sink.kinds().contains(&"tool_result".to_string()));
        }
    }

    #[tokio::test]
    async fn offline_a_bad_trajectory_fails_the_graders() {
        // Guesses without reading, cites a file that isn't there.
        let (host, repos) = fixture();
        let case = &CASES[1];
        let model = Scripted::new(
            vec![Box::new(|_| {
                say("Refunds are probably in a payments module [S1].")
            })],
            vec![r#"{"hands_back": false}"#],
        );
        let (out, _) = ask(&model, &host, &repos, case).await;
        let why = grade(case, &out, &host);
        assert!(why.iter().any(|w| w.contains("used none")), "{why:?}");
        assert!(why.iter().any(|w| w.contains("doesn't cite")), "{why:?}");
    }

    #[tokio::test]
    async fn offline_handing_work_back_is_caught_and_answered_with_the_tools() {
        // The screenshot's answer: no definition found, "open those files yourself".
        let (host, repos) = fixture();
        let case = &CASES[0];
        let mut steps: Vec<Step> = vec![Box::new(|_| {
            say("I couldn't find how harbor-api is built. Open the layout files yourself, or tell me its owner/name.")
        })];
        steps.extend(script(case));
        let model = Scripted::new(
            steps,
            vec![
                r#"{"hands_back": true, "what": "open files and name the repository"}"#,
                r#"{"hands_back": false}"#,
            ],
        );
        let (out, sink) = ask(&model, &host, &repos, case).await;
        assert!(out.rechecked);
        assert!(grade(case, &out, &host).is_empty(), "{}", out.fin.text);
        assert!(!out.fin.text.contains("yourself"));
        assert!(sink.kinds().contains(&"step".to_string()));
    }

    #[test]
    fn the_question_naming_a_repository_says_so_to_the_model() {
        let (_, repos) = fixture();
        let m = opening(&repos, "How is harbor-api put together?");
        let text = m[0]["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("names the team's repository northwind/harbor-api"),
            "{text}"
        );
        let p = system_prompt(&repos, true);
        assert!(p.contains("northwind/harbor-api (main), the backend repository"));
        assert!(p.contains("Never hand work back"));
    }

    /// The same cases against the real model. Ignored unless asked for.
    #[tokio::test]
    #[ignore = "calls the real model: set CHAT_EVAL_API_KEY and run with --ignored"]
    async fn live_chat_eval() {
        let key = std::env::var("CHAT_EVAL_API_KEY").expect("set CHAT_EVAL_API_KEY");
        let model_id =
            std::env::var("CHAT_EVAL_MODEL").unwrap_or_else(|_| "claude-sonnet-5-5".into());
        let check_id = std::env::var("CHAT_EVAL_CHECK_MODEL").unwrap_or_else(|_| model_id.clone());
        let (meter, recorded) = common::usage::UsageMeter::memory("TEAM#eval");
        let model = chat_agent::ApiModel {
            http: reqwest::Client::new(),
            api_key: key,
            meter,
            model_id,
            check_model_id: check_id,
            kind_of: |_| "code",
        };
        let (host, repos) = fixture();
        let mut failed = Vec::new();
        for case in CASES {
            let (out, _) = ask(&model, &host, &repos, case).await;
            let mut why = grade(case, &out, &host);
            // The answer check, run once more on the final answer, must pass it.
            let tools = crate::routes::chat_code::defs(&repos);
            let verdict = chat_agent::Model::ask(
                &model,
                chat_agent::check_body(
                    chat_agent::Model::check_id(&model),
                    case.question,
                    &out.fin.text,
                    &tools,
                ),
            )
            .await
            .and_then(|r| chat_agent::read_check(&r));
            if let Some(what) = verdict {
                why.push(format!("hands work back: {what}"));
            }
            println!(
                "\n=== {} ===\nQ: {}\ncalls: {:?}\nrechecked: {}\ncited: {:?}\nA: {}\nverdict: {}",
                case.name,
                case.question,
                out.calls,
                out.rechecked,
                out.fin
                    .citations
                    .iter()
                    .map(|c| format!("{} ({})", c.title, c.loc))
                    .collect::<Vec<_>>(),
                out.fin.text,
                if why.is_empty() {
                    "PASS".to_string()
                } else {
                    format!("FAIL {why:?}")
                }
            );
            if !why.is_empty() {
                failed.push((case.name, why));
            }
        }
        let calls = recorded.lock().unwrap().len();
        println!("\nmetered model calls: {calls}");
        assert!(calls > 0, "every model call is metered");
        assert!(failed.is_empty(), "{failed:?}");
    }
}
