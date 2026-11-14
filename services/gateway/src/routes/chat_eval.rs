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
//!
//! The work cases ([`WORK_CASES`]) ask about the team's own work: "what's
//! the latest on <a ticket's title>", "what's running now", "is <CVE> fixed
//! in <repo>", "why did the run for <key> stop", "which infra findings are
//! open", and a ticket of another team the asker can't see. They run the
//! real loop and the real lookups ([`WorkBox`]: the runs, tickets, pull
//! requests and findings code of `chat_work_*`) over an in-memory store that
//! holds two teams, and pass only when the answer looked where the answer
//! lives, cites those records, states their facts and dates, and says
//! nothing of the other team. `live_work_eval` runs them against the model.

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
            must_look: None,
        },
    )
    .await
    .unwrap_or_else(|_| panic!("{}: the model couldn't be reached", case.name));
    (out, sink)
}

// ── The team's work ────────────────────────────────────────────────────────

use super::chat_agent::{Called, Sink, Toolbox};
use super::chat_work::{self as work, Found};
use super::chat_work_runs as runs;
use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::Value;

/// The asker's team, and another one whose records must never show.
pub const TEAM: &str = "TEAM#northwind";
pub const OTHER_TEAM: &str = "TEAM#elsewhere";
/// The ticket the report's question is about, by its title.
pub const TITLE: &str = "Embed the booking portal on the signup pages";

fn run_item(
    team: &str,
    id: &str,
    status: &str,
    ticket: &str,
    title: &str,
    extra: &[(&str, &str)],
) -> (String, runs::Item) {
    let mut i: runs::Item = std::collections::HashMap::from([
        ("run_id".to_string(), AttributeValue::S(id.into())),
        ("status".to_string(), AttributeValue::S(status.into())),
        (
            "ticket_source".to_string(),
            AttributeValue::S("jira".into()),
        ),
        ("ticket_id".to_string(), AttributeValue::S(ticket.into())),
        ("title".to_string(), AttributeValue::S(title.into())),
        ("repo".to_string(), AttributeValue::S(WEB.into())),
    ]);
    for (k, v) in extra {
        i.insert(k.to_string(), AttributeValue::S(v.to_string()));
    }
    (team.to_string(), i)
}

/// Two teams' runs, tickets, pull requests and findings.
pub struct WorkStore {
    pub runs: Vec<(String, runs::Item)>,
    pub issues: Vec<(String, Value)>,
    pub prs: Vec<(String, Value)>,
    pub security: Vec<(String, Value)>,
    pub infra: Vec<(String, Value)>,
}

pub fn work_store() -> WorkStore {
    WorkStore {
        runs: vec![
            run_item(TEAM, "01JC0000000000000000000005", "running", "HAR-31", "Add a waitlist to full slips",
                &[("current_pass", "implement"), ("created_at", "2026-11-14T09:00:00Z"), ("updated_at", "2026-11-14T09:20:00Z")]),
            run_item(TEAM, "01JC0000000000000000000004", "awaiting_ci", "HAR-27", "Embed the booking portal on the signup pages",
                &[("current_pass", "ci_fix"), ("created_at", "2026-11-11T15:00:00Z"), ("updated_at", "2026-11-12T10:05:00Z"),
                  ("pr_url", "https://github.com/northwind/harbor-web/pull/88")]),
            run_item(TEAM, "01JC0000000000000000000003", "failed", "HAR-12", "Refund the deposit on early cancellations",
                &[("created_at", "2026-11-09T08:00:00Z"), ("updated_at", "2026-11-09T08:40:00Z"),
                  ("error_message", "The tests failed twice: refundService.test.ts expects 48 hours and the change used 24")]),
            run_item(OTHER_TEAM, "01JC0000000000000000000002", "running", "OTH-9", "Move the secret billing export",
                &[("created_at", "2026-11-14T08:00:00Z")]),
        ],
        issues: vec![
            (TEAM.into(), json!({"key": "HAR-27", "summary": TITLE, "status": "In Review", "type": "Story",
                "assignee": "Dana Ruiz", "priority": "High", "updated": "2026-11-12T10:00:00.000+0000",
                "comments": [{"author": "Dana Ruiz", "created": "2026-11-12T09:58:00.000+0000", "text": "PR is up, waiting on CI."}],
                "links": [{"title": "PR #88", "url": "https://github.com/northwind/harbor-web/pull/88"}]})),
            (TEAM.into(), json!({"key": "HAR-12", "summary": "Refund the deposit on early cancellations", "status": "In Progress",
                "assignee": "", "updated": "2026-11-09T08:41:00.000+0000"})),
            (OTHER_TEAM.into(), json!({"key": "OTH-9", "summary": "Move the secret billing export", "status": "To Do",
                "updated": "2026-11-14T08:00:00.000+0000"})),
        ],
        prs: vec![
            (TEAM.into(), json!({"number": 88, "title": "HAR-27: Embed the booking portal on the signup pages", "state": "open",
                "draft": false, "user": {"login": "coderhelm"}, "head": {"ref": "coderhelm/har-27", "sha": "abc"},
                "created_at": "2026-11-11T16:00:00Z", "updated_at": "2026-11-12T10:04:00Z",
                "html_url": "https://github.com/northwind/harbor-web/pull/88"})),
        ],
        security: vec![
            (TEAM.into(), json!({"id": "sf1", "repo": WEB, "package": "next", "version": "15.0.0", "fixed_version": "15.0.4",
                "advisory": {"ids": ["CVE-2026-1234", "GHSA-ab12-cd34-ef56"], "primary": "CVE-2026-1234", "title": "Cache poisoning"},
                "status": "fixed", "updated_at": "2026-11-08T12:00:00Z",
                "pr": {"number": 80, "url": "https://github.com/northwind/harbor-web/pull/80", "state": "merged"}})),
            (TEAM.into(), json!({"id": "sf2", "repo": API, "package": "express", "version": "4.19.2",
                "advisory": {"ids": ["CVE-2026-5678"], "primary": "CVE-2026-5678", "title": "Open redirect"},
                "status": "open", "updated_at": "2026-11-10T12:00:00Z"})),
        ],
        infra: vec![
            (TEAM.into(), json!({"id": "if1", "title": "Load balancer allows TLS 1.0", "severity": "high", "status": "open", "updated_at": "2026-11-13T00:00:00Z"})),
            (TEAM.into(), json!({"id": "if2", "title": "Database snapshot is public", "severity": "critical", "status": "pr_open", "updated_at": "2026-11-12T00:00:00Z"})),
            (TEAM.into(), json!({"id": "if3", "title": "Old container image", "severity": "low", "status": "fixed", "updated_at": "2026-11-01T00:00:00Z"})),
            (OTHER_TEAM.into(), json!({"id": "if9", "title": "Secret bucket open", "severity": "critical", "status": "open", "updated_at": "2026-11-14T00:00:00Z"})),
        ],
    }
}

/// Chat's work tools over the store, as one team's asker with what they may
/// read: the same matching, records and citations as production.
pub struct WorkBox<'a> {
    pub store: &'a WorkStore,
    pub team: String,
    pub readable: Vec<&'static str>,
    pub defs: Vec<Value>,
}

impl<'a> WorkBox<'a> {
    pub fn new(store: &'a WorkStore, team: &str, readable: Vec<&'static str>) -> Self {
        let defs = work::defs(&readable, &[API.to_string(), WEB.to_string()], true);
        WorkBox {
            store,
            team: team.into(),
            readable,
            defs,
        }
    }

    fn mine<T: Clone>(&self, rows: &[(String, T)]) -> Vec<T> {
        rows.iter()
            .filter(|(t, _)| *t == self.team)
            .map(|(_, r)| r.clone())
            .collect()
    }

    fn find(&self, name: &str, input: &Value) -> Result<(Vec<Found>, &'static str), String> {
        let kind_ok = |k: &str| work::may(&self.readable, k);
        let runs_of = |ask: &runs::RunAsk| -> Vec<Found> {
            let mut items = self.mine(&self.store.runs);
            items.sort_by(|a, b| {
                let id = |i: &runs::Item| {
                    i.get("run_id")
                        .and_then(|v| v.as_s().ok())
                        .cloned()
                        .unwrap_or_default()
                };
                id(b).cmp(&id(a))
            });
            runs::best(&items, ask, runs::MAX_RESULTS)
                .into_iter()
                .map(runs::record)
                .collect()
        };
        match name {
            work::RUNS if kind_ok("runs") => Ok((
                runs_of(&runs::RunAsk::from_input(input, chrono::Utc::now())),
                "runs",
            )),
            work::TICKETS if kind_ok("tickets") => {
                use super::chat_work_tickets as t;
                let key = input["key"].as_str().map(|k| k.to_ascii_uppercase());
                let words = work::words_of(input["query"].as_str().unwrap_or(""));
                let mut ask = runs::RunAsk::default();
                match &key {
                    Some(k) => ask.ticket = Some(k.to_lowercase()),
                    None => ask.words = words.clone(),
                }
                let found_runs = runs_of(&ask);
                let keys = t::keys_of(&found_runs);
                let issues: Vec<Value> = self
                    .mine(&self.store.issues)
                    .into_iter()
                    .filter(|i| {
                        let k = i["key"].as_str().unwrap_or("");
                        let title = i["summary"].as_str().unwrap_or("").to_lowercase();
                        key.as_deref() == Some(k)
                            || keys.iter().any(|x| x == k)
                            || (!words.is_empty()
                                && words.iter().filter(|w| title.contains(w.as_str())).count() * 2
                                    >= words.len())
                    })
                    .collect();
                let site = "https://northwind.atlassian.net";
                Ok((
                    issues
                        .iter()
                        .map(|i| t::from_jira(i, site, &found_runs))
                        .collect(),
                    "tickets",
                ))
            }
            work::PULL_REQUESTS if kind_ok("pull_requests") => {
                let ticket = input["ticket"].as_str().unwrap_or("").to_lowercase();
                let words = work::words_of(input["query"].as_str().unwrap_or(""));
                let number = input["number"].as_u64();
                let prs = self.mine(&self.store.prs).into_iter().filter(|p| {
                    let title = p["title"].as_str().unwrap_or("").to_lowercase();
                    number.is_some_and(|n| p["number"].as_u64() == Some(n))
                        || (!ticket.is_empty() && title.contains(&ticket))
                        || (!words.is_empty()
                            && words.iter().filter(|w| title.contains(w.as_str())).count() * 2
                                >= words.len())
                });
                let checks = json!({"check_runs": [{"name": "build", "status": "completed", "conclusion": "success"},
                    {"name": "e2e", "status": "in_progress"}]});
                let reviews =
                    json!([{"user": {"login": "coderhelm-reviewer"}, "state": "APPROVED"}]);
                Ok((
                    prs.map(|p| {
                        super::chat_work_prs::record(WEB, &p, Some(&checks), Some(&reviews))
                    })
                    .collect(),
                    "pull requests",
                ))
            }
            work::FINDINGS => {
                let k = input["kind"].as_str().unwrap_or("");
                if !kind_ok(k) {
                    return Err(
                        "Your role can't see these in the dashboard, so Chat doesn't read them."
                            .into(),
                    );
                }
                let ask = super::chat_work_records::RecordAsk::from_input(input, "open");
                let rows = match k {
                    "security" => self.mine(&self.store.security),
                    "infra" => self.mine(&self.store.infra),
                    _ => Vec::new(),
                };
                Ok((super::chat_work_records::pick(k, &rows, &ask), "findings"))
            }
            _ => {
                Err("Your role can't see these in the dashboard, so Chat doesn't read them.".into())
            }
        }
    }
}

#[async_trait::async_trait]
impl Toolbox for WorkBox<'_> {
    fn defs(&self) -> &[Value] {
        &self.defs
    }
    async fn call(
        &mut self,
        reg: &mut Registry,
        _sink: &dyn Sink,
        name: &str,
        input: &Value,
    ) -> Called {
        let (text, summary, is_error) = match self.find(name, input) {
            Ok((found, noun)) => work::register(reg, found, noun, ""),
            Err(e) => (e, "Not allowed".into(), true),
        };
        Called::Done {
            text,
            label: work::label(name, input),
            summary,
            is_error,
            kind: "work".into(),
        }
    }
}

/// One question about the team's work and what a good answer does.
pub struct WorkCase {
    pub name: &'static str,
    pub question: &'static str,
    /// Every one of these tools is called.
    pub uses_all: &'static [&'static str],
    /// The answer cites a source of each of these kinds.
    pub cites: &'static [&'static str],
    /// Facts the answer states (any one spelling of each).
    pub facts: &'static [&'static [&'static str]],
    /// Words that must never appear (another team's records).
    pub never: &'static [&'static str],
}

pub const WORK_CASES: &[WorkCase] = &[
    WorkCase {
        name: "the latest on a ticket by its title",
        question:
            "What's the latest on \u{201c}Embed the booking portal on the signup pages\u{201d}?",
        uses_all: &[work::TICKETS, work::PULL_REQUESTS, work::RUNS],
        cites: &["ticket", "pull_request", "run"],
        facts: &[
            &["HAR-27"],
            &["in review"],
            &["#88", "pull request 88", "pr 88"],
            &["2026-11-12", "nov 12", "november 12"],
            &["checks", "ci"],
        ],
        never: &["OTH-9", "billing export"],
    },
    WorkCase {
        name: "what is running now",
        question: "What's running now?",
        uses_all: &[work::RUNS],
        cites: &["run"],
        facts: &[&["waitlist"], &["HAR-31"]],
        never: &["OTH-9", "billing export"],
    },
    WorkCase {
        name: "is a CVE fixed in a repository",
        question: "Is CVE-2026-1234 fixed in harbor-web?",
        uses_all: &[work::FINDINGS],
        cites: &["finding"],
        facts: &[&["fixed"], &["15.0.4", "#80", "pull/80"]],
        never: &[],
    },
    WorkCase {
        name: "why a run stopped",
        question: "Why did the run for HAR-12 stop?",
        uses_all: &[work::RUNS],
        cites: &["run"],
        facts: &[&["tests failed"], &["48"]],
        never: &[],
    },
    WorkCase {
        name: "which infra findings are open",
        question: "Which infra findings are open?",
        uses_all: &[work::FINDINGS],
        cites: &["finding"],
        facts: &[&["TLS 1.0"], &["snapshot"]],
        never: &["Old container image", "Secret bucket"],
    },
    WorkCase {
        name: "a ticket the asker can't see",
        question: "What's the latest on OTH-9?",
        uses_all: &[work::TICKETS],
        cites: &[],
        facts: &[],
        never: &["billing export", "To Do"],
    },
];

/// Why an answer failed its work case (empty when it passed).
pub fn grade_work(case: &WorkCase, out: &Answered) -> Vec<String> {
    let mut why = Vec::new();
    for t in case.uses_all {
        if !out.calls.iter().any(|c| c == t) {
            why.push(format!("didn't call {t} (called {:?})", out.calls));
        }
    }
    for k in case.cites {
        if !out.fin.citations.iter().any(|c| c.kind == *k) {
            why.push(format!("cites no {k}"));
        }
    }
    if !case.cites.is_empty() && !out.fin.grounded {
        why.push("the answer cites nothing".into());
    }
    let text = out.fin.text.to_lowercase();
    for fact in case.facts {
        if !fact.iter().any(|f| text.contains(&f.to_lowercase())) {
            why.push(format!("doesn't say {}", fact.join(" / ")));
        }
    }
    let everything = format!(
        "{} {}",
        out.fin.text,
        out.fin
            .citations
            .iter()
            .map(|c| format!("{} {}", c.title, c.passage))
            .collect::<Vec<_>>()
            .join(" ")
    )
    .to_lowercase();
    for w in case.never {
        if everything.contains(&w.to_lowercase()) {
            why.push(format!("says {w}, another team's"));
        }
    }
    why
}

/// Runs one work case through the real answer loop, as Chat builds it
/// (hint note, tools put first, the must-look check).
pub async fn ask_work(
    model: &dyn chat_agent::Model,
    store: &WorkStore,
    readable: Vec<&'static str>,
    case: &WorkCase,
) -> (Answered, Recorder) {
    let mut tb = WorkBox::new(store, TEAM, readable);
    let hints = super::chat_hints::read(case.question);
    work::put_first(&mut tb.defs, &hints);
    let offered: Vec<&str> = tb.defs.iter().filter_map(|t| t["name"].as_str()).collect();
    let notes = work::hint_note(&hints, &offered);
    let must_look = hints.about_work().then(|| chat_agent::MustLook {
        tools: work::STATUS_TOOLS
            .iter()
            .filter(|t| offered.contains(t))
            .map(|t| t.to_string())
            .collect(),
        what: "the team's tickets, pull requests and Coderhelm's runs".into(),
    });
    let sink = Recorder::default();
    let mut reg = Registry::default();
    let (_, repos) = fixture();
    let out = chat_agent::run(
        model,
        &mut tb,
        &sink,
        &mut reg,
        Ask {
            system: system_prompt(&repos, true),
            messages: vec![json!({"role": "user", "content": [{"type": "text", "text":
                format!("<sources>\n(no knowledge passages matched this question)</sources>\n{notes}\nQuestion: {}", case.question)}]})],
            question: case.question,
            limits: LIMITS,
            must_look,
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
    use crate::routes::chat_agent::Toolbox;

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

    /// The source ids of `kind` in every tool result sent so far.
    fn ids_of(body: &serde_json::Value, kind: &str) -> Vec<String> {
        let all = body["messages"].to_string().replace("\\\"", "\"");
        let needle = format!("kind=\"{kind}\"");
        let mut out: Vec<String> = Vec::new();
        for part in all.split("<source id=\"").skip(1) {
            let id = part.split('"').next().unwrap_or("").to_string();
            if part.contains(&needle) && part.find(&needle) < part.find('>') && !out.contains(&id) {
                out.push(id);
            }
        }
        out
    }

    fn all_kinds() -> Vec<&'static str> {
        work::KINDS.iter().map(|k| k.key).collect()
    }

    /// A good answer's way through each work case, played by a scripted
    /// model over the real lookups.
    fn work_script(case: &WorkCase) -> Vec<Step> {
        match case.name {
            "the latest on a ticket by its title" => vec![
                Box::new(|_| calls("a", work::TICKETS, json!({"query": TITLE}))),
                Box::new(|_| calls("b", work::PULL_REQUESTS, json!({"ticket": "HAR-27"}))),
                Box::new(|_| calls("c", work::RUNS, json!({"ticket": "HAR-27"}))),
                Box::new(|b| {
                    let (t, p, r) = (
                        &ids_of(b, "ticket")[0],
                        &ids_of(b, "pull_request")[0],
                        &ids_of(b, "run")[0],
                    );
                    say(&format!(
                        "HAR-27 is In Review, assigned to Dana Ruiz, last changed 2026-11-12 [{t}]. Its pull request #88 is open: \
                         the build passed and the e2e checks are still running, with an approval [{p}]. Coderhelm's run on it is \
                         waiting on the pull request's CI checks, last updated 2026-11-12 [{r}]."
                    ))
                }),
            ],
            "what is running now" => vec![
                Box::new(|_| calls("a", work::RUNS, json!({"status": "active"}))),
                Box::new(|b| {
                    let r = ids_of(b, "run");
                    say(&format!(
                        "Two runs are active: HAR-31 \"Add a waitlist to full slips\" is working now, in the implement step [{}]; \
                         HAR-27 is waiting on its pull request's checks [{}].",
                        r[0], r[1]
                    ))
                }),
            ],
            "is a CVE fixed in a repository" => vec![
                Box::new(|_| {
                    calls(
                        "a",
                        work::FINDINGS,
                        json!({"kind": "security", "query": "CVE-2026-1234", "repo": "harbor-web", "state": "any"}),
                    )
                }),
                Box::new(|b| {
                    let f = &ids_of(b, "finding")[0];
                    say(&format!("Yes. CVE-2026-1234 in next is fixed in harbor-web: it moved to 15.0.4 in pull request #80, merged [{f}]."))
                }),
            ],
            "why a run stopped" => vec![
                Box::new(|_| calls("a", work::RUNS, json!({"ticket": "HAR-12"}))),
                Box::new(|b| {
                    let r = &ids_of(b, "run")[0];
                    say(&format!(
                        "It failed on 2026-11-09: the tests failed twice, because refundService.test.ts expects 48 hours and the \
                         change used 24 [{r}]."
                    ))
                }),
            ],
            "which infra findings are open" => vec![
                Box::new(|_| calls("a", work::FINDINGS, json!({"kind": "infra"}))),
                Box::new(|b| {
                    let f = ids_of(b, "finding");
                    say(&format!(
                        "Two are open: the load balancer allows TLS 1.0 (high) [{}], and a database snapshot is public \
                         (critical), with a fix pull request open [{}].",
                        f[0], f[1]
                    ))
                }),
            ],
            _ => vec![
                Box::new(|_| calls("a", work::TICKETS, json!({"key": "OTH-9"}))),
                Box::new(|_| calls("b", work::RUNS, json!({"ticket": "OTH-9"}))),
                Box::new(|_| {
                    say("I looked for OTH-9 in the team's tickets and Coderhelm's runs and found no ticket by that key that you can see.")
                }),
            ],
        }
    }

    #[tokio::test]
    async fn offline_every_work_case_passes_with_a_good_trajectory() {
        let store = work_store();
        for case in WORK_CASES {
            let model = Scripted::new(work_script(case), vec![r#"{"hands_back": false}"#]);
            let (out, _) = ask_work(&model, &store, all_kinds(), case).await;
            let why = grade_work(case, &out);
            assert!(why.is_empty(), "{}: {why:?}\n{}", case.name, out.fin.text);
            // Every record cited links somewhere: its page or the original.
            for c in &out.fin.citations {
                assert!(
                    c.href.is_some() || c.url.is_some(),
                    "{}: {} has no link",
                    case.name,
                    c.title
                );
            }
        }
    }

    /// The report: Chat searched the code and docs, said it found nothing,
    /// and never looked at the ticket, its pull requests or the runs.
    #[tokio::test]
    async fn offline_the_reported_answer_fails_before_and_is_sent_back_after() {
        let store = work_store();
        let case = &WORK_CASES[0];
        let reported = "I couldn't find anything on that in the team's docs, code or website content, so I can't give you a status.";
        // Before: no must-look check (the loop as it was) keeps that answer.
        let model = Scripted::new(
            vec![Box::new(move |_| say(reported))],
            vec![r#"{"hands_back": false}"#],
        );
        let mut tb = WorkBox::new(&store, TEAM, all_kinds());
        let mut reg = Registry::default();
        let before = chat_agent::run(
            &model,
            &mut tb,
            &Recorder::default(),
            &mut reg,
            Ask {
                system: "test".into(),
                messages: vec![
                    json!({"role": "user", "content": [{"type": "text", "text": case.question}]}),
                ],
                question: case.question,
                limits: LIMITS,
                must_look: None,
            },
        )
        .await
        .unwrap();
        let why = grade_work(case, &before);
        assert!(
            why.iter().any(|w| w.contains("didn't call work_tickets")),
            "{why:?}"
        );
        assert!(!before.rechecked);
        // After: the same first answer is sent back in code (the model's
        // check isn't even asked), and the answer looks where it lives.
        let mut steps: Vec<Step> = vec![Box::new(move |_| say(reported))];
        steps.extend(work_script(case));
        let model = Scripted::new(steps, vec![r#"{"hands_back": false}"#]);
        let (after, sink) = ask_work(&model, &store, all_kinds(), case).await;
        assert!(after.rechecked);
        assert!(
            grade_work(case, &after).is_empty(),
            "{:?}\n{}",
            grade_work(case, &after),
            after.fin.text
        );
        let bodies = model.bodies.lock().unwrap();
        let nudged = bodies[1]["messages"].to_string();
        assert!(
            nudged.contains("didn't look where the answer would")
                && nudged.contains("work_tickets"),
            "{nudged}"
        );
        // The first turn already pointed at the ticket.
        let first = bodies[0]["messages"].to_string();
        assert!(first.contains("work_tickets with query"), "{first}");
        assert_eq!(bodies[0]["tools"][0]["name"], work::TICKETS);
        assert!(sink.kinds().contains(&"step".to_string()));
    }

    #[tokio::test]
    async fn offline_another_teams_records_never_show_and_a_role_reads_only_what_it_sees() {
        let store = work_store();
        // Another team's running run and open infra finding stay out.
        let model = Scripted::new(
            vec![
                Box::new(|_| calls("a", work::RUNS, json!({"status": "active"}))),
                Box::new(|_| say("ok")),
            ],
            vec![],
        );
        let (_, sink) = ask_work(&model, &store, all_kinds(), &WORK_CASES[1]).await;
        let seen = format!("{:?}", sink.0.lock().unwrap());
        assert!(seen.contains("HAR-31") && !seen.contains("OTH-9"), "{seen}");
        // A role that can't see infrastructure findings isn't offered them,
        // and a call anyway reads nothing.
        let mut tb = WorkBox::new(&store, TEAM, vec!["runs", "tickets"]);
        assert!(!tb.defs.iter().any(|t| t["name"] == work::FINDINGS));
        let mut reg = Registry::default();
        let r = tb
            .call(
                &mut reg,
                &Recorder::default(),
                work::FINDINGS,
                &json!({"kind": "infra"}),
            )
            .await;
        match r {
            Called::Done { is_error, text, .. } => {
                assert!(is_error && text.contains("can't see"), "{text}")
            }
            Called::Stop(_) => panic!("stopped"),
        }
        assert!(reg.is_empty());
    }

    /// The work cases against the real model. Ignored unless asked for.
    #[tokio::test]
    #[ignore = "calls the real model: set CHAT_EVAL_API_KEY and run with --ignored"]
    async fn live_work_eval() {
        let key = std::env::var("CHAT_EVAL_API_KEY").expect("set CHAT_EVAL_API_KEY");
        let model_id =
            std::env::var("CHAT_EVAL_MODEL").unwrap_or_else(|_| "claude-sonnet-5-5".into());
        let (meter, _) = common::usage::UsageMeter::memory("TEAM#eval");
        let model = chat_agent::ApiModel {
            http: reqwest::Client::new(),
            api_key: key,
            meter,
            check_model_id: model_id.clone(),
            model_id,
            kind_of: crate::routes::brain::kind_of_tool,
        };
        let store = work_store();
        let mut failed = Vec::new();
        for case in WORK_CASES {
            let (out, _) = ask_work(&model, &store, all_kinds(), case).await;
            let why = grade_work(case, &out);
            println!(
                "\n=== {} ===\nQ: {}\ncalls: {:?}\nrechecked: {}\nA: {}\nverdict: {}",
                case.name,
                case.question,
                out.calls,
                out.rechecked,
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
        assert!(failed.is_empty(), "{failed:?}");
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
