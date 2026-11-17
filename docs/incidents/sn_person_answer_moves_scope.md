# sn. `sn_person_answer_moves_scope`

**What happened:** A plan left a page type out of scope, decided with a default. A reviewer answered on the pull request that those pages should cover both rows. The run replied "Understood", then reverted the work on those pages as out of scope. The scope ledger was seeded only from the ticket's acceptance criteria and the plan, and a person's comment counted only when the coder itself wrote a `Scope change:` line quoting it.

**The guarantee:** What people say decides scope as they say it. When the ledger is seeded, and on every round's comments, an out-of-scope entry that a person's answer or comment asks for moves into scope and records who asked. From then on, a diff that takes that work out is a breach that goes back to be put back. A comment that keeps the item out moves nothing.

**Where it's enforced:** `common::scope_ledger::Ledger::take_people`, `scope_ledger::breaches` (reverted items), `passes::scope_ledger::{people, load_or_seed, held, load_with_people}`.
