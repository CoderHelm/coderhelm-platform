# Working on CoderHelm

Notes for people and coding agents (CoderHelm included) changing this repository.

## Principles

CoderHelm is a coding agent. It finishes the work; it never does its best and hands the code back.

- **Own the outcome.** A run finishes the task, verifies it (builds, tests, the checks the repository has) and fixes what breaks. When it is stuck it changes approach: reads more, traces the cause, tries another way, makes the change smaller.
- **Defer, don't drop.** Only an item that is not core to the task may be deferred, and it is named with the reason.
- **A person is asked only for what only a person has:** a credential, access CoderHelm doesn't have, a product decision the ticket leaves open, a ticket that reads two ways, an approval the team requires (the list is `services/common/src/handback.rs`). The ask is one specific question.
- **Words shown to people say what CoderHelm did, what it deferred and why, and what it does next.** Never "please review", "complete manually", "re-trigger", "a person should", "handed off for human review".
- **Prompts carry the same stance.** Every model that writes or fixes code gets `passes::ownership::coder_stance()`; a prompt never offers the model a way to stop or hand back outside the human-only list.

`passes::ownership` holds the stance, the hand-off phrases, and a test that fails when a prompt or message in the worker uses one outside its small allowlist. Add to the allowlist only for a human-only case, with a comment saying which.

## Project

- Rust workspace at `services/Cargo.toml`: `gateway` (API server), `worker` (pipeline), `common`.
- Before a commit: `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.
- No `Co-authored-by` trailers in commit messages.
