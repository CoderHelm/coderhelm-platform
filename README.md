# CoderHelm

CoderHelm is an AI-powered coding agent platform that automates issue triage, implementation planning, code generation, and pull request workflows through deep integrations with GitHub and Jira.

## Architecture Overview

| Component | Description |
|-----------|-------------|
| `services/gateway` | Rust HTTP API handling authentication, webhooks, and request routing. |
| `services/worker` | Rust background worker that runs AI agent passes (plan, implement, review, etc.). |
| `infra/` | AWS CDK (TypeScript) stacks defining all cloud infrastructure. |
| `coderhelm-jira/` | Atlassian Forge app providing the Jira integration. |

## Getting Started

See [SETUP.md](SETUP.md) for prerequisites, environment variables, and deployment instructions.

## Docs

- Jira integration quickstart: [docs/jira-integration.md](docs/jira-integration.md)

## Contributing

### Branch Naming

Use the following conventions for branch names:

- `feat/<short-description>` — new features
- `fix/<short-description>` — bug fixes
- `chore/<short-description>` — maintenance tasks
- `docs/<short-description>` — documentation changes

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```
type: description
```

Allowed types: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `ci`.

> Include `CI:DEPLOY_PROD` in a commit message pushed to `main` to trigger a production deploy.

### Running Tests & Linting

**Per-crate commands (services/gateway):**

```bash
cd services/gateway
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

**Per-crate commands (services/worker):**

```bash
cd services/worker
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

**Workspace-level convenience commands:**

```bash
cd services && cargo test --workspace
cd services && cargo clippy --workspace -- -D warnings
cd services && cargo fmt --all --check
```

**CDK / infra validation:**

```bash
cd infra && npm ci && npx cdk synth --quiet
```

> **Note:** The project uses a `rustfmt.toml` with `max_width = 100` and `edition = "2021"`. Make sure your editor respects these settings so `cargo fmt --check` passes.

### Pull Request Process

- Target branch is `main`.
- All CI checks must pass before merge.
- PRs require review before merge.
- Keep PRs focused on a single concern.
