# Proposal: Write README for coderhelm-platform

## Problem Statement

The `coderhelm-platform` repository currently has a minimal `README.md` that contains only a single bullet point linking to `docs/jira-integration.md`. This leaves the repository without a meaningful entry point for developers, contributors, or evaluators trying to understand what the platform is, how it is architected, and how to get started. The existing `SETUP.md` provides operational depth but is not surfaced in the README, and there is no high-level narrative connecting the many moving parts of the system.

## Proposed Approach

Replace the stub `README.md` with a comprehensive, well-structured document that:

1. **Introduces the product** — what CoderHelm is and what problem it solves
2. **Describes the architecture** — the major subsystems (Gateway, Worker, Infra, Jira app, dashboard) and how they relate
3. **Explains the agent pipeline** — the five-pass orchestration flow (Triage → Plan → Implement → Review → PR)
4. **Covers integrations** — GitHub App setup and Jira webhook integration
5. **Documents the repository layout** — a directory tree with short descriptions of each component
6. **Links to existing docs** — `SETUP.md`, `docs/jira-integration.md`, and the CI/deploy workflows
7. **Provides quick-start guidance** — prerequisites, environment variables, and the one-liner deploy command already documented in `SETUP.md`

The README should serve as the single authoritative landing page for the repository — orienting a new reader in under five minutes while pointing them to deeper resources for each subsystem.

## Scope Boundaries

**In scope:**
- Rewriting `README.md` at the repository root
- Covering all major components visible in the repository tree
- Documenting the agent passes as discovered in `services/worker/src/passes/mod.rs`
- Documenting the infrastructure stacks as discovered in `infra/bin/coderhelm.ts`
- Linking to `SETUP.md` and `docs/jira-integration.md` rather than duplicating them
- Adding badges (CI status, language badges) where appropriate

**Out of scope:**
- Modifying `SETUP.md` or `docs/jira-integration.md`
- Creating new documentation pages beyond `README.md`
- Changing any source code, infrastructure, or workflow files
- Writing a contributor guide or code of conduct

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| README becomes stale as architecture evolves | Medium | Keep architectural details at a high level; link to code rather than duplicating it |
| Sensitive details accidentally included (account IDs, URLs) | Low | AWS account `654654210434` and domain `coderhelm.com` are already present in the codebase; include only what is already public |
| README too long / not scannable | Low | Use clear heading hierarchy, a table of contents, and concise prose |
| Inaccurate description of agent passes | Low | Derive pass list directly from `services/worker/src/passes/mod.rs` |