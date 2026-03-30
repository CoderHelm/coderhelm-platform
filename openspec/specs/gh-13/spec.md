# Spec: README for coderhelm-platform

## Scenario 1: Repository landing page is informative

**Given** a developer visits the `coderhelm-platform` GitHub repository for the first time  
**When** they read `README.md`  
**Then** they can understand what CoderHelm is and what problem it solves without reading any other file

---

## Scenario 2: Architecture is discoverable

**Given** a developer wants to understand how the system is structured  
**When** they read the Architecture Overview section of `README.md`  
**Then** they can identify all seven CDK infrastructure stacks (database, storage, api, worker, email, billing, monitoring) and the frontend stack  
**And** they can identify the two Rust Lambda services (gateway and worker) and their responsibilities

---

## Scenario 3: Agent pipeline is explained

**Given** a developer wants to understand how CoderHelm processes a GitHub issue  
**When** they read the Agent Pipeline section  
**Then** they see all five passes listed in order: Triage, Plan, Implement, Review, PR  
**And** each pass has at least a one-sentence description of its purpose

---

## Scenario 4: Repository layout is documented

**Given** a developer clones the repository  
**When** they cross-reference the directory tree against `README.md`  
**Then** every top-level directory (`services/`, `infra/`, `coderhelm-jira/`, `docs/`, `scripts/`, `.github/`) has a corresponding description in the README

---

## Scenario 5: Getting started is actionable

**Given** a developer wants to deploy CoderHelm  
**When** they follow the Getting Started section  
**Then** they are presented with the required prerequisites (Rust 1.80+, Node.js 22+, AWS CDK v2, AWS CLI)  
**And** they are shown the `MODEL_ID="..." cdk deploy --all` command  
**And** they are directed to `SETUP.md` for complete setup instructions

---

## Scenario 6: Integrations are signposted

**Given** a developer wants to connect CoderHelm to their Jira instance  
**When** they read the Integrations section  
**Then** they see a summary of the Jira webhook approach  
**And** they are given a link to `docs/jira-integration.md` for full detail

**Given** a developer wants to register the GitHub App  
**When** they read the GitHub App subsection  
**Then** they see the required permissions, event subscriptions, and webhook URL (`https://api.coderhelm.com/webhooks/github`)

---

## Scenario 7: CI/CD pipeline is described

**Given** a developer submits a pull request  
**When** they read the CI/CD section of `README.md`  
**Then** they understand that CI runs Rust `fmt` + `clippy` checks and CDK synth on every push/PR to `main`  
**And** they understand that merging to `main` triggers an automated CDK deploy via `deploy.yml`

---

## Scenario 8: The README replaces the stub

**Given** the current `README.md` contains only a `## Docs` heading with one link  
**When** the new README is committed  
**Then** the file contains at minimum 6 top-level sections  
**And** the existing `## Docs` link to `docs/jira-integration.md` is preserved within the new Links or Integrations section  
**And** no content from `SETUP.md` or `docs/jira-integration.md` is copied verbatim (they are linked, not duplicated)

---

## Scenario 9: Markdown renders without errors

**Given** the updated `README.md` is pushed to GitHub  
**When** GitHub renders the file  
**Then** all tables display with correct column alignment  
**And** all fenced code blocks have a language specifier  
**And** all internal links (e.g. to `SETUP.md`, `docs/jira-integration.md`) resolve to existing files in the repository  
**And** the CI badge image loads from the correct GitHub Actions workflow URL