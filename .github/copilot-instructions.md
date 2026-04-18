# Copilot Instructions

## Git Commits
- Never add `Co-authored-by` trailers to commit messages
- Use concise, descriptive commit messages with conventional commit prefixes (feat, fix, perf, refactor, chore)

## Project
- Rust workspace at `services/Cargo.toml`
- Two main crates: `gateway` (API server) and `worker` (pipeline)
- AWS profile: `nadya` for all CLI operations
- Always run `cargo check` before committing
