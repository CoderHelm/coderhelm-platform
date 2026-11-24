# cm. `cm_test_env_for_the_change`

**What happened:** A web ticket in a monorepo was told its test environment came from a lambda's deploy workflow. That workflow sorts first and only runs for the lambda's folder. The run took the first job with a test step, so every sandbox command went through the lambda's `npm ci`, and the coder was told to run the lambda's tests for a change to the web app. Before that, many of the coder's own test commands for that app failed to start (exit 127 or 254) or were refused.

**The guarantee:** The CI job chosen is the one whose tests run for the files the run plans to change. A workflow filtered to other paths, or a job that runs in a folder none of those files is in, is used only when nothing fits better. With no files to go by, a job that runs on every change from the repository root comes first. A run's saved choice that names another package's job is worked out again.

**Where it's enforced:** `common::test_env::from_workflows_for`, `TestEnv::misses`, `passes::test_setup::for_run` (given the plan's files by `implement::run`).
