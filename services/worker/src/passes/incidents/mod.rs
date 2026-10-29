//! The incident regression suite: every run failure that reached a person
//! is a case here, so it can never come back silently.
//!
//! Each case replays the failure as a scenario on one invented monorepo
//! (`fixture`), through the pipeline's own decision code, with fakes only at
//! the edges: GitHub, the model's replies, the sandbox's archive and log,
//! and DynamoDB records. Each asserts how the run ends (its status), what it
//! leaves (the pull request, the notes), and that the old failure message
//! never appears. Where a decision lives inside the pipeline itself, the
//! case also checks that the pipeline still calls it at that place.
//!
//! CI runs the suite as its own job (`cargo test -p worker incidents::`),
//! and `docs/incidents.md` lists each case with the guarantee it protects.
//! A fix for a new failure adds a case here and a line there.

mod fixture;

mod a_whole_branch;
mod b_open_pr_retry;
mod c_codegen_egress;
mod d_echoed_markers;
mod e_large_generated_output;
mod f_steps;
mod g_large_plan;
mod h_pr_updates;
mod i_finished_work_pr;
mod j_post_merge_watch;
mod k_own_review_findings;
