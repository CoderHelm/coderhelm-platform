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
mod aa_promised_round;
mod ab_label_prefix;
mod ac_partial_and_red_ci;
mod ad_hand_back_reply;
mod ae_budget_rounds;
mod af_lockfiles_out_of_sync;
mod ag_red_ci_ships_what_passes;
mod ah_dep_run_choice_and_proof;
mod ai_open_items_settled;
mod aj_coder_habits;
mod al_learning_loop;
mod am_description_from_the_head;
mod an_six_database_findings;
mod ao_stopped_log_finding;
mod ap_closed_pr_keeps_findings;
mod as_infra_closed_pr_keeps_record;
mod b_open_pr_retry;
mod ba_verdict_on_old_commit;
mod bb_flaky_not_broken;
mod bc_stays_through_merge_and_deploy;
mod c_codegen_egress;
mod d_echoed_markers;
mod e_large_generated_output;
mod f_steps;
mod g_large_plan;
mod h_pr_updates;
mod i_finished_work_pr;
mod j_post_merge_watch;
mod k_own_review_findings;
mod l_plan_time;
mod m_data_pipeline_rules;
mod n_suite_setup_broke;
mod o_mention_resumes_held_work;
mod p_finished_work_loop;
mod q_run_ceiling;
mod r_github_limit;
mod s_silent_draft;
mod t_unrun_check_hold;
mod u_stacked_plan_check;
mod v_dropped_field_hold;
mod w_stale_hold_recheck;
mod x_silent_review_hold;
mod y_skipped_checks_wait;
mod z_run_facts;
