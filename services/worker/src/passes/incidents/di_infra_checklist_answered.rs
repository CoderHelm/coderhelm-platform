//! di. A ticket asked for a queue between the API and the worker. The plan
//! read the Terraform, added the queue resource and the IAM statement, and
//! said nothing about how it rolls out across environments, how it rolls
//! back, what else the worker role serves, what the queue costs or limits,
//! or the alarm that shows it backing up. The change was right; what a
//! careful engineer settles before an infrastructure change was left to the
//! reviewer to ask, one comment at a time.
//!
//! Guarantee: when the plan's files or the ticket reach infrastructure, CI,
//! auth and security, data, a hot path or observability, design.md answers
//! that area's checklist under `## Non-functional checks`
//! (`common::nfr_check`; the lists are data in `nfr_checklists.json`).
//! Each answer says where it comes from (a file, a command, a lookup); an
//! item that doesn't apply says why. What is missing goes back with the
//! plan's other gaps in the same send-back: no new stop, no approval.

use common::nfr_check;

const TICKET: &str = "Add a queue between the API and the worker so exports don't time out.";

const TASKS: &str = "- [ ] infra/queues.tf: [create] The export queue and its dead-letter queue\n- [ ] infra/iam.tf: [modify] Let the worker role send and receive on the queue\n- [ ] services/api/src/export.rs: [modify] Send the export job to the queue\n";

const DESIGN_SILENT: &str =
    "## Approach\nAdd an SQS queue in `infra/queues.tf` and send exports to it.\n";

const DESIGN_ANSWERED: &str = "## Approach\nAdd an SQS queue.\n\n## Non-functional checks\n### Infrastructure\n- rollout: `deploy.yml` applies `infra/` to staging, then prod after approval (source: `.github/workflows/deploy.yml`)\n- rollback: revert the commit; the API keeps its direct path until the queue's consumer is live (source: `services/api/src/export.rs`)\n- blast_radius: a new queue; the worker role gains two actions on it only (source: `infra/iam.tf`)\n- environments: staging and prod both apply `infra/` with their own tfvars (source: `infra/envs/`)\n- permissions: `sqs:SendMessage` for the API role and `sqs:ReceiveMessage` for the worker role, on this queue's ARN (source: `infra/iam.tf`)\n- cost: n/a: about 3k messages a day, inside the free tier per the lookup of last month's export count\n- limits: 256 KB per message; an export job is under 2 KB (source: `services/api/src/export.rs`)\n- monitoring: an alarm on the oldest message's age over 10 minutes (source: `infra/alarms.tf`)\n### Auth and security\n- threat: only the two roles reach the queue; no public policy (source: `infra/iam.tf`)\n- permissions: least privilege as above (source: `infra/iam.tf`)\n- secrets: n/a: the roles use instance credentials, no new secret\n- input: the worker validates the job's schema before running it (source: `services/worker/src/jobs.rs`)\n- compatibility: existing exports keep the direct path until the flag flips (source: `services/api/src/export.rs`)\n";

fn planned() -> Vec<String> {
    crate::passes::feature::plan_files::planned_files(TASKS)
        .into_iter()
        .map(|f| f.path)
        .collect()
}

#[test]
fn an_infrastructure_plan_answers_its_checklist_or_goes_back() {
    let files = planned();
    let applies: Vec<String> = nfr_check::areas_for(&files, TICKET)
        .into_iter()
        .map(|a| a.area)
        .collect();
    assert_eq!(applies, vec!["infrastructure", "security"]);

    let gaps = nfr_check::plan_gaps(DESIGN_SILENT, &files, TICKET);
    assert_eq!(gaps.len(), 1, "{gaps:#?}");
    assert!(gaps[0].contains("`## Non-functional checks`"));
    assert!(gaps[0].contains("`rollback`") && gaps[0].contains("`blast_radius`"));
    assert!(gaps[0].contains("`infra/queues.tf`"));

    assert!(nfr_check::plan_gaps(DESIGN_ANSWERED, &files, TICKET).is_empty());

    // An answer with no source, and a "doesn't apply" with no reason, go back.
    let weak = DESIGN_ANSWERED
        .replace("(source: `infra/alarms.tf`)", "")
        .replace(
            "an alarm on the oldest message's age over 10 minutes",
            "an alarm on the queue",
        )
        .replace(
            "- secrets: n/a: the roles use instance credentials, no new secret",
            "- secrets: n/a",
        );
    let gaps = nfr_check::plan_gaps(&weak, &files, TICKET);
    assert_eq!(gaps.len(), 2, "{gaps:#?}");
    assert!(gaps
        .iter()
        .any(|g| g.contains("`monitoring`") && g.contains("where its answer comes from")));
    assert!(gaps
        .iter()
        .any(|g| g.contains("`secrets`") && g.contains("without saying why")));
}

#[test]
fn the_checklist_is_a_plan_gap_never_a_stop() {
    let plan = include_str!("../plan.rs");
    let at = plan
        .find("common::nfr_check::plan_gaps(")
        .expect("the plan check reads the checklists");
    let after = &plan[at..];
    let back = after
        .find("back.extend(nfr_gaps")
        .expect("into the send-back");
    assert!(back < 600, "the gaps join the send-back right there");
    assert!(!after[..back].contains("needs_input") && !after[..back].contains("approval"));
    assert!(plan.contains("nfr = common::nfr_check::planner_section()"));
    // The lists are data: every area has its items and the planner is told
    // each one.
    let told = nfr_check::planner_section();
    for a in nfr_check::areas() {
        assert!(told.contains(&a.title));
        for i in &a.items {
            assert!(told.contains(&format!("`{}`", i.id)), "{}.{}", a.id, i.id);
        }
    }
}
