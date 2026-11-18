# rk. `rk_team_reads_as_the_team_role`

**What happened:** On a self-hosted install that runs in the same AWS account as the team's resources, an AWS WAF alarm run called its WAF tools (`waf_sampled_requests`, `waf_log_objects`). AWS logged those calls (`wafv2:ListWebACLs` and the rest) as the worker's own Lambda service role, and refused them. `live::aws::assume` swapped the account's `CoderHelmLogReader` for the install's own role whenever the reader was in the install's account, and the install's own role never gets the WAF reads. The refusal was reported as "CoderHelm's AWS role doesn't allow wafv2:ListWebACLs yet" and recorded as the team role's setup item, so the run waited for it. The wait's re-check took on the reader role itself (`log_findings::session_for`), was allowed, and started the run again. The new run read with the worker's role again, was refused again and waited again: a loop.

**The guarantee:**
- Every read of a team's AWS data is made as the team's role. `live::aws::assume` always takes on the connected role with the team's private ID, the install's own account included. It returns a `TeamSession`, which only `assume` and `assume_exact` can make. The WAF tools, alarm evidence, WAF log objects, CloudWatch metrics, log reads and the permission check accept only a `TeamSession`, so the install's own credentials can't reach them.
- `log_findings::session_for`, which the re-check of a waiting run uses, is the account's connection taken on with `assume`. That is the same target and the same call the run's tools use, so the re-check and the run can't disagree.
- A refusal names the principal AWS refused when it isn't the team's role. It counts as CoderHelm's own failure (an error log), never as the team's setup item.
- A run that a re-check started again never waits again for a read the re-check had read (`role_wait::ending_after`). It records CoderHelm's own failure (an error log and one line on the run) and goes on with what it has.

**Test:** `services/worker/src/passes/incidents/rk_team_reads_as_the_team_role.rs`
