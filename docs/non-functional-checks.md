# Non-functional checks in planning

Some changes reach infrastructure, CI, auth and security, data, a hot path,
or observability. For those, the plan answers a short checklist for each
area before any code is written. A careful engineer settles these points
before such a change, and a reviewer otherwise has to ask about them one at
a time.

Code: `services/common/src/nfr_check.rs`. The checklists are data, in one
file: `services/common/src/nfr_checklists.json`.

## The areas

| Area | Applies when | Items |
| --- | --- | --- |
| `infrastructure` | Terraform, CDK, CloudFormation, Kubernetes, containers, serverless config | `rollout`, `rollback`, `blast_radius`, `environments`, `permissions`, `cost`, `limits`, `monitoring` |
| `ci` | CI workflows and deploy pipelines | `triggers`, `rollback`, `permissions`, `blast_radius`, `limits` |
| `security` | Authentication, authorization, IAM, WAF, secrets, untrusted input | `threat`, `permissions`, `secrets`, `input`, `compatibility` |
| `data` | Schemas, migrations, stored data | `migration_safety`, `compatibility`, `rollback`, `environments`, `existing_rows` |
| `performance` | Hot paths, queries, caching, anything the ticket calls slow | `hot_path`, `cost_per_call`, `limits`, `measurement` |
| `observability` | Alarms, metrics, dashboards, logs, tracing | `signal`, `thresholds`, `routing`, `noise` |

An area applies when one of the plan's files matches its `paths`:

- `ext`: the file's extension;
- `names`: the file name;
- `dirs`: a folder on the path;
- `prefixes`: the path's start;
- `contains`: a word in a path segment.

It also applies when the ticket names one of the area's `words`.

To add an area or an item, edit the JSON file: the planner's instructions
and the check both read it. Update the table above to match.

## What the plan writes

In design.md:

```
## Non-functional checks
### Infrastructure
- rollout: `deploy.yml` applies `infra/` to staging, then prod (source: `.github/workflows/deploy.yml`)
- cost: n/a: inside the free tier at the measured 3k messages a day
```

`- <area>.<item>: ...` without a heading works too.

- Every item of every area that applies has a line.
- An answer comes from the code or a lookup, and says where. That means a
  `source:`, a backticked file or command, or the lookup it rests on.
- An item that doesn't apply says `n/a: <why>`, with a reason of at least a
  few words.

## When something is missing

Each missing section, item, reason or source is a plan gap. It goes back to
the planner with the plan's other gaps, in the same single send-back
(`passes::plan`). The checklist adds no stop, no question to a person and no
approval. A plan whose change reaches none of the areas leaves the section
out.

The replay case is `services/worker/src/passes/incidents/di_infra_checklist_answered.rs`.
