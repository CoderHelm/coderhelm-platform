# jf. `jf_followups_filed_not_asked`

**What happened:** A run on an audit ticket in Jira finished the audit and wrote follow-up ticket drafts into proposal.md. Then it stopped and asked a person either to connect Jira so CoderHelm could file them, or to file them themselves. It marked this as needing a person, with no default. The team's Jira was connected. The run had been started from that Jira ticket, and CoderHelm had commented on it. The planner's list of what CoderHelm can do never named the team's ticket tracker; it only said "Connected apps: not set up", which the planner read as "no Jira". Runs had no tool to file tickets. And nothing stopped a question that asked a person for access CoderHelm already had.

**The guarantee:**
- The planner reads the team's ticket tracker as a capability, with its tool. For Jira, that is the Coderhelm app on the site: it reads tickets, comments, and files follow-up tickets. For a team without Jira, it is GitHub issues when the App may write issues.
- When a ticket's deliverable is follow-up tickets, the plan lists them in `followup_tickets` and CoderHelm files them itself, once the plan is final (see `jg_followups_once_per_topic`):
  - in the source ticket's Jira project;
  - linked to the source with the site's "relates to" link type;
  - typed by the project: the type asked for, else Task, else the project's first standard type. Jira keeps a project's default type in its issue type scheme, which the app's scopes can't read;
  - labelled `coderhelm`, `coderhelm:<repo>` and `coderhelm:followup:<source>`.
- The team's agent permissions decide first (`file_followup_tickets`, a write). Each draft is filed once per source ticket: the worker keeps what it filed, and the app finds a ticket it already filed with the same summary and follow-up label. So a retried run never files twice.
- One comment on the source ticket lists the new keys.
- A refusal is said exactly. A missing project permission is named, with where a Jira admin grants it, and the remaining drafts are posted on the ticket. It is never "connect Jira".
- Without Jira or issues write, the drafts go into the ticket comment and the run completes.
- A run that codes nothing and leaves nothing for a person once its follow-ups are filed ends `completed`, with its findings on the ticket. A plan written before `followup_tickets` existed files the list under proposal.md's follow-up heading, so Retry on such a run files its drafts.
- No ask, hold or plan step may ask a person to connect, or give access to, a system the run already reaches. The stop judge's rules refuse such an answer and re-ask, and its last pass turns one that still asks into a proceed. The plan's person steps and next step that ask it become CoderHelm's.

**Where it's enforced:** `common::followup_tickets` (drafts, ids, tracker, refusal words, the comment), `common::connected_access` (`asks_to_connect`, in `common::stop_judge::{validate, judge}`), `common::host_steps::tracker`; in the worker, `passes::followups::{planner_caps, reaches, prepare, file, nothing_left, finish_delivered}`, called from `passes::mod` (read before the fix-location question is judged, filed on the final plan), and `passes::stop_judge::decide` (the picture's `connected`); in the Jira app, `followups.js` and `createTicketHandler`'s follow-up path (`fileFollowup`).
