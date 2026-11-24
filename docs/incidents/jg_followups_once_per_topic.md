# jg. `jg_followups_once_per_topic`

**What happened:** A person retried a run on a Jira audit ticket. The earlier run had finished the audit and written eight follow-up drafts. The retry planned again instead of taking up that plan. It filed six follow-up tickets from its first plan. A later check then sent that plan back: "the plan stopped on a change outside the code that leaves nothing for a person and waits for nothing". The run planned again and filed four more tickets in other words. Three of the four were the same work as three of the six. Then the run failed with the same message. That made ten tickets, three pairs of duplicates, and a failed run whose deliverable (the audit and its follow-ups) was done.

**The causes:**
- The follow-ups were filed right after each plan was written, before the plan's last check. A plan that was sent back had already filed.
- A draft counted as filed only when its summary matched word for word. The same work, worded again, was filed again.
- The check read "a person has nothing to do here" as "the plan stopped short", even when the plan's deliverable was the findings themselves.
- The earlier run's plan was never read back. Its spec.md was empty (an audit writes none), and every read of a cached plan with an empty file came back as "no plan".

**The guarantee:**
- When the plan is written, its follow-ups are only read, and its filing steps become CoderHelm's. They are filed once, on the plan the run ends with or implements: the delivered ending, the no-code ending that doesn't plan again, or right before the implement pass. A plan sent back files nothing.
- Before filing, CoderHelm reads the follow-ups already filed for the source ticket: its own records, the comment it posted on the source, and the tracker's list (the Jira app's list of tickets with the `coderhelm:followup:<source>` label, or the repository's labelled issues).
- A draft whose topic one of them covers is not filed. Topic means the normalized title words and the key files and components the draft names. Drafts that are close but not clear go to a small model check, and one it doesn't clear is treated as covered. The comment lists a covered draft with the ticket that covers it. When the draft names files or components that ticket doesn't, that added scope is posted on that ticket.
- Each draft's record keeps its summary and key files, so the next run compares by topic. A retry and a re-plan never add a duplicate.
- A plan that codes nothing and answers a question (an audit, a review, a scoping), or that files follow-ups and leaves nothing for a person, ends `completed`. Its findings and follow-ups go on the ticket. A step it leaves for a person is listed as a recommendation, not a wait. The "leaves nothing for a person" check never fails such a run.
- A new run of the unchanged ticket takes up an earlier plan whose deliverable was findings with follow-up drafts, and files what is left of them instead of planning again. A cached plan with an empty spec.md or design.md is whole.

**Cleanup of the incident's tickets:** `scripts/followup_dupes.py` lists the follow-ups filed for a source ticket from CoderHelm's records, groups likely duplicates and proposes which to keep. It reads only; it never changes Jira.

**Where it's enforced:** `common::followup_tickets` (`marks`, `topic_words`, `topic`, `near`, `added_scope`, `same_topic_prompt`, `read_same_topic`, `titles_from_comment`, `Filed::Covered`); in the worker, `passes::followups::{prepare, file, delivers_findings, reusable_findings, delivered_report, finish_delivered}`, called from `passes::mod` (the delivered ending, `NoCodeEnd::Delivered`, the implement pass, `findings_plan`, `may_be_empty`); in the Jira app, the follow-up list path (`listFollowups`, `followups.listedIssues`).
