# wa. `wa_duplicate_keeper_stays`

**What happened:** One source ticket fanned out into many overlapping follow-ups before the one-per-topic filing rule shipped, and the hourly duplicate sweep went wrong on them in two ways. First, it ranked each group afresh on every sweep from each ticket's latest run: finished work first, then a pull request, then the earliest run. The keeper was never recorded, and a ticket the sweep had closed itself still ranked. One sweep kept a finished ticket and closed the other two. Its run then went back to work on a review and no longer counted as finished, so the next sweep kept the earliest ticket, which it had already closed, and closed the last open pull request on the topic in its favour. All the work on that topic was closed. Second, a close needed the word matcher to read two tickets as the same topic, while filing treats a maybe that the small check confirms as the same topic. Tickets that said the same thing in other words were flagged every hour and never settled, so their pull requests stayed open. The sweep also closed pull requests without the agent-permission gate and without checking who had worked on them.

**The guarantee:**
- A group's keeper is decided once and recorded (`FOLLOWUP_KEEP#<source>#<keeper>`, a conditional write) before anything closes. Every later sweep, in the worker or the gateway, keeps the recorded keeper while it is still in the running.
- A member that is out (closed, cancelled, covered by another pull request, or with its pull request closed unmerged) is never kept. If the recorded keeper went out, for example because a person closed it, the keeper is picked again from the open members.
- A duplicate closes only in favour of a keeper it is directly paired with. A chain through a third ticket never joins two topics.
- A close never takes the topic's or the family's last open pull request, and the keeper is read again right before each close.
- A family is grouped on the topic key that filing uses: the small check reads the two as the same work, and the matcher finds them near.
- Each pull request closes through the agent-permission gate, with a note naming the keeper's pull request, and its branch is kept. A pull request a person committed to, or asked for changes on, is never closed.
- Jira gets only the close note it always got.
