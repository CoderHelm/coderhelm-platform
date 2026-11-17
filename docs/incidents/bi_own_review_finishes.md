# bi. `bi_own_review_finishes`

**What happened:** A run's own review asked for changes; its fix rounds were blocked by a refused edit, checks skipped for time and a tool input error, the loop called that "no progress" at its cap of two, deferred a one-line config omission and a placeholder line, and marked the run completed while the run page said a fix round was taking them. The finding contradicted a comment on the ticket, and an outside step asked a person to add labels.

**The guarantee:** Each finding is tracked on its own: a blocked round is not a stall and the next one removes the block; a stalled finding climbs its own ladder; a finding the ticket, its comments or the plan rule out is settled by their words; only a person-gate reason defers one. A run is never completed while its own review asks for changes on the head or with a deferred item lacking such a reason. A finished run's card never says it is working. CoderHelm adds pull request labels itself.
