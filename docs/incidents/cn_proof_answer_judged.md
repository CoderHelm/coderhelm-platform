# cn. `cn_proof_answer_judged`

**What happened:** A ticket asked to stop slot descriptions showing literal HTML entities. The check before the pull request listed gaps that asked to confirm things: which stored descriptions hold entities, and whether the live page shows them. The fix round did that work (four database queries, three page checks, a run of the checks) and wrote no code, because the gaps asked for answers. The check then saw the same branch head, reused its result from before the round, called the round no progress and listed the same gaps under Open items. The answers never reached the judge. Across many runs, most such fix rounds wrote no code, and they carried most of the cost.

**The guarantee:** A fix round that made a lookup or ran a check is judged again on the same head, with what it found. Fewer open gaps lets fixing go on. The same number or more ends it, as before, so an answer can never loop.

**Where it's enforced:** `passes::proof::round_answered`, `run_guard::CheckRecord::answered`, called in `proof::prove` after each fix round.
