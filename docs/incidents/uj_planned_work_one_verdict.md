# uj. `uj_planned_work_one_verdict`

**What happened:** A run's planned-work round finished, and its check found "no planned work left". Twenty-eight minutes later, on the same head, the proof check judged the branch again and found "1 planned task(s) still not done". It held the pull request as a draft and sent the run back to work. Each check asked the judge on its own and kept its own result, so they disagreed. The run went round until its continuation ceiling stopped it and handed it to a person. Three more runs on the install did the same.

**The guarantee:** There is one verdict on planned work per head. Whichever check judges a head first keeps its planned work there. The other reads it and never judges that head again, so on one head the two checks always agree.
