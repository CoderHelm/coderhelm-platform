# i. `i_finished_work_pr`

**What happened:** A run with one planned task left ended with no pull request. Later the run opened a draft but left the work to a person.

**The guarantee:** Finished work always ends with a pull request. Planned work that is still open puts a draft PR up with a "Not finished" section. CoderHelm then keeps working those tasks in rounds on the PR and marks it ready when the section is empty. It stops only after two rounds in a row without progress. It then names the tasks and why, and sets the run to `needs_input`, never `completed`.
