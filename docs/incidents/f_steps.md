# f. `f_steps`

**What happened:** The generator check ran out of time and the run failed "not enough time left".

**The guarantee:** A gate that lacks time continues in a fresh step at that gate, as a new queue message. A run that keeps making progress continues past 20 steps. Two steps in a row without progress stop it, and it says where it was stuck.
