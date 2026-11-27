# bj. `bj_release_through_a_busy_hour`

**What happened:** Two repositories' Security pages read hundreds of findings "in a pull request" for days, through releases meant to give them back. The sweep that compares findings with GitHub logged only when it changed something, so a sweep that found every pull request still open left no trace; and while GitHub's budget was low it was held before its first read, several sweeps in a row. Both pull requests were in fact open.

**The guarantee:** Every sweep leaves one line with what it read, what GitHub said (open, merged, closed, unanswered), what it changed and what it left. A held budget slows the release and never stops it: a few reads a sweep go on a small allowance of their own (`github_budget::caller::FLOOR`), in a turn that reaches every pull request within a few sweeps.
