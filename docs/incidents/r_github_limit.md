# r. `r_github_limit`

**What happened:** A fix round hit GitHub's hourly limit. Dozens of writes and the diff came back "403 rate limit exceeded", and the run ended "the coder wrote no file ... That is CoderHelm's miss".

**The guarantee:** GitHub refusing for a while (its rate limit, a 5xx, no answer) never ends a run and is never blamed on the coder. The model is told the run pauses, the pass ends, and the run goes on as itself at GitHub's reset, with its page saying when. The wait adds no step and no pass. Writes GitHub rejected for a real reason are reported with that reason. A review waits for the reset without spending its retries.
