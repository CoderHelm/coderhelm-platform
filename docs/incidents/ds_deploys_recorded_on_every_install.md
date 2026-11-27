# ds. `ds_deploys_recorded_on_every_install`

**What happened:** The Insights line of deploys CoderHelm started went flat and stayed flat for weeks, on an install that reached GitHub through CoderHelm's token broker. That gateway had no GitHub App key of its own, so asking GitHub for the app's slug failed, and `queue_deploy_run` dropped every completed workflow run before it reached the worker, with nothing in any log. Every record the team had came from the one-time backfill, which read history up to the day the line stopped and was marked done, so the hole was never read back and nothing said the line had stopped.

**The guarantee:**
- A gateway that can't name the app's bot still sends every run a bot triggered, using the broker's bot login when it has one. The worker decides with its own bot, before it asks GitHub anything.
- A team that records deploys on most days (8 of the last 14), then records none for 48 hours while 10 or more runs its bots triggered arrive, is stalled. The Learning health page shows a red `deploy_records` loop, and every hourly check logs `deploy_records_stalled`, which raises the `deploy-records-stalled` alarm.
- Any such hole in the last 45 days is read back once from GitHub's run history, starting from each repository's newest record before the hole. The backfill's own passes do the reading, and the team's marker is left as it was. Reading a deploy again writes the same record.
