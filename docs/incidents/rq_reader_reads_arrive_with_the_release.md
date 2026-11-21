# rq. `rq_reader_reads_arrive_with_the_release`

**What happened:** The owner of a self-hosted install got a push alert in their team's channel: "AWS account (the install's own account) needs 1 new read-only permission. This CoderHelm release reads a little more from your AWS accounts... Until an admin updates the CoderHelmLogReader role, the parts that need it are skipped." The account was the install's own. The daily sweep (`passes::aws_permissions`) skipped the install's own account only when the check's `install_account` was true, but the check's builder always wrote false; only tests ever set it. So that account was treated like any other: the alert went out, and the AWS page offered the one-click update stack for it. And since every team read is made as the account's `CoderHelmLogReader` (case `rk`), that role in the install's own account really did fall behind each release: nothing in the release updated it.

**The guarantee:**
- No push alert for new read-only permissions, ever: the daily sweep only checks every account and saves the result for the AWS page. Its told and claim records and the message are gone.
- Another account shows it only as the AWS page's quiet item, **Update needed**, with its one-click update stack, and as the install's **AWS role reads** setup item.
- The install's own account on a one-stack install is marked by the check's own builder (`PermissionCheck::new`, from the install's read role and `INSTALL_READER_KEPT`). It gets no update stack, no setup item, and the page says it updates with the release.
- The release brings the reads itself: the access stack's reader update (`lambda/native-reader`, `infra/lib/native/access-stack.ts`) runs after each update of the install's stack, once a day, and when an admin applies the access stack. It creates or updates `CoderHelmLogReaderUpdate-release` from the running release's published update template, with read-only groups only. Its role may do only that, only from that template, and may write only that role's policy, only through CloudFormation.
- An access stack made before the reader update asks an AWS admin once (Settings, System, Permissions). From then on, no release needs anyone for that role's reads.

**Test:** `services/worker/src/passes/incidents/rq_reader_reads_arrive_with_the_release.rs`
