---
title: Deliveries
description: Every event that comes in and every message that goes out, what happened to it, and how to fix a failure. In every setup.
order: 12.5
---

**Settings, Deliveries** (admins) lists every delivery your CoderHelm handles, newest first, in hosted, self-hosted and hybrid setups alike:

| Source | What's recorded |
|---|---|
| GitHub | each webhook: received, signature checked, which team it went to, handled, ignored (not an event CoderHelm acts on) or failed, with the reason |
| CoderHelm relay (hybrid) | each forward to your install: every attempt, the answer, how long it took, the next retry |
| Token broker (hybrid) | each token minted or refused |
| Jira | issue events and their outcome (from the Jira event log) |
| Alerts | alarms received and what CoderHelm did (from the alert log) |
| Slack, Teams | each notification posted, one row per channel |
| Email | each email sent: the recipient's domain only, never the address |

At the top: the success rate over the last 24 hours, failures in the last 24 hours and the oldest delivery still pending. Filter by source, status and time, or search by delivery id, GitHub's delivery id, repository or PR number.

Open a row for its details:

- **Headers**, with every secret removed (signatures, authorization, cookies, tokens).
- **A payload preview**: at most 4 KB, with secret-looking fields masked. Records never keep more.
- **The attempts**: time, answer and duration of each.
- **What went wrong, in plain words, and the fix.**
- **Redeliver**, where it's safe: GitHub deliveries (GitHub sends them again, for about 3 days) and relay forwards while the event is still held. Notifications and email aren't kept, so they can't be sent again: send a test from their settings.

Records are kept for 14 days, then deleted.

## Sharing with CoderHelm support

Self-hosted and hybrid installs can choose to share delivery data with CoderHelm support, in the install guide or in **Settings, System, Hybrid**. It's **off** by default.

- **What's sent:** the metadata of failed deliveries (source, event type, status, the reason code, attempts and timings). No headers.
- **Payloads:** never, unless you also choose "Include payload previews".
- **Why:** support can see what failed and help you debug it without screen-sharing, and CoderHelm notices relay failures on its side before you do.

Your own Deliveries page works the same either way.
