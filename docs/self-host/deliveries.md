---
title: Deliveries
description: Every event that comes in and every message that goes out, what happened to it, and how to fix a failure. In every setup.
order: 12.5
---

**Settings, Deliveries** (admins) lists every delivery your CoderHelm handles, newest first, in hosted, self-hosted and hybrid setups alike:

| Source | What's recorded |
|---|---|
| GitHub | each webhook: received, signature checked, which team it went to, handled, ignored (not an event CoderHelm acts on) or failed, with the reason |
| CoderHelm relay (hybrid) | each forward to your install, GitHub's (`relay`) and Jira's (`relay_jira`) apart: every attempt, the answer, how long it took, the next retry |
| Token broker (hybrid) | each token minted or refused |
| Jira | issue events and their outcome (from the Jira event log) |
| Alerts | alarms received and what CoderHelm did (from the alert log) |
| Slack, Teams | each notification posted, one row per channel |
| Email | each email sent: the recipient's domain only, never the address. Bounces and complaints of email through CoderHelm, with a shortened address |

At the top: the success rate over the last 24 hours, failures in the last 24 hours and the oldest delivery still pending. Filter by source, status and time, or search by delivery id, GitHub's delivery id, repository or PR number.

Open a row for its details:

- **Headers**, with every secret removed (signatures, authorization, cookies, tokens).
- **A payload preview**: at most 4 KB, with secret-looking fields masked. Records never keep more.
- **The attempts**: time, answer and duration of each.
- **What went wrong, in plain words, and the fix.**
- **Redeliver**, where it's safe: GitHub deliveries (GitHub sends them again, for about 3 days) and relay forwards, GitHub's and Jira's, while the event is still held (up to 24 hours while retrying; after that, change the issue in Jira again). Notifications and email aren't kept, so they can't be sent again: send a test from their settings.

Records are kept for 14 days, then deleted.

## Overview

The **Overview** tab shows how deliveries went over the last 24 hours (by hour), 7 days or 30 days (by day), against the period before:

- the success rate, the number of deliveries and failures, and how long the slowest 5% took;
- deliveries and the success rate over time;
- failures by reason, in plain words with the fix;
- each outbound webhook endpoint's success rate, latency, last failure, failures in a row and whether it is turned off;
- deliveries by source, and the event types that failed most.

It has the same source and status filters as the list, plus the endpoint. Every bar, tile, reason, endpoint and source opens the list filtered to it.

The overview reads counters that are updated as each delivery is recorded, one set per hour (kept 3 days) and per day (kept 120 days), so it stays fast however many deliveries there are, and it covers more than the 14 days records are kept. A delivery counts once it finishes (retries included), so one still retrying shows in the list but not yet in the overview. The first time it opens after an update, deliveries recorded before then are counted once, which can take a few reloads on a busy install. On a hybrid install, the relay's counters come from CoderHelm, like its records.

The same numbers are in the API: `GET /api/deliveries/analytics` with an API key holding the `deliveries` scope.

## Relay: GitHub and Jira

On a hybrid install the Overview has a **Relay** panel with one row per provider, GitHub and Jira (Jira once a site is linked or has had a forward): forwards finished in the period, failures, the success rate, how long the slowest 5% took, and when a forward **last went through** and **last failed for good** (any period). The API has the same as `relay`. Each forward is also a row in the list (source `relay` for GitHub, `relay_jira` for Jira), with its attempts and **Redeliver**. Forwards are retried with growing waits for about 24 hours; one that never got through counts as failed with `relay_gave_up`.

## Email

A self-hosted install's Overview has an **Email** panel, and the API has it as `email`: for each kind of email (sign-in and invites, password resets, run and review notices, the weekly summary, alerts, other), how many were **sent**, **delivered**, **bounced**, marked as **spam** (complaints) and **rejected**, over the period's UTC days (24 hours shows yesterday and today), with when an email was last sent, delivered and bounced.

- **Through CoderHelm:** CoderHelm counts the mail service's own events (from Amazon SES) for your install, and your install reads them with its registration secret. Only counts and times: no address, subject or content.
- **Your own SES:** your install counts its own SES events (its configuration set sends them to its own worker). Sign-up codes and password resets count too once Amazon Cognito sends through your SES (`email.cognitoUsesSes`).
- **Amazon Cognito:** the panel says who sends sign-up codes and password resets. Cognito's own address is limited to about 50 emails a day; see [Email](./email.md#sign-up-and-password-emails).

Switching senders keeps both counts, added up.

## Sharing with CoderHelm support

Self-hosted and hybrid installs can choose to share delivery data with CoderHelm support, in the install guide or in **Settings, System, Hybrid**. It's **off** by default.

- **What's sent:** the metadata of failed deliveries (source, event type, status, the reason code, attempts and timings). No headers.
- **Payloads:** never, unless you also choose "Include payload previews".
- **Why:** support can see what failed and help you debug it without screen-sharing, and CoderHelm notices relay failures on its side before you do.

Your own Deliveries page works the same either way.
