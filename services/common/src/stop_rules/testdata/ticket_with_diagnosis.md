The "cards list" end-to-end spec fails on every run since Monday.

Failing test: `cards list › shows the card it created` in e2e/tests/cards.spec.ts

Root cause: This is a test bug, not a code bug: the spec reads the first card instead of the one it created. Staging has older cards from other runs, so the first card is not ours.

Fix plan: Select the card by the name the test gives it. Do not change the app.

Acceptance criteria:
- The spec passes 20 times in a row against staging.
- No new waits or retries.
