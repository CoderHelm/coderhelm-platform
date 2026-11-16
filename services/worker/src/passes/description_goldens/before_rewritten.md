<!-- coderhelm:lead -->
<!-- coderhelm:body v1 ticket=KEY-123 head=d78bb24 -->
### Problem

Hero subtitle character limit ([KEY-123](https://tracker.example.com/browse/KEY-123)).

### Changes

Adds a Studio warning when a Hero (Default) subtitle is longer than 160 characters, so editors fix long subtitles before they break the hero layout. Editors can still publish.<!-- ch:summary-end --> Done outside the diff: post the 23 documents already over the limit on KEY-123.
- Approach: decide whether this does what KEY-123 asks. CoderHelm's conclusion: all checks on this head passed.
- Read first: [`apps/studio/schemaTypes/blocks/hero-block-validation.ts`](https://github.com/acme/site/blob/d78bb24/apps/studio/schemaTypes/blocks/hero-block-validation.ts) (the rule, +21 lines); then the other 2 files.

### Needed from you

- [ ] <!-- ch:deploy-the-staging-studio --> **Studio owner**, after merge: Give CoderHelm the deploy credentials for this environment or run the command below once. Why a person: a credential only a person holds.

  ```sh
  cd apps/studio && npm run deploy:staging
  ```

### Risk

**Risk:** Low. Warning-level validation only; no schema, data, or publish-flow change.

**Rollback:** Revert this PR and redeploy Studio; nothing to undo in data.

### Verification

On d78bb24 (head of this PR):

| Check | Result |
|---|---|
| Lint and typecheck (CI) | Passed |
| Unit tests (CI) | Passed |
| Studio build (CI) | Passed |
| Studio in local dev, manual (CoderHelm) | Passed; warning shows at 161 characters; Publish stays enabled |

See it: document `/private-events/weddings`, Hero, Sub Title field.

How to check: `npm test -- hero-block-validation`.

No screenshot: CoderHelm had no runnable preview to capture for this run.

<!-- /coderhelm:lead -->

<details><!-- fold -->
<summary>Implementation notes</summary><!-- fold -->

Source ticket: KEY-123

Adds the warning rule.

- `hero-block-validation.ts`: a `Rule.custom` that returns a warning (not an error) above 160 characters, so publishing is never blocked.

</details><!-- fold -->

<details><!-- fold -->
<summary>Full test evidence</summary><!-- fold -->

<!-- coderhelm:all-checks -->
Every check on this head:
- [`Lint and typecheck`](https://ci.example.com/Lint-and-typecheck): passed
- [`Unit tests`](https://ci.example.com/Unit-tests): passed
- [`Studio build`](https://ci.example.com/Studio-build): passed
- [`Deploy to Production`](https://ci.example.com/Deploy-to-Production): skipped (its conditions left this PR out)
<!-- /coderhelm:all-checks -->

<!-- coderhelm:evidence -->
## Steps and evidence
- Done: Post the 23 documents already over the limit on KEY-123 (posted on the ticket for the content team).
- Checked: Studio in local dev, manual, warning shows at 161 characters; Publish stays enabled.
<!-- /coderhelm:evidence -->

</details><!-- fold -->

<details><!-- fold -->
<summary>How CoderHelm built this</summary><!-- fold -->

<!-- coderhelm:built -->
Plan, steps and logs: [the run page](https://app.example.com/runs/detail?id=run-4812).
<!-- /coderhelm:built -->

</details><!-- fold -->

<!-- coderhelm:footer -->
Opened by CoderHelm from [KEY-123](https://tracker.example.com/browse/KEY-123) · [run log](https://app.example.com/runs/detail?id=run-4812)
<!-- /coderhelm:footer -->