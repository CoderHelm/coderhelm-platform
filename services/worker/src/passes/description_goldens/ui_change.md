<!-- coderhelm:lead -->
<!-- coderhelm:body v1 ticket=KEY-123 head=d78bb24 -->
### Problem

Hero subtitle character limit ([KEY-123](https://tracker.example.com/browse/KEY-123)).

### Changes

Adds a Studio warning when a Hero (Default) subtitle is longer than 160 characters, so editors fix long subtitles before they break the hero layout. Editors can still publish.<!-- ch:summary-end --> Done outside the diff: post the 23 documents already over the limit on KEY-123.
- Approach: is a non-blocking warning what KEY-123 asks for, or should over-limit subtitles block publishing? CoderHelm's conclusion: all checks on this head passed.
- Read first: [`apps/studio/schemaTypes/blocks/hero-block-validation.ts`](https://github.com/acme/site/blob/d78bb24/apps/studio/schemaTypes/blocks/hero-block-validation.ts) (the rule, +21 lines); then the other 2 files.

### Needed from you

- [ ] <!-- ch:deploy-staging --> **@studio-owner**, after merge: Give CoderHelm the deploy credentials for this environment or run the command below once. Why a person: the repo has no deploy workflow CoderHelm can start.

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

See it: [the preview](https://preview.example.com/studio), document `/private-events/weddings`, Hero, Sub Title field.

How to check: `npm test -- hero-block-validation`.

| Before | After |
|---|---|
| <img src="https://github.com/user-attachments/assets/before-example" width="380" alt="Sub Title field with a 190-character subtitle and no warning"> | <img src="https://github.com/user-attachments/assets/after-example" width="380" alt="Same field showing the warning 'Over 160 characters (190)'"> |

<!-- /coderhelm:lead -->

<details><!-- fold -->
<summary>Implementation notes</summary><!-- fold -->

- `hero-block-validation.ts`: a `Rule.custom` that returns a warning (not an error) above 160 characters, so publishing is never blocked.
- `hero-block.ts` and `schemaTypes/index.ts`: register the rule on the Default variant only; other Hero variants have their own layouts.
- Rejected: `Rule.max(160)`, which is an error and would block 23 published documents from being re-saved.

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