<!-- coderhelm:lead -->
<!-- coderhelm:body v1 ticket=KEY-456 head=4be19a0 -->
<!-- coderhelm:fold-label All 48 updates -->
Updates 48 npm packages: 3 major, 9 minor, 36 patch ([KEY-456](https://tracker.example.com/browse/KEY-456)). Fixes 2 security advisories.<!-- ch:summary-end -->

### Review focus

- Approach: are the 3 major upgrades safe for how this code uses them (evidence in the table)?
- Read first: [`next.config.ts`](https://github.com/acme/site/blob/4be19a0/next.config.ts) (caching defaults changed in Next 15); then the other 4 files; skip `package-lock.json`, which is generated.

| Package | Change | Why it matters |
|---|---|---|
| `next` | 14.2.15 to 15.1.0 | Major. `fetch` is no longer cached by default; restored with explicit `cache: "force-cache"` in 2 routes |
| `eslint` | 8.57.0 to 9.17.0 | Major. `.eslintrc` replaced with `eslint.config.mjs`; same rules |
| `@types/node` | 20.17.6 to 22.10.2 | Major. Types only; matches the Node 22 runtime already in use |
| `cookie-jar` | 0.6.0 to 0.7.2 | Security: GHSA-aaaa-bbbb-0001 |
| `route-match` | 6.2.2 to 6.3.0 | Security: GHSA-aaaa-bbbb-0002 |

**Risk:** Medium. Next 15 changes request caching across all pages; a missed caching default would surface as stale or uncached data, not a crash.

**Rollback:** Revert this PR; the lockfile restores every version. No data or config outside the repo changes.

### Verification

On 4be19a0 (head of this PR):

| Check | Result |
|---|---|
| Install from lockfile, npm ci (CI) | Passed |
| Lint (CI) | Passed |
| Typecheck (CI) | Passed |
| Tests (CI): 4 jobs | Passed; all passed |
| Build (CI) | Passed |
| Cached routes `/products` and `/blog` (CoderHelm) | Passed; same before and after: `cache HIT on second request` |

<!-- /coderhelm:lead -->

<details><!-- fold -->
<summary>All 48 updates</summary><!-- fold -->

<!-- coderhelm:notes-top -->
| Package | From | To | Type |
|---|---|---|---|
| `next` | 14.2.15 | 15.1.0 | major |
| `eslint` | 8.57.0 | 9.17.0 | major |
| `@types/node` | 20.17.6 | 22.10.2 | major |
| `cookie-jar` | 0.6.0 | 0.7.2 | minor |
| `route-match` | 6.2.2 | 6.3.0 | minor |
| `util-00` | 1.4.2 | 1.5.0 | minor |
| `util-01` | 1.4.2 | 1.5.0 | minor |
| `util-02` | 1.4.2 | 1.5.0 | minor |
| `util-03` | 1.4.2 | 1.5.0 | minor |
| `util-04` | 1.4.2 | 1.5.0 | minor |
| `util-05` | 1.4.2 | 1.5.0 | minor |
| `util-06` | 1.4.2 | 1.5.0 | minor |
| `util-07` | 1.4.2 | 1.4.3 | patch |
| `util-08` | 1.4.2 | 1.4.3 | patch |
| `util-09` | 1.4.2 | 1.4.3 | patch |
| `util-10` | 1.4.2 | 1.4.3 | patch |
| `util-11` | 1.4.2 | 1.4.3 | patch |
| `util-12` | 1.4.2 | 1.4.3 | patch |
| `util-13` | 1.4.2 | 1.4.3 | patch |
| `util-14` | 1.4.2 | 1.4.3 | patch |
| `util-15` | 1.4.2 | 1.4.3 | patch |
| `util-16` | 1.4.2 | 1.4.3 | patch |
| `util-17` | 1.4.2 | 1.4.3 | patch |
| `util-18` | 1.4.2 | 1.4.3 | patch |
| `util-19` | 1.4.2 | 1.4.3 | patch |
| `util-20` | 1.4.2 | 1.4.3 | patch |
| `util-21` | 1.4.2 | 1.4.3 | patch |
| `util-22` | 1.4.2 | 1.4.3 | patch |
| `util-23` | 1.4.2 | 1.4.3 | patch |
| `util-24` | 1.4.2 | 1.4.3 | patch |
| `util-25` | 1.4.2 | 1.4.3 | patch |
| `util-26` | 1.4.2 | 1.4.3 | patch |
| `util-27` | 1.4.2 | 1.4.3 | patch |
| `util-28` | 1.4.2 | 1.4.3 | patch |
| `util-29` | 1.4.2 | 1.4.3 | patch |
| `util-30` | 1.4.2 | 1.4.3 | patch |
| `util-31` | 1.4.2 | 1.4.3 | patch |
| `util-32` | 1.4.2 | 1.4.3 | patch |
| `util-33` | 1.4.2 | 1.4.3 | patch |
| `util-34` | 1.4.2 | 1.4.3 | patch |
| `util-35` | 1.4.2 | 1.4.3 | patch |
| `util-36` | 1.4.2 | 1.4.3 | patch |
| `util-37` | 1.4.2 | 1.4.3 | patch |
| `util-38` | 1.4.2 | 1.4.3 | patch |
| `util-39` | 1.4.2 | 1.4.3 | patch |
| `util-40` | 1.4.2 | 1.4.3 | patch |
| `util-41` | 1.4.2 | 1.4.3 | patch |
| `util-42` | 1.4.2 | 1.4.3 | patch |
<!-- /coderhelm:notes-top -->

## Changes

### next 14.2.15 to 15.1.0

- `next.config.ts`: explicit `cache: "force-cache"` on the 2 routes that relied on the old default.

### eslint 8.57.0 to 9.17.0

- `eslint.config.mjs` replaces `.eslintrc.json` with the same rules.

</details><!-- fold -->

<details><!-- fold -->
<summary>Full test evidence</summary><!-- fold -->

<!-- coderhelm:all-checks -->
Every check on this head:
- [`Install from lockfile, npm ci`](https://ci.example.com/Install-from-lockfile,-npm-ci): passed
- [`Lint`](https://ci.example.com/Lint): passed
- [`Typecheck`](https://ci.example.com/Typecheck): passed
- [`Unit tests (1/3)`](https://ci.example.com/Unit-tests-(1/3)): passed
- [`Unit tests (2/3)`](https://ci.example.com/Unit-tests-(2/3)): passed
- [`Unit tests (3/3)`](https://ci.example.com/Unit-tests-(3/3)): passed
- [`Build`](https://ci.example.com/Build): passed
- [`E2E on preview`](https://ci.example.com/E2E-on-preview): passed
- [`Deploy preview to production`](https://ci.example.com/Deploy-preview-to-production): skipped (its conditions left this PR out)
<!-- /coderhelm:all-checks -->

</details><!-- fold -->

<details><!-- fold -->
<summary>How CoderHelm built this</summary><!-- fold -->

<!-- coderhelm:built -->
Plan, steps and logs: [the run page](https://app.example.com/runs/detail?id=run-4907).
<!-- /coderhelm:built -->

</details><!-- fold -->

<!-- coderhelm:footer -->
Opened by CoderHelm from [KEY-456](https://tracker.example.com/browse/KEY-456) · [run log](https://app.example.com/runs/detail?id=run-4907)
<!-- /coderhelm:footer -->