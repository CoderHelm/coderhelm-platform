#!/usr/bin/env bash
# The GitHub settings a CoderHelm release relies on
# (docs/self-host/updates-security.md, "What GitHub settings add").
# Code can't turn these on by itself; an organization owner runs this once,
# with the GitHub CLI signed in as an admin of both repositories.
#
#   bash scripts/release-protection.sh --check   # read only: what is and isn't applied
#   bash scripts/release-protection.sh           # apply everything below
#
# Options (apply):
#   --allow-admin-tag-bypass   let organization and repository admins bypass
#                              the "release tags: signed, never moved or
#                              deleted" ruleset (default: nobody can; changing
#                              a release tag means turning the ruleset off
#                              first, which the audit log shows)
#   --no-admin-merge-bypass    admins can't merge to main without an approving
#                              review either (default: they may merge their own
#                              pull request after checks pass, never push to
#                              main directly; with one maintainer, no one else
#                              can approve)
#
# What it applies:
#   both repositories (CoderHelm/coderhelm-platform, CoderHelm/coderhelm):
#     main: changes only through a pull request with an approving review and
#     passing checks, signed commits, no force pushes, no deletion.
#   CoderHelm/coderhelm-platform also:
#     v* tags: signed, never updated, moved or deleted; only admins create one.
#     the "release" environment: a listed reviewer approves every run, only v*
#       tags may deploy to it, admins can't skip the approval.
#     immutable releases; SHA-pinned actions; a read-only default token;
#     outside contributors' workflows wait for approval; Dependabot.
#
# Safe to run again: each setting is put in place by name. --check exits 1
# while anything is missing.
set -euo pipefail

PLATFORM="${PLATFORM:-CoderHelm/coderhelm-platform}"
DASHBOARD="${DASHBOARD:-CoderHelm/coderhelm}"
ORG="${PLATFORM%%/*}"
# Who approves a release (GitHub logins, comma-separated).
REVIEWERS="${REVIEWERS:-nambok}"
# The checks main must pass: the CI job names of each repository.
PLATFORM_CHECKS=("Workflow security" "Rust (workspace)" "Lambda (Python)" "MCP Proxy (Node)" "Jira app (Forge)" "Infra (config, synth)" "Self-host installer" "Launch Stack (template and installer)")
DASHBOARD_CHECKS=("Lint & Build" "Dashboard" "Docs" "Dashboard (self-host build)" "Infra")

MODE=apply
TAG_BYPASS=0
MERGE_BYPASS=1
for a in "$@"; do
  case "$a" in
    --check) MODE=check ;;
    --allow-admin-tag-bypass) TAG_BYPASS=1 ;;
    --no-admin-merge-bypass) MERGE_BYPASS=0 ;;
    -h|--help) sed -n '2,36p' "$0"; exit 0 ;;
    *) echo "Unknown option: $a (see --help)" >&2; exit 2 ;;
  esac
done

command -v gh >/dev/null || { echo "The GitHub CLI (gh) is needed." >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is needed." >&2; exit 1; }
api() { gh api -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" "$@"; }

# ── The desired rulesets ─────────────────────────────────────────────────

RS_MAIN="main: pull requests, checks, signed"
RS_TAGS="release tags: signed, never moved or deleted"
RS_TAG_CREATE="release tags: only admins create"

admins() { # bypass mode
  jq -n --arg m "$1" '[{actor_type: "OrganizationAdmin", actor_id: 1, bypass_mode: $m}, {actor_type: "RepositoryRole", actor_id: 5, bypass_mode: $m}]'
}

main_ruleset() { # check names...
  local checks bypass
  checks=$(printf '%s\n' "$@" | jq -R '{context: .}' | jq -s .)
  if [ "$MERGE_BYPASS" = 1 ]; then bypass=$(admins pull_request); else bypass='[]'; fi
  jq -n --arg name "$RS_MAIN" --argjson checks "$checks" --argjson bypass "$bypass" '{
    name: $name, target: "branch", enforcement: "active",
    conditions: {ref_name: {include: ["~DEFAULT_BRANCH"], exclude: []}},
    rules: [
      {type: "deletion"},
      {type: "non_fast_forward"},
      {type: "required_signatures"},
      {type: "pull_request", parameters: {
        required_approving_review_count: 1,
        dismiss_stale_reviews_on_push: true,
        require_code_owner_review: false,
        require_last_push_approval: true,
        required_review_thread_resolution: false
      }},
      {type: "required_status_checks", parameters: {
        strict_required_status_checks_policy: false,
        required_status_checks: $checks
      }}
    ],
    bypass_actors: $bypass
  }'
}

tags_ruleset() {
  local bypass='[]'
  [ "$TAG_BYPASS" = 1 ] && bypass=$(admins always)
  jq -n --arg name "$RS_TAGS" --argjson bypass "$bypass" '{
    name: $name, target: "tag", enforcement: "active",
    conditions: {ref_name: {include: ["refs/tags/v*"], exclude: []}},
    rules: [{type: "update"}, {type: "deletion"}, {type: "non_fast_forward"}, {type: "required_signatures"}],
    bypass_actors: $bypass
  }'
}

# Creating a v* tag is blocked for everyone but admins (the bypass), who are
# still held to the ruleset above: signed, and never moved afterwards.
tag_create_ruleset() {
  jq -n --arg name "$RS_TAG_CREATE" --argjson bypass "$(admins always)" '{
    name: $name, target: "tag", enforcement: "active",
    conditions: {ref_name: {include: ["refs/tags/v*"], exclude: []}},
    rules: [{type: "creation"}],
    bypass_actors: $bypass
  }'
}

# ── --check: read only ──────────────────────────────────────────────────

missing=0
ok() { printf '  ok       %s\n' "$1"; }
no() { printf '  MISSING  %s\n' "$1"; missing=1; }
unknown() { printf '  UNKNOWN  %s\n' "$1"; missing=1; }

# Compares a repository's ruleset (by name) with the desired one: active,
# the same targets, every rule type, the same required checks, and no more
# bypass than asked for.
check_ruleset() { # repo desired-json
  local repo="$1" want="$2" name id have
  name=$(jq -r .name <<<"$want")
  if ! id=$(api "repos/$repo/rulesets" --jq ".[] | select(.name == \"$name\") | .id" 2>/dev/null); then
    unknown "$repo ruleset \"$name\" (could not read rulesets)"; return
  fi
  if [ -z "$id" ]; then no "$repo ruleset \"$name\""; return; fi
  have=$(api "repos/$repo/rulesets/$id")
  local problems
  problems=$(jq -rn --argjson want "$want" --argjson have "$have" '
    [ (if $have.enforcement != "active" then "enforcement is \($have.enforcement)" else empty end),
      (if $have.target != $want.target then "targets \($have.target)" else empty end),
      (if ($have.conditions.ref_name.include | sort) != ($want.conditions.ref_name.include | sort) then "applies to \($have.conditions.ref_name.include | join(","))" else empty end),
      (($want.rules | map(.type)) - ($have.rules | map(.type)) | map("no \(.) rule") | .[]),
      ( ([$want.rules[] | select(.type == "required_status_checks") | .parameters.required_status_checks[].context] | sort) as $w
        | ([$have.rules[] | select(.type == "required_status_checks") | .parameters.required_status_checks[].context] | sort) as $h
        | ($w - $h) | map("check \"\(.)\" not required") | .[]),
      ( [$want.rules[] | select(.type == "pull_request") | .parameters.required_approving_review_count] as $w
        | [$have.rules[] | select(.type == "pull_request") | .parameters.required_approving_review_count] as $h
        | if ($w | length) > 0 and (($h | length) == 0 or $h[0] < $w[0]) then "fewer than \($w[0]) approving review(s)" else empty end),
      ( ($want.bypass_actors | map({key: (.actor_type + (if .actor_type == "OrganizationAdmin" then "" else "/\(.actor_id)" end)), value: .bypass_mode}) | from_entries) as $w
        | ($have.bypass_actors // [])[]
        | (.actor_type + (if .actor_type == "OrganizationAdmin" then "" else "/\(.actor_id)" end)) as $k
        | if $w[$k] == null then "extra bypass: \($k) (\(.bypass_mode))"
          elif $w[$k] != .bypass_mode then "bypass \($k) is \(.bypass_mode), wanted \($w[$k])"
          else empty end),
      (if ($have | has("bypass_actors")) then empty else "bypass list not readable with this token (needs admin)" end)
    ] | join("; ")')
  if [ -n "$problems" ]; then no "$repo ruleset \"$name\": $problems"; else ok "$repo ruleset \"$name\" ($(jq -r '[.rules[].type] | join(", ")' <<<"$have"); bypass: $(jq -r '[.bypass_actors[]? | "\(.actor_type)/\(.bypass_mode)"] | join(", ") | if . == "" then "nobody" else . end' <<<"$have"))"; fi
}

check_repo_basics() { # repo
  local repo="$1" v
  if v=$(api "repos/$repo/branches/main/protection" 2>/dev/null); then
    printf '  info     %s main also has classic branch protection (signed commits: %s)\n' "$repo" "$(jq -r '.required_signatures.enabled // false' <<<"$v")"
  fi
}

check_environment() {
  local repo="$PLATFORM" env reviewers policies
  if ! env=$(api "repos/$repo/environments/release" 2>/dev/null); then no "$repo environment \"release\" (it does not exist; GitHub would create it with no reviewers the first time a job names it)"; return; fi
  reviewers=$(jq -r '[.protection_rules[]? | select(.type == "required_reviewers") | .reviewers[]?.reviewer.login] | join(",")' <<<"$env")
  if [ -n "$reviewers" ]; then ok "$repo environment \"release\": required reviewers $reviewers"; else no "$repo environment \"release\": required reviewers"; fi
  if [ "$(jq -r '.can_admins_bypass' <<<"$env")" = false ]; then ok "$repo environment \"release\": admins can't skip the approval"; else no "$repo environment \"release\": admins can still skip the approval (can_admins_bypass)"; fi
  if [ "$(jq -r '.deployment_branch_policy.custom_branch_policies // false' <<<"$env")" = true ]; then
    policies=$(api "repos/$repo/environments/release/deployment-branch-policies" --jq '[.branch_policies[] | "\(.type):\(.name)"] | join(",")')
    if [ "$policies" = "tag:v*" ]; then ok "$repo environment \"release\": only v* tags deploy"; else no "$repo environment \"release\": deployment refs are \"$policies\", want only tag:v*"; fi
  else
    no "$repo environment \"release\": any branch or tag may deploy (no deployment tag rule v*)"
  fi
}

check_platform_settings() {
  local repo="$PLATFORM" v
  if v=$(api "repos/$repo/immutable-releases" --jq .enabled 2>/dev/null); then
    if [ "$v" = true ]; then ok "$repo immutable releases"; else no "$repo immutable releases"; fi
  else unknown "$repo immutable releases (needs admin read)"; fi
  if v=$(api "repos/$repo/actions/permissions" --jq .sha_pinning_required 2>/dev/null); then
    if [ "$v" = true ]; then ok "$repo actions must be pinned by SHA"; else no "$repo actions must be pinned by SHA"; fi
  else unknown "$repo actions settings (needs admin read)"; fi
  if v=$(api "repos/$repo/actions/permissions/workflow" --jq .default_workflow_permissions 2>/dev/null); then
    if [ "$v" = read ]; then ok "$repo default workflow token is read-only"; else no "$repo default workflow token is read-only (now: $v)"; fi
  else unknown "$repo default workflow token (needs admin read)"; fi
  if v=$(api "orgs/$ORG" --jq .two_factor_requirement_enabled 2>/dev/null) && [ "$v" != null ]; then
    if [ "$v" = true ]; then ok "$ORG requires two-factor authentication"; else no "$ORG requires two-factor authentication (by hand: https://github.com/organizations/$ORG/settings/security)"; fi
  else unknown "$ORG two-factor requirement (needs an owner's read)"; fi
}

if [ "$MODE" = check ]; then
  echo "$PLATFORM"
  check_ruleset "$PLATFORM" "$(main_ruleset "${PLATFORM_CHECKS[@]}")"
  check_ruleset "$PLATFORM" "$(tags_ruleset)"
  check_ruleset "$PLATFORM" "$(tag_create_ruleset)"
  check_repo_basics "$PLATFORM"
  check_environment
  check_platform_settings
  echo "$DASHBOARD"
  check_ruleset "$DASHBOARD" "$(main_ruleset "${DASHBOARD_CHECKS[@]}")"
  check_repo_basics "$DASHBOARD"
  echo
  if [ "$missing" = 0 ]; then echo "Everything above is applied."; else echo "Not everything is applied: run  bash $0  as an admin (see --help)."; fi
  exit "$missing"
fi

# ── apply ────────────────────────────────────────────────────────────────

ruleset() { # repo body
  local repo="$1" body="$2" name id
  name=$(jq -r .name <<<"$body")
  id=$(api "repos/$repo/rulesets" --jq ".[] | select(.name == \"$name\") | .id")
  if [ -n "$id" ]; then
    api -X PUT "repos/$repo/rulesets/$id" --input - <<<"$body" >/dev/null
  else
    api -X POST "repos/$repo/rulesets" --input - <<<"$body" >/dev/null
  fi
  echo "$repo ruleset: $name"
}

# 1. main on both repositories.
ruleset "$PLATFORM" "$(main_ruleset "${PLATFORM_CHECKS[@]}")"
ruleset "$DASHBOARD" "$(main_ruleset "${DASHBOARD_CHECKS[@]}")"

# 2. Release tags.
ruleset "$PLATFORM" "$(tags_ruleset)"
ruleset "$PLATFORM" "$(tag_create_ruleset)"
# The rulesets the first version of this script made, replaced by the two above.
for old in "release tags: never moved or deleted" "release tags: admins create, signed"; do
  id=$(api "repos/$PLATFORM/rulesets" --jq ".[] | select(.name == \"$old\") | .id")
  if [ -n "$id" ]; then
    api -X DELETE "repos/$PLATFORM/rulesets/$id" >/dev/null
    echo "$PLATFORM ruleset removed: $old"
  fi
done

# 3. The "release" environment: a listed reviewer approves every publish, only
#    v* tags may deploy to it, admins can't skip it. The AWS roles trust only
#    release.yml on a v* tag in this environment (infra/lib/worker-stack.ts).
reviewers_json=$(for u in ${REVIEWERS//,/ }; do api "users/$u" --jq '{type: "User", id: .id}'; done | jq -s .)
api -X PUT "repos/$PLATFORM/environments/release" --input - >/dev/null <<EOF
{
  "wait_timer": 0,
  "prevent_self_review": false,
  "reviewers": $reviewers_json,
  "can_admins_bypass": false,
  "deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true}
}
EOF
# Only the v* tag rule: anything else is removed.
api "repos/$PLATFORM/environments/release/deployment-branch-policies" --jq '.branch_policies[] | "\(.id) \(.type) \(.name)"' |
  while read -r id type name; do
    [ "$type $name" = "tag v*" ] || { api -X DELETE "repos/$PLATFORM/environments/release/deployment-branch-policies/$id" >/dev/null; echo "release environment: removed $type $name"; }
  done
if ! api "repos/$PLATFORM/environments/release/deployment-branch-policies" --jq '.branch_policies[] | select(.type == "tag") | .name' | grep -qxF 'v*'; then
  api -X POST "repos/$PLATFORM/environments/release/deployment-branch-policies" -f name='v*' -f type=tag >/dev/null
fi
echo "$PLATFORM environment: release (reviewers: $REVIEWERS; v* tags only; admins can't skip it)"

# 4. Published releases can't have their files replaced, or their tag moved.
api -X PUT "repos/$PLATFORM/immutable-releases" >/dev/null && echo "$PLATFORM immutable releases: on"

# 5. Actions: every action pinned to a full SHA, a read-only default token,
#    workflows from outside contributors' PRs wait for approval.
api -X PUT "repos/$PLATFORM/actions/permissions" -F enabled=true -f allowed_actions=all -F sha_pinning_required=true >/dev/null
api -X PUT "repos/$PLATFORM/actions/permissions/workflow" -f default_workflow_permissions=read -F can_approve_pull_request_reviews=false >/dev/null
api -X PUT "repos/$PLATFORM/actions/permissions/fork-pr-contributor-approval" -f approval_policy=all_external_contributors >/dev/null
echo "$PLATFORM actions: SHA pinning required, read-only default token, outside PRs need approval"

# 6. Dependabot alerts and security fixes.
api -X PUT "repos/$PLATFORM/vulnerability-alerts" >/dev/null
api -X PUT "repos/$PLATFORM/automated-security-fixes" >/dev/null
echo "$PLATFORM dependabot: alerts and security updates on"

# 7. Two-factor authentication for everyone in the organization has no API.
echo
echo "By hand: require 2FA at https://github.com/organizations/$ORG/settings/security"
echo "Then check: bash $0 --check"
