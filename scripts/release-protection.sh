#!/usr/bin/env bash
# Repository and organization settings that make a CoderHelm release
# impossible to fake (docs/self-host/updates-security.md, "Who can publish").
# The workflows assume them; the code can't turn them on by itself.
#
# Run once, as an organization owner, with the GitHub CLI signed in:
#
#   bash scripts/release-protection.sh            # apply
#   bash scripts/release-protection.sh --check    # only show what is set
#
# Safe to run again: each setting is put in place by name.
set -euo pipefail

REPO="${REPO:-CoderHelm/coderhelm-platform}"
ORG="${REPO%%/*}"
# Who approves a release (GitHub logins, comma-separated).
REVIEWERS="${REVIEWERS:-nambok}"
# The checks main must pass (CI job names, .github/workflows/ci.yml).
CHECKS=("Workflow security" "Rust (workspace)" "Lambda (Python)" "MCP Proxy (Node)" "Jira app (Forge)" "Infra (config, synth)" "Self-host installer" "Launch Stack (template and installer)")

api() { gh api -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" "$@"; }

if [ "${1:-}" = "--check" ]; then
  echo "Rulesets:";            api "repos/$REPO/rulesets" --jq '.[] | "  \(.name): \(.target), \(.enforcement)"'
  echo "Environments:";        api "repos/$REPO/environments" --jq '.environments[] | "  \(.name): \([.protection_rules[].type] | join(", "))"'
  echo "Immutable releases:";  api "repos/$REPO/immutable-releases" --jq '"  enabled: \(.enabled)"' || true
  echo "Actions:";             api "repos/$REPO/actions/permissions" --jq '"  allowed: \(.allowed_actions), sha pinning required: \(.sha_pinning_required)"'
  echo "Token default:";       api "repos/$REPO/actions/permissions/workflow" --jq '"  \(.default_workflow_permissions)"'
  echo "Org 2FA required:";    api "orgs/$ORG" --jq '"  \(.two_factor_requirement_enabled)"'
  exit 0
fi

# Creates or replaces a ruleset by name.
ruleset() {
  local name="$1" body="$2" id
  id=$(api "repos/$REPO/rulesets" --jq ".[] | select(.name == \"$name\") | .id")
  if [ -n "$id" ]; then
    api -X PUT "repos/$REPO/rulesets/$id" --input - <<<"$body" >/dev/null
  else
    api -X POST "repos/$REPO/rulesets" --input - <<<"$body" >/dev/null
  fi
  echo "ruleset: $name"
}

# 1. Release tags (v*) are never moved or deleted, by anyone. No bypass:
#    changing one means turning this ruleset off first, which the audit log shows.
ruleset "release tags: never moved or deleted" '{
  "name": "release tags: never moved or deleted",
  "target": "tag",
  "enforcement": "active",
  "conditions": {"ref_name": {"include": ["refs/tags/v*"], "exclude": []}},
  "rules": [{"type": "update"}, {"type": "deletion"}, {"type": "non_fast_forward"}],
  "bypass_actors": []
}'

# 2. Only repository and organization admins create release tags, and only
#    signed ones. (The release workflow also checks the tag is signed and on main.)
ruleset "release tags: admins create, signed" '{
  "name": "release tags: admins create, signed",
  "target": "tag",
  "enforcement": "active",
  "conditions": {"ref_name": {"include": ["refs/tags/v*"], "exclude": []}},
  "rules": [{"type": "creation"}, {"type": "required_signatures"}],
  "bypass_actors": [
    {"actor_type": "OrganizationAdmin", "actor_id": 1, "bypass_mode": "always"},
    {"actor_type": "RepositoryRole", "actor_id": 5, "bypass_mode": "always"}
  ]
}'

# 3. main: changes only through a pull request with passing checks, signed
#    commits, no force pushes or deletion. Admins may merge a PR without the
#    approval (bypass_mode "pull_request"), never push to main directly.
checks_json=$(printf '%s\n' "${CHECKS[@]}" | jq -R '{context: .}' | jq -s .)
ruleset "main: pull requests, checks, signed" "$(jq -n --argjson checks "$checks_json" '{
  name: "main: pull requests, checks, signed",
  target: "branch",
  enforcement: "active",
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
  bypass_actors: [
    {actor_type: "OrganizationAdmin", actor_id: 1, bypass_mode: "pull_request"},
    {actor_type: "RepositoryRole", actor_id: 5, bypass_mode: "pull_request"}
  ]
}')"

# 4. The "release" environment: a listed reviewer approves every publish,
#    and only v* tags may deploy to it. The publisher role in AWS trusts only
#    this environment (infra/lib/worker-stack.ts).
reviewers_json=$(for u in ${REVIEWERS//,/ }; do api "users/$u" --jq '{type: "User", id: .id}'; done | jq -s .)
api -X PUT "repos/$REPO/environments/release" --input - >/dev/null <<EOF
{
  "wait_timer": 0,
  "prevent_self_review": false,
  "reviewers": $reviewers_json,
  "can_admins_bypass": false,
  "deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true}
}
EOF
if ! api "repos/$REPO/environments/release/deployment-branch-policies" --jq '.branch_policies[] | select(.type == "tag") | .name' | grep -qxF 'v*'; then
  api -X POST "repos/$REPO/environments/release/deployment-branch-policies" -f name='v*' -f type=tag >/dev/null
fi
echo "environment: release (reviewers: $REVIEWERS; v* tags only; admins can't skip it)"

# 5. Published releases can't have their files replaced, or their tag moved.
api -X PUT "repos/$REPO/immutable-releases" >/dev/null && echo "immutable releases: on"

# 6. Actions: every action must be pinned to a full SHA, the default token is
#    read-only, and workflows from outside contributors' PRs wait for approval.
api -X PUT "repos/$REPO/actions/permissions" -F enabled=true -f allowed_actions=all -F sha_pinning_required=true >/dev/null
api -X PUT "repos/$REPO/actions/permissions/workflow" -f default_workflow_permissions=read -F can_approve_pull_request_reviews=false >/dev/null
api -X PUT "repos/$REPO/actions/permissions/fork-pr-contributor-approval" -f approval_policy=all_external_contributors >/dev/null
echo "actions: SHA pinning required, read-only default token, outside PRs need approval"

# 7. Dependabot: alerts and security fixes (.github/dependabot.yml keeps the
#    pinned actions current).
api -X PUT "repos/$REPO/vulnerability-alerts" >/dev/null
api -X PUT "repos/$REPO/automated-security-fixes" >/dev/null
echo "dependabot: alerts and security updates on"

# 8. Two-factor authentication for everyone in the organization has no API:
#    https://github.com/organizations/$ORG/settings/security
#    then "Require two-factor authentication for everyone in the organization".
echo
echo "Left to do by hand: require 2FA at https://github.com/organizations/$ORG/settings/security"
echo "Then check: bash scripts/release-protection.sh --check"
