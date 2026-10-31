#!/usr/bin/env bash
# Rewrites the uninstall stack (infra/cfn/coderhelm-selfhost-uninstall.yaml)
# from infra/lib/uninstall-stack.ts. Run from infra/ after `npx tsc`.
set -euo pipefail
node dist/lib/uninstall-stack.js > cfn/coderhelm-selfhost-uninstall.yaml
