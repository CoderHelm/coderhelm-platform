#!/usr/bin/env bash
# Rewrites the Launch Stack's generated access block (and the counts that
# follow it) from infra/lib/access.ts. Run from infra/ after `npx tsc`.
set -euo pipefail
node dist/lib/launch-access.js > /tmp/launch-access.yaml
python3 - "$@" <<'PY'
import re
p = "cfn/coderhelm-selfhost-launch.yaml"
s = open(p).read()
blk = open("/tmp/launch-access.yaml").read().rstrip("\n")
start = s.index("  # ── Generated from infra/lib/access.ts")
end = s.index("  # ── End of the generated block ──") + len("  # ── End of the generated block ──")
s = s[:start] + blk + s[end:]
parts = sorted(set(re.findall(r"^  (WindowPolicy\d+):", s, re.M)), key=lambda x: int(x[12:]))
s = re.sub(r"DependsOn: \[WindowFunction, DeployRole, AssetsBucket(, WindowPolicy\d+)*\]", "DependsOn: [WindowFunction, DeployRole, AssetsBucket, " + ", ".join(parts) + "]", s)
s = re.sub(r'(          PARTS: )"\d+"', r'\g<1>"%d"' % len(parts), s)
open(p, "w").write(s)
PY
# The window function's copy of the installer's buildspec hash and variables.
node - <<'JS'
const cdk = require("aws-cdk-lib");
const { CfnInclude } = require("aws-cdk-lib/cloudformation-include");
const { Template } = require("aws-cdk-lib/assertions");
const crypto = require("crypto");
const fs = require("fs");
const f = "cfn/coderhelm-selfhost-launch.yaml";
const st = new cdk.Stack(new cdk.App(), "L");
new CfnInclude(st, "T", { templateFile: f });
const p = Template.fromStack(st).toJSON().Resources.InstallerProject.Properties;
const norm = (x) => x.trim().split(/\r?\n/).map((l) => l.replace(/\s+$/, "")).join("\n");
const sha = crypto.createHash("sha256").update(norm(p.Source.BuildSpec)).digest("hex");
const vars = p.Environment.EnvironmentVariables.map((v) => v.Name).join(",");
let s = fs.readFileSync(f, "utf8");
s = s.replace(/(BUILDSPEC_SHA256: )"[0-9a-f]{64}"/, `$1"${sha}"`).replace(/(          VARIABLES: )"[A-Z0-9_,]+"/, `$1"${vars}"`);
fs.writeFileSync(f, s);
JS
