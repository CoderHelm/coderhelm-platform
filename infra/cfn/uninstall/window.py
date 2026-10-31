# The uninstall stack's window function (infra/lib/uninstall-stack.ts inlines
# this file into infra/cfn/coderhelm-selfhost-uninstall.yaml).
#
# Gives one uninstall build its window: only a running build of this stack's
# project, with exactly this template's buildspec, image and variables,
# asking as itself. The window is the build role's inline policy
# "uninstall-window", bound to that build's session (aws:userid) and an end
# time, holding the statements of infra/lib/uninstall-access.ts with this
# install's names filled in from what this function finds in this region:
# its Launch Stacks and their ids, its progress buckets, and every other
# install whose name starts with this one's (denied).
#
# MODE and STAGE are this stack's own parameters: what the build asks with
# never changes them. lambda/tests/test_uninstall_window.py tests this file.
import datetime, hashlib, json, os, re
import boto3

E = os.environ
MAX_WINDOW = 9400
ALIVE = ["CREATE_COMPLETE", "CREATE_FAILED", "ROLLBACK_COMPLETE", "ROLLBACK_FAILED", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
         "UPDATE_ROLLBACK_FAILED", "DELETE_FAILED", "IMPORT_COMPLETE", "IMPORT_ROLLBACK_COMPLETE", "UPDATE_FAILED", "CREATE_IN_PROGRESS",
         "UPDATE_IN_PROGRESS", "ROLLBACK_IN_PROGRESS", "UPDATE_ROLLBACK_IN_PROGRESS", "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
         "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS"]


def norm(text):
    return "\n".join(line.rstrip() for line in (text or "").strip().splitlines())


def problems(b, user_id, role_id):
    p = []
    if b.get("projectName") != E["PROJECT"]: p.append("it is not this stack's uninstall build")
    if b.get("buildStatus") != "IN_PROGRESS": p.append("it is not running")
    src = b.get("source") or {}
    if src.get("type") != "NO_SOURCE" or b.get("secondarySources"): p.append("its source was changed")
    if hashlib.sha256(norm(src.get("buildspec")).encode()).hexdigest() != E["BUILDSPEC_SHA256"]:
        p.append("its buildspec is not this template's")
    env = b.get("environment") or {}
    if env.get("image") != E["IMAGE"] or env.get("privilegedMode"): p.append("its build image was changed")
    allowed = set(E["VARIABLES"].split(","))
    for v in env.get("environmentVariables") or []:
        if v.get("name") not in allowed or v.get("type", "PLAINTEXT") != "PLAINTEXT":
            p.append("it sets " + str(v.get("name")))
    if not str(b.get("serviceRole") or "").endswith(":role/" + E["ROLE"]): p.append("it runs as another role")
    if not re.match(r"^" + re.escape(role_id) + r":[\w+=,.@-]{2,64}$", user_id): p.append("the caller is not the uninstall build's role")
    return p


def launch_stacks(cfn, stage):
    """This install's Launch Stacks in this region: named coderhelm-<stage> or
    coderhelm-<stage>-<n> (the install guide's names) with Stage=<stage>."""
    prefix = "coderhelm-" + stage
    found = {}
    for page in cfn.get_paginator("list_stacks").paginate(StackStatusFilter=ALIVE):
        for s in page.get("StackSummaries", []):
            n = s["StackName"]
            if n != prefix and not re.fullmatch(re.escape(prefix) + r"-\d{1,3}", n):
                continue
            d = cfn.describe_stacks(StackName=s["StackId"])["Stacks"][0]
            p = {x["ParameterKey"]: x.get("ParameterValue", "") for x in d.get("Parameters", [])}
            if p.get("Stage") == stage and "SetupToken" in p and "AdminEmail" in p:
                found[n] = {"id": d["StackId"].split("/")[2][:8], "status": p.get("StatusId", "")}
    return found


def region_of(s3, bucket):
    try:
        return s3.get_bucket_location(Bucket=bucket).get("LocationConstraint") or "us-east-1"
    except Exception:
        return None


def discover(stage, account, region, cfn, s3, tags, secrets, own_status="", install_status=""):
    prefix = "coderhelm-" + stage
    launch = launch_stacks(cfn, stage)
    ids = {v["id"] for v in launch.values()}
    setup = {v["status"] for v in launch.values() if re.fullmatch(r"[a-z0-9]{16}", v["status"])}
    # Pieces a deleted Launch Stack kept: its assets bucket names its id.
    for b in s3.list_buckets().get("Buckets", []):
        m = re.fullmatch(re.escape(prefix) + r"-assets-([0-9a-f]{8})-" + account, b["Name"])
        if m and region_of(s3, b["Name"]) == region:
            ids.add(m.group(1))
    # The install guide's progress id, when its file says it is this install's.
    if re.fullmatch(r"[a-z0-9]{16}", install_status or ""):
        try:
            doc = json.loads(s3.get_object(Bucket="coderhelm-setup-" + install_status, Key="status.json")["Body"].read())
            if doc.get("stage") == stage and doc.get("region") == region:
                setup.add(install_status)
        except Exception:
            pass
    setup.discard(own_status)
    # Other installs whose names start with this one's (coderhelm-prod-eu inside coderhelm-prod-*).
    names = set()
    kw = {"Key": "coderhelm:install"}
    while True:
        r = tags.get_tag_values(**kw)
        names.update(r.get("TagValues", []))
        if not r.get("PaginationToken"):
            break
        kw["PaginationToken"] = r["PaginationToken"]
    for page in secrets.get_paginator("list_secrets").paginate(Filters=[{"Key": "name", "Values": ["coderhelm/"]}]):
        for s in page.get("SecretList", []):
            parts = s["Name"].split("/")
            if len(parts) >= 3 and parts[0] == "coderhelm":
                names.add("coderhelm-" + parts[1])
    sib = sorted(x for x in names if x.startswith(prefix + "-") and re.fullmatch(r"coderhelm-[a-z][a-z0-9-]{0,19}", x))
    return {"ids": sorted(ids), "launch": sorted(launch), "setup": sorted(setup), "siblings": sib}


def expand(statements, subs):
    """Each string with a token becomes one per value (infra/lib/uninstall-access.ts expandWindow)."""
    def many(s):
        for t, vals in subs:
            if t in s:
                return [z for v in vals for z in many(s.replace(t, v))]
        return [s]

    def ex(x):
        if isinstance(x, list):
            return [z for y in x for z in (many(y) if isinstance(y, str) else [ex(y)])]
        if isinstance(x, str):
            m = many(x)
            return x if m == [x] else m
        if isinstance(x, dict):
            return {k: ex(v) for k, v in x.items()}
        return x

    def empty(x):
        if isinstance(x, list):
            return len(x) == 0
        return isinstance(x, dict) and any(empty(v) for v in x.values())

    out = []
    for s in statements:
        s = ex(s)
        if empty(s.get("Resource", s.get("NotResource"))) or empty(s.get("Condition", {})):
            continue
        out.append(s)
    return out


def handler(event, context):
    try:
        iam = boto3.client("iam")
        builds = boto3.client("codebuild").batch_get_builds(ids=[str(event.get("build_id"))]).get("builds") or []
        role_id = iam.get_role(RoleName=E["ROLE"])["Role"]["RoleId"]
        user_id = str(event.get("user_id") or "")
        bad = problems(builds[0], user_id, role_id) if builds else ["the build was not found"]
        if bad:
            return {"ok": False, "error": "the uninstall window was refused: " + "; ".join(bad)}
        found = discover(E["STAGE"], E["ACCOUNT"], E["REGION"], boto3.client("cloudformation"), boto3.client("s3"),
                         boto3.client("resourcegroupstaggingapi"), boto3.client("secretsmanager"), E.get("OWN_STATUS", ""), E.get("INSTALL_STATUS", ""))
        ssm = boto3.client("ssm")
        statements = []
        for i in range(1, int(E["PARTS"]) + 1):
            statements += json.loads(ssm.get_parameter(Name=E["PARAM"] + "/" + str(i))["Parameter"]["Value"])
        if E["MODE"] != "everything":
            statements = [s for s in statements if not s["Sid"].startswith("Data")]
        until = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=100)).strftime("%Y-%m-%dT%H:%M:%SZ")
        subs = [("__USER_ID__", [user_id]), ("__EXPIRES__", [until]), ("__ID__", found["ids"]), ("__LAUNCH__", found["launch"]),
                ("__SETUP__", found["setup"]), ("__SIB__", found["siblings"])]
        doc = json.dumps({"Version": "2012-10-17", "Statement": expand(statements, subs)}, separators=(",", ":"))
        # A role's inline policies share 10,240 characters; the idle policy takes the rest.
        if len(doc) > MAX_WINDOW:
            return {"ok": False, "error": "this install has more pieces than one window can hold (" + str(len(doc)) + " characters): nothing was deleted. "
                    "Remove it from a terminal with: npx -y github:CoderHelm/coderhelm-platform uninstall --everything (docs/self-host/uninstall.md)"}
        iam.put_role_policy(RoleName=E["ROLE"], PolicyName="uninstall-window", PolicyDocument=doc)
        return {"ok": True, "until": until, "found": found}
    except Exception as e:
        # AWS's own words: the action and resource it refused.
        return {"ok": False, "error": str(e)[:500]}
