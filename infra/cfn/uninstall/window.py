# The uninstall stack's window function (infra/lib/uninstall-stack.ts inlines
# this file into infra/cfn/coderhelm-selfhost-uninstall.yaml).
#
# Gives one uninstall build its window: only a running build of this stack's
# project, started by this stack's starter, with exactly this template's
# buildspec, image, variables and values, no debug session, asking as that
# build's own session, while this stack is still being created. The window is
# the build role's inline policy "uninstall-window", bound to that session
# (aws:userid) and an end time, holding the statements of
# infra/lib/uninstall-access.ts (their sha256 pinned here) with this install's
# names filled in from what this function finds in this region: its Launch
# Stacks and their ids, its progress buckets, the stacks' buckets that are in
# this region, and every other install whose name starts with this one's
# (denied). Nothing is found here: no window.
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
NAME = r"[a-z][a-z0-9-]{0,19}"


def norm(text):
    return "\n".join(line.rstrip() for line in (text or "").strip().splitlines())


def problems(b, user_id, role_id):
    p = []
    if b.get("projectName") != E["PROJECT"]: p.append("it is not this stack's uninstall build")
    if b.get("buildStatus") != "IN_PROGRESS": p.append("it is not running")
    if not str(b.get("initiator") or "").startswith(E["STARTER_ROLE"] + "/"): p.append("this stack's starter did not start it")
    if (b.get("debugSession") or {}).get("sessionEnabled"): p.append("it has a debug session")
    if b.get("fleet") or (b.get("environment") or {}).get("fleet"): p.append("it runs on another fleet")
    src = b.get("source") or {}
    if src.get("type") != "NO_SOURCE" or b.get("secondarySources"): p.append("its source was changed")
    if hashlib.sha256(norm(src.get("buildspec")).encode()).hexdigest() != E["BUILDSPEC_SHA256"]:
        p.append("its buildspec is not this template's")
    env = b.get("environment") or {}
    if env.get("image") != E["IMAGE"] or env.get("privilegedMode") or env.get("certificate") or env.get("type") != "LINUX_CONTAINER":
        p.append("its build image was changed")
    want = json.loads(E["VALUES"])
    for v in env.get("environmentVariables") or []:
        n = v.get("name")
        if v.get("type", "PLAINTEXT") != "PLAINTEXT" or (n not in want and n != "WAIT_URL"):
            p.append("it sets " + str(n))
        elif n == "WAIT_URL":
            if v.get("value") and not str(v.get("value")).startswith("https://cloudformation-waitcondition-"): p.append("it sets WAIT_URL")
        elif v.get("value") != want[n]:
            p.append("it changes " + str(n))
    if not str(b.get("serviceRole") or "").endswith(":role/" + E["ROLE"]): p.append("it runs as another role")
    build_uuid = str(b.get("id") or "").split(":")[-1]
    if not role_id or user_id != role_id + ":AWSCodeBuild-" + build_uuid: p.append("the caller is not that build's own session")
    return p


def launch_stacks(cfn, stage):
    """This install's Launch Stacks in this region (coderhelm-<stage> or -<n>,
    with Stage=<stage>, created without a service role), and the names of
    other installs found from their Launch Stacks."""
    prefix = "coderhelm-" + stage
    found, others = {}, set()
    for page in cfn.get_paginator("list_stacks").paginate(StackStatusFilter=ALIVE):
        for s in page.get("StackSummaries", []):
            n = s["StackName"]
            if n != prefix and not n.startswith(prefix + "-"):
                continue
            d = cfn.describe_stacks(StackName=s["StackId"])["Stacks"][0]
            p = {x["ParameterKey"]: x.get("ParameterValue", "") for x in d.get("Parameters", [])}
            if "SetupToken" not in p or "AdminEmail" not in p:
                continue
            if p.get("Stage") != stage:
                if re.fullmatch(NAME, p.get("Stage", "")):
                    others.add("coderhelm-" + p["Stage"])
                continue
            if (n == prefix or re.fullmatch(re.escape(prefix) + r"-\d{1,3}", n)) and not d.get("RoleARN"):
                found[n] = {"id": d["StackId"].split("/")[2][:8], "status": p.get("StatusId", "")}
    return found, others


def region_of(s3, bucket):
    try:
        return s3.get_bucket_location(Bucket=bucket).get("LocationConstraint") or "us-east-1"
    except Exception:
        return None


def discover(stage, account, region, cfn, s3, tags, secrets, iam=None, own_status="", install_status=""):
    prefix = "coderhelm-" + stage
    launch, others = launch_stacks(cfn, stage)
    ids = {v["id"] for v in launch.values()}
    setup = {v["status"] for v in launch.values() if re.fullmatch(r"[a-z0-9]{16}", v["status"])}
    names = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    # Pieces a deleted Launch Stack kept: its assets bucket names its id.
    for b in names:
        m = re.fullmatch(re.escape(prefix) + r"-assets-([0-9a-f]{8})-" + account, b)
        if m and region_of(s3, b) == region:
            ids.add(m.group(1))
    # S3 names carry no region: only the stacks' buckets that are in this region.
    here = lambda b: b in names and region_of(s3, b) == region
    buckets = [b for b in (prefix + "-dashboard-" + account, prefix + "-sandbox-" + account) if here(b)]
    data = [b for b in (prefix + "-data-" + account,) if here(b)]
    # The install guide's progress id, when its file says it is this install's.
    if re.fullmatch(r"[a-z0-9]{16}", install_status or ""):
        try:
            doc = json.loads(s3.get_object(Bucket="coderhelm-setup-" + install_status, Key="status.json")["Body"].read())
            if doc.get("stage") == stage and doc.get("region") == region:
                setup.add(install_status)
        except Exception:
            pass
    setup.discard(own_status)
    # Installs in this region, by their tag and their secret; and others anywhere, by Launch Stack and boundary.
    tagged = set()
    kw = {"Key": "coderhelm:install"}
    while True:
        r = tags.get_tag_values(**kw)
        tagged.update(r.get("TagValues", []))
        if not r.get("PaginationToken"):
            break
        kw["PaginationToken"] = r["PaginationToken"]
    secret_here = False
    for page in secrets.get_paginator("list_secrets").paginate(Filters=[{"Key": "name", "Values": ["coderhelm/"]}]):
        for s in page.get("SecretList", []):
            parts = s["Name"].split("/")
            if len(parts) >= 3 and parts[0] == "coderhelm":
                others.add("coderhelm-" + parts[1])
                secret_here = secret_here or parts[1] == stage
    if iam is not None:
        for page in iam.get_paginator("list_policies").paginate(Scope="Local"):
            for pol in page.get("Policies", []):
                m = re.fullmatch(r"(coderhelm-" + NAME + r")-boundary-[0-9a-f]{8}", pol["PolicyName"])
                if m:
                    others.add(m.group(1))
    others |= tagged
    sib = sorted(x for x in others if x.startswith(prefix + "-") and re.fullmatch("coderhelm-" + NAME, x))
    anchored = bool(launch) or prefix in tagged or secret_here
    return {"ids": sorted(ids), "launch": sorted(launch), "setup": sorted(setup), "siblings": sib, "buckets": buckets, "data": data, "anchored": anchored}


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
        cfn = boto3.client("cloudformation")
        # Only while this stack is being created: a stack kept after its end (rollback turned off) opens nothing.
        if cfn.describe_stacks(StackName=E["STACK_ID"])["Stacks"][0].get("StackStatus") != "CREATE_IN_PROGRESS":
            return {"ok": False, "error": "the uninstall window was refused: this uninstall stack has ended"}
        builds = boto3.client("codebuild").batch_get_builds(ids=[str(event.get("build_id"))]).get("builds") or []
        role_id = iam.get_role(RoleName=E["ROLE"])["Role"]["RoleId"]
        user_id = str(event.get("user_id") or "")
        bad = problems(builds[0], user_id, role_id) if builds else ["the build was not found"]
        if bad:
            return {"ok": False, "error": "the uninstall window was refused: " + "; ".join(bad)}
        ssm = boto3.client("ssm")
        raw = [ssm.get_parameter(Name=E["PARAM"] + "/" + str(i))["Parameter"]["Value"] for i in range(1, int(E["PARTS"]) + 1)]
        if hashlib.sha256("\n".join(raw).encode()).hexdigest() != E["WINDOW_SHA256"]:
            return {"ok": False, "error": "the uninstall window was refused: its statements are not this template's"}
        found = discover(E["STAGE"], E["ACCOUNT"], E["REGION"], cfn, boto3.client("s3"), boto3.client("resourcegroupstaggingapi"),
                         boto3.client("secretsmanager"), iam, E.get("OWN_STATUS", ""), E.get("INSTALL_STATUS", ""))
        if not found["anchored"]:
            return {"ok": False, "error": "nothing of the install coderhelm-" + E["STAGE"] + " is in " + E["REGION"] + " (no Launch Stack, tag or secret): nothing was deleted. Check the install name and the region."}
        filled = [r.replace("__PARTITION__", E["PARTITION"]).replace("__ACCOUNT__", E["ACCOUNT"]).replace("__REGION__", E["REGION"])
                  .replace("__STAGE__", E["STAGE"]).replace("__UID__", E["ID"]).replace("__OWN_STATUS__", E.get("OWN_STATUS", "")) for r in raw]
        if any(re.search(r"__(PARTITION|ACCOUNT|REGION|STAGE|UID|OWN_STATUS)__", r) for r in filled):
            return {"ok": False, "error": "the uninstall window was refused: its statements name something unknown"}
        statements = [s for r in filled for s in json.loads(r)]
        if E["MODE"] != "everything":
            statements = [s for s in statements if not s["Sid"].startswith("Data")]
        until = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=100)).strftime("%Y-%m-%dT%H:%M:%SZ")
        subs = [("__USER_ID__", [user_id]), ("__EXPIRES__", [until]), ("__ID__", found["ids"]), ("__LAUNCH__", found["launch"]),
                ("__SETUP__", found["setup"]), ("__SIB__", found["siblings"]), ("__BUCKET__", found["buckets"]), ("__DATA__", found["data"])]
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
