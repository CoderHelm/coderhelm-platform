"""The access stack's one custom resource (infra/lib/native/access-stack.ts).

A native install's parent stack runs every update as its deploy role, and
the boundary forbids that role to change itself or a boundary. So a newer
boundary or deploy policy reaches an install only through this stack:
coderhelm-<name>-access, created by an AWS admin from the release's
access.json with the admin's own permissions (the stack has no service
role, and no CoderHelm role may create, change or delete it:
TheAccessStackStays).

On Create and Update it writes the documents the template gives it:
  - each boundary (Boundary, DeployBoundary): a new default policy version
    when the document differs (the oldest non-default version goes first
    when IAM's five are used up);
  - the deploy role's inline policy "deploy", when it differs.
On Delete it changes nothing: the documents stay as they are.

Its code is inline in the signed access.json; its role is the only one that
may write these three documents, and it carries no CoderHelm boundary.
"""

import json
import re
import urllib.request

RESPONSE_URL_RE = re.compile(r"^https://cloudformation-custom-resource-response-[a-z0-9-]+\.s3(\.[a-z0-9-]+)?\.amazonaws\.com/")
LISTS = ("Action", "NotAction", "Resource", "NotResource")

_iam = None


def iam():
    global _iam
    if _iam is None:
        import boto3
        _iam = boto3.client("iam")
    return _iam


def canonical(doc):
    """A policy document as IAM means it: one value or a list of one are the
    same, and the order of actions or resources doesn't matter."""
    if isinstance(doc, str):
        doc = json.loads(doc)

    def walk(v, key=None):
        if isinstance(v, dict):
            return {k: walk(x, k) for k, x in sorted(v.items())}
        if isinstance(v, list):
            items = [walk(x) for x in v]
            if key in LISTS:
                return sorted(items, key=lambda x: json.dumps(x, sort_keys=True))
            return items
        if key in LISTS and isinstance(v, str):
            return [v]
        return v

    out = walk(doc)
    if isinstance(out, dict):
        sts = out.get("Statement")
        out["Statement"] = sts if isinstance(sts, list) else ([sts] if sts else [])
        for st in out["Statement"]:
            for op in (st.get("Condition") or {}).values():
                for k, val in list(op.items()):
                    op[k] = sorted(val) if isinstance(val, list) else [val]
        # The order of statements doesn't matter either.
        out["Statement"] = sorted(out["Statement"], key=lambda x: json.dumps(x, sort_keys=True))
    return json.dumps(out, sort_keys=True, separators=(",", ":"))


def same(a, b):
    return canonical(a) == canonical(b)


def write_policy(arn, doc, client=None):
    """The policy's default version becomes `doc` (False when it already is)."""
    c = client or iam()
    current = c.get_policy(PolicyArn=arn)["Policy"]["DefaultVersionId"]
    have = c.get_policy_version(PolicyArn=arn, VersionId=current)["PolicyVersion"]["Document"]
    if same(have, doc):
        return False
    versions = c.list_policy_versions(PolicyArn=arn)["Versions"]
    if len(versions) >= 5:
        old = sorted((v for v in versions if not v.get("IsDefaultVersion")), key=lambda v: str(v.get("CreateDate")))
        c.delete_policy_version(PolicyArn=arn, VersionId=old[0]["VersionId"])
    c.create_policy_version(PolicyArn=arn, PolicyDocument=json.dumps(doc), SetAsDefault=True)
    return True


def write_role_policy(role, name, doc, client=None):
    c = client or iam()
    try:
        have = c.get_role_policy(RoleName=role, PolicyName=name)["PolicyDocument"]
    except Exception as e:
        if "NoSuchEntity" not in type(e).__name__ + str(e):
            raise
        have = None
    if have is not None and same(have, doc):
        return False
    c.put_role_policy(RoleName=role, PolicyName=name, PolicyDocument=json.dumps(doc))
    return True


def apply(props, client=None):
    """Writes what differs; returns the names written."""
    done = []
    for key in ("DeployBoundary", "Boundary"):
        p = props.get(key) or {}
        if p.get("Arn") and p.get("Document") and write_policy(p["Arn"], p["Document"], client):
            done.append(key)
    r = props.get("DeployRole") or {}
    if r.get("RoleName") and r.get("Document") and write_role_policy(r["RoleName"], r.get("PolicyName") or "deploy", r["Document"], client):
        done.append("DeployRole")
    return done


def respond(event, status, reason="", data=None):
    body = json.dumps({
        "Status": status, "Reason": (reason or status)[:3500],
        "PhysicalResourceId": event.get("PhysicalResourceId") or "access",
        "StackId": event["StackId"], "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"], "Data": data or {},
    }).encode()
    if not RESPONSE_URL_RE.match(str(event.get("ResponseURL", ""))):
        print("not answering: the response URL is not CloudFormation's", flush=True)
        return
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT", headers={"Content-Type": "", "Content-Length": str(len(body))})
    urllib.request.urlopen(req, timeout=30).read()


def handler(event, context=None):
    status, reason, data = "SUCCESS", "", {}
    try:
        if event.get("RequestType") in ("Create", "Update"):
            done = apply(event.get("ResourceProperties") or {})
            data = {"Written": ",".join(done) or "none"}
            reason = "written: " + (", ".join(done) or "nothing (already as this release has them)")
        else:
            reason = "nothing changed: the documents stay as they are"
        print(reason, flush=True)
    except Exception as e:
        status, reason = "FAILED", f"{type(e).__name__}: {e}"
        print(reason, flush=True)
    respond(event, status, reason, data)
