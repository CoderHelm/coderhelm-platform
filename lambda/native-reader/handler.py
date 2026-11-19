"""The access stack's reader update (infra/lib/native/access-stack.ts).

Every read of a team's data in a native install's own AWS account is made
as the team's CoderHelmLogReader role there, and each release may read a
little more. The install's own roles may never change IAM, so on their own
they can't bring those reads to that role. This function does, with exactly
the release's published update template and nothing else:

  - when the install's stack finishes an update (CloudFormation's stack
    status event), once a day, and when an AWS admin creates or updates the
    access stack;
  - only when the role CoderHelmLogReader exists in the account;
  - it creates or updates one stack, CoderHelmLogReaderUpdate-release, from
    https://coderhelm-public.s3.amazonaws.com/launch/<running release>/coderhelm-log-reader-update.yaml,
    the template every Update link of the AWS page opens;
  - with only the release's read-only groups: the three switches of the AWS
    card as the install's stack has them (InfrastructureVulnerabilities,
    InspectorFindings, HealthEvents) and every other parameter empty or
    off, so no knowledge base, bucket, WAF log, database or warehouse, no
    copying into Bedrock and no database maintenance. What a team chose in
    the account stays on its own update stacks.

Its role (outside the install's names, so no CoderHelm role can invoke,
pass or change it) may create and update only that stack, only from that
template URL (cloudformation:TemplateUrl), and may write only that role's
inline policies, only through CloudFormation (aws:CalledVia). It never
reads the event it is sent: every run does the same thing.

As a custom resource it always answers SUCCESS: a reader it couldn't update
never fails the admin's access stack (the next run tries again, and the AWS
page says what the role lacks).
"""

import json
import os
import re
import urllib.request

ROLE = "CoderHelmLogReader"
STACK = "CoderHelmLogReaderUpdate-release"
HOST = "coderhelm-public.s3.amazonaws.com"
TEMPLATE = "coderhelm-log-reader-update.yaml"
VERSION_RE = re.compile(r"^v\d+\.\d+\.\d+(-[0-9A-Za-z.]+)?$")
RESPONSE_URL_RE = re.compile(r"^https://cloudformation-custom-resource-response-[a-z0-9-]+\.s3(\.[a-z0-9-]+)?\.amazonaws\.com/")
# The AWS card's switches: the install's stack parameter, and its default
# when the stack has none (infra/lib/own-account-read.ts, OWN_OPTIONS).
SWITCHES = {"InfrastructureVulnerabilities": "on", "InspectorFindings": "off", "HealthEvents": "off"}
# Every other parameter of the update template: empty or off. A parameter a
# later template adds and this list doesn't know keeps the template's own
# default (each template's opt-ins default to off).
FIXED = {
    "RoleName": ROLE,
    "KnowledgeBases": "",
    "ImportBuckets": "",
    "SyncKnowledgeBases": "",
    "ApplyMaintenance": "off",
    "WafLogs": "off",
    "WafLogResults": "",
    "WafLogKey": "",
    "DatabaseClusters": "",
    "Warehouses": "",
}
# A stack in one of these can't be updated: it is removed (only this stack,
# whose one resource is this function's inline policy) and made again.
DEAD = ("ROLLBACK_COMPLETE", "ROLLBACK_FAILED", "CREATE_FAILED", "DELETE_FAILED")

_clients = {}


def client(name):
    if name not in _clients:
        import boto3
        _clients[name] = boto3.client(name)
    return _clients[name]


def _missing(e):
    text = type(e).__name__ + str(e)
    return "NoSuchEntity" in text or "does not exist" in text


def template_url(version):
    if not VERSION_RE.match(str(version or "")):
        raise ValueError(f"not a release version: {version!r}")
    return f"https://{HOST}/launch/{version}/{TEMPLATE}"


def running(cfn, root):
    """(version, switches) of the install's stack, or (None, reason) while it can't be read as finished."""
    st = cfn.describe_stacks(StackName=root)["Stacks"][0]
    status = st.get("StackStatus") or ""
    if status.endswith("_IN_PROGRESS"):
        return None, f"the install's stack is busy ({status})"
    out = {o.get("OutputKey"): o.get("OutputValue") for o in st.get("Outputs") or []}
    version = out.get("Version")
    if not VERSION_RE.match(str(version or "")):
        return None, "the install's stack names no release (its Version output)"
    have = {p.get("ParameterKey"): p.get("ParameterValue") for p in st.get("Parameters") or []}
    switches = {k: (have.get(k) if have.get(k) in ("on", "off") else d) for k, d in SWITCHES.items()}
    return version, switches


def parameters(declared, switches):
    """The stack's parameters: only ones the template declares, read-only groups only."""
    values = dict(FIXED, **switches)
    return [{"ParameterKey": k, "ParameterValue": values[k]} for k in sorted(declared) if k in values]


def keep(cfn=None, iam=None, root=None):
    """Brings CoderHelmLogReader up to the running release. Returns what it did, in a few words."""
    cfn = cfn or client("cloudformation")
    iam = iam or client("iam")
    root = root or os.environ["ROOT_STACK"]
    try:
        iam.get_role(RoleName=ROLE)
    except Exception as e:  # noqa: BLE001
        if _missing(e):
            return "no CoderHelmLogReader role in this account: nothing to update"
        raise
    version, switches = running(cfn, root)
    if version is None:
        return f"not now: {switches}"
    url = template_url(version)
    declared = [p["ParameterKey"] for p in cfn.get_template_summary(TemplateURL=url).get("Parameters") or []]
    params = parameters(declared, switches)
    try:
        st = cfn.describe_stacks(StackName=STACK)["Stacks"][0]
    except Exception as e:  # noqa: BLE001
        if not _missing(e):
            raise
        st = None
    status = (st or {}).get("StackStatus") or ""
    if status.endswith("_IN_PROGRESS"):
        return f"{STACK} is busy ({status}): the next run updates it"
    if status in DEAD:
        cfn.delete_stack(StackName=STACK)
        cfn.get_waiter("stack_delete_complete").wait(StackName=STACK, WaiterConfig={"Delay": 5, "MaxAttempts": 30})
        st = None
    args = {
        "StackName": STACK,
        "TemplateURL": url,
        "Parameters": params,
        "Capabilities": ["CAPABILITY_IAM"],
    }
    if st is None:
        cfn.create_stack(**args)
        return f"creating {STACK} from {version}'s update template"
    try:
        cfn.update_stack(**args)
    except Exception as e:  # noqa: BLE001
        if "No updates are to be performed" in str(e):
            return f"{ROLE} already has {version}'s reads"
        raise
    return f"updating {STACK} to {version}'s update template"


def respond(event, status, reason):
    body = json.dumps({
        "Status": status, "Reason": (reason or status)[:3500],
        "PhysicalResourceId": event.get("PhysicalResourceId") or "reader-update",
        "StackId": event["StackId"], "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"], "Data": {},
    }).encode()
    if not RESPONSE_URL_RE.match(str(event.get("ResponseURL", ""))):
        print("not answering: the response URL is not CloudFormation's", flush=True)
        return
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT", headers={"Content-Type": "", "Content-Length": str(len(body))})
    urllib.request.urlopen(req, timeout=30).read()


def handler(event, context=None, **kw):
    custom = isinstance(event, dict) and "ResponseURL" in event and "RequestType" in event
    if custom and event.get("RequestType") == "Delete":
        reason = "nothing changed: the reader keeps what it has"
    else:
        try:
            reason = keep(**kw)
        except Exception as e:  # noqa: BLE001
            reason = f"not updated ({type(e).__name__}: {e}); the next run tries again"
    print(reason, flush=True)
    if custom:
        respond(event, "SUCCESS", reason)
    return {"result": reason}
