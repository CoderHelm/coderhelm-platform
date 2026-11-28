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
    InspectorFindings, HealthEvents), the databases an AWS admin already
    chose (below), and every other parameter empty or off, so no knowledge
    base, bucket, WAF log or warehouse, no copying into Bedrock and no
    database maintenance. What a team chose in the account stays on its own
    update stacks.

The databases: the setup script an AWS admin runs in CloudShell makes the
read-only database user of each database they pick, and keeps its password
in a secret named coderhelm-readonly-<cluster> in the account. Each such
secret is that admin's choice of that cluster, so the reader names those
clusters in DatabaseClusters. Without it a data source in this account,
whose role nobody is ever asked to update, could never query its database
through this role (rds-data:BeginTransaction refused on every check). It
reads only the secrets' names (ListSecrets), never a value, and the role
still reads no secret but those users' passwords.

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
# The setup script's read-only database user's secret: coderhelm-readonly-<cluster id>
# (services/common/src/live/aws.rs, SECRET_PREFIX).
SECRET_PREFIX = "coderhelm-readonly-"
# An Aurora cluster identifier as AWS keeps it: lowercase, starts with a letter.
CLUSTER_ID_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")

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


def databases(sm, stack_id):
    """The clusters an AWS admin made a read-only user for in this account
    and region (a coderhelm-readonly-<cluster> secret each), as ARNs,
    sorted. Reads only the secrets' names."""
    parts = str(stack_id or "").split(":")
    if len(parts) < 6 or not parts[1] or not parts[3] or not parts[4]:
        return []
    partition, region, account = parts[1], parts[3], parts[4]
    out, token = set(), None
    while True:
        kw = {"Filters": [{"Key": "name", "Values": [SECRET_PREFIX]}]}
        if token:
            kw["NextToken"] = token
        page = sm.list_secrets(**kw)
        for s in page.get("SecretList") or []:
            name = str(s.get("Name") or "")
            cluster = name[len(SECRET_PREFIX):] if name.startswith(SECRET_PREFIX) else ""
            if CLUSTER_ID_RE.match(cluster):
                out.add(f"arn:{partition}:rds:{region}:{account}:cluster:{cluster}")
        token = page.get("NextToken")
        if not token:
            return sorted(out)


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


def parameters(declared, switches, clusters=()):
    """The stack's parameters: only ones the template declares, read-only groups only."""
    values = dict(FIXED, **switches)
    values["DatabaseClusters"] = ",".join(clusters)
    return [{"ParameterKey": k, "ParameterValue": values[k]} for k in sorted(declared) if k in values]


def keep(cfn=None, iam=None, root=None, sm=None):
    """Brings CoderHelmLogReader up to the running release. Returns what it did, in a few words."""
    cfn = cfn or client("cloudformation")
    iam = iam or client("iam")
    sm = sm or client("secretsmanager")
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
    try:
        clusters = databases(sm, cfn.describe_stacks(StackName=root)["Stacks"][0].get("StackId"))
    except Exception as e:  # noqa: BLE001
        # An access stack from before this read: the databases wait for its update.
        print(f"databases not read ({type(e).__name__}: {e}): none named this time", flush=True)
        clusters = []
    params = parameters(declared, switches, clusters)
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
