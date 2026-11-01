"""The custom resources of a native CoderHelm install's parent stack.

One function backs every custom resource in coderhelm.json (infra/lib/native/
parent.ts). Its code is inline in the parent template, so it is covered by
the release's signed SHA256SUMS like the template itself, and needs no bucket.

Resource types (event["ResourceType"]):

  Custom::ReleaseAssets  copies the release's child templates and code into
                         the install's own assets bucket, each checked
                         against the sha256 the template lists. Nothing that
                         doesn't match is written. On delete: empties the
                         assets bucket (it holds no data).
  Custom::SetupKey       stores the one-time setup key in the install's
                         secret, only while no GitHub App is stored.
  Custom::Allowlist      the first admin, the sign-up domain and two-factor
                         on the settings table's allowlist (added, never removed).
  Custom::Domain         asks the updater for the certificate of the Hostname
                         launch parameter (domain_set).
  Custom::Status         the install guide's progress file (phase, message,
                         URL); on a failed create, CloudFormation's own reason.
  Custom::HybridLeave    on stack delete, releases the CoderHelm address and
                         links of a registered install.
  Custom::Sweep          on stack delete with KeepDataOnDelete=false, or with
                         the admin's "delete everything" switch on (Settings,
                         System, Uninstall), or a rolled-back first launch:
                         empties the data buckets, and deletes the tables
                         itself, retrying and waiting, so CloudFormation's
                         GlobalTable delete finds nothing left to fail on.
                         With the switch on and KeepDataOnDelete=true the
                         stack retains its data, so it also deletes the data
                         bucket and the user pool itself.
  Custom::Final          deleted after every part: the certificates the updater
                         requested for this install (and their validation
                         records); the exact log groups the parts declare
                         (Lambda recreates one when a function's last logs
                         arrive after CloudFormation deleted it); with
                         KeepDataOnDelete=false, the other log groups Lambda
                         made by itself, and the install's secret (after
                         HybridLeave and SetupKey, which read it). With the
                         switch on and KeepDataOnDelete=true (the stack
                         retains its keys), schedules the install's KMS keys
                         for deletion (AWS's 7 days) and removes their
                         aliases; on every stack delete, the switch itself.

Called directly (not by CloudFormation), {"action": "stage", "version",
"template_sha256"} from the updater: copies that release's child templates
into the install's assets bucket before it proposes the update, so
CloudFormation can read them while it builds the change set with the nested
stacks. The helper reads the release's coderhelm.json itself from the
release location, checks it against the sha256 (from the signed
SHA256SUMS), and copies only the files that template lists, each checked.

Delete-time work runs only when the ROOT stack is being deleted (or its first
create is rolling back), never when an update removes or replaces one of
these resources. Every step and every AWS error is printed (CloudWatch) and
the reason goes back to CloudFormation, which shows it in the stack's events.
"""

import base64
import hashlib
import hmac
import json
import os
import re
import tempfile
import threading
import time
import urllib.request

RELEASE_HOST = "coderhelm-public.s3.amazonaws.com"
RELEASE_FILE = "coderhelm.json"
VERSION_RE = re.compile(r"^v\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
SETUP_TOKEN_RE = re.compile(r"^[A-Za-z0-9]{32,64}$")
INSTALL_ID_RE = re.compile(r"^ins_[a-z0-9]{24}$")

_clients = {}


def client(name, region=None):
    key = f"{name}@{region}"
    if key not in _clients:
        import boto3
        _clients[key] = boto3.client(name, region_name=region) if region else boto3.client(name)
    return _clients[key]


def log(*parts):
    print(" ".join(str(p) for p in parts), flush=True)


# ── CloudFormation ─────────────────────────────────────────────────────────

RESPONSE_URL_RE = re.compile(r"^https://cloudformation-custom-resource-response-[a-z0-9-]+\.s3(\.[a-z0-9-]+)?\.amazonaws\.com/")


def respond(event, status, reason="", data=None, physical=None, put=None):
    body = json.dumps({
        "Status": status,
        "Reason": (reason or status)[:3500],
        "PhysicalResourceId": physical or event.get("PhysicalResourceId") or event["LogicalResourceId"],
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": data or {},
    }).encode()
    if put:
        return put(event["ResponseURL"], body)
    if not RESPONSE_URL_RE.match(str(event.get("ResponseURL", ""))):
        # Only CloudFormation's own presigned answer URL; anything else is not a stack asking.
        log("not answering: the response URL is not CloudFormation's")
        return None
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT",
                                 headers={"Content-Type": "", "Content-Length": str(len(body))})
    urllib.request.urlopen(req, timeout=30).read()


def root_stack(props, cfn=None):
    """The root stack as CloudFormation describes it ({} when it can't be read)."""
    root = props.get("RootStackId")
    if not root:
        return {}
    try:
        return (cfn or client("cloudformation")).describe_stacks(StackName=root)["Stacks"][0]
    except Exception as e:
        log("could not read the root stack:", e)
        return {}


def root_status(props, cfn=None):
    """The root stack's status ("" when it can't be read)."""
    return root_stack(props, cfn).get("StackStatus", "")


def tearing_down(props, cfn=None, stack=None):
    """"delete" while the root stack is deleted, "rollback" while its first
    create rolls back, else None (an update: never clean anything)."""
    st = (stack if stack is not None else root_stack(props, cfn)).get("StackStatus", "")
    if st == "DELETE_IN_PROGRESS":
        return "delete"
    if st == "ROLLBACK_IN_PROGRESS":
        return "rollback"
    return None


def stack_keeps_data(props, stack):
    """KeepDataOnDelete as the root stack itself has it (never only what the
    event says)."""
    params = {p.get("ParameterKey"): p.get("ParameterValue") for p in stack.get("Parameters") or []}
    value = params["KeepDataOnDelete"] if "KeepDataOnDelete" in params else props.get("KeepData", "true")
    return str(value).lower() != "false"


def delete_switch(props, ssm=None):
    """The admin's "delete everything when uninstalled" switch (Settings,
    System, Uninstall; the updater writes it): True only when the
    parameter says "delete". Missing or unreadable: keep."""
    name = props.get("DeleteSwitch")
    if not name:
        return False
    try:
        value = (ssm or client("ssm")).get_parameter(Name=name)["Parameter"]["Value"]
    except Exception as e:
        if "ParameterNotFound" not in type(e).__name__ + str(e):
            log("could not read the uninstall switch (data is kept):", e)
        return False
    return str(value).strip().lower() == "delete"


def keep_data(props, cfn=None, stack=None, ssm=None):
    """Whether the data stays: data and the secret go only when the stack
    says KeepDataOnDelete=false or the admin turned the switch on."""
    stack = stack if stack is not None else root_stack(props, cfn)
    if not stack:
        return True  # unreadable: keep
    if not stack_keeps_data(props, stack):
        return False
    return not delete_switch(props, ssm)


def retained_but_deleted(props, cfn=None, stack=None, ssm=None):
    """The switch says delete while the stack keeps its data (its
    DeletionPolicy retains the tables, buckets, keys and user pool): the
    helper deletes what CloudFormation will leave."""
    stack = stack if stack is not None else root_stack(props, cfn)
    return bool(stack) and stack_keeps_data(props, stack) and delete_switch(props, ssm)


def first_failure(root, cfn=None):
    try:
        events = (cfn or client("cloudformation")).describe_stack_events(StackName=root)["StackEvents"]
    except Exception as e:
        return f"could not read the stack's events: {e}"
    failed = [e for e in events if str(e.get("ResourceStatus", "")).endswith("_FAILED")
              and "cancelled" not in str(e.get("ResourceStatusReason", ""))]
    if not failed:
        return ""
    e = failed[-1]
    return f"{e.get('LogicalResourceId')}: {e.get('ResourceStatusReason') or e.get('ResourceStatus')}"


# ── Release assets ─────────────────────────────────────────────────────────

def b64(sha_hex):
    return base64.b64encode(bytes.fromhex(sha_hex)).decode()


def release_url(base, key):
    base = base.rstrip("/")
    if not base.startswith(f"https://{RELEASE_HOST}/launch/v"):
        raise ValueError(f"not a CoderHelm release location: {base}")
    if ".." in key or key.startswith("/"):
        raise ValueError(f"bad file name {key}")
    return f"{base}/{key}"


def download_verified(url, want, opener=None):
    """Downloads to a temporary file while hashing; returns the path only when
    the sha256 matches."""
    h = hashlib.sha256()
    f = tempfile.NamedTemporaryFile(delete=False, dir="/tmp")
    req = urllib.request.Request(url, headers={"User-Agent": "coderhelm-install"})
    with (opener or urllib.request.urlopen)(req, timeout=120) as r:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
            f.write(chunk)
    f.close()
    got = h.hexdigest()
    if got != want:
        os.unlink(f.name)
        raise ValueError(f"{url} has sha256 {got}, not {want} as the signed template lists: refused")
    return f.name


def copy_assets(props, s3=None, opener=None, workers=6):
    s3 = s3 or client("s3")
    bucket = props["Bucket"]
    files = props.get("Files") or []
    errors = []
    copied = []
    lock = threading.Lock()
    queue = list(files)

    def present(key, sha):
        """Already there, by the SHA-256 S3 computed itself on upload (an
        uploader can't set it to anything but its content's)."""
        try:
            head = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
            return head.get("ChecksumSHA256") == b64(sha)
        except Exception:
            return False

    def work():
        while True:
            with lock:
                if not queue:
                    return
                item = queue.pop()
            key, sha = item["Key"], item["Sha256"]
            dest = item.get("Dest") or key
            try:
                if present(dest, sha):
                    continue
                path = download_verified(release_url(props["BaseUrl"], key), sha, opener)
                try:
                    with open(path, "rb") as fh:
                        s3.put_object(Bucket=bucket, Key=dest, Body=fh, ChecksumSHA256=b64(sha),
                                      ContentType="application/json" if key.endswith(".json") else "application/zip")
                finally:
                    os.unlink(path)
                with lock:
                    copied.append(key)
                log("copied and verified", key)
            except Exception as e:
                with lock:
                    errors.append(f"{key}: {e}")
                log("FAILED", key, e)

    threads = [threading.Thread(target=work) for _ in range(max(1, min(workers, len(files))))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError("release files not copied: " + "; ".join(errors))
    return {"Copied": str(len(copied)), "Files": str(len(files))}


def prune_assets(props, old, s3=None):
    """Keeps the files this version and the previous one list (a rollback
    needs the previous one); deletes the rest."""
    s3 = s3 or client("s3")
    def dests(p):
        return {f.get("Dest") or f["Key"] for f in ((p or {}).get("Files") or [])}
    keep = dests(props) | dests(old)
    gone = []
    for key in list_keys(s3, props["Bucket"]):
        if key not in keep:
            s3.delete_object(Bucket=props["Bucket"], Key=key)
            gone.append(key)
    if gone:
        log("removed files no version in use lists:", len(gone))
    return gone


def stage_release(event, bucket, s3=None, opener=None):
    """The updater's step before a change set: the child templates of
    `version` into the assets bucket, from its verified coderhelm.json."""
    version = str(event.get("version") or "")
    want = str(event.get("template_sha256") or "").lower()
    if not VERSION_RE.match(version):
        raise ValueError(f"not a release version: {version!r}")
    if not SHA_RE.match(want):
        raise ValueError("the template's sha256 is missing")
    if not bucket:
        raise ValueError("no assets bucket")
    base = f"https://{RELEASE_HOST}/launch/{version}"
    path = download_verified(release_url(base, RELEASE_FILE), want, opener)
    try:
        with open(path, "rb") as fh:
            template = json.load(fh)
    finally:
        os.unlink(path)
    props = ((template.get("Resources") or {}).get("ReleaseAssets") or {}).get("Properties") or {}
    if props.get("Version") != version or str(props.get("BaseUrl", "")).rstrip("/") != base:
        raise ValueError(f"{version}'s template lists another release's files: refused")
    prefix = f"releases/{version}/stacks/"
    child = re.compile(r"^stacks/([A-Za-z0-9_-]+)\.json$")
    files = []
    for f in props.get("Files") or []:
        if not str(f.get("Dest", "")).startswith(prefix):
            continue
        m = child.match(str(f.get("Key", "")))
        if not m or f.get("Dest") != f"{prefix}{m.group(1)}.json" or not SHA_RE.match(str(f.get("Sha256", ""))):
            raise ValueError(f"{version}'s template lists an odd part template: {f.get('Key')!r} -> {f.get('Dest')!r}")
        files.append(f)
    if not files:
        raise ValueError(f"{version}'s template lists no child templates")
    data = copy_assets({"Bucket": bucket, "BaseUrl": base, "Files": files}, s3=s3, opener=opener)
    return {"ok": True, "version": version, "staged": int(data["Files"]), "copied": int(data["Copied"])}


def delete_secret(secret_id, sm=None):
    """The install's secret, without the recovery window (CloudFormation's
    own delete does the same); one already gone is fine."""
    sm = sm or client("secretsmanager")
    try:
        sm.delete_secret(SecretId=secret_id, ForceDeleteWithoutRecovery=True)
        log("deleted the install's secret")
        return True
    except Exception as e:
        if "ResourceNotFound" in type(e).__name__ + str(e):
            return False
        raise


def list_keys(s3, bucket):
    keys = []
    token = None
    while True:
        kw = {"Bucket": bucket}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in r.get("Contents") or []]
        if not r.get("IsTruncated"):
            return keys
        token = r.get("NextContinuationToken")


def empty_bucket(bucket, s3=None):
    """Every object version and delete marker; a bucket that is already gone is fine."""
    s3 = s3 or client("s3")
    removed = 0
    try:
        kw = {"Bucket": bucket}
        while True:
            r = s3.list_object_versions(**kw)
            items = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in (r.get("Versions") or []) + (r.get("DeleteMarkers") or [])]
            for i in range(0, len(items), 1000):
                out = s3.delete_objects(Bucket=bucket, Delete={"Objects": items[i:i + 1000], "Quiet": True})
                if out.get("Errors"):
                    raise RuntimeError(f"{bucket}: " + "; ".join(f"{e.get('Key')}: {e.get('Message')}" for e in out["Errors"][:5]))
                removed += len(items[i:i + 1000])
            if not r.get("IsTruncated"):
                break
            kw = {"Bucket": bucket, "KeyMarker": r.get("NextKeyMarker"), "VersionIdMarker": r.get("NextVersionIdMarker")}
    except Exception as e:
        if "NoSuchBucket" in str(e) or "NoSuchBucket" in type(e).__name__:
            log(bucket, "is already gone")
            return 0
        raise
    log("emptied", bucket, f"({removed} objects)")
    return removed


def delete_bucket(bucket, s3=None):
    """Empties and deletes a bucket the stack retains; one already gone is fine."""
    s3 = s3 or client("s3")
    empty_bucket(bucket, s3=s3)
    try:
        s3.delete_bucket(Bucket=bucket)
        log("deleted bucket", bucket)
        return True
    except Exception as e:
        if "NoSuchBucket" in type(e).__name__ + str(e):
            return False
        raise


def delete_user_pool(pool_id, cognito=None):
    """The install's user pool the stack retains: deletion protection off,
    its domains removed, then the pool. Only a pool tagged for this install
    (the role's policy says so too); one already gone is fine."""
    cognito = cognito or client("cognito-idp")
    try:
        p = cognito.describe_user_pool(UserPoolId=pool_id)["UserPool"]
    except Exception as e:
        if "ResourceNotFound" in type(e).__name__ + str(e):
            return False
        raise
    if p.get("DeletionProtection") == "ACTIVE":
        cognito.update_user_pool(UserPoolId=pool_id, DeletionProtection="INACTIVE")
    for d in [p.get("Domain"), p.get("CustomDomain")]:
        if d:
            cognito.delete_user_pool_domain(Domain=d, UserPoolId=pool_id)
    cognito.delete_user_pool(UserPoolId=pool_id)
    log("deleted user pool", pool_id)
    return True


def schedule_keys(alias_prefix, kms=None, days=7):
    """The install's KMS keys (behind its aliases alias/coderhelm-<stage>-...)
    scheduled for deletion after AWS's shortest wait, and their aliases
    removed. A key already pending deletion is left as it is."""
    kms = kms or client("kms")
    done, problems = [], []
    if not str(alias_prefix).startswith("alias/coderhelm-"):
        return done, [f"{alias_prefix}: not an install's aliases"]
    for page in kms.get_paginator("list_aliases").paginate():
        for a in page.get("Aliases") or []:
            name = a.get("AliasName", "")
            if not name.startswith(alias_prefix) or not a.get("TargetKeyId"):
                continue
            try:
                kms.delete_alias(AliasName=name)
                kms.schedule_key_deletion(KeyId=a["TargetKeyId"], PendingWindowInDays=days)
                done.append(name)
                log("scheduled for deletion:", name)
            except Exception as e:
                if "KMSInvalidState" in type(e).__name__ + str(e):
                    continue
                problems.append(f"key {name}: {e}")
    return done, problems


def delete_switch_parameter(name, ssm=None):
    if not name:
        return
    try:
        (ssm or client("ssm")).delete_parameter(Name=name)
        log("removed the uninstall switch")
    except Exception as e:
        if "ParameterNotFound" not in type(e).__name__ + str(e):
            log("uninstall switch left:", e)


# ── Tables ─────────────────────────────────────────────────────────────────

RETRYABLE = ("ResourceInUseException", "LimitExceededException", "ThrottlingException", "ProvisionedThroughputExceeded", "InternalServerError")


def delete_tables(names, ddb=None, deadline=None, sleep=time.sleep, now=time.monotonic):
    """Deletes each table and waits until it is gone. Retries what AWS says
    is temporary (in use, throttled, a replica still settling) with backoff.
    Returns the tables still there at the deadline, each with AWS's reason."""
    ddb = ddb or client("dynamodb")
    deadline = deadline or (now() + 600)
    left = {n: "" for n in names if n}
    asked = set()
    delay = 2
    while left and now() < deadline:
        for name in list(left):
            try:
                d = ddb.describe_table(TableName=name)["Table"]
            except Exception as e:
                if "ResourceNotFound" in type(e).__name__ + str(e):
                    log("table gone:", name)
                    left.pop(name)
                    continue
                left[name] = str(e)
                continue
            if d.get("TableStatus") == "DELETING":
                left[name] = "still deleting"
                continue
            try:
                if d.get("DeletionProtectionEnabled"):
                    ddb.update_table(TableName=name, DeletionProtectionEnabled=False)
                    log("turned deletion protection off:", name)
                    left[name] = "deletion protection was on"
                    continue
                ddb.delete_table(TableName=name)
                asked.add(name)
                log("deleting table:", name)
                left[name] = "deleting"
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                if "ResourceNotFound" in msg:
                    left.pop(name)
                    continue
                left[name] = msg
                log("table not deleted yet:", name, msg, "(retrying)" if any(r in msg for r in RETRYABLE) else "")
        if left:
            sleep(delay)
            delay = min(delay * 2, 20)
    return left


# ── Secret, allowlist, hybrid ──────────────────────────────────────────────

def read_secret(secret_id, sm=None):
    sm = sm or client("secretsmanager")
    return json.loads(sm.get_secret_value(SecretId=secret_id)["SecretString"] or "{}")


def setup_key(props, sm=None):
    token = str(props.get("Token") or "")
    if not token:
        return {"Stored": "false", "Message": "no setup key given"}
    if not SETUP_TOKEN_RE.match(token):
        raise ValueError("the setup key must be 32 to 64 letters and digits")
    sm = sm or client("secretsmanager")
    secret = read_secret(props["SecretId"], sm)
    if secret.get("github_app_id") and secret.get("github_private_key"):
        return {"Stored": "false", "Message": "the GitHub App exists: the setup key is not needed"}
    if secret.get("setup_token") == token:
        return {"Stored": "true", "Message": "already stored"}
    sm.put_secret_value(SecretId=props["SecretId"], SecretString=json.dumps(dict(secret, setup_token=token)))
    return {"Stored": "true", "Message": "stored"}


def allowlist_items(props):
    admin = str(props.get("Admin") or "").strip().lower()
    items = []
    if admin:
        items.append({"sk": f"EMAIL#{admin}", "kind": "admin"})
    domain = str(props.get("Domain") or "").strip().lower()
    if domain:
        items.append({"sk": f"EMAIL#*@{domain}", "kind": "domain"})
    if str(props.get("TwoFactor") or "off") == "on":
        items.append({"sk": "TWO_FACTOR", "kind": "rule"})
    return items


def allowlist(props, ddb=None):
    ddb = ddb or client("dynamodb")
    items = allowlist_items(props)
    if items:
        ddb.batch_write_item(RequestItems={props["Table"]: [{"PutRequest": {"Item": {
            "pk": {"S": "ALLOWLIST"}, "sk": {"S": i["sk"]}, "kind": {"S": i["kind"]}, "added_by": {"S": "config"},
            **({"require": {"BOOL": True}} if i["sk"] == "TWO_FACTOR" else {}),
        }}} for i in items]})
    return {"Entries": str(len(items))}


def sign(secret, ts, method, path, body=b""):
    msg = f"{ts}.{method.upper()}.{path}.{hashlib.sha256(body).hexdigest()}"
    return "v1=" + hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()


def hybrid_leave(props, sm=None, opener=None):
    try:
        secret = read_secret(props["SecretId"], sm)
    except Exception as e:
        return f"no registration read ({e}); nothing to release"
    rid, key = secret.get("hybrid_install_id"), secret.get("hybrid_install_secret")
    if not (isinstance(rid, str) and INSTALL_ID_RE.match(rid) and key):
        return "not registered with CoderHelm: nothing to release"
    url = (props.get("HybridUrl") or "https://api.coderhelm.com").rstrip("/")
    if url == "off":
        return "hybrid is off: nothing to release"
    path = "/hybrid/installs"
    ts = int(time.time())
    req = urllib.request.Request(url + path, method="DELETE", headers={
        "x-coderhelm-install": rid, "x-coderhelm-timestamp": str(ts),
        "x-coderhelm-signature": sign(key, ts, "DELETE", path), "user-agent": "coderhelm-install"})
    try:
        with (opener or urllib.request.urlopen)(req, timeout=8) as r:
            return f"released the CoderHelm address and links ({getattr(r, 'status', 200)})"
    except Exception as e:
        # Never blocks the delete: the hosted side also releases names whose install stops answering.
        return f"CoderHelm could not be reached ({e}); the address is released when the install stops answering"


# ── Domain, status, final sweep ────────────────────────────────────────────

def request_domain(props, lam=None):
    host = str(props.get("Hostname") or "").strip().lower()
    if not host:
        return {"Message": "no domain: CoderHelm uses its AWS address"}
    lam = lam or client("lambda")
    out = lam.invoke(FunctionName=props["UpdaterFunction"],
                     Payload=json.dumps({"action": "domain_set", "hostname": host, "actor": "install"}).encode())
    r = json.loads(out["Payload"].read() or b"{}")
    if not r.get("ok", True):
        # The install works on its AWS address; Settings, System shows the same reason.
        return {"Message": f"{host}: {r.get('error') or 'the certificate was not requested'}"}
    return {"Message": f"{host}: certificate requested ({r.get('state') or 'validating'}); Settings, System shows the DNS records"}


def redact(text):
    """Account numbers and ARNs out of anything in the public progress file."""
    t = re.sub(r"arn:aws[a-z-]*:[^\s\"']+", "<arn>", str(text or ""))
    t = re.sub(r"\b\d{12}\b", "<account>", t)
    return re.sub(r"[^\s@\"'<>]+@[^\s@\"'<>]+\.[a-z]{2,}", "<email>", t, flags=re.I)


def write_status(props, phase, message, url=None, s3=None):
    bucket = props.get("StatusBucket")
    if not bucket:
        return
    s3 = s3 or client("s3")
    if phase == "deploying":
        # Lets anyone read status.json (only that file). Account-wide Block
        # Public Access may refuse it: the guide then asks for the stack's
        # Outputs instead, and the install goes on.
        try:
            s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps({"Version": "2012-10-17", "Statement": [{
                "Sid": "InstallGuideReadsProgress", "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket}/status.json"}]}))
        except Exception as e:
            log("progress file stays private:", e)
    doc = {"schema": 1, "phase": phase, "message": redact(message)[:300], "url": url, "version": props.get("Version"),
           "stage": props.get("Stage"), "region": props.get("Region"),
           "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    s3.put_object(Bucket=bucket, Key="status.json", Body=json.dumps(doc).encode(),
                                    ContentType="application/json", CacheControl="no-store")


def delete_certificates(prefix, acm=None, r53=None):
    """The certificates the updater requested for this install (tag
    coderhelm=<prefix>, us-east-1), with their validation records."""
    acm = acm or client("acm", "us-east-1")
    r53 = r53 or client("route53")
    done, problems = [], []
    for page in acm.get_paginator("list_certificates").paginate():
        for c in page.get("CertificateSummaryList") or []:
            arn = c["CertificateArn"]
            tags = {t["Key"]: t.get("Value") for t in acm.list_tags_for_certificate(CertificateArn=arn).get("Tags") or []}
            if tags.get("coderhelm") != prefix:
                continue
            try:
                cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
                for o in cert.get("DomainValidationOptions") or []:
                    rr = o.get("ResourceRecord")
                    if rr:
                        remove_record(r53, rr)
                acm.delete_certificate(CertificateArn=arn)
                done.append(arn.split("/")[-1])
                log("deleted certificate", arn)
            except Exception as e:
                problems.append(f"certificate {arn.split('/')[-1]}: {e}")
                log("certificate not deleted:", arn, e)
    return done, problems


def remove_record(r53, rr):
    name = rr["Name"].rstrip(".")
    for z in r53.list_hosted_zones().get("HostedZones") or []:
        zn = z["Name"].rstrip(".")
        if z.get("Config", {}).get("PrivateZone") or not (name == zn or name.endswith("." + zn)):
            continue
        try:
            r53.change_resource_record_sets(HostedZoneId=z["Id"].split("/")[-1], ChangeBatch={"Changes": [{
                "Action": "DELETE", "ResourceRecordSet": {"Name": rr["Name"], "Type": rr["Type"], "TTL": 300,
                                                          "ResourceRecords": [{"Value": rr["Value"]}]}}]})
            log("removed validation record", name)
        except Exception as e:
            log("validation record left as it was:", name, e)


def delete_named_log_groups(names, prefix, logs=None):
    """The exact log groups the parts declare (CloudFormation already
    deleted each once; anything there now Lambda recreated). Only names
    under this install's own /aws/lambda/<prefix>- ."""
    logs = logs or client("logs")
    gone, problems = [], []
    for name in names or []:
        if not (str(name).startswith(f"/aws/lambda/{prefix}-") or str(name).startswith(f"/aws/codebuild/{prefix}-")):
            problems.append(f"{name}: not this install's")
            continue
        try:
            logs.delete_log_group(logGroupName=name)
            gone.append(name)
            log("deleted recreated log group", name)
        except Exception as e:
            if "ResourceNotFound" not in type(e).__name__ + str(e):
                problems.append(f"{name}: {e}")
    return gone, problems


def delete_log_groups(prefixes, tag=None, logs=None):
    """Log groups under the prefixes, only those tagged coderhelm:install=<tag>
    (every log group the stack declares is): never another install's."""
    logs = logs or client("logs")
    gone, problems = [], []
    for p in prefixes:
        for page in logs.get_paginator("describe_log_groups").paginate(logGroupNamePrefix=p):
            for g in page.get("logGroups") or []:
                if tag:
                    try:
                        arn = g.get("arn", "").removesuffix(":*")
                        tags = logs.list_tags_for_resource(resourceArn=arn).get("tags") or {}
                    except Exception as e:
                        log("log group left (tags unreadable):", g["logGroupName"], e)
                        continue
                    if tags.get("coderhelm:install") != tag:
                        continue
                try:
                    logs.delete_log_group(logGroupName=g["logGroupName"])
                    gone.append(g["logGroupName"])
                except Exception as e:
                    if "ResourceNotFound" not in type(e).__name__ + str(e):
                        problems.append(f"{g['logGroupName']}: {e}")
    log("deleted log groups:", len(gone))
    return gone, problems


# ── Dispatch ───────────────────────────────────────────────────────────────

def handle(event, context=None):
    kind = event["ResourceType"]
    rt = event["RequestType"]
    props = event.get("ResourceProperties") or {}
    old = event.get("OldResourceProperties") or {}
    log(rt, kind, event["LogicalResourceId"])

    if kind == "Custom::ReleaseAssets":
        if rt in ("Create", "Update"):
            data = copy_assets(props)
            if rt == "Update":
                prune_assets(props, old)
            return "SUCCESS", f"{data['Files']} release files verified ({data['Copied']} copied)", data
        if tearing_down(props):
            empty_bucket(props["Bucket"])
            return "SUCCESS", "assets bucket emptied", {}
        return "SUCCESS", "kept (not a stack delete)", {}

    if kind == "Custom::SetupKey":
        if rt == "Delete":
            return "SUCCESS", "nothing to do", {}
        data = setup_key(props)
        return "SUCCESS", data["Message"], data

    if kind == "Custom::Allowlist":
        if rt == "Delete":
            return "SUCCESS", "entries stay (remove them in Settings, Sign-in)", {}
        data = allowlist(props)
        return "SUCCESS", f"{data['Entries']} allowlist entries", data

    if kind == "Custom::Domain":
        if rt == "Delete" or (rt == "Update" and props.get("Hostname") == old.get("Hostname")):
            return "SUCCESS", "nothing to do", {"Message": ""}
        data = request_domain(props)
        return "SUCCESS", data["Message"], data

    if kind == "Custom::Status":
        phase = props.get("Phase", "done")
        if rt == "Create":
            msg = "CoderHelm is running" if phase == "done" else "Creating your CoderHelm (about 20 minutes)"
            write_status(props, "done" if phase == "done" else "deploying", msg, props.get("Url") if phase == "done" else None)
            return "SUCCESS", msg, {}
        if rt == "Delete" and phase == "start" and tearing_down(props) == "rollback":
            why = first_failure(props["RootStackId"]) or "the stack is rolling back"
            write_status(props, "failed", f"The install failed: {why}")
            return "SUCCESS", f"reported: {why}", {}
        return "SUCCESS", "nothing to do", {}

    if kind == "Custom::HybridLeave":
        if rt == "Delete" and tearing_down(props) == "delete":
            msg = hybrid_leave(props)
            log(msg)
            return "SUCCESS", msg, {}
        return "SUCCESS", "nothing to do", {}

    if kind == "Custom::Sweep":
        # One read of the root stack decides both whether it is a delete and whether data goes.
        stack = root_stack(props) if rt == "Delete" else {}
        mode = tearing_down(props, stack=stack) if rt == "Delete" else None
        if mode is None or (mode == "delete" and keep_data(props, stack=stack)):
            return "SUCCESS", "data kept" if mode == "delete" else "nothing to do", {}
        problems = []
        for b in props.get("Buckets") or []:
            try:
                empty_bucket(b)
            except Exception as e:
                problems.append(f"bucket {b}: {e}")
        remaining = (context.get_remaining_time_in_millis() / 1000.0 - 60) if context else 600
        left = delete_tables(props.get("Tables") or [], deadline=time.monotonic() + max(30, remaining))
        problems += [f"table {n}: {why}" for n, why in left.items()]
        if mode == "delete" and retained_but_deleted(props, stack=stack):
            # The stack retains these (KeepDataOnDelete=true): the switch says they go.
            for b in props.get("Buckets") or []:
                try:
                    delete_bucket(b)
                except Exception as e:
                    problems.append(f"bucket {b}: {e}")
            if props.get("UserPoolId"):
                try:
                    delete_user_pool(props["UserPoolId"])
                except Exception as e:
                    problems.append(f"user pool: {e}")
        if problems:
            # Delete the stack again to retry: CloudFormation picks up where it stopped.
            return "FAILED", "not removed yet: " + "; ".join(problems), {}
        return "SUCCESS", f"emptied {len(props.get('Buckets') or [])} buckets, deleted {len(props.get('Tables') or [])} tables", {}

    if kind == "Custom::Final":
        stack = root_stack(props) if rt == "Delete" else {}
        mode = tearing_down(props, stack=stack) if rt == "Delete" else None
        if mode is None:
            return "SUCCESS", "nothing to do", {}
        certs, problems = delete_certificates(props["Prefix"])
        if props.get("StatusBucket"):
            try:
                empty_bucket(props["StatusBucket"])
            except Exception as e:
                problems.append(f"bucket {props['StatusBucket']}: {e}")
        groups, more = delete_named_log_groups(props.get("LogGroupNames") or [], props["Prefix"])
        problems += more
        if mode == "rollback" or not keep_data(props, stack=stack):
            tagged, more = delete_log_groups(props.get("LogGroupPrefixes") or [], tag=props["Prefix"])
            groups += tagged
            problems += more
        if mode == "delete" and props.get("KeyAliasPrefix") and retained_but_deleted(props, stack=stack):
            keys, more = schedule_keys(props["KeyAliasPrefix"])
            problems += more
        secret = ""
        if mode == "delete" and not keep_data(props, stack=stack) and props.get("SecretId"):
            try:
                secret = " and the install's secret" if delete_secret(props["SecretId"]) else ""
            except Exception as e:
                problems.append(f"secret: {e}")
        if mode == "delete" and not problems:
            delete_switch_parameter(props.get("DeleteSwitch"))
        msg = f"deleted {len(certs)} certificates and {len(groups)} log groups{secret}"
        if problems:
            return "FAILED", msg + "; not removed: " + "; ".join(problems), {}
        return "SUCCESS", msg, {}

    return "FAILED", f"unknown resource type {kind}", {}


def handler(event, context):
    if event.get("action") == "stage":
        # The updater, directly (CloudFormation's events have no "action").
        try:
            r = stage_release(event, os.environ.get("ASSETS_BUCKET", ""))
        except Exception as e:
            log("stage FAILED:", type(e).__name__, e)
            r = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        log("stage", json.dumps(r))
        return r
    if not RESPONSE_URL_RE.match(str(event.get("ResponseURL", ""))):
        # Only CloudFormation asks with its own presigned answer URL: anything else is not a stack, so nothing is done.
        log("ignored: the response URL is not CloudFormation's")
        return None
    try:
        status, reason, data = handle(event, context)
    except Exception as e:
        log("FAILED:", type(e).__name__, e)
        status, reason, data = "FAILED", f"{type(e).__name__}: {e}", {}
    log(status, reason)
    respond(event, status, reason, data, physical=event.get("PhysicalResourceId") or f"{event['LogicalResourceId']}-native")
