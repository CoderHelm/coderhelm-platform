# The uninstall stack's starter (infra/lib/uninstall-stack.ts inlines this
# file into infra/cfn/coderhelm-selfhost-uninstall.yaml).
#
# Create: opens the progress file to the uninstall guide and starts the
# uninstall build once.
#
# Delete (the stack rolls itself back when the build ends, with the
# credentials of the person who launched it, so the uninstaller removes
# itself without any role deleting itself): closes a window the build left,
# and when the uninstall finished, waits for the build to stop and removes
# the two things the stack keeps on purpose so a failure stays readable:
# the build's log group and the progress bucket. After a failure they stay.
#
# lambda/tests/test_uninstall_window.py tests this file.
import json, os, time, urllib.request
import boto3

E = os.environ


def respond(event, context, status, reason=""):
    """Tells CloudFormation the result. Written here, not imported: the
    cfnresponse helper isn't provided on every runtime."""
    body = json.dumps({
        "Status": status,
        "Reason": (reason or "See the log: " + context.log_stream_name)[:500],
        "PhysicalResourceId": "uninstaller",
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": {},
    }).encode()
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT",
                                 headers={"Content-Type": "", "Content-Length": str(len(body))})
    urllib.request.urlopen(req, timeout=30).read()


def public_status(s3, bucket, p):
    """Lets anyone read status.json (only that file). Account-wide Block
    Public Access may refuse it: the guide then follows CloudFormation."""
    try:
        s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Sid": "UninstallGuideReadsProgress", "Effect": "Allow", "Principal": "*",
                           "Action": "s3:GetObject", "Resource": f"arn:{p['Partition']}:s3:::{bucket}/status.json"}]}))
    except Exception as e:
        print("progress file stays private:", e)
    s3.put_object(Bucket=bucket, Key="status.json", ContentType="application/json", CacheControl="no-store",
                  Body=json.dumps({"schema": 1, "kind": "uninstall", "phase": "starting", "message": "Starting the uninstaller",
                                   "mode": p["Mode"], "stage": p["Stage"], "region": p["Region"],
                                   "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}))


def finished(s3, bucket):
    try:
        doc = json.loads(s3.get_object(Bucket=bucket, Key="status.json")["Body"].read())
        return doc.get("phase") == "done"
    except Exception:
        return False


def empty_and_delete(s3, bucket):
    kw = {"Bucket": bucket}
    while True:
        r = s3.list_object_versions(**kw)
        objs = [{"Key": o["Key"], "VersionId": o["VersionId"]} for o in r.get("Versions", []) + r.get("DeleteMarkers", [])]
        if objs:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs, "Quiet": True})
        if not r.get("IsTruncated"):
            break
        kw = {"Bucket": bucket, "KeyMarker": r.get("NextKeyMarker"), "VersionIdMarker": r.get("NextVersionIdMarker")}
    s3.delete_bucket(Bucket=bucket)


def clean_up(p, s3=None, logs=None, codebuild=None, sleep=time.sleep):
    """After a finished uninstall: the build's log group and the progress bucket go too."""
    s3 = s3 or boto3.client("s3")
    bucket = p.get("Bucket") or ""
    codebuild = codebuild or boto3.client("codebuild")
    builds = []
    for _ in range(40):
        ids = codebuild.list_builds_for_project(projectName=p["Project"]).get("ids", [])[:5]
        builds = codebuild.batch_get_builds(ids=ids).get("builds", []) if ids else []
        if not any(b.get("buildStatus") == "IN_PROGRESS" for b in builds):
            break
        sleep(6)
    # Newest first: the uninstall build must have succeeded and said so.
    ok = bool(builds) and builds[0].get("buildStatus") == "SUCCEEDED" and (not bucket or finished(s3, bucket))
    if not ok:
        print("the uninstall did not finish: its log and progress file stay")
        return False
    # The guide reads the last state before the file goes.
    sleep(20)
    (logs or boto3.client("logs")).delete_log_group(logGroupName=p["LogGroup"])
    if bucket:
        empty_and_delete(s3, bucket)
    return True


def close_window(role, iam=None):
    try:
        (iam or boto3.client("iam")).delete_role_policy(RoleName=role, PolicyName="uninstall-window")
    except Exception as e:
        print("no window to close:", e)


def handler(event, context):
    try:
        p = event["ResourceProperties"]
        if event["RequestType"] == "Delete":
            close_window(p["BuildRole"])
            try:
                clean_up(p)
            except Exception as e:
                print("could not remove the log or progress bucket:", e)
        if event["RequestType"] == "Create":
            if p.get("Bucket"):
                try:
                    public_status(boto3.client("s3"), p["Bucket"], p)
                except Exception as e:
                    print("progress file not written:", e)
            boto3.client("codebuild").start_build(
                projectName=p["Project"],
                environmentVariablesOverride=[{"name": "WAIT_URL", "value": p["WaitUrl"], "type": "PLAINTEXT"}],
            )
        respond(event, context, "SUCCESS")
    except Exception as e:
        print(e)
        respond(event, context, "FAILED", str(e)[:200])
