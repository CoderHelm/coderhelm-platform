"""Updates for a self-hosted CoderHelm, run in the install's own account.

Actions (the `action` field of the event):

- tick      hourly (EventBridge): is a newer release out, and may it be
            applied now? Notifies admins once per release; starts the update
            build when auto-update allows it (on, in the maintenance window,
            and not a major release while majors need an admin).
- status    for the dashboard and the CLI: current and latest version,
            release notes, settings, history.
- settings  saves auto-update settings (validated).
- update_now  starts the update build for a version (default: the latest).
            Only a version newer than the one running: going back is a
            rollback.
- rollback  starts the build for the version before the last update, or an
            admin's pick, which must be a version this install ran before.
- finished  the update build reports its result; history and notices.
- refresh_secrets  the gateway stored new keys in the install's secret (the
            GitHub App, from the install guide): restart the services that
            read it at start, by stamping SECRETS_REVISION on each.
- domain_set     use your own domain: requests its certificate (ACM, us-east-1,
            DNS validation), and when the domain's zone is in Route 53 in
            this account adds the validation record there too.
- domain_status  the domain's state, its certificate and the DNS records to
            add, and moves it along: once the certificate is issued it starts
            the build that puts the domain on CloudFront (mode "domain").
            Also every 5 minutes (EventBridge).
- domain_remove  back to the AWS (CloudFront) address.
- model_check  every model the next release calls gets one test call through
            the install's provider (Settings, System, Models), made by the
            worker with the install's own role or key. Also before every
            update (automatic or Update now), on first sight of a release,
            and once a day while a model is refused. A refused model holds
            the update: admins are told which model, the fix and the console
            page, and it applies by itself once the check passes (with
            auto-update on). Nothing here accepts terms or subscribes to
            anything; a person does that in the AWS console.

The build itself (CodeBuild, infra/lib/updates-stack.ts) runs
`coderhelm-selfhost update`: health check, checksum and signature check,
deploy, health check, and a rollback when the new version is unhealthy.
The build checks the release itself (signature made by exactly this
repository's release.yml on the version's tag), so the release list read
here only decides what to offer, never what is trusted. It is still checked:
it must come from api.github.com for CoderHelm/coderhelm-platform, and a list
that no longer shows the newest version seen before is reported as stale in
status (a stale copy can't quietly hold an install back).

State lives on the settings table under pk SYSTEM. The domain's states and
checks are documented in docs/self-host/domains.md.
"""

import json
import logging
import os
import re
import time
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
CHANNELS = ("stable", "preview")
MAJORS = ("ask", "auto")
CACHE_SECONDS = 3600
# A release list not refreshed for this long is shown as stale.
STALE_SECONDS = 3 * 24 * 3600
RELEASE_REPO = "CoderHelm/coderhelm-platform"
RELEASES_URL = f"https://api.github.com/repos/{RELEASE_REPO}/releases?per_page=30"
RUNNING_STALE = timedelta(hours=3)
# A refused model is checked again this often (and a pass re-proven before applying).
MODEL_RETRY = timedelta(days=1)
PK = "SYSTEM"

DEFAULT_SETTINGS = {
    "auto": False,
    "channel": "stable",
    "day": "sun",
    "hour": 3,
    "timezone": "UTC",
    "majors": "ask",
}

VERSION_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?$")


# ── Versions ──────────────────────────────────────────────────────────────

def parse_version(v):
    m = VERSION_RE.match(str(v or "").strip())
    if not m:
        return None
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)), m.group(4) or "")


def compare(a, b):
    """-1, 0 or 1; a prerelease sorts before its release."""
    pa, pb = parse_version(a), parse_version(b)
    if pa is None or pb is None:
        raise ValueError(f"not a version: {a if pa is None else b}")
    if pa[:3] != pb[:3]:
        return -1 if pa[:3] < pb[:3] else 1
    if pa[3] == pb[3]:
        return 0
    if not pa[3]:
        return 1
    if not pb[3]:
        return -1
    return -1 if pa[3] < pb[3] else 1


def is_major(current, target):
    pc, pt = parse_version(current), parse_version(target)
    return bool(pc and pt and pt[0] > pc[0])


# ── Settings ──────────────────────────────────────────────────────────────

def defaults_from_env(env=None):
    env = os.environ if env is None else env
    out = dict(DEFAULT_SETTINGS)
    try:
        out.update(json.loads(env.get("UPDATE_DEFAULTS") or "{}"))
    except ValueError:
        pass
    return out


def validate_settings(raw, base=None):
    """The settings with `raw` applied, or ValueError naming the bad field."""
    s = dict(base or DEFAULT_SETTINGS)
    raw = raw or {}
    unknown = set(raw) - set(DEFAULT_SETTINGS)
    if unknown:
        raise ValueError(f"unknown setting: {sorted(unknown)[0]}")
    if "auto" in raw:
        if not isinstance(raw["auto"], bool):
            raise ValueError("auto: true or false")
        s["auto"] = raw["auto"]
    if "channel" in raw:
        if raw["channel"] not in CHANNELS:
            raise ValueError("channel: stable or preview")
        s["channel"] = raw["channel"]
    if "day" in raw:
        d = str(raw["day"]).lower()[:3] if raw["day"] != "daily" else "daily"
        if d not in DAYS and d != "daily":
            raise ValueError("day: mon to sun, or daily")
        s["day"] = d
    if "hour" in raw:
        if not isinstance(raw["hour"], int) or isinstance(raw["hour"], bool) or not 0 <= raw["hour"] <= 23:
            raise ValueError("hour: 0 to 23")
        s["hour"] = raw["hour"]
    if "timezone" in raw:
        tz = str(raw["timezone"])
        if zone(tz) is None:
            raise ValueError("timezone: an IANA name such as Europe/Berlin")
        s["timezone"] = tz
    if "majors" in raw:
        if raw["majors"] not in MAJORS:
            raise ValueError("majors: ask or auto")
        s["majors"] = raw["majors"]
    return s


def zone(name):
    if name in ("UTC", "Etc/UTC"):
        return timezone.utc
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(name)
    except Exception:
        return None


def in_window(now_utc, settings):
    """True during the maintenance hour, in the install's time zone."""
    tz = zone(settings.get("timezone", "UTC")) or timezone.utc
    local = now_utc.astimezone(tz)
    day_ok = settings.get("day") == "daily" or DAYS[local.weekday()] == settings.get("day")
    return day_ok and local.hour == settings.get("hour")


# ── The decision ──────────────────────────────────────────────────────────

def pick_latest(releases, channel, current):
    """The newest release above `current` that the channel allows."""
    best = None
    for r in releases or []:
        tag = r.get("tag_name") or r.get("version")
        p = parse_version(tag)
        if not p or r.get("draft"):
            continue
        pre = bool(r.get("prerelease")) or bool(p[3])
        if pre and channel != "preview":
            continue
        if current and parse_version(current) and compare(tag, current) <= 0:
            continue
        if best is None or compare(tag, best["version"]) > 0:
            best = {
                "version": tag,
                "notes": (r.get("body") or "")[:8000],
                "published_at": r.get("published_at"),
                "url": r.get("html_url"),
                "prerelease": pre,
            }
    if best:
        best["breaking"] = is_major(current, best["version"])
    return best


def decide(now_utc, settings, current, releases, state, model_check=None):
    """What the hourly check does: none, notify or update (with the reason)."""
    latest = pick_latest(releases, settings["channel"], current)
    if not latest:
        return {"action": "none", "reason": "up to date"}
    v = latest["version"]
    if running(state, now_utc):
        return {"action": "none", "version": v, "reason": "an update is running"}
    notify = state.get("notified_version") != v
    quiet = {"action": "notify" if notify else "none", "version": v}
    if v in (state.get("failed_versions") or []):
        return {**quiet, "reason": "this version failed before; an admin can retry it"}
    if blocking(model_check, v):
        return {**quiet, "reason": "a model this release calls was refused; checking again daily"}
    if not settings["auto"]:
        return {**quiet, "reason": "auto-update is off"}
    if latest["breaking"] and settings["majors"] == "ask":
        return {**quiet, "reason": "a major release waits for an admin"}
    if not in_window(now_utc, settings):
        return {**quiet, "reason": "waiting for the maintenance window"}
    return {"action": "update", "version": v, "reason": "auto-update in the maintenance window"}


def running(state, now_utc):
    started = state.get("running_since")
    if not started:
        return False
    try:
        at = datetime.fromisoformat(started)
    except ValueError:
        return False
    return now_utc - at < RUNNING_STALE


# ── Storage (settings table, pk SYSTEM) ───────────────────────────────────

class Store:
    def __init__(self, client, table):
        self.c = client
        self.t = table

    def get(self, sk, default=None):
        r = self.c.get_item(TableName=self.t, Key={"pk": {"S": PK}, "sk": {"S": sk}})
        item = r.get("Item")
        if not item or "data" not in item:
            return default
        return json.loads(item["data"]["S"])

    def put(self, sk, value):
        self.c.put_item(TableName=self.t, Item={"pk": {"S": PK}, "sk": {"S": sk}, "data": {"S": json.dumps(value)}})

    def history(self, limit=20):
        r = self.c.query(
            TableName=self.t,
            KeyConditionExpression="pk = :p AND begins_with(sk, :s)",
            ExpressionAttributeValues={":p": {"S": PK}, ":s": {"S": "UPDATE#"}},
            ScanIndexForward=False,
            Limit=limit,
        )
        return [dict(json.loads(i["data"]["S"]), id=i["sk"]["S"]) for i in r.get("Items", [])]


# ── Outside calls (swapped in tests) ──────────────────────────────────────

_clients = {}


def client(name, region=None):
    key = f"{name}@{region}" if region else name
    if key not in _clients:
        import boto3
        _clients[key] = boto3.client(name, region_name=region) if region else boto3.client(name)
    return _clients[key]


def check_repo(repo):
    """Releases come only from CoderHelm's own repository."""
    if repo != RELEASE_REPO:
        raise ValueError(f"releases come only from {RELEASE_REPO}, not {repo}")
    return repo


def fetch_releases(repo):
    check_repo(repo)
    req = urllib.request.Request(
        RELEASES_URL,
        headers={"accept": "application/vnd.github+json", "user-agent": "coderhelm-updater"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        # A redirect anywhere but GitHub's API over HTTPS is refused.
        if not r.geturl().startswith("https://api.github.com/"):
            raise ValueError(f"the release list came from {r.geturl()}")
        return json.loads(r.read())


def newest(releases):
    """The highest release version in a list (drafts and odd tags skipped), or None."""
    best = None
    for r in releases or []:
        tag = r.get("tag_name") or r.get("version")
        if r.get("draft") or not parse_version(tag):
            continue
        if best is None or compare(tag, best) > 0:
            best = tag
    return best


def releases_cached(store, repo, now_ts, fetch=fetch_releases):
    cached = store.get("RELEASES_CACHE", {})
    if cached and now_ts - cached.get("fetched_at", 0) < CACHE_SECONDS:
        return cached.get("releases", [])
    try:
        check_repo(repo)
        rel = [
            {k: r.get(k) for k in ("tag_name", "prerelease", "draft", "body", "published_at", "html_url")}
            for r in fetch(repo)
        ]
    except Exception as e:  # GitHub down: keep the old list
        logger.warning(f"Could not read releases: {e}")
        return cached.get("releases", [])
    # Freshness: a list without the newest version seen before is either a
    # stale copy or a release that was withdrawn. Either way it can only hold
    # back an update (nothing below the running version is ever applied), so
    # the list is used as it is, and admins see why it looks older.
    seen = cached.get("newest_seen") if cached else None
    top = newest(rel)
    doc = {"fetched_at": now_ts, "releases": rel, "newest_seen": top or seen}
    if seen and (top is None or compare(top, seen) < 0):
        logger.warning(f"Release list looks stale: newest is {top}, {seen} was seen before")
        doc.update(newest_seen=seen, stale_reason=f"GitHub's release list no longer shows {seen} (withdrawn, or a stale copy)")
    store.put("RELEASES_CACHE", doc)
    return rel


def releases_freshness(store, now_ts):
    """Whether the release list is current: {fetched_at, stale, reason}."""
    cached = store.get("RELEASES_CACHE", {}) or {}
    at = cached.get("fetched_at")
    reason = cached.get("stale_reason")
    if not reason and (not at or now_ts - at > STALE_SECONDS):
        reason = "the release list has not been read from GitHub for over 3 days"
    return {"fetched_at": at, "stale": bool(reason), "reason": reason}


def ran_versions(store, state, current):
    """Every version this install has run: recorded ones, the history, the current one."""
    out = set(v for v in (state.get("ran_versions") or []) if parse_version(v))
    for h in store.history(50):
        if h.get("mode") == "domain":
            continue
        if h.get("from_version") and parse_version(h["from_version"]):
            out.add(h["from_version"])
        if h.get("status") == "succeeded" and parse_version(h.get("version")):
            out.add(h["version"])
    if parse_version(current):
        out.add(current)
    return out


def notify(subject, text, env=None):
    """Email every install admin, and post to the updates webhook if set."""
    env = os.environ if env is None else env
    admins = [a.strip() for a in (env.get("ADMIN_EMAILS") or "").split(",") if a.strip()]
    sender = env.get("SES_FROM_ADDRESS")
    sent = 0
    if admins and sender:
        try:
            client("sesv2").send_email(
                FromEmailAddress=sender,
                Destination={"ToAddresses": admins},
                Content={"Simple": {"Subject": {"Data": subject}, "Body": {"Text": {"Data": text}}}},
            )
            sent += 1
        except Exception as e:
            logger.warning(f"Update email not sent: {e}")
    hook = env.get("UPDATE_WEBHOOK_URL")
    if hook and hook.startswith("https://"):
        try:
            body = json.dumps({"text": f"{subject}\n{text}"}).encode()
            req = urllib.request.Request(hook, data=body, headers={"content-type": "application/json"}, method="POST")
            urllib.request.urlopen(req, timeout=10).close()
            sent += 1
        except Exception as e:
            logger.warning(f"Update webhook not posted: {e}")
    return sent


def start_build(env, version, actor, mode, history_id, extra=None):
    variables = {"VERSION": version, "ACTOR": actor, "MODE": mode, "HISTORY_ID": history_id, **(extra or {})}
    r = client("codebuild").start_build(
        projectName=env["UPDATE_PROJECT_NAME"],
        environmentVariablesOverride=[{"name": k, "value": str(v), "type": "PLAINTEXT"} for k, v in variables.items()],
    )
    return r["build"]["id"]


def refresh_functions(lam, names, revision, sleep=time.sleep, tries=5):
    """Sets SECRETS_REVISION on each function (its other settings unchanged),
    so its next request starts a fresh instance that reads the secret again.
    Retries while an earlier change to the function is still applying."""
    done = []
    for name in names:
        for attempt in range(tries):
            try:
                cfg = lam.get_function_configuration(FunctionName=name)
                variables = dict((cfg.get("Environment") or {}).get("Variables") or {})
                variables["SECRETS_REVISION"] = revision
                lam.update_function_configuration(FunctionName=name, Environment={"Variables": variables})
                done.append(name)
                break
            except Exception as e:  # ResourceConflictException while an update applies
                if "ResourceConflict" not in type(e).__name__ + str(e) or attempt == tries - 1:
                    logger.error("refresh %s failed: %s", name, e)
                    break
                sleep(2 * (attempt + 1))
    return done


# ── Your own domain ───────────────────────────────────────────────────────
#
# One record on the settings table (pk SYSTEM, sk DOMAIN):
#   {hostname, zone, zone_id, certificate_arn, state, reason, requested_at,
#    actor, history_id}
# state: validating (the certificate waits for its DNS record), attaching
# (the build puts the domain on CloudFront), live, failed, removing.
# CloudFront only uses certificates from us-east-1, so ACM is called there.

HOST_RE = re.compile(r"^(?=.{4,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$")
ACM_REGION = "us-east-1"
DOMAIN_BUSY = ("attaching", "removing")


def clean_hostname(raw):
    """The hostname people will open, or ValueError in plain words."""
    h = str(raw or "").strip().lower().rstrip(".")
    h = re.sub(r"^https?://", "", h).split("/")[0]
    if not HOST_RE.match(h):
        raise ValueError("enter a hostname such as coderhelm.example.com")
    if h.endswith(".cloudfront.net") or h.endswith(".amazonaws.com"):
        raise ValueError("enter a domain you own, not an AWS address")
    return h


def find_zone(zones, hostname):
    """The public Route 53 zone in this account the hostname belongs to (longest match), or None."""
    best = None
    for z in zones or []:
        name = str(z.get("Name") or "").rstrip(".").lower()
        if not name or (z.get("Config") or {}).get("PrivateZone"):
            continue
        if hostname == name or hostname.endswith("." + name):
            if best is None or len(name) > len(best["name"]):
                best = {"name": name, "id": str(z.get("Id") or "").replace("/hostedzone/", "")}
    return best


def list_zones(r53):
    zones, marker = [], None
    while True:
        kw = {"Marker": marker} if marker else {}
        r = r53.list_hosted_zones(**kw)
        zones.extend(r.get("HostedZones") or [])
        if not r.get("IsTruncated"):
            return zones
        marker = r.get("NextMarker")


def validation_record(cert):
    """The CNAME ACM checks for, without trailing dots, or None until ACM has made it."""
    for o in (cert or {}).get("DomainValidationOptions") or []:
        rr = o.get("ResourceRecord")
        if rr and rr.get("Name") and rr.get("Value"):
            return {"name": rr["Name"].rstrip(".").lower(), "type": rr.get("Type") or "CNAME", "value": rr["Value"].rstrip(".").lower()}
    return None


def describe_certificate(arn, acm=None):
    if not arn:
        return None
    try:
        return (acm or client("acm", ACM_REGION)).describe_certificate(CertificateArn=arn).get("Certificate")
    except Exception as e:
        logger.warning(f"Could not read the certificate: {e}")
        return None


def distribution_domain(env, ssm=None):
    """dxxxx.cloudfront.net: the address record's value (the web stack stores it)."""
    name = env.get("DISTRIBUTION_DOMAIN_PARAMETER")
    if not name:
        return None
    try:
        return (ssm or client("ssm")).get_parameter(Name=name)["Parameter"]["Value"].rstrip(".").lower()
    except Exception as e:
        logger.warning(f"Could not read the CloudFront address: {e}")
        return None


def zone_name_servers(zone_id, r53=None):
    if not zone_id:
        return []
    try:
        ds = (r53 or client("route53")).get_hosted_zone(Id=zone_id).get("DelegationSet") or {}
        return [n.rstrip(".").lower() for n in ds.get("NameServers") or []]
    except Exception as e:
        logger.warning(f"Could not read the zone's name servers: {e}")
        return []


def current_hostname(env):
    """The hostname the install answers on now (APP_URL), and whether it is CloudFront's own."""
    host = re.sub(r"^https?://", "", app_url(env)).split("/")[0].lower()
    return host, host.endswith(".cloudfront.net") or not host


def domain_view(dom, cert, target, name_servers, env):
    """What the dashboard, the guide and the gateway's checks see. No account ids."""
    host, on_aws = current_hostname(env)
    if not dom or not dom.get("hostname"):
        if on_aws:
            return {"hostname": None, "state": "none", "mode": None, "current": host or None, "target": target, "records": []}
        # A domain from the config (the Launch Stack with a Route 53 zone, or the CLI).
        return {"hostname": host, "state": "live", "mode": "config", "current": host, "target": target, "records": []}
    route53 = bool(dom.get("zone"))
    v = validation_record(cert)
    records = []
    if not route53:
        if v:
            records.append({"purpose": "certificate", **v})
        if target:
            records.append({"purpose": "address", "name": dom["hostname"], "type": "CNAME", "value": target})
    c = None
    if cert:
        c = {"status": cert.get("Status"), "failure_reason": cert.get("FailureReason"), "validation": v,
             "in_use": bool(cert.get("InUseBy"))}
    return {
        "hostname": dom["hostname"],
        "state": dom.get("state") or "validating",
        "reason": dom.get("reason") or "",
        "mode": "route53" if route53 else "external",
        "zone": dom.get("zone"),
        "name_servers": name_servers,
        "current": host or None,
        "target": target,
        "certificate": c,
        "records": records,
        "requested_at": dom.get("requested_at"),
    }


def domain_build_env(dom, action):
    return {
        "DOMAIN_ACTION": action,
        "DOMAIN_HOSTNAME": dom.get("hostname") or "",
        "DOMAIN_CERT": dom.get("certificate_arn") or "",
        "DOMAIN_ZONE": dom.get("zone") or "",
        "DOMAIN_ZONE_ID": dom.get("zone_id") or "",
    }


def advance(store, env, now, dom, cert):
    """Moves a waiting domain along: an issued certificate starts the build, a failed one stops."""
    if not dom or dom.get("state") != "validating" or not cert:
        return dom
    status = cert.get("Status")
    if status == "ISSUED":
        if running(store.get("UPDATE_STATE", {}), now):
            return dom  # the next check starts it
        entry = begin(store, env, now, env.get("CODERHELM_VERSION", ""), dom.get("actor") or "auto", "domain",
                      domain_build_env(dom, "attach"), note=f"use {dom['hostname']}")
        dom = dict(dom, state="attaching", reason="", history_id=entry["id"])
        store.put("DOMAIN", dom)
    elif status in ("FAILED", "VALIDATION_TIMED_OUT", "REVOKED", "EXPIRED"):
        dom = dict(dom, state="failed", reason=cert.get("FailureReason") or status)
        store.put("DOMAIN", dom)
    return dom


def domain_status(store, env, now):
    dom = store.get("DOMAIN") or None
    cert = describe_certificate(dom.get("certificate_arn")) if dom else None
    dom = advance(store, env, now, dom, cert)
    ns = zone_name_servers(dom.get("zone_id")) if dom and dom.get("zone") else []
    return domain_view(dom, cert, distribution_domain(env), ns, env)


def domain_set(store, env, now, raw, actor, sleep=time.sleep):
    hostname = clean_hostname(raw)
    dom = store.get("DOMAIN") or {}
    if dom.get("state") in DOMAIN_BUSY:
        raise ValueError("the address is being changed right now; wait for it to finish (about 10 minutes)")
    host, _ = current_hostname(env)
    if hostname == host:
        raise ValueError(f"CoderHelm already answers on {hostname}")
    zone = find_zone(list_zones(client("route53")), hostname)
    acm = client("acm", ACM_REGION)
    token = re.sub(r"[^a-z0-9]", "", hostname)[:24] or "coderhelm"
    if dom.get("hostname") == hostname and dom.get("state") == "failed":
        # Try again: a new certificate (the same token within an hour returns the failed one).
        token = (token + now.strftime("%H%M%S"))[:32]
    arn = acm.request_certificate(
        DomainName=hostname, ValidationMethod="DNS", IdempotencyToken=token,
        Tags=[{"Key": "coderhelm", "Value": env.get("RESOURCE_PREFIX") or "coderhelm"}],
    )["CertificateArn"]
    cert = None
    for _ in range(6):  # ACM fills in the record a few seconds after the request
        cert = describe_certificate(arn, acm)
        if validation_record(cert):
            break
        sleep(2)
    v = validation_record(cert)
    if zone and v:
        client("route53").change_resource_record_sets(
            HostedZoneId=zone["id"],
            ChangeBatch={"Comment": "CoderHelm certificate validation", "Changes": [{
                "Action": "UPSERT",
                "ResourceRecordSet": {"Name": v["name"] + ".", "Type": v["type"], "TTL": 300, "ResourceRecords": [{"Value": v["value"] + "."}]},
            }]},
        )
    dom = {
        "hostname": hostname, "zone": zone["name"] if zone else None, "zone_id": zone["id"] if zone else None,
        "certificate_arn": arn, "state": "validating", "reason": "", "requested_at": now.isoformat(), "actor": actor,
    }
    store.put("DOMAIN", dom)
    dom = advance(store, env, now, dom, cert)
    ns = zone_name_servers(dom.get("zone_id")) if zone else []
    return domain_view(dom, cert, distribution_domain(env), ns, env)


def domain_remove(store, env, now, actor):
    dom = store.get("DOMAIN") or {}
    if dom.get("state") in DOMAIN_BUSY:
        raise ValueError("the address is being changed right now; wait for it to finish (about 10 minutes)")
    host, on_aws = current_hostname(env)
    if on_aws:
        if not dom:
            raise ValueError("CoderHelm already uses its AWS address")
        # Never attached: forget it (the certificate stays in ACM, unused and free).
        store.put("DOMAIN", {})
        return domain_view(None, None, distribution_domain(env), [], env)
    entry = begin(store, env, now, env.get("CODERHELM_VERSION", ""), actor, "domain",
                  domain_build_env({"hostname": host}, "remove"), note="use the AWS address")
    dom = dict(dom or {"hostname": host}, state="removing", reason="", history_id=entry["id"])
    store.put("DOMAIN", dom)
    return domain_view(dom, None, distribution_domain(env), [], env)


def domain_finished(store, env, now, entry, status):
    dom = store.get("DOMAIN") or {}
    removing = dom.get("state") == "removing"
    if status == "succeeded":
        if removing:
            store.put("DOMAIN", {})
            subject = "CoderHelm is back on its AWS address"
        else:
            dom = dict(dom, state="live", reason="")
            store.put("DOMAIN", dom)
            subject = f"CoderHelm now answers on {dom.get('hostname')}"
    else:
        dom = dict(dom, state="failed", reason=entry.get("reason") or "the build failed: see its log")
        store.put("DOMAIN", dom)
        subject = "CoderHelm could not change its address"
    notify(subject, f"{entry.get('reason') or 'Done.'}\nActor: {entry.get('actor', 'auto')}\n{app_url(env)}/settings/system/#domain", env)
    return {"status": status, "domain": dom.get("state") or "none"}


# ── The model check before an update ──────────────────────────────────────
#
# One record on the settings table (pk SYSTEM, sk MODEL_CHECK):
#   {version, checked_at, provider, region, inference_prefix, ok, results,
#    notified}
# results: one per model, from the worker (common::model_access): model, id,
# status (pass, fail, skip), code, detail, fix, url, and `required` (the
# release calls it, or the deploy config names it).
#
# The release's models.json (written by its release workflow from the code,
# selfhost/release-models.mjs, and covered by the signed SHA256SUMS) lists
# the models it calls and their Bedrock ids for every inference profile
# prefix. The updater never downloads it: the release
# check build (its own read-only project) checks SHA256SUMS's signature and
# the file's checksum, and reports it back (models_verified). Until then the check is pending and
# holds the update. A signed release without one (older releases) has
# nothing to check.
#
# The check only calls models. It never subscribes, accepts an agreement or
# submits a form for anyone: those stay a person's action in the console.


class ModelsBlocked(ValueError):
    """Update now refused by the model check (the record goes with it)."""

    def __init__(self, message, record):
        super().__init__(message)
        self.record = record


VERIFY_RETRY = timedelta(minutes=30)


def verified_manifest(store, version):
    """(known, manifest): the release's models.json as the update build
    reported it after checking SHA256SUMS's signature and the file's
    checksum. Never read from anywhere else: an unverified file is never
    trusted. `manifest` is None for a signed release that lists no models."""
    rec = store.get(f"MANIFEST#{version}")
    if rec is None or not rec.get("verified"):
        return False, None
    return True, rec.get("manifest")


def start_check(env, version):
    """Starts the release check build (infra/lib/updates-stack.ts,
    releaseCheckBuildSpec): read-only, it only verifies and reports."""
    r = client("codebuild").start_build(
        projectName=env["RELEASE_CHECK_PROJECT_NAME"],
        environmentVariablesOverride=[{"name": "VERSION", "value": str(version), "type": "PLAINTEXT"}],
    )
    return r["build"]["id"]


def start_verify(store, env, now, version):
    """Starts the release check: it verifies the release's signature, reads
    models.json only when the signed checksums list it and it matches, and
    reports back (models_verified). At most once every 30 minutes per release."""
    state = store.get("UPDATE_STATE", {})
    v = state.get("verifying") or {}
    if v.get("version") == version:
        try:
            if now - datetime.fromisoformat(v["at"]) < VERIFY_RETRY:
                return False
        except (KeyError, ValueError):
            pass
    try:
        start_check(env, version)
    except Exception as e:
        logger.warning(f"Release check not started: {e}")
        return False
    state["verifying"] = {"version": version, "at": now.isoformat()}
    store.put("UPDATE_STATE", state)
    return True


def configured_models(env):
    return [m for m in (env.get("CONFIGURED_MODELS") or "").split(",") if m]


def check_targets(manifest, env):
    """Every model the release calls, plus the models the deploy config names,
    with the release's Bedrock id for each prefix."""
    calls = list(manifest.get("calls") or [])
    for m in configured_models(env):
        if m not in calls:
            calls.append(m)
    ids = manifest.get("bedrock") or {}
    return [{"model": m, "bedrock_ids": ids.get(m) or {}} for m in calls]


def invoke_worker(env, payload):
    """The worker makes the test calls (its role, its meter) and explains them."""
    r = client("lambda").invoke(
        FunctionName=env["WORKER_FUNCTION_NAME"],
        InvocationType="RequestResponse",
        Payload=json.dumps({"model_check": payload}).encode(),
    )
    if r.get("FunctionError"):
        raise RuntimeError(f"the worker's model check failed: {r['Payload'].read()[:300]!r}")
    return json.loads(r["Payload"].read() or b"{}")


def blocking(record, version):
    """True when the check for `version` found a model the release calls
    refused, or hasn't run yet because the release's signature is being checked."""
    return bool(record and record.get("version") == version
                and (record.get("pending") or failures(record, required=True)))


def failures(record, required=None):
    out = [r for r in (record or {}).get("results") or [] if r.get("status") == "fail"]
    if required is not None:
        out = [r for r in out if bool(r.get("required", True)) == required]
    return out


def model_check(store, env, now, version, repo, probe=invoke_worker):
    """Runs the check for `version` and stores it. Until the release's
    signature has been checked, the record is pending (and holds updates)."""
    known, manifest = verified_manifest(store, version)
    if not known:
        start_verify(store, env, now, version)
        record = {"version": version, "checked_at": now.isoformat(), "ok": False, "pending": True, "results": [],
                  "note": "checking the release's signature before reading the models it calls"}
        store.put("MODEL_CHECK", dict(record, notified=False))
        return record
    if manifest is None:
        record = {"version": version, "checked_at": now.isoformat(), "ok": True, "results": [],
                  "note": "this release lists no models to check"}
    else:
        targets = check_targets(manifest, env)
        required = set(manifest.get("calls") or []) | set(configured_models(env))
        models = store.get("MODELS") or {}
        try:
            answer = probe(env, {"targets": targets, "probe_team": models.get("probe_team")})
        except Exception as e:  # the worker didn't answer: unknown, not refused
            logger.warning(f"Model check not run: {e}")
            answer = {"results": [{"model": t["model"], "id": t["model"], "status": "fail", "code": "unreachable",
                                   "detail": "The model check couldn't run.",
                                   "fix": "Check again in a few minutes; if it keeps failing, see the worker's log.",
                                   } for t in targets]}
        results = [dict(r, required=r.get("model") in required) for r in answer.get("results") or []]
        record = {
            "version": version, "checked_at": now.isoformat(),
            "provider": answer.get("provider"), "region": answer.get("region"),
            "inference_prefix": answer.get("inference_prefix"),
            "ok": not any(r.get("status") == "fail" for r in results),
            "results": results,
        }
    old = store.get("MODEL_CHECK") or {}
    # Admins hear about a refused model once per release, not every day.
    record["notified"] = bool(old.get("version") == version and old.get("notified"))
    store.put("MODEL_CHECK", record)
    return record


def needs_check(record, version, now):
    if not record or record.get("version") != version or record.get("pending"):
        return True
    try:
        at = datetime.fromisoformat(record["checked_at"])
    except (KeyError, ValueError):
        return True
    return now - at >= MODEL_RETRY


def blocked_text(record, env):
    lines = []
    for f in failures(record, required=True):
        lines.append(f"- {f.get('model')}: {f.get('detail')}\n  Fix: {f.get('fix')}" + (f"\n  {f['url']}" if f.get("url") else ""))
    return (
        f"CoderHelm won't update to {record['version']} until every model it calls answers.\n\n"
        + "\n".join(lines)
        + "\n\nAgreeing to model terms and the Bedrock use case form are done by a person in the AWS console; "
        "CoderHelm only checks. The check runs again daily, and the update applies by itself once it passes "
        "if auto-update is on. Check again now: "
        f"{app_url(env)}/settings/system/#readiness"
    )


def notify_blocked(store, env, record):
    if record.get("notified") or not failures(record, required=True):
        return
    notify(f"Update to {record['version']} needs your action", blocked_text(record, env), env)
    record["notified"] = True
    store.put("MODEL_CHECK", record)


def blocked_message(record):
    first = failures(record, required=True) or failures(record)
    f = first[0] if first else {}
    return f"Update to {record['version']} needs your action: {f.get('detail', 'a model was refused.')} {f.get('fix', '')}".strip()


# ── Actions ───────────────────────────────────────────────────────────────

def app_url(env):
    return (env.get("APP_URL") or "").rstrip("/")


def begin(store, env, now, version, actor, mode, extra=None, note=None):
    """Records the update in history and starts the build."""
    state = store.get("UPDATE_STATE", {})
    if running(state, now):
        raise ValueError("an update is already running")
    current = env.get("CODERHELM_VERSION", "")
    if parse_version(current):
        state["ran_versions"] = sorted(set(state.get("ran_versions") or []) | {current})
    hid = f"UPDATE#{now.strftime('%Y-%m-%dT%H:%M:%SZ')}#{uuid.uuid4().hex[:8]}"
    entry = {"version": version, "from_version": current, "actor": actor, "mode": mode,
             "status": "running", "started_at": now.isoformat(), "reason": ""}
    if note:
        entry["note"] = note
    store.put(hid, entry)
    build_id = start_build(env, version, actor, mode, hid, extra) if extra else start_build(env, version, actor, mode, hid)
    entry["build_id"] = build_id
    store.put(hid, entry)
    state.update({"running_since": now.isoformat(), "running_id": hid})
    store.put("UPDATE_STATE", state)
    return dict(entry, id=hid)


def handle(event, env, store, now, fetch=fetch_releases, probe=None):
    probe = probe or invoke_worker
    action = (event or {}).get("action", "tick")
    current = env.get("CODERHELM_VERSION", "")
    repo = env.get("RELEASE_REPO", "CoderHelm/coderhelm-platform")
    settings = validate_settings(store.get("AUTO_UPDATE", {}) or {}, defaults_from_env(env))
    state = store.get("UPDATE_STATE", {})

    if action == "status":
        rel = releases_cached(store, repo, now.timestamp(), fetch)
        latest = pick_latest(rel, settings["channel"], current)
        return {
            "current": {
                "version": current or None,
                "commit": env.get("CODERHELM_COMMIT") or None,
                "released_at": env.get("CODERHELM_RELEASED_AT") or None,
                "config_fingerprint": env.get("CODERHELM_CONFIG_FINGERPRINT") or None,
            },
            "latest": latest,
            "update_available": latest is not None,
            "releases": releases_freshness(store, now.timestamp()),
            "settings": settings,
            "running": running(state, now),
            # Releases whose last try failed or rolled back: the dashboard's
            # update preflight shows them with the reason from history.
            "failed_versions": sorted(state.get("failed_versions") or []),
            "history": store.history(),
            # The model check for the latest release (Settings, System and Setup).
            "model_check": (lambda mc: mc if latest and mc and mc.get("version") == latest["version"] else None)(
                store.get("MODEL_CHECK")),
        }

    if action == "settings":
        new = validate_settings(event.get("settings"), settings)
        store.put("AUTO_UPDATE", new)
        return {"settings": new}

    if action == "update_now":
        version = event.get("version")
        if not version:
            latest = pick_latest(releases_cached(store, repo, now.timestamp(), fetch), settings["channel"], current)
            if not latest:
                raise ValueError("already on the latest release")
            version = latest["version"]
        if not parse_version(version):
            raise ValueError("version must look like v1.2.3")
        # Forward only: an older (or the same) release is a rollback.
        if parse_version(current) and compare(version, current) <= 0:
            raise ValueError(f"{version} is not newer than {current}; use Roll back to go to an earlier release")
        if running(state, now):
            raise ValueError("an update is already running")
        mc = model_check(store, env, now, version, repo, probe)
        if mc.get("pending"):
            raise ModelsBlocked(f"Checking {version}'s signature and the models it calls first; try Update now again in a few minutes.", mc)
        if failures(mc, required=True):
            # No override: the release would call a model this install can't.
            raise ModelsBlocked(blocked_message(mc), mc)
        note = None
        if failures(mc):
            if not event.get("skip_check"):
                raise ModelsBlocked(blocked_message(mc) + " This release doesn't call it, so you can skip this check.",
                                    dict(mc, skippable=True))
            note = "model check skipped for " + ", ".join(f.get("model", "?") for f in failures(mc))
        return begin(store, env, now, version, event.get("actor") or "unknown", "update", note=note)

    if action == "models_verified":
        # From the release check build, after the signature check.
        version = event.get("version")
        if not parse_version(version):
            raise ValueError("version must look like v1.2.3")
        if event.get("status") != "verified":
            logger.warning(f"Release check for {version} failed: {event.get('reason')}")
            state.pop("verifying", None)
            store.put("UPDATE_STATE", state)
            return {"verified": False}
        m = event.get("manifest")
        if m is not None and (not isinstance(m, dict) or not isinstance(m.get("calls"), list) or m.get("version") not in (None, version)):
            raise ValueError("models.json isn't a model manifest for this release")
        store.put(f"MANIFEST#{version}", {"verified": True, "manifest": m, "at": now.isoformat()})
        state.pop("verifying", None)
        store.put("UPDATE_STATE", state)
        mc = model_check(store, env, now, version, repo, probe)
        notify_blocked(store, env, mc)
        return {"verified": True, "model_check": mc}

    if action == "model_check":
        version = event.get("version")
        if not version:
            latest = pick_latest(releases_cached(store, repo, now.timestamp(), fetch), settings["channel"], current)
            if not latest:
                return {"model_check": None}
            version = latest["version"]
        return {"model_check": model_check(store, env, now, version, repo, probe)}

    if action == "rollback":
        target = event.get("version")
        if not target:
            done = [h for h in store.history(50) if h.get("status") == "succeeded" and h.get("from_version")]
            if not done:
                raise ValueError("no earlier version to roll back to")
            target = done[0]["from_version"]
        if not parse_version(target):
            raise ValueError("version must look like v1.2.3")
        # Only to a release this install ran: an old signed release with a
        # since-fixed problem can't be pushed onto it.
        ran = ran_versions(store, state, current) - {current}
        if target not in ran:
            raise ValueError(f"{target} is not a version this install ran before, so it can't be rolled back to")
        return begin(store, env, now, target, event.get("actor") or "unknown", "rollback")

    if action == "finished":
        hid = event.get("history_id") or state.get("running_id")
        entry = store.get(hid, {}) if hid else {}
        status = event.get("status") if event.get("status") in ("succeeded", "failed", "rolled_back") else "failed"
        entry.update({"status": status, "reason": str(event.get("reason") or "")[:500], "finished_at": now.isoformat()})
        if hid:
            store.put(hid, entry)
        state.pop("running_since", None)
        state.pop("running_id", None)
        version = entry.get("version") or event.get("version")
        if entry.get("mode") == "domain":
            # A domain change redeploys the same release: it says nothing
            # about the release, so it never marks the version as failed.
            store.put("UPDATE_STATE", state)
            return domain_finished(store, env, now, entry, status)
        if status != "succeeded" and version:
            failed = set(state.get("failed_versions") or [])
            failed.add(version)
            state["failed_versions"] = sorted(failed)
        if status == "succeeded" and parse_version(version):
            state["ran_versions"] = sorted(set(state.get("ran_versions") or []) | {version})
        store.put("UPDATE_STATE", state)
        words = {"succeeded": "is now on", "failed": "could not update to", "rolled_back": "rolled back after trying"}[status]
        notify(
            f"CoderHelm {words} {version}",
            f"{entry.get('reason') or 'Done.'}\nActor: {entry.get('actor', 'auto')}\n{app_url(env)}/settings/system/",
            env,
        )
        return {"status": status, "version": version}

    if action == "refresh_secrets":
        revision = re.sub(r"[^A-Za-z0-9-]", "", str(event.get("revision") or uuid.uuid4().hex))[:64] or uuid.uuid4().hex
        names = [n for n in (env.get("REFRESH_FUNCTIONS") or "").split(",") if n]
        return {"refreshed": refresh_functions(client("lambda"), names, revision)}

    if action == "domain_status":
        return domain_status(store, env, now)

    if action == "domain_set":
        return domain_set(store, env, now, event.get("hostname"), event.get("actor") or "unknown")

    if action == "domain_remove":
        return domain_remove(store, env, now, event.get("actor") or "unknown")

    if action == "tick":
        rel = releases_cached(store, repo, now.timestamp(), fetch)
        latest = pick_latest(rel, settings["channel"], current)
        mc = store.get("MODEL_CHECK")
        # First sight of a release, and daily while a model is refused.
        if latest and not running(state, now) and needs_check(mc, latest["version"], now):
            mc = model_check(store, env, now, latest["version"], repo, probe)
            notify_blocked(store, env, mc)
        d = decide(now, settings, current, rel, state, mc)
        if d["action"] in ("notify", "update") and state.get("notified_version") != d.get("version"):
            latest = pick_latest(rel, settings["channel"], current) or {}
            notify(
                f"CoderHelm {d['version']} is available",
                f"You run {current or 'an unknown version'}. {d['reason'].capitalize()}.\n"
                f"{'This is a major release. ' if latest.get('breaking') else ''}"
                f"Release notes and Update now: {app_url(env)}/settings/system/",
                env,
            )
            state = store.get("UPDATE_STATE", {})
            state["notified_version"] = d["version"]
            store.put("UPDATE_STATE", state)
        if d["action"] == "update":
            # Proven again right before applying, whatever the last check said.
            mc = model_check(store, env, now, d["version"], repo, probe)
            if blocking(mc, d["version"]):
                notify_blocked(store, env, mc)
                d = {"action": "none", "version": d["version"],
                     "reason": "a model this release calls was refused; checking again daily"}
            else:
                begin(store, env, now, d["version"], "auto", "update")
        logger.info(json.dumps(d))
        return d

    raise ValueError(f"unknown action: {action}")


def handler(event, context):
    env = dict(os.environ)
    store = Store(client("dynamodb"), env["SETTINGS_TABLE_NAME"])
    now = datetime.now(timezone.utc)
    try:
        return {"ok": True, **handle(event, env, store, now)}
    except ModelsBlocked as e:
        return {"ok": False, "error": str(e), "model_check": e.record}
    except ValueError as e:
        return {"ok": False, "error": str(e)}
