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
- open_window  a hardened install's update build asks for its deploy window
            (infra/lib/access.ts). Only for the build this updater started,
            still running, with exactly the expected buildspec, image,
            variables and role: then this build's own session (aws:userid)
            may deploy through the install's deploy role until the window's
            end time. Closed on finished, and on every tick when no update
            runs. Installs without DEPLOY_WINDOW (not hardened) refuse it.
- model_check  every model the next release calls gets one test call through
            the install's provider (Settings, System, Models), made by the
            worker with the install's own role or key. Also before every
            update (automatic or Update now), on first sight of a release,
            and once a day while a model is refused. A refused model holds
            the update: admins are told which model, the fix and the console
            page, and it applies by itself once the check passes (with
            auto-update on). Nothing here accepts terms or subscribes to
            anything; a person does that in the AWS console.

A native install (one parent CloudFormation stack, NATIVE_STACK set) has no
update build: an update is a change set on that stack, proposed only from
a template whose sha256 the release check took from the signed SHA256SUMS
(see "A native install" below). An install from before native releases
has an update build (CodeBuild, infra/lib/updates-stack.ts) that runs
`coderhelm-selfhost update`: health check, checksum and signature check,
deploy, health check, and a rollback when the new version is unhealthy.
That build checks the release itself (infra/lib/release-verify.ts: the tag
is an annotated tag on a commit on main, signed with a maintainer key
pinned in the running release's selfhost/maintainers.json or attested by
auto-release.yml on main for that tag and commit; the signature was
made by exactly this repository's release.yml on that tag, from that same
commit), so the release list read here only decides what to offer, never
what is trusted. The release check build runs the same checks first, so a
release that fails them stays held and is never applied. Its report
(models_verified) is believed only once the updater has read that build's
own result from CodeBuild (confirm_check): the build it started, of its own
release check project, succeeded, and exported the same checksums. The release list is still checked:
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
import hashlib
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


# Hybrid: the hosted API (services/common/src/hybrid/mod.rs), used only once
# an admin chose to send email through CoderHelm. HYBRID_URL overrides it,
# or is "off" when the config turned hybrid off.
DEFAULT_HOSTED_URL = "https://api.coderhelm.com"
CODERHELM_ADDRESS = re.compile(r"@(?:[a-z0-9-]+\.)*coderhelm\.com$", re.I)
UPDATE_WHAT = ("available", "succeeded", "failed", "rolled_back", "blocked", "domain_live", "domain_failed", "domain_removed")
NOTICE_VERSION_RE = re.compile(r"^[A-Za-z0-9.+-]{1,40}$")


def email_transport(env):
    """How the install sends: Settings, Emails (services/common/src/mail), else
    the config's starting value, else its own SES. None when unreadable."""
    try:
        r = client("dynamodb").get_item(
            TableName=env["SETTINGS_TABLE_NAME"],
            Key={"pk": {"S": "SYSTEM"}, "sk": {"S": "SETTINGS#EMAIL_OPTIONAL"}},
            ConsistentRead=True,
        )
        data = (r.get("Item") or {}).get("data", {}).get("S")
        if data:
            t = json.loads(data).get("transport")
            if t in ("ses", "coderhelm"):
                return t
    except Exception as e:
        logger.warning(f"Email settings unreadable: {e}")
        return None
    t = (env.get("EMAIL_TRANSPORT_DEFAULT") or "ses").strip()
    return t if t in ("ses", "coderhelm") else "ses"


def sign_request(secret, ts, method, path, body):
    """The hybrid request signature (services/common/src/hybrid/sign.rs)."""
    import hashlib
    import hmac
    msg = f"{ts}.{method.upper()}.{path}.{hashlib.sha256(body).hexdigest()}"
    return "v1=" + hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()


def relay_call(env, creds, path, payload, opener=None):
    url_base = (env.get("HYBRID_URL") or DEFAULT_HOSTED_URL).strip().rstrip("/")
    if not url_base.startswith("https://"):
        raise RuntimeError("no hosted address for CoderHelm's mail service")
    body = json.dumps(payload).encode()
    ts = int(time.time())
    req = urllib.request.Request(
        url_base + path,
        data=body,
        method="POST",
        headers={
            "content-type": "application/json",
            "user-agent": "coderhelm-selfhost",
            "x-coderhelm-install": creds["id"],
            "x-coderhelm-timestamp": str(ts),
            "x-coderhelm-signature": sign_request(creds["secret"], ts, "POST", path, body),
        },
    )
    (opener or urllib.request.urlopen)(req, timeout=10).close()


def relay_credentials(env):
    try:
        raw = client("secretsmanager").get_secret_value(SecretId=env["SECRETS_NAME"])["SecretString"]
        s = json.loads(raw)
        if s.get("hybrid_install_id") and s.get("hybrid_install_secret"):
            return {"id": s["hybrid_install_id"], "secret": s["hybrid_install_secret"]}
    except Exception as e:
        logger.warning(f"Hybrid registration unreadable: {e}")
    return None


# ── Update notices ────────────────────────────────────────────────────────
#
# One notice, two routes: CoderHelm's mail relay writes it from the
# `update_notice` template (services/common/src/mail/templates.rs) with the
# parameters below; the install's own SES gets the same parts written here.
# Keep the two in step (the golden files under services/common/src/mail/golden
# show the relay's).

CLI = "npx -y github:CoderHelm/coderhelm-platform"
CODERHELM_ZONE = "coderhelm.com"
NOTICE_SUBJECTS = {
    "available": "{version} is available",
    "succeeded": "updated to {version}",
    "failed": "update to {version} failed",
    "rolled_back": "update to {version} rolled back",
    "blocked": "update to {version} needs your OK",
    "domain_live": "the new address is live",
    "domain_failed": "the address change failed",
    "domain_removed": "back on the AWS address",
}
# The relay refuses an email whose text reads as CoderHelm staff speaking
# (templates::uses_reserved_wording): such a part is left out instead.
RESERVED_PHRASES = ("coderhelm support", "coderhelm security", "coderhelm team", "coderhelm staff",
                    "coderhelm billing", "coderhelm trust", "coderhelm admin", "from coderhelm", "coderhelm account")
LIMITS = {"checked_at": 40, "reason": 1500, "action": 800, "waiting": 160, "notes": 1200}
AWS_HOST_RE = re.compile(r"cloudfront\.net|amazonaws\.com|awsapps\.com", re.I)


def checked_at(now):
    return now.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fit(text, limit):
    text = str(text or "").strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def reserved(text):
    squashed = re.sub(r"[^a-z0-9]+", " ", str(text).lower()).strip().replace("coder helm", "coderhelm")
    return any(p in squashed for p in RESERVED_PHRASES)


def org_name(env):
    """email.organization, else email.fromName without a trailing "CoderHelm"."""
    org = " ".join((env.get("ORGANIZATION_NAME") or "").split())
    if org:
        return org
    name = " ".join((env.get("EMAIL_FROM_NAME") or "").split())
    if not name or name.lower() == "coderhelm":
        return None
    if name.lower().endswith(" coderhelm"):
        name = name[: -len(" coderhelm")].strip()
    return name or None


def host_label(host):
    """The install's own address as a name: `acme` for acme.coderhelm.com, a
    custom domain as written, never an AWS host."""
    h = (host or "").strip().rstrip(".").lower()
    if not h or AWS_HOST_RE.search(h) or not re.fullmatch(r"[a-z0-9.-]{1,80}", h):
        return None
    if h.endswith("." + CODERHELM_ZONE):
        name = h[: -len(CODERHELM_ZONE) - 1]
        return name if name and "." not in name else None
    return h


def install_host(env):
    url = app_url(env)
    return url.split("://", 1)[-1].split("/", 1)[0]


def notice_label(env):
    """The install's name for its own SES mail: its organization, else its address."""
    org = org_name(env)
    if org and not AWS_HOST_RE.search(org):
        return org[:40]
    return host_label(install_host(env))


def subject_prefix(label):
    return f"{label} CoderHelm: " if label else "Your CoderHelm install: "


def sender_name(env):
    """email.fromName when an admin set one, else `<label> CoderHelm`."""
    name = " ".join((env.get("EMAIL_FROM_NAME") or "").split())
    if name and name != "CoderHelm" and not AWS_HOST_RE.search(name):
        return name
    label = notice_label(env)
    return f"{label} CoderHelm" if label else "CoderHelm"


def from_header(name, address):
    """RFC 5322 From: quoted ASCII name, or RFC 2047 encoded words."""
    import base64
    if "<" in address:
        return address
    name = " ".join(re.sub(r'["\\<>\x00-\x1f\x7f]', "", name or "").split())
    if not name:
        return address
    if name.isascii():
        return f'"{name}" <{address}>'
    words, chunk = [], ""
    for ch in name:
        if len((chunk + ch).encode()) > 45:
            words.append(chunk)
            chunk = ""
        chunk += ch
    words.append(chunk)
    enc = " ".join(f"=?UTF-8?B?{base64.b64encode(w.encode()).decode()}?=" for w in words if w)
    return f"{enc} <{address}>"


def notice_link(what, env):
    anchor = {"blocked": "#readiness", "domain_live": "#domain", "domain_failed": "#domain",
              "domain_removed": "#domain"}.get(what, "")
    return f"{app_url(env)}/settings/system/{anchor}"


def update_params(what, version, env, facts=None):
    """The relay's `update_notice` parameters: typed, within the template's
    limits, and only parts the relay will accept."""
    what = what if what in UPDATE_WHAT else "available"
    p = {"what": what, "link": notice_link(what, env)}
    if version and NOTICE_VERSION_RE.match(str(version)):
        p["version"] = str(version)
        if not what.startswith("domain_"):
            p["notes_link"] = f"{app_url(env)}/settings/system/#version"
    facts = facts or {}
    cur = facts.get("current")
    if cur and NOTICE_VERSION_RE.match(str(cur)):
        p["current"] = str(cur)
    for k, limit in LIMITS.items():
        v = facts.get(k)
        if not v:
            continue
        v = fit(" ".join(str(v).split()) if k in ("checked_at", "waiting") else v, limit)
        if not reserved(v):
            p[k] = v
    if facts.get("major"):
        p["major"] = "yes"
    return p


def notice(what, version, env, facts=None):
    """The subject, text and HTML of an update notice for the install's own
    SES, the same parts as the relay's template."""
    p = update_params(what, version, env, facts)
    v = dict(p, install=install_host(env))
    for k in ("reason", "action", "notes"):
        if (facts or {}).get(k):
            v[k] = fit((facts or {})[k], 4000)
    w = p["what"]
    parts = {
        "available": [
            ("p", "CoderHelm {version} is out, and this install can move to it."),
            ("facts", [("Install", "{install}"), ("Running now", "{current}"), ("New release", "{version}"),
                       ("Not applied yet because", "{waiting}"), ("Checked", "{checked_at}")]),
            ("p", "This is a major release. Read its notes before you update." if v.get("major") else ""),
            ("quote", "What's new", "notes"),
            ("p", "To update, open Settings, then System, and press Update now to {version}. Or run this where the install's AWS sign-in works:"),
            ("cmd", CLI + " update --to {version}"),
        ],
        "blocked": [
            ("p", "This install won't update to {version} until someone fixes what the update check found. Nothing has changed yet."),
            ("facts", [("Install", "{install}"), ("Running now", "{current}"), ("Waiting to install", "{version}"),
                       ("Checked", "{checked_at}")]),
            ("quote", "Why it's blocked", "reason"),
            ("quote", "What to do", "action"),
            ("p", "Once it is fixed, open Settings, then System, then Update readiness, and press Check again. Once it passes, press Update now to {version}. The check also runs by itself every day, and with automatic updates on the update then goes ahead on its own."),
            ("p", "From a terminal, see what fails and how to fix it, then update:"),
            ("cmd", CLI + " doctor"),
            ("cmd", CLI + " update --to {version}"),
        ],
        "succeeded": [
            ("p", "This install finished updating to {version}."),
            ("facts", [("Install", "{install}"), ("Updated from", "{current}"), ("Now running", "{version}"),
                       ("Finished", "{checked_at}")]),
            ("quote", "Notes from the update", "reason"),
            ("p", "Nothing to do. The release notes say what changed."),
        ],
        "failed": [
            ("p", "The update to {version} failed."),
            ("facts", [("Install", "{install}"), ("Was running", "{current}"), ("Tried to install", "{version}"),
                       ("Failed", "{checked_at}")]),
            ("quote", "What went wrong", "reason"),
            ("quote", "What to do", "action"),
            ("p", "Open Settings, then System, then Update history for the full log. Fix what it shows, then press Update now to try again. From a terminal:"),
            ("cmd", CLI + " doctor"),
            ("cmd", CLI + " update --to {version}"),
        ],
        "rolled_back": [
            ("p", "This install tried {version}, found it unhealthy, and went back to the release it ran before."),
            ("facts", [("Install", "{install}"), ("Running now", "{current}"), ("Tried", "{version}"),
                       ("Rolled back", "{checked_at}")]),
            ("quote", "What went wrong", "reason"),
            ("quote", "What to do", "action"),
            ("p", "Open Settings, then System, then Update history for the full log. Fix what it shows, then press Update now to try again. From a terminal:"),
            ("cmd", CLI + " doctor"),
            ("cmd", CLI + " update --to {version}"),
        ],
        "domain_live": [
            ("p", "This install now answers on its new address. Tell your team to sign in there from now on."),
            ("facts", [("Install", "{install}"), ("Changed", "{checked_at}")]),
        ],
        "domain_failed": [
            ("p", "This install couldn't change its address, so it still answers on the old one."),
            ("facts", [("Install", "{install}"), ("Tried", "{checked_at}")]),
            ("quote", "What went wrong", "reason"),
            ("p", "Open Settings, then System, then Domain to see the DNS records and certificate it waits for. Fix them and try again."),
        ],
        "domain_removed": [
            ("p", "This install is back on its AWS address. People sign in there from now on."),
            ("facts", [("Install", "{install}"), ("Changed", "{checked_at}")]),
        ],
    }[w]
    buttons = [("Open Update readiness" if w == "blocked" else "Open Settings, System", "link"),
               ("Read the release notes", "notes_link")]

    def slots(t):
        return re.findall(r"\{(\w+)\}", t)

    def complete(t):
        return bool(t) and all(v.get(n) for n in slots(t))

    def fill(t):
        return re.sub(r"\{(\w+)\}", lambda m: str(v.get(m.group(1), "")), t)

    def esc(t):
        return (t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace('"', "&quot;").replace("'", "&#39;"))

    text, html = [], []
    for part in parts:
        kind = part[0]
        if kind == "p" and complete(part[1]):
            text.append(fill(part[1]))
            html.append(f'<p style="color:#222;font-size:15px;line-height:1.5;margin:0 0 16px">{esc(fill(part[1]))}</p>')
        elif kind == "facts":
            rows = [(label, fill(val)) for label, val in part[1] if complete(val)]
            if rows:
                text.append("\n".join(f"{label}: {val}" for label, val in rows))
                html.append('<table role="presentation" style="border-collapse:collapse;margin:0 0 16px">' + "".join(
                    f'<tr><td style="color:#666;font-size:14px;padding:3px 16px 3px 0;vertical-align:top;white-space:nowrap">{esc(label)}</td>'
                    f'<td style="color:#222;font-size:14px;padding:3px 0;vertical-align:top">{esc(val)}</td></tr>' for label, val in rows) + "</table>")
        elif kind == "quote" and v.get(part[2]):
            q = str(v[part[2]])
            text.append(part[1] + ":\n" + "\n".join("> " + line for line in q.splitlines()))
            html.append(f'<p style="color:#222;font-size:14px;font-weight:600;margin:0 0 6px">{esc(part[1])}</p>'
                        f'<blockquote style="border-left:3px solid #ccc;margin:0 0 16px;padding:4px 12px;color:#444;white-space:pre-wrap">{esc(q)}</blockquote>')
        elif kind == "cmd" and complete(part[1]):
            text.append("    " + fill(part[1]))
            html.append('<pre style="background:#f5f5f5;border:1px solid #e5e5e5;border-radius:6px;padding:10px 12px;margin:0 0 16px;'
                        'font-family:SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;color:#222;white-space:pre-wrap;word-break:break-all">'
                        f"<code>{esc(fill(part[1]))}</code></pre>")
    for label, key in buttons:
        if v.get(key):
            text.append(f"{label}: {v[key]}")
            html.append(f'<p style="margin:0 0 16px"><a href="{esc(v[key])}" style="display:inline-block;background:#222;color:#fff;'
                        f'padding:10px 16px;border-radius:6px;text-decoration:none">{esc(label)}</a></p>')
    footer = f"Sent by the self-hosted CoderHelm install at {v['install']} to its admins."
    text += ["", footer]
    html.append(f'<p style="color:#777;font-size:12px;border-top:1px solid #ddd;padding-top:12px;margin-top:24px">{esc(footer)}</p>')
    base = NOTICE_SUBJECTS[w]
    subject = fill(base) if complete(base) else "an update notice"
    return {
        "subject": subject_prefix(notice_label(env)) + subject,
        "base_subject": subject,
        "text": "\n\n".join(text),
        "html": "<!DOCTYPE html><html><head><meta charset=\"utf-8\"></head><body style=\"font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#fff;padding:24px\">"
                + "".join(html) + "</body></html>",
        "params": p,
    }


def notify(subject, text, env=None, what="available", version=None, opener=None, facts=None):
    """Email every install admin by the install's route (its own SES, or
    CoderHelm's mail relay when an admin chose it, never both), and post to
    the updates webhook if set. A CoderHelm address is never emailed.
    `subject` and `text` are what the webhook posts; the emails are written
    from `what`, `version` and `facts` ([`notice`])."""
    env = os.environ if env is None else env
    admins = [a.strip() for a in (env.get("ADMIN_EMAILS") or "").split(",") if a.strip()]
    admins = [a for a in admins if not CODERHELM_ADDRESS.search(a)]
    sender = env.get("SES_FROM_ADDRESS")
    sent = 0
    n = notice(what, version, env, facts)
    transport = email_transport(env) if admins else None
    if admins and transport == "ses" and sender:
        try:
            client("sesv2").send_email(
                FromEmailAddress=from_header(sender_name(env), sender),
                Destination={"ToAddresses": admins},
                Content={"Simple": {"Subject": {"Data": n["subject"]},
                                    "Body": {"Text": {"Data": n["text"]}, "Html": {"Data": n["html"]}}}},
            )
            sent += 1
        except Exception as e:
            logger.warning(f"Update email not sent: {e}")
    elif admins and transport == "coderhelm":
        hosted = (env.get("HYBRID_URL") or DEFAULT_HOSTED_URL).strip()
        creds = relay_credentials(env) if hosted.startswith("https://") else None
        if not creds:
            logger.warning("Update email not sent: email goes through CoderHelm but this install isn't registered")
        else:
            try:
                relay_call(env, creds, "/hybrid/email/recipients", {
                    "request_id": uuid.uuid4().hex,
                    "by": admins[0],
                    "add": [{"email": a, "kind": "admin"} for a in admins],
                }, opener)
                for a in admins:
                    body = {
                        "request_id": uuid.uuid4().hex,
                        "to": a,
                        "template": "update_notice",
                        "params": n["params"],
                        "contact": admins[0],
                    }
                    org = org_name(env)
                    if org:
                        body["org"] = org
                    relay_call(env, creds, "/hybrid/email", body, opener)
                sent += 1
            except Exception as e:
                logger.warning(f"Update email not sent through CoderHelm: {e}")
    hook = env.get("UPDATE_WEBHOOK_URL")
    if hook and hook.startswith("https://"):
        try:
            body = json.dumps({"text": f"{subject_prefix(notice_label(env))}{subject}\n{text}"}).encode()
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
            subject = "back on the AWS address"
        else:
            dom = dict(dom, state="live", reason="")
            store.put("DOMAIN", dom)
            subject = f"now answers on {dom.get('hostname')}"
    else:
        dom = dict(dom, state="failed", reason=entry.get("reason") or "the build failed: see its log")
        store.put("DOMAIN", dom)
        subject = "the address change failed"
    what = "domain_failed" if status != "succeeded" else ("domain_removed" if removing else "domain_live")
    notify(subject, f"{entry.get('reason') or 'Done.'}\nActor: {entry.get('actor', 'auto')}\n{app_url(env)}/settings/system/#domain", env, what=what,
           facts={"checked_at": checked_at(now), "reason": dom.get("reason") if status != "succeeded" else None})
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
        build = start_check(env, version)
    except Exception as e:
        logger.warning(f"Release check not started: {e}")
        return False
    state["verifying"] = {"version": version, "at": now.isoformat(), "build": build}
    store.put("UPDATE_STATE", state)
    return True


def claim_problem(event, state, version):
    """Why a models_verified report can't be the build this updater started
    for this version ("" when it can)."""
    v = state.get("verifying") or {}
    if v.get("version") != version or not v.get("build"):
        return f"no release check of {version} is running"
    if event.get("build") != v.get("build"):
        return "the report doesn't come from the release check build this updater started"
    return ""


def build_vars(build, key):
    return {x.get("name"): x.get("value") for x in (build.get(key) or []) if isinstance(x, dict)}


def confirm_check(store, env, now, probe=None, cb=None):
    """Believes a release check's report only once CodeBuild says that build
    (the one this updater started, of its release check project, for that
    version) succeeded and exported the same checksums: the template's from
    the signed SHA256SUMS, and models.json's. Whoever else can call the
    updater can't make that true. Returns what happened."""
    state = store.get("UPDATE_STATE", {})
    v = state.get("verifying") or {}
    claim = v.get("claim")
    if not claim or not v.get("build"):
        return None
    try:
        builds = (cb or client("codebuild")).batch_get_builds(ids=[v["build"]]).get("builds") or []
    except Exception as e:
        logger.warning(f"Release check not read: {e}")
        return None
    b = builds[0] if builds else {}
    status = b.get("buildStatus")
    if status == "IN_PROGRESS" or not b:
        return "pending"
    version = v.get("version")
    exported = build_vars(b, "exportedEnvironmentVariables")
    problem = ""
    if status != "SUCCEEDED":
        problem = f"the release check build ended {status}"
    elif b.get("projectName") != env.get("RELEASE_CHECK_PROJECT_NAME"):
        problem = "the build isn't this install's release check"
    elif build_vars(b.get("environment") or {}, "environmentVariables").get("VERSION") != version:
        problem = "the build checked another version"
    elif (exported.get("TEMPLATE_SHA256") or "none") != (claim.get("template_sha256") or "none"):
        problem = "the reported template checksum isn't the one the build exported"
    elif (exported.get("MODELS_SHA256") or "none") != claim.get("models_sha256", "none"):
        problem = "the reported models.json isn't the one the build exported"
    state.pop("verifying", None)
    store.put("UPDATE_STATE", state)
    if problem:
        logger.warning(f"Release check of {version} not believed: {problem}")
        return "refused"
    store.put(f"MANIFEST#{version}", {"verified": True, "manifest": claim.get("manifest"), "template_sha256": claim.get("template_sha256"),
                                     "build": v["build"], "at": now.isoformat()})
    mc = model_check(store, env, now, version, env.get("RELEASE_REPO", RELEASE_REPO), probe or invoke_worker)
    notify_blocked(store, env, mc)
    return "verified"


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
        note = provider_note(answer, results)
        if note:
            record["note"] = note
    old = store.get("MODEL_CHECK") or {}
    # Admins hear about a refused model once per release, not every day.
    record["notified"] = bool(old.get("version") == version and old.get("notified"))
    store.put("MODEL_CHECK", record)
    return record


def provider_note(answer, results):
    """Why nothing was called: an install on Anthropic API keys checks the
    models through a team's key (the worker never calls Bedrock for it), and
    each team adds its own key, so with none yet there is nothing to check
    and nothing holds the update."""
    if answer.get("provider") != "anthropic" or not results:
        return ""
    if all(r.get("code") == "no_key" for r in results):
        return ("This install uses Anthropic API keys, added per team: no key to check the models with yet, "
                "so nothing holds the update. Each team's key is checked when it's added (Settings, AI models).")
    return "Checked with a team's Anthropic API key (this install doesn't use Bedrock)."


def needs_check(record, version, now):
    if not record or record.get("version") != version or record.get("pending"):
        return True
    try:
        at = datetime.fromisoformat(record["checked_at"])
    except (KeyError, ValueError):
        return True
    return now - at >= MODEL_RETRY


def blocked_reason(record):
    """Why the update is held: each refused model and what it said."""
    return "\n".join(f"- {f.get('model')}: {f.get('detail')}" for f in failures(record, required=True))


def blocked_action(record):
    """What a person does about it: each refused model's fix."""
    fixes = [f"- {f.get('model')}: {f.get('fix')}" for f in failures(record, required=True) if f.get("fix")]
    return "\n".join(fixes + ["Agreeing to model terms and the Bedrock use case form are done by a person in the AWS console; the install only checks."])


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


def checked(record):
    try:
        return datetime.fromisoformat(record["checked_at"])
    except (KeyError, TypeError, ValueError):
        return datetime.now(timezone.utc)


def notify_blocked(store, env, record):
    if record.get("notified") or not failures(record, required=True):
        return
    notify(f"update to {record['version']} needs your OK", blocked_text(record, env), env, what="blocked", version=record.get("version"),
           facts={"current": env.get("CODERHELM_VERSION"), "checked_at": checked_at(checked(record)),
                  "reason": blocked_reason(record), "action": blocked_action(record)})
    record["notified"] = True
    store.put("MODEL_CHECK", record)


def blocked_message(record):
    first = failures(record, required=True) or failures(record)
    f = first[0] if first else {}
    return f"Update to {record['version']} needs your action: {f.get('detail', 'a model was refused.')} {f.get('fix', '')}".strip()


# ── The deploy window (hardened installs) ────────────────────────────────

WINDOW_POLICY_NAME = "deploy-window"
WINDOW_USER_ID = "__CODERHELM_USER_ID__"
WINDOW_EXPIRES = "__CODERHELM_EXPIRES__"
USER_ID_RE = re.compile(r"^(AROA[A-Z0-9]{8,}):([\w+=,.@-]{2,64})$")


def window_config(env):
    raw = env.get("DEPLOY_WINDOW") or ""
    return json.loads(raw) if raw else None


def canonical_json(value):
    """Sorted keys, no spaces: equal to canonicalJson in infra/lib/updates-stack.ts."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def buildspec_sha256(text):
    try:
        spec = json.loads(text or "")
    except ValueError:
        return None
    return hashlib.sha256(canonical_json(spec).encode("utf-8")).hexdigest()


def build_problems(build, cfg, env, account=None):
    """Why a build may not get the deploy window (empty: it may)."""
    p = []
    if build.get("projectName") != env.get("UPDATE_PROJECT_NAME"):
        p.append("it is not the update project's build")
    if build.get("buildStatus") != "IN_PROGRESS":
        p.append(f"it is {build.get('buildStatus')}, not running")
    src = build.get("source") or {}
    if src.get("type") != "NO_SOURCE" or build.get("secondarySources"):
        p.append("its source was changed")
    if buildspec_sha256(src.get("buildspec")) != cfg["buildspec"]:
        p.append("its buildspec is not the one this release defines")
    e = build.get("environment") or {}
    if e.get("image") != cfg["image"] or e.get("type") != "LINUX_CONTAINER" or e.get("privilegedMode"):
        p.append("its build image was changed")
    allowed = set(cfg.get("variables") or [])
    for v in e.get("environmentVariables") or []:
        if v.get("name") not in allowed or v.get("type", "PLAINTEXT") != "PLAINTEXT":
            p.append(f"it sets {v.get('name')}, which the update build doesn't take")
    if not str(build.get("serviceRole") or "").endswith(f":role/{cfg['role']}"):
        p.append("it runs as another role")
    return p


def window_document(env, cfg, user_id, until, ssm=None):
    ssm = ssm or client("ssm")
    statements = []
    for i in range(1, int(cfg.get("parts") or 1) + 1):
        part = ssm.get_parameter(Name=f"{cfg['param']}/{i}")["Parameter"]["Value"]
        statements.extend(json.loads(part))
    text = json.dumps({"Version": "2012-10-17", "Statement": statements}, separators=(",", ":"))
    return text.replace(WINDOW_USER_ID, user_id).replace(WINDOW_EXPIRES, until)


def open_window(store, env, now, event, cb=None, iam=None, ssm=None):
    cfg = window_config(env)
    if not cfg:
        raise ValueError("this install has no deploy window (it isn't hardened)")
    build_id = str(event.get("build_id") or "")
    user_id = str(event.get("user_id") or "")
    hid = event.get("history_id") or ""
    entry = store.get(hid, {}) if hid else {}
    if not entry or entry.get("build_id") != build_id or entry.get("status") != "running":
        raise ValueError(f"build {build_id or '(none)'} is not an update this updater started and is running")
    cb = cb or client("codebuild")
    builds = cb.batch_get_builds(ids=[build_id]).get("builds") or []
    if not builds:
        raise ValueError(f"build {build_id} was not found")
    problems = build_problems(builds[0], cfg, env)
    if problems:
        raise ValueError("the deploy window was refused: " + "; ".join(problems))
    iam = iam or client("iam")
    m = USER_ID_RE.match(user_id)
    role_id = iam.get_role(RoleName=cfg["role"])["Role"]["RoleId"]
    if not m or m.group(1) != role_id:
        raise ValueError("the deploy window was refused: the caller is not the update build's role")
    until = (now + timedelta(minutes=int(cfg.get("minutes") or 100))).strftime("%Y-%m-%dT%H:%M:%SZ")
    iam.put_role_policy(RoleName=cfg["role"], PolicyName=WINDOW_POLICY_NAME,
                        PolicyDocument=window_document(env, cfg, user_id, until, ssm))
    entry["window_until"] = until
    store.put(hid, entry)
    logger.info(json.dumps({"window": "open", "build": build_id, "until": until}))
    return {"window": "open", "until": until}


def close_window(env, iam=None):
    """Removes the window (nothing to do when there is none)."""
    cfg = window_config(env)
    if not cfg:
        return False
    iam = iam or client("iam")
    try:
        iam.delete_role_policy(RoleName=cfg["role"], PolicyName=WINDOW_POLICY_NAME)
        return True
    except Exception as e:  # NoSuchEntity: already closed
        if "NoSuchEntity" not in type(e).__name__ + str(e):
            logger.error("closing the deploy window failed: %s", e)
        return False



# ── Native releases: an update is a stack update ──────────────────────────
#
# A native install (NATIVE_STACK set) is one parent CloudFormation stack.
# An update proposes a change set on it to the release's parent template:
# only after the release check reported the template's sha256 from the
# signed SHA256SUMS (models_verified), and only when the template downloaded
# from the release's write-once URL matches it. Without DEPLOY_ROLE_ARN
# (auto-update off) the change set waits for an admin to review and execute
# it in the console; with it, the updater executes it through that role.
# native_poll (every 5 minutes) follows it to the end and reports it like a
# finished build. Every reason comes from CloudFormation's own words.

NATIVE_CAPABILITIES = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"]
NATIVE_HOST = "coderhelm-public.s3.amazonaws.com"


def is_native(env):
    return bool(env.get("NATIVE_STACK"))


def native_template_url(env, version):
    base = (env.get("NATIVE_TEMPLATE_BASE") or f"https://{NATIVE_HOST}/launch").rstrip("/")
    return f"{base}/{version}/{env.get('NATIVE_TEMPLATE_FILE') or 'coderhelm.json'}"


def fetch_template(url, opener=None):
    if not url.startswith(f"https://{NATIVE_HOST}/launch/v"):
        raise ValueError(f"not a CoderHelm release template: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "coderhelm-updater"})
    with (opener or urllib.request.urlopen)(req, timeout=20) as r:
        return r.read()


def native_parameters(stack, template, overrides=None):
    """Every parameter the new template declares: an override, else the
    stack's current value, else the new template's default."""
    overrides = overrides or {}
    have = {p["ParameterKey"] for p in stack.get("Parameters") or []}
    out = []
    for key in sorted((template.get("Parameters") or {}).keys()):
        if key in overrides:
            out.append({"ParameterKey": key, "ParameterValue": str(overrides[key])})
        elif key in have:
            out.append({"ParameterKey": key, "UsePreviousValue": True})
    return out


def native_propose(store, env, version, mode, extra=None, cfn=None, opener=None):
    """Creates the change set. Returns {change_set_id, apply}."""
    cfn = cfn or client("cloudformation")
    url = native_template_url(env, version)
    body = fetch_template(url, opener)
    rec = store.get(f"MANIFEST#{version}") or {}
    want = rec.get("template_sha256")
    if not want:
        # Every change set, a domain move included, is made only from a template the release check verified.
        if mode == "domain":
            start_verify(store, env, datetime.now(timezone.utc), version)
        raise ValueError(f"{version}'s stack template isn't verified yet (its release check reports it); try again in a few minutes")
    got = hashlib.sha256(body).hexdigest()
    if got != want:
        raise ValueError(f"{version}'s stack template doesn't match its signed checksum, so it was not proposed")
    current = env.get("CODERHELM_VERSION", "")
    if mode not in ("update", "rollback", "domain"):
        raise ValueError(f"unknown change: {mode}")
    if parse_version(current):
        # Every kind of change set, not only an update: a domain move keeps the
        # running release, and going back only to one this install ran.
        if mode == "update" and compare(version, current) < 0:
            raise ValueError(f"{version} is older than {current}: going back is a rollback")
        if mode == "domain" and version != current:
            raise ValueError(f"a domain change keeps the running release ({current}), not {version}")
        if mode == "rollback" and version not in ran_versions(store, store.get("UPDATE_STATE", {}), current):
            raise ValueError(f"{version} is not a version this install ran before, so it can't be rolled back to")
    template = json.loads(body)
    if template.get("Transform"):
        # CloudFormation stores such a template already resolved, so what it
        # fetched could never be compared with what was verified.
        raise ValueError(f"{version}'s stack template uses a transform, which the updater can't check after CloudFormation reads it; update it in the console with a change set")
    stack = cfn.describe_stacks(StackName=env["NATIVE_STACK"])["Stacks"][0]
    overrides = {}
    if mode == "domain" and extra:
        attach = extra.get("DOMAIN_ACTION") != "remove"
        overrides = {
            "Hostname": extra.get("DOMAIN_HOSTNAME", "") if attach else "",
            "DomainCertificateArn": extra.get("DOMAIN_CERT", "") if attach else "",
            "DomainZoneId": extra.get("DOMAIN_ZONE_ID", "") if attach else "",
        }
    name = f"coderhelm-{re.sub(r'[^A-Za-z0-9]', '-', version)}-{uuid.uuid4().hex[:6]}"
    kwargs = {
        "StackName": env["NATIVE_STACK"],
        "ChangeSetName": name,
        "ChangeSetType": "UPDATE",
        "TemplateURL": url,
        "Parameters": native_parameters(stack, template, overrides),
        "Capabilities": NATIVE_CAPABILITIES,
        "Description": f"CoderHelm {mode}: {version}"[:1024],
        # The parts' own change sets too: every table, bucket, key, user pool
        # and secret change is read before anything runs.
        "IncludeNestedStacks": True,
    }
    role = env.get("DEPLOY_ROLE_ARN") or ""
    if role:
        kwargs["RoleARN"] = role
    # The parts' templates must be readable while CloudFormation builds the
    # nested change sets: the helper copies them into the install's bucket
    # first, each checked against this verified template.
    stage_release(env, version, want)
    r = cfn.create_change_set(**kwargs)
    return {"change_set_id": r["Id"], "apply": bool(role), "template_canonical": canonical_sha(template)}


DATA_TYPES = ("AWS::DynamoDB::GlobalTable", "AWS::DynamoDB::Table", "AWS::S3::Bucket", "AWS::KMS::Key", "AWS::Cognito::UserPool", "AWS::SecretsManager::Secret")


def canonical_sha(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def data_changes(cfn, cs):
    """Tables, buckets, keys, user pools and secrets the change set (and its
    parts' nested change sets) would replace or remove."""
    out = []
    seen = set()

    def walk(c, part):
        for ch in c.get("Changes") or []:
            rc = ch.get("ResourceChange") or {}
            if rc.get("ResourceType") in DATA_TYPES and (rc.get("Action") == "Remove" or rc.get("Replacement") in ("True", "Conditional")):
                out.append(f"{part}.{rc.get('LogicalResourceId')} ({rc.get('ResourceType')}: {rc.get('Action')}, replacement {rc.get('Replacement', 'False')})")
            nested = rc.get("ChangeSetId")
            if rc.get("ResourceType") == "AWS::CloudFormation::Stack" and nested and nested not in seen:
                seen.add(nested)
                walk(cfn.describe_change_set(ChangeSetName=nested), rc.get("LogicalResourceId"))
    walk(cs, "parent")
    return out


# What the auto-update role may not change about itself (the boundary's
# TheBoundaryStays and TheDeployRoleStays): a change set that touches one
# would fail and roll back through that role, so an admin applies it.
ADMIN_ONLY = ("DeployRole", "Boundary", "DeployBoundary")


def admin_changes(cs):
    """The parent's auto-update role and boundaries the change set changes."""
    out = []
    for ch in cs.get("Changes") or []:
        rc = ch.get("ResourceChange") or {}
        if rc.get("LogicalResourceId") in ADMIN_ONLY:
            out.append(f"{rc.get('LogicalResourceId')} ({rc.get('Action')})")
    return out


# What auto-update never applies by itself, in the parent or any part: a
# change to who may do what. IAM resources, function permissions and URLs,
# resource policies, and a principal other than an AWS service anywhere.
POLICY_TYPES = (
    "AWS::Lambda::Permission", "AWS::Lambda::Url", "AWS::Lambda::LayerVersionPermission",
    "AWS::S3::BucketPolicy", "AWS::SQS::QueuePolicy", "AWS::SNS::TopicPolicy", "AWS::SecretsManager::ResourcePolicy",
    "AWS::Logs::ResourcePolicy", "AWS::Events::EventBusPolicy", "AWS::OpenSearchService::Domain",
)
# Properties that are resource policies on other types (a key's, a table's, a secret's, a bus's).
POLICY_PROPERTIES = ("KeyPolicy", "ResourcePolicy", "PolicyDocument", "Policy", "PolicyText", "AccessPolicies")


def principals(value):
    """Every principal a property names, as text ("Service:x" for services)."""
    out = set()

    def walk(v):
        if isinstance(v, dict):
            for k, x in v.items():
                if k in ("Principal", "NotPrincipal"):
                    if isinstance(x, dict):
                        for kind, ids in x.items():
                            for i in (ids if isinstance(ids, list) else [ids]):
                                out.add(f"{kind}:{json.dumps(i, sort_keys=True) if not isinstance(i, str) else i}")
                    else:
                        out.add(f"*:{x}")
                else:
                    walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)
    walk(value)
    return out


def context(raw):
    if isinstance(raw, dict):
        return raw
    try:
        v = json.loads(raw or "{}")
    except ValueError:
        return {"unreadable": raw}
    return v if isinstance(v, dict) else {}


def permission_change(rc):
    """Why this resource change alters permissions ("" when it doesn't)."""
    if rc.get("Action") not in ("Add", "Modify", "Import", "Dynamic"):
        return ""
    t = rc.get("ResourceType") or ""
    if t.startswith("AWS::IAM::"):
        return "an IAM change"
    if t in POLICY_TYPES:
        return "a resource policy"
    before, after = context(rc.get("BeforeContext")), context(rc.get("AfterContext"))
    before, after = before.get("Properties", before), after.get("Properties", after)
    for k in POLICY_PROPERTIES:
        if k in after and after.get(k) != before.get(k):
            return f"its {k}"
    new = {p for p in principals(after) - principals(before) if not p.startswith("Service:")}
    if new:
        return "a principal: " + ", ".join(sorted(new))[:200]
    return ""


def change_set_pages(cfn, cs_id, stack=None):
    """Every change of a change set, its property values included."""
    kw = {"ChangeSetName": cs_id, "IncludePropertyValues": True}
    if stack:
        kw["StackName"] = stack
    while True:
        page = cfn.describe_change_set(**kw)
        for ch in page.get("Changes") or []:
            yield ch
        if not page.get("NextToken"):
            return
        kw["NextToken"] = page["NextToken"]


def permission_changes(cfn, cs_id, stack):
    """What the change set (and each part's nested change set) changes about
    who may do what, read with property values (before and after)."""
    out = []
    seen = set()

    def walk(cid, part, stack_name):
        for ch in change_set_pages(cfn, cid, stack_name):
            rc = ch.get("ResourceChange") or {}
            why = permission_change(rc)
            if why:
                out.append(f"{part}.{rc.get('LogicalResourceId')} ({rc.get('ResourceType')} {rc.get('Action')}: {why})")
            nested = rc.get("ChangeSetId")
            if rc.get("ResourceType") == "AWS::CloudFormation::Stack" and nested and nested not in seen:
                seen.add(nested)
                walk(nested, rc.get("LogicalResourceId"), None)
    walk(cs_id, "parent", stack)
    return out


def stage_release(env, version, template_sha256, lam=None):
    """Asks the install's helper (lambda/native-helper, "stage") to copy the
    release's child templates into the install's assets bucket."""
    fn = env.get("NATIVE_HELPER_FUNCTION") or ""
    if not fn:
        raise ValueError("this install has no helper to stage the release's files")
    out = (lam or client("lambda")).invoke(
        FunctionName=fn, InvocationType="RequestResponse",
        Payload=json.dumps({"action": "stage", "version": version, "template_sha256": template_sha256}).encode(),
    )
    raw = out["Payload"].read() or b"{}"
    try:
        r = json.loads(raw)
    except ValueError:
        r = {}
    if out.get("FunctionError") or not r.get("ok"):
        raise ValueError(f"{version}'s files couldn't be staged for the change set: {r.get('error') or raw[:300]!r}")
    return r


def fetched_template_matches(cfn, env, cs_id, want):
    """The template CloudFormation fetched for the change set is the one verified (no swap after the check)."""
    if not want:
        return False
    body = cfn.get_template(StackName=env["NATIVE_STACK"], ChangeSetName=cs_id, TemplateStage="Original").get("TemplateBody")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except ValueError:
            return False
    return canonical_sha(body) == want


def native_status(store, env, state):
    hid = state.get("running_id") or state.get("review_id")
    entry = store.get(hid, {}) if hid else {}
    return {
        "stack": env.get("NATIVE_STACK"),
        "auto_apply": bool(env.get("DEPLOY_ROLE_ARN")),
        "pending": dict(entry.get("change_set") or {}, version=entry.get("version"), status=entry.get("status")) if entry.get("change_set_id") else None,
    }


def change_set_link(region, stack_id, cs_id):
    from urllib.parse import quote
    return (f"https://{region}.console.aws.amazon.com/cloudformation/home?region={region}"
            f"#/stacks/changesets/changes?stackId={quote(stack_id, safe='')}&changeSetId={quote(cs_id, safe='')}")


def change_set_summary(cs, region):
    """What an admin reviews: counts, replacements, the parts it touches, the link."""
    counts = {"add": 0, "modify": 0, "remove": 0}
    replace = []
    parts = []
    for c in cs.get("Changes") or []:
        rc = c.get("ResourceChange") or {}
        action = (rc.get("Action") or "").lower()
        if action in counts:
            counts[action] += 1
        if rc.get("Replacement") == "True":
            replace.append(rc.get("LogicalResourceId"))
        if rc.get("ResourceType") == "AWS::CloudFormation::Stack":
            parts.append(rc.get("LogicalResourceId"))
    return {
        "name": cs.get("ChangeSetName"),
        "status": cs.get("Status"),
        "execution": cs.get("ExecutionStatus"),
        "reason": cs.get("StatusReason") or "",
        "counts": counts,
        "replacements": sorted(x for x in replace if x),
        "parts": sorted(x for x in parts if x),
        "role": bool(cs.get("RoleARN")),
        "data_changes": [],
        "link": change_set_link(region, cs.get("StackId", ""), cs.get("ChangeSetId", "")),
    }


def first_failure(cfn, stack_name, since):
    """CloudFormation's own reason for the first resource that failed since `since`."""
    try:
        events = cfn.describe_stack_events(StackName=stack_name).get("StackEvents") or []
    except Exception as e:
        return f"could not read the stack's events: {e}"
    failed = [e for e in events if str(e.get("ResourceStatus", "")).endswith("_FAILED")
              and (not since or str(e.get("Timestamp", "")) >= since)]
    if not failed:
        return ""
    e = failed[-1]  # events are newest first: the last one is the first failure
    return f"{e.get('LogicalResourceId')}: {e.get('ResourceStatusReason') or e.get('ResourceStatus')}"[:500]


def native_poll(store, env, now, cfn=None):
    """Moves a proposed or running stack update along; returns what happened."""
    cfn = cfn or client("cloudformation")
    state = store.get("UPDATE_STATE", {})
    hid = state.get("running_id") or state.get("review_id")
    if not hid:
        return {"native": "idle"}
    entry = store.get(hid, {})
    cs_id = entry.get("change_set_id")
    if not cs_id:
        return {"native": "idle"}
    region = env.get("AWS_REGION") or ""
    try:
        cs = cfn.describe_change_set(ChangeSetName=cs_id, StackName=env["NATIVE_STACK"])
    except Exception as e:
        if "does not exist" in str(e) or "ChangeSetNotFound" in type(e).__name__:
            return native_finish(store, env, now, hid, "failed", "the proposed change set was deleted before it ran")
        raise
    summary = change_set_summary(cs, region)
    entry["change_set"] = summary
    status, execution = cs.get("Status"), cs.get("ExecutionStatus")
    if status == "FAILED":
        reason = cs.get("StatusReason") or "the change set could not be created"
        if "didn't contain changes" in reason or "No updates" in reason:
            return native_finish(store, env, now, hid, "succeeded", "already up to date: nothing to change")
        return native_finish(store, env, now, hid, "failed", reason)
    if status != "CREATE_COMPLETE":
        store.put(hid, entry)
        return {"native": "preparing"}
    if execution == "AVAILABLE" and "checked" not in entry:
        if not fetched_template_matches(cfn, env, cs_id, entry.get("template_canonical")):
            try:
                cfn.delete_change_set(ChangeSetName=cs_id, StackName=env["NATIVE_STACK"])
            except Exception as e:
                logger.warning(f"change set not deleted: {e}")
            return native_finish(store, env, now, hid, "failed", "the template CloudFormation fetched is not the verified one, so the change set was deleted")
        entry["data_changes"] = data_changes(cfn, cs)
        entry["admin_changes"] = admin_changes(cs)
        entry["permission_changes"] = permission_changes(cfn, cs_id, env["NATIVE_STACK"])
        entry["checked"] = True
        summary["data_changes"] = entry["data_changes"]
        entry["change_set"] = summary
    if execution == "AVAILABLE":
        if entry.get("data_changes") and entry.get("apply"):
            # Never automatic when data would be replaced or removed: a person decides.
            entry["apply"] = False
            entry["reason"] = "auto-update held: it would replace or remove " + "; ".join(entry["data_changes"])[:400]
        elif entry.get("admin_changes") and entry.get("apply"):
            # The auto-update role can't change itself or a boundary: an admin executes it.
            entry["apply"] = False
            entry["reason"] = "auto-update held: it changes the auto-update role or a boundary (" + "; ".join(entry["admin_changes"])[:300] + "), which only an admin can apply"
        elif entry.get("permission_changes") and entry.get("apply"):
            # Never automatic when it changes who may do what: a person reads it first.
            entry["apply"] = False
            entry["reason"] = "auto-update held: it changes permissions (" + "; ".join(entry["permission_changes"])[:400] + "), which an admin reviews and applies"
        if entry.get("apply") and not entry.get("executed_at"):
            cfn.execute_change_set(ChangeSetName=cs_id, StackName=env["NATIVE_STACK"])
            entry.update({"executed_at": now.isoformat(), "status": "running"})
            store.put(hid, entry)
            return {"native": "executing"}
        if entry.get("status") != "review":
            # Waiting on an admin: not "running", so it never blocks or goes stale.
            entry["status"] = "review"
            store.put(hid, entry)
            state.pop("running_since", None)
            state.pop("running_id", None)
            state["review_id"] = hid
            store.put("UPDATE_STATE", state)
            notify(f"CoderHelm {entry.get('version')} is ready to review",
                   f"The stack update is prepared. Review it and choose Execute in the AWS console:\n{summary['link']}",
                   env, what="review", version=entry.get("version"))
        else:
            store.put(hid, entry)
        return {"native": "review"}
    if execution in ("EXECUTE_IN_PROGRESS", "EXECUTE_COMPLETE", "EXECUTE_FAILED"):
        if entry.get("status") == "review":
            # The admin executed it in the console.
            entry.update({"status": "running", "executed_at": entry.get("executed_at") or now.isoformat()})
            state.pop("review_id", None)
            state.update({"running_since": now.isoformat(), "running_id": hid})
            store.put("UPDATE_STATE", state)
        store.put(hid, entry)
        st = cfn.describe_stacks(StackName=env["NATIVE_STACK"])["Stacks"][0]["StackStatus"]
        if st.endswith("_IN_PROGRESS"):
            return {"native": "running", "stack": st}
        if st in ("UPDATE_COMPLETE",):
            return native_finish(store, env, now, hid, "succeeded", "")
        reason = first_failure(cfn, env["NATIVE_STACK"], entry.get("executed_at")) or f"the stack ended {st}"
        result = "rolled_back" if "ROLLBACK_COMPLETE" in st else "failed"
        return native_finish(store, env, now, hid, result, reason)
    if execution in ("OBSOLETE", "UNAVAILABLE"):
        return native_finish(store, env, now, hid, "failed",
                             cs.get("StatusReason") or "the change set can no longer run (the stack changed since it was made)")
    store.put(hid, entry)
    return {"native": str(execution or status).lower()}


def native_finish(store, env, now, hid, status, reason):
    state = store.get("UPDATE_STATE", {})
    if state.get("review_id") == hid:
        state.pop("review_id", None)
        state.update({"running_id": hid})
        store.put("UPDATE_STATE", state)
    return handle({"action": "finished", "history_id": hid, "status": status, "reason": reason}, env, store, now)


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
    if is_native(env):
        try:
            entry.update(native_propose(store, env, version, mode, extra))
        except ValueError as e:
            entry.update({"status": "failed", "reason": str(e)[:500], "finished_at": now.isoformat()})
            store.put(hid, entry)
            raise
        entry["status"] = "preparing"
    else:
        build_id = start_build(env, version, actor, mode, hid, extra) if extra else start_build(env, version, actor, mode, hid)
        entry["build_id"] = build_id
    store.put(hid, entry)
    state.update({"running_since": now.isoformat(), "running_id": hid})
    store.put("UPDATE_STATE", state)
    return dict(entry, id=hid)


def install_identity(env):
    """The install's name and region (its resources are coderhelm-<name>-*), or None for a custom prefix."""
    prefix = env.get("RESOURCE_PREFIX") or ""
    region = env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or ""
    m = re.fullmatch(r"coderhelm-([a-z][a-z0-9-]{0,19})", prefix)
    return {"stage": m.group(1), "region": region} if m and region else None


def handle(event, env, store, now, fetch=fetch_releases, probe=None):
    probe = probe or invoke_worker
    action = (event or {}).get("action", "tick")
    current = env.get("CODERHELM_VERSION", "")
    repo = env.get("RELEASE_REPO", "CoderHelm/coderhelm-platform")
    settings = validate_settings(store.get("AUTO_UPDATE", {}) or {}, defaults_from_env(env))
    if action in ("tick", "status", "update_now", "model_check", "native_poll", "domain_status") and ((store.get("UPDATE_STATE", {}) or {}).get("verifying") or {}).get("claim"):
        confirm_check(store, env, now, probe)
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
            # Which install this is, for Settings, System, Uninstall's link to the uninstall page.
            "install": install_identity(env),
            # A native install: the stack, and the change set an admin reviews.
            "native": native_status(store, env, state) if is_native(env) else None,
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
        # From the release check build, after the signature check. Only a
        # claim until confirm_check reads that build's own result.
        version = event.get("version")
        if not parse_version(version):
            raise ValueError("version must look like v1.2.3")
        problem = claim_problem(event, state, version)
        if problem:
            raise ValueError(problem)
        if event.get("status") != "verified":
            logger.warning(f"Release check for {version} failed: {event.get('reason')}")
            state.pop("verifying", None)
            store.put("UPDATE_STATE", state)
            return {"verified": False}
        sha = event.get("template_sha256")
        if sha is not None and not (isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{64}", sha)):
            raise ValueError("template_sha256 must be a sha256")
        text = event.get("manifest_text")
        if text is not None and not isinstance(text, str):
            raise ValueError("manifest_text must be models.json's text")
        m = None
        if text is not None:
            try:
                m = json.loads(text)
            except ValueError:
                raise ValueError("models.json isn't JSON")
            if not isinstance(m, dict) or not isinstance(m.get("calls"), list) or m.get("version") not in (None, version):
                raise ValueError("models.json isn't a model manifest for this release")
        state["verifying"]["claim"] = {
            "template_sha256": sha, "manifest": m,
            "models_sha256": hashlib.sha256(text.encode()).hexdigest() if text is not None else "none",
        }
        store.put("UPDATE_STATE", state)
        return {"verified": confirm_check(store, env, now, probe) or "pending"}

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

    if action == "open_window":
        return open_window(store, env, now, event)

    if action == "finished":
        close_window(env)
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
        notify(
            NOTICE_SUBJECTS[status].format(version=version),
            f"{entry.get('reason') or 'Done.'}\nActor: {entry.get('actor', 'auto')}\n{app_url(env)}/settings/system/",
            env,
            what=status,
            version=version,
            facts={
                "current": entry.get("from_version"),
                "checked_at": checked_at(now),
                "reason": entry.get("reason"),
                "action": None if status == "succeeded" else
                "Open the update's log under Settings, System, Update history, fix what it names, then press Update now.",
            },
        )
        return {"status": status, "version": version}

    if action == "refresh_secrets":
        revision = re.sub(r"[^A-Za-z0-9-]", "", str(event.get("revision") or uuid.uuid4().hex))[:64] or uuid.uuid4().hex
        names = [n for n in (env.get("REFRESH_FUNCTIONS") or "").split(",") if n]
        return {"refreshed": refresh_functions(client("lambda"), names, revision)}

    if action == "domain_status":
        return domain_status(store, env, now)

    if action == "native_poll":
        if not is_native(env):
            raise ValueError("native_poll: not a native install")
        return native_poll(store, env, now)

    if action == "domain_set":
        return domain_set(store, env, now, event.get("hostname"), event.get("actor") or "unknown")

    if action == "domain_remove":
        return domain_remove(store, env, now, event.get("actor") or "unknown")

    if action == "tick":
        # No update running: no deploy window either.
        if not running(state, now):
            close_window(env)
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
                f"{d['version']} is available",
                f"You run {current or 'an unknown version'}. {d['reason'].capitalize()}.\n"
                f"{'This is a major release. ' if latest.get('breaking') else ''}"
                f"Release notes and Update now: {app_url(env)}/settings/system/",
                env,
                what="available",
                version=d["version"],
                facts={"current": current, "checked_at": checked_at(now), "waiting": d["reason"],
                       "notes": latest.get("notes"), "major": latest.get("breaking")},
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
