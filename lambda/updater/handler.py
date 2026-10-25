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
- rollback  starts the build for the version before the last update.
- finished  the update build reports its result; history and notices.
- refresh_secrets  the gateway stored new keys in the install's secret (the
            GitHub App, from the install guide): restart the services that
            read it at start, by stamping SECRETS_REVISION on each.

The build itself (CodeBuild, infra/lib/updates-stack.ts) runs
`coderhelm-selfhost update`: health check, checksum and signature check,
deploy, health check, and a rollback when the new version is unhealthy.

State lives on the settings table under pk SYSTEM.
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
RUNNING_STALE = timedelta(hours=3)
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


def decide(now_utc, settings, current, releases, state):
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


def client(name):
    if name not in _clients:
        import boto3
        _clients[name] = boto3.client(name)
    return _clients[name]


def fetch_releases(repo):
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/releases?per_page=30",
        headers={"accept": "application/vnd.github+json", "user-agent": "coderhelm-updater"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def releases_cached(store, repo, now_ts, fetch=fetch_releases):
    cached = store.get("RELEASES_CACHE", {})
    if cached and now_ts - cached.get("fetched_at", 0) < CACHE_SECONDS:
        return cached.get("releases", [])
    try:
        rel = [
            {k: r.get(k) for k in ("tag_name", "prerelease", "draft", "body", "published_at", "html_url")}
            for r in fetch(repo)
        ]
    except Exception as e:  # GitHub down: keep the old list
        logger.warning(f"Could not read releases: {e}")
        return cached.get("releases", [])
    store.put("RELEASES_CACHE", {"fetched_at": now_ts, "releases": rel})
    return rel


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


def start_build(env, version, actor, mode, history_id):
    r = client("codebuild").start_build(
        projectName=env["UPDATE_PROJECT_NAME"],
        environmentVariablesOverride=[
            {"name": "VERSION", "value": version, "type": "PLAINTEXT"},
            {"name": "ACTOR", "value": actor, "type": "PLAINTEXT"},
            {"name": "MODE", "value": mode, "type": "PLAINTEXT"},
            {"name": "HISTORY_ID", "value": history_id, "type": "PLAINTEXT"},
        ],
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


# ── Actions ───────────────────────────────────────────────────────────────

def app_url(env):
    return (env.get("APP_URL") or "").rstrip("/")


def begin(store, env, now, version, actor, mode):
    """Records the update in history and starts the build."""
    state = store.get("UPDATE_STATE", {})
    if running(state, now):
        raise ValueError("an update is already running")
    current = env.get("CODERHELM_VERSION", "")
    hid = f"UPDATE#{now.strftime('%Y-%m-%dT%H:%M:%SZ')}#{uuid.uuid4().hex[:8]}"
    entry = {"version": version, "from_version": current, "actor": actor, "mode": mode,
             "status": "running", "started_at": now.isoformat(), "reason": ""}
    store.put(hid, entry)
    build_id = start_build(env, version, actor, mode, hid)
    entry["build_id"] = build_id
    store.put(hid, entry)
    state.update({"running_since": now.isoformat(), "running_id": hid})
    store.put("UPDATE_STATE", state)
    return dict(entry, id=hid)


def handle(event, env, store, now, fetch=fetch_releases):
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
            "settings": settings,
            "running": running(state, now),
            # Releases whose last try failed or rolled back: the dashboard's
            # update preflight shows them with the reason from history.
            "failed_versions": sorted(state.get("failed_versions") or []),
            "history": store.history(),
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
        return begin(store, env, now, version, event.get("actor") or "unknown", "update")

    if action == "rollback":
        target = event.get("version")
        if not target:
            done = [h for h in store.history(50) if h.get("status") == "succeeded" and h.get("from_version")]
            if not done:
                raise ValueError("no earlier version to roll back to")
            target = done[0]["from_version"]
        if not parse_version(target):
            raise ValueError("version must look like v1.2.3")
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
        if status != "succeeded" and version:
            failed = set(state.get("failed_versions") or [])
            failed.add(version)
            state["failed_versions"] = sorted(failed)
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

    if action == "tick":
        rel = releases_cached(store, repo, now.timestamp(), fetch)
        d = decide(now, settings, current, rel, state)
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
    except ValueError as e:
        return {"ok": False, "error": str(e)}
