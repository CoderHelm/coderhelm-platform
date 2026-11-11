"""Updates for a self-hosted CoderHelm, run in the install's own account.

Actions (the `action` field of the event):

- tick      hourly (EventBridge): is a newer release out, and may it be
            applied now? Notifies admins once per release; starts the update
            build when auto-update allows it (on, in the maintenance window,
            and not a major release while majors need an admin).
- status    for the dashboard and the CLI: current and latest version,
            release notes, settings, history. Reads GitHub's release list
            again when the copy is over FRESH_SECONDS old (opening Settings,
            System), or now with `refresh` (Check for updates); at most one
            read per FETCH_GAP_SECONDS (REFRESH_GAP_SECONDS for Check for
            updates), conditional on the ETag. A release the channel's hint
            (launch/latest.json for stable, launch/beta.json for beta) names
            that the list lacks is never undercut: no older one is offered
            meanwhile.
- settings  saves auto-update settings (validated).
- update_now  starts the update build for a version (default: the latest,
            read from GitHub at that moment).
            Only a version newer than the one running: going back is a
            rollback. Pressed while the release check runs, the press is
            recorded and the update starts by itself once the check passes
            (follow_request); pressed again for the update in progress, it
            answers with that update.
- rollback  starts the build for the version before the last update, or an
            admin's pick, which must be a version this install ran before.
- apply_update  a native install: an admin applies the prepared update that
            waits for them (review), from Settings, System. When it would
            replace or remove data or change permissions, only with
            `confirm`, the token of exactly those changes (native_status).
- finished  the update build reports its result; history and notices.
- refresh_secrets  the gateway stored new keys in the install's secret (the
            GitHub App, from the install guide): restart the services that
            read it at start, by stamping SECRETS_REVISION on each.
- domain_set     use your own domain: requests its certificate (ACM, us-east-1,
            DNS validation). CoderHelm never changes DNS: the page shows the
            two records for the admin to add wherever the domain's DNS is.
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

import functools
import json
import urllib.parse
import logging
import os
import re
import time
import urllib.error
import urllib.request
import uuid
import hashlib
import hmac
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
# stable: releases only. beta: the nightly prereleases too (and every
# release). "preview" is the old name of beta, still accepted in stored
# settings, configs and calls.
CHANNELS = ("stable", "beta")
CHANNEL_ALIASES = {"preview": "beta"}
MAJORS = ("ask", "auto")
# The hourly check reads GitHub's release list when its copy is this old.
CACHE_SECONDS = 3600
# Settings, System reads it again when its copy is older than this.
FRESH_SECONDS = 120
# At most one read of GitHub (or latest.json) per this many seconds per
# install, whoever asks: page reloads can't hammer it.
FETCH_GAP_SECONDS = 30
# Check for updates reads again unless the last read was this recent: it is
# conditional on the ETag, and an unchanged answer (304) costs no quota.
REFRESH_GAP_SECONDS = 5
# The channel's hint (a small file in S3, written when a release is
# published) is read this often even when the release list is read hourly,
# so a new release shows within minutes of being published.
HINT_SECONDS = 300
# A release the install knows of (latest.json, or seen before) that the list
# lacks for this long is taken as withdrawn.
BEHIND_SECONDS = 24 * 3600
# A release list not refreshed for this long is shown as stale.
STALE_SECONDS = 3 * 24 * 3600
RELEASE_REPO = "CoderHelm/coderhelm-platform"
RELEASES_URL = f"https://api.github.com/repos/{RELEASE_REPO}/releases?per_page=30"
LATEST_URL = "https://coderhelm-public.s3.amazonaws.com/launch/latest.json"
BETA_URL = "https://coderhelm-public.s3.amazonaws.com/launch/beta.json"
# Each channel's hint: the newest release published for it.
HINT_URLS = {"stable": LATEST_URL, "beta": BETA_URL}
RUNNING_STALE = timedelta(hours=3)
# Who started an update when the call names no one: the dashboard always
# sends the signed-in admin, the install guide and the auto-update window name
# themselves, so a call without an actor is a direct invoke of this Lambda.
DIRECT_ACTOR = "AWS CLI"
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
    # The "What's new" email: weekly, biweekly or off.
    "digest": "weekly",
}
DIGESTS = {"weekly": timedelta(days=7), "biweekly": timedelta(days=14), "off": None}

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

def channel_of(name):
    """The channel a stored or sent name means ("preview" is beta), or None."""
    if not isinstance(name, str):
        return None
    name = CHANNEL_ALIASES.get(name, name)
    return name if name in CHANNELS else None


def takes_prereleases(channel):
    return channel_of(channel) == "beta"


def defaults_from_env(env=None):
    env = os.environ if env is None else env
    out = dict(DEFAULT_SETTINGS)
    try:
        out.update(json.loads(env.get("UPDATE_DEFAULTS") or "{}"))
    except ValueError:
        pass
    out["channel"] = channel_of(out.get("channel")) or "stable"
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
        ch = channel_of(raw["channel"])
        if ch is None:
            raise ValueError("channel: stable or beta")
        s["channel"] = ch
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
    if "digest" in raw:
        if raw["digest"] not in DIGESTS:
            raise ValueError("digest: weekly, biweekly or off")
        s["digest"] = raw["digest"]
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
    """The newest release above `current` that the channel allows: never one
    at or below it, so an install that moves from beta to stable stays on its
    beta until a newer stable release is out."""
    best = None
    for r in releases or []:
        tag = r.get("tag_name") or r.get("version")
        p = parse_version(tag)
        if not p or r.get("draft"):
            continue
        pre = bool(r.get("prerelease")) or bool(p[3])
        if pre and not takes_prereleases(channel):
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


class NotModified(Exception):
    """The copy this install holds is still current (HTTP 304 to If-None-Match)."""


class Fetched(list):
    """A release list with the ETag GitHub sent for it."""
    etag = None


def read_json(url, etag=None, opener=None):
    """GET `url` as JSON, conditionally when `etag` is known: (body, etag),
    or NotModified. A 304 costs no GitHub API quota."""
    headers = {"accept": "application/vnd.github+json", "user-agent": "coderhelm-updater"}
    if etag:
        headers["if-none-match"] = etag
    req = urllib.request.Request(url, headers=headers)
    try:
        with (opener or urllib.request.urlopen)(req, timeout=10) as r:
            return json.loads(r.read()), r.headers.get("etag"), r.geturl()
    except urllib.error.HTTPError as e:
        if e.code == 304:
            raise NotModified() from None
        raise


def fetch_releases(repo, etag=None, opener=None):
    check_repo(repo)
    body, tag, at = read_json(RELEASES_URL, etag, opener)
    # A redirect anywhere but GitHub's API over HTTPS is refused.
    if not at.startswith("https://api.github.com/"):
        raise ValueError(f"the release list came from {at}")
    out = Fetched(body)
    out.etag = tag
    return out


def fetch_latest(channel="stable", etag=None, opener=None):
    """The channel's hint: launch/latest.json (the newest stable release) or
    launch/beta.json (the newest release of any kind), each moved only after
    the release workflow checked the published release. A hint of what
    exists, never what is trusted."""
    url = HINT_URLS[channel_of(channel) or "stable"]
    body, tag, at = read_json(url, etag, opener)
    if not at.startswith(f"https://{NATIVE_HOST}/launch/"):
        raise ValueError(f"{url.rsplit('/', 1)[-1]} came from {at}")
    return {"version": (body or {}).get("version"), "etag": tag}


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


def due(doc, now_ts, max_age, gap=FETCH_GAP_SECONDS):
    """Whether a cached read should be made again: older than `max_age`, and
    no read (or failed try) in the last `gap` seconds, whoever asked."""
    if not doc:
        return True
    last = max(doc.get("fetched_at") or 0, doc.get("attempted_at") or 0)
    if now_ts - last < gap:
        return False
    return now_ts - (doc.get("fetched_at") or 0) >= max_age


def hint_key(channel):
    """Where the channel's hint is cached (stable keeps the key it always had)."""
    return "RELEASE_HINT#beta" if channel_of(channel) == "beta" else "RELEASE_HINT"


def latest_hint(store, now_ts, max_age, read=None, channel="stable", gap=FETCH_GAP_SECONDS):
    """The version the channel's hint names (latest.json or beta.json, cached
    like the release list), or None. `read(channel, etag=...)` reads it."""
    key = hint_key(channel)
    doc = store.get(key, {}) or {}
    if read is None or not due(doc, now_ts, max_age, gap):
        return doc.get("version")
    channel = channel_of(channel) or "stable"
    try:
        got = read(channel, etag=doc.get("etag")) if doc.get("etag") else read(channel)
        v = got.get("version")
        if parse_version(v) and not (doc.get("version") and compare(v, doc["version"]) < 0):
            doc.update(version=v, etag=got.get("etag"))
        doc["fetched_at"] = now_ts
    except NotModified:
        doc["fetched_at"] = now_ts
    except Exception as e:  # a hint only: the release list still decides
        logger.warning(f"Could not read the {channel} hint: {e}")
    doc["attempted_at"] = now_ts
    store.put(key, doc)
    return doc.get("version")


def releases_cached(store, repo, now_ts, fetch=fetch_releases, max_age=CACHE_SECONDS, newer=None, gap=FETCH_GAP_SECONDS):
    """The release list, read again when older than `max_age` (the hourly
    check: an hour; Settings, System: FRESH_SECONDS; Check for updates and
    Update now: 0), or when `newer` (the channel's hint) names a version the
    copy doesn't have. At most one read per `gap` seconds per install
    (FETCH_GAP_SECONDS; REFRESH_GAP_SECONDS for Check for updates), and each
    one conditional on the ETag, so page loads can't use up GitHub's quota."""
    cached = store.get("RELEASES_CACHE", {}) or {}
    wait = due(cached, now_ts, max_age, gap)
    if not wait and newer and cached and parse_version(newer):
        top = newest(cached.get("releases"))
        wait = (top is None or compare(newer, top) > 0) and due(cached, now_ts, 0, gap)
    if not wait:
        return cached.get("releases", [])
    etag = cached.get("etag") if cached.get("releases") is not None else None
    try:
        check_repo(repo)
        got = fetch(repo, etag=etag) if etag else fetch(repo)
        rel = [
            {k: r.get(k) for k in ("tag_name", "prerelease", "draft", "body", "published_at", "html_url")}
            for r in got
        ]
    except NotModified:
        store.put("RELEASES_CACHE", dict(cached, fetched_at=now_ts, attempted_at=now_ts, error=None))
        return cached.get("releases", [])
    except Exception as e:  # GitHub down: keep the old list
        logger.warning(f"Could not read releases: {e}")
        store.put("RELEASES_CACHE", dict(cached, attempted_at=now_ts, error=fit(str(e), 200)))
        return cached.get("releases", [])
    # Freshness: a list without the newest version seen before is either a
    # stale copy or a release that was withdrawn. Either way it can only hold
    # back an update (nothing below the running version is ever applied), so
    # the list is used as it is, and admins see why it looks older.
    seen = cached.get("newest_seen") if cached else None
    top = newest(rel)
    doc = {"fetched_at": now_ts, "attempted_at": now_ts, "releases": rel, "newest_seen": top or seen,
           "etag": getattr(got, "etag", None), "behind_since": cached.get("behind_since")}
    if seen and (top is None or compare(top, seen) < 0):
        logger.warning(f"Release list looks stale: newest is {top}, {seen} was seen before")
        doc.update(newest_seen=seen, stale_reason=f"GitHub's release list no longer shows {seen} (withdrawn, or a stale copy)")
    store.put("RELEASES_CACHE", doc)
    return rel


def known_newest(store, hint, channel):
    """The newest release this install knows exists for its channel: what
    the channel's hint names, or the newest the release list ever showed
    (a prerelease only on beta)."""
    seen = (store.get("RELEASES_CACHE", {}) or {}).get("newest_seen")
    best = hint if parse_version(hint) else None
    if best and parse_version(best)[3] and not takes_prereleases(channel):
        best = None
    p = parse_version(seen)
    if p and (takes_prereleases(channel) or not p[3]) and (best is None or compare(seen, best) > 0):
        best = seen
    return best


def offer(store, rel, settings, current, now_ts, hint=None):
    """The release to offer, or None, and `behind` when the list lacks a
    newer release the install knows of. Then nothing is offered: never vN
    while vN+1 is out. A newer one missing for over BEHIND_SECONDS is taken
    as withdrawn, and the list is used as it is (its staleness shown)."""
    latest = pick_latest(rel, settings["channel"], current)
    known = known_newest(store, hint, settings["channel"])
    lagging = bool(known and parse_version(current) and compare(known, current) > 0
                   and (latest is None or compare(latest["version"], known) < 0))
    cached = store.get("RELEASES_CACHE", {}) or {}
    since = cached.get("behind_since") if lagging else None
    if lagging and not since:
        since = now_ts
    if (cached.get("behind_since") or None) != since and cached:
        store.put("RELEASES_CACHE", dict(cached, behind_since=since))
    if lagging and now_ts - since < BEHIND_SECONDS:
        behind = {"version": known,
                  "reason": f"{known} is out, but GitHub's release list doesn't show it yet; it shows here once it does"}
        return None, behind
    return latest, None


def releases_freshness(store, now_ts, behind=None):
    """Whether the release list is current: {fetched_at, stale, reason,
    error, behind}. `fetched_at` is when GitHub last answered (a 304 counts)."""
    cached = store.get("RELEASES_CACHE", {}) or {}
    at = cached.get("fetched_at")
    reason = cached.get("stale_reason")
    if not reason and (not at or now_ts - at > STALE_SECONDS):
        reason = "the release list has not been read from GitHub for over 3 days"
    tried = cached.get("attempted_at")
    error = cached.get("error") if tried and (not at or tried > at) else None
    return {"fetched_at": at, "stale": bool(reason), "reason": reason, "error": error, "behind": behind}


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
UPDATE_WHAT = ("available", "succeeded", "failed", "rolled_back", "blocked", "domain_live", "domain_failed", "domain_removed",
               "review", "digest")
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


def with_sender_name(env):
    """env with the sender name an admin set in Settings, Emails
    (services/common/src/mail/sender_name.rs) in place of the config's
    EMAIL_FROM_NAME and ORGANIZATION_NAME. Nothing set, or unreadable: env
    as it is."""
    try:
        r = client("dynamodb").get_item(
            TableName=env["SETTINGS_TABLE_NAME"],
            Key={"pk": {"S": "SYSTEM"}, "sk": {"S": "SETTINGS#EMAIL_SENDER"}},
            ConsistentRead=True,
        )
        data = json.loads((r.get("Item") or {}).get("data", {}).get("S") or "{}")
    except Exception as e:
        logger.warning(f"Email sender name unreadable: {e}")
        return env
    one_line = lambda v: " ".join(v.split()) if isinstance(v, str) and not re.search(r"[\x00-\x1f\x7f]", v) else ""
    name, org = one_line(data.get("sender_name")), one_line(data.get("organization"))
    if not name and not org:
        return env
    env = dict(env)
    # The same order as sender_name::ses_name and relay_org: the stored
    # sender name, then the stored organization, then the config.
    env["EMAIL_FROM_NAME"] = name
    if org:
        env["ORGANIZATION_NAME"] = org
    elif org_name({"EMAIL_FROM_NAME": name}):
        env["ORGANIZATION_NAME"] = ""
    return env


def sign_request(secret, ts, method, path, body, request_id):
    """The hybrid request signature, v2 (services/common/src/hybrid/sign.rs
    sign_request_v2): the time, the request's single-use id, the method, the
    path with its query, and the body's hash."""
    import hashlib
    import hmac
    msg = f"v2.{ts}.{request_id}.{method.upper()}.{path}.{hashlib.sha256(body).hexdigest()}"
    return "v2=" + hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()


def new_request_id():
    """A fresh single-use id for one signed call (req_ and 32 characters)."""
    import secrets
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "req_" + "".join(secrets.choice(alphabet) for _ in range(32))


def relay_call(env, creds, path, payload, opener=None):
    url_base = (env.get("HYBRID_URL") or DEFAULT_HOSTED_URL).strip().rstrip("/")
    if not url_base.startswith("https://"):
        raise RuntimeError("no hosted address for CoderHelm's mail service")
    body = json.dumps(payload).encode()
    ts = int(time.time())
    rid = new_request_id()
    req = urllib.request.Request(
        url_base + path,
        data=body,
        method="POST",
        headers={
            "content-type": "application/json",
            "user-agent": "coderhelm-selfhost",
            "x-coderhelm-install": creds["id"],
            "x-coderhelm-timestamp": str(ts),
            "x-coderhelm-signature": sign_request(creds["secret"], ts, "POST", path, body, rid),
            "x-coderhelm-request-id": rid,
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
#
# Admins are emailed only when a person must act or something went wrong:
# an update failed or rolled back, an update waits for their OK (a prepared
# update to review, or a model check refusing it; one reminder after
# REMIND_AFTER), and address changes. Each at most once per version and reason
# (`notify_once`). A new release and a finished update never email: Settings,
# System shows both. Besides these, the optional "What's new" digest
# (settings `digest`: weekly, biweekly or off) sums up new releases.
# "available" and "succeeded" stay renderable for the relay's older installs.

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
    "review": "update to {version} needs your OK",
    "stuck": "the last update couldn't be rolled back",
    "digest": "what's new, up to {version}",
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


def safe_link(raw):
    """A button's link as it is sent: an https:// link with no space or control
    character (a parser drops line breaks, so the raw text could carry other
    words past a host check), written back from its parsed parts. Anything
    else: None, and the button is left out."""
    if not isinstance(raw, str) or not raw or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in raw):
        return None
    u = urllib.parse.urlsplit(raw)
    if u.scheme != "https" or not u.hostname or u.username or u.password or u.port:
        return None
    return urllib.parse.urlunsplit(u)


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
        if not what.startswith("domain_") and what != "digest":
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
    if what == "digest" and facts.get("on_latest") in ("yes", "no"):
        p["on_latest"] = facts["on_latest"]
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
            ("p", "To update, open Settings, then System, and press Update now. The release notes say what changed."),
        ],
        "blocked": [
            ("p", "This install won't update to {version} until someone fixes what the update check found. Nothing has changed yet."),
            ("facts", [("Install", "{install}"), ("Running now", "{current}"), ("Waiting to install", "{version}"),
                       ("Checked", "{checked_at}")]),
            ("quote", "Why it's blocked", "reason"),
            ("quote", "What to do", "action"),
            ("p", "Once it is fixed, open Settings, then System, then Update readiness, and press Check again. Once it passes, press Update now to {version}. The check also runs by itself every day, and with automatic updates on the update then goes ahead on its own."),
            ("p", "From a terminal, see what fails and how to fix it:"),
            ("cmd", CLI + " doctor"),
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
        ],
        "rolled_back": [
            ("p", "This install tried {version}, found it unhealthy, and went back to the release it ran before."),
            ("facts", [("Install", "{install}"), ("Running now", "{current}"), ("Tried", "{version}"),
                       ("Rolled back", "{checked_at}")]),
            ("quote", "What went wrong", "reason"),
            ("quote", "What to do", "action"),
            ("p", "Open Settings, then System, then Update history for the full log. Fix what it shows, then press Update now to try again. From a terminal:"),
            ("cmd", CLI + " doctor"),
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
        "review": [
            ("p", "An update to {version} is prepared and waits for an admin's OK. Nothing has changed yet."),
            ("facts", [("Install", "{install}"), ("Running now", "{current}"), ("Waiting to install", "{version}"),
                       ("Waiting since", "{waiting}"), ("Checked", "{checked_at}")]),
            ("quote", "Why it waits", "reason"),
            ("quote", "What to do", "action"),
        ],
        "digest": [
            ("p", "What's new in CoderHelm since the last summary."),
            ("facts", [("Install", "{install}"), ("Running now", "{current}"), ("Newest release", "{version}"),
                       ("Checked", "{checked_at}")]),
            ("quote", "Releases", "notes"),
            ("p", {"yes": "This install runs the newest release. Nothing to do.",
                   "no": "This install isn't on the newest release yet. Settings, System shows the release notes and Update now."}
             .get(v.get("on_latest"), "")),
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
        link = safe_link(v.get(key))
        if link:
            text.append(f"{label}: {link}")
            html.append(f'<p style="margin:0 0 16px"><a href="{esc(link)}" style="display:inline-block;background:#222;color:#fff;'
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
    transport = email_transport(env) if admins else None
    if admins and transport:
        env = with_sender_name(env)
    n = notice(what, version, env, facts)
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


NOTICES = "NOTICES"
# An update waiting this long for an admin gets one reminder.
REMIND_AFTER = timedelta(days=7)
DIGEST_LINES = 10


def notify_once(store, key, now, subject, text, env=None, **kw):
    """notify(), at most once per `key` ("<reason>:<version>") whatever the
    ticks: the keys sent are kept on the settings table (sk NOTICES)."""
    rec = store.get(NOTICES, {}) or {}
    sent = dict(rec.get("sent") or {})
    if key in sent:
        return 0
    n = notify(subject, text, env, **kw)
    sent[key] = now.isoformat()
    # Keep the newest keys only: an old version's never come back.
    sent = dict(sorted(sent.items(), key=lambda kv: kv[1])[-200:])
    store.put(NOTICES, dict(store.get(NOTICES, {}) or {}, sent=sent))
    return n


def when(iso):
    try:
        at = datetime.fromisoformat(str(iso))
    except (TypeError, ValueError):
        return None
    return at if at.tzinfo else at.replace(tzinfo=timezone.utc)


def remind_waiting(store, env, now, current):
    """One reminder when an update has waited REMIND_AFTER for an admin: a
    prepared update to review, or a model check that refuses it."""
    state = store.get("UPDATE_STATE", {}) or {}
    hid = state.get("review_id")
    entry = (store.get(hid, {}) or {}) if hid else {}
    if entry.get("status") == "review" and entry.get("version") and entry.get("mode") != "options":
        version = entry["version"]
        since = when(entry.get("review_at") or entry.get("started_at"))
        how = (f"Open Settings, then System, check what it changes and press Apply: {app_url(env)}/settings/system/#version"
               if can_apply_here(env, entry) else
               "CoderHelm can't apply this one for you: someone with AWS admin rights reviews it and chooses Execute in the AWS console.")
        what, facts = "review", {"reason": entry.get("reason"), "action": how}
    else:
        mc = store.get("MODEL_CHECK") or {}
        version = mc.get("version")
        if not version or not failures(mc, required=True) or not parse_version(version):
            return 0
        if current and parse_version(current) and compare(version, current) <= 0:
            return 0
        since = when(((store.get(NOTICES, {}) or {}).get("sent") or {}).get(f"blocked:{version}"))
        what, facts = "blocked", {"reason": blocked_reason(mc), "action": blocked_action(mc)}
    if not since or now - since < REMIND_AFTER:
        return 0
    facts.update({"current": current, "checked_at": checked_at(now), "waiting": checked_at(since)})
    return notify_once(store, f"reminder:{version}", now, f"update to {version} still needs your OK",
                       f"The update to {version} has waited since {checked_at(since)}.\n{app_url(env)}/settings/system/",
                       env, what=what, version=version, facts=facts)


def headline(body):
    """A release's first line of notes that isn't a heading, as plain words."""
    for line in str(body or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        line = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", line.lstrip("-*+ ").strip())
        line = line.replace("**", "").replace("`", "")
        if line:
            return fit(line, 140)
    return ""


def digest_releases(releases, channel, since):
    """The releases published after `since` on `channel` (prereleases only on
    a channel other than stable), newest first."""
    out = []
    for r in releases or []:
        tag = r.get("tag_name") or r.get("version")
        p = parse_version(tag)
        if not p or r.get("draft"):
            continue
        if (bool(r.get("prerelease")) or bool(p[3])) and channel == "stable":
            continue
        at = when((r.get("published_at") or "").replace("Z", "+00:00"))
        if at and at > since:
            out.append({"version": tag, "at": at, "headline": headline(r.get("body"))})
    out.sort(key=functools.cmp_to_key(lambda a, b: compare(a["version"], b["version"])), reverse=True)
    return out


def send_digest(store, env, now, settings, releases, current):
    """The "What's new" email: at most once per period, only with a new
    release on the install's channel since the last one. The first tick only
    starts the clock."""
    period = DIGESTS.get(settings.get("digest"))
    if not period:
        return 0
    rec = store.get(NOTICES, {}) or {}
    last = when(rec.get("digest_at"))
    if not last:
        store.put(NOTICES, dict(rec, digest_at=now.isoformat()))
        return 0
    if now - last < period:
        return 0
    new = digest_releases(releases, settings["channel"], last)
    if not new:
        return 0
    # Recorded before sending: a failed send waits for the next period, never repeats.
    store.put(NOTICES, dict(rec, digest_at=now.isoformat()))
    top = new[0]["version"]
    lines = [f"{r['version']} ({r['at'].strftime('%Y-%m-%d')})" + (f": {r['headline']}" if r["headline"] else "")
             for r in new[:DIGEST_LINES]]
    if len(new) > DIGEST_LINES:
        lines.append(f"and {len(new) - DIGEST_LINES} more")
    on_latest = "yes" if pick_latest(releases, settings["channel"], current) is None else "no"
    return notify(f"what's new, up to {top}", "\n".join(lines) + f"\n{app_url(env)}/settings/system/", env,
                  what="digest", version=top,
                  facts={"current": current, "checked_at": checked_at(now), "notes": "\n".join(lines), "on_latest": on_latest})


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
#   {hostname, certificate_arn, state, reason, requested_at,
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


def current_hostname(env):
    """The hostname the install answers on now (APP_URL), and whether it is CloudFront's own."""
    host = re.sub(r"^https?://", "", app_url(env)).split("/")[0].lower()
    return host, host.endswith(".cloudfront.net") or not host


def domain_view(dom, cert, target, env):
    """What the dashboard, the guide and the gateway's checks see. No account ids.
    CoderHelm never changes DNS: until the domain is live the view carries the
    two records for the admin to add, wherever the domain's DNS is hosted."""
    host, on_aws = current_hostname(env)
    if not dom or not dom.get("hostname"):
        if on_aws:
            return {"hostname": None, "state": "none", "mode": None, "current": host or None, "target": target, "records": []}
        # A domain from the config (the Launch Stack's hostname with its certificate, or the CLI).
        return {"hostname": host, "state": "live", "mode": "config", "current": host, "target": target, "records": []}
    v = validation_record(cert)
    records = []
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
        "mode": "external",
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
        # CoderHelm never changes DNS: the move adds no records (kept empty for the build's flags).
        "DOMAIN_ZONE": "",
        "DOMAIN_ZONE_ID": "",
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
    return domain_view(dom, cert, distribution_domain(env), env)


def domain_set(store, env, now, raw, actor, sleep=time.sleep):
    hostname = clean_hostname(raw)
    dom = store.get("DOMAIN") or {}
    if dom.get("state") in DOMAIN_BUSY:
        raise ValueError("the address is being changed right now; wait for it to finish (about 10 minutes)")
    host, _ = current_hostname(env)
    if hostname == host:
        raise ValueError(f"CoderHelm already answers on {hostname}")
    # CoderHelm never changes DNS: the admin adds the two records the page shows.
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
    dom = {
        "hostname": hostname,
        "certificate_arn": arn, "state": "validating", "reason": "", "requested_at": now.isoformat(), "actor": actor,
    }
    store.put("DOMAIN", dom)
    dom = advance(store, env, now, dom, cert)
    return domain_view(dom, cert, distribution_domain(env), env)


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
        return domain_view(None, None, distribution_domain(env), env)
    entry = begin(store, env, now, env.get("CODERHELM_VERSION", ""), actor, "domain",
                  domain_build_env({"hostname": host}, "remove"), note="use the AWS address")
    dom = dict(dom or {"hostname": host}, state="removing", reason="", history_id=entry["id"])
    store.put("DOMAIN", dom)
    return domain_view(dom, None, distribution_domain(env), env)


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


class ReleaseNotReady(ValueError):
    """The release isn't ready to apply yet (still publishing, or its release
    check hasn't verified it): a wait. Nothing is recorded in history."""


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
            recent = now - datetime.fromisoformat(v["at"]) < VERIFY_RETRY
        except (KeyError, ValueError):
            recent = False
        # A build that ended without reporting back (it crashed, or was
        # stopped) is started again at once: waiting 30 minutes for it would
        # only keep the page saying "Checking".
        if recent and (v.get("claim") or build_state(v.get("build")) in (None, "IN_PROGRESS")):
            return False
    try:
        build = start_check(env, version)
    except Exception as e:
        logger.warning(f"Release check not started: {e}")
        return False
    state["verifying"] = {"version": version, "at": now.isoformat(), "build": build}
    store.put("UPDATE_STATE", state)
    return True


def read_build(build_id, cb=None):
    """CodeBuild's record of a release check build, or None when it can't be read."""
    if not build_id:
        return None
    try:
        builds = (cb or client("codebuild")).batch_get_builds(ids=[build_id]).get("builds") or []
    except Exception as e:  # noqa: BLE001  unknown is not a result
        logger.warning(f"Release check build not read: {e}")
        return None
    return builds[0] if builds else None


def build_state(build_id, cb=None):
    """CodeBuild's status for a release check build (IN_PROGRESS, SUCCEEDED,
    FAILED, ...), or None when it can't be read."""
    return ((read_build(build_id, cb) or {}).get("buildStatus")) or None


# ── A release check that failed before it verified anything ──────────────
#
# The release check build downloads before it verifies: cosign in its
# install phase, the release's SHA256SUMS, signature and tag in pre_build
# (infra/lib/updates-stack.ts, releaseCheckBuildSpec). A failure there is
# GitHub not answering, or CodeBuild not starting the build, never the
# release: nothing reports back, and CodeBuild's record names the phase.
# The one download in its build phase (models.json) reports "unavailable".
# Either way the updater starts the check again by itself, CHECK_ATTEMPTS
# builds in all, each after its CHECK_BACKOFF. A failure while verifying
# (the build or post_build phase: a bad signature, a wrong tag) is never
# started again by itself: the check ends at once with its reason.

CHECK_ATTEMPTS = 3
CHECK_BACKOFF = (timedelta(seconds=30), timedelta(minutes=2))
# The phases before the release check verifies anything.
SETUP_PHASES = ("SUBMITTED", "QUEUED", "PROVISIONING", "DOWNLOAD_SOURCE", "INSTALL", "PRE_BUILD")
PHASE_FAILED = ("FAILED", "FAULT", "TIMED_OUT", "CLIENT_ERROR")
DOWNLOAD_PHASES = ("INSTALL", "PRE_BUILD", "DOWNLOAD")


def setup_failure(build, verifying=None):
    """The phase a release check build failed in before it verified anything
    (INSTALL, PRE_BUILD, PROVISIONING, ...; DOWNLOAD when its build phase
    reported a download that failed), or "" when it failed while verifying,
    was stopped, or CodeBuild doesn't say where."""
    build = build or {}
    if build.get("buildStatus") not in ("FAILED", "FAULT", "TIMED_OUT"):
        return ""
    if (verifying or {}).get("unavailable"):
        return "DOWNLOAD"
    failed = [p.get("phaseType") for p in build.get("phases") or [] if p.get("phaseStatus") in PHASE_FAILED]
    if not failed or any(p not in SETUP_PHASES for p in failed):
        return ""
    return failed[0]


def setup_reason(phase):
    if phase in DOWNLOAD_PHASES:
        return "GitHub didn't answer a download"
    return f"CodeBuild couldn't run the release check ({phase.lower().replace('_', ' ')} failed)"


def when(value):
    """A CodeBuild time (a datetime from boto3, or ISO text) as an aware datetime, or None."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def retry_check(store, env, now, version, v, build):
    """Starts the release check of `version` again by itself when its build
    failed before verifying anything, at most CHECK_ATTEMPTS builds in all.
    Returns the retry ({attempt, of, reason, note, waiting}) while the check
    carries on, or None when it doesn't: the build failed while verifying,
    was stopped, or every attempt was used."""
    phase = setup_failure(build, v)
    attempt = int(v.get("attempt") or 1)
    if not phase or attempt >= CHECK_ATTEMPTS:
        return None
    nxt = attempt + 1
    reason = setup_reason(phase)
    out = {"attempt": nxt, "of": CHECK_ATTEMPTS, "reason": reason, "phase": phase,
           "note": f"{reason}; checking again ({nxt} of {CHECK_ATTEMPTS})"}
    ended = when((build or {}).get("endTime")) or when(v.get("at")) or now
    if now - ended < CHECK_BACKOFF[attempt - 1]:
        return dict(out, waiting=True)
    state = store.get("UPDATE_STATE", {}) or {}
    if (state.get("verifying") or {}).get("build") != v.get("build"):
        return dict(out, waiting=False)  # started again meanwhile (another read of the page)
    try:
        new = start_check(env, version)
    except Exception as e:  # noqa: BLE001  the next read tries again
        logger.warning(f"Release check of {version} not started again: {e}")
        return dict(out, waiting=True)
    logger.warning(f"Release check of {version} failed in {phase} before verifying; started again ({nxt} of {CHECK_ATTEMPTS})")
    state["verifying"] = {"version": version, "at": now.isoformat(), "build": new, "attempt": nxt, "retried": phase}
    store.put("UPDATE_STATE", state)
    return dict(out, waiting=False)


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
    # An admin pressed Update while this ran: it carries on now.
    follow_request(store, env, now, probe, record=mc)
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
                and (record.get("pending") or record.get("stalled") or failures(record, required=True)))


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
        v = (store.get("UPDATE_STATE", {}) or {}).get("verifying") or {}
        record = {"version": version, "checked_at": now.isoformat(), "ok": False, "pending": True, "results": [],
                  "started_at": v.get("at") if v.get("version") == version else now.isoformat(),
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


# A release check normally reports back within a few minutes. Past this, a
# check still "pending" is shown as stalled (with Check again), never as
# "Checking" forever.
CHECK_LIMIT = timedelta(minutes=30)


def model_check_view(store, env, now, version, probe=None, cb=None):
    """The model check for `version` as it really stands, for the version
    route: a pending record whose release was verified since is checked now
    (the record was left behind); one whose release check build ended
    without reporting back, or that is older than CHECK_LIMIT, is stalled.
    Pending carries its progress: the step and when it started."""
    mc = store.get("MODEL_CHECK")
    if not version or not mc or mc.get("version") != version:
        return None
    if not mc.get("pending"):
        return mc
    known, _ = verified_manifest(store, version)
    if known:
        try:
            return model_check(store, env, now, version, env.get("RELEASE_REPO", RELEASE_REPO), probe or invoke_worker)
        except Exception as e:  # noqa: BLE001  the stored record still stands
            logger.warning(f"Model check not run: {e}")
    v = (store.get("UPDATE_STATE", {}) or {}).get("verifying") or {}
    mine = v.get("version") == version
    started = (v.get("at") if mine else None) or mc.get("started_at") or mc.get("checked_at")
    record = read_build(v.get("build"), cb) if mine else None
    build = (record or {}).get("buildStatus") or None
    view = dict(mc, progress={"step": "signature", "started_at": started, "build": build})
    if mine and int(v.get("attempt") or 1) > 1:
        view["progress"]["attempt"] = {"attempt": int(v["attempt"]), "of": CHECK_ATTEMPTS}
    reason = ""
    if build and build != "IN_PROGRESS" and not v.get("claim"):
        # Failed before verifying anything (GitHub didn't answer a
        # download): started again by itself, the request keeps waiting.
        retry = retry_check(store, env, now, version, v, record)
        if retry:
            progress = dict(view["progress"], retry=retry)
            if not retry.get("waiting"):
                progress.update(build="IN_PROGRESS", started_at=now.isoformat())
            return dict(view, note=retry["note"], progress=progress)
        phase = setup_failure(record, v)
        if phase:
            reason = f"{setup_reason(phase)} in {CHECK_ATTEMPTS} tries"
        else:
            reason = f"the release check build ended {build} without reporting back"
    else:
        try:
            if now - datetime.fromisoformat(started) > CHECK_LIMIT:
                reason = "the release check didn't report back within 30 minutes"
        except (TypeError, ValueError):
            pass
    if reason:
        view.update(pending=False, stalled=True, ok=False,
                    note=f"Checking {version} didn't finish: {reason}. Check again to start it over.")
    return view


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
    if not record or record.get("version") != version or record.get("pending") or record.get("stalled"):
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
    notify_once(store, f"blocked:{record.get('version')}", checked(record),
                f"update to {record['version']} needs your OK", blocked_text(record, env), env, what="blocked", version=record.get("version"),
                facts={"current": env.get("CODERHELM_VERSION"), "checked_at": checked_at(checked(record)),
                       "reason": blocked_reason(record), "action": blocked_action(record)})
    record["notified"] = True
    store.put("MODEL_CHECK", record)


def blocked_message(record):
    first = failures(record, required=True) or failures(record)
    f = first[0] if first else {}
    return f"Update to {record['version']} needs your action: {f.get('detail', 'a model was refused.')} {f.get('fix', '')}".strip()


# ── Update pressed while the release check runs ──────────────────────────
#
# Pressing Update while the release's signature and models are still being
# checked is never a refusal: the press is recorded (UPDATE_REQUEST: who,
# which version, when) and the updater carries on by itself once the check
# ends, exactly as if the admin pressed again: it prepares the update (begin)
# and the update then applies or waits for their OK as usual. A check that
# fails ends the request with the check's own reason, which the card shows.
# Another press, or a newer release, replaces the request. A request that
# never sees its check end is dropped after REQUEST_LIMIT.
#
# status: waiting (the check runs, or the release is still publishing),
# started (begin ran: `id` is the update), ended (`reason` says why), or done
# (the install already runs that version).

REQUEST_LIMIT = timedelta(hours=2)


def record_request(store, now, version, actor, skip_check=False, waiting_for="the release check", consent=False):
    """Records an admin's Update press that waits for the release check."""
    old = store.get("UPDATE_REQUEST") or {}
    req = {"version": version, "actor": actor, "requested_at": now.isoformat(), "status": "waiting", "waiting_for": waiting_for}
    if skip_check:
        req["skip_check"] = True
    if consent:
        req["accept_permissions"] = True
    if old.get("status") == "waiting" and old.get("version") != version:
        req["replaces"] = old.get("version")
    store.put("UPDATE_REQUEST", req)
    return req


def settle_request(store, req, now, status, reason="", **extra):
    req = dict(req, status=status, reason=reason, ended_at=now.isoformat(), **extra)
    store.put("UPDATE_REQUEST", req)
    return req


def follow_request(store, env, now, probe=None, latest=None, record=None):
    """Carries a waiting Update press on: once its release check passed, the
    update starts as if the admin pressed again; a check that failed ends it
    with the check's reason. `latest` (the newest release offered) replaces
    the request's version when it is newer. `record` is the model check as
    the version route sees it (a stalled view). Returns the request, or None."""
    req = store.get("UPDATE_REQUEST")
    if not req or req.get("status") != "waiting":
        return req or None
    version = req.get("version") or ""
    current = env.get("CODERHELM_VERSION", "")
    if not parse_version(version):
        return settle_request(store, req, now, "ended", "the requested version isn't a release")
    if parse_version(current) and compare(version, current) <= 0:
        return settle_request(store, req, now, "done", f"this install already runs {current}")
    if latest and parse_version(latest) and compare(latest, version) > 0:
        # A newer release came out while this one was checked: Update means the newest.
        req = record_request(store, now, latest, req.get("actor") or DIRECT_ACTOR, req.get("skip_check", False),
                             consent=req.get("accept_permissions", False))
        version = latest
        record = model_check(store, env, now, version, env.get("RELEASE_REPO", RELEASE_REPO), probe or invoke_worker)
    state = store.get("UPDATE_STATE", {}) or {}
    if running(state, now):
        live = (store.get(state["running_id"], {}) or {}) if state.get("running_id") else {}
        if live.get("version") == version:
            # The window (or another press) started this very update.
            return settle_request(store, req, now, "started", id=state.get("running_id"))
        if not (is_native(env) and unapplied(live)):
            return req  # another update runs: this one waits for it
    mc = record if record and record.get("version") == version else (store.get("MODEL_CHECK") or {})
    if mc.get("version") != version or mc.get("pending"):
        try:
            old = now - datetime.fromisoformat(req.get("requested_at") or "") > REQUEST_LIMIT
        except ValueError:
            old = True
        if old:
            return settle_request(store, req, now, "ended", f"checking {version} didn't finish; press Update to start it over")
        return req
    if mc.get("stalled"):
        return settle_request(store, req, now, "ended", mc.get("note") or f"checking {version} didn't finish")
    if failures(mc, required=True):
        return settle_request(store, req, now, "ended", blocked_message(mc))
    note = None
    if failures(mc):
        if not req.get("skip_check"):
            return settle_request(store, req, now, "ended", blocked_message(mc) + " This release doesn't call it, so you can skip this check.")
        note = "model check skipped for " + ", ".join(f.get("model", "?") for f in failures(mc))
    try:
        started = begin(store, env, now, version, req.get("actor") or DIRECT_ACTOR, "update", note=note,
                        consent=req.get("accept_permissions", False))
    except ReleaseNotReady as e:
        req = dict(req, waiting_for=str(e))
        store.put("UPDATE_REQUEST", req)
        return req
    except ValueError as e:
        return settle_request(store, req, now, "ended", str(e)[:500])
    return settle_request(store, req, now, "started", id=started.get("id"))


def request_view(req, now):
    """The Update press for the version route: while it waits, and for a day after it ended."""
    if not req or req.get("status") in ("done", None):
        return None
    if req.get("status") != "waiting":
        try:
            if now - datetime.fromisoformat(req.get("ended_at") or "") > timedelta(days=1):
                return None
        except ValueError:
            return None
    return {k: req.get(k) for k in ("version", "actor", "requested_at", "status", "waiting_for", "reason", "ended_at", "id", "replaces") if req.get(k) is not None}


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
# from the release's write-once URL matches it. Every native install has the
# parent's deploy role (DEPLOY_ROLE_ARN), and the updater executes the change
# set through it: when an admin pressed Update (or Roll back, or a domain
# move), and on a schedule only with auto-update on (`scheduled`). A change
# set that would replace or remove data or change permissions waits for an
# admin to confirm exactly those changes in Settings, System (apply_update).
# One that changes the deploy role or a boundary the role can't apply, so it
# waits for an AWS admin; so does every change set of an older parent
# without the role. native_poll (every 5 minutes) follows it to the end and
# reports it like a finished build. Every reason comes from CloudFormation's
# own words.

NATIVE_CAPABILITIES = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"]
NATIVE_HOST = "coderhelm-public.s3.amazonaws.com"


def is_native(env):
    return bool(env.get("NATIVE_STACK"))


# ── Uninstall: keep the data, or delete everything ──────────────────────────
#
# The admin's choice for when the install's stack is deleted (Settings,
# System, Uninstall). It lives in an SSM parameter the parent's helper reads
# on a stack delete (lambda/native-helper: Sweep and Final), so it takes
# effect without a stack update. Missing means keep.

def uninstall_parameter(env):
    prefix = env.get("RESOURCE_PREFIX") or ""
    if not re.fullmatch(r"coderhelm-[a-z][a-z0-9-]{0,19}", prefix):
        return None
    return f"/{prefix}/uninstall/delete-data"


def uninstall_status(store, env, ssm=None):
    name = uninstall_parameter(env)
    if not name or not is_native(env):
        return None
    try:
        value = (ssm or client("ssm")).get_parameter(Name=name)["Parameter"]["Value"]
    except Exception as e:
        if "ParameterNotFound" not in type(e).__name__ + str(e):
            logger.warning(f"Uninstall switch not read: {e}")
        value = ""
    rec = store.get("UNINSTALL", {}) or {}
    return {"delete_data": value == "delete", "changed_by": rec.get("actor"), "changed_at": rec.get("at")}


def set_uninstall_mode(store, env, now, event, ssm=None):
    """Turns "delete everything when uninstalled" on or off. On needs the
    install's name typed back (confirm). Every change is recorded with who
    and when (UNINSTALL_LOG keeps the last 20)."""
    name = uninstall_parameter(env)
    if not name or not is_native(env):
        return {"ok": False, "error": "only a one-stack install has this switch"}
    delete = event.get("delete_data")
    if not isinstance(delete, bool):
        return {"ok": False, "error": "delete_data must be true or false"}
    stage = name.split("/")[1][len("coderhelm-"):]
    if delete and str(event.get("confirm") or "").strip() != stage:
        return {"ok": False, "error": f"type the install's name ({stage}) to confirm"}
    ssm = ssm or client("ssm")
    if delete:
        ssm.put_parameter(Name=name, Value="delete", Type="String", Overwrite=True,
                          Description="CoderHelm: delete the install's data when its stack is deleted (Settings, System, Uninstall)")
    else:
        try:
            ssm.delete_parameter(Name=name)
        except Exception as e:
            if "ParameterNotFound" not in type(e).__name__ + str(e):
                raise
    actor = event.get("actor") or DIRECT_ACTOR
    rec = {"delete_data": delete, "actor": actor, "at": now.isoformat()}
    store.put("UNINSTALL", rec)
    store.put("UNINSTALL_LOG", ([rec] + (store.get("UNINSTALL_LOG", []) or []))[:20])
    logger.info(f"Uninstall switch: delete_data={delete} by {actor}")
    return {"ok": True, "uninstall": uninstall_status(store, env, ssm)}


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
    if mode == "options":
        return options_propose(env, version, (extra or {}).get("options") or {}, cfn)
    url = native_template_url(env, version)
    try:
        body = fetch_template(url, opener)
    except urllib.error.HTTPError as e:
        # The release publishes its stack template last, and S3 answers a
        # key that isn't there yet with 403 (listing isn't public) or 404.
        if e.code in (403, 404):
            raise ReleaseNotReady(f"{version} is still publishing; try again in a minute") from e
        raise
    rec = store.get(f"MANIFEST#{version}") or {}
    want = rec.get("template_sha256")
    if not want:
        # Every change set, a domain move included, is made only from a template the release check verified.
        if mode == "domain":
            start_verify(store, env, datetime.now(timezone.utc), version)
        raise ReleaseNotReady(f"{version}'s stack template isn't verified yet (its release check reports it); try again in a few minutes")
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
    st = stack.get("StackStatus") or ""
    if st == STUCK:
        raise ValueError("the install's stack is still stuck after an earlier update's rollback stopped; CoderHelm finishes "
                         "that rollback by itself (Settings, System says how far it is), then press Update again")
    if st.endswith("_IN_PROGRESS"):
        raise ValueError(f"AWS is still working on the install's stack ({st}); press Update again once it has finished")
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
    proposed, held, deployed_access = template, [], {}
    if role:
        # The update runs as the deploy role, which may never change itself or
        # a boundary: the install's own copies of those stay as they are, and
        # a newer one waits as a separate item for an AWS admin (access).
        deployed = deployed_template(cfn, env["NATIVE_STACK"])
        # What a rollback of this update restores (recover_stuck compares with it).
        deployed_access = {k: v for k, v in (deployed.get("Resources") or {}).items() if k in HELD}
        keep = keep_for(template, deployed)
        if keep:
            proposed = held_template(template, keep)
            r = stage_held(env, version, want, keep)
            if r.get("canonical") != canonical_sha(proposed):
                raise ValueError(f"{version}'s template with the install's own permissions kept isn't the one the updater made, so it was not proposed")
            kwargs["TemplateURL"] = held_url(env, stack, version)
            held = sorted(keep)
    r = cfn.create_change_set(**kwargs)
    # DescribeChangeSet never returns the role a change set was made with, so
    # the entry records it: apply checks it against the deploy role.
    return {"change_set_id": r["Id"], "apply": bool(role), "role_arn": role, "template_canonical": canonical_sha(proposed), "held": held,
            "deployed_access": deployed_access}


# ── What the install reads in its own account: stack parameters ──────────
#
# Infrastructure vulnerabilities, Amazon Inspector findings and AWS Health
# events are parameters of the install's stack (infra/lib/own-account-read.ts,
# OWN_OPTIONS): each adds or removes its read-only statements on the
# install's own roles. An admin switches one from the AWS card; the change
# set keeps the running release's template exactly (UsePreviousTemplate)
# and changes only those parameters. It goes the way a version update goes:
# the same preflight of the deploy role, the same review of what it changes
# about permissions (never applied without an admin's confirmation), the
# same history row, and CloudFormation's own rollback when it fails.

OPTION_PARAMS = ("InfrastructureVulnerabilities", "InspectorFindings", "HealthEvents")
OPTION_VALUES = ("on", "off")
OPTION_NAMES = {"InfrastructureVulnerabilities": "infrastructure vulnerabilities",
                "InspectorFindings": "Amazon Inspector findings", "HealthEvents": "AWS Health events"}


def stack_options(stack):
    """{parameter: "on" | "off" | None}: None when the stack's release has no such parameter yet."""
    have = {p.get("ParameterKey"): p.get("ParameterValue") for p in stack.get("Parameters") or []}
    return {k: (have[k] if have.get(k) in OPTION_VALUES else None) if k in have else None for k in OPTION_PARAMS}


def install_options(env, cfn=None):
    """The install's switches as its stack has them now, or None when unreadable."""
    if not is_native(env):
        return None
    try:
        stack = (cfn or client("cloudformation")).describe_stacks(StackName=env["NATIVE_STACK"])["Stacks"][0]
    except Exception as e:  # noqa: BLE001
        logger.warning(f"install options not read: {e}")
        return None
    return stack_options(stack)


def clean_options(raw):
    """{parameter: "on" | "off"} from a request, or ValueError."""
    if not isinstance(raw, dict) or not raw:
        raise ValueError("options must name at least one of " + ", ".join(OPTION_PARAMS))
    out = {}
    for k, v in raw.items():
        if k not in OPTION_PARAMS:
            raise ValueError(f"{k} is not a switch this install has (one of {', '.join(OPTION_PARAMS)})")
        if v not in OPTION_VALUES:
            raise ValueError(f"{k} must be on or off")
        out[k] = v
    return out


def options_text(changes):
    """"turn on Amazon Inspector findings" and the like, for history and notices."""
    return "; ".join(f"turn {c['to']} {OPTION_NAMES.get(k, k)}" for k, c in sorted(changes.items()))


def options_propose(env, version, wanted, cfn):
    """The parameter-only change set on the running release."""
    current = env.get("CODERHELM_VERSION", "")
    if version != current:
        raise ValueError(f"a switch keeps the running release ({current}), not {version}")
    stack = cfn.describe_stacks(StackName=env["NATIVE_STACK"])["Stacks"][0]
    st = stack.get("StackStatus") or ""
    if st == STUCK:
        raise ValueError("the install's stack is still stuck after an earlier update's rollback stopped; CoderHelm finishes that rollback by itself first")
    if st.endswith("_IN_PROGRESS"):
        raise ValueError(f"AWS is still working on the install's stack ({st}); try again once it has finished")
    have = stack_options(stack)
    missing = [k for k in wanted if have.get(k) is None]
    if missing:
        raise ValueError(f"{current} doesn't have the {', '.join(OPTION_NAMES[k] for k in missing)} switch yet: it arrives with the next version update")
    changes = {k: {"from": have[k], "to": v} for k, v in wanted.items() if have[k] != v}
    if not changes:
        raise ValueError("already set that way: nothing to change")
    deployed = deployed_template(cfn, env["NATIVE_STACK"])
    if not deployed:
        raise ValueError("the install's stack template couldn't be read, so nothing was proposed")
    name = f"coderhelm-options-{uuid.uuid4().hex[:8]}"
    kwargs = {
        "StackName": env["NATIVE_STACK"],
        "ChangeSetName": name,
        "ChangeSetType": "UPDATE",
        # The running release, exactly: only parameters change.
        "UsePreviousTemplate": True,
        "Parameters": native_parameters(stack, deployed, {k: c["to"] for k, c in changes.items()}),
        "Capabilities": NATIVE_CAPABILITIES,
        "Description": f"CoderHelm: {options_text(changes)}"[:1024],
        "IncludeNestedStacks": True,
    }
    role = env.get("DEPLOY_ROLE_ARN") or ""
    if role:
        kwargs["RoleARN"] = role
    r = cfn.create_change_set(**kwargs)
    return {"change_set_id": r["Id"], "apply": bool(role), "role_arn": role,
            # The template the change set must keep: checked before it is offered (native_poll).
            "template_canonical": canonical_sha(deployed), "held": [],
            "deployed_access": {k: v for k, v in (deployed.get("Resources") or {}).items() if k in HELD},
            "options": changes}


def set_options(store, env, now, event):
    """An admin switches what the install reads in its own account: a
    parameter-only change set, reviewed and confirmed like any update."""
    if not is_native(env):
        raise ValueError("only a one-stack install switches these from here")
    wanted = clean_options(event.get("options"))
    state = store.get("UPDATE_STATE", {})
    hid = state.get("running_id") or state.get("review_id")
    live = (store.get(hid, {}) or {}) if hid else {}
    if hid and live.get("status") in ("preparing", "review", "running") and live.get("mode") != "options":
        # Never replaces a version update an admin is reviewing or AWS is applying.
        raise ValueError(f"the update to {live.get('version') or 'a new version'} is {'waiting for your OK' if live.get('status') == 'review' else 'in progress'}; "
                         "finish it first (Settings, System), then switch this")
    current = env.get("CODERHELM_VERSION", "")
    actor = event.get("actor") or DIRECT_ACTOR
    # Never with the press's consent: the change to who may do what is always confirmed (native_hold).
    return begin(store, env, now, current, actor, "options", extra={"options": wanted})


# ── The deploy role's own access ──────────────────────────────────────────
#
# Every update runs as the parent stack's deploy role. CloudFormation keeps
# a stack's service role for every later operation, so even an AWS admin who
# executes a change set in the console runs it as that role. The boundary
# forbids that role to change itself or a boundary (TheBoundaryStays,
# TheDeployRoleStays): a change set that touches them can never succeed, and
# its rollback fails the same way (UPDATE_ROLLBACK_FAILED). So:
#
# - held: every update keeps the install's own Boundary, DeployBoundary and
#   DeployRole (the helper writes the verified release with those three
#   replaced; nothing else differs), and the rest of the release installs.
# - preflight: before a prepared update runs, each IAM call its changes need
#   is evaluated against the deploy role's policies and boundary as IAM has
#   them now; one it would be refused ends the update before it starts.
# - access: a newer boundary or deploy policy is a separate, optional item an
#   AWS admin applies with their own permissions, through the access stack
#   (the release's access.json; infra/lib/native/access-stack.ts).
# - recovery: a stack left in UPDATE_ROLLBACK_FAILED is rolled back the rest
#   of the way, skipping only resources checked to be exactly as the rollback
#   would leave them.

HELD = ("Boundary", "DeployBoundary", "DeployRole")
HELD_FILE = "held/coderhelm.json"
ACCESS_FILE = "access.json"
ACCESS_CACHE_SECONDS = 300
# While a permission change waits for an AWS admin, it is read again this
# often, so the card goes away within a minute of their access stack finishing.
ACCESS_PENDING_SECONDS = 30


def held_template(release, keep):
    """`release` with each of HELD replaced by the install's own definition
    (lambda/native-helper/handler.py has the same function)."""
    out = json.loads(json.dumps(release))
    res = out.get("Resources") or {}
    for k, v in (keep or {}).items():
        if k not in HELD:
            raise ValueError(f"{k} is not a resource an update keeps")
        if k not in res or not isinstance(v, dict) or v.get("Type") != res[k].get("Type") or "Condition" in v:
            raise ValueError(f"the install's {k} can't be kept in this release")
        res[k] = v
    return out


def keep_for(release, deployed):
    """The install's own definitions of HELD that differ from the release's:
    what an update keeps (an update can never change them)."""
    rr = (release or {}).get("Resources") or {}
    dr = (deployed or {}).get("Resources") or {}
    keep = {}
    for k in HELD:
        mine, theirs = dr.get(k), rr.get(k)
        if not isinstance(mine, dict) or not isinstance(theirs, dict) or mine == theirs:
            continue
        if mine.get("Type") != theirs.get("Type") or "Condition" in mine:
            continue  # can't be kept: preflight says what it would need
        keep[k] = mine
    return keep


def deployed_template(cfn, stack_name):
    """The stack's current template, as CloudFormation stored it ({} when unreadable)."""
    body = cfn.get_template(StackName=stack_name, TemplateStage="Original").get("TemplateBody")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except ValueError:
            return {}
    return body if isinstance(body, dict) else {}


def stack_account(stack):
    parts = str(stack.get("StackId") or "").split(":")
    return parts[4] if len(parts) > 5 else ""


def stack_partition(stack):
    parts = str(stack.get("StackId") or "").split(":")
    return parts[1] if len(parts) > 5 else "aws"


def held_url(env, stack, version):
    prefix = env.get("RESOURCE_PREFIX") or env["NATIVE_STACK"]
    region = env.get("AWS_REGION") or ""
    return f"https://{prefix}-release-{stack_account(stack)}.s3.{region}.amazonaws.com/releases/{version}/{HELD_FILE}"


def stage_held(env, version, template_sha256, keep, lam=None):
    """Asks the install's helper ("stage_held") to write the verified release
    with `keep` in place; returns its answer ({key, sha256, canonical})."""
    fn = env.get("NATIVE_HELPER_FUNCTION") or ""
    if not fn:
        raise ValueError("this install has no helper to stage the release's files")
    out = (lam or client("lambda")).invoke(
        FunctionName=fn, InvocationType="RequestResponse",
        Payload=json.dumps({"action": "stage_held", "version": version, "template_sha256": template_sha256, "keep": keep}).encode(),
    )
    raw = out["Payload"].read() or b"{}"
    try:
        r = json.loads(raw)
    except ValueError:
        r = {}
    if out.get("FunctionError") or not r.get("ok"):
        err = r.get("error") or raw[:300]
        if "unknown resource type" in str(err) or "response URL" in str(err) or r == {}:
            # A helper from before "stage_held" (it answers only "stage").
            raise ValueError(f"{version} changes the deploy role or its boundary, and this install's helper can't keep them yet; "
                             "update to the release before it first, or apply the permission change in Settings, System")
        raise ValueError(f"{version}'s template couldn't be staged with the install's own permissions kept: {err!r}")
    return r


def policy_doc(raw):
    """A policy document as a dict (IAM returns it URL-encoded, boto3 decoded)."""
    if isinstance(raw, str):
        from urllib.parse import unquote
        try:
            return json.loads(raw)
        except ValueError:
            return json.loads(unquote(raw))
    return raw or {}


_LISTS = ("Action", "NotAction", "Resource", "NotResource")


def canonical_policy(doc):
    """A policy as IAM means it: one value or a list of one are the same, and
    the order of actions, resources and condition values doesn't matter
    (lambda/native-access/handler.py has the same)."""
    doc = policy_doc(doc)

    def walk(v, key=None):
        if isinstance(v, dict):
            return {k: walk(x, k) for k, x in sorted(v.items())}
        if isinstance(v, list):
            items = [walk(x) for x in v]
            return sorted(items, key=lambda x: json.dumps(x, sort_keys=True)) if key in _LISTS else items
        if key in _LISTS and isinstance(v, str):
            return [v]
        return v

    out = walk(doc)
    if isinstance(out, dict):
        out["Statement"] = _as_list(out.get("Statement"))
        for st in out["Statement"]:
            for op in (st.get("Condition") or {}).values():
                for k, val in list(op.items()):
                    op[k] = sorted(val) if isinstance(val, list) else [val]
        # The order of statements doesn't matter either.
        out["Statement"] = sorted(out["Statement"], key=lambda x: json.dumps(x, sort_keys=True))
    return json.dumps(out, sort_keys=True, separators=(",", ":"))


def same_policy(a, b):
    return canonical_policy(a) == canonical_policy(b)


# A small IAM evaluator: identity policies AND the permissions boundary,
# explicit denies first (infra/test/iam-eval.ts is the same, checked against
# AWS's policy simulator).

def _iam_glob(pattern, value, ci=False):
    rx = "^" + ".*".join(".".join(re.escape(q) for q in p.split("?")) for p in str(pattern).split("*")) + "$"
    return re.match(rx, str(value), re.I if ci else 0) is not None


def _as_list(v):
    return [] if v is None else (v if isinstance(v, list) else [v])


POSITIVE_OPS = ("StringEquals", "StringEqualsIgnoreCase", "StringLike", "ArnEquals", "ArnLike", "Bool",
                "ForAnyValue:StringEquals", "ForAnyValue:StringEqualsIgnoreCase", "ForAnyValue:StringLike")


def _condition_holds(cond, ctx, deny=False):
    """Whether a statement's condition holds for the request. Fails closed:
    an operator it doesn't know raises; a Deny whose positive operator names a
    key the request doesn't carry counts as applying (it may well apply in
    AWS), while an Allow with such a key doesn't."""
    for op, kv in (cond or {}).items():
        for key, want in kv.items():
            have = ctx.get(key)
            if have is None:
                have = next((v for k, v in ctx.items() if k.lower() == key.lower()), None)
            wants, haves = _as_list(want), _as_list(have)

            def any_(f):
                return any(f(h, w) for h in haves for w in wants)
            base = op[:-len("IfExists")] if op.endswith("IfExists") else op
            if op.endswith("IfExists") and have is None:
                continue
            if have is None and base in POSITIVE_OPS:
                if deny:
                    continue  # unknown here: a deny counts as applying
                return False
            if base in ("StringEquals", "ArnEquals", "ForAnyValue:StringEquals"):
                ok = any_(lambda h, w: h == w)
            elif base in ("StringEqualsIgnoreCase", "ForAnyValue:StringEqualsIgnoreCase"):
                ok = any_(lambda h, w: h.lower() == w.lower())
            elif base in ("StringNotEquals", "ArnNotEquals"):
                ok = have is None or not any_(lambda h, w: h == w)
            elif base in ("StringLike", "ArnLike", "ForAnyValue:StringLike"):
                ok = any_(lambda h, w: _iam_glob(w, h))
            elif base in ("StringNotLike", "ArnNotLike"):
                ok = have is None or not any_(lambda h, w: _iam_glob(w, h))
            elif base == "ForAllValues:StringEquals":
                ok = all(h in wants for h in haves)
            elif base == "ForAllValues:StringLike":
                ok = all(any(_iam_glob(w, h) for w in wants) for h in haves)
            elif base == "Null":
                ok = (have is None) == (str(wants[0]).lower() == "true")
            elif base == "Bool":
                ok = any_(lambda h, w: str(h).lower() == str(w).lower())
            else:
                raise ValueError(f"the preflight can't evaluate the condition operator {op}")
            if not ok:
                return False
    return True


def _statement_matches(s, action, resource, ctx):
    if "Action" in s:
        hit = any(_iam_glob(a, action, True) for a in _as_list(s["Action"]))
    else:
        hit = not any(_iam_glob(a, action, True) for a in _as_list(s.get("NotAction")))
    if not hit:
        return False
    if "Resource" in s:
        hit = any(_iam_glob(r, resource) for r in _as_list(s["Resource"]))
    else:
        hit = not any(_iam_glob(r, resource) for r in _as_list(s.get("NotResource")))
    return hit and _condition_holds(s.get("Condition"), ctx, deny=s.get("Effect") == "Deny")


def iam_decision(identity, boundary, action, resource, ctx=None):
    """("allowed" | "explicitDeny" | "implicitDeny", the Sid that decided or "")."""
    ctx = ctx or {}
    docs = list(identity) + ([boundary] if boundary else [])
    for d in docs:
        for s in _as_list(policy_doc(d).get("Statement")):
            if s.get("Effect") == "Deny" and _statement_matches(s, action, resource, ctx):
                return "explicitDeny", s.get("Sid") or ""
    by_identity = any(s.get("Effect") == "Allow" and _statement_matches(s, action, resource, ctx)
                      for d in identity for s in _as_list(policy_doc(d).get("Statement")))
    by_boundary = not boundary or any(s.get("Effect") == "Allow" and _statement_matches(s, action, resource, ctx)
                                      for s in _as_list(policy_doc(boundary).get("Statement")))
    return ("allowed", "") if by_identity and by_boundary else ("implicitDeny", "" if by_identity else "its own policy")


def managed_policy_doc(iam, arn):
    v = iam.get_policy(PolicyArn=arn)["Policy"]["DefaultVersionId"]
    return policy_doc(iam.get_policy_version(PolicyArn=arn, VersionId=v)["PolicyVersion"]["Document"])


def deploy_role_access(env, iam=None):
    """The deploy role's policies and boundary as IAM has them now."""
    iam = iam or client("iam")
    arn = env["DEPLOY_ROLE_ARN"]
    name = arn.split("/")[-1]
    role = iam.get_role(RoleName=name)["Role"]
    identity = [policy_doc(iam.get_role_policy(RoleName=name, PolicyName=p)["PolicyDocument"])
                for p in iam.list_role_policies(RoleName=name).get("PolicyNames") or []]
    for a in iam.list_attached_role_policies(RoleName=name).get("AttachedPolicies") or []:
        identity.append(managed_policy_doc(iam, a["PolicyArn"]))
    b = ((role.get("PermissionsBoundary") or {}).get("PermissionsBoundaryArn")) or ""
    return {"role_arn": arn, "identity": identity, "boundary_arn": b, "boundary": managed_policy_doc(iam, b) if b else None}


def _known(v):
    return isinstance(v, str) and v and "{{" not in v and "KNOWN_AFTER_APPLY" not in v


ROLE_PROPERTY_CALLS = {
    "Policies": ["iam:PutRolePolicy", "iam:DeleteRolePolicy"],
    "AssumeRolePolicyDocument": ["iam:UpdateAssumeRolePolicy"],
    "PermissionsBoundary": ["iam:PutRolePermissionsBoundary"],
    "Tags": ["iam:TagRole", "iam:UntagRole"],
    "ManagedPolicyArns": ["iam:AttachRolePolicy", "iam:DetachRolePolicy"],
    "Description": ["iam:UpdateRole"],
    "MaxSessionDuration": ["iam:UpdateRole"],
}


def _role_names(v):
    return [r for r in _as_list(v) if _known(r)]


def _passed_roles(props, account):
    """Role ARNs a resource hands to a service (its Role, RoleArn, ServiceRole, ExecutionRoleArn, ...): iam:PassRole."""
    out = set()

    def walk(v, key=""):
        if isinstance(v, dict):
            for k, x in v.items():
                walk(x, k)
        elif isinstance(v, list):
            for x in v:
                walk(x, key)
        elif isinstance(v, str) and re.match(r"^arn:[\w-]+:iam::\d+:role/", v) and re.search(r"Role(Arn)?$", key or ""):
            out.add(v)
    walk(props)
    return sorted(out)


def iam_calls(rc, account, partition, install_boundary, prefix="coderhelm"):
    """[(action, resource ARN, context)] the IAM calls CloudFormation makes for
    one resource change: IAM resources (a replacement is a create and a
    delete), managed policy attachments, inline policies, and iam:PassRole
    for a role a resource hands to a service. A role or policy whose name
    isn't known yet gets the name CloudFormation would make (the stack's
    name, the logical id, a suffix), which is inside the install's names."""
    t, act = rc.get("ResourceType") or "", rc.get("Action") or ""
    lid = rc.get("LogicalResourceId") or "x"
    phys = rc.get("PhysicalResourceId") or ""
    after = context(rc.get("AfterContext"))
    after = after.get("Properties", after) if isinstance(after, dict) else {}
    targets = {(d.get("Target") or {}).get("Name") for d in rc.get("Details") or []} - {None}
    replaced = act == "Modify" and rc.get("Replacement") in ("True", "Conditional")
    iam_arn = f"arn:{partition}:iam::{account}"
    generated = f"{prefix}-{lid}-GENERATED"
    calls = []
    if act in ("Add", "Modify"):
        calls += [("iam:PassRole", r, {"iam:PassedToService": "cloudformation.amazonaws.com"}) for r in _passed_roles(after, account)]
    if t == "AWS::IAM::ManagedPolicy":
        name = after.get("ManagedPolicyName")
        new = f"{iam_arn}:policy/{name if _known(name) else generated}"
        old = phys if phys.startswith("arn:") else new
        if act == "Add" or replaced:
            calls.append(("iam:CreatePolicy", new, {}))
        if replaced:
            calls += [("iam:DeletePolicy", old, {}), ("iam:DeletePolicyVersion", old, {})]
        elif act == "Modify":
            calls += [(a, old, {}) for a in ("iam:CreatePolicyVersion", "iam:DeletePolicyVersion", "iam:ListPolicyVersions")]
        elif act == "Remove":
            calls += [(a, old, {}) for a in ("iam:DeletePolicy", "iam:DeletePolicyVersion", "iam:ListPolicyVersions")]
        if act in ("Add", "Modify"):
            ctx = {"iam:PolicyARN": new}
            calls += [("iam:AttachRolePolicy", f"{iam_arn}:role/{r}", dict(ctx, **{"iam:PermissionsBoundary": install_boundary})) for r in _role_names(after.get("Roles"))]
            calls += [("iam:DetachRolePolicy", f"{iam_arn}:role/{r}", dict(ctx, **{"iam:PermissionsBoundary": install_boundary})) for r in _role_names(after.get("Roles")) if act == "Modify"]
            calls += [("iam:AttachUserPolicy", f"{iam_arn}:user/{u}", ctx) for u in _role_names(after.get("Users"))]
            calls += [("iam:AttachGroupPolicy", f"{iam_arn}:group/{g}", ctx) for g in _role_names(after.get("Groups"))]
        return calls
    if t == "AWS::IAM::Role":
        name = after.get("RoleName")
        new_name = name if _known(name) else generated
        old_name = phys or new_name
        pb = after.get("PermissionsBoundary")
        ctx = {"iam:PermissionsBoundary": pb if _known(pb) and pb.startswith("arn:") else install_boundary}
        if act == "Add" or replaced:
            acts = ["iam:CreateRole"] + (["iam:PutRolePolicy"] if after.get("Policies") else []) + (["iam:AttachRolePolicy"] if after.get("ManagedPolicyArns") else [])
            calls += [(a, f"{iam_arn}:role/{new_name}", ctx) for a in acts]
        if replaced or act == "Remove":
            calls += [(a, f"{iam_arn}:role/{old_name}", ctx) for a in ("iam:DeleteRole", "iam:DeleteRolePolicy", "iam:DetachRolePolicy")]
        elif act == "Modify":
            acts = sorted({a for k in targets for a in ROLE_PROPERTY_CALLS.get(k, ["iam:UpdateRole"])})
            calls += [(a, f"{iam_arn}:role/{old_name}", ctx) for a in acts]
        return calls
    if t in ("AWS::IAM::Policy", "AWS::IAM::RolePolicy"):
        roles = _role_names(after.get("Roles")) + _role_names(after.get("RoleName"))
        if act in ("Add", "Modify"):
            calls += [("iam:PutRolePolicy", f"{iam_arn}:role/{r}", {"iam:PermissionsBoundary": install_boundary}) for r in roles]
            if not roles:
                calls.append(("iam:PutRolePolicy", f"{iam_arn}:role/{generated}", {"iam:PermissionsBoundary": install_boundary}))
        elif act == "Remove":
            calls += [("iam:DeleteRolePolicy", f"{iam_arn}:role/{r}", {"iam:PermissionsBoundary": install_boundary}) for r in roles]
        return calls
    return calls


class Unchecked(Exception):
    """The preflight couldn't check the change set: it holds the update."""


def preflight(cfn, env, cs_id, stack, iam=None):
    """Each IAM call the change set (and its parts') needs that the deploy role
    would be refused, in words; [] when it may make them all. When it can't
    tell (the role's policies unreadable, a condition it can't evaluate), it
    says so as a refused call: an update is never started unchecked."""
    if not env.get("DEPLOY_ROLE_ARN"):
        return []
    try:
        access = deploy_role_access(env, iam)
    except Exception as e:  # noqa: BLE001
        return [f"couldn't check: the deploy role's policies couldn't be read ({type(e).__name__}: {str(e)[:160]})"]
    account, partition = stack_account(stack), stack_partition(stack)
    prefix = env.get("RESOURCE_PREFIX") or env["NATIVE_STACK"]
    install_boundary = f"arn:{partition}:iam::{account}:policy/{prefix}-boundary-stack"
    out, seen = [], set()

    def walk(cid, part, stack_name):
        for ch in change_set_pages(cfn, cid, stack_name):
            rc = ch.get("ResourceChange") or {}
            for action, arn, ctx in iam_calls(rc, account, partition, install_boundary, prefix):
                try:
                    verdict, sid = iam_decision(access["identity"], access["boundary"], action, arn,
                                                dict(ctx, **{"aws:PrincipalArn": access["role_arn"]}))
                except ValueError as e:
                    out.append(f"{part}.{rc.get('LogicalResourceId')}: {action} on {arn} (couldn't check: {e})")
                    continue
                if verdict != "allowed":
                    why = f"denied by {sid}" if verdict == "explicitDeny" else f"not allowed by {sid or 'its boundary'}"
                    out.append(f"{part}.{rc.get('LogicalResourceId')}: {action} on {arn} ({why})")
            nested = rc.get("ChangeSetId")
            if rc.get("ResourceType") == "AWS::CloudFormation::Stack" and nested and nested not in seen:
                seen.add(nested)
                walk(nested, rc.get("LogicalResourceId"), None)
    try:
        walk(cs_id, "parent", env["NATIVE_STACK"])
    except Exception as e:  # noqa: BLE001
        out.append(f"couldn't check: the change set couldn't be read in full ({type(e).__name__}: {str(e)[:160]})")
    return out


PREFLIGHT_MARK = "the deploy role CoderHelm's updates run as may not make these calls"


# ── Template values (the few intrinsics policies use) ─────────────────────

def cfn_values(stack, env):
    """Parameter and pseudo parameter values of the stack, for cfn_resolve."""
    vals = {p.get("ParameterKey"): p.get("ParameterValue") for p in stack.get("Parameters") or [] if "ParameterValue" in p}
    vals.update({"AWS::AccountId": stack_account(stack), "AWS::Partition": stack_partition(stack),
                 "AWS::Region": env.get("AWS_REGION") or "", "AWS::StackName": stack.get("StackName") or env.get("NATIVE_STACK") or "",
                 "AWS::URLSuffix": "amazonaws.com", "AWS::StackId": stack.get("StackId") or ""})
    return vals


def cfn_resolve(v, values, conditions=None):
    """Ref, Fn::Sub, Fn::If, Fn::Join and conditions of Fn::Equals, Fn::Not,
    Fn::And, Fn::Or. Raises ValueError on anything it can't resolve."""
    conditions = conditions or {}

    def cond(name):
        if name not in conditions:
            raise ValueError(f"unknown condition {name}")
        return bool(r(conditions[name]))

    def r(x):
        if isinstance(x, list):
            return [r(i) for i in x]
        if not isinstance(x, dict):
            return x
        if len(x) == 1:
            (k, a), = x.items()
            if k == "Ref":
                if a in values:
                    return values[a]
                raise ValueError(f"no value for {a}")
            if k == "Fn::Sub":
                text, vars_ = (a, {}) if isinstance(a, str) else (a[0], r(a[1]))

                def one(m):
                    name = m.group(1)
                    if name.startswith("!"):
                        return "${" + name[1:] + "}"
                    if name in vars_:
                        return str(vars_[name])
                    if name in values:
                        return str(values[name])
                    raise ValueError(f"no value for ${{{name}}}")
                return re.sub(r"\$\{([^}]+)\}", one, text)
            if k == "Fn::If":
                return r(a[1] if cond(a[0]) else a[2])
            if k == "Fn::Join":
                return str(a[0]).join(str(p) for p in r(a[1]))
            if k == "Fn::Equals":
                return r(a[0]) == r(a[1])
            if k == "Fn::Not":
                return not r(a[0])
            if k == "Fn::And":
                return all(r(i) for i in a)
            if k == "Fn::Or":
                return any(r(i) for i in a)
            if k == "Condition":
                return cond(a)
        return {k: r(i) for k, i in x.items()}
    return r(v)


# ── The access stack: newer permissions, applied by an AWS admin ──────────

SERVICE_NAMES = {"airflow": "Airflow", "ecs": "ECS", "ecr": "ECR", "iam": "IAM", "sts": "STS", "logs": "CloudWatch Logs",
                 "bedrock": "Bedrock", "lambda": "Lambda", "s3": "S3", "dynamodb": "DynamoDB", "cloudwatch": "CloudWatch",
                 "ssm": "Systems Manager", "sqs": "SQS", "sns": "SNS", "ses": "SES", "kms": "KMS", "events": "EventBridge"}
READ_VERBS = ("Get", "List", "Describe", "BatchGet", "Simulate", "InvokeRestApi")
# What a statement means, for the few that change who may do what.
STATEMENT_WORDS = {
    "ReadRolesHereOnlyCoderHelmRuntime": "lets the log analyzer take on CoderHelm's read roles in this account, like the gateway and worker",
    "ReadRolesHereOnlyGatewayAndWorker": "lets only the gateway and worker take on CoderHelm's read roles in this account",
}


def _names(services):
    names = [SERVICE_NAMES.get(s, s) for s in sorted(set(services), key=lambda s: SERVICE_NAMES.get(s, s))]
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


def describe_policy_change(have, want):
    """What changes from `have` to `want`, in a few plain lines."""
    hs = {s.get("Sid"): s for s in _as_list(policy_doc(have).get("Statement"))}
    ws = {s.get("Sid"): s for s in _as_list(policy_doc(want).get("Statement"))}
    lines = []
    added = []
    for sid, s in ws.items():
        if s.get("Effect") == "Allow" and "Action" in s:
            old = set(_as_list((hs.get(sid) or {}).get("Action")))
            added += [a for a in _as_list(s["Action"]) if a not in old]
    if added:
        svcs = [a.split(":")[0] for a in added]
        read = all(any(a.split(":", 1)[1].startswith(v) for v in READ_VERBS) for a in added if ":" in a)
        lines.append(f"adds {'read ' if read else ''}access to {_names(svcs)} ({', '.join(added)})")
    for sid in ws.keys() - hs.keys():
        if ws[sid].get("Effect") == "Deny" or sid in STATEMENT_WORDS:
            lines.append(STATEMENT_WORDS.get(sid) or f"adds the rule {sid}")
    for sid in hs.keys() - ws.keys():
        if sid not in STATEMENT_WORDS or not any(STATEMENT_WORDS.get(n) for n in ws.keys() - hs.keys()):
            lines.append(f"replaces the rule {sid}")
    for sid in hs.keys() & ws.keys():
        a, b = dict(hs[sid]), dict(ws[sid])
        if a.get("Effect") == "Allow" and "Action" in a:
            a.pop("Action", None), b.pop("Action", None)
        if canonical_policy({"Statement": [a]}) != canonical_policy({"Statement": [b]}):
            lines.append(STATEMENT_WORDS.get(sid) or f"rewrites the rule {sid}")
    return lines or ["rewrites the policy (the same rules)"]


def access_link(region, stack_name, template_url, params):
    from urllib.parse import quote, urlencode
    q = {"stackName": f"{stack_name}-access", "templateURL": template_url}
    q.update({f"param_{k}": v for k, v in params.items()})
    return (f"https://console.aws.amazon.com/cloudformation/home?region={region}#/stacks/quickcreate?"
            + urlencode(q, quote_via=quote, safe=""))


def access_stack_origin(cfn, st):
    """Who made the stack named <install>-access: a release's access.json (its
    template says kind "access"; deleting it changes nothing, so it may be
    deleted to make room for the newer one), or something else of that name,
    like the boundary stack of an install hardened from the CLI, which must
    never be deleted. Unreadable counts as something else."""
    try:
        meta = (deployed_template(cfn, st.get("StackId") or st.get("StackName")).get("Metadata") or {}).get("CoderHelm") or {}
    except Exception as e:  # noqa: BLE001
        logger.warning(f"access stack template not read: {e}")
        meta = {}
    if meta.get("kind") == "access":
        return {"kind": "access", "version": meta.get("version") or None}
    return {"kind": "other", "version": None}


def access_compute(env, cfn=None, iam=None, opener=None):
    """The install's access against what its release's access.json gives: the
    pending permission change, if any, and the one link that applies it."""
    cfn = cfn or client("cloudformation")
    root = env["NATIVE_STACK"]
    stack = cfn.describe_stacks(StackName=root)["Stacks"][0]
    deployed = deployed_template(cfn, root)
    files = ((((deployed.get("Resources") or {}).get("ReleaseAssets") or {}).get("Properties") or {}).get("Files")) or []
    pin = next((f for f in files if f.get("Key") == ACCESS_FILE), None)
    # The version comes from the pin itself (releases/<version>/access.json), the same
    # template the sha256 comes from: mid-update the stack's Version output is still
    # the older release while its template already pins the newer access.json.
    m = re.match(r"releases/(v[^/]+)/" + re.escape(ACCESS_FILE) + "$", str((pin or {}).get("Dest") or ""))
    version = (m.group(1) if m else "") or stack_version(stack) or env.get("CODERHELM_VERSION") or ""
    if not pin or not parse_version(version):
        return {"state": "none"}
    url = f"https://{NATIVE_HOST}/launch/{version}/{ACCESS_FILE}"
    body = fetch_template(url, opener)
    if hashlib.sha256(body).hexdigest() != pin.get("Sha256"):
        return {"state": "unknown", "reason": f"{version}'s access.json doesn't match the sha256 its signed template pins"}
    access = json.loads(body)
    props = ((access.get("Resources") or {}).get("Access") or {}).get("Properties") or {}
    values = cfn_values(stack, env)
    conds = access.get("Conditions") or {}
    iam = iam or client("iam")
    changes = []
    for key in ("Boundary", "DeployBoundary"):
        want = cfn_resolve(props[key]["Document"], values, conds)
        have = managed_policy_doc(iam, cfn_resolve(props[key]["Arn"], values, conds))
        if not same_policy(have, want):
            changes.append({"resource": key, "lines": describe_policy_change(have, want)})
    role = props.get("DeployRole") or {}
    if role:
        name = cfn_resolve(role["RoleName"], values, conds)
        want = cfn_resolve(role["Document"], values, conds)
        have = policy_doc(iam.get_role_policy(RoleName=name, PolicyName=role.get("PolicyName") or "deploy")["PolicyDocument"])
        if not same_policy(have, want):
            changes.append({"resource": "DeployRole", "lines": describe_policy_change(have, want)})
    params = {p: values.get(p, "") for p in (access.get("Parameters") or {})}
    try:
        st = cfn.describe_stacks(StackName=f"{root}-access")["Stacks"][0]
        existing = {"status": st.get("StackStatus"), "link": f"https://{env.get('AWS_REGION')}.console.aws.amazon.com/cloudformation/home?region={env.get('AWS_REGION')}#/stacks/stackinfo?stackId={st.get('StackId')}"}
        existing.update(access_stack_origin(cfn, st))
    except Exception as e:  # noqa: BLE001
        if "does not exist" not in str(e):
            raise
        existing = None
    lines = []
    for c in changes:
        for ln in c["lines"]:
            if ln not in lines:
                lines.append(ln)
    return {
        "state": "pending" if changes else "applied",
        "version": version,
        "summary": lines,
        "resources": [c["resource"] for c in changes],
        "link": access_link(env.get("AWS_REGION") or "", root, url, params) if changes else None,
        "template_url": url,
        "stack": f"{root}-access",
        "existing": existing,
    }


def access_view(store, env, now, force=False, **kw):
    """access_compute, at most every ACCESS_CACHE_SECONDS, or ACCESS_PENDING_SECONDS while a change waits (never fails the route)."""
    rec = store.get("ACCESS") or {}
    # A change waiting, or a read that didn't settle (unknown), is read again soon.
    ttl = ACCESS_PENDING_SECONDS if (rec.get("view") or {}).get("state") in ("pending", "unknown") else ACCESS_CACHE_SECONDS
    try:
        fresh = rec.get("at") and (now - datetime.fromisoformat(rec["at"])).total_seconds() < ttl
    except ValueError:
        fresh = False
    if fresh and not force:
        return rec.get("view")
    try:
        view = access_compute(env, **kw)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"access not read: {e}")
        view = dict(rec.get("view") or {"state": "unknown"}, error=str(e)[:300])
    store.put("ACCESS", {"at": now.isoformat(), "view": view})
    return view


# ── A stack whose rollback stopped (UPDATE_ROLLBACK_FAILED) ──────────────

STUCK = "UPDATE_ROLLBACK_FAILED"
RECOVERY_TRIES = 3


def rollback_failures(cfn, stack_name):
    """The resources whose rollback failed in the stack's last rollback: [(id, type, reason)], every page read."""
    out, seen, token = [], set(), None
    while True:
        kw = {"StackName": stack_name}
        if token:
            kw["NextToken"] = token
        page = cfn.describe_stack_events(**kw)
        for e in page.get("StackEvents") or []:  # newest first
            if e.get("LogicalResourceId") == stack_name and e.get("ResourceStatus") == "UPDATE_ROLLBACK_IN_PROGRESS":
                return out
            lid = e.get("LogicalResourceId")
            if str(e.get("ResourceStatus", "")).endswith("_FAILED") and lid != stack_name and lid not in seen:
                seen.add(lid)
                out.append((lid, e.get("ResourceType") or "", e.get("ResourceStatusReason") or ""))
        token = page.get("NextToken")
        if not token:
            return out


def _resource_values(old, values, conds):
    """Ref to a managed policy of the template resolves to its ARN (by its name)."""
    out = dict(values)
    for lid, r in (old.get("Resources") or {}).items():
        if r.get("Type") == "AWS::IAM::ManagedPolicy" and "ManagedPolicyName" in (r.get("Properties") or {}):
            try:
                out[lid] = f"arn:{values['AWS::Partition']}:iam::{values['AWS::AccountId']}:policy/{cfn_resolve(r['Properties']['ManagedPolicyName'], values, conds)}"
            except ValueError:
                pass
    return out


def _tags(v):
    return sorted((t.get("Key"), t.get("Value")) for t in v or [])


def unchanged_by_rollback(lid, rtype, old, values, iam):
    """'' when the resource is exactly as the rollback would leave it (so it
    may be skipped), else why not. A managed policy: its default document. A
    role: its trust policy, boundary, tags, managed policies, and the names
    and documents of its inline policies. Anything else is never skipped."""
    r = ((old.get("Resources") or {}).get(lid)) or {}
    if not r:
        return "the release it rolls back to doesn't have it"
    props = r.get("Properties") or {}
    conds = old.get("Conditions") or {}
    vals = _resource_values(old, values, conds)
    try:
        if rtype == "AWS::IAM::ManagedPolicy":
            arn = f"arn:{values['AWS::Partition']}:iam::{values['AWS::AccountId']}:policy/{cfn_resolve(props['ManagedPolicyName'], vals, conds)}"
            if same_policy(managed_policy_doc(iam, arn), cfn_resolve(props["PolicyDocument"], vals, conds)):
                return ""
            return "its policy differs from the release it rolls back to"
        if rtype == "AWS::IAM::Role":
            p = cfn_resolve(props, vals, conds)
            name = p["RoleName"]
            role = iam.get_role(RoleName=name)["Role"]
            if not same_policy(role.get("AssumeRolePolicyDocument"), p.get("AssumeRolePolicyDocument")):
                return "its trust policy differs from the release it rolls back to"
            if ((role.get("PermissionsBoundary") or {}).get("PermissionsBoundaryArn") or "") != (p.get("PermissionsBoundary") or ""):
                return "its permissions boundary differs from the release it rolls back to"
            if _tags(role.get("Tags")) != _tags(p.get("Tags")):
                return "its tags differ from the release it rolls back to"
            attached = sorted(a["PolicyArn"] for a in iam.list_attached_role_policies(RoleName=name).get("AttachedPolicies") or [])
            if attached != sorted(p.get("ManagedPolicyArns") or []):
                return "its managed policies differ from the release it rolls back to"
            inline = sorted(iam.list_role_policies(RoleName=name).get("PolicyNames") or [])
            if inline != sorted(x["PolicyName"] for x in p.get("Policies") or []):
                return "its inline policies differ from the release it rolls back to"
            for x in p.get("Policies") or []:
                have = policy_doc(iam.get_role_policy(RoleName=name, PolicyName=x["PolicyName"])["PolicyDocument"])
                if not same_policy(have, x["PolicyDocument"]):
                    return f"its policy {x['PolicyName']} differs from the release it rolls back to"
            return ""
    except Exception as e:  # noqa: BLE001
        return f"it couldn't be checked ({type(e).__name__}: {str(e)[:120]})"
    return f"a {rtype} isn't one CoderHelm checks by itself"


def rollback_target(store, env):
    """(release, the install's own access resources as that update found them) a
    failed rollback goes back to: the last executed update's from_version, else
    the running one. A held update kept the install's own deploy role and
    boundaries, so those are what the rollback restores, not the release's."""
    for h in store.history(10):
        if h.get("executed_at") and parse_version(h.get("from_version") or ""):
            return h["from_version"], h.get("deployed_access") or {}
    return env.get("CODERHELM_VERSION") or "", {}


def _stuck_notice(store, env, now, stack, rec):
    notify_once(store, f"stuck:{stack.get('LastUpdatedTime') or now.date().isoformat()}", now,
                "your CoderHelm install's last update couldn't be rolled back",
                "AWS stopped while putting the previous release back, and CoderHelm couldn't safely finish it by itself: "
                f"{rec['reason']}. CoderHelm keeps running. Settings, System says what stopped it:\n{app_url(env)}/settings/system/#version",
                env, what="stuck", version=rec.get("target"), facts={"reason": rec["reason"], "action": rec.get("link")})


def recover_stuck(store, env, now, cfn=None, iam=None, opener=None):
    """Moves a stack out of UPDATE_ROLLBACK_FAILED by itself, when it safely
    can; returns the recovery record (None when the stack isn't stuck)."""
    cfn = cfn or client("cloudformation")
    root = env["NATIVE_STACK"]
    stack = cfn.describe_stacks(StackName=root)["Stacks"][0]
    st = stack.get("StackStatus") or ""
    state = store.get("UPDATE_STATE", {})
    rec = state.get("recovery") or {}
    if st != STUCK:
        if rec.get("status") in ("continuing", "retrying") and not st.endswith("_IN_PROGRESS"):
            rec.update({"status": "recovered" if st == "UPDATE_ROLLBACK_COMPLETE" else "failed", "finished_at": now.isoformat(),
                        "stack_status": st})
            state["recovery"] = rec
            store.put("UPDATE_STATE", state)
        return None
    tries = int(rec.get("tries") or 0) if rec.get("status") in ("continuing", "retrying", "stuck") else 0
    failed = rollback_failures(cfn, root)
    target, kept = rollback_target(store, env)
    why = {}
    if tries >= RECOVERY_TRIES:
        why = {"": f"continuing the rollback didn't finish after {tries} tries" + (f" (last: {rec.get('error')})" if rec.get("error") else "")}
    elif not failed:
        why = {"": "AWS doesn't say which resource stopped the rollback"}
    else:
        try:
            want = (store.get(f"MANIFEST#{target}") or {}).get("template_sha256")
            if not want:
                raise ValueError(f"{target}'s template isn't verified on this install, so it can't be compared")
            body = fetch_template(native_template_url(env, target), opener)
            if hashlib.sha256(body).hexdigest() != want:
                raise ValueError("it doesn't match its signed checksum")
            old = json.loads(body)
            for k, v in kept.items():
                if k in HELD and k in (old.get("Resources") or {}):
                    old["Resources"][k] = v
            values = cfn_values(stack, env)
            iam = iam or client("iam")
            for lid, rtype, _ in failed:
                w = unchanged_by_rollback(lid, rtype, old, values, iam)
                if w:
                    why[lid] = w
        except Exception as e:  # noqa: BLE001
            why = {"": f"{target}'s template couldn't be read to check them ({type(e).__name__}: {str(e)[:160]})"}
    region = env.get("AWS_REGION") or ""
    link = f"https://{region}.console.aws.amazon.com/cloudformation/home?region={region}#/stacks/events?stackId={stack.get('StackId', '')}"
    if why:
        rec = {"status": "stuck", "at": now.isoformat(), "failed": [f[0] for f in failed], "target": target, "tries": tries,
               "reason": "; ".join(f"{k}: {v}" if k else v for k, v in why.items())[:600], "link": link}
        state["recovery"] = rec
        store.put("UPDATE_STATE", state)
        _stuck_notice(store, env, now, stack, rec)
        return rec
    skip = [f[0] for f in failed]
    kw = {"StackName": root, "ResourcesToSkip": skip}
    if env.get("DEPLOY_ROLE_ARN"):
        kw["RoleARN"] = env["DEPLOY_ROLE_ARN"]
    try:
        cfn.continue_update_rollback(**kw)
    except Exception as e:  # noqa: BLE001  recorded and counted, never swallowed
        err = f"{type(e).__name__}: {str(e)[:200]}"
        rec = {"status": "retrying", "at": now.isoformat(), "failed": skip, "target": target, "tries": tries + 1, "error": err, "link": link,
               "reason": f"AWS refused to continue the rollback ({err}); CoderHelm tries again in 5 minutes"}
        if tries + 1 >= RECOVERY_TRIES:
            rec.update({"status": "stuck", "reason": f"AWS refused to continue the rollback {tries + 1} times ({err})"})
            _stuck_notice(store, env, now, stack, rec)
        state["recovery"] = rec
        store.put("UPDATE_STATE", state)
        logger.warning(f"continue rollback refused: {err}")
        return rec
    rec = {"status": "continuing", "at": now.isoformat(), "skipped": skip, "target": target, "tries": tries + 1,
           "reason": f"AWS stopped while putting {target} back; CoderHelm continued the rollback, skipping "
                     f"{', '.join(skip)}, each checked to be exactly as {target} has it"}
    state["recovery"] = rec
    store.put("UPDATE_STATE", state)
    logger.info(f"continued the rollback of {root}, skipping {skip}")
    return rec


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
    pending = None
    if entry.get("change_set_id"):
        pending = dict(entry.get("change_set") or {}, version=entry.get("version"), status=entry.get("status"), id=hid,
                       # Who started it, and who applied it (an admin here, or someone in the AWS console).
                       actor=entry.get("actor"), started_at=entry.get("started_at"),
                       applied_by=entry.get("applied_by"), executed_at=entry.get("executed_at"),
                       # The install's own deploy role and boundaries, kept as they are (a newer
                       # version of them is the separate `access` item below).
                       held=entry.get("held") or [],
                       # A switch (parameter-only change set): what it turns on or off.
                       mode=entry.get("mode"), options=entry.get("options"))
        if entry.get("status") == "review":
            # What an admin confirms in Settings, System before it applies.
            pending.update({
                "hold": entry.get("reason") or "",
                "data_changes": entry.get("data_changes") or [],
                "permission_changes": entry.get("permission_changes") or [],
                "admin_changes": entry.get("admin_changes") or [],
                "preflight": entry.get("preflight") or [],
                "can_apply": can_apply_here(env, entry),
                "confirm": confirm_token(hid, entry),
            })
    role = bool(env.get("DEPLOY_ROLE_ARN"))
    return {
        "stack": env.get("NATIVE_STACK"),
        # The install has the deploy role: an admin's Update applies (older name: auto_apply).
        "can_apply": role,
        "auto_apply": role,
        "pending": pending,
        # A newer boundary or deploy role policy than the install has: optional,
        # applied by an AWS admin through the access stack (one link).
        "access": (store.get("ACCESS") or {}).get("view"),
        # A stack whose rollback stopped, and what CoderHelm did about it.
        "recovery": state.get("recovery"),
        # What the install reads in its own account, as its stack has it (None: not readable).
        "options": install_options(env),
    }


def confirm_token(hid, entry):
    """What an admin's confirmation names: this prepared update and exactly
    the data and permission changes it would make ("" when there are none)."""
    held = {"data": entry.get("data_changes") or [], "permissions": entry.get("permission_changes") or []}
    if not held["data"] and not held["permissions"]:
        return ""
    return canonical_sha({"id": hid, "change_set": entry.get("change_set_id"), **held})


def can_apply_here(env, entry):
    """Whether the updater can apply this prepared update through the deploy
    role: it was made with the role, and changes neither the role nor a boundary."""
    return bool(env.get("DEPLOY_ROLE_ARN")) and bool(entry.get("apply")) and not entry.get("admin_changes")


def auto_on(store, env):
    return validate_settings(store.get("AUTO_UPDATE", {}) or {}, defaults_from_env(env))["auto"]


def change_set_link(region, stack_id, cs_id):
    from urllib.parse import quote
    return (f"https://{region}.console.aws.amazon.com/cloudformation/home?region={region}"
            f"#/stacks/changesets/changes?stackId={quote(stack_id, safe='')}&changeSetId={quote(cs_id, safe='')}")


def change_set_summary(cs, region, role=False):
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
        "role": bool(role),
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


def native_poll(store, env, now, cfn=None, wait=False):
    """Moves a proposed or running stack update along; returns what happened.
    What CloudFormation says decides, the stored entry only follows it: a
    change set executed in the AWS console is followed like one applied here.
    `wait` (the scheduled poll only): after UPDATE_COMPLETE it keeps asking the
    new release to answer for up to API_WINDOW; the status route asks once."""
    cfn = cfn or client("cloudformation")
    state = store.get("UPDATE_STATE", {})
    hid = state.get("running_id") or state.get("review_id")
    if not hid:
        rec = safe_recover(store, env, now, cfn)
        return {"native": "recovering", "recovery": rec} if rec else {"native": "idle"}
    entry = store.get(hid, {})
    cs_id = entry.get("change_set_id")
    if entry.get("status") in ENDED:
        # Ended (or replaced) while this pointer was left behind.
        for k in ("running_id", "review_id"):
            if state.get(k) == hid:
                state.pop(k, None)
                if k == "running_id":
                    state.pop("running_since", None)
        store.put("UPDATE_STATE", state)
        return {"native": "idle"}
    if not cs_id:
        return {"native": "idle"}
    was = entry.get("status")

    def keep(e):
        """Writes the entry unless someone else moved it on meanwhile (an
        admin's Apply, a newer update replacing it): theirs stands."""
        if (store.get(hid, {}) or {}).get("status") != was:
            return False
        store.put(hid, e)
        return True
    region = env.get("AWS_REGION") or ""
    try:
        cs = cfn.describe_change_set(ChangeSetName=cs_id, StackName=env["NATIVE_STACK"])
    except Exception as e:
        if "does not exist" in str(e) or "ChangeSetNotFound" in type(e).__name__:
            return native_finish(store, env, now, hid, "failed", "the proposed change set was deleted before it ran", PREPARE)
        raise
    summary = change_set_summary(cs, region, role=prepared_role(entry))
    entry["change_set"] = summary
    status, execution = cs.get("Status"), cs.get("ExecutionStatus")
    if status == "FAILED":
        reason = cs.get("StatusReason") or "the change set could not be created"
        if "didn't contain changes" in reason or "No updates" in reason:
            return native_finish(store, env, now, hid, "succeeded", "already up to date: nothing to change")
        return native_finish(store, env, now, hid, "failed", reason, PREPARE)
    if status != "CREATE_COMPLETE":
        keep(entry)
        return {"native": "preparing"}
    if execution == "AVAILABLE" and "checked" not in entry:
        if not fetched_template_matches(cfn, env, cs_id, entry.get("template_canonical")):
            try:
                cfn.delete_change_set(ChangeSetName=cs_id, StackName=env["NATIVE_STACK"])
            except Exception as e:
                logger.warning(f"change set not deleted: {e}")
            return native_finish(store, env, now, hid, "failed", "the template CloudFormation fetched is not the verified one, so the change set was deleted", PREPARE)
        entry["data_changes"] = data_changes(cfn, cs)
        entry["admin_changes"] = admin_changes(cs)
        entry["permission_changes"] = permission_changes(cfn, cs_id, env["NATIVE_STACK"])
        if prepared_role(entry) or (prepared_role(entry) is None and entry.get("apply")):
            stack = cfn.describe_stacks(StackName=env["NATIVE_STACK"])["Stacks"][0]
            entry["preflight"] = preflight(cfn, env, cs_id, stack)
        entry["checked"] = True
        summary["data_changes"] = entry["data_changes"]
        entry["change_set"] = summary
    never = never_runs(entry) if execution == "AVAILABLE" and entry.get("checked") else ""
    if never and not entry.get("executed_at"):
        # It runs as the deploy role whoever executes it (the stack's service
        # role), and that role would be refused: never offered, never started.
        # An update an older updater left waiting "for an AWS admin" ends here too.
        try:
            cfn.delete_change_set(ChangeSetName=cs_id, StackName=env["NATIVE_STACK"])
        except Exception as e:  # noqa: BLE001
            logger.warning(f"change set not deleted: {e}")
        if (store.get(hid, {}) or {}).get("status") != was:
            return {"native": "moved on"}
        store.put(hid, entry)
        return native_finish(store, env, now, hid, "failed", never, PREPARE)
    if execution == "AVAILABLE" and entry.get("executed_at"):
        # Executed a moment ago: CloudFormation can still answer AVAILABLE
        # for a few seconds. It runs; it never goes back to waiting for an OK.
        return {"native": "running"}
    if execution == "AVAILABLE":
        # An admin's Update applies at once; a scheduled one only while auto-update is on.
        # Neither applies by itself what a person must decide.
        hold = native_hold(store, env, entry)
        if hold:
            entry["reason"] = hold
        if entry.get("apply") and not hold and not entry.get("executed_at"):
            if (store.get(hid, {}) or {}).get("status") != was:
                return {"native": "moved on"}
            cfn.execute_change_set(ChangeSetName=cs_id, StackName=env["NATIVE_STACK"])
            entry.update({"executed_at": now.isoformat(), "status": "running"})
            if entry.get("permission_changes") and consented(entry):
                # What the press agreed to, as it turned out: kept with the update.
                c = entry["consent"]
                entry["confirmed"] = {"by": c.get("by"), "at": c.get("at"), "data_changes": [],
                                      "permission_changes": entry["permission_changes"], "on": "update"}
            store.put(hid, entry)
            return {"native": "executing"}
        if entry.get("status") != "review":
            # Waiting on an admin: not "running", so it never blocks or goes stale.
            entry["status"] = "review"
            entry["review_at"] = now.isoformat()
            if not keep(entry):
                return {"native": "moved on"}
            state.pop("running_since", None)
            state.pop("running_id", None)
            state["review_id"] = hid
            store.put("UPDATE_STATE", state)
            if can_apply_here(env, entry):
                how = f"Open Settings, then System, check what it changes and press Apply:\n{app_url(env)}/settings/system/#version"
            else:
                how = f"CoderHelm can't apply this one for you. Review it and choose Execute in the AWS console:\n{summary['link']}"
            if entry.get("mode") == "options":
                # A switch an admin just pressed on the AWS card: the card asks them, no email.
                return {"native": "review"}
            notify_once(store, f"review:{entry.get('version')}", now, f"update to {entry.get('version')} needs your OK",
                        f"The stack update is prepared. {entry.get('reason') or ''}\n{how}".replace(". \n", ".\n"),
                        env, what="review", version=entry.get("version"),
                        facts={"current": entry.get("from_version"), "checked_at": checked_at(now),
                               "reason": entry.get("reason"), "action": how})
        else:
            keep(entry)
        return {"native": "review"}
    if execution in EXECUTED:
        if entry.get("status") != "running":
            # Executed outside the app (the AWS console or CLI), whatever the
            # entry said: it runs now, and the page says so.
            if not entry.get("executed_at"):
                # When exactly isn't known, only that it was after the change set
                # was made: its failures are read from then on.
                entry.update({"executed_at": entry.get("started_at") or now.isoformat(), "seen_running_at": now.isoformat(),
                              "applied_by": entry.get("applied_by") or CONSOLE_ACTOR})
            entry["status"] = "running"
            entry["reason"] = ""
            state.pop("review_id", None)
            since = state.get("running_since") if state.get("running_id") == hid else None
            state.update({"running_since": since or now.isoformat(), "running_id": hid})
            store.put("UPDATE_STATE", state)
        store.put(hid, entry)
        stack = cfn.describe_stacks(StackName=env["NATIVE_STACK"])["Stacks"][0]
        st = stack["StackStatus"]
        if st.endswith("_IN_PROGRESS"):
            return {"native": "running", "stack": st}
        if st == STUCK:
            # AWS stopped while putting the previous release back: CoderHelm
            # finishes that rollback itself when it safely can, and the update
            # ends once the stack says how it ended.
            if not entry.get("rollback_failed_at"):
                entry["rollback_failed_at"] = now.isoformat()
                entry["first_failure"] = first_failure(cfn, env["NATIVE_STACK"], entry.get("executed_at"))
                store.put(hid, entry)
            rec = safe_recover(store, env, now, cfn)
            if rec and rec.get("status") == "continuing":
                return {"native": "recovering", "recovery": rec}
            reason = entry.get("first_failure") or "the stack's rollback stopped"
            return native_finish(store, env, now, hid, "failed", f"{reason}. {(rec or {}).get('reason') or ''}".strip())
        if st == "UPDATE_ROLLBACK_COMPLETE" and entry.get("rollback_failed_at"):
            reason = entry.get("first_failure") or first_failure(cfn, env["NATIVE_STACK"], entry.get("executed_at")) or "the stack rolled back"
            rec = (store.get("UPDATE_STATE", {}) or {}).get("recovery") or {}
            return native_finish(store, env, now, hid, "rolled_back", f"{reason}. {rec.get('reason') or ''}".strip())
        if st in ("UPDATE_COMPLETE",):
            ran = stack_version(stack)
            if ran and entry.get("version") and ran != entry.get("version"):
                return native_finish(store, env, now, hid, "failed",
                                     f"the stack finished, but it reports {ran}, not {entry.get('version')}")
            result, note = answers_after(store, env, now, hid, entry, wait)
            if result is None:
                return {"native": "checking", "api": note}
            if result == "failed":
                return native_finish(store, env, now, hid, "failed", note, UNANSWERED)
            return native_finish(store, env, now, hid, "succeeded", note)
        reason = first_failure(cfn, env["NATIVE_STACK"], entry.get("executed_at")) or f"the stack ended {st}"
        result = "rolled_back" if "ROLLBACK_COMPLETE" in st else "failed"
        return native_finish(store, env, now, hid, result, reason)
    if execution in ("OBSOLETE", "UNAVAILABLE"):
        return native_finish(store, env, now, hid, "failed",
                             cs.get("StatusReason") or "the change set can no longer run (the stack changed since it was made)", PREPARE)
    keep(entry)
    return {"native": str(execution or status).lower()}


def safe_recover(store, env, now, cfn):
    """recover_stuck, never failing the poll."""
    try:
        return recover_stuck(store, env, now, cfn)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"stuck stack not recovered: {e}")
        return None


def never_runs(entry):
    """Why a change set prepared with the deploy role can never apply ("" when it can).

    The stack's service role is the deploy role, so CloudFormation runs the
    change set as that role whoever executes it, an AWS admin in the console
    included. A change to the role or a boundary, or a call the preflight
    found it would be refused, fails, and then its rollback fails too."""
    if not (entry.get("role_arn") or (entry.get("role_arn") is None and entry.get("apply"))):
        return ""
    if "preflight" in entry and entry["preflight"] is None:
        # Never unchecked: an older entry that couldn't be checked is checked as refused.
        return f"{PREFLIGHT_MARK}: couldn't check them"
    calls = list(entry.get("preflight") or [])
    if entry.get("admin_changes") and not calls:
        calls = [f"parent.{c}" for c in entry["admin_changes"]]
    if not calls:
        return ""
    return (f"{PREFLIGHT_MARK}: " + "; ".join(calls))[:900]


# After UPDATE_COMPLETE the new release must answer, as `coderhelm-selfhost
# update` checks it (installer/lib/commands.mjs, health): GET /api/me is 401
# (up, not signed in) or 200. The updater runs outside any VPC and reaches the
# internet as it does for the release templates; when it can't reach the
# install at all, that says nothing about the release, so it isn't a failure.
API_WINDOW = 120   # seconds the new release has to answer
API_PAUSE = 10     # between two asks
API_OPENER = None  # tests replace it; None is urllib's
_sleep, _clock = time.sleep, time.monotonic


def api_status(url, opener=None):
    """GET {url}/api/me: its HTTP status, or None when it couldn't be reached."""
    req = urllib.request.Request(f"{url}/api/me", headers={"User-Agent": "coderhelm-updater"})
    try:
        with (opener or API_OPENER or urllib.request.urlopen)(req, timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001 (DNS, refused, timeout, no route)
        logger.warning(f"couldn't reach {url}/api/me: {e}")
        return None


def answers_after(store, env, now, hid, entry, wait):
    """(result, note) once the stack is UPDATE_COMPLETE: ("succeeded", note),
    ("failed", what it answered), or (None, what it answered) while the window
    is still open and this call doesn't wait. Never rolls anything back."""
    url = app_url(env)
    if not url:
        return "succeeded", "the stack finished; whether the new version answers wasn't checked (the updater knows no public URL for this install)"
    if not entry.get("api_since"):
        entry["api_since"] = now.isoformat()
        store.put(hid, entry)
    before = (now - datetime.fromisoformat(entry["api_since"])).total_seconds()
    start = _clock()
    while True:
        code = api_status(url)
        if code in (200, 401):
            return "succeeded", ""
        if code is None:
            return "succeeded", (f"the stack finished, but the updater couldn't reach {url}/api/me, so it couldn't check that "
                                 "the new version answers. Open CoderHelm to check it does")
        if before + (_clock() - start) >= API_WINDOW:
            entry = store.get(hid, {}) or entry
            entry["unanswered"] = f"HTTP {code}"
            store.put(hid, entry)
            return "failed", f"HTTP {code}"
        if not wait:
            return None, f"HTTP {code}"
        _sleep(API_PAUSE)


def stack_version(stack):
    """The release the parent stack says it runs (its Version output), or None."""
    for o in stack.get("Outputs") or []:
        if o.get("OutputKey") == "Version" and parse_version(o.get("OutputValue")):
            return o["OutputValue"]
    return None


def native_hold(store, env, entry):
    """Why this prepared update waits for an admin ("" when it may run now)."""
    if not entry.get("apply"):
        return "this install's stack has no deploy role yet, so an AWS admin executes this update once in the AWS console (it adds the role)"
    if entry.get("admin_changes"):
        # The deploy role can't change itself or a boundary (TheBoundaryStays, TheDeployRoleStays).
        return ("it changes the deploy role or a permissions boundary (" + "; ".join(entry["admin_changes"])[:300]
                + "), which CoderHelm can't apply for you: an AWS admin executes it in the AWS console")
    who = "auto-update held" if entry.get("scheduled") else "waiting for you to confirm"
    if entry.get("data_changes"):
        # Never without a person: data would be replaced or removed.
        return f"{who}: it would replace or remove " + "; ".join(entry["data_changes"])[:400]
    if entry.get("permission_changes") and not consented(entry):
        # Never without a person: it changes who may do what.
        return f"{who}: it changes permissions (" + "; ".join(entry["permission_changes"])[:400] + ")"
    if entry.get("scheduled") and not auto_on(store, env):
        return "auto-update was turned off before it ran: press Apply to apply it"
    return ""


def consented(entry):
    """An admin's Update press (never the window) agreed to permission changes
    of CoderHelm's own roles. They stay inside the permissions boundary; the
    deploy role and the boundary themselves are admin_changes, never applied here."""
    c = entry.get("consent") or {}
    return bool(c.get("permissions")) and not entry.get("scheduled")


def prepared_role(entry):
    """The deploy role this update's change set was made with ("" when none).
    DescribeChangeSet doesn't return it, so prepare records it on the entry.
    An entry from an older updater has no record; its change set was made with
    the deploy role whenever `apply` is set (only the role makes `apply` true)."""
    if "role_arn" in entry:
        return entry.get("role_arn") or ""
    return None if entry.get("apply") else ""


def native_apply(store, env, now, event, cfn=None):
    """An admin applies the prepared update that waits for them. Only through
    the deploy role, only a change set still as it was checked, and, when it
    would replace or remove data or change permissions, only with `confirm`
    naming exactly those changes. A change to the role or a boundary is never
    applied here: the role can't, and nothing else here may."""
    if not is_native(env):
        raise ValueError("only a one-stack install applies a prepared update")
    state = store.get("UPDATE_STATE", {})
    hid = state.get("review_id")
    if not hid or (event.get("history_id") and event.get("history_id") != hid):
        raise ValueError("no prepared update is waiting for you (reload the page)")
    entry = store.get(hid, {})
    cs_id = entry.get("change_set_id")
    if entry.get("status") != "review" or not cs_id or not entry.get("checked"):
        raise ValueError("no prepared update is waiting for you (reload the page)")
    if not can_apply_here(env, entry):
        raise ValueError(native_hold(store, env, entry) or "CoderHelm can't apply this update")
    want = confirm_token(hid, entry)
    if want and not hmac.compare_digest(str(event.get("confirm") or ""), want):
        raise ValueError("confirm the changes it would make first (if the page was open a while, reload it: the list may have changed)")
    cfn = cfn or client("cloudformation")
    role = env["DEPLOY_ROLE_ARN"]
    made_with = prepared_role(entry)
    if made_with is None:
        made_with = role  # older entry: `apply` means it was made with the deploy role
    if not entry.get("apply") or made_with != role:
        raise ValueError("this update was prepared without the deploy role; press Update to prepare it again")
    cs = cfn.describe_change_set(ChangeSetName=cs_id, StackName=env["NATIVE_STACK"])
    if cs.get("ChangeSetId") and cs["ChangeSetId"] != cs_id:
        raise ValueError("the change set found is not the one this update prepared; press Update to prepare it again")
    if cs.get("Status") != "CREATE_COMPLETE" or cs.get("ExecutionStatus") != "AVAILABLE":
        raise ValueError("this prepared update can no longer run (the stack changed since); press Update to prepare it again")
    actor = event.get("actor") or DIRECT_ACTOR
    cfn.execute_change_set(ChangeSetName=cs_id, StackName=env["NATIVE_STACK"])
    entry.update({"status": "running", "executed_at": now.isoformat(), "applied_by": actor, "reason": ""})
    if want:
        entry["confirmed"] = {"by": actor, "at": now.isoformat(), "data_changes": entry.get("data_changes") or [],
                              "permission_changes": entry.get("permission_changes") or []}
    store.put(hid, entry)
    state.pop("review_id", None)
    state.update({"running_since": now.isoformat(), "running_id": hid})
    store.put("UPDATE_STATE", state)
    logger.info(f"Prepared update {hid} applied by {actor}{' (confirmed)' if want else ''}")
    return dict(entry, id=hid)


# ── One pending update at a time ──────────────────────────────────────────
#
# A prepared update that never ran (a change set, not executed) is replaced
# by the next one an admin or the window starts: its change set is deleted
# and history keeps it as superseded. Only one update is ever pending, and
# the version route returns that one. A change set CloudFormation says is
# executing or executed is never replaced: that one is the update running.

ENDED = ("succeeded", "failed", "rolled_back", "not_started", "superseded")
UNAPPLIED = ("preparing", "review")
EXECUTED = ("EXECUTE_IN_PROGRESS", "EXECUTE_COMPLETE", "EXECUTE_FAILED")
CONSOLE_ACTOR = "someone in the AWS console"


def unapplied(entry):
    """A native update prepared (or being prepared) that hasn't run."""
    return bool(entry.get("change_set_id")) and entry.get("status") in UNAPPLIED and not entry.get("executed_at")


def change_set_state(cfn, env, cs_id):
    """(Status, ExecutionStatus) of a change set, or (None, None) when it no longer exists."""
    try:
        cs = cfn.describe_change_set(ChangeSetName=cs_id, StackName=env["NATIVE_STACK"])
    except Exception as e:
        if "does not exist" in str(e) or "ChangeSetNotFound" in type(e).__name__:
            return None, None
        raise
    return cs.get("Status"), cs.get("ExecutionStatus")


def open_updates(store, state, skip=None):
    """Every prepared-but-unapplied update: the live ones (state) and any a
    lost race or an older updater left open in history, newest first."""
    out, seen = [], {skip}
    for hid in (state.get("review_id"), state.get("running_id")):
        if hid and hid not in seen:
            seen.add(hid)
            e = store.get(hid, {}) or {}
            if unapplied(e):
                out.append((hid, e))
    for h in store.history(50):
        hid = h.get("id")
        if hid not in seen and h.get("mode") != "domain" and unapplied(h):
            seen.add(hid)
            out.append((hid, {k: v for k, v in h.items() if k != "id"}))
    return sorted(out, key=lambda x: x[0], reverse=True)


def check_replaceable(store, env, state, cfn=None):
    """Refuses (ValueError) when an open update can't be replaced: its change
    set runs (executed in the console) or is still being made."""
    found = open_updates(store, state)
    if not found:
        return
    cfn = cfn or client("cloudformation")
    for hid, e in found:
        status, execution = change_set_state(cfn, env, e["change_set_id"])
        if execution in EXECUTED:
            raise ValueError(f"the update to {e.get('version')} is already being applied (it was executed in AWS); wait for it to finish")
        if status in ("CREATE_PENDING", "CREATE_IN_PROGRESS"):
            raise ValueError(f"the update to {e.get('version')} is still being prepared; try again in a minute")


def supersede_open(store, env, state, now, by_id, by_version, cfn=None):
    """Marks every other prepared-but-unapplied update superseded by
    `by_id` and deletes its change set while it never ran. `state` loses
    its pointers to them. Returns the ids replaced."""
    found = open_updates(store, state, skip=by_id)
    if not found:
        return []
    cfn = cfn or client("cloudformation")
    done = []
    for hid, e in found:
        try:
            _, execution = change_set_state(cfn, env, e["change_set_id"])
        except Exception as ex:  # noqa: BLE001  unreadable: leave it as it is
            logger.warning(f"open update {hid} not read: {ex}")
            continue
        if execution in EXECUTED:
            continue  # it runs: native_poll follows it
        if execution is not None:
            try:
                cfn.delete_change_set(ChangeSetName=e["change_set_id"], StackName=env["NATIVE_STACK"])
            except Exception as ex:  # noqa: BLE001
                logger.warning(f"replaced change set not deleted: {ex}")
        e.update({"status": "superseded", "superseded_by": by_id, "finished_at": now.isoformat(),
                  "reason": f"replaced by the update to {by_version} before it was applied; nothing was changed"})
        store.put(hid, e)
        for k in ("review_id", "running_id"):
            if state.get(k) == hid:
                state.pop(k, None)
                if k == "running_id":
                    state.pop("running_since", None)
        done.append(hid)
    return done


def reconcile(store, env, now, cfn=None):
    """Before the version route answers: the pending update as CloudFormation
    has it now (prepared, waiting, executed in the console, finished), and
    any older prepared update left open marked superseded. Never fails the
    route: what can't be read stays as stored, and the scheduled poll retries."""
    try:
        if env.get("DEPLOY_ROLE_ARN"):
            access_view(store, env, now)
        state = store.get("UPDATE_STATE", {})
        hid = state.get("running_id") or state.get("review_id")
        opened = open_updates(store, state)
        if not hid and not opened:
            safe_recover(store, env, now, cfn or client("cloudformation"))
            return
        cfn = cfn or client("cloudformation")
        entry = (store.get(hid, {}) or {}) if hid else {}
        if opened and (not hid or unapplied(entry)) and opened[0][0] != hid:
            # The newest prepared update is the one an admin pressed last: it
            # is the pending one, whatever pointer an older updater left.
            hid, entry = opened[0]
            for k in ("review_id", "running_id", "running_since"):
                state.pop(k, None)
            if entry.get("status") == "review":
                state["review_id"] = hid
            else:
                state.update({"running_id": hid, "running_since": entry.get("started_at") or now.isoformat()})
            store.put("UPDATE_STATE", state)
        if supersede_open(store, env, state, now, hid, entry.get("version") or "a newer release", cfn):
            store.put("UPDATE_STATE", state)
        native_poll(store, env, now, cfn)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"pending update not reconciled: {e}")


# ── Why an update didn't succeed, in plain words ──────────────────────────
#
# CloudFormation's own text (ARNs, API names) goes to `technical`, which
# Settings, System shows under Technical details. `summary` says what
# happened and where the install is; `fix` is the next step; `reason` is
# both, for the history, the email and dashboards that read only `reason`.

PREPARE = "prepare"      # the change set: nothing ran
ROLLED_BACK = "rolled_back"
STOPPED = "stopped"      # ran, and didn't end cleanly
UNANSWERED = "unanswered"  # ran to UPDATE_COMPLETE, and the new release doesn't answer

_NOT_ALLOWED = re.compile(r"is not authorized to perform:?\s*([\w-]+:[\w*]+)", re.I)
_TRANSFORM = re.compile(r":transform/[\w-]+", re.I)
_NEXT_RELEASE = "Update again once a newer release is out (CoderHelm checks for one every hour)."
_SAME_AGAIN = "If the next try fails the same way, wait for a newer release (CoderHelm checks every hour)."


def _cause(raw):
    """(what went wrong, the next step) for CloudFormation's text, or (None, None)."""
    text = raw or ""
    low = text.lower()
    m = _NOT_ALLOWED.search(text)
    if m and m.group(1).lower() == "cloudformation:createchangeset" and _TRANSFORM.search(text):
        return ("CoderHelm's update role isn't allowed to use an AWS template feature this release needs", _NEXT_RELEASE)
    if m or "accessdenied" in low or "access denied" in low:
        return ("CoderHelm's update role isn't allowed to do something this release needs",
                _NEXT_RELEASE + " If that one fails the same way, send the technical details to CoderHelm support.")
    if "rate exceeded" in low or "throttl" in low or "too many requests" in low:
        return ("AWS was busy and turned some calls away", "Press Update again in a few minutes.")
    if "limitexceeded" in low or "limit exceeded" in low or "quota" in low:
        return ("your AWS account reached one of its limits",
                "Raise the limit the technical details name (AWS console, Service Quotas) or remove what you don't use, then press Update again.")
    if "already exists" in low:
        return ("something with the same name is already in your AWS account",
                "Remove or rename the leftover the technical details name, then press Update again.")
    if "not the verified one" in low or "doesn't match its signed checksum" in low:
        return ("what AWS read didn't match the signed release, so CoderHelm stopped it",
                "Press Update again. If it happens again, send the technical details to CoderHelm support.")
    if ("does not exist" in low and "change set" in low) or "change set was deleted" in low:
        return ("the prepared change was deleted in AWS before it ran", "Press Update again.")
    if PREFLIGHT_MARK.lower() in low:
        return ("it needs permissions CoderHelm's update role doesn't have, so it was stopped before anything ran",
                "An AWS admin applies the permission change shown under Version, Permissions (one link to the AWS console), then press Update again.")
    if "can no longer run" in low or "stack changed since" in low:
        return ("the install changed after the update was prepared", "Press Update again.")
    return (None, None)


def explain_failure(raw, version, from_version, phase):
    """{summary, fix, technical, reason} for an update that didn't succeed."""
    raw = str(raw or "").strip()
    v = version or "the new release"
    was = from_version or "the release it ran before"
    what, fix = _cause(raw)
    part = re.match(r"([A-Za-z][A-Za-z0-9]{0,63}): ", raw)
    broken = f"the {part.group(1)} part of the install could not be updated" if part else "a part of the install could not be updated"
    if phase == UNANSWERED:
        # Never rolled back by itself: from_version stays in the history for Roll back.
        summary = f"The update finished but the new version doesn't answer: {raw}."
        return {"summary": summary, "fix": f"Use Roll back to return to {was}.", "technical": f"GET /api/me answered {raw}",
                "reason": f"{summary} Use Roll back to return to {was}."}
    if phase == PREPARE:
        summary = f"The update couldn't start: {what or 'AWS could not prepare the change this release needs'}. Nothing changed: your install stayed on {was}."
        fix = fix or "Press Update again. " + _SAME_AGAIN
    elif phase == ROLLED_BACK:
        summary = f"The update to {v} stopped: {what or broken}. AWS put everything back: your install stayed on {was}."
        fix = fix or "Press Update again. " + _SAME_AGAIN
    else:
        summary = (f"The update to {v} stopped partway: {what or broken}. "
                   f"AWS couldn't put everything back by itself, so the install may be between {was} and {v}.")
        fix = "Open the stack in the AWS console to see what it says, then press Update again."
    return {"summary": summary, "fix": fix, "technical": raw, "reason": f"{summary} {fix}"}


def native_finish(store, env, now, hid, status, reason, phase=None):
    state = store.get("UPDATE_STATE", {})
    if state.get("review_id") == hid:
        state.pop("review_id", None)
        state.update({"running_id": hid})
        store.put("UPDATE_STATE", state)
    event = {"action": "finished", "history_id": hid, "status": status, "reason": reason}
    if status != "succeeded":
        entry = store.get(hid, {}) or {}
        phase = phase or (ROLLED_BACK if status == "rolled_back" else STOPPED)
        event.update(explain_failure(reason, entry.get("version"), entry.get("from_version"), phase))
    return handle(event, env, store, now)


# ── Actions ───────────────────────────────────────────────────────────────

def app_url(env):
    return (env.get("APP_URL") or "").rstrip("/")


def not_started_reason(e):
    """Plain words for why an update didn't start (nothing was changed)."""
    if isinstance(e, ValueError):
        return str(e)[:500]
    return f"couldn't prepare the update ({type(e).__name__}: {str(e)[:200]}); nothing was changed, try again"


def not_started_details(e, version, from_version):
    """{summary, fix, technical} for an AWS refusal before anything changed, or {}."""
    if isinstance(e, ValueError):
        return {}
    d = explain_failure(f"{type(e).__name__}: {e}", version, from_version, PREPARE)
    return {k: d[k][:1000] for k in ("summary", "fix", "technical")}


def begin(store, env, now, version, actor, mode, extra=None, note=None, scheduled=False, consent=False):
    """Starts the update and records it in history.

    History holds updates that started: the row is written once the change
    set (or the build) exists. A wait (the release still publishing or not
    verified yet) writes nothing; any other refusal before anything changed
    is recorded as not_started with its reason, never as failed."""
    state = store.get("UPDATE_STATE", {})
    native = is_native(env)
    live = (store.get(state["running_id"], {}) or {}) if native and state.get("running_id") else {}
    # A prepared update that hasn't run doesn't block the next one: it is replaced.
    if running(state, now) and not (native and unapplied(live)):
        raise ValueError("an update is already running")
    if native:
        check_replaceable(store, env, state)
    current = env.get("CODERHELM_VERSION", "")
    if parse_version(current):
        state["ran_versions"] = sorted(set(state.get("ran_versions") or []) | {current})
    hid = f"UPDATE#{now.strftime('%Y-%m-%dT%H:%M:%SZ')}#{uuid.uuid4().hex[:8]}"
    entry = {"version": version, "from_version": current, "actor": actor, "mode": mode,
             "status": "running", "started_at": now.isoformat(), "reason": ""}
    if note:
        entry["note"] = note
    if scheduled:
        # The maintenance window started it: it applies only while auto-update is on.
        entry["scheduled"] = True
    elif consent:
        # The admin pressed Update next to "it may change what CoderHelm's own
        # roles may do, inside the install's permissions boundary": that press
        # is their OK for permission changes (see native_hold). Never for data.
        entry["consent"] = {"permissions": True, "by": actor, "at": now.isoformat()}

    def refused(e):
        entry.update({"status": "not_started", "reason": not_started_reason(e), "finished_at": now.isoformat()})
        entry.update(not_started_details(e, version, current))
        store.put(hid, entry)
        if isinstance(e, ValueError):
            raise e
        raise ValueError(entry["reason"]) from e

    if is_native(env):
        try:
            entry.update(native_propose(store, env, version, mode, extra))
        except ReleaseNotReady:
            raise
        except Exception as e:  # noqa: BLE001  nothing changed: say so, never leave it running
            refused(e)
        entry["status"] = "preparing"
        store.put(hid, entry)
        supersede_open(store, env, state, now, hid, version)
    else:
        store.put(hid, entry)
        try:
            entry["build_id"] = start_build(env, version, actor, mode, hid, extra) if extra else start_build(env, version, actor, mode, hid)
        except Exception as e:  # noqa: BLE001
            refused(e)
    store.put(hid, entry)
    state.update({"running_since": now.isoformat(), "running_id": hid})
    store.put("UPDATE_STATE", state)
    return dict(entry, id=hid)


def history_view(entries, state, now):
    """History as people read it. A row left running or preparing that isn't
    the update in progress has ended: one that never made a change set or a
    build stopped before it changed anything (not_started); one past the
    running limit ended without a result."""
    live = {state.get("running_id"), state.get("review_id")} - {None}
    out = []
    for h in entries:
        if unapplied(h) and h.get("id") not in live:
            # Prepared and never run, and no longer the update pending: a
            # newer one replaced it (an older updater left it open).
            h = dict(h, status="superseded",
                     reason=h.get("reason") or "replaced by a newer update before it was applied; nothing was changed")
        if h.get("status") in ("running", "preparing") and h.get("id") not in live:
            if not h.get("change_set_id") and not h.get("build_id"):
                h = dict(h, status="not_started",
                         reason=h.get("reason") or "stopped before it changed anything; a later try is the one that counts")
            else:
                try:
                    old = now - datetime.fromisoformat(h.get("started_at") or "") > RUNNING_STALE
                except ValueError:
                    old = True
                if old:
                    h = dict(h, status="failed", reason=h.get("reason") or "ended without a result; check the stack in AWS")
        if h.get("status") in ("failed", "rolled_back") and h.get("finished_at") and h.get("change_set_id") and h.get("reason") and not h.get("summary"):
            # Written by an older updater with CloudFormation's own text: plain words, the text under technical.
            raw = h["reason"]
            ran = bool(h.get("executed_at"))
            phase = ROLLED_BACK if h["status"] == "rolled_back" else (STOPPED if ran else PREPARE)
            d = explain_failure(raw, h.get("version"), h.get("from_version"), phase)
            h = dict(h, reason=d["reason"], summary=d["summary"], fix=d["fix"], technical=d["technical"])
        out.append(h)
    return out


def install_identity(env):
    """The install's name and region (its resources are coderhelm-<name>-*), or None for a custom prefix."""
    prefix = env.get("RESOURCE_PREFIX") or ""
    region = env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or ""
    m = re.fullmatch(r"coderhelm-([a-z][a-z0-9-]{0,19})", prefix)
    return {"stage": m.group(1), "region": region} if m and region else None


def handle(event, env, store, now, fetch=fetch_releases, probe=None, hint=None):
    """`hint(channel, etag=...)` reads the channel's hint, latest.json or
    beta.json (fetch_latest in the Lambda; none in tests unless given)."""
    probe = probe or invoke_worker
    action = (event or {}).get("action", "tick")
    current = env.get("CODERHELM_VERSION", "")
    repo = env.get("RELEASE_REPO", "CoderHelm/coderhelm-platform")
    settings = validate_settings(store.get("AUTO_UPDATE", {}) or {}, defaults_from_env(env))

    def releases(max_age, refresh=False):
        """The release list no older than `max_age` (rate limited), what to
        offer from it, and `behind` when a newer release isn't listed yet.
        The channel's hint is read at least every HINT_SECONDS, so a release
        published since the list was read shows within minutes; `refresh`
        (Check for updates) reads both again unless just read."""
        ts = now.timestamp()
        gap = REFRESH_GAP_SECONDS if refresh else FETCH_GAP_SECONDS
        known = latest_hint(store, ts, min(max_age, HINT_SECONDS), hint, settings["channel"], gap)
        rel = releases_cached(store, repo, ts, fetch, max_age, newer=known, gap=gap)
        latest, behind = offer(store, rel, settings, current, ts, known)
        return rel, latest, behind
    if action in ("tick", "status", "update_now", "model_check", "native_poll", "domain_status") and ((store.get("UPDATE_STATE", {}) or {}).get("verifying") or {}).get("claim"):
        confirm_check(store, env, now, probe)
    state = store.get("UPDATE_STATE", {})

    if action == "status":
        # Opening Settings, System reads the list again when it is a couple
        # of minutes old; Check for updates (refresh) reads it now.
        _, latest, behind = releases(0, refresh=True) if event.get("refresh") else releases(FRESH_SECONDS)
        mc_view = model_check_view(store, env, now, latest["version"], probe) if latest else None
        # An Update press that waited for the check carries on (or ends) here too.
        follow_request(store, env, now, probe, latest=latest["version"] if latest else None, record=mc_view)
        if (store.get("UPDATE_REQUEST") or {}).get("version") != (mc_view or {}).get("version") and latest:
            mc_view = model_check_view(store, env, now, latest["version"], probe)
        if is_native(env):
            # The pending update as CloudFormation has it now, not as last stored.
            reconcile(store, env, now)
        state = store.get("UPDATE_STATE", {})
        return {
            "current": {
                "version": current or None,
                "commit": env.get("CODERHELM_COMMIT") or None,
                "released_at": env.get("CODERHELM_RELEASED_AT") or None,
                "config_fingerprint": env.get("CODERHELM_CONFIG_FINGERPRINT") or None,
            },
            "latest": latest,
            "update_available": latest is not None,
            "releases": releases_freshness(store, now.timestamp(), behind),
            "settings": settings,
            "running": running(state, now),
            # Releases whose last try failed or rolled back: the dashboard's
            # update preflight shows them with the reason from history.
            "failed_versions": sorted(state.get("failed_versions") or []),
            "history": history_view(store.history(), state, now),
            # The model check for the latest release (Settings, System and Setup).
            "model_check": mc_view,
            # An admin's Update press waiting for the release check (or how it ended).
            "update_request": request_view(store.get("UPDATE_REQUEST"), now),
            # Which install this is, for Settings, System, Uninstall's link to the uninstall page.
            "install": install_identity(env),
            # A native install: the stack, and the change set an admin reviews.
            "native": native_status(store, env, state) if is_native(env) else None,
            # What happens to the data when the stack is deleted (Settings, System, Uninstall).
            "uninstall": uninstall_status(store, env),
        }

    if action == "uninstall_mode":
        return set_uninstall_mode(store, env, now, event)

    if action == "settings":
        new = validate_settings(event.get("settings"), settings)
        store.put("AUTO_UPDATE", new)
        return {"settings": new}

    if action == "update_now":
        version = event.get("version")
        if not version:
            # The newest release right now, never the copy the page showed.
            _, latest, behind = releases(0)
            if behind:
                raise ValueError(behind["reason"] + ". Try again in a minute.")
            if not latest:
                raise ValueError("already on the latest release")
            version = latest["version"]
        if not parse_version(version):
            raise ValueError("version must look like v1.2.3")
        # Forward only: an older (or the same) release is a rollback.
        if parse_version(current) and compare(version, current) <= 0:
            raise ValueError(f"{version} is not newer than {current}; use Roll back to go to an earlier release")
        actor = event.get("actor") or DIRECT_ACTOR
        if running(state, now):
            live = (store.get(state["running_id"], {}) or {}) if state.get("running_id") else {}
            if live.get("version") == version and live.get("status") in ("running", "preparing", "review"):
                # Pressed again (a second click, or a waiting press already
                # started it): the update in progress is the answer.
                return dict(live, id=state["running_id"], state="started")
            if not (is_native(env) and unapplied(live)):
                raise ValueError(f"the update to {live.get('version') or 'another version'} is still running; this one can start once it finishes")
        mc = model_check(store, env, now, version, repo, probe)
        if mc.get("pending"):
            # Not a refusal: the press is kept, and the update starts by
            # itself once the check passes (follow_request).
            req = record_request(store, now, version, actor, bool(event.get("skip_check")), consent=bool(event.get("accept_permissions")))
            return {"state": "checking", "version": version, "request": request_view(req, now), "model_check": mc}
        if failures(mc, required=True):
            # No override: the release would call a model this install can't.
            raise ModelsBlocked(blocked_message(mc), mc)
        note = None
        if failures(mc):
            if not event.get("skip_check"):
                raise ModelsBlocked(blocked_message(mc) + " This release doesn't call it, so you can skip this check.",
                                    dict(mc, skippable=True))
            note = "model check skipped for " + ", ".join(f.get("model", "?") for f in failures(mc))
        try:
            started = begin(store, env, now, version, actor, "update", note=note, consent=bool(event.get("accept_permissions")))
        except ReleaseNotReady as e:
            # Still publishing, or its template not verified yet: a wait, kept like a check.
            req = record_request(store, now, version, actor, bool(event.get("skip_check")), waiting_for=str(e),
                                 consent=bool(event.get("accept_permissions")))
            return {"state": "checking", "version": version, "request": request_view(req, now), "model_check": mc}
        req = store.get("UPDATE_REQUEST") or {}
        if req.get("status") == "waiting":
            # This press replaces the one that waited.
            settle_request(store, req, now, "started", id=started.get("id"))
        stored = store.get("MODEL_CHECK") or {}
        if stored.get("version") == version and (stored.get("pending") or stored.get("stalled")):
            # Written meanwhile by an older check: the one that just passed stands.
            store.put("MODEL_CHECK", mc)
        return dict(started, state="started")

    if action == "models_verified":
        # From the release check build, after the signature check. Only a
        # claim until confirm_check reads that build's own result.
        version = event.get("version")
        if not parse_version(version):
            raise ValueError("version must look like v1.2.3")
        problem = claim_problem(event, state, version)
        if problem:
            raise ValueError(problem)
        if event.get("status") == "unavailable":
            # A release file didn't download (GitHub didn't answer): nothing
            # was verified or refused. The version route starts the check
            # again once CodeBuild shows the build ended (retry_check).
            logger.warning(f"Release check for {version} couldn't download: {event.get('reason')}")
            state["verifying"]["unavailable"] = True
            store.put("UPDATE_STATE", state)
            return {"verified": False, "retrying": True}
        if event.get("status") != "verified":
            logger.warning(f"Release check for {version} failed: {event.get('reason')}")
            state.pop("verifying", None)
            store.put("UPDATE_STATE", state)
            mc = store.get("MODEL_CHECK") or {}
            if mc.get("version") == version and mc.get("pending"):
                # Never "Checking" for a check that ended: it holds the update, with its reason.
                why = fit(event.get("reason") or "its signature or checksums didn't verify", 300)
                mc.update(pending=False, stalled=True, ok=False, checked_at=now.isoformat(),
                          note=f"The release check couldn't verify {version}: {why}. Check again to start it over.")
                store.put("MODEL_CHECK", mc)
            follow_request(store, env, now, probe, record=mc)
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
            _, latest, _ = releases(FRESH_SECONDS)
            if not latest:
                return {"model_check": None}
            version = latest["version"]
        mc = model_check(store, env, now, version, repo, probe)
        follow_request(store, env, now, probe, record=mc)
        return {"model_check": mc, "request": request_view(store.get("UPDATE_REQUEST"), now)}

    if action == "rollback":
        target = event.get("version")
        if not target:
            # A finished update whose release doesn't answer goes back to where it came from too.
            done = [h for h in store.history(50) if (h.get("status") == "succeeded" or h.get("unanswered")) and h.get("from_version")]
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
        return begin(store, env, now, target, event.get("actor") or DIRECT_ACTOR, "rollback")

    if action == "apply_update":
        return native_apply(store, env, now, event)

    if action == "set_options":
        return set_options(store, env, now, event)

    if action == "open_window":
        return open_window(store, env, now, event)

    if action == "finished":
        close_window(env)
        hid = event.get("history_id") or state.get("running_id")
        entry = store.get(hid, {}) if hid else {}
        status = event.get("status") if event.get("status") in ("succeeded", "failed", "rolled_back") else "failed"
        entry.update({"status": status, "reason": str(event.get("reason") or "")[:500], "finished_at": now.isoformat()})
        for k in ("summary", "fix", "technical"):
            # Plain words (summary, fix) and AWS's own text (technical) apart.
            if event.get(k):
                entry[k] = str(event[k])[:1000]
        if hid:
            store.put(hid, entry)
        state.pop("running_since", None)
        state.pop("running_id", None)
        version = entry.get("version") or event.get("version")
        if entry.get("mode") == "options":
            # A switch keeps the release: it says nothing about the version, and
            # the card shows how it ended (history keeps the row).
            store.put("UPDATE_STATE", state)
            return {"status": status, "version": version, "options": entry.get("options")}
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
        if status != "succeeded":
            # A finished update is in the history; only a failure emails.
            notify_once(
                store, f"{status}:{version}", now,
                NOTICE_SUBJECTS[status].format(version=version),
                f"{entry.get('reason') or 'Done.'}\nActor: {entry.get('actor', 'auto')}\n{app_url(env)}/settings/system/",
                env,
                what=status,
                version=version,
                facts={
                    "current": entry.get("from_version"),
                    "checked_at": checked_at(now),
                    "reason": entry.get("reason"),
                    "action": "Open the update's log under Settings, System, Update history, fix what it names, then press Update now.",
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
        return native_poll(store, env, now, wait=True)

    if action == "domain_set":
        return domain_set(store, env, now, event.get("hostname"), event.get("actor") or DIRECT_ACTOR)

    if action == "domain_remove":
        return domain_remove(store, env, now, event.get("actor") or DIRECT_ACTOR)

    if action == "tick":
        # No update running: no deploy window either.
        if not running(state, now):
            close_window(env)
        rel, latest, behind = releases(CACHE_SECONDS)
        mc = store.get("MODEL_CHECK")
        # First sight of a release, and daily while a model is refused.
        if latest and not running(state, now) and needs_check(mc, latest["version"], now):
            mc = model_check(store, env, now, latest["version"], repo, probe)
            notify_blocked(store, env, mc)
        if not behind:
            # An Update press that waited for the check carries on.
            follow_request(store, env, now, probe, latest=latest["version"] if latest else None)
            state = store.get("UPDATE_STATE", {})
            mc = store.get("MODEL_CHECK")
        if behind:
            # Never an older release while a newer one is out.
            d = {"action": "none", "version": behind["version"], "reason": behind["reason"]}
        else:
            d = decide(now, settings, current, rel, state, mc)
        if d["action"] in ("notify", "update") and state.get("notified_version") != d.get("version"):
            # Seen once. No email: Settings, System shows a new release
            # (a major one too), and the digest sums them up.
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
                begin(store, env, now, d["version"], "auto", "update", scheduled=True)
        remind_waiting(store, env, now, current)
        send_digest(store, env, now, settings, rel, current)
        logger.info(json.dumps(d))
        return d

    raise ValueError(f"unknown action: {action}")


def handler(event, context):
    env = dict(os.environ)
    store = Store(client("dynamodb"), env["SETTINGS_TABLE_NAME"])
    now = datetime.now(timezone.utc)
    try:
        return {"ok": True, **handle(event, env, store, now, hint=fetch_latest)}
    except ModelsBlocked as e:
        return {"ok": False, "error": str(e), "model_check": e.record}
    except ReleaseNotReady as e:
        return {"ok": False, "error": str(e), "waiting": True}
    except ValueError as e:
        return {"ok": False, "error": str(e)}
