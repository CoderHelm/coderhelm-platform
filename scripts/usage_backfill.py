#!/usr/bin/env python3
"""Recompute teams' monthly usage counters from the per-run records.

Dry run by default: prints, per team and month, what the analytics item holds
now and what it should hold. Nothing is written unless --apply is given.

What "should" is:
  * tokens: per part, the larger of the old counter and the four parts summed
    over the month's runs (runs table) and the reviews that stored their usage
    (REVIEW# items), for runs created before --cutoff (the old counter holds
    usage that never reached a run item, like plan chat; run items hold runs
    whose completion never reached the counter), plus whatever per-call
    accounting has counted since the cutoff (DAY#<day> items, written per
    model call from the deploy on);
  * cost: each run priced at the correct per-model rates (model_prices.json):
    passes with a trace (traces table, 30-day TTL) are priced at the model
    that pass uses (implement: the team's heavy model; others: primary);
    runs without traces at the heavy model (the upper bound; the primary-model
    lower bound is printed next to it). Reviews use the heavy model.
  * calls that never stored usage before per-call accounting (plan chat via
    the API, team agent asks outside runs, onboarding, infra analysis, memory
    extraction, log analysis) cannot be recovered; they are reported as such.

Usage:
  python3 scripts/usage_backfill.py                       # this month and last
  python3 scripts/usage_backfill.py --months 2026-09,2026-10 --stage prod
  python3 scripts/usage_backfill.py --cutoff 2026-10-18T22:00:00Z --apply

Run --apply once, after the deploy, with --cutoff set to the deploy time.
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key

HERE = os.path.dirname(os.path.abspath(__file__))
PRICES = json.load(open(os.path.join(HERE, "..", "services", "common", "model_prices.json")))["models"]
HEAVY_PASSES = {"implement", "review_agent", "release_notes"}

PARTS = ("tokens_in", "cache_read", "cache_write", "tokens_out")
# Until the 5.5 models shipped (PR #89, 2026-09-29) runs used these defaults.
LEGACY_HEAVY, LEGACY_PRIMARY = "claude-opus-4-8", "claude-sonnet-4-6"


def models_at(created_at, heavy, primary, legacy_before):
    if created_at and created_at < legacy_before:
        return LEGACY_HEAVY, LEGACY_PRIMARY
    return heavy, primary


def rates(model):
    m = (model or "").strip().lower()
    return PRICES.get(m) or max(PRICES.values(), key=lambda r: r["input"] + r["output"])


def cost(model, t):
    r = rates(model)
    return (
        t["tokens_in"] * r["input"]
        + t["tokens_out"] * r["output"]
        + t["cache_read"] * r["cache_read"]
        + t["cache_write"] * r["cache_write_5m"]
    ) / 1_000_000


def blended_old(t):
    """The old flat estimate: $10/M input, $50/M output, read 0.1x, write 1.25x."""
    return (t["tokens_in"] * 10 + t["tokens_out"] * 50 + t["cache_read"] * 1 + t["cache_write"] * 12.5) / 1_000_000


def n(item, *keys):
    for k in keys:
        if k in item and item[k] is not None:
            return int(Decimal(str(item[k])))
    return 0


def tokens_of(item, names=("tokens_in", "cache_read_tokens", "cache_write_tokens", "tokens_out")):
    return dict(zip(PARTS, (n(item, k) for k in names)))


def add(a, b):
    for k in PARTS:
        a[k] += b[k]


def zero():
    return {k: 0 for k in PARTS}


def scan_all(table, **kw):
    out, start = [], None
    while True:
        if start:
            kw["ExclusiveStartKey"] = start
        r = table.scan(**kw)
        out += r.get("Items", [])
        start = r.get("LastEvaluatedKey")
        if not start:
            return out


def query_all(table, **kw):
    out, start = [], None
    while True:
        if start:
            kw["ExclusiveStartKey"] = start
        r = table.query(**kw)
        out += r.get("Items", [])
        start = r.get("LastEvaluatedKey")
        if not start:
            return out


def main():
    ap = argparse.ArgumentParser()
    now = datetime.now(timezone.utc)
    last = (now.replace(day=1) - __import__("datetime").timedelta(days=1)).strftime("%Y-%m")
    ap.add_argument("--months", default=f"{last},{now.strftime('%Y-%m')}")
    ap.add_argument("--stage", default="prod")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--cutoff", default=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    help="runs created before this come from run records; after it, from per-call counters")
    ap.add_argument("--legacy-before", default="2026-09-29T19:00:00Z",
                    help="runs created before this used the 4.x defaults (opus-4-8 / sonnet-4-6)")
    ap.add_argument("--apply", action="store_true", help="write the recomputed counters (default: print only)")
    a = ap.parse_args()
    months = [m.strip() for m in a.months.split(",") if m.strip()]
    ddb = boto3.resource("dynamodb", region_name=a.region)
    p = f"coderhelm-{a.stage}"
    runs_t, an_t = ddb.Table(f"{p}-runs"), ddb.Table(f"{p}-analytics")
    tr_t, st_t = ddb.Table(f"{p}-traces"), ddb.Table(f"{p}-settings")

    runs = scan_all(
        runs_t,
        ProjectionExpression="team_id, run_id, created_at, tokens_in, tokens_out, cache_read_tokens, cache_write_tokens, cost_usd",
        FilterExpression=Attr("created_at").begins_with(months[0]) if len(months) == 1 else
        Attr("created_at").gte(months[0]) & Attr("created_at").lt(months[-1] + "-99"),
    )
    by_team = defaultdict(list)
    for r in runs:
        by_team[r["team_id"]].append(r)

    for team in sorted(by_team):
        prov = st_t.get_item(Key={"pk": team, "sk": "SETTINGS#MODEL_PROVIDER"}).get("Item", {})
        heavy = prov.get("heavy_model") or "claude-opus-5-5"
        primary = prov.get("primary_model") or "claude-sonnet-5-5"
        traces = defaultdict(list)
        for t in query_all(tr_t, KeyConditionExpression=Key("team_id").eq(team) & Key("sk").begins_with("RUN#")):
            parts = t["sk"].split("#")
            if len(parts) >= 4:
                traces[parts[1]].append(t)
        reviews = query_all(st_t, KeyConditionExpression=Key("pk").eq(team) & Key("sk").begins_with("REVIEW#"),
                            FilterExpression=Attr("tokens_in").exists())

        for month in months:
            item = an_t.get_item(Key={"team_id": team, "period": month}).get("Item", {})
            before = {"tokens": tokens_of(item, ("total_tokens_in", "cache_read_tokens", "cache_write_tokens", "total_tokens_out")),
                      "cost": float(item.get("total_cost_usd", 0))}
            hist, run_cost_old, cost_est, cost_low, traced = zero(), 0.0, 0.0, 0.0, 0
            n_runs = 0
            for r in by_team[team]:
                ca = r.get("created_at", "")
                if not ca.startswith(month) or ca >= a.cutoff:
                    continue
                n_runs += 1
                t = tokens_of(r)
                add(hist, t)
                run_cost_old += float(r.get("cost_usd", 0))
                h, pm = models_at(ca, heavy, primary, a.legacy_before)
                tr = traces.get(r["run_id"])
                if tr:
                    traced += 1
                    rest = dict(t)
                    for pss in tr:
                        pt = tokens_of(pss, ("input_tokens", "cache_read_tokens", "cache_write_tokens", "output_tokens"))
                        model = h if pss.get("pass") in HEAVY_PASSES else pm
                        cost_est += cost(model, pt)
                        for k in PARTS:
                            rest[k] = max(rest[k] - pt[k], 0)
                    cost_est += cost(pm, rest)  # untraced remainder (feedback, CI fixes)
                    cost_low += cost(pm, t)
                else:
                    cost_est += cost(h, t)
                    cost_low += cost(pm, t)
            n_rev = 0
            for rv in reviews:
                ca = rv.get("created_at", "")
                if not ca.startswith(month) or ca >= a.cutoff:
                    continue
                n_rev += 1
                t = tokens_of(rv)
                add(hist, t)
                h, pm = models_at(ca, heavy, primary, a.legacy_before)
                cost_est += cost(h, t)
                cost_low += cost(pm, t)
            # Per-call accounting since the cutoff (DAY# items exist only from the deploy on).
            live, live_cost = zero(), 0.0
            for d in range(1, 32):
                day = f"{month}-{d:02d}"
                if day < a.cutoff[:10]:
                    continue
                di = an_t.get_item(Key={"team_id": team, "period": f"DAY#{day}"}).get("Item")
                if di:
                    add(live, tokens_of(di, ("total_tokens_in", "cache_read_tokens", "cache_write_tokens", "total_tokens_out")))
                    live_cost += float(di.get("total_cost_usd", 0))
            # The old counter also holds usage that never reached a run item
            # (plan chat, cancelled runs, feedback rounds whose run item was
            # later overwritten). Keep whichever is larger per part, and
            # price that excess at the heavy model (plan chat's model).
            excess = {k: max(before["tokens"][k] - hist[k], 0) for k in PARTS}
            after = zero()
            for k in PARTS:
                after[k] = max(before["tokens"][k], hist[k])
            add(after, live)
            h, pm = models_at(f"{month}-15", heavy, primary, a.legacy_before)
            after_cost = cost_est + cost(h, excess) + live_cost
            cost_low += cost(pm, excess)
            tot = lambda t: sum(t.values())
            print(f"\n== {team} {month}: {n_runs} runs ({traced} with per-pass traces), {n_rev} reviews with usage; "
                  f"models heavy={heavy} primary={primary} (before {a.legacy_before}: {LEGACY_HEAVY}/{LEGACY_PRIMARY})")
            print(f"   {'':22}{'new input':>14}{'cache read':>14}{'cache write':>14}{'output':>14}{'total':>15}")
            for label, t in (("before (counter)", before["tokens"]), ("runs + reviews", hist),
                             ("after (to write)", after)):
                print(f"   {label:22}" + "".join(f"{t[k]:>14,}" for k in PARTS) + f"{tot(t):>15,}")
            print(f"   cost  before counter ${before['cost']:,.2f} | runs' stored (old blended) ${run_cost_old:,.2f}"
                  f" | old blended on these tokens ${blended_old(hist):,.2f}")
            print(f"         after: per-model ${after_cost:,.2f} (untraced runs at the heavy model; all at the primary model: ${cost_low + live_cost:,.2f})")
            if a.apply:
                an_t.update_item(
                    Key={"team_id": team, "period": month},
                    UpdateExpression="SET total_tokens_in = :i, tokens_in_uncached = :i, cache_read_tokens = :r, "
                                     "cache_write_tokens = :w, total_tokens_out = :o, total_tokens = :t, "
                                     "total_cost_usd = :c, backfilled_at = :b",
                    ExpressionAttributeValues={
                        ":i": after["tokens_in"], ":r": after["cache_read"], ":w": after["cache_write"],
                        ":o": after["tokens_out"], ":t": tot(after), ":c": Decimal(f"{after_cost:.6f}"),
                        ":b": now.isoformat(),
                    },
                )
                print("   written")
    if not a.apply:
        print("\n(dry run: nothing written; add --apply to write the 'after' rows)")


if __name__ == "__main__":
    sys.exit(main())
