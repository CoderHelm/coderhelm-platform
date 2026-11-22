#!/usr/bin/env python3
"""
Turn off "Require single sign-on" on a self-hosted CoderHelm install.

Use it when the organization's identity provider (its Okta or its Microsoft
Entra tenant) is down or misconfigured, so nobody can sign in. There is no
way around the rule inside CoderHelm itself: this script is the way back in,
and it works only with the install's own AWS credentials (the account the
install runs in), which is who already controls the install.

What it does, in one DynamoDB transaction on the install's settings table:

1. Turns the rule off on the organization record (`pk = ORG`, `sk = META`):
   removes `require_sso` and `require_sso_since`, and marks
   `sso_recovery_notice` so the install emails every owner at its next
   request (through its own mail settings, the same way it sends every
   other email).
2. Writes a line in the organization's audit log (Settings, Roles, Audit
   log), with who ran it and the note you give.

It never makes a session, never signs anyone in, and changes nothing else.
Everyone signs in again the usual ways (password, Google, GitHub, Microsoft
or Okta) until an owner turns the rule back on.

Usage (dry run first, then --apply):

    python3 scripts/sso-recovery.py --table <SETTINGS_TABLE_NAME> \\
        --operator "Your name" --note "Okta was down; turned off to sign in"
    python3 scripts/sso-recovery.py --table <SETTINGS_TABLE_NAME> \\
        --operator "Your name" --note "..." --apply

<SETTINGS_TABLE_NAME> is the `SETTINGS_TABLE_NAME` environment variable of
the install's API function (AWS Lambda console, the API function,
Configuration, Environment variables). Run it with the install account's
credentials and region (`AWS_PROFILE`, `AWS_REGION`, or --region).

Needs boto3 (`pip install boto3`).
"""

import argparse
import datetime
import secrets
import sys

ORG_PK = "ORG"
ORG_SK = "META"
# The audit log's rows (services/gateway/src/routes/roles.rs).
AUDIT_PREFIX = "ROLE_AUDIT#"
AUDIT_DAYS = 365
ACTION = "sign_in_rules"
ACTOR_ID = "SYSTEM#sso-recovery"
NOTICE = "sso_recovery_notice"
PROVIDERS = {"okta": "Okta", "microsoft": "Microsoft Entra"}


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def stamp(t):
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def read_org(dynamo, table):
    out = dynamo.get_item(
        TableName=table,
        Key={"pk": {"S": ORG_PK}, "sk": {"S": ORG_SK}},
        ConsistentRead=True,
    )
    return out.get("Item")


def rule_of(item):
    """The provider the organization requires (a slug), or None."""
    v = (item or {}).get("require_sso", {}).get("S", "")
    return v or None


def check_args(operator, note):
    operator = (operator or "").strip()
    note = (note or "").strip()
    if len(operator) < 2:
        return None, None, "Give --operator: who is running this."
    if len(note) < 10:
        return None, None, "Give --note: why the rule is turned off, in a sentence."
    return operator[:100], note[:600], None


def recovery_items(table, org_team, provider, operator, note, t):
    """The transaction: the rule off (only while it is on) and the audit line."""
    at = stamp(t)
    ttl = int((t + datetime.timedelta(days=AUDIT_DAYS)).timestamp())
    name = PROVIDERS.get(provider, provider)
    detail = (
        f"Require single sign-on ({name}) turned off with the recovery script "
        f"by {operator}: {note}"
    )[:1000]
    return [
        {
            "Update": {
                "TableName": table,
                "Key": {"pk": {"S": ORG_PK}, "sk": {"S": ORG_SK}},
                "UpdateExpression": (
                    "REMOVE require_sso, require_sso_since "
                    f"SET {NOTICE} = :n, updated_at = :at, updated_by = :by"
                ),
                "ConditionExpression": "attribute_exists(require_sso)",
                "ExpressionAttributeValues": {
                    ":n": {"S": f"{provider}|{operator}|{at}"},
                    ":at": {"S": at},
                    ":by": {"S": f"Recovery script ({operator})"},
                },
            }
        },
        {
            "Put": {
                "TableName": table,
                "Item": {
                    "pk": {"S": org_team},
                    "sk": {"S": f"{AUDIT_PREFIX}{at}#{secrets.token_hex(8)}"},
                    "record_type": {"S": "role_audit"},
                    "at": {"S": at},
                    "actor": {"S": f"Recovery script: {operator}"},
                    "actor_id": {"S": ACTOR_ID},
                    "action": {"S": ACTION},
                    "role_id": {"S": ""},
                    "role_name": {"S": ""},
                    "detail": {"S": detail},
                    "ttl": {"N": str(ttl)},
                },
                "ConditionExpression": "attribute_not_exists(pk)",
            }
        },
    ]


def run(dynamo, table, operator, note, apply, out=print, t=None):
    """The script's work against a DynamoDB client. Returns the exit code."""
    operator, note, why = check_args(operator, note)
    if why:
        out(why)
        return 2
    org = read_org(dynamo, table)
    if not org or not org.get("team_id", {}).get("S"):
        out("No organization record (ORG/META) in that table. Check --table.")
        return 1
    provider = rule_of(org)
    if provider is None:
        out("Require single sign-on is already off. Nothing to do.")
        return 0
    name = PROVIDERS.get(provider, provider)
    out(f"Require single sign-on is on ({name}).")
    if not apply:
        out("Dry run: nothing changed. Run again with --apply to turn it off.")
        return 0
    items = recovery_items(table, org["team_id"]["S"], provider, operator, note, t or now_utc())
    dynamo.transact_write_items(TransactItems=items)
    out(
        "Turned off. The audit log has the line, and every owner is emailed "
        "at the install's next request (open the sign-in page once); a mail failure is tried again every 10 minutes."
    )
    out("Sign in the usual way; an owner turns the rule back on once the provider works.")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--table", required=True, help="the install's SETTINGS_TABLE_NAME")
    ap.add_argument("--operator", required=True, help="who is running this")
    ap.add_argument("--note", required=True, help="why the rule is turned off")
    ap.add_argument("--region", default=None)
    ap.add_argument("--apply", action="store_true", help="make the change (default: dry run)")
    args = ap.parse_args(argv)
    import boto3  # only when run for real, so the tests need no boto3

    dynamo = boto3.client("dynamodb", region_name=args.region)
    return run(dynamo, args.table, args.operator, args.note, args.apply)


if __name__ == "__main__":
    sys.exit(main())
