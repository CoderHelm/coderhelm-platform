#!/usr/bin/env python3
"""
Migrate USER#, STRIPE_EVENTS, PAYMENT#, INVOICE#, and STRIPE# entities
from the main coderhelm-prod table to the new users and events tables.

Usage:
    python scripts/migrate-tables.py --dry-run   # Preview only
    python scripts/migrate-tables.py              # Execute migration

Requires: boto3, AWS credentials with DynamoDB access.
"""

import argparse
import boto3
from collections import defaultdict

MAIN_TABLE = "coderhelm-prod"
EVENTS_TABLE = "coderhelm-prod-events"
USERS_TABLE = "coderhelm-prod-users"

REGION = "us-east-1"


def get_client():
    session = boto3.Session(region_name=REGION, profile_name="nadya")
    return session.resource("dynamodb")


def scan_main_table(dynamo):
    """Scan the main table and classify items for migration."""
    table = dynamo.Table(MAIN_TABLE)
    events_items = []
    users_items = []

    scan_kwargs = {}
    while True:
        response = table.scan(**scan_kwargs)
        for item in response.get("Items", []):
            pk = item.get("pk", "")
            sk = item.get("sk", "")

            # Events table candidates
            if pk == "STRIPE_EVENTS":
                events_items.append(item)
            elif sk.startswith("PAYMENT#"):
                events_items.append(item)
            elif sk.startswith("INVOICE#"):
                events_items.append(item)
            elif pk.startswith("STRIPE#") and sk == "MAPPING":
                events_items.append(item)

            # Users table candidates
            elif sk.startswith("USER#"):
                users_items.append(item)

        if "LastEvaluatedKey" not in response:
            break
        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    return events_items, users_items


def batch_write(dynamo, table_name, items, dry_run):
    """Write items to target table in batches of 25."""
    table = dynamo.Table(table_name)
    total = len(items)
    written = 0

    for i in range(0, total, 25):
        batch = items[i : i + 25]
        if dry_run:
            written += len(batch)
            continue

        with table.batch_writer() as writer:
            for item in batch:
                writer.put_item(Item=item)
        written += len(batch)
        print(f"  {table_name}: {written}/{total}")

    return written


def batch_delete(dynamo, table_name, keys, dry_run):
    """Delete items from source table in batches of 25."""
    table = dynamo.Table(table_name)
    total = len(keys)
    deleted = 0

    for i in range(0, total, 25):
        batch = keys[i : i + 25]
        if dry_run:
            deleted += len(batch)
            continue

        with table.batch_writer() as writer:
            for key in batch:
                writer.delete_item(Key=key)
        deleted += len(batch)
        print(f"  {table_name} delete: {deleted}/{total}")

    return deleted


def main():
    parser = argparse.ArgumentParser(description="Migrate DynamoDB entities to new tables")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--no-delete", action="store_true", help="Copy only, don't delete from main table")
    args = parser.parse_args()

    dynamo = get_client()

    print(f"Scanning {MAIN_TABLE}...")
    events_items, users_items = scan_main_table(dynamo)

    # Summary
    events_by_type = defaultdict(int)
    for item in events_items:
        pk = item.get("pk", "")
        sk = item.get("sk", "")
        if pk == "STRIPE_EVENTS":
            events_by_type["STRIPE_EVENTS"] += 1
        elif sk.startswith("PAYMENT#"):
            events_by_type["PAYMENT#"] += 1
        elif sk.startswith("INVOICE#"):
            events_by_type["INVOICE#"] += 1
        elif pk.startswith("STRIPE#"):
            events_by_type["STRIPE#MAPPING"] += 1

    print(f"\nEvents table ({EVENTS_TABLE}):")
    for k, v in sorted(events_by_type.items()):
        print(f"  {k}: {v} items")
    print(f"  Total: {len(events_items)}")

    print(f"\nUsers table ({USERS_TABLE}):")
    print(f"  USER# records: {len(users_items)}")

    if args.dry_run:
        print("\n[DRY RUN] No changes made.")
        return

    # Write to new tables
    print(f"\nWriting to {EVENTS_TABLE}...")
    batch_write(dynamo, EVENTS_TABLE, events_items, False)

    print(f"Writing to {USERS_TABLE}...")
    batch_write(dynamo, USERS_TABLE, users_items, False)

    # Delete from main table (optional)
    if not args.no_delete:
        print(f"\nDeleting migrated items from {MAIN_TABLE}...")
        all_migrated = events_items + users_items
        keys = [{"pk": item["pk"], "sk": item["sk"]} for item in all_migrated]
        batch_delete(dynamo, MAIN_TABLE, keys, False)

    print("\nMigration complete.")


if __name__ == "__main__":
    main()
