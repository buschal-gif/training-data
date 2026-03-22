#!/usr/bin/env python3
"""
pending_workouts.json → Intervals.icu Calendar
Idempotent: dedup via content hash, skip already-pushed IDs.
Description-only mode — all workouts use Intervals.icu description syntax.

Usage:
  python push.py --athlete-id i123456 --intervals-key YOUR_KEY
  python push.py --dry-run   # simulate without writing

Version 1.1 — description-only, workout_doc removed
"""

import requests
import json
import os
import hashlib
import argparse
from datetime import datetime
from pathlib import Path


INTERVALS_BASE_URL = "https://intervals.icu/api/v1"

SPORT_TYPE_MAP = {
    "Ride": "Ride",
    "VirtualRide": "VirtualRide",
    "MountainBikeRide": "MountainBikeRide",
    "GravelRide": "GravelRide",
    "Run": "Run",
    "VirtualRun": "VirtualRun",
    "TrailRun": "TrailRun",
    "Swim": "Swim",
    "Rowing": "Rowing",
    "WeightTraining": "WeightTraining",
    "NordicSki": "NordicSki",
    "Walk": "Walk",
    "Workout": "Workout",
}

CATEGORY_MAP = {
    "WORKOUT": "WORKOUT",
    "RACE_A": "RACE_A",
    "RACE_B": "RACE_B",
    "RACE_C": "RACE_C",
    "NOTE": "NOTE",
    "TARGET": "TARGET",
    "PLAN": "PLAN",
}


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------

def make_hash(workout: dict) -> str:
    """
    Deterministic content hash for idempotency.
    Same input → same ID, every time.
    Components: date + name + sport_type + moving_time
    """
    key = (
        str(workout.get("date", ""))
        + str(workout.get("name", ""))
        + str(workout.get("sport_type", ""))
        + str(workout.get("moving_time", 0))
    )
    return hashlib.sha256(key.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def load_pushed_ids(pushed_path: Path) -> set:
    """Load already-pushed workout IDs to enable idempotent runs."""
    if not pushed_path.exists():
        return set()
    try:
        with open(pushed_path) as f:
            data = json.load(f)
        return {entry["id"] for entry in data.get("pushed", [])}
    except Exception:
        return set()


def save_pushed_log(pushed_path: Path, new_entries: list):
    """Append newly pushed workouts to the audit log."""
    existing = []
    if pushed_path.exists():
        try:
            with open(pushed_path) as f:
                existing = json.load(f).get("pushed", [])
        except Exception:
            pass
    existing.extend(new_entries)
    with open(pushed_path, "w") as f:
        json.dump({"pushed": existing}, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def push_workout(athlete_id: str, auth: str, workout: dict,
                 dry_run: bool = False) -> dict:
    """
    POST a single workout to Intervals.icu /events endpoint.
    Description-only — no workout_doc or steps processing.
    Returns the created event dict on success.
    """
    url = f"{INTERVALS_BASE_URL}/athlete/{athlete_id}/events"
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json",
    }

    # Date: API requires datetime format, not date-only
    date_str = workout["date"]
    if "T" not in str(date_str):
        date_str = f"{date_str}T00:00:00"

    payload = {
        "start_date_local": date_str,
        "name": workout.get("name", "Workout"),
        "type": SPORT_TYPE_MAP.get(workout.get("sport_type", "Ride"), "Ride"),
        "category": CATEGORY_MAP.get(workout.get("category", "WORKOUT"), "WORKOUT"),
        "description": workout.get("description", ""),
    }

    if workout.get("moving_time"):
        payload["moving_time"] = int(workout["moving_time"])
    if workout.get("planned_tss") is not None:
        payload["load"] = float(workout["planned_tss"])
    if workout.get("distance"):
        payload["distance"] = float(workout["distance"])

    if dry_run:
        print(f"  [DRY RUN] Would push: {workout['date']} — {workout['name']}")
        return {"id": f"dry_{workout.get('id', 'unknown')}", **payload}

    response = requests.post(url, headers=headers, json=payload)

    if not response.ok:
        # Log full response for diagnosis before raising
        try:
            body = json.dumps(response.json(), indent=6)
        except Exception:
            body = response.text[:500]
        print(f"      API response: {body}")

    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Push pending_workouts.json to Intervals.icu calendar"
    )
    parser.add_argument("--athlete-id", help="Intervals.icu athlete ID")
    parser.add_argument("--intervals-key", help="Intervals.icu API key")
    parser.add_argument("--pending", default="pending_workouts.json",
                        help="Input file (default: pending_workouts.json)")
    parser.add_argument("--pushed-log", default="pushed_workouts.json",
                        help="Audit log file (default: pushed_workouts.json)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate — no writes to Intervals.icu")
    parser.add_argument("--no-clear", action="store_true",
                        help="Keep successfully pushed workouts in pending file")
    args = parser.parse_args()

    # Credentials: CLI → env
    athlete_id = args.athlete_id or os.getenv("ATHLETE_ID")
    intervals_key = args.intervals_key or os.getenv("INTERVALS_KEY")

    if not athlete_id or not intervals_key:
        print("❌  Missing ATHLETE_ID or INTERVALS_KEY")
        return 1

    import base64
    auth = base64.b64encode(f"API_KEY:{intervals_key}".encode()).decode()

    pending_path = Path(args.pending)
    pushed_path = Path(args.pushed_log)

    if not pending_path.exists():
        print("✅  No pending_workouts.json found — nothing to push")
        return 0

    with open(pending_path) as f:
        pending_data = json.load(f)

    workouts = pending_data.get("workouts", [])
    if not workouts:
        print("✅  pending_workouts.json is empty — nothing to push")
        return 0

    # Warn if any workout still has steps or workout_doc (ignored in v1.1)
    has_legacy = [w for w in workouts if w.get("steps") or w.get("workout_doc")]
    if has_legacy:
        print(f"⚠️   {len(has_legacy)} workout(s) have 'steps' or 'workout_doc' fields — ignored (description-only mode)")

    pushed_ids = load_pushed_ids(pushed_path)

    print(f"📋  {len(workouts)} workout(s) in pending_workouts.json")
    if pushed_ids:
        print(f"    Already pushed (will skip): {len(pushed_ids)} known ID(s)")
    if args.dry_run:
        print("🧪  DRY RUN — no writes to Intervals.icu")
    print()

    pushed_this_run = []
    remaining = []

    for workout in workouts:
        if not workout.get("id"):
            workout["id"] = make_hash(workout)

        wid = workout["id"]

        if wid in pushed_ids:
            print(f"  ⏭️   Skip (already pushed): {workout['date']} — {workout['name']} [{wid}]")
            continue

        try:
            result = push_workout(
                athlete_id, auth, workout,
                dry_run=args.dry_run
            )
            intervals_id = result.get("id", "unknown")
            print(f"  ✅  Pushed: {workout['date']} — {workout['name']}"
                  f" → Intervals ID: {intervals_id} [{wid}]")

            pushed_this_run.append({
                "id": wid,
                "intervals_event_id": intervals_id,
                "date": workout["date"],
                "name": workout["name"],
                "sport_type": workout.get("sport_type"),
                "pushed_at": datetime.now().isoformat(),
                "source": workout.get("source", "unknown"),
                "dry_run": args.dry_run,
            })

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            print(f"  ❌  HTTP {status}: {workout['date']} — {workout['name']}")
            remaining.append(workout)

        except Exception as e:
            print(f"  ❌  Unexpected error: {workout['date']} — {workout['name']}: {e}")
            remaining.append(workout)

    print()

    if pushed_this_run:
        save_pushed_log(pushed_path, pushed_this_run)
        print(f"📝  Audit log: {len(pushed_this_run)} new entries → {pushed_path}")

    if not args.no_clear:
        pending_data["workouts"] = remaining
        pending_data["last_pushed"] = datetime.now().isoformat()
        with open(pending_path, "w") as f:
            json.dump(pending_data, f, indent=2, default=str)

        if not remaining:
            print(f"🧹  pending_workouts.json cleared (all workouts pushed)")
        else:
            print(f"⚠️   {len(remaining)} workout(s) remain in pending (failed pushes)")

    skipped = len(workouts) - len(pushed_this_run) - len(remaining)
    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}"
          f"✅  Done — pushed: {len(pushed_this_run)}, "
          f"skipped (already pushed): {skipped}, "
          f"failed: {len(remaining)}")

    return 0 if not remaining else 1


if __name__ == "__main__":
    exit(main())
