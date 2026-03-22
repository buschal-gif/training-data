#!/usr/bin/env python3
"""
pending_workouts.json → Intervals.icu Calendar
Stateless: pushes everything, clears file after success.

Version 2.0
"""

import requests
import json
import os
import argparse
from datetime import datetime
from pathlib import Path


INTERVALS_BASE_URL = "https://intervals.icu/api/v1"

SPORT_TYPE_MAP = {
    "Ride": "Ride", "VirtualRide": "VirtualRide", "MountainBikeRide": "MountainBikeRide",
    "GravelRide": "GravelRide", "Run": "Run", "VirtualRun": "VirtualRun",
    "TrailRun": "TrailRun", "Swim": "Swim", "Rowing": "Rowing",
    "WeightTraining": "WeightTraining", "NordicSki": "NordicSki",
    "Walk": "Walk", "Workout": "Workout",
}

CATEGORY_MAP = {
    "WORKOUT": "WORKOUT", "RACE_A": "RACE_A", "RACE_B": "RACE_B",
    "RACE_C": "RACE_C", "NOTE": "NOTE", "TARGET": "TARGET", "PLAN": "PLAN",
}


def normalise(entry: dict) -> dict:
    w = dict(entry)
    if "sport_type" not in w and "sport" in w:
        w["sport_type"] = w["sport"]
    if "moving_time" not in w and "duration_min" in w:
        w["moving_time"] = int(w["duration_min"]) * 60
    if "planned_tss" not in w and "tss" in w:
        w["planned_tss"] = w["tss"]
    if "category" not in w:
        w["category"] = w.get("type", "WORKOUT")
    return w


def should_skip(entry: dict) -> tuple:
    if entry.get("existing") is True:
        return True, "existing: true"
    if entry.get("type") == "REST":
        return True, "REST day"
    if not (entry.get("sport_type") or entry.get("sport")):
        return True, "no sport"
    return False, ""


def push_workout(athlete_id: str, auth: str, workout: dict, dry_run: bool = False) -> dict:
    url = f"{INTERVALS_BASE_URL}/athlete/{athlete_id}/events"
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}

    date_str = workout["date"]
    if "T" not in str(date_str):
        date_str = f"{date_str}T00:00:00"

    sport_type = workout.get("sport_type") or workout.get("sport", "Ride")

    payload = {
        "start_date_local": date_str,
        "name": workout.get("name", "Workout"),
        "type": SPORT_TYPE_MAP.get(sport_type, sport_type),
        "category": CATEGORY_MAP.get(workout.get("category", "WORKOUT"), "WORKOUT"),
        "description": workout.get("description", ""),
    }

    if workout.get("moving_time"):
        payload["moving_time"] = int(workout["moving_time"])
    if workout.get("planned_tss") and float(workout["planned_tss"]) > 0:
        payload["load"] = float(workout["planned_tss"])
    if workout.get("distance"):
        payload["distance"] = float(workout["distance"])

    if dry_run:
        print(f"  [DRY RUN] {workout['date']} — {workout['name']}")
        return {"id": "dry_run"}

    response = requests.post(url, headers=headers, json=payload)
    if not response.ok:
        try:
            print(f"      API: {json.dumps(response.json())}")
        except Exception:
            print(f"      API: {response.text[:300]}")
    response.raise_for_status()
    return response.json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--athlete-id")
    parser.add_argument("--intervals-key")
    parser.add_argument("--pending", default="pending_workouts.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    athlete_id = args.athlete_id or os.getenv("ATHLETE_ID")
    intervals_key = args.intervals_key or os.getenv("INTERVALS_KEY")

    if not athlete_id or not intervals_key:
        print("❌  Missing ATHLETE_ID or INTERVALS_KEY")
        return 1

    import base64
    auth = base64.b64encode(f"API_KEY:{intervals_key}".encode()).decode()

    pending_path = Path(args.pending)
    if not pending_path.exists():
        print("✅  No pending_workouts.json — nothing to push")
        return 0

    with open(pending_path) as f:
        pending_data = json.load(f)

    raw = pending_data.get("workouts") or pending_data.get("sessions") or []
    source_key = "workouts" if "workouts" in pending_data else "sessions"

    if not raw:
        print("✅  Nothing to push")
        return 0

    # Filter
    to_push = []
    for entry in raw:
        skip, reason = should_skip(entry)
        if skip:
            print(f"  ⏭️   {entry.get('name', '?')} — {reason}")
        else:
            to_push.append(normalise(entry))

    if not to_push:
        print("✅  No pushable workouts after filtering")
        return 0

    if args.dry_run:
        print("🧪  DRY RUN — no writes to Intervals.icu\n")

    pushed = 0
    failed = []

    for workout in to_push:
        try:
            result = push_workout(athlete_id, auth, workout, dry_run=args.dry_run)
            print(f"  ✅  {workout['date']} — {workout['name']} → {result.get('id', '?')}")
            pushed += 1
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            print(f"  ❌  HTTP {status}: {workout['date']} — {workout['name']}")
            failed.append(workout)
        except Exception as e:
            print(f"  ❌  {workout['date']} — {workout['name']}: {e}")
            failed.append(workout)

    print(f"\n✅  Done — pushed: {pushed}, failed: {len(failed)}")

    # Clear file — leave only failed entries
    if not args.dry_run:
        pending_data[source_key] = failed
        pending_data["last_pushed"] = datetime.now().isoformat()
        with open(pending_path, "w") as f:
            json.dump(pending_data, f, indent=2, default=str)
        if not failed:
            print(f"🧹  {pending_path} cleared")

    return 0 if not failed else 1


if __name__ == "__main__":
    exit(main())
