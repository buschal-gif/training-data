#!/usr/bin/env python3
"""
pending_workouts.json → Intervals.icu Calendar
Idempotent: dedup via content hash, skip already-pushed IDs.
Description-only mode — all workouts use Intervals.icu description syntax.

Usage:
  python push.py --athlete-id i123456 --intervals-key YOUR_KEY
  python push.py --dry-run   # simulate without writing

Version 1.2 — accepts both 'workouts' and 'sessions' keys,
              skips REST entries and entries with existing: true,
              auto-builds description from targets when missing
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
    key = (
        str(workout.get("date", ""))
        + str(workout.get("name", ""))
        + str(workout.get("sport_type") or workout.get("sport", ""))
        + str(workout.get("moving_time") or workout.get("duration_min", 0))
    )
    return hashlib.sha256(key.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def load_pushed_ids(pushed_path: Path) -> set:
    if not pushed_path.exists():
        return set()
    try:
        with open(pushed_path) as f:
            data = json.load(f)
        return {entry["id"] for entry in data.get("pushed", [])}
    except Exception:
        return set()


def save_pushed_log(pushed_path: Path, new_entries: list):
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
# Format normalisation
# ---------------------------------------------------------------------------

def normalise_entry(entry: dict) -> dict:
    """
    Normalise a workout entry from either push.py format ('workouts')
    or coach/sessions format ('sessions').

    Field mappings:
      sport        → sport_type
      duration_min → moving_time (×60)
      tss          → planned_tss
      type         → category (when category not present)
    """
    w = dict(entry)

    if "sport_type" not in w and "sport" in w:
        w["sport_type"] = w["sport"]

    if "moving_time" not in w and "duration_min" in w:
        w["moving_time"] = int(w["duration_min"]) * 60

    if "planned_tss" not in w and "tss" in w:
        w["planned_tss"] = w["tss"]

    if "category" not in w:
        w["category"] = w.get("type", "WORKOUT")

    # Build description from zones + targets when description is missing
    if not w.get("description"):
        parts = []
        zones = w.get("zones", "")
        dur = w.get("duration_min", 0)
        sport = w.get("sport_type", "")
        if zones and dur:
            z = zones.replace("–", "-")
            if sport == "Run":
                parts.append(f"- {dur}m {z} {z} HR {z} Pace")
            elif sport in ("Ride", "VirtualRide"):
                parts.append(f"- {dur}m {z} Power {z} HR")
            else:
                parts.append(f"- {dur}m {z}")

        targets = w.get("targets", {})
        notes = []
        if targets.get("focus"):
            notes.append(targets["focus"])
        if targets.get("hr_max_bpm"):
            notes.append(f"HR unter {targets['hr_max_bpm']} bpm.")
        if targets.get("power_min_w") and targets.get("power_max_w"):
            notes.append(f"Power {targets['power_min_w']}–{targets['power_max_w']} W.")
        if notes:
            parts.append(" ".join(notes))

        if w.get("description_note"):
            parts.append(w["description_note"])

        w["description"] = "\n\n".join(parts)

    return w


def should_skip(entry: dict) -> tuple:
    """Returns (skip: bool, reason: str)."""
    if entry.get("existing") is True:
        return True, "existing: true"

    entry_type = entry.get("type", "")
    if entry_type == "REST":
        return True, "REST day"

    sport = entry.get("sport_type") or entry.get("sport")
    if not sport:
        return True, "no sport"

    return False, ""


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def push_workout(athlete_id: str, auth: str, workout: dict,
                 dry_run: bool = False) -> dict:
    url = f"{INTERVALS_BASE_URL}/athlete/{athlete_id}/events"
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json",
    }

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

    moving_time = workout.get("moving_time")
    if moving_time:
        payload["moving_time"] = int(moving_time)

    planned_tss = workout.get("planned_tss")
    if planned_tss is not None and float(planned_tss) > 0:
        payload["load"] = float(planned_tss)

    if workout.get("distance"):
        payload["distance"] = float(workout["distance"])

    if dry_run:
        print(f"  [DRY RUN] Would push: {workout['date']} — {workout['name']}")
        return {"id": f"dry_{workout.get('id', 'unknown')}", **payload}

    response = requests.post(url, headers=headers, json=payload)

    if not response.ok:
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
    parser.add_argument("--pending", default="pending_workouts.json")
    parser.add_argument("--pushed-log", default="pushed_workouts.json")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-clear", action="store_true")
    args = parser.parse_args()

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

    # Accept both 'workouts' (push.py canonical) and 'sessions' (coach output)
    raw_entries = pending_data.get("workouts") or pending_data.get("sessions") or []
    source_key = "workouts" if "workouts" in pending_data else "sessions"

    if not raw_entries:
        print("✅  No entries found — nothing to push")
        return 0

    # Filter and normalise
    workouts = []
    skipped_pre = []
    for entry in raw_entries:
        skip, reason = should_skip(entry)
        if skip:
            skipped_pre.append((entry.get("name", "?"), reason))
            continue
        workouts.append(normalise_entry(entry))

    if skipped_pre:
        print(f"ℹ️   Pre-filtered {len(skipped_pre)} entr{'y' if len(skipped_pre) == 1 else 'ies'}:")
        for name, reason in skipped_pre:
            print(f"      {name} — {reason}")
        print()

    if not workouts:
        print("✅  No pushable workouts after filtering — nothing to push")
        return 0

    pushed_ids = load_pushed_ids(pushed_path)

    print(f"📋  {len(workouts)} workout(s) to push")
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
            result = push_workout(athlete_id, auth, workout, dry_run=args.dry_run)
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
        pending_data[source_key] = remaining
        pending_data["last_pushed"] = datetime.now().isoformat()
        with open(pending_path, "w") as f:
            json.dump(pending_data, f, indent=2, default=str)

        if not remaining:
            print(f"🧹  {pending_path} cleared (all workouts pushed)")
        else:
            print(f"⚠️   {len(remaining)} workout(s) remain in pending (failed pushes)")

    skipped_idem = len(workouts) - len(pushed_this_run) - len(remaining)
    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}"
          f"✅  Done — pushed: {len(pushed_this_run)}, "
          f"skipped (already pushed): {skipped_idem}, "
          f"failed: {len(remaining)}")

    return 0 if not remaining else 1


if __name__ == "__main__":
    exit(main())
