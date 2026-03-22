#!/usr/bin/env python3
"""
pending_workouts.json → Intervals.icu Calendar
Idempotent: dedup via content hash, skip already-pushed IDs.

Supports:
- Simple workouts (description-only)
- Structured workouts with workout_doc (Intervals.icu builder format)

Usage:
  python push.py --athlete-id i123456 --intervals-key YOUR_KEY
  python push.py --dry-run   # simulate without writing

Version 1.0
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
# Workout-Doc builder helpers
# ---------------------------------------------------------------------------

def _resolve_power(target: dict, ftp: float) -> dict:
    """
    Resolve a power target to watts.
    Supports:
      {"type": "power", "value": 250}          → absolute watts
      {"type": "ftp_pct", "value": 0.85}       → 85% FTP
      {"type": "zone", "zone": 3, "ftp": 200}  → zone midpoint
    Returns {"value": <watts>, "units": "W"} for Intervals.icu _power field.
    """
    t = target.get("type", "power")
    val = target.get("value")

    if t == "ftp_pct":
        if ftp and val:
            return {"value": round(ftp * val), "units": "W"}
    elif t == "zone":
        # 7-zone midpoints as % of FTP
        zone_midpoints = {1: 0.50, 2: 0.65, 3: 0.83, 4: 0.98,
                          5: 1.13, 6: 1.35, 7: 1.60}
        z = target.get("zone", 3)
        mid = zone_midpoints.get(z, 0.83)
        if ftp:
            return {"value": round(ftp * mid), "units": "W"}
    # Default: absolute watts
    if val is not None:
        return {"value": val, "units": "W"}
    return {}


def build_workout_doc(steps_input: list, ftp: float = None) -> dict:
    """
    Convert simplified step definitions into Intervals.icu workout_doc format.

    Simplified step format:
      {
        "type": "steady",          # steady | ramp | repeat | free
        "duration": 600,           # seconds
        "power": {"type": "ftp_pct", "value": 0.60},
        "cadence": 90,             # optional
        "reps": 5,                 # for repeat blocks only
        "steps": [...]             # nested steps for repeat blocks
      }

    Returns a workout_doc dict ready for the Intervals.icu API.
    """
    def _build_step(s: dict) -> dict:
        step_type = s.get("type", "steady")

        # Repeat block
        if step_type == "repeat":
            nested = [_build_step(ns) for ns in s.get("steps", [])]
            return {
                "type": "Repeat",
                "reps": s.get("reps", 1),
                "steps": nested,
            }

        # Steady or ramp step
        step = {
            "type": "SteadyState" if step_type in ("steady", "free") else "Ramp",
            "duration": s.get("duration", 60),
        }

        power_target = s.get("power")
        if power_target and ftp is not None:
            step["_power"] = _resolve_power(power_target, ftp)
        elif power_target and power_target.get("value"):
            step["_power"] = {"value": power_target["value"], "units": "W"}

        if s.get("cadence"):
            step["cadence"] = s["cadence"]

        if step_type == "ramp":
            power_end = s.get("power_end")
            if power_end and ftp is not None:
                step["_powerEnd"] = _resolve_power(power_end, ftp)

        return step

    return {
        "type": "bike",
        "steps": [_build_step(s) for s in steps_input],
    }


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def push_workout(athlete_id: str, auth: str, workout: dict,
                 ftp: float = None, dry_run: bool = False) -> dict:
    """
    POST a single workout to Intervals.icu /events endpoint.
    Supports both description-only and structured workout_doc workouts.
    Returns the created event dict on success.
    """
    url = f"{INTERVALS_BASE_URL}/athlete/{athlete_id}/events"
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json",
    }

    # Base payload
    payload = {
        "start_date_local": workout["date"] if "T" in str(workout["date"]) else f"{workout['date']}T00:00:00",
        "name": workout.get("name", "Workout"),
        "type": SPORT_TYPE_MAP.get(workout.get("sport_type", "Ride"), "Ride"),
        "category": CATEGORY_MAP.get(workout.get("category", "WORKOUT"), "WORKOUT"),
        "description": workout.get("description", ""),
    }

    # Optional scalar fields
    if workout.get("moving_time"):
        payload["moving_time"] = int(workout["moving_time"])
    if workout.get("planned_tss") is not None:
        payload["load"] = float(workout["planned_tss"])
    # Note: POST /events uses "load", not "icu_training_load" (that is the GET response field name)
    if workout.get("distance"):
        payload["distance"] = float(workout["distance"])  # meters

    # Structured workout_doc — two paths:
    # 1. Pre-built workout_doc already in the JSON (full Intervals format)
    # 2. Simplified "steps" array that we build into a workout_doc here
    if workout.get("workout_doc"):
        payload["workout_doc"] = workout["workout_doc"]
    elif workout.get("steps"):
        effective_ftp = ftp or workout.get("ftp")
        payload["workout_doc"] = build_workout_doc(workout["steps"], ftp=effective_ftp)

    if dry_run:
        print(f"  [DRY RUN] Would push: {workout['date']} — {workout['name']}")
        if payload.get("workout_doc"):
            n_steps = len(payload["workout_doc"].get("steps", []))
            print(f"            workout_doc: {n_steps} top-level step(s)")
        return {"id": f"dry_{workout.get('id', 'unknown')}", **payload}

    response = requests.post(url, headers=headers, json=payload)
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
    parser.add_argument("--ftp", type=float, default=None,
                        help="Override FTP for power target resolution (watts)")
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

    # Nothing to do
    if not pending_path.exists():
        print("✅  No pending_workouts.json found — nothing to push")
        return 0

    with open(pending_path) as f:
        pending_data = json.load(f)

    workouts = pending_data.get("workouts", [])
    if not workouts:
        print("✅  pending_workouts.json is empty — nothing to push")
        return 0

    # FTP for power resolution: CLI flag > pending file metadata > None
    effective_ftp = args.ftp or pending_data.get("ftp") or None

    # Load already-pushed IDs
    pushed_ids = load_pushed_ids(pushed_path)

    print(f"📋  {len(workouts)} workout(s) in pending_workouts.json")
    if pushed_ids:
        print(f"    Already pushed (will skip): {len(pushed_ids)} known ID(s)")
    if args.dry_run:
        print("🧪  DRY RUN — no writes to Intervals.icu")
    if effective_ftp:
        print(f"⚡  FTP for power resolution: {effective_ftp} W")
    print()

    pushed_this_run = []
    remaining = []

    for workout in workouts:
        # Ensure deterministic ID
        if not workout.get("id"):
            workout["id"] = make_hash(workout)

        wid = workout["id"]

        # Idempotency check
        if wid in pushed_ids:
            print(f"  ⏭️   Skip (already pushed): {workout['date']} — {workout['name']} [{wid}]")
            continue

        try:
            result = push_workout(
                athlete_id, auth, workout,
                ftp=effective_ftp,
                dry_run=args.dry_run
            )
            intervals_id = result.get("id", "unknown")
            has_doc = "workout_doc" in result or workout.get("workout_doc") or workout.get("steps")
            doc_marker = " [structured]" if has_doc else ""
            print(f"  ✅  Pushed{doc_marker}: {workout['date']} — {workout['name']}"
                  f" → Intervals ID: {intervals_id} [{wid}]")

            pushed_this_run.append({
                "id": wid,
                "intervals_event_id": intervals_id,
                "date": workout["date"],
                "name": workout["name"],
                "sport_type": workout.get("sport_type"),
                "pushed_at": datetime.now().isoformat(),
                "source": workout.get("source", "unknown"),
                "has_workout_doc": bool(has_doc),
                "dry_run": args.dry_run,
            })

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            print(f"  ❌  HTTP {status}: {workout['date']} — {workout['name']}")
            if e.response is not None:
                # Print full response body for diagnosis
                try:
                    full_body = e.response.json()
                    print(f"      API response: {json.dumps(full_body, indent=6)}")
                except Exception:
                    print(f"      API response (raw): {e.response.text[:500]}")
                # Also print the payload we sent (without auth)
                print(f"      Payload sent: {json.dumps({k: v for k, v in locals().get('payload', {}).items()}, indent=6)}")
            remaining.append(workout)

        except Exception as e:
            print(f"  ❌  Unexpected error: {workout['date']} — {workout['name']}: {e}")
            remaining.append(workout)

    print()

    # Update audit log
    if pushed_this_run:
        save_pushed_log(pushed_path, pushed_this_run)
        print(f"📝  Audit log: {len(pushed_this_run)} new entries → {pushed_path}")

    # Clear pending file unless --no-clear
    if not args.no_clear:
        pending_data["workouts"] = remaining
        pending_data["last_pushed"] = datetime.now().isoformat()
        with open(pending_path, "w") as f:
            json.dump(pending_data, f, indent=2, default=str)

        if not remaining:
            print(f"🧹  pending_workouts.json cleared (all workouts pushed)")
        else:
            print(f"⚠️   {len(remaining)} workout(s) remain in pending (failed pushes)")

    # Exit summary
    skipped = len(workouts) - len(pushed_this_run) - len(remaining)
    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}"
          f"✅  Done — pushed: {len(pushed_this_run)}, "
          f"skipped (already pushed): {skipped}, "
          f"failed: {len(remaining)}")

    return 0 if not remaining else 1


if __name__ == "__main__":
    exit(main())
