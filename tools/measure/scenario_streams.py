"""Turn a recorded edge-adas scenario (an RSU and a car, see edge-adas/offline-data/scenarios.yaml) into camera
streams for the load generator (loadgen.py --scenario).

Each recorded device becomes one stream: its frames in recording order, each with the GPS position, heading and
speed the device logged with it, built the way edge-adas/offline-data/send_offline_to_queue.py builds them. The
device's offset (frames skipped at the start) is applied, and both streams are cut to the same length, so frame i
of the RSU and frame i of the car show the same moment of the scenario.

Usage:
  python tools/measure/scenario_streams.py --offline-data ~/thesis/edge-adas/offline-data --scenario 15-41 \
      --out ~/thesis/scenarios/15-41.json
  python tools/measure/scenario_streams.py --offline-data ~/thesis/edge-adas/offline-data --list
"""
import argparse
import json
import os
from pathlib import Path

import yaml

# Defaults of send_offline_to_queue.py, used when the scenario does not set them
DEFAULT_HEADING_OFFSET_DEG = 115
DEFAULT_SPEED = 60
ROLES = ("rsu", "car")


def safe_float(value):
    """The value as a float, or None when it is missing or NaN"""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def read_session(device_dir, session):
    """The records of a session log whose frame file exists, in recording order"""
    frames_dir = device_dir / "frames"
    records = []
    with open(device_dir / "logs" / session, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line.replace("NaN", "null"))
            ts = record.get("frame_ts_ns")
            image = frames_dir / f"frame_{ts}.jpg"
            if ts is not None and image.exists():
                record["image"] = str(image)
                records.append(record)
    return records


def frame_metadata(record, heading_offset):
    """The position, heading and speed of one frame, as send_offline_to_queue.py puts them in the message"""
    heading = safe_float(record.get("heading_true_deg"))
    return {
        "position": {"lat": safe_float(record.get("lat")), "lon": safe_float(record.get("lon")),
                     "alt": safe_float(record.get("alt")) or 0.0},
        "heading": (heading + heading_offset) % 360 if heading is not None else 0,
        "speed": safe_float(record.get("speed_mps")) or DEFAULT_SPEED,
    }


def device_stream(offline_data, role, config):
    """One stream of a scenario: the device's frames after its offset, with their metadata"""
    env = config.get("env", {})
    heading_offset = float(env.get("HEADING_OFFSET_DEG", DEFAULT_HEADING_OFFSET_DEG))
    records = read_session(offline_data / config["device_dir"], config["session"])[int(config.get("offset", 0)):]
    frames = [{"image": r["image"], "metadata": frame_metadata(r, heading_offset)} for r in records
              if safe_float(r.get("lat")) is not None and safe_float(r.get("lon")) is not None]
    return {"role": role, "vehicle_id": env.get("VEHICLE_ID", role), "frames": frames}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--offline-data", required=True, help="edge-adas/offline-data (scenarios.yaml and the recordings)")
    p.add_argument("--scenario", help="scenario id, e.g. 15-41")
    p.add_argument("--out", help="output JSON for loadgen.py --scenario")
    p.add_argument("--list", action="store_true", help="list the scenarios and their frame counts")
    args = p.parse_args()

    offline_data = Path(os.path.expanduser(args.offline_data)).resolve()  # absolute image paths in the output
    scenarios = yaml.safe_load(open(offline_data / "scenarios.yaml"))["scenarios"]
    if args.list:
        for sc in scenarios:
            counts = [len(device_stream(offline_data, role, sc[role])["frames"]) for role in ROLES]
            print(f"{sc['id']}  {sc.get('name', '')}  frames rsu/car: {counts[0]}/{counts[1]}")
        return

    sc = next((s for s in scenarios if s["id"] == args.scenario), None)
    if sc is None or not args.out:
        raise SystemExit("give --scenario (one of: " + ", ".join(s["id"] for s in scenarios) + ") and --out")
    streams = [device_stream(offline_data, role, sc[role]) for role in ROLES]
    length = min(len(s["frames"]) for s in streams)
    if length == 0:
        raise SystemExit(f"scenario {args.scenario}: a device has no usable frame")
    for s in streams:
        s["frames"] = s["frames"][:length]
    out = Path(os.path.expanduser(args.out))
    out.parent.mkdir(parents=True, exist_ok=True)
    json.dump({"scenario": sc["id"], "name": sc.get("name", ""), "streams": streams}, open(out, "w"))
    print(f"{out}: scenario {sc['id']}, {len(streams)} streams ("
          + ", ".join(f"{s['role']} {s['vehicle_id']}" for s in streams) + f"), {length} frames each")


if __name__ == "__main__":
    main()
