#!/usr/bin/env python3
"""
Fire Event Simulator for Smart Wildfire Intelligence System.
Replays real historical NASA FIRMS CSV records into Kafka topic 'firms_fire_events'.
Does NOT fabricate fire observations.
"""

import argparse
import csv
import json
import os
import sys
import time
from kafka import KafkaProducer

def normalize_time_str(val):
    if not val:
        return "00:00"
    v = str(val).strip()
    padded = v.zfill(4)
    return f"{padded[:2]}:{padded[2:]}"

def parse_record(row, is_nrt=False):
    """
    Parses a CSV row into canonical NASA FIRMS event dictionary.
    """
    try:
        lat = float(row["latitude"])
        lon = float(row["longitude"])
    except (ValueError, KeyError):
        return None

    brightness = float(row["brightness"]) if row.get("brightness") else None
    scan = float(row["scan"]) if row.get("scan") else None
    track = float(row["track"]) if row.get("track") else None
    acq_date = row.get("acq_date", "").strip()
    acq_time = normalize_time_str(row.get("acq_time", "00:00"))
    satellite = row.get("satellite", "").strip() or None
    instrument = row.get("instrument", "").strip() or None
    confidence = row.get("confidence", "").strip() or None
    version = row.get("version", "").strip() or None
    bright_t31 = float(row["bright_t31"]) if row.get("bright_t31") else None
    frp = float(row["frp"]) if row.get("frp") else None
    daynight = row.get("daynight", "").strip() or None

    # Handle 'type' for archive vs NRT
    fire_type = None
    if not is_nrt and row.get("type") is not None and row["type"] != "":
        try:
            fire_type = int(float(row["type"]))
        except ValueError:
            fire_type = None

    event = {
        "latitude": lat,
        "longitude": lon,
        "brightness": brightness,
        "scan": scan,
        "track": track,
        "acq_date": acq_date,
        "acq_time": acq_time,
        "satellite": satellite,
        "instrument": instrument,
        "confidence": confidence,
        "version": version,
        "bright_t31": bright_t31,
        "frp": frp,
        "daynight": daynight,
        "type": fire_type
    }
    return event

def main():
    parser = argparse.ArgumentParser(description="NASA FIRMS Fire Event Simulator")
    parser.add_argument("--input", default="nasa-wildfire-data/2026/fire_archive_SV-C2_800029.csv", help="Input NASA CSV file")
    parser.add_argument("--bootstrap-server", default="localhost:9094", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="firms_fire_events", help="Kafka topic")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second")
    parser.add_argument("--max-events", type=int, default=100, help="Max events to produce (0 for unlimited)")
    parser.add_argument("--loop", action="store_true", help="Loop indefinitely over the file")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"[ERROR] Input CSV not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    print("=" * 60)
    print("  FIRE EVENT SIMULATOR (NASA FIRMS Replay)")
    print(f"  Input File:       {args.input}")
    print(f"  Kafka Broker:     {args.bootstrap_server}")
    print(f"  Kafka Topic:      {args.topic}")
    print(f"  Target Rate:      {args.rate} events/sec")
    print(f"  Max Events:       {args.max_events}")
    print("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=10000
        )
    except Exception as e:
        print(f"[ERROR] Could not connect to Kafka broker at {args.bootstrap_server}: {e}", file=sys.stderr)
        sys.exit(1)

    is_nrt = "nrt" in os.path.basename(args.input).lower()
    interval = 1.0 / args.rate if args.rate > 0 else 0.01

    events_sent = 0
    start_time = time.time()

    try:
        while True:
            with open(args.input, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    event = parse_record(row, is_nrt=is_nrt)
                    if not event:
                        continue

                    producer.send(args.topic, value=event)
                    events_sent += 1

                    if events_sent % 50 == 0 or events_sent == args.max_events:
                        elapsed = time.time() - start_time
                        current_rate = round(events_sent / elapsed, 1) if elapsed > 0 else 0
                        print(f"[SIMULATOR] Sent {events_sent} events | Rate: {current_rate} evt/s | Latest Lat/Lon: ({event['latitude']:.4f}, {event['longitude']:.4f})")

                    if args.max_events and events_sent >= args.max_events:
                        break

                    time.sleep(interval)

            if not args.loop or (args.max_events and events_sent >= args.max_events):
                break

    except KeyboardInterrupt:
        print("\n[SIMULATOR] Stopped by user.")
    finally:
        producer.flush()
        producer.close()
        total_time = round(time.time() - start_time, 2)
        print("=" * 60)
        print(f"[SIMULATOR COMPLETED] Total Sent: {events_sent} events in {total_time}s")
        print("=" * 60)

if __name__ == "__main__":
    main()
