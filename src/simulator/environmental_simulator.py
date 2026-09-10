#!/usr/bin/env python3
"""
Environmental Telemetry Simulator for Smart Wildfire Intelligence System.
Generates synthetic environmental observations (temperature, humidity, wind, rainfall, smoke)
and produces them to Kafka topic 'environmental_data'.
NOTE: This source is explicitly marked as SYNTHETIC / SIMULATED TELEMETRY.
"""

import argparse
import datetime
import json
import random
import sys
import time
from kafka import KafkaProducer

def generate_environmental_event(lat_min, lat_max, lon_min, lon_max):
    """
    Generates a single synthetic environmental observation with realistic physical correlations.
    """
    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lat = round(random.uniform(lat_min, lat_max), 5)
    lon = round(random.uniform(lon_min, lon_max), 5)

    # Physical correlation: higher temp often correlates with lower humidity & higher smoke risk
    temperature = round(random.uniform(18.0, 43.5), 1)
    if temperature > 35.0:
        humidity = round(random.uniform(8.0, 30.0), 1)
        wind_speed = round(random.uniform(15.0, 55.0), 1)
        rainfall = 0.0 if random.random() > 0.05 else round(random.uniform(0.1, 1.2), 1)
        smoke = round(random.uniform(25.0, 180.0), 1)
        aqi = int(min(500, smoke * 2.2 + random.uniform(10, 40)))
    else:
        humidity = round(random.uniform(30.0, 85.0), 1)
        wind_speed = round(random.uniform(2.0, 25.0), 1)
        rainfall = round(random.uniform(0.0, 12.0), 1) if random.random() > 0.6 else 0.0
        smoke = round(random.uniform(2.0, 25.0), 1)
        aqi = int(min(150, smoke * 1.5 + random.uniform(5, 20)))

    wind_direction = round(random.uniform(0.0, 360.0), 1)

    event = {
        "timestamp": now_iso,
        "latitude": lat,
        "longitude": lon,
        "temperature": temperature,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "wind_direction": wind_direction,
        "rainfall": rainfall,
        "smoke": smoke,
        "air_quality_index": aqi,
        "source": "SIMULATED_ENVIRONMENTAL_TELEMETRY"
    }
    return event

def main():
    parser = argparse.ArgumentParser(description="Synthetic Environmental Telemetry Simulator")
    parser.add_argument("--bootstrap-server", default="localhost:9094", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="environmental_data", help="Kafka topic")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second")
    parser.add_argument("--max-events", type=int, default=100, help="Max events to generate (0 for unlimited)")
    parser.add_argument("--lat-min", type=float, default=32.0, help="Minimum latitude")
    parser.add_argument("--lat-max", type=float, default=42.0, help="Maximum latitude")
    parser.add_argument("--lon-min", type=float, default=-124.0, help="Minimum longitude")
    parser.add_argument("--lon-max", type=float, default=-114.0, help="Maximum longitude")
    args = parser.parse_args()

    print("=" * 65)
    print("  ENVIRONMENTAL TELEMETRY SIMULATOR")
    print("  [DISCLAIMER: Generates SYNTHETIC weather/smoke telemetry]")
    print(f"  Kafka Broker:     {args.bootstrap_server}")
    print(f"  Kafka Topic:      {args.topic}")
    print(f"  Target Rate:      {args.rate} events/sec")
    print(f"  Max Events:       {args.max_events}")
    print(f"  Spatial Bounds:   Lat [{args.lat_min}, {args.lat_max}], Lon [{args.lon_min}, {args.lon_max}]")
    print("=" * 65)

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=10000
        )
    except Exception as e:
        print(f"[ERROR] Could not connect to Kafka broker at {args.bootstrap_server}: {e}", file=sys.stderr)
        sys.exit(1)

    interval = 1.0 / args.rate if args.rate > 0 else 0.01
    events_sent = 0
    start_time = time.time()

    try:
        while True:
            event = generate_environmental_event(args.lat_min, args.lat_max, args.lon_min, args.lon_max)
            producer.send(args.topic, value=event)
            events_sent += 1

            if events_sent % 50 == 0 or events_sent == args.max_events:
                elapsed = time.time() - start_time
                current_rate = round(events_sent / elapsed, 1) if elapsed > 0 else 0
                print(f"[ENV-SIM] Sent {events_sent} events | Temp: {event['temperature']}°C | Hum: {event['humidity']}% | Smoke: {event['smoke']} | AQI: {event['air_quality_index']}")

            if args.max_events and events_sent >= args.max_events:
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[ENV-SIM] Stopped by user.")
    finally:
        producer.flush()
        producer.close()
        total_time = round(time.time() - start_time, 2)
        print("=" * 65)
        print(f"[ENV-SIM COMPLETED] Total Sent: {events_sent} events in {total_time}s")
        print("=" * 65)

if __name__ == "__main__":
    main()
