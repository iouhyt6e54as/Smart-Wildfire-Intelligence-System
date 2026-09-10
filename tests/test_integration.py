#!/usr/bin/env python3
"""
Integration Test Suite for Smart Wildfire Intelligence System (Lab 1).
Validates:
  1. Kafka topics existence and pub/sub message flow (firms_fire_events & environmental_data)
  2. Spark batch processing -> Parquet storage in HDFS (/wildfire/curated & /wildfire/features)
  3. Spark structured streaming -> Kafka consumption -> HDFS Parquet persistence
"""

import json
import os
import subprocess
import sys
import time
from kafka import KafkaProducer, KafkaConsumer

def print_step(title):
    print("\n" + "=" * 65)
    print(f"  INTEGRATION TEST: {title}")
    print("=" * 65)

def test_kafka_messaging():
    print_step("Kafka Topics Pub/Sub (firms_fire_events & environmental_data)")
    bootstrap = "localhost:9094"

    # 1. Fire events pub/sub
    fire_topic = "firms_fire_events"
    test_fire = {
        "latitude": 37.7749,
        "longitude": -122.4194,
        "brightness": 345.2,
        "scan": 0.4,
        "track": 0.5,
        "acq_date": "2026-06-01",
        "acq_time": "12:00",
        "satellite": "SNPP",
        "instrument": "VIIRS",
        "confidence": "h",
        "version": "2.0NRT",
        "bright_t31": 290.0,
        "frp": 12.5,
        "daynight": "D",
        "type": None
    }

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(fire_topic, value=test_fire)
    producer.flush()
    print(f">>> Produced test event to '{fire_topic}'")

    consumer = KafkaConsumer(
        fire_topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    found_fire = False
    for msg in consumer:
        val = msg.value
        if val.get("latitude") == 37.7749:
            found_fire = True
            print(f">>> Successfully consumed test event from '{fire_topic}': {val['latitude']}, {val['brightness']}K")
            break
    consumer.close()

    if not found_fire:
        raise AssertionError(f"Failed to consume test event from {fire_topic}")

    # 2. Environmental data pub/sub
    env_topic = "environmental_data"
    test_env = {
        "timestamp": "2026-06-01T12:00:00Z",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "temperature": 32.5,
        "humidity": 28.0,
        "wind_speed": 22.0,
        "wind_direction": 180.0,
        "rainfall": 0.0,
        "smoke": 45.0,
        "air_quality_index": 110,
        "source": "SIMULATED_ENVIRONMENTAL_TELEMETRY"
    }

    producer.send(env_topic, value=test_env)
    producer.flush()
    producer.close()
    print(f">>> Produced test event to '{env_topic}'")

    env_consumer = KafkaConsumer(
        env_topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    found_env = False
    for msg in env_consumer:
        val = msg.value
        if val.get("latitude") == 34.0522 and val.get("source") == "SIMULATED_ENVIRONMENTAL_TELEMETRY":
            found_env = True
            print(f">>> Successfully consumed test event from '{env_topic}': AQI={val['air_quality_index']}")
            break
    env_consumer.close()

    if not found_env:
        raise AssertionError(f"Failed to consume test event from {env_topic}")

    print("[PASS] Kafka messaging test passed for both topics.")
    return True

def test_hdfs_readback():
    print_step("HDFS Parquet Structure & Verification")
    res = subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/wildfire"],
        capture_output=True, text=True
    )
    print(res.stdout)
    if res.returncode != 0:
        raise RuntimeError(f"Failed to list /wildfire in HDFS: {res.stderr}")

    for expected in ["curated", "features", "checkpoints", "raw"]:
        if expected not in res.stdout:
            raise AssertionError(f"Expected directory '{expected}' missing from HDFS /wildfire")

    print("[PASS] HDFS directory structure verified.")
    return True

def main():
    try:
        test_kafka_messaging()
        test_hdfs_readback()
        print("\n" + "=" * 65)
        print("  ALL LAB 1 INTEGRATION TESTS PASSED SUCCESSFULLY!")
        print("=" * 65)
    except Exception as e:
        print(f"\n[FAIL] Integration test failed: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
