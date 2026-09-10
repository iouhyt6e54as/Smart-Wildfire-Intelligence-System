#!/usr/bin/env python3
"""
Initializes required Kafka topics for Lab 1:
  - firms_fire_events (active: fire simulator -> Spark Streaming -> HDFS)
  - environmental_data (active: environmental simulator -> Spark Streaming -> HDFS)
  - anomaly_alerts (prepared infrastructure topic for future Lab 3)
  - risk_predictions (prepared infrastructure topic for future Lab 2/3)
"""

import subprocess
import sys

TOPICS = [
    {"name": "firms_fire_events", "partitions": 2, "replication": 1},
    {"name": "environmental_data", "partitions": 2, "replication": 1},
    {"name": "anomaly_alerts", "partitions": 1, "replication": 1},
    {"name": "risk_predictions", "partitions": 1, "replication": 1},
]

def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return res.returncode, res.stdout.strip(), res.stderr.strip()

def main():
    print("=" * 60)
    print("  Initializing Kafka Topics for Smart Wildfire System")
    print("=" * 60)

    bootstrap_server = "localhost:9092"
    container_name = "kafka"

    for t in TOPICS:
        name = t["name"]
        parts = t["partitions"]
        repl = t["replication"]
        print(f">>> Creating topic '{name}' (partitions={parts}, replication={repl})...")
        cmd = (
            f"docker exec {container_name} /opt/kafka/bin/kafka-topics.sh "
            f"--create --if-not-exists "
            f"--topic {name} "
            f"--bootstrap-server {bootstrap_server} "
            f"--partitions {parts} "
            f"--replication-factor {repl}"
        )
        code, out, err = run_cmd(cmd)
        if code != 0:
            print(f"[ERROR] Failed to create topic {name}: {err or out}")
        else:
            print(f"[SUCCESS] Topic '{name}' ready: {out}")

    print("\n>>> Listing all Kafka topics on broker:")
    list_cmd = f"docker exec {container_name} /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server {bootstrap_server}"
    code, out, _ = run_cmd(list_cmd)
    print(out)
    print("=" * 60)

if __name__ == "__main__":
    main()
