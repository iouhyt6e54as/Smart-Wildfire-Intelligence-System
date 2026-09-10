#!/usr/bin/env python3
"""
Automated Infrastructure Smoke Test Suite for Smart Wildfire Intelligence System
Validates:
  1. Docker container status & health
  2. Kafka topic creation, produce & consume
  3. HDFS directory structure, write, read & cleanup
  4. Spark cluster computation (1+2+3+4+5=15) & HDFS access
  5. PostgreSQL connectivity (both 'airflow' and 'wildfire' databases)
  6. Airflow health endpoint (if orchestration profile is active)
  7. End-to-End pipeline: synthetic event -> Kafka -> Spark -> HDFS
"""

import json
import subprocess
import sys
import time
import urllib.request

RESULTS = {}

def run_cmd(cmd, check=False):
    """Executes a command and returns (returncode, stdout, stderr)."""
    if isinstance(cmd, list):
        res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    else:
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if check and res.returncode != 0:
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}\nStderr: {res.stderr}\nStdout: {res.stdout}")
    return res.returncode, res.stdout.strip(), res.stderr.strip()

def print_banner(title):
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)

def test_docker():
    print_banner("TEST 1: Docker Containers & Health Checks")
    code, stdout, stderr = run_cmd(['docker', 'ps', '--format', '{{.Names}}|{{.Status}}'])
    if code != 0:
        print(f"[FAIL] Could not query Docker containers: {stderr}")
        RESULTS["Docker"] = "FAIL"
        return False

    running = {}
    for line in stdout.splitlines():
        line = line.strip().strip("'\"")
        if "|" in line:
            name, status = line.split("|", 1)
            running[name.strip()] = status.strip()

    print(f"Detected {len(running)} running container(s):")
    for name, status in running.items():
        print(f"  - {name:<20}: {status}")

    required_core = ["kafka", "namenode", "datanode", "spark-master", "spark-worker", "airflow-postgres"]
    missing = [c for c in required_core if c not in running]

    if missing:
        print(f"[FAIL] Missing required core containers: {missing}")
        RESULTS["Docker"] = "FAIL"
        return False

    unhealthy = [name for name, status in running.items() if "unhealthy" in status.lower()]
    if unhealthy:
        print(f"[FAIL] Unhealthy containers detected: {unhealthy}")
        RESULTS["Docker"] = "FAIL"
        return False

    print("[PASS] All core infrastructure containers are running and healthy.")
    RESULTS["Docker"] = "PASS"
    return True

def test_kafka():
    print_banner("TEST 2: Kafka Broker & Topic Messaging")
    topic = "fire-events"
    test_msg = json.dumps({"test": True, "message": "wildfire infrastructure test", "ts": time.time()})

    # 1. Create topic if not exists
    print(f">>> Ensuring topic '{topic}' exists...")
    create_cmd = [
        "docker", "exec", "kafka",
        "/opt/kafka/bin/kafka-topics.sh", "--create", "--if-not-exists",
        "--topic", topic,
        "--bootstrap-server", "localhost:9092",
        "--partitions", "1",
        "--replication-factor", "1"
    ]
    code, out, err = run_cmd(create_cmd)
    if code != 0:
        print(f"[FAIL] Failed to create Kafka topic '{topic}': {err or out}")
        RESULTS["Kafka"] = "FAIL"
        return False

    # 2. Produce test message
    print(f">>> Producing test message to '{topic}': {test_msg}")
    produce_cmd = ["docker", "exec", "-i", "kafka", "/opt/kafka/bin/kafka-console-producer.sh", "--topic", topic, "--bootstrap-server", "localhost:9092"]
    p = subprocess.Popen(produce_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        out, err = p.communicate(input=f"{test_msg}\n", timeout=30)
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
    
    if p.returncode != 0:
        print(f"[FAIL] Failed to produce message to Kafka: {err or out}")
        RESULTS["Kafka"] = "FAIL"
        return False

    # 3. Consume message
    print(f">>> Consuming message from '{topic}'...")
    consume_cmd = [
        "docker", "exec", "kafka",
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--topic", topic,
        "--bootstrap-server", "localhost:9092",
        "--from-beginning",
        "--max-messages", "1",
        "--timeout-ms", "15000"
    ]
    code, out, err = run_cmd(consume_cmd)
    if code != 0 or "wildfire infrastructure test" not in out:
        print(f"[FAIL] Failed to consume test message from Kafka: {err or out}")
        RESULTS["Kafka"] = "FAIL"
        return False

    print(f"[PASS] Kafka topic '{topic}' produced and consumed message successfully.")
    RESULTS["Kafka"] = "PASS"
    return True

def test_hdfs():
    print_banner("TEST 3: HDFS Directory Structure & File I/O")
    
    dirs = [
        "/wildfire/raw",
        "/wildfire/curated",
        "/wildfire/features",
        "/wildfire/models",
        "/wildfire/predictions",
        "/wildfire/test"
    ]
    print(">>> Creating / verifying required HDFS directories...")
    mkdir_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p"] + dirs
    code, out, err = run_cmd(mkdir_cmd)
    if code != 0:
        print(f"[FAIL] Failed to create HDFS directories: {err or out}")
        RESULTS["HDFS"] = "FAIL"
        return False

    # 2. Test write, read, and delete
    test_content = f"wildfire_hdfs_smoke_test_{int(time.time())}"
    print(f">>> Writing test file to HDFS with content: '{test_content}'...")
    put_cmd = ["docker", "exec", "-i", "namenode", "bash", "-c", f"echo '{test_content}' | hdfs dfs -put -f - /wildfire/test/smoke.txt"]
    code, out, err = run_cmd(put_cmd)
    if code != 0:
        print(f"[FAIL] Failed to write test file to HDFS: {err or out}")
        RESULTS["HDFS"] = "FAIL"
        return False

    print(">>> Reading back test file from HDFS...")
    cat_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", "/wildfire/test/smoke.txt"]
    code, out, err = run_cmd(cat_cmd)
    if code != 0 or test_content not in out:
        print(f"[FAIL] Read content mismatch. Expected '{test_content}', got '{out}': {err}")
        RESULTS["HDFS"] = "FAIL"
        return False

    # 3. Clean up test file
    print(">>> Cleaning up temporary HDFS test file...")
    del_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-rm", "-f", "/wildfire/test/smoke.txt"]
    run_cmd(del_cmd)

    # 4. List directories to confirm structure
    list_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/wildfire"]
    _, out, _ = run_cmd(list_cmd)
    print("Verified HDFS /wildfire structure:\n" + out)

    print("[PASS] HDFS read, write, directory structure, and cleanup verified.")
    RESULTS["HDFS"] = "PASS"
    return True

def test_spark():
    print_banner("TEST 4: Spark Cluster Execution & HDFS Access")
    print(">>> Submitting test job to Spark Cluster (spark://spark-master:7077)...")
    submit_cmd = [
        "docker", "exec", "spark-master",
        "/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "/opt/spark-apps/test_spark_cluster.py"
    ]
    code, out, err = run_cmd(submit_cmd)
    combined = out + "\n" + err

    if "Result of sum: 15" not in combined or "SUCCESS: Spark cluster compute and HDFS I/O verified" not in combined:
        print(f"[FAIL] Spark test failed.\nOutput:\n{combined}")
        RESULTS["Spark"] = "FAIL"
        return False

    # Cleanup spark test output in HDFS
    run_cmd(["docker", "exec", "namenode", "hdfs", "dfs", "-rm", "-r", "-f", "/wildfire/test/spark_verify.txt"])

    print("[PASS] Spark cluster successfully computed sum(1..5) = 15 and verified HDFS I/O.")
    RESULTS["Spark"] = "PASS"
    return True

def test_postgres():
    print_banner("TEST 5: PostgreSQL Database Connectivity")
    
    # 1. Test Airflow database
    print(">>> Testing connectivity to database 'airflow'...")
    af_cmd = ["docker", "exec", "airflow-postgres", "psql", "-U", "airflow", "-d", "airflow", "-t", "-c", "SELECT 1;"]
    code, out, err = run_cmd(af_cmd)
    if code != 0 or "1" not in out:
        print(f"[FAIL] Failed to connect to 'airflow' database: {err or out}")
        RESULTS["PostgreSQL"] = "FAIL"
        return False

    # 2. Test Wildfire application database
    print(">>> Testing connectivity to database 'wildfire'...")
    wf_cmd = ["docker", "exec", "airflow-postgres", "psql", "-U", "wildfire", "-d", "wildfire", "-t", "-c", "SELECT 1;"]
    code, out, err = run_cmd(wf_cmd)

    if code != 0 or "1" not in out:
        print(f"[FAIL] Failed to connect to 'wildfire' database: {err or out}")
        RESULTS["PostgreSQL"] = "FAIL"
        return False

    print("[PASS] PostgreSQL verified successfully for both 'airflow' and 'wildfire' databases.")
    RESULTS["PostgreSQL"] = "PASS"
    return True

def test_airflow():
    print_banner("TEST 6: Airflow Health Check (Profile: orchestration)")
    code, stdout, _ = run_cmd(["docker", "ps", "--filter", "name=airflow-webserver", "--format", "{{.Names}}"])
    if not stdout or "airflow-webserver" not in stdout:
        print("[INFO] Airflow webserver container is not running.")
        print("       (Orchestration profile is optional during Lab 1 infrastructure work)")
        RESULTS["Airflow"] = "SKIPPED (Optional profile not active)"
        return True

    print(">>> Querying Airflow health endpoint http://localhost:8082/health...")
    try:
        req = urllib.request.Request("http://localhost:8082/health")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            metadb_status = data.get("metadatabase", {}).get("status")
            print(f">>> Airflow metadatabase status: {metadb_status}")
            if metadb_status == "healthy":
                print("[PASS] Airflow health endpoint responded healthy.")
                RESULTS["Airflow"] = "PASS"
                return True
            else:
                print(f"[FAIL] Airflow reported status: {data}")
                RESULTS["Airflow"] = "FAIL"
                return False
    except Exception as e:
        print(f"[FAIL] Could not connect to Airflow health endpoint: {e}")
        RESULTS["Airflow"] = "FAIL"
        return False

def test_e2e_pipeline():
    print_banner("TEST 7: End-to-End Pipeline (Kafka -> Spark -> HDFS)")
    topic = "fire-events"
    synthetic_event = json.dumps({
        "event_id": "test-fire-001",
        "latitude": 37.7749,
        "longitude": -122.4194,
        "confidence": "nominal",
        "brightness": 325.4,
        "frp": 12.8,
        "is_mock": True
    })

    print(f">>> Producing synthetic fire event to Kafka '{topic}'...")
    produce_cmd = ["docker", "exec", "-i", "kafka", "/opt/kafka/bin/kafka-console-producer.sh", "--topic", topic, "--bootstrap-server", "localhost:9092"]
    p = subprocess.Popen(produce_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        p.communicate(input=f"{synthetic_event}\n", timeout=30)
    except subprocess.TimeoutExpired:
        p.kill()
        p.communicate()

    print(">>> Submitting Spark batch reading job to ingest Kafka event to HDFS...")
    submit_cmd = [
        "docker", "exec", "spark-master",
        "/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "/opt/spark-apps/test_e2e_pipeline.py"
    ]
    code, out, err = run_cmd(submit_cmd)
    combined = out + "\n" + err

    if "SUCCESS: End-to-End pipeline (Kafka -> Spark -> HDFS) passed!" not in combined:
        print(f"[FAIL] End-to-end pipeline execution failed.\nOutput:\n{combined}")
        RESULTS["Kafka -> Spark -> HDFS"] = "FAIL"
        return False

    # Verify HDFS output
    cat_cmd = ["docker", "exec", "namenode", "bash", "-c", "hdfs dfs -cat /wildfire/test/e2e_output/*"]
    code, out, err = run_cmd(cat_cmd)
    if code != 0 or "test-fire-001" not in out:
        print(f"[FAIL] Could not find synthetic event in HDFS output: {out}")
        RESULTS["Kafka -> Spark -> HDFS"] = "FAIL"
        return False

    print(">>> Cleaning up temporary HDFS test output directory...")
    run_cmd(["docker", "exec", "namenode", "hdfs", "dfs", "-rm", "-r", "-f", "/wildfire/test/e2e_output"])

    print("[PASS] End-to-End infrastructure pipeline verified successfully!")
    RESULTS["Kafka -> Spark -> HDFS"] = "PASS"
    return True

def print_summary():
    print_banner("INFRASTRUCTURE SMOKE TEST SUMMARY")
    all_passed = True
    for test_name, status in RESULTS.items():
        pass_symbol = "PASS" in status
        if not pass_symbol and "SKIPPED" not in status:
            all_passed = False
        print(f"  {test_name:<25}: {status}")

    print("-" * 65)
    core_ready = all(RESULTS.get(t) == "PASS" for t in ["Docker", "Kafka", "HDFS", "Spark", "PostgreSQL", "Kafka -> Spark -> HDFS"])
    print(f"  Core Infrastructure Ready : {'YES' if core_ready else 'NO'}")
    print(f"  Ready for Lab 1           : {'YES' if core_ready else 'NO'}")
    print("=" * 65)
    return core_ready

def main():
    start_time = time.time()
    t1 = test_docker()
    if not t1:
        print_summary()
        sys.exit(1)

    test_kafka()
    test_hdfs()
    test_spark()
    test_postgres()
    test_airflow()
    test_e2e_pipeline()

    success = print_summary()
    print(f"Total test execution time: {time.time() - start_time:.2f}s")
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
