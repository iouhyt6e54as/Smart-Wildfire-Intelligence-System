#!/usr/bin/env bash
# Smart Wildfire Intelligence System — Status & UI URLs (Bash)
echo "============================================================"
echo " Smart Wildfire Intelligence System — Service Status"
echo "============================================================"

docker compose ps

echo ""
echo "------------------------------------------------------------"
echo " Service Web Interfaces"
echo "------------------------------------------------------------"
echo "  HDFS NameNode UI   : http://localhost:9870"
echo "  HDFS DataNode UI   : http://localhost:9864"
echo "  Spark Master UI    : http://localhost:8080"
echo "  Spark Worker UI    : http://localhost:8081"
echo "  Kafka UI           : http://localhost:8090"
echo "  PostgreSQL Port    : localhost:5432 (databases: airflow, wildfire)"
echo "  Airflow UI (opt)   : http://localhost:8082 (admin / admin)"
echo "  Jupyter Lab (opt)  : http://localhost:8888 (token: lab)"
echo "============================================================"
