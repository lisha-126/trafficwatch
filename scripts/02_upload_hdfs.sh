#!/bin/bash
echo "========================================"
echo " TrafficWatch - HDFS Upload"
echo "========================================"

echo ""
echo "[1/4] Checking if Hadoop daemons are alive..."
if ! jps | grep -q "NameNode"; then
    echo "Error: Hadoop NameNode is not running."
    echo "Run: start-dfs.sh && start-yarn.sh"
    exit 1
fi
echo "  ✓ NameNode is running"

if ! jps | grep -q "DataNode"; then
    echo "Error: Hadoop DataNode is not running."
    exit 1
fi
echo "  ✓ DataNode is running"

if jps | grep -q "ResourceManager"; then
    echo "  ✓ ResourceManager (YARN) is running"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOCAL_CSV="$PROJECT_DIR/data/traffic_accidents.csv"

echo ""
echo "[2/4] Checking local CSV file..."
if [ ! -f "$LOCAL_CSV" ]; then
    echo "Error: $LOCAL_CSV not found. Run 01_download_data.py first."
    exit 1
fi
echo "  ✓ Found: data/traffic_accidents.csv"

echo ""
echo "[3/4] Creating HDFS directory structure..."
hdfs dfs -mkdir -p /traffic_watch/raw
hdfs dfs -mkdir -p /traffic_watch/output/batch
hdfs dfs -mkdir -p /traffic_watch/output/hotspots
hdfs dfs -mkdir -p /traffic_watch/output/streaming
echo "  ✓ HDFS directories ready"

echo ""
echo "[4/4] Uploading to HDFS (overwriting previous copy)..."
hdfs dfs -put -f "$LOCAL_CSV" /traffic_watch/raw/

echo ""
echo "Verifying upload..."
hdfs dfs -ls /traffic_watch/raw/

echo ""
echo "✓ TrafficWatch HDFS upload complete!"
echo "  Raw data path: /traffic_watch/raw/traffic_accidents.csv"
