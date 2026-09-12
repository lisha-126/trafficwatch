import os
import io
import subprocess
import pandas as pd
from flask import Flask, jsonify, render_template

app = Flask(__name__)

BASE_DIR = os.path.dirname(__file__)
LOCAL_CSV = os.path.join(BASE_DIR, "data", "traffic_accidents.csv")


def hdfs_available():
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", "/traffic_watch/raw/traffic_accidents.csv"],
            timeout=5, capture_output=True,
        )
        return result.returncode == 0
    except Exception:
        return False


def load_dataframe():
    mode = "local"
    if hdfs_available():
        try:
            result = subprocess.run(
                ["hdfs", "dfs", "-cat", "/traffic_watch/raw/traffic_accidents.csv"],
                timeout=10, capture_output=True,
            )
            if result.returncode == 0 and result.stdout:
                df = pd.read_csv(io.BytesIO(result.stdout))
                mode = "hdfs"
                return df, mode
        except Exception:
            pass
    df = pd.read_csv(LOCAL_CSV)
    return df, mode


def classify(df):
    bins = [-1, 3, 5, 7, 100]
    labels = ["Minor Fender Bender", "Moderate Collision", "Severe Accident", "Major Multi-Vehicle Pileup"]
    df["traffic_class"] = pd.cut(df["severity_score"], bins=bins, labels=labels)
    return df


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    online = hdfs_available()
    return jsonify({"hdfs_online": online, "label": "HDFS Cluster Synced" if online else "Dynamic Local Fallback"})


@app.route("/api/incidents")
def api_incidents():
    df, mode = load_dataframe()
    df = classify(df)
    records = df.head(1000).to_dict(orient="records")
    return jsonify({"mode": mode, "count": len(df), "incidents": records})


@app.route("/api/class-distribution")
def api_class_distribution():
    df, mode = load_dataframe()
    df = classify(df)
    stats = df.groupby("traffic_class", observed=True).agg(
        incident_count=("id", "count"),
        avg_severity=("severity_score", "mean"),
    ).reset_index()
    return jsonify({"mode": mode, "data": stats.to_dict(orient="records")})


@app.route("/api/corridors")
def api_corridors():
    df, mode = load_dataframe()
    corridors = df.groupby("road_name").agg(
        incident_count=("id", "count"),
        avg_severity=("severity_score", "mean"),
    ).reset_index().sort_values("avg_severity", ascending=False).head(10)
    return jsonify({"mode": mode, "data": corridors.to_dict(orient="records")})


@app.route("/api/blackspots")
def api_blackspots():
    df, mode = load_dataframe()
    df["lat_grid"] = df["latitude"].round(1)
    df["lon_grid"] = df["longitude"].round(1)
    grid = df.groupby(["lat_grid", "lon_grid"]).agg(
        incident_count=("id", "count"),
        avg_severity=("severity_score", "mean"),
        max_severity=("severity_score", "max"),
    ).reset_index()

    def danger_level(n):
        if n > 50:
            return "CRITICAL BLACKSPOT"
        if n > 20:
            return "HIGH DANGER"
        if n > 10:
            return "ELEVATED RISK"
        return "MODERATE RISK ZONE"

    grid["danger_level"] = grid["incident_count"].apply(danger_level)
    grid = grid.sort_values("incident_count", ascending=False).head(15)
    return jsonify({"mode": mode, "data": grid.to_dict(orient="records")})


@app.route("/api/region-stats")
def api_region_stats():
    df, mode = load_dataframe()
    stats = df.groupby("region").agg(
        total_crashes=("id", "count"),
        avg_severity_score=("severity_score", "mean"),
        max_severity_score=("severity_score", "max"),
    ).reset_index()
    return jsonify({"mode": mode, "data": stats.to_dict(orient="records")})


@app.route("/api/speedup")
def api_speedup():
    P = 0.85
    cores = [1, 2, 4]
    theoretical = [1.0 / ((1 - P) + P / n) for n in cores]
    return jsonify({"cores": cores, "theoretical": theoretical})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
