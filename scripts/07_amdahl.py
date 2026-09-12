import os
import time
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "traffic_accidents.csv")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
P = 0.85


def amdahl_speedup(n, p=P):
    return 1.0 / ((1 - p) + p / n)


def run_job(cores):
    spark = SparkSession.builder \
        .appName(f"TrafficWatch_Benchmark_{cores}cores") \
        .master(f"local[{cores}]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    start = time.time()
    df = spark.read.csv(f"file://{os.path.abspath(DATA_PATH)}", header=True, inferSchema=True)
    grid = df.withColumn("lat_grid", spark_round(col("latitude"), 1)) \
             .withColumn("lon_grid", spark_round(col("longitude"), 1))
    result = grid.groupBy("lat_grid", "lon_grid") \
                 .agg(count("*").alias("incident_count"),
                      avg("severity_score").alias("avg_severity")) \
                 .orderBy(col("incident_count").desc())
    result.collect()
    elapsed = time.time() - start

    spark.stop()
    return elapsed


def main():
    print("=" * 60)
    print(" TrafficWatch - Amdahl's Law Scalability Benchmark")
    print("=" * 60)

    cores_list = [1, 2, 4]
    measured_times = []

    for c in cores_list:
        print(f"\nRunning batch job on {c} core(s)...")
        t = run_job(c)
        measured_times.append(t)
        print(f"  -> Elapsed: {t:.3f}s")

    baseline = measured_times[0]
    measured_speedup = [baseline / t for t in measured_times]
    theoretical_speedup = [amdahl_speedup(n) for n in cores_list]
    efficiency = [(s / n) * 100 for s, n in zip(measured_speedup, cores_list)]

    print("\nCores | Theoretical S(n) | Measured Speedup | Efficiency (%)")
    for n, th, m, e in zip(cores_list, theoretical_speedup, measured_speedup, efficiency):
        print(f"{n:5d} | {th:16.3f} | {m:17.3f} | {e:13.1f}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].bar([str(c) for c in cores_list], measured_speedup, color=["#4e79a7", "#f28e2b", "#59a14f"])
    axes[0].set_title("TrafficWatch — Measured Speedup")
    axes[0].set_xlabel("Number of Cores")
    axes[0].set_ylabel("Speedup Factor")
    for i, v in enumerate(measured_speedup):
        axes[0].text(i, v + 0.02, f"{v:.2f}", ha="center")

    n_range = np.linspace(1, 32, 200)
    axes[1].plot(n_range, [amdahl_speedup(n) for n in n_range], label=f"Theoretical (P={P})", color="blue")
    axes[1].plot(cores_list, measured_speedup, "ro-", label="Actual Measured")
    axes[1].set_title("Amdahl's Law: Theory vs Reality")
    axes[1].set_xlabel("Number of Cores")
    axes[1].set_ylabel("Speedup Factor")
    axes[1].legend()

    plt.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, "speedup_chart.png")
    plt.savefig(out_path, dpi=120)
    print(f"\n✓ Speedup chart saved to {out_path}")


if __name__ == "__main__":
    main() 
