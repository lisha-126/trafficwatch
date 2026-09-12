from pyspark.sql import SparkSession
from pyspark.sql.functions import round as spark_round, col, count, avg, max as spark_max, when

HDFS_INPUT = "hdfs:///traffic_watch/raw/traffic_accidents.csv"
HDFS_OUTPUT = "hdfs:///traffic_watch/output/hotspots"


def main():
    spark = SparkSession.builder.appName("TrafficWatch_Hotspots").getOrCreate()

    print("=" * 70)
    print(" TrafficWatch - Accident Blackspot & Dangerous Corridor Detection")
    print("=" * 70)

    df = spark.read.csv(HDFS_INPUT, header=True, inferSchema=True)
    print(f"\nLoaded {df.count():,} traffic incident records")

    print("\nIdentifying Global Accident Blackspots (grid: 0.1° ≈ 10km)...")
    grid_df = df.withColumn("lat_grid", spark_round(col("latitude"), 1)) \
                .withColumn("lon_grid", spark_round(col("longitude"), 1))

    hotspots = grid_df.groupBy("lat_grid", "lon_grid") \
        .agg(count("*").alias("incident_count"),
             avg("severity_score").alias("avg_severity"),
             spark_max("severity_score").alias("max_severity")) \
        .withColumn(
            "danger_level",
            when(col("incident_count") > 50, "CRITICAL BLACKSPOT")
            .when(col("incident_count") > 20, "HIGH DANGER")
            .when(col("incident_count") > 10, "ELEVATED RISK")
            .otherwise("MODERATE RISK ZONE"),
        ) \
        .orderBy(col("incident_count").desc())

    hotspots.show(15, truncate=False)
    hotspots.write.mode("overwrite").parquet(HDFS_OUTPUT)

    pk_count = df.filter(col("region") == "Pakistan").select("latitude", "longitude").distinct().count()
    print("\nIdentifying Pakistan Motorway Accident Blackspots...")
    print(f"Found {pk_count} distinct grid cells in Pakistan")

    print("\n✓ Blackspot detection complete. Results written to HDFS under /traffic_watch/output/hotspots")
    spark.stop()


if __name__ == "__main__":
    main()
