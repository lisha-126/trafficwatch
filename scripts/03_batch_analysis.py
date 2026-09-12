from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, when

HDFS_INPUT = "hdfs:///traffic_watch/raw/traffic_accidents.csv"
HDFS_CORRIDORS_OUT = "hdfs:///traffic_watch/output/batch/corridors"
HDFS_CLASS_OUT = "hdfs:///traffic_watch/output/batch/traffic_class"
HDFS_REGION_OUT = "hdfs:///traffic_watch/output/batch/region_stats"


def main():
    spark = SparkSession.builder \
        .appName("TrafficWatch_BatchAnalysis") \
        .getOrCreate()

    print("=" * 70)
    print(" TrafficWatch - Batch Analysis Pipeline")
    print("=" * 70)

    df = spark.read.csv(HDFS_INPUT, header=True, inferSchema=True)
    total = df.count()
    print(f"\nTotal traffic incident records loaded: {total:,}")

    df = df.withColumn(
        "traffic_class",
        when(col("severity_score") <= 3, "Minor Fender Bender")
        .when(col("severity_score") <= 5, "Moderate Collision")
        .when(col("severity_score") <= 7, "Severe Accident")
        .otherwise("Major Multi-Vehicle Pileup"),
    )

    print("\nTraffic Class Distribution:")
    class_stats = df.groupBy("traffic_class") \
        .agg(count("*").alias("incident_count"),
             avg("severity_score").alias("avg_severity"),
             spark_max("severity_score").alias("max_severity")) \
        .orderBy(col("avg_severity"))
    class_stats.show(truncate=False)
    class_stats.write.mode("overwrite").parquet(HDFS_CLASS_OUT)

    print("\nRegional Crash Statistics:")
    region_stats = df.groupBy("region") \
        .agg(count("*").alias("total_crashes"),
             avg("severity_score").alias("avg_severity_score"),
             spark_max("severity_score").alias("max_severity_score")) \
        .orderBy(col("total_crashes").desc())
    region_stats.show(truncate=False)
    region_stats.write.mode("overwrite").parquet(HDFS_REGION_OUT)

    print("\nTop-10 Dangerous Corridors:")
    corridors = df.groupBy("road_name") \
        .agg(count("*").alias("incident_count"),
             avg("severity_score").alias("avg_severity")) \
        .orderBy(col("avg_severity").desc()) \
        .limit(10)
    corridors.show(truncate=False)
    corridors.write.mode("overwrite").parquet(HDFS_CORRIDORS_OUT)

    print("\n✓ Batch analysis complete. Results written to HDFS under /traffic_watch/output/batch/")
    spark.stop()


if __name__ == "__main__":
    main()
