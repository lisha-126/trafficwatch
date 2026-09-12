from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

SEVERITY_THRESHOLD = 7.0
HDFS_ALERTS_PATH = "hdfs://localhost:9000/traffic_watch/output/streaming/alerts"
CHECKPOINT_PATH = "/tmp/trafficwatch_checkpoint/"

schema = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("severity_score", DoubleType()),
    StructField("delay", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("road_name", StringType()),
    StructField("region", StringType()),
    StructField("timestamp", StringType()),
])


def main():
    spark = SparkSession.builder \
        .appName("TrafficWatch_Alerts") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    parsed = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

    alerts = parsed.filter(col("severity_score") > SEVERITY_THRESHOLD)

    console_query = alerts.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    hdfs_query = alerts.writeStream \
        .format("parquet") \
        .option("path", HDFS_ALERTS_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    print(f"✓ Listening for critical incidents (severity > {SEVERITY_THRESHOLD}) on localhost:9999")
    print(f"✓ Alerts will be written to {HDFS_ALERTS_PATH}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main() 
