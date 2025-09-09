import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract,
    concat_ws,
    to_timestamp,
    col,
    window
)

# Initialize Spark
def create_spark_session():
    return SparkSession.builder .appName("StreamingHDFSLogs_Task").getOrCreate()

def extract_timestamp(df):
    return df \
        .withColumn("log_date", regexp_extract("value", r'^(\d{6})', 1)) \
        .withColumn("log_time", regexp_extract("value", r'^\d{6}\s(\d{6})', 1)) \
        .withColumn("combined_dt", concat_ws(' ', col("log_date"), col("log_time"))) \
        .withColumn("timestamp", to_timestamp("combined_dt", "yyMMdd HHmmss"))

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Load environment variables
    hdfs_host = os.getenv("STREAMING_SERVER_HDFS")
    hdfs_port = int(os.getenv("STREAMING_SERVER_HDFS_PORT"))
    print(f"Using host: {hdfs_host}")
    print(f"Using port: {hdfs_port}")

    # Read stream from socket
    raw_stream = spark.readStream \
        .format("socket") \
        .option("host", hdfs_host) \
        .option("port", hdfs_port) \
        .load()

    # # Question 1
    # print("Question 1")
    # logs_with_ts = extract_timestamp(raw_stream).select("value", "timestamp")
    # q1_query = logs_with_ts.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
    # time.sleep(60)
    # q1_query.stop()

    # # Question 2
    # print("Question 2")
    # logs_with_ts = extract_timestamp(raw_stream).select("value", "timestamp")
    # logs_with_watermark = logs_with_ts.withWatermark("timestamp", "5 seconds")
    # q2_query = logs_with_watermark.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
    # time.sleep(60)
    # q2_query.stop()

    # # Question 4
    # print("Question 4")
    # datanode_logs = extract_timestamp(raw_stream) \
    #     .withColumn("component", regexp_extract("value", r'\s([\w\.$]+):', 1)) \
    #     .withWatermark("timestamp", "5 seconds") \
    #     .filter(col("component").contains("DataNode"))
    # datanode_counts = datanode_logs \
    #     .groupBy(window("timestamp", "60 seconds", "30 seconds")) \
    #     .count() \
    #     .withColumnRenamed("count", "datanode_count")
    # q4_query = datanode_counts.writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
    # time.sleep(60)
    # q4_query.stop()

    # # Question 5
    # print("Question 5")
    # log_with_fields = raw_stream \
    #     .withColumn("source", regexp_extract("value", r'(/[\d\.]+:\d+)', 1)) \
    #     .withColumn("block_size", regexp_extract("value", r'size\s+(\d+)', 1).cast("long")) \
    #     .withColumn("hostname", regexp_extract("source", r'/([\d\.]+)', 1)) \
    #     .select("hostname", "block_size")
    # host_block_agg = log_with_fields \
    #     .groupBy("hostname") \
    #     .sum("block_size") \
    #     .withColumnRenamed("sum(block_size)", "total_bytes") \
    #     .orderBy(col("total_bytes").desc())
    # q5_query = host_block_agg.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
    # time.sleep(300)
    # q5_query.stop()

    # Question 6
    print("Question 6")
    log_info = raw_stream \
        .withColumn("log_level", regexp_extract("value", r'\d+\s+\d+\s+\d+\s+(\w+)', 1)) \
        .withColumn("block_id", regexp_extract("value", r'(blk_-?\d+)', 1)) \
        .withColumn("source", regexp_extract("value", r'(/[\d\.]+:\d+)', 1)) \
        .withColumn("hostname", regexp_extract("source", r'/([\d\.]+)', 1)) \
        .filter((col("log_level") == "INFO") & (col("block_id") != "")) \
        .select("hostname")
    host_info_counts = log_info \
        .groupBy("hostname") \
        .count() \
        .withColumnRenamed("count", "entry_count") \
        .orderBy(col("entry_count").desc())
    q6_query = host_info_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="15 seconds") \
        .start()
    time.sleep(300)
    q6_query.stop()

    # Stop Spark session
    spark.stop()
