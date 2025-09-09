import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, unix_timestamp, from_unixtime
from pyspark.sql.functions import round as round_col, concat, lit, hour, when, count, avg

if __name__ == "__main__":
    
    # Initialize Spark
    spark = SparkSession.builder.appName("Twitter_Data_Analysis").getOrCreate()

    # Load environment variables
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    # Configure Hadoop for S3 access
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Question 1
    print("Question 1")
    input_path = "s3a://data-repository-bkt/ECS765/Twitter/twitter.csv"
    tweets_df = spark.read.option("header", "true").csv(input_path)
    entry_count = tweets_df.count()
    print(f"Total number of entries in the Twitter Dataset: {entry_count}")

    # Question 2
    print("Question 2")
    tweets_df = tweets_df.withColumn("timestamp_parsed", to_timestamp(col("timestamp").cast("string"), "yyyyMMddHHmmss"))
    tweets_df = tweets_df.withColumn("timezone", col("timezone").cast("int")).filter(col("timezone").isNotNull())
    tweets_df = tweets_df.withColumn("utc_time", from_unixtime(unix_timestamp("timestamp_parsed") - col("timezone") * 3600))
    tweets_df = tweets_df.withColumn("tweet_date", date_format(col("utc_time"), "yyyy-MM-dd"))
    tweets_df = tweets_df.withColumn("weekday", date_format(col("utc_time"), "EEEE"))
    weekday_df = tweets_df.filter(~col("weekday").isin(["Saturday", "Sunday"])).orderBy("tweet_date")
    print("Displaying tweets from weekdays sorted by date")
    weekday_df.select("longitude", "latitude", "timestamp", "timezone", "utc_time", "tweet_date", "weekday").show(10, truncate=False)
    # Reset tweets_df for following tasks to include all days again
    tweets_df = tweets_df.withColumn("weekday", date_format(col("utc_time"), "EEEE"))

    # Question 3
    print("Question 3")
    geo_df = tweets_df.withColumn("longitude_rounded", round_col(col("longitude"), 1))\
                       .withColumn("latitude_rounded", round_col(col("latitude"), 1))\
                       .withColumn("location", concat(lit("("), col("longitude_rounded"), lit(", "), col("latitude_rounded"), lit(")")))\
                       .withColumn("latitude_rounded", round_col(col("latitude"), 1))\
                       .withColumn("location", concat(lit("("), col("longitude_rounded"), lit(", "), col("latitude_rounded"), lit(")")))
    # Save results for visualization
    location_counts = geo_df.groupBy("longitude_rounded", "latitude_rounded")\
    .count()\
    .orderBy("count", ascending=False)
    print("Number of tweets per location")
    location_counts.show(10, truncate=False) 
    location_counts.coalesce(1)\
    .write.option("header", True).mode("overwrite")\
    .csv("s3a://object-bucket-ec24848-8bb6712d-c99d-4727-a9d6-d67ec41345d8/task1_Q3/")

    # Question 4
    print("Question 4")
    geo_df = geo_df.withColumn("hour_of_day", hour(col("utc_time")))\
                   .withColumn("weekday", date_format(col("utc_time"), "EEEE"))
    geo_df = geo_df.withColumn("time_period",
        when((col("hour_of_day") >= 5) & (col("hour_of_day") <= 11), "Morning")
       .when((col("hour_of_day") >= 12) & (col("hour_of_day") <= 16), "Afternoon")
       .when((col("hour_of_day") >= 17) & (col("hour_of_day") <= 21), "Evening")
       .otherwise("Night"))
    print("Showing 10 rows with hour, weekday, and time_period classification")
    geo_df.select("hour_of_day", "weekday", "time_period").show(10, truncate=False)
    time_period_counts = geo_df.groupBy("time_period").count()
    # Show results
    time_period_counts.show()
    # Save results for visualization
    time_period_counts.coalesce(1).write.option("header", True).mode("overwrite")\
        .csv("s3a://object-bucket-ec24848-8bb6712d-c99d-4727-a9d6-d67ec41345d8/task1_Q4/")
    
    # Question 5
    print("Question 5")
    ordered_days = when(col("weekday") == "Monday", 1)\
                   .when(col("weekday") == "Tuesday", 2)\
                   .when(col("weekday") == "Wednesday", 3)\
                   .when(col("weekday") == "Thursday", 4)\
                   .when(col("weekday") == "Friday", 5)\
                   .when(col("weekday") == "Saturday", 6)\
                   .when(col("weekday") == "Sunday", 7)
    daily_counts = geo_df.groupBy("weekday").agg(count("*").alias("total_tweets"))\
                          .withColumn("tweets_k", (col("total_tweets") / 1000).cast("int"))\
                          .withColumn("day_index", ordered_days)\
                          .orderBy("day_index")
    print("Rows of tweet counts per day of the week")
    daily_counts.show(10, truncate=False)
    # Save results for visualization
    daily_counts.coalesce(1).write.option("header", True).mode("overwrite")\
                .csv("s3a://object-bucket-ec24848-8bb6712d-c99d-4727-a9d6-d67ec41345d8/task1_Q5/")

    # Question 6
    print("Question 6")
    mean_count = daily_counts.agg(avg("total_tweets").alias("avg_tweets")).collect()[0]["avg_tweets"]
    above_avg_days = daily_counts.filter(col("total_tweets") > mean_count)
    print(f"Average Tweets per Weekday: {int(mean_count)}")
    print("Days with above-average tweet volumes:")
    above_avg_days.show()

    # Question 7
    print("Question 7")
    top_locations = geo_df.groupBy("location").count().withColumnRenamed("count", "num_tweets")\
                          .orderBy("num_tweets", ascending=False)
    print("Top 10 most active tweet locations")
    top_locations.show(10, truncate=False)

    
    spark.stop()
