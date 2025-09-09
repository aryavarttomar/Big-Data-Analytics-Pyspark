import sys, string
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from graphframes import GraphFrame
from pyspark.sql.functions import col, hour, to_timestamp, sum as spark_sum, avg
from pyspark.sql.functions import col, lit, when, array, size


if __name__ == "__main__":

    # Initialize Spark
    spark = SparkSession.builder.appName("Chicago_Taxi_Trips_Analysis").getOrCreate()

    # Load environment variables
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
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
    data_source_path = "s3a://data-repository-bkt/ECS765/Chicago_Taxitrips/chicago_taxi_trips.csv"
    taxiTripsDF = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(data_source_path)
    total_rows = taxiTripsDF.count()
    print(f"Total number of entries in the DataFrame: {total_rows}")
    taxiTripsDF.show(5, truncate=False)

    # Question 2
    print("Question 2")
    # Define schema for nodes (vertices)
    nodeSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("census_tract", StringType(), True)])
    # Define schema for links (edges)
    linkSchema = StructType([
        StructField("src", IntegerType(), True),
        StructField("dst", IntegerType(), True),
        StructField("trip_miles", DoubleType(), True),
        StructField("trip_seconds", IntegerType(), True),
        StructField("fare", DoubleType(), True)])
    # Extract pickup location info
    pickup_nodes = taxiTripsDF \
        .withColumn("id", col("Pickup Community Area").cast("int")) \
        .withColumn("latitude", col("Pickup Centroid Latitude").cast("double")) \
        .withColumn("longitude", col("Pickup Centroid Longitude").cast("double")) \
        .withColumn("census_tract", col("Pickup Census Tract")) \
        .select("id", "latitude", "longitude", "census_tract")
    # Extract dropoff location info
    dropoff_nodes = taxiTripsDF \
        .withColumn("id", col("Dropoff Community Area").cast("int")) \
        .withColumn("latitude", col("Dropoff Centroid Latitude").cast("double")) \
        .withColumn("longitude", col("Dropoff Centroid Longitude").cast("double")) \
        .withColumn("census_tract", col("Dropoff Census Tract")) \
        .select("id", "latitude", "longitude", "census_tract")
    # Combine pickup and dropoff into a unified node list
    all_nodes = pickup_nodes.unionByName(dropoff_nodes).dropDuplicates(["id"])
    nodesDF = spark.createDataFrame(all_nodes.rdd, schema=nodeSchema)
    # Create links (edges) between pickup and dropoff areas
    links_raw = taxiTripsDF \
        .withColumn("src", col("Pickup Community Area").cast("int")) \
        .withColumn("dst", col("Dropoff Community Area").cast("int")) \
        .withColumn("trip_miles", col("Trip Miles").cast("double")) \
        .withColumn("trip_seconds", col("Trip Seconds").cast("int")) \
        .withColumn("fare", col("Fare").cast("double")) \
        .select("src", "dst", "trip_miles", "trip_seconds", "fare") \
        .dropna(subset=["src", "dst"])
    linksDF = spark.createDataFrame(links_raw.rdd, schema=linkSchema)
    # Show sample output
    print("Sample of vertices :")
    nodesDF.show(5, truncate=False)
    print("Sample of edges :")
    linksDF.show(5, truncate=False)

    # Question 3
    print("Question 3")
    tripGraph = GraphFrame(nodesDF, linksDF)
    # Join source (src) node metadata
    src_enriched = tripGraph.edges.join(
        tripGraph.vertices,
        tripGraph.edges.src == tripGraph.vertices.id,
        how="left"
    ).withColumnRenamed("latitude", "src_latitude") \
     .withColumnRenamed("longitude", "src_longitude") \
     .withColumnRenamed("census_tract", "src_census_tract") \
     .drop("id")
    # Join destination (dst) node metadata
    full_enriched = src_enriched.join(
        tripGraph.vertices,
        src_enriched.dst == tripGraph.vertices.id,
        how="left"
    ).withColumnRenamed("latitude", "dst_latitude") \
     .withColumnRenamed("longitude", "dst_longitude") \
     .withColumnRenamed("census_tract", "dst_census_tract") \
     .drop("id")
    # Select and show final sample columns
    resultDF = full_enriched.select(
        "src", "dst", "trip_miles", "trip_seconds", "fare",
        "src_latitude", "src_longitude", "src_census_tract",
        "dst_latitude", "dst_longitude", "dst_census_tract"
    ).dropDuplicates(["src", "dst"])
    print("Sample enriched graph edges:")
    resultDF.show(10, truncate=False)

    # Question 4
    print("Question 4")
    internalTripsDF = tripGraph.edges.filter(col("src") == col("dst"))
    # Count of such trips
    same_area_trip_count = internalTripsDF.count()
    print(f"Total number of trips within the same community area: {same_area_trip_count}")
    # Show 10 sample trips
    print("Sample internal trips (same-area pickups and dropoffs):")
    internalTripsDF.show(10, truncate=False)

    #Question 5
    print("Question 5")
    shortest_paths_df = tripGraph.bfs(
    fromExpr="id != 49",
    toExpr="id = 49",
    maxPathLength=10)
    edge_columns = [c for c in shortest_paths_df.columns if c.startswith("e")]
    distance_expr = size(array([when(col(c).isNotNull(), 1).otherwise(0) for c in edge_columns]))
    result_df = shortest_paths_df.select(
    col("from.id").alias("id"),
    lit(49).alias("landmark"),
    distance_expr.alias("distance")
    ).distinct()
    print("Shortest paths to community area 49:")
    result_df.orderBy("distance").show(10, truncate=False)
    
    # Question 6
    print("Question 6")
    # === Unweighted PageRank ===
    # Prepare unique vertex IDs as strings
    allAreasDF = nodesDF.select("id").distinct().withColumn("id", col("id").cast("string"))
    # Prepare basic edges with string IDs
    simpleLinksDF = linksDF \
        .select(
            col("src").cast("string").alias("src"),
            col("dst").cast("string").alias("dst"))
    # Create GraphFrame for unweighted PageRank
    unweightedGraph = GraphFrame(allAreasDF, simpleLinksDF)
    # Run PageRank without weights
    unweightedPR = unweightedGraph.pageRank(resetProbability=0.15, tol=0.01)
    # Top 5 community areas by PageRank
    topUnweightedPR = unweightedPR.vertices.select("id", "pagerank") \
        .orderBy(col("pagerank").desc()) \
        .limit(5)
    print("Top 5 community areas (Unweighted PageRank):")
    topUnweightedPR.show(truncate=False)
    # Save result to S3
    unweighted_output_path = f"s3a://{s3_bucket}/task3_Q6_unweighted"
    topUnweightedPR.coalesce(1).write.mode("overwrite").option("header", True).csv(unweighted_output_path)
    # === Weighted PageRank (using Fare as weight) ===
    # Aggregate weights (total fare per edge)
    weightedEdgesDF = linksDF \
        .groupBy("src", "dst") \
        .agg(spark_sum("fare").alias("weight")) \
        .withColumn("src", col("src").cast("string")) \
        .withColumn("dst", col("dst").cast("string"))
    # Build weighted GraphFrame
    weightedGraph = GraphFrame(allAreasDF, weightedEdgesDF)
    # Run weighted PageRank
    weightedPR = weightedGraph.pageRank(resetProbability=0.15, maxIter=10)
    # Top 5 by weighted PageRank
    topWeightedPR = weightedPR.vertices.select("id", "pagerank") \
        .orderBy(col("pagerank").desc()) \
        .limit(5)
    print("Top 5 community areas (Weighted PageRank using Fare):")
    topWeightedPR.show(truncate=False)
    # Save result to S3
    weighted_output_path = f"s3a://{s3_bucket}/task3_Q6_weighted"
    topWeightedPR.coalesce(1).write.mode("overwrite").option("header", True).csv(weighted_output_path)

    # Question 7
    print("Question 7")
    # Prepare DataFrame with required columns and proper types
    fareMetricsDF = taxiTripsDF \
        .withColumn("fare", col("Fare").cast("double")) \
        .withColumn("trip_miles", col("Trip Miles").cast("double")) \
        .withColumn("trip_seconds", col("Trip Seconds").cast("int")) \
        .withColumn("HourOfDay", hour(to_timestamp("Trip Start Timestamp", "MM/dd/yyyy hh:mm:ss a"))) \
        .select("fare", "trip_miles", "trip_seconds", "HourOfDay") \
        .dropna()
    # Show sample records
    print("Sample fare-related trip metrics:")
    fareMetricsDF.show(10, truncate=False)
    # Summary statistics
    print("Summary statistics of fare, trip miles, and duration:")
    fareMetricsDF.describe(["fare", "trip_miles", "trip_seconds"]).show()
    # Group 1: Average fare by Trip Miles
    print("Average fare grouped by Trip Miles:")
    fareMetricsDF.groupBy("trip_miles") \
        .agg(avg("fare").alias("Avg Fare")) \
        .orderBy("trip_miles") \
        .show()
    # Group 2: Average fare by Trip Seconds
    print("Average fare grouped by Trip Seconds:")
    fareMetricsDF.groupBy("trip_seconds") \
        .agg(avg("fare").alias("Avg Fare")) \
        .orderBy("trip_seconds") \
        .show()
    # Group 3: Average fare by Hour of Day
    print("Average fare grouped by Start Hour:")
    fareMetricsDF.groupBy("HourOfDay") \
        .agg(avg("fare").alias("Avg Fare")) \
        .orderBy("HourOfDay") \
        .show()
    # Save the DataFrame to S3
    fare_output_path = f"s3a://{s3_bucket}/task3_Q7"
    fareMetricsDF.coalesce(1).write.csv(fare_output_path, header=True, mode="overwrite")




