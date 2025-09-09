import sys
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, length, when, dayofweek, to_csv, round, year, split, explode, avg, desc
from pyspark.sql.types import FloatType, IntegerType
    
if __name__ == "__main__":
    
    # Initialize Spark
    spark = SparkSession.builder.appName("movie_ratings_data").getOrCreate()

    # Load environment variables
    s3_data_repository_bucket = os.environ.get('DATA_REPOSITORY_BUCKET', '')
    s3_endpoint_url = os.environ.get('S3_ENDPOINT_URL', '') + ':' + os.environ.get('BUCKET_PORT', '')
    s3_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID', '')
    s3_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
    s3_bucket = os.environ.get('BUCKET_NAME', '')

    # Configure Hadoop for S3 access
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
       
    # Question 1
    print("Question 1")
    # Define paths
    ratings_path = "s3a://" + s3_data_repository_bucket + "/ECS765/MovieLens/ratings.csv"
    movies_path = "s3a://" + s3_data_repository_bucket + "/ECS765/MovieLens/movies.csv"
    # Load datasets 
    ratings_df = spark.read.option("header", True).option("inferSchema", True).csv(ratings_path)
    movies_df = spark.read.option("header", True).option("inferSchema", True).csv(movies_path)
    # Show sample data
    print("\nSample ratings data:")
    ratings_df.select("userId", "movieId", "rating", "timestamp").show(5, truncate=False)
    # Count unique users 
    unique_users = ratings_df.select("userId").distinct().count()
    print(f"\nNumber of unique users: {unique_users}")

    # Question 2
    print("Question 2")
    # Convert the Unix timestamp into date format (YYYY-MM-DD)
    ratings_with_date = ratings_df.withColumn("rating_date", to_date(from_unixtime(col("timestamp"))))
    # Sort the DataFrame by the date column
    sorted_ratings = ratings_with_date.orderBy("rating_date")
    # Display the sorted results
    print("\nRatings Sorted by Date:")
    sorted_ratings.select("userId", "movieId", "rating", "timestamp", "rating_date").show(10, truncate=False)

    # Question 3
    print("Question 3")
    # Add a new column for rating category
    ratings_categorized = ratings_df.withColumn(
    "rating_group",
    when((col("rating") >= 0.5) & (col("rating") <= 2.5), "Low")
    .when((col("rating") >= 3.0) & (col("rating") <= 4.5), "Medium")
    .when(col("rating") > 4.5, "High")
    .otherwise("Unknown"))
    # Group by category and count
    category_counts = ratings_categorized.groupBy("rating_group").agg(
        count("*").alias("rating_count")).orderBy("rating_count", ascending=False)
    print("\nRating Distribution by Category:")
    category_counts.show(truncate=False)
    # Save to S3 
    output_rating_path = f"s3a://{s3_bucket}/task2_Q3"
    category_counts.coalesce(1).write.mode("overwrite").csv(output_rating_path, header=True)
    print(f"\nRating distribution saved to: {output_rating_path}")

    # Question 4
    print("Question 4")
    # Convert timestamp and extract year and month
    ratings_time = ratings_df.withColumn("timestamp_dt", to_timestamp(from_unixtime(col("timestamp")))) \
                         .withColumn("year", year(col("timestamp_dt"))) \
                         .withColumn("month", month(col("timestamp_dt")))
    # Categorize as Early Year or Late Year
    ratings_time = ratings_time.withColumn(
    "time_of_year",
    when((col("month") >= 1) & (col("month") <= 6), "Early Year")
    .otherwise("Late Year"))
    # Show 10 rows
    print("\nSample Rows with Year, Month, Time of Year:")
    ratings_time.select("userId", "movieId", "rating", "timestamp", "year", "month", "time_of_year").show(10, truncate=False)
    # Count ratings in each half of the year
    time_grouped = ratings_time.groupBy("time_of_year").agg(count("*").alias("rating_count")).orderBy("time_of_year")
    print("\nNumber of Ratings by Time of Year:")
    time_grouped.show()
    # Save to S3
    output_time_path = f"s3a://{s3_bucket}/task2_Q4"
    time_grouped.coalesce(1).write.mode("overwrite").csv(output_time_path, header=True)
    print(f"\nTime of year distribution saved to: {output_time_path}")

    # Question 5
    print("Question 5")
    # Join ratings with movies
    ratings_with_genres = ratings_df.join(movies_df, on="movieId", how="inner")
    # Split genres by '|', then explode the list into rows
    ratings_exploded = ratings_with_genres.withColumn("genre", explode(split(col("genres"), "\|")))
    # Filter only valid genres
    valid_genres = [
    "Action", "Adventure", "Animation", "Children", "Comedy",
    "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir",
    "Horror", "Musical", "Mystery", "Romance", "Sci-Fi",
    "Thriller", "War", "Western"]
    filtered_genres = ratings_exploded.filter(col("genre").isin(valid_genres))
    # Count number of ratings per genre
    genre_counts = filtered_genres.groupBy("genre").agg(count("*").alias("rating_count")).orderBy("rating_count", ascending=False)
    print("\nTop Genres by Rating Count:")
    genre_counts.show(10, truncate=False)
    # Save to S3
    output_genre_path = f"s3a://{s3_bucket}/task2_Q5"
    genre_counts.coalesce(1).write.mode("overwrite").csv(output_genre_path, header=True)
    print(f"\nGenre rating distribution saved to: {output_genre_path}")

    # Question 6
    print("Question 6")
    # Extract year from timestamp
    ratings_by_year = ratings_df.withColumn("year", year(from_unixtime(col("timestamp"))))
    # Group by year and count
    yearly_counts = ratings_by_year.groupBy("year").agg(
    count("*").alias("rating_count")).orderBy("year")
    # Add percentage column
    total_ratings = ratings_df.count()
    yearly_counts = yearly_counts.withColumn("percentage", (col("rating_count") / total_ratings) * 100)
    # Show result
    print("\nRatings per Year:")
    yearly_counts.show(10, truncate=False)
    # Save to S3
    output_yearly_path = f"s3a://{s3_bucket}/task2_Q6"
    yearly_counts.coalesce(1).write.mode("overwrite").csv(output_yearly_path, header=True)
    print(f"\nYearly rating summary saved to: {output_yearly_path}")

    # Question 7
    print("Question 7")
    # Group by movieId to compute average rating and total number of ratings
    movie_ratings = ratings_df.groupBy("movieId").agg(
    avg("rating").alias("avg_rating"),
    count("*").alias("num_ratings"))
    # Filter for movies with at least 100 ratings
    movie_ratings_filtered = movie_ratings.filter(col("num_ratings") >= 100)
    # Join with movies_df to get titles
    top_movies = movie_ratings_filtered.join(movies_df, on="movieId", how="inner")
    # Select and format output
    top_10 = top_movies.select(
    "title",
    round("avg_rating", 2).alias("average_rating"),
    "num_ratings").orderBy(desc("average_rating"), desc("num_ratings")).limit(10)
    # Show top 10 movies
    print("\nTop 10 Movies by Average Rating:")
    top_10.show(truncate=False)

    # Question 8
    print("Question 8")
    # Count ratings per user
    user_activity = ratings_df.groupBy("userId").agg(count("*").alias("rating_count"))
    # Categorize users
    user_categorized = user_activity.withColumn(
    "user_category",
    when(col("rating_count") > 50, "Frequent Rater")
    .otherwise("Infrequent Rater"))
    # Show top 10 users by rating count
    top_users = user_categorized.orderBy(col("rating_count").desc()).limit(10)
    print("\nTop 10 Most Active Users:")
    top_users.show(truncate=False)
    # Count users by category
    user_distribution = user_categorized.groupBy("user_category").agg(count("*").alias("user_count"))
    print("\nUser Category Distribution:")
    user_distribution.show()
    # Save to S3
    output_user_path = f"s3a://{s3_bucket}/task2_Q8"
    user_distribution.coalesce(1).write.mode("overwrite").csv(output_user_path, header=True)
    print(f"\nUser category distribution saved to: {output_user_path}")


    spark.stop()
