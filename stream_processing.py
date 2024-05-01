from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, desc, from_json, current_timestamp, row_number, max, lower, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql import Window

def write_to_file(batch_df, batch_id):
    if batch_df.count() > 0:
        # Convert to Pandas DataFrame
        pandas_df = batch_df.toPandas()
        # Drop the 'timestamp' column if it exists in the DataFrame
        if 'timestamp' in pandas_df.columns:
            pandas_df = pandas_df.drop('timestamp', axis=1)
        # Write the remaining data to a file
        with open('/home/vaishnavi/Desktop/DBT/results.txt', 'a') as f:
            f.write(f"Batch ID: {batch_id}\n")
            pandas_df.to_csv(f, index=False)
    else:
        print(f"Batch ID: {batch_id} contains no data.")
        
spark = SparkSession.builder \
    .appName("YouTubeDataProcessing") \
    .master("local[2]") \
    .getOrCreate()

# Define schemas for videos, comments, and searches
video_schema = StructType([
    StructField("title", StringType()),
    StructField("channel_title", StringType()),
    StructField("like_count", StringType()),
    StructField("view_count", StringType()),
    StructField("dislike_count", IntegerType()),
    StructField("comment_count", StringType()),
])

comment_schema = ArrayType(
    StructType([
        StructField("comment_id", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("replies_count", StringType(), True),
        StructField("like_count", StringType(), True),
        StructField("channel_title", StringType(), True),
        StructField("published_at", TimestampType(), True)
    ])
)

search_schema = StructType([
    StructField("search_id", StringType()),
    StructField("title", StringType()),
    StructField("channel_title", StringType()),
    StructField("published_at", TimestampType()),
])

# Create DataFrames from Kafka streams
video_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "youtube_videos") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", video_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

comment_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "youtube_comments") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), comment_schema).alias("comments")) \
    .select(explode(col("comments")).alias("comment_dict")) \
    .select(
        "comment_dict.comment_id",
        "comment_dict.comment",
        "comment_dict.replies_count",
        "comment_dict.like_count",
        "comment_dict.channel_title",
        "comment_dict.published_at"
    ) \
    .withColumn("timestamp", current_timestamp())
comment_df = comment_df.select(
    "comment_id",
    "comment",
    "replies_count",
    "like_count",
    "channel_title",
    "timestamp"
)

search_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "youtube_searches") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", search_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

# Define a window specification for time-based grouping
time_window = window(col("timestamp"), "15 minutes")

# Compute the maximum likes per window
max_likes_df = video_df \
    .groupBy(time_window, "title") \
    .agg(max("like_count").alias("max_likes")) \
    .select("title", "max_likes")  # Select only title and max_likes, excluding window start and end

# Windowed Aggregations
nice_comments = comment_df \
    .filter(lower(col("comment")).contains("nice")) \
    .groupBy(time_window) \
    .count() \
    .withColumnRenamed("count", "nice_comment_count") \
    .orderBy(desc("nice_comment_count"))

# Assuming 'nice_comments' DataFrame still has 'window' (start, end) and 'nice_comment_count'
# Modifying the DataFrame before writing:
nice_comments = nice_comments.select(
    "nice_comment_count"
)

shorts_searches = search_df \
    .filter(col("title").contains("#shorts")) \
    .select("title") \
    .distinct()  # Ensuring distinct entries if needed




# Write each stream to file using foreachBatch
query1 = max_likes_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_file) \
    .start()

query2 = nice_comments \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_file) \
    .start()

query3 = shorts_searches \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_file) \
    .start()


query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
