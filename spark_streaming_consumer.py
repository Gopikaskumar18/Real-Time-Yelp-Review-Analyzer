import findspark
findspark.init()

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime

import nltk
nltk.download('vader_lexicon')

# Sentiment analysis setup
sid = SentimentIntensityAnalyzer()

def get_sentiment(text):
    score = sid.polarity_scores(text)['compound']
    if score >= 0.05:
        return 'positive'
    elif score <= -0.05:
        return 'negative'
    else:
        return 'neutral'

# Spark session setup
spark = SparkSession.builder \
    .appName("YelpReviewConsumer") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/yelp.reviews") \
    .config("spark.jars", ",".join([
        "jars/mongo-spark-connector_2.12-3.0.1.jar",
        "jars/mongo-java-driver-3.12.10.jar",
        "jars/spark-sql-kafka-0-10_2.12-3.0.1.jar",
        "jars/spark-token-provider-kafka-0-10_2.12-3.0.1.jar",
        "jars/kafka-clients-2.4.1.jar",
        "jars/commons-pool2-2.8.0.jar"
    ])) \
    .getOrCreate()



spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType() \
    .add("review_id", StringType()) \
    .add("text", StringType()) \
    .add("stars", IntegerType())

# UDF for sentiment
sentiment_udf = udf(get_sentiment, StringType())

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "yelp-reviews") \
    .load()

# Parse the review JSON
parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Add sentiment and timestamp
enriched = parsed.withColumn("sentiment", sentiment_udf(col("text"))) \
                 .withColumn("timestamp", current_timestamp())

# Function to write micro-batch to MongoDB
def write_to_mongo(batch_df, batch_id):
    batch_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", "mongodb://localhost:27017/yelp.reviews") \
        .save()

# Start the stream
query = enriched.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_mongo) \
    .option("checkpointLocation", "/tmp/mongo-checkpoint") \
    .start()

query.awaitTermination()
