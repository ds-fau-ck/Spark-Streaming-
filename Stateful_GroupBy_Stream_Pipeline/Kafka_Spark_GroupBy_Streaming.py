from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("GroupByStreaming") \
    .master("yarn") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Read the stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-4j8dq.southeastasia.aws.confluent.cloud:9092") \
    .option("subscribe", "trx_topic_data") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 
            f"org.apache.kafka.common.security.plain.PlainLoginModule required username='7TUSIFBKFOX4UR' password='Ks4s1dZAgPEtSwRhTwxv5MprrvTd5oaLaZ6en79iYAgBTeoeZ8tJw';") \
    .load()

# Define the schema of the JSON data
schema = StructType([
    StructField("user_id", StringType()),
    StructField("amount", IntegerType()),
    StructField("timestamp", TimestampType())
])

# Parse the JSON data and select the fields
df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Perform the aggregation
df = df.groupBy("user_id").agg(sum("amount").alias("total_amount"))

checkpoint_dir = "/tmp/checkpoint-dir/dir_new2"

# Start streaming and print to console
query = df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()
print("Write successfull")

# Wait for the query to terminate
query.awaitTermination()
