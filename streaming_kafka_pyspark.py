from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder\
                    .appName("KafkaSparkStreaming")\
                    .master("local[*]")\
                    .config("spark.sql.streaming.schemaInference", True)\
                    .getOrCreate()

kafka_df = spark.readStream\
                .format("kafka")\
                .option("kafka.bootstrap.servers", "localhost:9092")\
                .option("subscribe", "Pavan_Topic")\
                .option("startingOffsets", "earliest")\
                .load()


# Convert Kafka "value" column (binary) into string
messages_df = kafka_df.selectExpr("CAST(value AS STRING) as message")
# Add a simple transformation
processed_df = messages_df.withColumn("message_length", expr("length(message)"))
# Write output to console
query = (
processed_df.writeStream
.outputMode("append")
.format("console")
.start()
)
query.awaitTermination()
