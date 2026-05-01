from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

KAFKA_BOOTSTRAP = "34.55.81.150:9092"
TOPIC = "airquality-events"

spark = SparkSession.builder \
    .appName("AirQualityRanking") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/aq-checkpoint") \
    .getOrCreate()

schema = StructType([
    StructField("city", StringType()),
    StructField("datetime", TimestampType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("so2", DoubleType()),
    StructField("o3", DoubleType()),
    StructField("co", DoubleType()),
    StructField("aqi", DoubleType())
])

raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .load()

parsed = raw.select(from_json(col("value").cast("string"), schema).alias("d")) \
            .select("d.*") \
            .withWatermark("datetime", "1 hour")

# 8‑hour sliding window (slide 1 hour)
metrics = parsed.groupBy(
    window(col("datetime"), "8 hours", "1 hour").alias("w"),
    col("city")
).agg(
    max("aqi").alias("V1"),
    (sum("pm25") + sum("pm10") + sum("no2") +
     sum("so2") + sum("o3") + sum("co")).alias("V2")
)

# Rank per window: lower V1, then lower V2
ranked = metrics.withColumn(
    "rank",
    row_number().over(
        Window.partitionBy("w.start").orderBy(asc("V1"), asc("V2"))
    )
).orderBy("w.start", "rank")

query = ranked.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
