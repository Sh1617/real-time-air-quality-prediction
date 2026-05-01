from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

KAFKA_BOOTSTRAP = "34.55.81.150:9092"   # Kafka VM IP
TOPIC = "airquality-events"

spark = SparkSession.builder.appName("AirQualityProducer").getOrCreate()

# Read raw CSV from GCS
df = spark.read.option("header", "true").csv(
    "gs://generic-streaming-analytics-bucket/airquality/globalAirQuality.csv"
)

# Use 'timestamp' column from dataset
df = df.withColumn("event_time", to_timestamp("timestamp")) \
       .filter(col("city").isNotNull() & col("event_time").isNotNull())

pollutants = ["pm25", "pm10", "no2", "so2", "o3", "co", "aqi"]
for c in pollutants:
    df = df.withColumn(c, col(c).cast("double"))

df = df.orderBy("city", "event_time")

w = Window.partitionBy("city").orderBy("event_time") \
         .rowsBetween(Window.unboundedPreceding, 0)
for c in pollutants:
    df = df.withColumn(c, last(c, ignorenulls=True).over(w))

events = df.select(
    to_json(struct(
        col("city"),
        col("event_time").alias("datetime"),
        "pm25","pm10","no2","so2","o3","co","aqi"
    )).alias("value")
)

events.write.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", TOPIC) \
    .save()

print(f"✅ Sent {events.count()} records to Kafka topic {TOPIC}")
spark.stop()
