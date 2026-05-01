from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("AirQualityBatch8h").getOrCreate()

# 1) Read data from GCS
df = spark.read.option("header", "true").csv(
    "gs://generic-streaming-analytics-bucket/airquality/globalAirQuality.csv"
)

# 2) Cast timestamp and pollutants
df = df.withColumn("datetime", to_timestamp("timestamp")) \
       .filter(col("city").isNotNull() & col("datetime").isNotNull())

pollutants = ["pm25","pm10","no2","so2","o3","co","aqi"]
for c in pollutants:
    df = df.withColumn(c, col(c).cast("double"))

# 3) Drop rows with null pollutants/aqi
for c in pollutants:
    df = df.filter(col(c).isNotNull())

# 4) 8‑hour sliding window with 1‑hour slide
agg = df.groupBy(
    window(col("datetime"), "8 hours", "1 hour").alias("w"),
    col("city")
).agg(
    max("aqi").alias("V1"),
    (sum("pm25") + sum("pm10") + sum("no2") +
     sum("so2") + sum("o3") + sum("co")).alias("V2")
)

# 5) Rank cities per window start: lower V1, then lower V2
rank_window = Window.partitionBy("w.start").orderBy(asc("V1"), asc("V2"))
ranked = agg.withColumn("rank", row_number().over(rank_window)) \
            .orderBy("w.start", "rank")

ranked.show(200, truncate=False)
spark.stop()
