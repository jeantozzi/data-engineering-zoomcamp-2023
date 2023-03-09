import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .parquet('./parquet/')

df.createOrReplaceTempView("trips")

query_result = spark.sql("""
SELECT COUNT(*)
FROM trips
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2021
    AND EXTRACT(MONTH FROM pickup_datetime) = 6
    AND EXTRACT(DAY FROM pickup_datetime) = 15
""")

query_result.show()