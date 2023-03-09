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
SELECT 
pickup_datetime, dropoff_datetime, ((bigint(to_timestamp(dropoff_datetime)))-(bigint(to_timestamp(pickup_datetime))))/3600 AS diff_in_hours
FROM trips
ORDER BY 3 DESC
LIMIT 1
""")

query_result.show()