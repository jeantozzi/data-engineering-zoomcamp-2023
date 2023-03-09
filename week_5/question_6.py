import pyspark
from pyspark.sql import SparkSession
import wget

file_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
file_path = wget.download(file_url)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df_trips = spark.read \
    .option("header", "true") \
    .parquet('./parquet/')

df_zones = spark.read \
    .option("header", "true") \
    .csv(file_path)

df_join = df_trips.join(df_zones, (df_trips['PULocationID'] == df_zones['LocationID']))

df_join.createOrReplaceTempView("trips_joined")

query_result = spark.sql("""
SELECT Zone, COUNT(*)
FROM trips_joined
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1
""")

query_result.show()