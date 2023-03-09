import pyspark
from pyspark.sql import SparkSession
import wget

file_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz'
file_path = wget.download(file_url)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv(file_path)

df.repartition(12).write.parquet('parquet/')