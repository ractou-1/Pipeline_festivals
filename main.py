from pyspark.sql import SparkSession
from bronze import ingest_bronze

spark = SparkSession.builder.appName("pipeline_festivals").master("local[*]").getOrCreate()

ingest_bronze(spark)

spark.stop()