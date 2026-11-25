from pyspark.sql import SparkSession
from bronze import ingest_bronze
from silver import transform_silver

spark = SparkSession.builder.appName("pipeline_festivals").master("local[*]").getOrCreate()

ingest_bronze(spark)
transform_silver(spark)

spark.stop()