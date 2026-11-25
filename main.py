from pyspark.sql import SparkSession
from bronze import ingest_bronze
from silver import transform_silver
from gold import build_gold

spark = SparkSession.builder.appName("pipeline_festivals").master("local[*]").getOrCreate()

ingest_bronze(spark)
transform_silver(spark)
build_gold(spark)

spark.stop()