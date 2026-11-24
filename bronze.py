from pyspark.sql import functions as F


def ingest_bronze(spark):
    # Lecture des sources
    # on ne perd aucune valeur (ex. "59,90"). Le typage sera fait en Silver.
    billetterie = spark.read.csv("data/raw/billetterie.csv", header=True)
    lieux = spark.read.csv("data/raw/lieux.csv", header=True)
    evenements = (spark.read
                  .option("multiLine", True)
                  .option("primitivesAsString", True)
                  .json("data/raw/evenements.json"))

    # On affiche les colonnes lues
    billetterie.printSchema()
    evenements.printSchema()
    lieux.printSchema()

    # on ajoute Métadonnées techniques (provenance et date d'ingestion)
    billetterie = (billetterie
                   .withColumn("source_fichier", F.lit("billetterie.csv"))
                   .withColumn("date_ingestion", F.current_timestamp()))
    evenements = (evenements
                  .withColumn("source_fichier", F.lit("evenements.json"))
                  .withColumn("date_ingestion", F.current_timestamp()))
    lieux = (lieux
             .withColumn("source_fichier", F.lit("lieux.csv"))
             .withColumn("date_ingestion", F.current_timestamp()))

    # Écriture dans la couche Bronze en Parquet
    billetterie.write.mode("overwrite").parquet("data/bronze/billetterie")
    evenements.write.mode("overwrite").parquet("data/bronze/evenements")
    lieux.write.mode("overwrite").parquet("data/bronze/lieux")