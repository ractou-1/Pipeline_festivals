from pyspark.sql import functions as F


def transform_silver(spark):
    # Une valeur illisible (date, nombre) devient NULL au lieu de faire planter Spark
    spark.conf.set("spark.sql.ansi.enabled", "false")

    # Lecture de Bronze
    billetterie = spark.read.parquet("data/bronze/billetterie")
    evenements = spark.read.parquet("data/bronze/evenements")
    lieux = spark.read.parquet("data/bronze/lieux")

    #LIEUX
    # on fait l'harmonisation 
    #"nord" et "NORD" deviennent "Nord", capacité en nombre...
    lieux = (lieux
             .withColumn("ville", F.initcap(F.lower(F.trim("ville"))))
             .withColumn("region", F.initcap(F.lower(F.trim("region"))))
             .withColumn("capacite_site", F.col("capacite_site").cast("int")))

    # Une ligne est valide si elle a un identifiant et une capacité absente ou positive
    lieu_valide = (F.col("id_lieu").isNotNull()
                   & (F.col("capacite_site").isNull() | (F.col("capacite_site") > 0)))
    lieux_rejets = lieux.filter(~lieu_valide)
    # on supprime les doublons car un identifiant représente un seul lieu
    lieux_ok = lieux.filter(lieu_valide).dropDuplicates(["id_lieu"])

    #EVENEMENTS
    # on fait l'harmonisation ( catégorie, dates (AAAA-MM-JJ) et capacite_max)
    evenements = (evenements
                  .withColumn("categorie", F.initcap(F.lower(F.trim("categorie"))))
                  .withColumn("date_debut", F.to_date("date_debut")) #passage en date
                  .withColumn("date_fin", F.to_date("date_fin"))
                  .withColumn("capacite_max", F.col("capacite_max").cast("int"))) #passage en entier

    # Valide si : identifiant et dates présents, fin après début, capacité absente ou positive
    evenement_valide = (F.col("id_evenement").isNotNull()
                        & F.col("date_debut").isNotNull()
                        & F.col("date_fin").isNotNull()
                        & (F.col("date_fin") >= F.col("date_debut"))
                        & (F.col("capacite_max").isNull() | (F.col("capacite_max") > 0)))
    evenements_rejets = evenements.filter(~evenement_valide)

    # on supprime les doublons car un identifiant représente un seul événement
    # Un événement dont le lieu n'existe pas est conservé (ses ventes sont réelles) : sa ville restera vide
    evenements_ok = evenements.filter(evenement_valide).dropDuplicates(["id_evenement"])

    # BILLETTERIE
    # on fait l'harmonisation ( date, prix "59,90" -> 59.90, quantité en nombre, statut en minuscules )
    # la date de vente existe sous 2 formats : AAAA-MM-JJ et AAAA/MM/JJ
    # on remplace d'abord "/" par "-", puis on convertit en date
    billetterie = (billetterie
                   .withColumn("date_vente", F.coalesce(
                       F.to_date(F.translate("date_vente", "/", "-")),
                       F.to_date("date_vente", "d/M/yyyy")))
                   .withColumn("prix_unitaire", F.translate("prix_unitaire", ",", ".").cast("double"))
                   .withColumn("quantite", F.col("quantite").cast("int"))
                   .withColumn("statut", F.lower(F.trim("statut"))))

    # Valide si identifiants, date, prix (positif ou nul), quantité (positive) et statut présents
    vente_valide = (F.col("id_vente").isNotNull()
                    & F.col("id_evenement").isNotNull()
                    & F.col("date_vente").isNotNull()
                    & F.col("prix_unitaire").isNotNull() & (F.col("prix_unitaire") >= 0) #si entrée gratuite
                    & F.col("quantite").isNotNull() & (F.col("quantite") > 0)
                    & F.col("statut").isNotNull()
                    & F.col("statut").isin("validée", "en_attente", "remboursée", "annulée"))
    ventes_invalides = billetterie.filter(~vente_valide)
    ventes_valides = billetterie.filter(vente_valide)

    # l'événement de la vente doit exister dans les événements valides
    ids_evenements = evenements_ok.select("id_evenement")
    ventes_sans_evenement = ventes_valides.join(ids_evenements, "id_evenement", "left_anti")
    billetterie_ok = ventes_valides.join(ids_evenements, "id_evenement").dropDuplicates(["id_vente"])
    billetterie_rejets = ventes_invalides.unionByName(ventes_sans_evenement)

    #RAPPROCHEMENT
    # Chaque vente reçoit les informations de son événement, puis de son lieu
    # "left" : une vente dont le lieu est inconnu est gardée (sa ville reste vide)
    ventes_complete = (billetterie_ok
                        .join(evenements_ok.drop("source_fichier", "date_ingestion"), "id_evenement")
                        .join(lieux_ok.drop("source_fichier", "date_ingestion"), "id_lieu", "left"))

    # Nombre de lignes rejetées, par cause
    print("Lieux invalides :", lieux_rejets.count())
    print("Evenements invalides :", evenements_rejets.count())
    print("Ventes invalides :", ventes_invalides.count())
    print("Ventes dont l'evenement n'existe pas :", ventes_sans_evenement.count())

    print("id_vente manquant :", billetterie.filter(F.col("id_vente").isNull()).count())
    print("date null :", billetterie.filter(F.col("date_vente").isNull()).count())
    print("prix manquant :", billetterie.filter(F.col("prix_unitaire").isNull()).count())
    print("prix négatif :", billetterie.filter(F.col("prix_unitaire") < 0).count())
    print("quantité manquante :", billetterie.filter(F.col("quantite").isNull()).count())
    print("quantité <= 0 :", billetterie.filter(F.col("quantite") <= 0).count())
    billetterie.groupBy("statut").count().show()

    # Ecriture
    lieux_ok.write.mode("overwrite").parquet("data/silver/lieux_nettoyes")
    evenements_ok.write.mode("overwrite").parquet("data/silver/evenements_nettoyes")
    billetterie_ok.write.mode("overwrite").parquet("data/silver/billetterie_nettoyee")
    ventes_complete.write.mode("overwrite").parquet("data/silver/ventes_enrichies")

    lieux_rejets.write.mode("overwrite").parquet("data/silver/rejets/lieux")
    evenements_rejets.write.mode("overwrite").parquet("data/silver/rejets/evenements")
    billetterie_rejets.write.mode("overwrite").parquet("data/silver/rejets/billetterie")