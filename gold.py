from pyspark.sql import functions as F


def build_gold(spark):
    # lecture de Silver
    ventes = spark.read.parquet("data/silver/ventes_complete")
    evenements = spark.read.parquet("data/silver/evenements_nettoyes")

    # Règle de gestion : on ne compte que les ventes au statut "validée".
    # Les ventes annulées, remboursées ou en attente n'ont pas encore rapporté d'argent.
    ventes_valides = ventes.filter(F.col("statut") == "validée")

    # Montant d'une vente = prix unitaire x quantité
    ventes_valides = ventes_valides.withColumn("montant_tot", F.col("prix_unitaire") * F.col("quantite"))


    # On additionne les ventes valides de chaque événement :
    # chiffre d'affaires, nombre de ventes et nombre de billets vendus
    par_evenement = (ventes_valides
                     .groupBy("id_evenement")
                     .agg(F.sum("montant_tot").alias("chiffre_affaires"),
                          F.count("id_vente").alias("nb_ventes"),
                          F.sum("quantite").alias("billets_vendus")))

    # Un événement sans aucune vente valide
    # doit apparaître avec 0, et non disparaître ("left" garde tous les événements).
    base = (evenements
            .select("id_evenement", "nom_evenement", "categorie", "capacite_max")
            .join(par_evenement, "id_evenement", "left")
            .fillna(0, subset=["chiffre_affaires", "nb_ventes", "billets_vendus"]))

    # Indicateur 1 : chiffre d'affaires par événement
    chiffre_affaires_evenement = (base
                                  .select("id_evenement", "nom_evenement", "categorie",
                                          F.round("chiffre_affaires", 2).alias("chiffre_affaires"),
                                          "nb_ventes", "billets_vendus")
                                  .orderBy(F.col("chiffre_affaires").desc()))

    # Indicateur 2 : taux de remplissage = billets vendus / capacité de l'événement (en %)
    # (une capacité manquante donne un taux vide)
    taux_remplissage_evenement = (base
                                  .withColumn("taux_remplissage",
                                              F.round(F.col("billets_vendus") / F.col("capacite_max") * 100, 1))
                                  .select("id_evenement", "nom_evenement", "categorie",
                                          "billets_vendus", "capacite_max", "taux_remplissage")
                                  .orderBy(F.col("taux_remplissage").desc()))

    # Indicateur 3 : Performance par ville et catégorie
    # Les ventes dont le lieu est inconnu (ville vide) sont regroupées sous "Inconnu"
    performances_ville_categorie = (ventes_valides
                                    .fillna("Inconnu", subset=["ville"])
                                    .groupBy("ville", "categorie")
                                    .agg(F.round(F.sum("montant_tot"), 2).alias("chiffre_affaires"),
                                         F.count("id_vente").alias("nb_ventes"),
                                         F.sum("quantite").alias("billets_vendus"))
                                    .orderBy(F.col("chiffre_affaires").desc()))

    # Indicateur 4 : Evolution dans le temps
    # Ventes regroupées par jour de vente
    evolution_ventes = (ventes_valides
                        .groupBy("date_vente")
                        .agg(F.round(F.sum("montant_tot"), 2).alias("chiffre_affaires"),
                             F.count("id_vente").alias("nb_ventes"),
                             F.sum("quantite").alias("billets_vendus"))
                        .orderBy("date_vente"))

    # 5. Ecriture de Gold
    chiffre_affaires_evenement.write.mode("overwrite").parquet("data/gold/chiffre_affaires_evenement")
    taux_remplissage_evenement.write.mode("overwrite").parquet("data/gold/taux_remplissage_evenement")
    performances_ville_categorie.write.mode("overwrite").parquet("data/gold/performances_ville_categorie")
    evolution_ventes.write.mode("overwrite").parquet("data/gold/evolution_ventes")

    # Aperçu des résultats
    chiffre_affaires_evenement.show(truncate=False)
    taux_remplissage_evenement.show(truncate=False)
    performances_ville_categorie.show(truncate=False)
    evolution_ventes.show(10)