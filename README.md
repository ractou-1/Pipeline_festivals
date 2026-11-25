# Pipeline de données — Festivals

Pipeline PySpark d'architecture **Medallion** (Bronze / Silver / Gold) qui intègre trois sources
(billetterie, événements, lieux) d'une entreprise d'organisation de festivals, fiabilise les données
et prépare des indicateurs pour la direction.

> **État d'avancement** : Bronze et Silver sont réalisés. Gold (partie D) et les contrôles
> complets (partie E) restent à faire ; les sections concernées sont marquées *à compléter*.

## 1. Prérequis et installation

- **Python** 3.10 ou plus
- **Java** 17 ou 21 (variable `JAVA_HOME` vers ce Java)
- **PySpark** : `pip install pyspark`
- **Windows uniquement** : Spark a besoin de `winutils.exe` pour écrire les fichiers.
  Placer `winutils.exe` dans `C:\hadoop\bin` et définir la variable d'environnement
  `HADOOP_HOME = C:\hadoop`.

## 2. Organisation du projet

```
pipeline_festivals/
├── main.py          # point d'entrée : enchaîne les couches
├── bronze.py        # ingest_bronze()   : couche Bronze
├── silver.py        # transform_silver(): couche Silver
├── (gold.py)        # build_gold()      : couche Gold (à compléter)
└── data/
    ├── raw/         # fichiers sources : billetterie.csv, evenements.json, lieux.csv
    ├── bronze/      # billetterie/, evenements/, lieux/
    ├── silver/      # billetterie_nettoyee/, evenements_nettoyes/, lieux_nettoyes/,
    │                #   ventes_enrichies/, rejets/ (billetterie, evenements, lieux)
    └── gold/        # indicateurs (à compléter)
```

Chaque couche est une fonction qui lit la couche précédente et écrit la suivante.

## 3. Exécution

1. Déposer les trois fichiers sources dans `data/raw/`.
2. Lancer depuis le dossier du projet : `python main.py`

Les données sont écrites en **Parquet** dans `data/bronze`, `data/silver` et `data/gold`.
Chaque exécution remplace les données précédentes (`overwrite`) : on peut relancer sans créer de doublons.

## 4. Choix techniques

- **Medallion** : chaque couche a une responsabilité. Bronze conserve le brut, Silver fiabilise,
  Gold produit les données métier. Les sources restent intactes et on peut rejouer les traitements.
- **ELT, traitement par lots (batch)** : les données sont d'abord chargées (Bronze) puis transformées.
  Les sources sont des fichiers et les besoins relèvent du reporting : pas de streaming.
- **Parquet** : format compressé adapté à l'analyse, exigé par l'énoncé.
- **Architecture centralisée, exécution locale** : un seul stockage, pas d'infrastructure distribuée.
- Le schéma d'architecture est fourni à part (`partie_A.pdf`).

## 5. Règles de gestion

### Bronze
- Toutes les colonnes sont lues **en texte** : aucune valeur n'est perdue ou modifiée.
- Deux colonnes sont ajoutées pour la traçabilité : `source_fichier` et `date_ingestion`.
- Aucune transformation métier.

### Silver
Pour chaque table : harmonisation des formats, rejet des lignes inexploitables, suppression des doublons.
Les lignes rejetées ne sont pas perdues : elles sont écrites dans `data/silver/rejets/`.

**Lieux**
- Ville et région harmonisées (« nord », « NORD » → « Nord »), capacité en nombre.
- Rejet si l'identifiant du lieu est manquant ou si la capacité est négative ou nulle.
- Une capacité manquante est tolérée (la valeur reste vide).
- Doublons : un seul lieu par identifiant.

**Événements**
- Catégorie harmonisée, dates lues au format `AAAA-MM-JJ`, capacité en nombre.
- Rejet si l'identifiant ou une date est manquant ou illisible, si la date de fin précède la date de
  début, ou si la capacité est négative ou nulle.
- Doublons : un seul événement par identifiant.
- **Événement dont le lieu n'existe pas** (`E013`, lieu `L999`, absent des lieux) : il est **conservé**.
  Ses ventes sont réelles et son chiffre d'affaires comme son taux de remplissage ne dépendent pas du lieu.
  Seules la ville et la région restent vides dans le rapprochement.

**Billetterie**
- Date de vente en vraie date : deux formats existent dans la source (`AAAA-MM-JJ` et `AAAA/MM/JJ`),
  le second étant d'abord converti en `AAAA-MM-JJ`,
  prix « 59,90 » converti en 59.90, quantité en nombre, statut en minuscules.
- Rejet si l'identifiant de vente ou d'événement est manquant, si la date est illisible, si le prix est
  manquant ou négatif, si la quantité est manquante ou négative ou nulle, ou si le statut est manquant
  ou différent de `validée`, `en_attente`, `remboursée`, `annulée` (ex. `en_cours_inconnu`).
- Rejet des ventes dont l'événement n'existe pas dans les événements (ex. `E999`).
- Doublons : une seule ligne par identifiant de vente.
- **Transactions annulées et remboursées** : conservées avec leur statut, car ce sont des informations
  métier. Seules les ventes au statut `validée` seront comptées dans les indicateurs de la couche Gold.
  Les ventes `en_attente` ne sont pas comptées : elles ne sont pas encore confirmées.

**Rapprochement** : la table `ventes_enrichies` associe chaque vente à son événement, puis à son lieu.

### Gold *(à compléter)*
Indicateurs prévus : chiffre d'affaires par événement, taux de remplissage par événement,
performances par ville et catégorie, évolution des ventes dans le temps. Les règles de calcul
(statuts comptés, capacité retenue) seront précisées ici.

## 6. Contrôles qualité réalisés

- **Bronze** : affichage du schéma de chaque table lue (`printSchema`).
- **Silver** : contrôle des types, des valeurs manquantes et aberrantes, des doublons et des références
  entre sources. Les lignes rejetées sont isolées dans `data/silver/rejets/` et leur nombre par cause
  est affiché à l'exécution (lieux invalides, événements invalides, ventes invalides, ventes dont
  l'événement n'existe pas).
- *À compléter (partie E)* : comparaison des volumes entre couches, cohérence des indicateurs Gold,
  journaux d'exécution, gestion des erreurs, et présentation de l'automatisation en production
  (rôle d'un orchestrateur comme Apache Airflow).

## 7. Anomalies constatées dans les données

Anomalies mises en évidence par les contrôles de la couche Silver, et traitement retenu.

| Source | Anomalie | Nombre | Traitement |
|---|---|---|---|
| Billetterie | `id_vente` manquant | 3 | Rejet |
| Billetterie | Date de vente illisible (hors formats `AAAA-MM-JJ` et `AAAA/MM/JJ`) | 8 | Rejet |
| Billetterie | Prix manquant | 12 | Rejet |
| Billetterie | Prix négatif | 6 | Rejet |
| Billetterie | Quantité manquante | 8 | Rejet |
| Billetterie | Quantité négative ou nulle | 10 | Rejet |
| Billetterie | Statut inconnu (`en_cours_inconnu`) | 6 | Rejet |
| **Billetterie** | **Total des ventes invalides** | **53** | Isolées dans `rejets/billetterie` |
| Billetterie | Vente d'un événement qui n'existe pas (`E999`) | 12 | Rejet (même dossier) |
| Billetterie | Dates écrites `AAAA/MM/JJ` au lieu de `AAAA-MM-JJ` | environ 270 | Harmonisées, non rejetées |
| Événements | `E013` rattaché à un lieu inexistant (`L999`) | 1 événement, environ 500 ventes | **Conservé**, ville et région vides |
| Lieux | Lignes invalides | 0 | Sans objet |
| Événements | Lignes invalides | 0 | Sans objet |
| Toutes | Doublons | à mesurer | Supprimés (un identifiant = une ligne) |

Remarques :
- Une vente peut avoir plusieurs défauts : le total de 53 compte chaque ligne une seule fois.
- Les ventes d'`E013` auraient été perdues avec une règle stricte sur les lieux : les conserver évite de
  retirer environ 500 ventes réelles du chiffre d'affaires.
- Le nombre de doublons supprimés sera chiffré avec la comparaison des volumes entre couches (partie E).
