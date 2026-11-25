# Pipeline de données — Festivals

Pipeline PySpark d'architecture **Medallion** (Bronze / Silver / Gold) qui intègre trois sources
(billetterie, événements, lieux) d'une entreprise d'organisation de festivals, corrige les données
et prépare des indicateurs pour la direction.

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
├── main.py          # point d'entrée 
├── bronze.py        # ingest_bronze() pour la couche Bronze
├── silver.py        # transform_silver() pour la couche Silver
├── gold.py        # build_gold() pour la couche Gold 
└── data/
    ├── raw/         # billetterie.csv, evenements.json, lieux.csv
    ├── bronze/      # billetterie/, evenements/, lieux/
    ├── silver/      # billetterie_nettoyee/, evenements_nettoyes/, lieux_nettoyes/,
    │                #   ventes_complete/, rejets/ (billetterie, evenements, lieux)
    └── gold/        # indicateurs 
```

Chaque couche est une fonction qui lit la couche précédente et écrit la suivante.

## 3. Exécution

1. Déposer les trois fichiers sources dans `data/raw/`.
2. Lancer depuis le dossier du projet : `python main.py`

Les données sont écrites en **Parquet** dans `data/bronze`, `data/silver` et `data/gold`.
Chaque exécution remplace les données précédentes (`overwrite`), on peut relancer sans créer de doublons.

## 4. Choix techniques

- **Medallion** : chaque couche a une responsabilité. Bronze conserve le brut, Silver fiabilise,
  Gold produit les données métier. Les sources restent intactes et on peut refaire les traitements.
- **ELT, traitement par lots** : les données sont d'abord chargées (Bronze) puis transformées.
- **Parquet** : format compressé adapté à l'analyse, demandé par l'énoncé.

## 5. Règles de gestion

### Bronze
- Toutes les colonnes sont lues **en texte** : aucune valeur n'est perdue ou modifiée.
- Deux colonnes sont ajoutées pour la traçabilité : `source_fichier` et `date_ingestion`.
- Aucune transformation métier.

### Silver
Pour chaque table, harmonisation des formats, rejet des lignes inexploitables, suppression des doublons.
Les lignes rejetées ne sont pas perdues, elles sont écrites dans `data/silver/rejets/`.

**Lieux**
- Ville et région harmonisées (« nord », « NORD » → « Nord »), capacité en nombre.
- Rejet si l'identifiant du lieu est manquant ou si la capacité est négative ou nulle.
- Une capacité manquante est acceptée (la valeur reste vide).
- Doublons : un seul lieu par identifiant.

**Événements**
- Catégorie harmonisée, dates lues au format `AAAA-MM-JJ`, capacité en nombre.
- Rejet si l'identifiant ou une date est manquant ou illisible, si la date de fin précède la date de
  début, ou si la capacité est négative ou nulle.
- Un seul événement par identifiant.
- **Événement dont le lieu n'existe pas** (`E013`, lieu `L999`, absent des lieux) : il est **conservé**.
  Ses ventes sont réelles et son chiffre d'affaires comme son taux de remplissage ne dépendent pas du lieu.
  Seules la ville et la région restent vides dans le rapprochement.

**Billetterie**
- Date de vente en vraie date, deux formats existent dans la source (`AAAA-MM-JJ` et `JJ/MM/AAAA`),
  le second étant d'abord converti en `AAAA-MM-JJ`,
  prix « 59,90 » converti en 59.90, quantité en nombre, statut en minuscules.
- Rejet si l'identifiant de vente ou d'événement est manquant, si la date est illisible, si le prix est
  manquant ou négatif, si la quantité est manquante ou négative ou nulle, ou si le statut est manquant
  ou différent de `validée`, `en_attente`, `remboursée`, `annulée` (ex. `en_cours_inconnu`).
- Rejet des ventes dont l'événement n'existe pas dans les événements (ex. `E999`).
- Une seule ligne par identifiant de vente.
- **Transactions annulées et remboursées** : conservées avec leur statut, car ce sont des informations
  métier. Seules les ventes au statut `validée` seront comptées dans les indicateurs de la couche Gold.
  Les ventes `en_attente` ne sont pas comptées car elles ne sont pas encore confirmées.

**Rapprochement** : la table `ventes_complete` associe chaque vente à son événement, puis à son lieu.

### Gold
Les indicateurs sont calculés à partir de Silver uniquement (`ventes_enrichies` et `evenements_nettoyes`).

**Règle commune : seules les ventes au statut `validée` sont comptées.** Les ventes annulées et
remboursées ne rapportent pas d'argent, les ventes `en_attente` ne sont pas encore confirmées.
Le montant d'une vente est `prix unitaire × quantité`.

| Table Gold | Contenu | Règle de calcul |
|---|---|---|
| `chiffre_affaires_evenement` | Chiffre d'affaires, nombre de ventes et billets vendus par événement | Somme des montants des ventes validées. Un événement sans vente validée apparaît avec 0. |
| `taux_remplissage_evenement` | Billets vendus, capacité et taux de remplissage par événement | Taux = billets vendus (ventes validées) / `capacite_max` de l'événement × 100. On retient la capacité de l'événement, car c'est elle qui limite les billets vendables. Une capacité manquante donne un taux vide.|
| `performances_ville_categorie` | Chiffre d'affaires, ventes et billets par ville et par catégorie d'événement | Mêmes ventes validées. Les ventes sans lieu connu (`E013`) sont regroupées sous la ville « Inconnu ». |
| `evolution_ventes` | Chiffre d'affaires, ventes et billets par jour de vente | Regroupement par `date_vente`, triée par date. |

## 6. Contrôles qualité réalisés

- **Bronze** : affichage du schéma de chaque table lue (`printSchema`).
- **Silver** : contrôle des types, des valeurs manquantes et aberrantes, des doublons et des références
  entre sources. Les lignes rejetées sont isolées dans `data/silver/rejets/` et leur nombre par cause
  est affiché à l'exécution (lieux invalides, événements invalides, ventes invalides, ventes dont
  l'événement n'existe pas).

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
- Le total de 53 ventes invalides est cohérent avec le détail : 3 (id manquant) + 8 (date illisible) + 12 (prix manquant) + 6 (prix négatif) + 8 (quantité manquante) + 10 (quantité négative ou nulle) + 6 (statut inconnu) = 53. Aucune ligne n'a plusieurs défauts à la fois, donc chaque ligne n'est comptée qu'une seule fois.
- Une vente peut avoir plusieurs défauts, le total de 53 compte chaque ligne une seule fois.
- Les ventes d'`E013` auraient été perdues avec une règle stricte sur les lieux, les conserver évite de
  retirer environ 500 ventes réelles du chiffre d'affaires.

#### 8. Résultats et observations

Sur les 12 événements ayant des ventes validées : **419 922 € de chiffre d'affaires**,
**9 627 billets vendus** et **4 987 ventes validées**. Les ventes commencent le 2026-04-01.

**Chiffre d'affaires**
- Les chiffres d'affaires sont très proches d'un événement à l'autre (de 30 790 € pour `E011` à
  37 445 € pour `E001`). Le meilleur est `E001` (Nuits Sonores du Nord).
- `E012` (Scènes d'Automne) n'a aucune vente : il apparaît à 0 € et 0 %, grâce à la jointure qui
  conserve tous les événements.
- Par catégorie, la Culture (4 événements) est en tête avec 145 131 €, devant la Musique (4 événements)
  avec 140 157 €.

**Taux de remplissage**
- `E013` (Festival Hors Cadre) est le mieux rempli : 94,6 % (851 billets pour 900 places). Il est
  suivi de `E007` (68,8 %) et `E006` (51,7 %).
- La plupart des autres événements sont entre 23 % et 41 %. Comme les chiffres d'affaires sont proches,
  l'écart vient surtout de la capacité : `E005` a la plus grande capacité (3 500 places) et le plus faible
  taux parmi les événements vendus (23,6 %).
- Aucun taux ne dépasse 100 % : il n'y a pas de vente au-delà de la capacité.

**Ville et catégorie**
- Lyon est la première ville (69 259 €), mais c'est parce qu'elle accueille deux événements de
  musique (`E002` et `E009`).
- Les ventes d'`E013` (36 398 €, 851 billets) sont regroupées sous la ville « Inconnu », car son lieu
  `L999` n'existe pas dans le référentiel des lieux.
