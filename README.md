# 4DDEV : création d'une architecture Data sur les voyages de taxis et la météo
## Auteur : Julien RENOULT - Tom JOUSSET - Béatrice BEAVOGUI - Mamadou-alpha DIALLO
## Promo : SUPINFO Programme Grande École 4ème année
### Spécialité : Ingénierie Data
### Date : 01/05/2026 - 05/06/2026

# Introduction

Dans le cadre du projet, nous devions construire une architecture data pour la collecte et l'analyse des voyages de taxis à New York City et la météo associée.

Pour présenter ce projet, nous allons d'abord vous présenter les technologies utilisées, ensuite les pré-requis pour pouvoir lancer l'architecture et pour finir ceux que nous avons faits pour chaque partie.

# Technologies utilisées  (Installation/Déploiement)

Pour mener à bien ce projet, plusieurs technologies ont dû être utilisées notamment :

- **python** : langage de programmation
- **Airflow** : orchestrateur de nos DAGs avec 
- **Spark/Spark Streaming** : outil pour la transformation de données volumineuses
- **PostgreSQL** : base de données permettant l'insertion et le traitement de JSON très simplement pour un gros volume de données
- **Marimo** : Notebook python permettant de faire une analyse exploratoire sur nos données
- **Docker** : technologie pour faciliter le déploiement de notre projet

# Pré-requis pour l'utilisation de l'architecture

Pour pouvoir faire fonctionner architecture, nous demandons que Docker Desktop ou équivalent soit installée et configurée avec ces configurations :

- autoriser au moins 4 CPU à utiliser pour **Docker** afin que *Airflow* lance facilement ses DAGs créés
- autoriser au moins 3 Go de mémoire pour **Docker** afin que la mémoire ne soit pas dépassée

Si vous avez installé **Docker**, vous pouvez passer à la suite de cette documentation.

# Utilisation de l'architecture

Pour utiliser notre architecture sans le *hot reload*, vous devez utiliser le **docker-compose-dev-prod.yaml**. Pour ce faire, vous devez utiliser les commandes ci-dessous :

```sh

# Le lancer pour la première fois
docker compose --env-file .env -f docker-compose-dev-prod.yaml up -d --build

# l'arrêter
docker compose -f docker-compose-dev-prod.yaml down

# Avec suppression des volumes
docker compose -f docker-compose-dev-prod.yaml down -v

# Relancer les conteneurs
docker compose --env-file .env -f docker-compose-dev-prod.yaml up

```

Si vous voules le lancer avec le hot reaload dev, vous devez utiliser **docker-compose-dev.yaml** :

```sh

# Le lancer pour la première fois
docker compose --env-file .env -f docker-compose-dev.yaml up -d --build

# l'arrêter
docker compose -f docker-compose-dev.yaml down

# Avec suppression des volumes
docker compose -f docker-compose-dev.yaml down -v

# Relancer les conteneurs
docker compose --env-file .env -f docker-compose-dev.yaml up

```

Si vous téléchargez la première fois les images, cela peut prendre pas mal de temps (10 à 15 minutes le temps de build et de démarrer le serveur d'airflow/minio proprement). Ensuite vous pourrez explorer le travail réalisé en suivant les différentes parties documentées ci-dessous. 

# Partie 1 : collecte des données

La première étape de notre architecture consiste à collecter automatiquement les données de trajets de taxis new-yorkais ainsi que les données météorologiques associées afin d’alimenter un Data Lake centralisé.

## Sources de données

Deux sources principales ont été utilisées :

### Données Taxi NYC

Les données proviennent du portail officiel NYC TLC :

- Source : `https://d37ci6vzurychx.cloudfront.net/trip-data/`
- Format : fichiers Parquet mensuels (`yellow_tripdata`)

Ces données contiennent notamment :

- les horaires des trajets,
- les distances parcourues,
- les montants des courses,
- les pourboires,
- les types de paiement.

### Données météo

Les données météo sont récupérées via l’API OpenWeatherMap :

- Endpoint : `https://api.openweathermap.org/data/2.5/weather`

Les informations collectées incluent :

- la température,
- l’humidité,
- les conditions météo,
- la vitesse du vent.

## Architecture de collecte

La collecte repose sur :

- `Python` pour les scripts,
- `Requests` pour les appels HTTP,
- `MinIO` comme Data Lake compatible S3,
- `Airflow` pour l’orchestration,
- `Docker` pour le déploiement.

Les données brutes sont historisées dans `MinIO` avant transformation.

## Collecte des données météo

Le pipeline météo :

1. interroge l’API OpenWeatherMap,
2. transforme la réponse JSON,
3. ajoute des métadonnées techniques,
4. stocke les données dans MinIO.

Les fichiers sont organisés par date :
```
raw-weather/weather/YYYY/MM/DD/
```
Exemple :
```
raw-weather/weather/2026/05/06/
```
Un fichier `_metadata.json` est généré pour chaque import afin de conserver les informations techniques d’ingestion.

## Orchestration

Les pipelines sont exécutés automatiquement via Airflow afin d’assurer :

- l’automatisation des collectes,
- le suivi des exécutions,
- la reprise sur erreur.

L’ensemble de l’architecture est conteneurisé avec `Docker-Compose` pour faciliter le déploiement et la reproductibilité du projet.
# Partie 2 : transformation des données (DAGs Airflow pyspark)

La seconde étape de l’architecture consiste à transformer les données brutes stockées dans MinIO afin de produire des données exploitables pour l’analyse et la modélisation analytique.

Les traitements sont réalisés avec **PySpark** et orchestrés via **Airflow**.

## Transformation des données météo

Les données météo sont traitées en streaming avec PySpark Structured Streaming.

Le pipeline :

- surveille automatiquement les buckets MinIO contenant les données météo réelles et fictives,
- lit les fichiers JSON au fil de leur arrivée,
- applique des transformations métier,
- insère les données transformées dans PostgreSQL.
- Transformations appliquées

### Les principales transformations sont :

1. conversion des timestamps Unix en dates exploitables,
2. création de colonnes temporelles :
    - heure,
    - jour de semaine,
    - date de mesure,
3. ajout d’un indicateur permettant de distinguer les données réelles des données fictives.

Les données finales sont enregistrées dans la table :
```
public.dim_weather
```
## Transformation des données taxi

Les données taxi sont traitées en batch avec PySpark.

Le pipeline :

- lit les fichiers Parquet présents dans MinIO,
- applique des filtres qualité,
- enrichit les données,
- charge les données transformées dans PostgreSQL.

### Nettoyage des données

Les traitements de qualité incluent :

- suppression des trajets invalides,
- suppression des distances négatives ou nulles,
- suppression des montants incohérents,
- suppression des valeurs manquantes critiques.

### Enrichissement métier

Plusieurs colonnes calculées sont créées :

- durée du trajet,
- jour et heure de prise en charge,
- tranche de distance,
- pourcentage de pourboire,
- type de paiement lisible.

Les données sont également enrichies avec les zones géographiques NYC via des jointures avec le fichier `taxi_zone_lookup.csv`.

### Chargement PostgreSQL

Les données finales sont insérées dans :
```
public.fact_taxi_trips
```
## Architecture technique

Les traitements utilisent :

- `Spark Structured Streaming` pour la météo,
- `Spark Batch` pour les données taxi historiques,
- `MinIO (S3A)` comme source de données,
- `PostgreSQL` comme Data Warehouse analytique.

Des mécanismes supplémentaires ont été mis en place pour :

- éviter les doublons,
- gérer les erreurs de lecture,
- optimiser la mémoire,
- garantir la reprise après interruption via les checkpoints Spark.

## Résultat des transformations

À l’issue de cette étape :

- les données sont nettoyées et enrichies,
- les données météo et taxi sont centralisées dans PostgreSQL,
- les tables analytiques sont prêtes pour les modèles dbt et les analyses métier.

## Orchestration avec Airflow

L’ensemble des pipelines de collecte et de transformation est orchestré avec **Apache Airflow**. Chaque traitement est représenté par un DAG dédié, ce qui permet de séparer clairement les responsabilités entre ingestion, génération de données fictives et transformation Spark.

### DAGs d’ingestion

Plusieurs DAGs assurent l’alimentation du Data Lake MinIO :

- `collect_taxi_zone_lookup` : télécharge le fichier de référence des zones NYC Taxi et le stocke dans MinIO.
- `collecte_taxi_trips_batch` : collecte mensuellement les fichiers Parquet des taxis jaunes NYC.
- `weather_streaming_ingestion_minio` : collecte les données météo réelles depuis l’API OpenWeatherMap toutes les heures.
- `weather_fake_generator_minio` : génère des données météo fictives historiques afin de compléter les périodes sans données réelles.

### DAGs de transformation

Les transformations sont également pilotées par Airflow :

- `taxi_trips_spark_batch` : lance un job Spark batch pour transformer les fichiers Parquet taxi et alimenter la table `fact_taxi_trips`.
- `weather_streaming_pipeline` : génère des données météo fictives puis lance un job Spark Structured Streaming pour alimenter la table `dim_weather`.

Les jobs Spark sont exécutés avec `spark-submit` via des `BashOperator`, tandis que les scripts Python de collecte utilisent des `PythonOperator`.

### Gestion de l’exécution

Les DAGs intègrent plusieurs mécanismes de fiabilité :

- exécution planifiée (`@hourly`, `@monthly`, `@once`),
- retries automatiques en cas d’échec,
- délais entre les tentatives,
- absence de `catchup` pour éviter les exécutions historiques non souhaitées,
- passage des variables sensibles via les variables d’environnement,
- limitation du temps d’exécution pour certains jobs Spark.

Airflow permet ainsi de superviser chaque étape du pipeline, de suivre les logs d’exécution et de relancer facilement les traitements en cas d’erreur.

# Partie 3 : modèles dbt pour l'analytics

La troisième étape consiste à construire des modèles analytiques avec **dbt** à partir des tables PostgreSQL produites par Spark : `fact_taxi_trips` et `dim_weather`.

Les sources sont déclarées dans un fichier `sources.yml`, ce qui permet à dbt de référencer proprement les tables du schéma `public`.

## Modèles créés

Trois modèles principaux ont été développés dans le schéma `marts`.

### `trip_enriched`

Ce modèle enrichit les trajets de taxi avec les informations météo.

Il réalise une jointure entre :
- `fact_taxi_trips`
- `dim_weather`

La jointure est faite à partir de la date de prise en charge du trajet et de la date de mesure météo.
La troisième étape consiste à construire des modèles analytiques avec **dbt** à partir des tables PostgreSQL produites par Spark : `fact_taxi_trips` et `dim_weather`.

Les sources sont déclarées dans un fichier `sources.yml`, ce qui permet à dbt de référencer proprement les tables du schéma `public`.

## Modèles créés

Trois modèles principaux ont été développés dans le schéma `marts`.

### `trip_enriched`

Ce modèle enrichit les trajets de taxi avec les informations météo.

Il réalise une jointure entre :
- `fact_taxi_trips`
- `dim_weather`

La jointure est faite à partir de la date de prise en charge du trajet et de la date de mesure météo.
La troisième étape consiste à construire des modèles analytiques avec **dbt** à partir des tables PostgreSQL produites par Spark : `fact_taxi_trips` et `dim_weather`.

Les sources sont déclarées dans un fichier `sources.yml`, ce qui permet à dbt de référencer proprement les tables du schéma `public`.

## Modèles créés

Trois modèles principaux ont été développés dans le schéma `marts`.

### `trip_enriched`

Ce modèle enrichit les trajets de taxi avec les informations météo.

Il réalise une jointure entre :
- `fact_taxi_trips`
- `dim_weather`

La jointure est faite à partir de la date de prise en charge du trajet et de la date de mesure météo.

Le modèle contient notamment :
- les informations du trajet,
- l’heure et la date de prise en charge,
- la tranche de distance,
- le montant de la course,
- le pourboire,
- le pourcentage de pourboire,
- le type de paiement,
- la météo associée.

Ce modèle est matérialisé en mode `incremental` afin de ne traiter que les nouveaux trajets lors des prochaines exécutions.

### `trip_summary_per_hour`

Ce modèle agrège les trajets par heure et description météo.

Il calcule :
- le nombre de trajets,
- la durée moyenne des trajets,
- le pourboire moyen.

Ce modèle permet d’analyser l’impact des conditions météo sur l’activité des taxis selon les heures de la journée.

### `high_value_customers`

Ce modèle regroupe les trajets par nombre de passagers.

Il calcule :
- le nombre total de trajets,
- le montant total dépensé,
- le pourcentage moyen de pourboire.

Il permet d’identifier les groupes de passagers les plus rentables selon leur comportement de dépense et de pourboire.

## Exécution dbt

Les modèles sont exécutés avec la commande :

```sh
dbt run
```
Exemple de résultat obtenu :
```
1 of 3 OK created sql table model marts.high_value_customers
2 of 3 OK created sql table model marts.trip_enriched
3 of 3 OK created sql table model marts.trip_summary_per_hour
```
# Partie 4 : analyse par notebook marimo des visuels sur les tables analytiques


# Conclusion

