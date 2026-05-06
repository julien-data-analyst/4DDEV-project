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

Utilisation de la librairie request pour récolter les différentes données.

Partie année actuelle : streaming avec l'API
pour les mois et années précédentes : ajout de données fictives

Deux solution :
- données fictives en se basant sur le format de l'API (à créer avec un script Python)
- données historiques mais faut faire une demande (déjà fait en attente de réponse)

Pour le parquet, juste requête pour télécharger et les envoyer dans le datalake.

# Partie 2 : transformation des données (DAGs Airflow pyspark)

Taxis : Sélection, nettoyage et enfin création des colonnes calculées. Insertion dans la base de données.

Weather : Nettoyage et création de colonnes calculées. Insertion dans la base de données avec précision si ce sont des données fictives ou non.

# Partie 3 : modèles dbt pour l'analytics

18:37:00  1 of 3 OK created sql table model marts.high_value_customers ................... [SELECT 8 in 26.33s]
18:37:00  2 of 3 START sql table model marts.trip_enriched ............................... [RUN]
18:43:40  2 of 3 OK created sql table model marts.trip_enriched .......................... [SELECT 19324381 in 399.82s]
18:43:40  3 of 3 START sql table model marts.trip_summary_per_hour ....................... [RUN]
18:44:12  3 of 3 OK created sql table model marts.trip_summary_per_hour .................. [SELECT 96 in 31.68s]

# Partie 4 : analyse par notebook marimo des visuels sur les tables analytiques


# Conclusion

