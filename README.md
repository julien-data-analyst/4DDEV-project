# 4DDEV : création d'une architecture Data sur les voyages de taxis et la météo
## Auteur : Julien RENOULT - Tom JOUSSET - ...
## Promo : SUPINFO Programme Grande École 4ème année
### Spécialité : Ingénierie Data
### Date : 01/05/2026

# Pour lancer la phase dev

```sh

# Le lancer pour la première fois
docker compose --env-file .env -f docker-compose-dev.yaml up -d --build

# l'arrêter
docker compose -f docker-compose-dev.yaml down

# Avec suppression des volumes
docker compose -f docker-compose-dev.yaml down -v

# Relancer les conteneurs
docker compose -f docker-compose-dev.yaml up

```

Ca peut prendre pas mal de temps (10 à 15 minutes le temps de build et de démarrer le serveur d'airflow notamment).

Les autres conteneurs devraient être accesible rapidements.

Les inits sert pouvoir bien initialiser nos différents buckets/packages qu'on a besoin.

Marimo servira de notebook à la place de Jupyter qui est beaucoup trop chiant à configurer sous docker.

dbt sera dans un conteneur séparé pour créer les différents modèles. 

Il faudra donc se connecter au terminal du conteneur et tester les modèles dbt dans ce conteneur directement.

# Partie 1 : collecte des données

Utilisation de la librairie request pour récolter les différentes données.

Pour l'API de la météo notamment, récupérer des données historiques passées de janvier 2024 à mars 2026, les conserver directement 
dans le repo afin de ne pas les refaire
et seulement pour la partie actuelle d'utiliser le streaming.

Deux solution :
- données fictives en se basant sur le format de l'API (à créer avec un script Python)
- données historiques mais faut faire une demande (déjà fait en attente de réponse)

Pour le parquet, juste requête pour télécharger et les envoyer dans le datalake

# Partie 2 : transformation des données (DAGs Airflow pyspark)

# Partie 3 : modèles dbt pour l'analytics

# Partie 4 : analyse par notebook marimo des visuels sur les tables analytiques

# Conclusion