# 4DDEV : création d'une architecture Data sur les voyages de taxis et la météo
## Auteur : Julien RENOULT - Tom JOUSSET - Béatrice BEAVOGUI - Mamadou-alpha DIALLO
## Promo : SUPINFO Programme Grande École 4ème année
### Spécialité : Ingénierie Data
### Date : 01/05/2026 - 05/06/2026

# Pour lancer la phase dev

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

Ca peut prendre pas mal de temps (10 à 15 minutes le temps de build et de démarrer le serveur d'airflow notamment).

Les autres conteneurs devraient être accesible rapidements.

Les inits sert pouvoir bien initialiser nos différents buckets/packages qu'on a besoin.

Marimo servira de notebook à la place de Jupyter qui est beaucoup trop complexe à configurer sous docker.

dbt sera dans un conteneur séparé pour créer les différents modèles. 

Il faudra donc se connecter au terminal du conteneur et tester les modèles dbt dans ce conteneur directement.

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


# Partie 4 : analyse par notebook marimo des visuels sur les tables analytiques


# Conclusion

