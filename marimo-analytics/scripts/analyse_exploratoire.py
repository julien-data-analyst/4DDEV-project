import marimo

__generated_with = "0.23.4"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Analyse exploratoire des voyages de taxis et de l'influence de la météo
    ## Auteur : Julien RENOULT - Tom JOUSSET - Béatrice BEAVOGUI - Mamadou-alpha DIALLO
    ## Promo : SUPINFO Programme Grande École 4ème année
    ### Spécialité : Ingénierie Data
    ### Date : 01/05/2026 - 08/06/2026

    Dans ce notebook, vous trouverez les réponses aux questions posées dans le projet sur notamment :

    Spark
    - Quelle est la distribution des durées de trajets ?
    - Les longs trajets reçoivent-ils plus de pourboires ?
    - Quelles sont les heures de prise en charge les plus chargées ?
    - Existe-t-il une corrélation entre la distance du trajet et le pourcentage de pourboire ?


    Spark Streaming ou Flink
    - Quelle est la température moyenne lors des pics de trajets ?
    - Quel est l’impact du vent ou de la pluie sur le nombre de trajets ?

    dbt / Analyse
    - Quels comportements de trajets observe-t-on selon les types de météo ?
    - À quelle heure observe-t-on le plus de clients à haute valeur ?
    - La météo influence-t-elle le comportement en matière de pourboires ?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Initialisation de la connexion à la base de données PostgreSQL
    """)
    return


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import os
    import plotnine as plt
    from sqlalchemy import create_engine

    return create_engine, mo, os, pd, plt


@app.cell
def _(os):
    # Propriétés de la connexion à la bdd
    connection_properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "db" : os.getenv("POSTGRES_DB"),
        "port" : 5432,
        "host" : "postgres"
    }
    return (connection_properties,)


@app.cell
def _(connection_properties):
    # URL SQLAlchemy avec psycopg2
    DATABASE_URL = (
        f"postgresql+psycopg2://{connection_properties['user']}:{connection_properties['password']}@{connection_properties['host']}:{connection_properties['port']}/{connection_properties['db']}"
    )
    return (DATABASE_URL,)


@app.cell
def _(DATABASE_URL, create_engine):
    # Création du moteur SQLAlchemy
    engine = create_engine(DATABASE_URL)
    return (engine,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Partie **Spark**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## *Quelle est la distribution des durées de trajets (en 2026) ?*
    """)
    return


@app.cell
def _():
    # Requête pour calculer la distribution des durées de trajets
    query = """
    WITH 
    stats AS (
        SELECT
            MIN(trip_duration_min) AS min_val,
            MAX(trip_duration_min) AS max_val
        FROM public.fact_taxi_trips
    ),
    hist AS (
        SELECT
            width_bucket(trip_duration_min, min_val, max_val, 50) AS bucket,
            COUNT(*) AS nb
        FROM public.fact_taxi_trips, stats
        WHERE trip_duration_min IS NOT NULL
        GROUP BY bucket
    )
    SELECT
        bucket,
        nb,
        nb::float / SUM(nb) OVER () AS density
    FROM hist;
    """
    return (query,)


@app.cell
def _(engine, pd, query):
    # Exécution de la requête
    df_trajets = pd.read_sql(query, engine)
    return (df_trajets,)


@app.cell
def _(df_trajets):
    # Vérification de l'exécution 
    print(df_trajets.dtypes)
    print("Nombre de lignes/colonnes DataFrame : ", df_trajets.shape)
    df_trajets.head(5)
    return


@app.cell
def _(df_trajets, plt):
    (
        plt.ggplot(df_trajets, plt.aes(x="bucket", y="nb"))
        + plt.geom_bar(stat="identity")
        + plt.scale_y_log10()
        + plt.labs(
            title="Distribution de la durée des trajets (log scale)",
            x="Bucket",
            y="Nombre de trajets (log)"
        )
        + plt.theme_minimal()
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    On peut observer que la majorité des trajets en taxis se fait dans des durées très courtes (0 à 20 minutes).

    Pour mieux analyser cet axe, on se propose de compter via une tranche de durée :
    - 0 à 20 minutes
    - 20 à 40 minutes
    - 40 à 60 minutes
    - \>=60 minutes
    """)
    return


@app.cell
def _():
    # Requête pour calculer la distribution des trajets selon la tranche définie au-dessus
    query_tranche_duree = """
        SELECT
        CASE
            WHEN trip_duration_min < 20 THEN '0-20 min'
            WHEN trip_duration_min < 40 THEN '20-40 min'
            WHEN trip_duration_min < 60 THEN '40-60 min'
            ELSE '>= 60 min'
        END AS tranche_duree,
        COUNT(*) AS nb_trajets
        FROM public.fact_taxi_trips
        WHERE trip_duration_min IS NOT NULL
        GROUP BY tranche_duree
        ORDER BY tranche_duree
    """
    return (query_tranche_duree,)


@app.cell
def _(engine, pd, query_tranche_duree):
    # Exécution de la requête
    df_tranche_trajets = pd.read_sql(query_tranche_duree, engine)
    return (df_tranche_trajets,)


@app.cell
def _(df_tranche_trajets):
    # Vérification de l'exécution 
    print(df_tranche_trajets.dtypes)
    print("Nombre de colonnes/lignes DataFrame : ", len(df_tranche_trajets.columns), "/", df_tranche_trajets.count())
    df_tranche_trajets.head(5)
    return


@app.cell
def _(df_tranche_trajets):
    # Conversion en pourcentage du nb tranche
    df_tranche_trajets["prct_trajets"] = round(df_tranche_trajets["nb_trajets"] / sum(df_tranche_trajets["nb_trajets"]) * 100, 2)
    return


@app.cell
def _(df_tranche_trajets):
    df_tranche_trajets.head()
    return


@app.cell
def _(df_tranche_trajets):
    liste_tranche = list(df_tranche_trajets["tranche_duree"])
    return (liste_tranche,)


@app.cell
def _(df_tranche_trajets, liste_tranche, plt):
    # Visuel en barre 
    (
        plt.ggplot(df_tranche_trajets, plt.aes(x="factor(tranche_duree)", y="prct_trajets")) # Jeu de données
            + plt.geom_bar(color="black",
                           fill="blue",
                           stat="identity") # Ajout du type de graphique avec couleur des bordures + barres
            + plt.labs(title="Répartition des trajets selon la durée",
                       x = "Tranche durée",
                       y = "Pourcentage") # Ajout des titres
             + plt.geom_text(
                plt.aes(label="prct_trajets"),  # new
                position=plt.position_dodge(width=0.9),
                size=8,
                va="bottom",
                format_string="{}%"
            )
            + plt.theme_bw() # Thème utilisé
            + plt.scale_x_discrete(limits=liste_tranche)
            + plt.theme(figure_size=(10, 4), 
                        panel_grid= plt.element_blank()) # Enlever les carreaux gris
        )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Commentaire :**

    Rien qu'avec une répartition par tranche nous montre que les voyages en taxis durent moins de 20 minutes pour **83,33%** d'entre eux suivies de loin par **16,67%** des trajets qui durent entre 20 et 40 minutes.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## *Les longs trajets reçoivent-ils plus de pourboires ?*
    """)
    return


@app.cell
def _():
    # Requête pour calculer le nombre de trajets selon la tranche de distance 
    # et si un pourboire a été offert
    query_tranche_distance_pourboire = """
        SELECT
        tranche_trip_distance_km,
        COUNT(id_taxi) AS nb_trajets
        FROM public.fact_taxi_trips
        WHERE tip_amount > 0
        GROUP BY tranche_trip_distance_km
        ORDER BY tranche_trip_distance_km
    """
    return (query_tranche_distance_pourboire,)


@app.cell
def _(engine, pd, query_tranche_distance_pourboire):
    # Exécution de la requête
    df_tranche_distance_pourboire = pd.read_sql(query_tranche_distance_pourboire, engine)
    return (df_tranche_distance_pourboire,)


@app.cell
def _(df_tranche_distance_pourboire):
    # Vérification de l'exécution 
    print(df_tranche_distance_pourboire.dtypes)
    print("Nombre de colonnes/lignes DataFrame : ", df_tranche_distance_pourboire.shape)
    df_tranche_distance_pourboire.head(5)
    return


@app.cell
def _(df_tranche_distance_pourboire):
    # Conversion en pourcentage
    df_tranche_distance_pourboire["prct_trajets"] = round(df_tranche_distance_pourboire["nb_trajets"] / sum(df_tranche_distance_pourboire["nb_trajets"]) * 100, 2)
    return


@app.cell
def _(df_tranche_distance_pourboire):
    df_tranche_distance_pourboire
    return


@app.cell
def _(df_tranche_distance_pourboire):
    liste_tranche_pourboire = list(df_tranche_distance_pourboire["tranche_trip_distance_km"])
    return (liste_tranche_pourboire,)


@app.cell
def _(df_tranche_distance_pourboire, liste_tranche_pourboire, plt):
    # Visuel en barre 
    (
        plt.ggplot(df_tranche_distance_pourboire, plt.aes(x="factor(tranche_trip_distance_km)", y="prct_trajets")) # Jeu de données
            + plt.geom_bar(color="black",
                           fill="blue",
                           stat="identity") # Ajout du type de graphique avec couleur des bordures + barres
            + plt.labs(title="Répartition des trajets selon la distance",
                       x = "Tranche distance trajet",
                       y = "Pourcentage") # Ajout des titres
             + plt.geom_text(
                plt.aes(label="prct_trajets"),  # new
                position=plt.position_dodge(width=0.9),
                size=8,
                va="bottom",
                format_string="{}%"
            )

            + plt.theme_bw() # Thème utilisé
            + plt.scale_x_discrete(limits=liste_tranche_pourboire)
            + plt.theme(figure_size=(10, 4), 
                        panel_grid= plt.element_blank()) # Enlever les carreaux gris
        )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Commentaire :**

    Comme on peut le voir ci-dessus, les longs trajets ne rapportent pas forcément plus de pourboires. C'est même l'inverse, Il y a moins de pourboire par rapport aux autres avec une différence supérieure à plus de **10%**.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## *Quelles sont les heures de prise en charge les plus chargées ?*
    """)
    return


@app.cell
def _():
    # Requête pour avoir les 10 heures où le nombre de trajets est le plus élevé
    query_top_10_heures_chargees = """
        SELECT
        pickup_hour,
        COUNT(id_taxi) AS nb_trajets
        FROM fact_taxi_trips
        GROUP BY pickup_hour
        ORDER BY nb_trajets DESC
        LIMIT 10
    """
    return (query_top_10_heures_chargees,)


@app.cell
def _(engine, pd, query_top_10_heures_chargees):
    # Exécution de la requête
    df_top_10_heures = pd.read_sql(query_top_10_heures_chargees, engine)
    return (df_top_10_heures,)


@app.cell
def _(df_top_10_heures):
    # Vérification de l'exécution 
    print(df_top_10_heures.dtypes)
    print("Nombre de colonnes/lignes DataFrame : ", df_top_10_heures.shape)
    df_top_10_heures.head(10)
    return


@app.cell
def _(df_top_10_heures):
    liste_top_10_heures = list(df_top_10_heures["pickup_hour"])
    return (liste_top_10_heures,)


@app.cell
def _(df_top_10_heures, liste_top_10_heures, plt):
    # Visuel en barre 
    (
        plt.ggplot(df_top_10_heures, plt.aes(x="factor(pickup_hour)", y="nb_trajets")) # Jeu de données
            + plt.geom_bar(color="black",
                           fill="blue",
                           stat="identity") # Ajout du type de graphique avec couleur des bordures + barres
            + plt.labs(title="Top 10 des heures de prises en charges les plus chargées",
                       x = "Heure de prise en charge",
                       y = "Effectif") # Ajout des titres
             + plt.geom_text(
                plt.aes(label="nb_trajets"),  # new
                position=plt.position_dodge(width=0.9),
                size=8,
                va="bottom"
            )
            + plt.coord_cartesian(ylim=(300000, None))
            + plt.theme_bw() # Thème utilisé
            + plt.scale_x_discrete(limits=liste_top_10_heures)
            + plt.theme(figure_size=(10, 4), 
                        panel_grid= plt.element_blank()) # Enlever les carreaux gris
        )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Commentaire :**

    On peut observer que le top 10 des heures de prises en charges les plus chargées correspondent à des heures de l'après-midi et de début de soirée. Dans le podium, nous retrouvons pour **564 293** trajets l'heure de prise en charge de 18 heure suivies de près par 17 heure et 16 heure.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## *Existe-t-il une corrélation entre la distance du trajet et le pourcentage de pourboire ?*
    """)
    return


@app.cell
def _():
    query_indicateurs_quantitatives_pourboire = """
    SELECT
        tranche_trip_distance_km,

        MIN(prct_pourboire) AS min_pourboire,
        MAX(prct_pourboire) AS max_pourboire,
        AVG(prct_pourboire) AS moyenne_pourboire,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY prct_pourboire) AS q1,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY prct_pourboire) AS mediane,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY prct_pourboire) AS q3

    FROM public.fact_taxi_trips
    WHERE prct_pourboire IS NOT NULL
    GROUP BY tranche_trip_distance_km
    ORDER BY tranche_trip_distance_km
    """
    return (query_indicateurs_quantitatives_pourboire,)


@app.cell
def _(engine, pd, query_indicateurs_quantitatives_pourboire):
    # Exécution de la requête
    df_indicateurs_quantitatives_pourboire = pd.read_sql(query_indicateurs_quantitatives_pourboire, engine)
    return (df_indicateurs_quantitatives_pourboire,)


@app.cell
def _(df_indicateurs_quantitatives_pourboire):
    # Vérification de l'exécution 
    print(df_indicateurs_quantitatives_pourboire.dtypes)
    print("Nombre de colonnes/lignes DataFrame : ", df_indicateurs_quantitatives_pourboire.shape)
    df_indicateurs_quantitatives_pourboire.head(5)
    return


@app.cell
def _():
    # Calcul de la corrélation linéaire de Pearson afin de voir s'il y a une corrélation linéaire
    query_pearson_distance_pourboire = """
    SELECT
        CORR(prct_pourboire, trip_distance_km) AS corr_pearson
    FROM public.fact_taxi_trips
    WHERE prct_pourboire IS NOT NULL
      AND trip_distance_km IS NOT NULL
    """
    return (query_pearson_distance_pourboire,)


@app.cell
def _(engine, pd, query_pearson_distance_pourboire):
    # Exécution de la requête
    pd.read_sql(query_pearson_distance_pourboire, engine).head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Commentaire :**

    On peut observer des erreurs au niveau du pourcentage pour les trois tranches avec le pourcentage maximale supérieure à 100%.

    À observer sur les lignes concernées pourquoi on a plus de 100%.

    Pour le reste, on peut observer que pour chaque tranche, le pourcentage de pourboire varie assez peu à l'exception de ceux qui ont une distance de plus de 5 kilomètres.

    On peut observer par contre que le pourcentage de pourboire avec les indicateurs de moyenne, médiane et quantiles baisse au fur et à mesure que le trajet soit plus long.

    Pour regarder la force de cette relation négative linéaire, on a calculé la corrélation de Pearson et on observe qu'il y a une relation linéaire très légèrement négative, voir quasi inexistante.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Partie Spark Streaming**

    Attention : vu que les données météos fictives, il n'y aura pas d'interprétation véritable à faire sur les graphiques/indicateurs qui va suivre.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## *Quelle est la température moyenne lors des pics de trajets ?*
    """)
    return


@app.cell
def _():
    # Récupérer la température moyenne pour les heures les plus chargés
    query_top_10_heures_chargees_moyenne_temperature = """
        SELECT
        f.pickup_hour,
        COUNT(f.id_taxi) AS nb_trajets,
        AVG(d.temp_celsius) AS moyenne_temperature
        FROM fact_taxi_trips f
        LEFT JOIN dim_weather d
        ON d.measure_hour = f.pickup_hour
        GROUP BY f.pickup_hour
        ORDER BY nb_trajets DESC
        LIMIT 10
    """
    return (query_top_10_heures_chargees_moyenne_temperature,)


@app.cell
def _(engine, pd, query_top_10_heures_chargees_moyenne_temperature):
    # Exécution de la requête
    df_top_10_heures_temperature = pd.read_sql(query_top_10_heures_chargees_moyenne_temperature, engine)
    return (df_top_10_heures_temperature,)


@app.cell
def _(df_top_10_heures_temperature):
    # Vérification de l'exécution 
    print(df_top_10_heures_temperature.dtypes)
    print("Nombre de colonnes/lignes DataFrame : ", df_top_10_heures_temperature.shape)
    df_top_10_heures_temperature.head(10)
    return


@app.cell
def _(df_top_10_heures_temperature, liste_top_10_heures, plt):
    # Visuel en barre 
    (
        plt.ggplot(df_top_10_heures_temperature, plt.aes(x="factor(pickup_hour)", y="moyenne_temperature")) # Jeu de données
            + plt.geom_bar(color="black",
                           fill="blue",
                           stat="identity") # Ajout du type de graphique avec couleur des bordures + barres
            + plt.labs(title="Top 10 des heures de prises en charges les plus chargées avec leur moyenne de température",
                       x = "Heure de prise en charge",
                       y = "Température (C°)") # Ajout des titres
             + plt.geom_text(
                plt.aes(label="moyenne_temperature"),  # new
                position=plt.position_dodge(width=0.9),
                size=8,
                va="bottom",
                format_string="{:.2f}°C"
            )
            + plt.coord_cartesian(ylim=(5, None))
            + plt.theme_bw() # Thème utilisé
            + plt.scale_x_discrete(limits=liste_top_10_heures)
            + plt.theme(figure_size=(10, 4), 
                        panel_grid= plt.element_blank()) # Enlever les carreaux gris
        )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## *Quel est l’impact du vent ou de la pluie sur le nombre de trajets ?*
    """)
    return


@app.cell
def _():
    # Récupérer pour chaque date et heure, le nombre de trajets, l'humidité et la vitesse du vent
    query_vent_pluie_trajet = """
        SELECT
        f.pickup_date,
        f.pickup_hour,
        COUNT(f.id_taxi) AS nb_trajets,
        AVG(d.humidity_pct) AS moyenne_humidite_pct,
        AVG(d.wind_speed_ms) AS moyenne_vitesse_vent

        FROM fact_taxi_trips f
        LEFT JOIN dim_weather d
        ON d.measure_hour = f.pickup_hour
        GROUP BY f.pickup_hour, f.pickup_date
    """
    return (query_vent_pluie_trajet,)


@app.cell
def _(engine, pd, query_vent_pluie_trajet):
    # Exécution de la requête
    df_vent_pluie = pd.read_sql(query_vent_pluie_trajet, engine)
    return (df_vent_pluie,)


@app.cell
def _(df_vent_pluie):
    # Vérification de l'exécution 
    print(df_vent_pluie.dtypes)
    print("Nombre de lignes/colonnes DataFrame : ", df_vent_pluie.shape)
    df_vent_pluie.head(10)
    return


@app.cell
def _(df_vent_pluie):
    # Calcul des corrélations de Pearson avec pandas
    corr_humidite = df_vent_pluie["nb_trajets"].corr(
        df_vent_pluie["moyenne_humidite_pct"]
    )

    corr_vent = df_vent_pluie["nb_trajets"].corr(
        df_vent_pluie["moyenne_vitesse_vent"]
    )

    return corr_humidite, corr_vent


@app.cell
def _(corr_humidite, corr_vent):
    print(f"Corrélation humidité : {corr_humidite}")
    print(f"Corrélation vent : {corr_vent}")
    return


@app.cell
def _(df_vent_pluie, plt):
    (
        plt.ggplot(df_vent_pluie, plt.aes(x="moyenne_humidite_pct", y="nb_trajets"))
        + plt.geom_point(alpha=0.6)
        + plt.geom_smooth(method="lm", se=True)  # régression linéaire
        + plt.labs(
            title="Relation entre humidité et nombre de trajets",
            x="Humidité moyenne (%)",
            y="Nombre de trajets"
        )
        + plt.theme_bw() # Thème utilisé
        + plt.theme(figure_size=(10, 4), 
                        panel_grid= plt.element_blank()) # Enlever les carreaux gris
    )
    return


@app.cell
def _(df_vent_pluie, plt):
    (
        plt.ggplot(df_vent_pluie, plt.aes(x="moyenne_vitesse_vent", y="nb_trajets"))
        + plt.geom_point(alpha=0.6)
        + plt.geom_smooth(method="lm", se=True)  # régression linéaire
        + plt.labs(
            title="Relation entre vitesse du vent et nombre de trajets",
            x="Vitesse du vent en moyenne (MS)",
            y="Nombre de trajets"
        )
        + plt.theme_bw() # Thème utilisé
        + plt.theme(figure_size=(10, 4), 
                        panel_grid= plt.element_blank()) # Enlever les carreaux gris
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Partie DBT/Analyse**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## *Quels comportements de trajets observe-t-on selon les types de météo ?*
    """)
    return


@app.cell
def _():
    # Récupérer les calculs réalisées dans la table de dimension trip_summary_per_hour
    query_trip_summary_per_hour = """
        SELECT *
        FROM marts.trip_summary_per_hour
    """
    return (query_trip_summary_per_hour,)


@app.cell
def _(engine, pd, query_trip_summary_per_hour):
    # Exécution de la requête
    df_distance_temps = pd.read_sql(query_trip_summary_per_hour, engine)
    return (df_distance_temps,)


@app.cell
def _(df_distance_temps):
    # Vérification de l'exécution 
    print(df_distance_temps.dtypes)
    print("Nombre de lignes/colonnes DataFrame : ", df_distance_temps.shape)
    df_distance_temps.head(10)
    return


@app.cell
def _(df_distance_temps, plt):
    # Le nombre de trajets selon l'heure de prise en charge et la météo
    (
        plt.ggplot(df_distance_temps, plt.aes(x="pickup_hour", y="trips_count"))
         + plt.geom_bar(color="black",
                           fill="blue",
                           stat="identity") # Ajout du type de graphique avec couleur des bordures + barres
        + plt.labs(
            title="Nombre de trajets selon l'heure de prise en charge par météo",
            x="Heure de la prise en charge",
            y="Nombre de trajets"
        )
        + plt.facet_wrap(
            "weather_description",
            nrow=4,  # change the number of columns
        )
         + plt.geom_text(
                plt.aes(label="trips_count"),  # new
                position=plt.position_dodge(width=0.9),
                size=8,
                va="bottom"
            )
        + plt.theme_bw() # Thème utilisé
        + plt.theme(figure_size=(15, 10), 
                        panel_grid= plt.element_blank()) # Enlever les carreaux gris
    )
    return


@app.cell
def _(df_distance_temps, plt):
    # La durée moyenne du trajet en minute selon l'heure de prise en charge et la météo
    (
        plt.ggplot(df_distance_temps, plt.aes(x="pickup_hour", y="avg_trip_duration_min"))
         + plt.geom_bar(color="black",
                           fill="blue",
                           stat="identity") # Ajout du type de graphique avec couleur des bordures + barres
        + plt.labs(
            title="Durée en moyenne du trajet selon l'heure de prise en charge par météo",
            x="Heure de la prise en charge",
            y="Moyenne de la durée du trajet (min)"
        )
        + plt.facet_wrap(
            "weather_description",
            nrow=4,  # change the number of columns
        )
         + plt.geom_text(
                plt.aes(label="avg_trip_duration_min"),  # new
                position=plt.position_dodge(width=0.9),
                size=7.5,
                va="bottom",
                format_string="{:.2f}min"
            )
        + plt.theme_bw() # Thème utilisé
        + plt.theme(figure_size=(15, 10), 
                        panel_grid= plt.element_blank()) # Enlever les carreaux gris
    )
    return


@app.cell
def _(df_distance_temps, plt):
    # La durée moyenne du trajet en minute selon l'heure de prise en charge et la météo
    (
        plt.ggplot(df_distance_temps, plt.aes(x="pickup_hour", y="avg_tip_amount"))
         + plt.geom_bar(color="black",
                           fill="blue",
                           stat="identity") # Ajout du type de graphique avec couleur des bordures + barres
        + plt.labs(
            title="Montant du pourboire moyen du trajet selon l'heure de prise en charge par météo",
            x="Heure de la prise en charge",
            y="Moyenne du pourboire ($)"
        )
        + plt.facet_wrap(
            "weather_description",
            nrow=4,  # change the number of columns
        )
         + plt.geom_text(
                plt.aes(label="avg_tip_amount"),  # new
                position=plt.position_dodge(width=0.9),
                size=7.5,
                va="bottom",
                format_string="{:.2f}$"
            )
        + plt.theme_bw() # Thème utilisé
        + plt.theme(figure_size=(15, 10), 
                        panel_grid= plt.element_blank()) # Enlever les carreaux gris
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## *À quelle heure observe-t-on le plus de clients à haute valeur ?*
    """)
    return


@app.cell
def _():
    # Requête pour récupérer les indicateurs numériques de passenger_count
    query_high_value_customers = """
        SELECT *
        FROM marts.high_value_customers
    """
    return (query_high_value_customers,)


@app.cell
def _(engine, pd, query_high_value_customers):
    # Exécution de la requête
    pd.read_sql(query_high_value_customers, engine).head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## *La météo influence-t-elle le comportement en matière de pourboires ?*
    """)
    return


@app.cell
def _():
    # Requête pour récupérer les indicateurs numériques de passenger_count
    query_pourboire_meteo = """
        SELECT 
            weather_description,
            AVG(prct_pourboire) AS moyenne_pourboire_prct,
            AVG(tip_amount) AS moyenne_pourboire,
            COUNT(CASE WHEN tip_amount > 0 THEN 1 ELSE NULL END) AS nb_pourboire
        FROM marts.trip_enriched
        WHERE weather_description IS NOT NULL
        GROUP BY weather_description
    """
    return (query_pourboire_meteo,)


@app.cell
def _(engine, pd, query_pourboire_meteo):
    # Exécution de la requête
    df_pourboire_meteo = pd.read_sql(query_pourboire_meteo, engine)
    return (df_pourboire_meteo,)


@app.cell
def _(df_pourboire_meteo):
    # Vérification de l'exécution 
    print(df_pourboire_meteo.dtypes)
    print("Nombre de colonnes/lignes DataFrame : ", len(df_pourboire_meteo.columns), "/", df_pourboire_meteo.count())
    df_pourboire_meteo.head(10)
    return


if __name__ == "__main__":
    app.run()
