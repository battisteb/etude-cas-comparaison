"""Étude de cas : accidents de la route en Australie-Méridionale (2012-2019) avec PySpark.

Reprend l'étude « Big Data Analysis of Road Crash Data using PySpark » (Rakesh Nain) : chargement des deux
jeux de données (Crash = un accident par ligne, Units = un véhicule / usager impliqué par ligne), puis les mêmes
requêtes écrites avec les trois API de Spark (RDD, DataFrame, SQL), afin de vérifier qu'elles donnent le même résultat.
"""
import csv
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import IntegerType  # noqa: E402

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(RACINE, "data")

# Colonnes (ordre des fichiers CSV), utiles pour l'API RDD qui manipule des listes
CRASH_COLS = ["REPORT_ID", "Stats Area", "Suburb", "Postcode", "LGA Name", "Total Units", "Total Cas", "Total Fats",
              "Total SI", "Total MI", "Year", "Month", "Day", "Time", "Area Speed", "Position Type", "Horizontal Align",
              "Vertical Align", "Other Feat", "Road Surface", "Moisture Cond", "Weather Cond", "DayNight", "Crash Type",
              "Unit Resp", "Entity Code", "CSEF Severity", "Traffic Ctrls", "DUI Involved", "Drugs Involved",
              "ACCLOC_X", "ACCLOC_Y", "UNIQUE_LOC"]
UNITS_COLS = ["REPORT_ID", "Unit No", "No Of Cas", "Veh Reg State", "Unit Type", "Veh Year", "Direction Of Travel",
              "Sex", "Age", "Lic State", "Licence Class", "Licence Type", "Towing", "Unit Movement",
              "Number Occupants", "Postcode", "Rollover", "Fire"]
C = {nom: i for i, nom in enumerate(CRASH_COLS)}
U = {nom: i for i, nom in enumerate(UNITS_COLS)}

SEVERITES = ["1: PDO", "2: MI", "3: SI", "4: Fatal"]
SCENARIOS = ["Drogue", "Alcool", "Drogue + alcool", "Ni l'un ni l'autre"]


def creer_session(master="local[*]", nom="road-crash"):
    spark = (SparkSession.builder.master(master).appName(nom)
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    # les fonctions de ce module sont utilisées dans les RDD : les executors doivent pouvoir l'importer
    spark.sparkContext.addPyFile(os.path.abspath(__file__))
    return spark


# ----------------------------------------------------------------------------------------------------------
# Chargement
# ----------------------------------------------------------------------------------------------------------
def parser_csv(ligne):
    """Découpe une ligne CSV en respectant les guillemets ("CURVED, VIEW OPEN" reste un seul champ)."""
    return next(csv.reader([ligne]))


def parser_naif(ligne):
    """Découpage de l'article : split(',') puis suppression des guillemets — faux si un champ contient une virgule."""
    return [v.replace('"', "") for v in ligne.split(",")]


def charger_rdd(spark, dossier=DATA):
    """RDD de lignes brutes (sans en-tête) pour Units et Crash."""
    sc = spark.sparkContext
    rdds = []
    for sous_dossier in ["units_data", "crash_data"]:
        rdd = sc.textFile(os.path.join(dossier, sous_dossier, "*.csv"))
        entete = rdd.first()
        rdds.append(rdd.filter(lambda ligne, e=entete: ligne != e))   # chaque fichier annuel a le même en-tête
    return tuple(rdds)


def charger_df(spark, dossier=DATA):
    """DataFrames Units et Crash, avec les colonnes numériques typées."""
    lire = lambda d: (spark.read.option("header", True).option("escape", '"')
                      .csv(os.path.join(dossier, d, "*.csv")))
    units = lire("units_data")
    crash = lire("crash_data")
    for col in ["Total Units", "Total Cas", "Total Fats", "Total SI", "Total MI", "Year"]:
        crash = crash.withColumn(col, F.col(col).cast(IntegerType()))
    units = units.withColumn("No Of Cas", F.col("No Of Cas").cast(IntegerType()))
    return units, crash


def enregistrer_vues(units_df, crash_df):
    units_df.createOrReplaceTempView("units")
    crash_df.createOrReplaceTempView("crash")


def age_valide(age):
    return age is not None and age.strip().isdigit()


# ----------------------------------------------------------------------------------------------------------
# Requête 1 : âge moyen des conducteurs/usagers impliqués, par sexe
# ----------------------------------------------------------------------------------------------------------
def age_moyen_rdd(units_rdd):
    return dict(units_rdd.map(parser_csv)
                .filter(lambda r: r[U["Sex"]] in ("Male", "Female") and age_valide(r[U["Age"]]))
                .map(lambda r: (r[U["Sex"]], (int(r[U["Age"]]), 1)))
                .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                .mapValues(lambda s: round(s[0] / s[1], 2))
                .collect())


def age_moyen_df(units_df):
    res = (units_df.filter(F.col("Sex").isin("Male", "Female") & F.col("Age").rlike(r"^\d+$"))
           .groupBy("Sex").agg(F.round(F.avg(F.col("Age").cast("int")), 2).alias("age_moyen")))
    return {r["Sex"]: r["age_moyen"] for r in res.collect()}


def age_moyen_sql(spark):
    res = spark.sql(r"""
        SELECT Sex, ROUND(AVG(CAST(Age AS INT)), 2) AS age_moyen
        FROM units
        WHERE Sex IN ('Male', 'Female') AND Age RLIKE '^[0-9]+$'
        GROUP BY Sex""")
    return {r["Sex"]: r["age_moyen"] for r in res.collect()}


# ----------------------------------------------------------------------------------------------------------
# Requête 2 : victimes par quartier (suburb) quand un conducteur sans permis est impliqué
# On compte chaque accident une seule fois, même si plusieurs véhicules impliqués sont sans permis.
# ----------------------------------------------------------------------------------------------------------
def sans_permis_rdd(units_rdd, crash_rdd):
    ids = (units_rdd.map(parser_csv).filter(lambda r: r[U["Licence Type"]] == "Unlicenced")
           .map(lambda r: (r[U["REPORT_ID"]], None)).distinct())
    accidents = crash_rdd.map(parser_csv).map(lambda r: (r[C["REPORT_ID"]], (r[C["Suburb"]], int(r[C["Total Cas"]]))))
    return dict(accidents.join(ids)
                .map(lambda kv: kv[1][0])
                .reduceByKey(lambda a, b: a + b)
                .collect())


def sans_permis_df(units_df, crash_df):
    ids = units_df.filter(F.col("Licence Type") == "Unlicenced").select("REPORT_ID").distinct()
    res = crash_df.join(ids, "REPORT_ID").groupBy("Suburb").agg(F.sum("Total Cas").alias("victimes"))
    return {r["Suburb"]: r["victimes"] for r in res.collect()}


def sans_permis_sql(spark):
    res = spark.sql("""
        SELECT c.Suburb, SUM(c.`Total Cas`) AS victimes
        FROM crash c
        WHERE c.REPORT_ID IN (SELECT REPORT_ID FROM units WHERE `Licence Type` = 'Unlicenced')
        GROUP BY c.Suburb""")
    return {r["Suburb"]: r["victimes"] for r in res.collect()}


# ----------------------------------------------------------------------------------------------------------
# Requête 3 : répartition des accidents par gravité selon drogue / alcool (en % de chaque scénario)
# ----------------------------------------------------------------------------------------------------------
def _scenario(dui, drogue):
    dui, drogue = dui == "Y", drogue == "Y"
    if dui and drogue:
        return ["Drogue", "Alcool", "Drogue + alcool"]
    if drogue:
        return ["Drogue"]
    if dui:
        return ["Alcool"]
    return ["Ni l'un ni l'autre"]


def gravite_scenarios_rdd(crash_rdd):
    """{(scénario, gravité): nombre d'accidents}. Un accident positif aux deux compte dans 3 scénarios (comme l'article)."""
    return dict(crash_rdd.map(parser_csv)
                .flatMap(lambda r: [((s, r[C["CSEF Severity"]]), 1)
                                    for s in _scenario(r[C["DUI Involved"]], r[C["Drugs Involved"]])])
                .reduceByKey(lambda a, b: a + b)
                .collect())


def _colonne_scenarios():
    dui, drogue = F.col("DUI Involved") == "Y", F.col("Drugs Involved") == "Y"
    dui, drogue = F.coalesce(dui, F.lit(False)), F.coalesce(drogue, F.lit(False))
    return F.filter(F.array(F.when(drogue, F.lit("Drogue")),
                            F.when(dui, F.lit("Alcool")),
                            F.when(dui & drogue, F.lit("Drogue + alcool")),
                            F.when(~dui & ~drogue, F.lit("Ni l'un ni l'autre"))),
                    lambda x: x.isNotNull())


def gravite_scenarios_df(crash_df):
    """DataFrame (scénario, gravité, accidents, pourcentage) ; pourcentage calculé par une fenêtre, sans UDF."""
    par_scenario = Window.partitionBy("scenario")
    return (crash_df.select(F.explode(_colonne_scenarios()).alias("scenario"), "CSEF Severity")
            .groupBy("scenario", "CSEF Severity").agg(F.count("*").alias("accidents"))
            .withColumn("pourcentage", F.round(100 * F.col("accidents") / F.sum("accidents").over(par_scenario), 2)))


def gravite_scenarios_sql(spark):
    return spark.sql("""
        WITH s AS (
            SELECT `CSEF Severity` AS severite, scenario
            FROM crash
            LATERAL VIEW explode(filter(array(
                CASE WHEN `Drugs Involved` = 'Y' THEN 'Drogue' END,
                CASE WHEN `DUI Involved` = 'Y' THEN 'Alcool' END,
                CASE WHEN `DUI Involved` = 'Y' AND `Drugs Involved` = 'Y' THEN 'Drogue + alcool' END,
                CASE WHEN `DUI Involved` IS NULL AND `Drugs Involved` IS NULL THEN "Ni l'un ni l'autre" END),
                x -> x IS NOT NULL)) t AS scenario
        )
        SELECT scenario, severite AS `CSEF Severity`, COUNT(*) AS accidents,
               ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY scenario), 2) AS pourcentage
        FROM s GROUP BY scenario, severite""")


def en_dict(df):
    return {(r["scenario"], r["CSEF Severity"]): r["accidents"] for r in df.collect()}


# ----------------------------------------------------------------------------------------------------------
# Requête 4 : détail des usagers impliqués dans les accidents du quartier ADELAIDE (jointure Crash x Units)
# Date au format Année-Mois-JourDeLaSemaine : le jour exact n'est pas publié (donnée sensible).
# ----------------------------------------------------------------------------------------------------------
def adelaide_rdd(units_rdd, crash_rdd):
    u = units_rdd.map(parser_csv).map(lambda r: (r[U["REPORT_ID"]], (r[U["Sex"]], r[U["Age"]], r[U["Licence Type"]])))
    c = (crash_rdd.map(parser_csv).filter(lambda r: r[C["Suburb"]] == "ADELAIDE")
         .map(lambda r: (r[C["REPORT_ID"]], (f'{r[C["Year"]]}-{r[C["Month"]]}-{r[C["Day"]]}', r[C["Time"]],
                                             int(r[C["Total Cas"]])))))
    return sorted(c.join(u).map(lambda kv: (kv[0],) + kv[1][0] + kv[1][1]).collect(), key=str)


def adelaide_df(units_df, crash_df):
    res = (crash_df.filter(F.col("Suburb") == "ADELAIDE")
           .join(units_df, "REPORT_ID")
           .select("REPORT_ID", F.concat_ws("-", "Year", "Month", "Day").alias("Date"), "Time", "Total Cas",
                   "Sex", "Age", "Licence Type"))
    return sorted([tuple("" if v is None else v for v in r) for r in res.collect()], key=str)


def adelaide_sql(spark):
    res = spark.sql("""
        SELECT c.REPORT_ID, concat_ws('-', c.Year, c.Month, c.Day) AS Date, c.Time, c.`Total Cas`,
               u.Sex, u.Age, u.`Licence Type`
        FROM crash c JOIN units u ON c.REPORT_ID = u.REPORT_ID
        WHERE c.Suburb = 'ADELAIDE'""")
    return sorted([tuple("" if v is None else v for v in r) for r in res.collect()], key=str)
