"""Tests sur un mini jeu de données construit à la main : résultats attendus connus,
et les trois API (RDD, DataFrame, SQL) doivent donner exactement le même résultat."""
import csv
import os
import sys

import pytest

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RACINE, "src"))

import road_crash as rc  # noqa: E402


def accident(report_id, suburb, total_cas, severite, dui="", drogue="", lga="CITY OF ADELAIDE",
             align="Straight road", year="2019", month="June", day="Monday", time="10:00 am"):
    ligne = dict.fromkeys(rc.CRASH_COLS, "")
    ligne.update({"REPORT_ID": report_id, "Suburb": suburb, "LGA Name": lga, "Total Cas": str(total_cas),
                  "Total Units": "2", "Total Fats": "0", "Total SI": "0", "Total MI": "0", "Year": year,
                  "Month": month, "Day": day, "Time": time, "Horizontal Align": align, "CSEF Severity": severite,
                  "DUI Involved": dui, "Drugs Involved": drogue})
    return [ligne[c] for c in rc.CRASH_COLS]


def unite(report_id, no, sexe, age, licence):
    ligne = dict.fromkeys(rc.UNITS_COLS, "")
    ligne.update({"REPORT_ID": report_id, "Unit No": no, "No Of Cas": "0", "Sex": sexe, "Age": age,
                  "Licence Type": licence, "Veh Reg State": "SA"})
    return [ligne[c] for c in rc.UNITS_COLS]


ACCIDENTS = [
    accident("A1", "ADELAIDE", 2, "2: MI", lga="CC OF NORWOOD,PAYNEHAM & ST PETERS", align="CURVED, VIEW OPEN"),
    accident("A2", "ADELAIDE", 0, "1: PDO", dui="Y"),
    accident("A3", "PROSPECT", 3, "4: Fatal", dui="Y", drogue="Y"),
    accident("A4", "PROSPECT", 1, "3: SI", drogue="Y"),
]
UNITES = [
    unite("A1", "01", "Male", "030", "Unlicenced"),
    unite("A1", "02", "Female", "050", "Unlicenced"),      # 2 conducteurs sans permis dans le même accident
    unite("A2", "01", "Female", "XXX", "Full"),            # âge masqué : exclu de la moyenne
    unite("A3", "01", "Male", "020", "Unlicenced"),
    unite("A4", "01", "Unknown", "040", "Full"),           # sexe inconnu : exclu
    unite("A4", "02", "Male", "", "Learners"),             # âge vide : exclu
]


@pytest.fixture(scope="session")
def donnees(tmp_path_factory):
    dossier = tmp_path_factory.mktemp("data")
    for sous_dossier, nom, colonnes, lignes in [("crash_data", "2019_DATA_SA_Crash.csv", rc.CRASH_COLS, ACCIDENTS),
                                                ("units_data", "2019_DATA_SA_Units.csv", rc.UNITS_COLS, UNITES)]:
        os.makedirs(dossier / sous_dossier)
        with open(dossier / sous_dossier / nom, "w", newline="", encoding="utf-8") as f:
            csv.writer(f, quoting=csv.QUOTE_ALL).writerows([colonnes] + lignes)
    return str(dossier)


@pytest.fixture(scope="session")
def spark():
    session = rc.creer_session("local[2]", nom="tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def jeux(spark, donnees):
    u_rdd, c_rdd = rc.charger_rdd(spark, donnees)
    u_df, c_df = rc.charger_df(spark, donnees)
    rc.enregistrer_vues(u_df, c_df)
    return u_rdd, c_rdd, u_df, c_df


def test_chargement(jeux):
    u_rdd, c_rdd, u_df, c_df = jeux
    assert (u_rdd.count(), c_rdd.count(), u_df.count(), c_df.count()) == (6, 4, 6, 4)


def test_parseur_csv_respecte_les_guillemets(jeux):
    _, c_rdd, _, _ = jeux
    ligne_a1 = c_rdd.filter(lambda l: l.startswith('"A1"')).first()
    assert len(rc.parser_csv(ligne_a1)) == len(rc.CRASH_COLS)
    assert rc.parser_csv(ligne_a1)[rc.C["Total Cas"]] == "2"
    # le découpage naïf de l'article décale les colonnes (2 virgules en trop)
    assert len(rc.parser_naif(ligne_a1)) == len(rc.CRASH_COLS) + 2
    assert rc.parser_csv(ligne_a1)[rc.C["CSEF Severity"]] == "2: MI"
    assert rc.parser_naif(ligne_a1)[rc.C["CSEF Severity"]] != "2: MI"


def test_age_moyen(spark, jeux):
    u_rdd, _, u_df, _ = jeux
    attendu = {"Male": 25.0, "Female": 50.0}
    assert rc.age_moyen_rdd(u_rdd) == attendu
    assert rc.age_moyen_df(u_df) == attendu
    assert rc.age_moyen_sql(spark) == attendu


def test_sans_permis_un_accident_compte_une_fois(spark, jeux):
    u_rdd, c_rdd, u_df, c_df = jeux
    attendu = {"ADELAIDE": 2, "PROSPECT": 3}          # A1 compté une seule fois malgré ses 2 conducteurs sans permis
    assert rc.sans_permis_rdd(u_rdd, c_rdd) == attendu
    assert rc.sans_permis_df(u_df, c_df) == attendu
    assert rc.sans_permis_sql(spark) == attendu


def test_gravite_scenarios(spark, jeux):
    _, c_rdd, _, c_df = jeux
    attendu = {("Ni l'un ni l'autre", "2: MI"): 1,
               ("Alcool", "1: PDO"): 1,
               ("Drogue", "4: Fatal"): 1, ("Alcool", "4: Fatal"): 1, ("Drogue + alcool", "4: Fatal"): 1,
               ("Drogue", "3: SI"): 1}
    assert rc.gravite_scenarios_rdd(c_rdd) == attendu
    assert rc.en_dict(rc.gravite_scenarios_df(c_df)) == attendu
    assert rc.en_dict(rc.gravite_scenarios_sql(spark)) == attendu
    pourcentages = {(r["scenario"], r["CSEF Severity"]): r["pourcentage"] for r in rc.gravite_scenarios_df(c_df).collect()}
    assert pourcentages[("Drogue", "4: Fatal")] == 50.0 and pourcentages[("Alcool", "1: PDO")] == 50.0


def test_adelaide(spark, jeux):
    u_rdd, c_rdd, u_df, c_df = jeux
    resultat = rc.adelaide_rdd(u_rdd, c_rdd)
    assert len(resultat) == 3                                    # A1 (2 usagers) + A2 (1 usager)
    assert resultat[0][:3] == ("A1", "2019-June-Monday", "10:00 am")
    assert resultat == rc.adelaide_df(u_df, c_df) == rc.adelaide_sql(spark)
