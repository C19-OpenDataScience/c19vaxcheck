#!/usr/bin/env python
import os
import re
import time
import shutil
import unidecode
import collections as col
import csv
from contextlib import contextmanager
import sqlite3
import click
import matplotlib.pyplot as plt
#from selenium.webdriver.chrome.options import Options

HERE = os.path.abspath(os.path.dirname(__file__))
#DOWNLOAD_DIR = os.path.join(HERE, '_download')
DATA_DIR = os.path.join(HERE, 'data')

#YEARS_RANGE=range(2010, 2021+1)

@click.group()
def main():
    pass

@main.command("all")
def cmd_all():
    pass
    #download_data()
    #import_data()
    # plot_reactions_by_year_c19()
    # plot_reactions_by_year_c19(severe=True)
    # plot_reactions_by_year_c19(death=True)


#def download_data():

    # civic_hospi
    # https://www.data.gouv.fr/fr/datasets/donnees-hospitalieres-relatives-a-lepidemie-de-covid-19/ -> donnees-hospitalieres-covid19-2021-12-20-19h06.csv
    # https://www.data.gouv.fr/fr/datasets/r/63352e38-d353-4b54-bfd1-f1b3ee1cabd7

    # civic_new_hospi
    # https://www.data.gouv.fr/fr/datasets/donnees-hospitalieres-relatives-a-lepidemie-de-covid-19/ -> covid-hosp-txad-fra-2021-12-20-19h06.csv
    # https://www.data.gouv.fr/fr/datasets/r/fe3e7099-a975-4181-9fb5-2dd1b8f1b552
    # https://www.data.gouv.fr/fr/datasets/donnees-hospitalieres-relatives-a-lepidemie-de-covid-19/ -> donnees-hospitalieres-nouveaux-covid19-2021-12-20-19h06.csv
    # https://www.data.gouv.fr/fr/datasets/r/6fadff46-9efd-4c53-942a-54aca783c30c 




@main.command("import_data")
def cmd_import_data():
    init_db()
    import_data()

def db_connect():
    return sqlite3.connect(os.path.join(HERE, "data.sqlite"))

def init_db():
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS drees_by_age(date text, vac_statut text, vac_statut_group text, age txt, nb_PCR real, nb_PCR_p real, HC real, HC_PCR_p real, SC real, SC_PCR_p real, effectif integer)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS drees_nat(date text, vac_statut text, vac_statut_group text, age txt, nb_PCR real, nb_PCR_sympt real, nb_PCR_p real, nb_PCR_p_sympt real, HC real, HC_PCR_p real, SC real, SC_PCR_p real, DC real, DC_PCR_p real, effectif integer)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS civic_hospi (date text, dep text, sexe text, hosp integer, rea integer, HospConv integer, deces integer)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS civic_new_hospi (date text, dep text, incid_hosp integer, incid_rea integer, incid_dc integer, incid_rad integer)''')
        cur.execute('''DELETE FROM drees_by_age''')
        cur.execute('''DELETE FROM drees_nat''')
        cur.execute('''DELETE FROM civic_hospi''')
        cur.execute('''DELETE FROM civic_new_hospi''')

def import_data():
    with db_connect() as conn:

        # drees par age
        dates = set()
        for data_fpath in (
            os.path.join(DATA_DIR, "drees-covid-19-resultats-par-age-issus-des-appariements-entre-si-vic-si-dep-et-vac-si.csv"),
            os.path.join(DATA_DIR, "drees-covid-19-anciens-resultats-par-age-issus-des-appariements-entre-si-vic-si-dep-et.csv")
        ):
            values = []
            with open(data_fpath, newline='') as csvf:
                for row in csv.DictReader(csvf, delimiter=';'):
                    dates.add(row["date"])
                    values.append({
                        "date": row["date"],
                        "vac_statut": row["vac_statut"],
                        "vac_statut_group": _get_vac_statut_group(row["vac_statut"]),
                        "age": row["age"],
                        "nb_PCR": row["nb_PCR"],
                        "nb_PCR_p": row["nb_PCR+"],
                        "HC": row["HC"],
                        "HC_PCR_p": row["HC_PCR+"],
                        "SC": row["SC"],
                        "SC_PCR_p": row["SC_PCR+"],
                        "effectif": row["effectif"] if "effectif" in row else row["effectif J-7"],
                    })
            db_bulk_insert(conn, "drees_by_age", values)

        # dress nationaux
        for data_fpath in (
            os.path.join(DATA_DIR, "drees-covid-19-resultats-issus-des-appariements-entre-si-vic-si-dep-et-vac-si.csv"),
            os.path.join(DATA_DIR, "drees-covid-19-anciens-resultats-nationaux-issus-des-appariements-entre-si-vic-si-dep-.csv")
        ):
            values = []
            with open(data_fpath, newline='') as csvf:
                for row in csv.DictReader(csvf, delimiter=';'):
                    dates.add(row["date"])
                    values.append({
                        "date": row["date"],
                        "vac_statut": row["vac_statut"],
                        "vac_statut_group": _get_vac_statut_group(row["vac_statut"]),
                        "nb_PCR": row["nb_PCR"],
                        "nb_PCR_sympt": row["nb_PCR_sympt"],
                        "nb_PCR_p": row["nb_PCR+"],
                        "nb_PCR_p_sympt": row["nb_PCR+_sympt"],
                        "HC": row["HC"],
                        "HC_PCR_p": row["HC_PCR+"],
                        "SC": row["SC"],
                        "SC_PCR_p": row["SC_PCR+"],
                        "DC": row["DC"],
                        "DC_PCR_p": row["DC_PCR+"],
                        "effectif": row["effectif"] if "effectif" in row else row["effectif J-7"],
                    })
            db_bulk_insert(conn, "drees_nat", values)
            
        # civic hospi
        data_fpath = os.path.join(DATA_DIR, "civic-donnees-hospitalieres-covid19.csv")
        values = []
        with open(data_fpath, newline='') as csvf:
            for row in csv.DictReader(csvf, delimiter=';'):
                def parse_int(val):
                    try:
                        return int(val)
                    except ValueError:
                        return 0
                sexe = row["sexe"]
                if sexe == "0":
                    # sexe = 0 means "both"
                    continue
                values.append({
                    "date": row["jour"],
                    "dep": row["dep"],
                    "sexe": sexe,
                    "hosp": parse_int(row["hosp"]),
                    "rea": parse_int(row["rea"]),
                    "HospConv": parse_int(row["HospConv"]),
                    "deces": parse_int(row["dc"])
                })
        db_bulk_insert(conn, "civic_hospi", values)

        # civic new hospi
        data_fpath = os.path.join(DATA_DIR, "civic-donnees-hospitalieres-nouveaux-covid19.csv")
        values = []
        with open(data_fpath, newline='') as csvf:
            for row in csv.DictReader(csvf, delimiter=';'):
                values.append({
                    "date": row["jour"],
                    "dep": row["dep"],
                    "incid_hosp": parse_int(row["incid_hosp"]),
                    "incid_rea": parse_int(row["incid_rea"]),
                    "incid_dc": parse_int(row["incid_dc"]),
                    "incid_rad": parse_int(row["incid_rad"])
                })
        db_bulk_insert(conn, "civic_new_hospi", values)


def _get_vac_statut_group(vac_statut):
    vac_statut = vac_statut.lower()
    if "compl" in vac_statut:
        return "Complet"
    elif "primo dose" in vac_statut:
        return "Incomplet"
    else:
        return "Non vacciné"


@main.command("plot_hospitalisation_par_statut_vaccinal")
def cmd_plot_hospitalisation_par_statut_vaccinal():
    plot_hospitalisation_par_statut_vaccinal()

def plot_hospitalisation_par_statut_vaccinal():
    plt.clf()
    title = "[DREES] Hospitalisation par status vaccinal"
    plt.title(title)
    with db_connect() as conn:

        req = 'SELECT date, vac_statut_group, sum(HC) FROM drees_by_age GROUP BY date, vac_statut_group'
        rows = conn.cursor().execute(req)
        res = {}
        for date, vac_statut_group, val in rows:
            res.setdefault(date, {})[vac_statut_group] = val
        dates = sorted(res.keys())
        statuses = sorted(set(
            vac_statut_group
            for vals in res.values()
            for vac_statut_group in vals.keys()
        ))
        for label in statuses:
            plt.plot(dates, [res[d].get(label, 0) for d in dates], label=label)
        
        req = 'SELECT date, sum(incid_hosp) FROM civic_new_hospi GROUP BY date'
        rows = conn.cursor().execute(req)
        civic = {
            date: val
            for date, val in rows
        }
        plt.plot(dates, [civic.get(d, 0) for d in dates], label="civic")

    plt.xticks(dates, [
        d if d.endswith("-01") else None
        for d in dates
    ], rotation=20)
    plt.legend()
    plt.savefig(os.path.join(HERE, f'results/hospitalisation_par_statut_vaccinal.png'))


@main.command("plot_hospitalisation_par_age")
@click.option("--rolling", is_flag=True)
def cmd_plot_hospitalisation_par_age(rolling):
    plot_hospitalisation_par_age(rolling=rolling)

def plot_hospitalisation_par_age(rolling=False):
    with db_connect() as conn:
        req = 'SELECT date, age, sum(HC_PCR_p) FROM drees_by_age GROUP BY date, age'
        rows = conn.cursor().execute(req)
        res = {}
        for date, age, val in rows:
            res.setdefault(date, {})[age] = val
        dates = sorted(res.keys())
        ages = sorted(set(
            age
            for vals in res.values()
            for age in vals.keys()
        ))

    plt.clf()
    title = ["[DREES] Hospitalisation par age"]
    if rolling: title.append("(rolling)")
    plt.title("_".join(title))
    for age in ages:
        vals = [res[d].get(age, 0) for d in dates]
        if rolling: vals = rolls(vals, 7)
        plt.plot(dates, vals, label=age)
    plt.xticks(dates, [
        d if d.endswith("-01") else None
        for d in dates
    ], rotation=20)
    plt.legend()
    figname = ["hospitalisation_par_age"]
    if rolling: figname.append("rolling")
    plt.savefig(os.path.join(HERE, f'results/{"_".join(figname)}.png'))


@main.command("plot_deces_par_statut_vaccinal")
def cmd_plot_deces_par_statut_vaccinal():
    plot_deces_par_statut_vaccinal()

def plot_deces_par_statut_vaccinal():
    plt.clf()
    title = "[DREES] Déces par status vaccinal (semaine gissante)"
    plt.title(title)
    with db_connect() as conn:
        req = 'SELECT date, vac_statut_group, sum(DC_PCR_p) FROM drees_nat GROUP BY date, vac_statut_group'
        rows = conn.cursor().execute(req)
        res = {}
        for date, vac_statut_group, val in rows:
            res.setdefault(date, {})[vac_statut_group] = val
        dates = sorted(res.keys())
        statuses = sorted(set(
            vac_statut_group
            for vals in res.values()
            for vac_statut_group in vals.keys()
        ))
        for label in statuses:
            plt.plot(dates, rolls([res[d].get(label, 0) for d in dates], 3), label=label)
        plt.xticks(dates, [
            d if d.endswith("-01") else None
            for d in dates
        ], rotation=20)
        plt.legend()
        plt.savefig(os.path.join(HERE, f'results/deces_par_statut_vaccinal.png'))

# utils

def rmdir(path):
    if os.path.exists(path):
        shutil.rmtree(path)

def mkdir(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass

def db_bulk_insert(conn, table_name, values):
    if len(values) == 0:
        return
    conn.cursor().executemany(
        f"INSERT INTO {table_name} ({','.join(values[0].keys())}) VALUES ({','.join('?' for _ in range(len(values[0])))})",
        [list(v.values()) for v in values])

def rolls(arr, n):
    res = []
    size = len(arr)
    for i in range(size):
        vals = arr[max(0,i-n):min(size,i+n)]
        res.append(sum(vals)/len(vals))
    return res

if __name__ == "__main__":
    main()