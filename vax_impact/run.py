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
import urllib.request
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
    download_data()
    init_db()
    import_data()
    plot_hosp_real_vs_comp()
    plot_hosp_distribution()
    plot_hosp_repartition()


@main.command("download_data")
def cmd_download_data():
    download_data()


def download_data():

    download_data_file("https://www.data.gouv.fr/fr/datasets/r/54dd5f8d-1e2e-4ccb-8fb8-eac68245befd", "vacsi-a-fra.csv")
    download_data_file("https://www.data.gouv.fr/fr/datasets/r/08c18e08-6780-452d-9b8c-ae244ad529b3", "donnees-hospitalieres-classe-age-covid19.csv")

    # https://static.data.gouv.fr/resources/donnees-hospitalieres-relatives-a-lepidemie-de-covid-19/20220102-191008/covid-hosp-txad-age-fra-2022-01-02-19h10.csv
    # hosp_by_age.csv


@main.command("import_data")
def cmd_import_data():
    init_db()
    import_data()

def db_connect():
    return sqlite3.connect(os.path.join(HERE, "data.sqlite"))

def init_db():
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS vax_couv_by_age(clage_vacsi text, clage text, date text, n_dose1 int, n_complet int, n_rappel int, n_cum_dose1 int, n_cum_complet int, n_cum_rappel int, couv_dose1 float, couv_complet float, couv_rappel float)''')
        #cur.execute('''CREATE TABLE IF NOT EXISTS hosp_by_age(clage text, date text, tx_indic_7J_DC float, tx_indic_7J_hosp float, tx_indic_7J_SC float, tx_prev_hosp float, tx_prev_SC float)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS hosps(reg text, clage text, date text, hosp int, rea int, HospConv int, rad int, dc int)''')
        cur.execute('''DELETE FROM vax_couv_by_age''')
        #cur.execute('''DELETE FROM hosp_by_age''')
        cur.execute('''DELETE FROM hosps''')

CLAGES = ("0-39", "40-49", "50-59", "60-69", "70-79", "80+")
HOSP_TYPES = {
    "rea": "soins critiques",
    "hosp": "hospitalisations"
}

def import_data():
    with db_connect() as conn:

        def parse_int(val):
            try:
                return int(val)
            except ValueError:
                return 0
        def parse_float(val):
            try:
                return float(val)
            except ValueError:
                return 0.0
        def parse_clage(val):
            val = int(val)
            if val == 0: return
            if val <= 39: return "0-39"
            if val <= 49: return "40-49"
            if val <= 59: return "50-59"
            if val <= 69: return "60-69"
            if val <= 79: return "70-79"
            return "80+"
            
        # vax_couv_by_age
        data_fpath = os.path.join(DATA_DIR, "vacsi-a-fra.csv")
        values = []
        with open(data_fpath, newline='') as csvf:
            for row in csv.DictReader(csvf, delimiter=';'):
                clage = parse_clage(row["clage_vacsi"])
                if clage is None: continue
                values.append({
                    "clage_vacsi": row["clage_vacsi"],
                    "clage": clage,
                    "date": row["jour"],
                    "n_dose1": parse_int(row["n_dose1"]),
                    "n_complet": parse_int(row["n_complet"]),
                    "n_rappel": parse_int(row["n_rappel"]),
                    "n_cum_dose1": parse_int(row["n_cum_dose1"]),
                    "n_cum_complet": parse_int(row["n_cum_complet"]),
                    "n_cum_rappel": parse_int(row["n_cum_rappel"]),
                    "couv_dose1": parse_float(row["couv_dose1"]),
                    "couv_complet": parse_float(row["couv_complet"]),
                    "couv_rappel": parse_float(row["couv_rappel"]),
                })
        db_bulk_insert(conn, "vax_couv_by_age", values)
            
        # hosp_by_age
        # data_fpath = os.path.join(DATA_DIR, "hosp_by_age.csv")
        # values = []
        # with open(data_fpath, newline='') as csvf:
        #     for row in csv.DictReader(csvf, delimiter=';'):
        #         clage = parse_clage(row["clage_90"])
        #         if clage is None: continue
        #         values.append({
        #             "clage": clage,
        #             "date": row["jour"],
        #             "tx_indic_7J_DC": parse_float(row["tx_indic_7J_DC"]),
        #             "tx_indic_7J_hosp": parse_float(row["tx_indic_7J_hosp"]),
        #             "tx_indic_7J_SC": parse_float(row["tx_indic_7J_SC"]),
        #             "tx_prev_hosp": parse_float(row["tx_prev_hosp"]),
        #             "tx_prev_SC": parse_float(row["tx_prev_SC"]),
        #         })
        # db_bulk_insert(conn, "hosp_by_age", values)

        # donnees-hospitalieres-classe-age-covid19.csv
        data_fpath = os.path.join(DATA_DIR, "donnees-hospitalieres-classe-age-covid19.csv")
        values = []
        with open(data_fpath, newline='') as csvf:
            for row in csv.DictReader(csvf, delimiter=';'):
                clage = parse_clage(row["cl_age90"])
                if clage is None: continue
                values.append({
                    "reg": row["reg"],
                    "clage": clage,
                    "date": row["jour"],
                    "hosp": parse_int(row["hosp"]),
                    "rea": parse_int(row["rea"]),
                    "HospConv": parse_int(row["HospConv"]),
                    "rad": parse_int(row["rad"]),
                    "dc": parse_int(row["dc"]),
                })
        db_bulk_insert(conn, "hosps", values)


def _comp_pop_by_clage(conn):
    tmp = {}
    for clage_vacsi, clage, n_cum_complet, couv_complet in conn.execute(
        'SELECT clage_vacsi, clage, n_cum_complet, couv_complet FROM vax_couv_by_age'
    ):
        if couv_complet > 0.1:
            tmp.setdefault(clage, {}).setdefault(clage_vacsi, []).append(n_cum_complet/couv_complet)
    return {
        clage: sum(
            int(sum(vals) / len(vals))
            for _clage_vacsi, vals in tmp2.items()
        )
        for clage, tmp2 in tmp.items()
    }

def _comp_vax_cov_by_date_clage(conn):

    pop_by_clage = _comp_pop_by_clage(conn)

    return {
        (date, clage): ncum / pop_by_clage[clage]
        for date, clage, ncum in conn.execute(
            'SELECT date, clage, sum(n_cum_complet) FROM vax_couv_by_age GROUP BY date, clage'
        )
    }


@main.command("plot_hosp_real_vs_comp")
@click.option("--type", type=click.Choice(HOSP_TYPES.keys()), default="rea")
@click.option("--clage", type=click.Choice(CLAGES))
def cmd_plot_hosp_real_vs_comp(type, clage):
    plot_hosp_real_vs_comp(hosp_type=type, clage=clage)

def plot_hosp_real_vs_comp(hosp_type="rea", clage=None):
    with db_connect() as conn:

        clages = [clage] if clage else CLAGES
        clage_sql = (["clage = ?"], [clage]) if clage else ([],[])

        date_hosps = {
            date: hosps
            for date, hosps in conn.execute(
                f'SELECT date, sum({hosp_type}) from hosps {where(clage_sql[0])} GROUP BY date',
                clage_sql[1]
            )
        }
        dates = list(date_hosps.keys())

        min_date_vax = next(conn.execute(
            f'select min(date) from vax_couv_by_age {where(clage_sql[0])}',
            clage_sql[1]
        ))[0]
        vax_dates = [d for d in dates if d >= min_date_vax]

        max_hosp_date, max_hosp = sorted(
            (
                (date, hosps)
                for date, hosps in date_hosps.items()
                if date < min_date_vax
            ),
            key=lambda v: v[1]
        )[-1]

        max_hosp_by_clage = {
            clage: val
            for clage, val in conn.execute(
                f'SELECT clage, sum({hosp_type}) from hosps where date = ? group by clage',
                (max_hosp_date,)
            )
        }

        vax_cov_by_date_clage = _comp_vax_cov_by_date_clage(conn)

    plt.clf()
    title = ["[France]", HOSP_TYPES[hosp_type].title()]
    if clage: title.append(f"({clage})")
    plt.suptitle(" ".join(title))
    plt.title("Source: data.gouv.fr", fontsize=10)

    plt.plot(dates, date_hosps.values(), label="covid hosps")

    def plot_vax_impact(vax_eff):
        vals = [
            max_hosp
            for d in dates
            if d >= max_hosp_date and d < min_date_vax
        ]
        for date in vax_dates:
            vals.append(sum(
                (1-(vax_cov_by_date_clage.get((date,clage),0)/100*vax_eff)) * max_hosp_by_clage[clage]
                for clage in clages
            ))
        plt.plot([d for d in dates if d >= max_hosp_date], vals, label=f"covid MAX hosps ({int(vax_eff*100)}% vax efficiency)")
    
    plot_vax_impact(.95)
    plot_vax_impact(.50)
    plot_vax_impact(.0)

    plt.xticks(dates, [
        d if d.endswith("-01") else None
        for d in dates
    ], rotation=20)
    plt.legend()
    figname = [hosp_type, "real_vs_comp"]
    if clage: figname.append(clage)
    plt.savefig(os.path.join(HERE, f'results/{"_".join(figname)}.png'))


@main.command("plot_hosp_by_clage")
@click.option("--type", type=click.Choice(HOSP_TYPES.keys()), default="rea")
@click.option("--clage", type=click.Choice(CLAGES))
def cmd_plot_hosp_distribution(type, clage):
    plot_hosp_distribution(hosp_type=type, clage=clage)

def plot_hosp_distribution(hosp_type="rea", clage=None):

    with db_connect() as conn:

        if not clage:
            clages = CLAGES
            req = conn.execute(
                f'SELECT date, clage, sum({hosp_type}) from hosps GROUP BY date, clage',
            )
        else:
            clages = (clage,)
            req = conn.execute(
                f'SELECT date, clage, sum({hosp_type}) from hosps WHERE clage = ? GROUP BY date, clage',
                [clage]
            )

        date_hosps = {
            (date, clage): val
            for date, clage, val in req
        }
        dates = list(d for d, _ in date_hosps.keys())

    plt.clf()
    title = ["[France]", HOSP_TYPES[hosp_type].title(), "Covid19"]
    if clage: title.append(f"({clage} ans)")
    plt.suptitle(" ".join(title))
    plt.title("Source: data.gouv.fr", fontsize=10)

    plt.stackplot(
        dates,
        *[
            [
                date_hosps.get((date, clage), 0)
                for date in dates
            ]
            for clage in clages
        ],
        labels=clages if not clage else []
    )

    plt.xticks([
        d for d in dates
        if d[-5:] in ("01-01", "04-01", "07-01", "10-01")
    ], rotation=20, ha='right')

    plt.legend()

    plotname = [hosp_type, "distribution"]
    if clage: plotname.append(clage)
    plt.savefig(os.path.join(HERE, f'results/{"_".join(plotname)}.png'))


@main.command("plot_hosp_repartition")
def cmd_plot_hosp_repartition():
    plot_hosp_repartition()

def plot_hosp_repartition():
    vax_effs = (.95, .5, .0)

    with db_connect() as conn:

        hosp_by_date_clage = {
            (date, clage): val
            for date, clage, val in conn.execute(
                'SELECT date, clage, sum(rea) from hosps group by date, clage'
            )
        }

        dates = sorted(set(date for (date, _), _ in hosp_by_date_clage.items()))

        tot_hosp_by_date = {
            date: sum(
                hosp_by_date_clage[(date, clage)]
                for clage in CLAGES
            )
            for date in dates
        }

        min_date_vax = next(conn.execute(
            'select min(date) from vax_couv_by_age'
        ))[0]

        mean_hosp_by_clage_before_vax = {
            clage: sum(
                hosp_by_date_clage[(date, clage)]
                for date in dates
                if date < min_date_vax
            ) / len(dates)
            for clage in CLAGES
        }

        vax_cov_by_date_clage = _comp_vax_cov_by_date_clage(conn)

        est_hosp_by_date_clage = {
            (vax_eff, date, clage): mean_hosp_by_clage_before_vax[clage] * (1-(vax_cov_by_date_clage.get((date,clage),0)/100*vax_eff))
            for vax_eff in vax_effs
            for clage in CLAGES
            for date in dates
            if date >= min_date_vax
        }

        tot_est_hosp_by_date = {
            (vax_eff, date): sum(
                est_hosp_by_date_clage[(vax_eff, date, clage)]
                for clage in CLAGES
            )
            for vax_eff in vax_effs
            for date in dates
            if date >= min_date_vax
        }
    
    # real hosp repartition
    plt.clf()
    fig, axs = plt.subplots(4)
    axs[0].set_title("[France] Hospitalisation répartition réelle")
    axs[0].stackplot(
        dates,
        *[
            [
                hosp_by_date_clage[(date, clage)] / tot_hosp_by_date[date] * 100
                for date in dates
            ]
            for clage in CLAGES
        ],
        labels=CLAGES
    )

    plt.sca(axs[0])
    plt.xticks(dates, [
        d if d.endswith("-01") else None
        for d in dates
    ], rotation=20)
    axs[0].legend()

    for i, vax_eff in enumerate(vax_effs):

        # hosp repartition estimations
        axs[i+1].set_title(f"[France] Hospitalisation répartition estimée ({int(vax_eff*100)}% vax efficiency)")
        axs[i+1].stackplot(
            dates,
            *[
                [
                    hosp_by_date_clage[(date, clage)] / tot_hosp_by_date[date] * 100
                    for date in dates
                    if date < min_date_vax
                ] + [
                    est_hosp_by_date_clage[(vax_eff, date, clage)] / tot_est_hosp_by_date[(vax_eff, date)] * 100
                    for date in dates
                    if date >= min_date_vax
                ]
                for clage in CLAGES
            ],
            labels=CLAGES
        )

        plt.sca(axs[i+1])
        plt.xticks(dates, [
            d if d.endswith("-01") else None
            for d in dates
        ], rotation=20)
        axs[i+1].legend()
    
    plt.savefig(os.path.join(HERE, f'results/hosp_repartitions.png'))


# utils

def rmdir(path):
    if os.path.exists(path):
        shutil.rmtree(path)

def mkdir(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass

def download_data_file(url, fname):
    mkdir(DATA_DIR)
    fpath = os.path.join(DATA_DIR, fname)
    if not os.path.exists(fpath):
        print(f'Download {fname}... ', end='', flush=True)
        urllib.request.urlretrieve(url, fpath)
        print(f'DONE')

def db_bulk_insert(conn, table_name, values):
    if len(values) == 0:
        return
    conn.cursor().executemany(
        f"INSERT INTO {table_name} ({','.join(values[0].keys())}) VALUES ({','.join('?' for _ in range(len(values[0])))})",
        [list(v.values()) for v in values])

def glisser(arr, n):
    res = []
    size = len(arr)
    for i in range(size):
        vals = arr[max(0,i-n):min(size,i+n)]
        res.append(sum(vals)/len(vals))
    return res

def where(conds):
    if not conds: return ""
    return "where " + " and ".join(conds)

if __name__ == "__main__":
    main()