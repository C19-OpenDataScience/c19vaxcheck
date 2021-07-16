#!/usr/bin/env python
import os
import re
import time
import shutil
import unidecode
import csv
from contextlib import contextmanager
import sqlite3
import click
import selenium
from selenium.webdriver.common.by import By
import matplotlib.pyplot as plt
#from selenium.webdriver.chrome.options import Options

HERE = os.path.abspath(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(HERE, '_download')
DATA_DIR = os.path.join(HERE, 'data')

YEARS_RANGE=range(2010, 2021+1)

@click.group()
def main():
    pass

@main.command("all")
def cmd_all():
    download_data()
    import_data()
    plot_reactions_by_year_c19()
    plot_reactions_by_year_c19(severe=True)
    plot_reactions_by_year_c19(death=True)



@main.command("download_data")
def cmd_download_data():
    download_data()

def download_data():
    if os.path.exists(DOWNLOAD_DIR):
        shutil.rmtree(DOWNLOAD_DIR)
    def _ofpath(subst, year, ext):
        return os.path.join(DATA_DIR, f"{norm_fname(subst)}_{year}.{ext}")
    driver = init_driver()
    driver.get('https://www.adrreports.eu/en/search_subst.html')
    # choose substance
    all_buts_el = find_el(driver, '#alphabetnavigation')
    for letter_link_el in find_els(all_buts_el, 'a'):
        letter = letter_link_el.get_attribute('innerHTML')
        letter_link_el.click()
        def get_res_el():
            res_el = find_el(driver, "#result_table")
            if not res_el.get_attribute('innerHTML'): return None
            link_el = find_el(res_el, "a")
            if not link_el: return res_el
            text = link_el.get_attribute('innerHTML')
            if text[0].lower() != letter.lower():
                return None
            return res_el if res_el.get_attribute('innerHTML') else None
        res_el = wait_el(get_res_el, timeout=5)
        for subst_link_el in find_els(res_el, "a"):
            subst = subst_link_el.get_attribute('innerHTML')
            if is_vax_subst(subst):
                # filter years that need to be done
                years = [
                    year
                    for year in YEARS_RANGE
                    if (
                        not os.path.exists(_ofpath(subst, year, "csv"))
                        and not os.path.exists(_ofpath(subst, year, "noData"))
                    )
                ]
                if not years:
                    continue
                subst_link_el.click()
                with switch_to_next_tab(driver, 1):
                    # choose line listing
                    linelist_el = wait_el(lambda: find_el(driver, 'td[title="Line Listing"]'))
                    linelist_el.click()
                    # choose year
                    year_input_title = "2021"
                    for year in years:
                        year_inputs = wait_el(lambda: find_el(driver, f'input[title="{year_input_title}"]'))
                        year_inputs.click()
                        year_input = wait_el(lambda: find_el(driver, f'.promptMenuOption[title="{year}"]'))
                        year_input.click()
                        year_input_title = str(year)
                        time.sleep(1)
                        run_but = find_el(driver, 'a[name="SectionElements"]')
                        run_but.click()
                        with switch_to_next_tab(driver, 2):
                            # download
                            try:
                                def _try_download():
                                    def _try_find_report_but():
                                        res = find_el(driver, 'a[title="Export to different format"]')
                                        if res: return res
                                        no_data_el = find_el(driver, 'div[result="noData"]')
                                        if no_data_el:
                                            raise NoDataError()
                                    report_but = wait_el(_try_find_report_but, timeout=3)
                                    report_but.click()
                                    data_but = wait_el(lambda: find_el(driver, 'a[aria-label="Data"]'), timeout=3)
                                    data_but.click()
                                    csv_but = wait_el(lambda: find_el(driver, 'a[aria-label="CSV"]'), timeout=3)
                                    csv_but.click()
                                retry(5, AttributeError, _try_download)
                            except NoDataError:
                                open(_ofpath(subst, year, "noData"), 'a').close()
                                continue
                            fname = wait_download(timeout=300)
                            shutil.move(
                                os.path.join(DOWNLOAD_DIR, fname),
                                _ofpath(subst, year, "csv")
                            )
    #driver.quit()

def is_vax_subst(subst):
    return "vaccine" in subst.lower()

class NoDataError(Exception):
    pass

def init_driver():
    #options = Options()
    #options.add_argument("--disable-infobars")
    #driver = webdriver.Chrome(chrome_options=options)
    # profile using multi-tab
    firefox_capabilities = selenium.webdriver.DesiredCapabilities.FIREFOX
    firefox_capabilities['marionette'] = True
    profile = selenium.webdriver.FirefoxProfile()
    profile.DEFAULT_PREFERENCES["frozen"]["browser.link.open_newwindow"] = 3 # open in new tag
    # do not prompt when downloading
    profile.set_preference('browser.download.folderList', 2) # custom location
    profile.set_preference('browser.download.manager.showWhenStarting', False)
    profile.set_preference('browser.download.dir', DOWNLOAD_DIR)
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
    driver = selenium.webdriver.Firefox(profile)
    # set size so that some buttons will appear
    driver.set_window_position(0, 0)
    driver.set_window_size(1600, 1200)
    return driver

def norm_fname(text):
    res = unidecode.unidecode(text).upper()
    res = re.sub('[\W]', '_', res)
    res = re.sub('_+', '_', res)
    return res

def find_el(parent, sel):
    try:
        return parent.find_element(By.CSS_SELECTOR, sel)
    except selenium.common.exceptions.NoSuchElementException:
        pass

def find_els(parent, sel):
    return parent.find_elements(By.CSS_SELECTOR, sel)

def set_el_attr(driver, el, attr, val):
    driver.execute_script("arguments[0].setAttribute(arguments[1], arguments[2])", el, attr, val)

def print_el(el):
    print(el.get_attribute('outerHTML'))

def wait_el(getter, timeout=60):
    sleep_period = .5
    sleep_time = 0
    while True:
        el = getter()
        if el:
            return el
        time.sleep(sleep_period)
        sleep_time += sleep_period
        if sleep_time > timeout:
            return

def wait_download(timeout=300):
    sleep_period = .5
    sleep_time = 0
    try:
        os.makedirs(DOWNLOAD_DIR)
    except FileExistsError:
        pass
    while True:
        fnames = [name for name in os.listdir(DOWNLOAD_DIR) if os.path.isfile(os.path.join(DOWNLOAD_DIR, name))]
        if len(fnames) == 1:
            return fnames[0]
        time.sleep(sleep_period)
        sleep_time += sleep_period
        if sleep_time > timeout:
            return

@contextmanager
def switch_to_next_tab(driver, tab_num):
    driver.switch_to.window(driver.window_handles[tab_num])
    try:
        yield
    finally:
        driver.close()
        driver.switch_to.window(driver.window_handles[tab_num-1])

def retry(nb_tries, Err, fun):
    for i in range(0, nb_tries):
        try:
            fun()
        except Err:
            continue
        break



@main.command("import_data")
@click.option("--force", is_flag=True)
def cmd_import_data(force):
    init_db(force=force)
    import_data()

def db_connect():
    return sqlite3.connect(os.path.join(HERE, "data.sqlite"))

def init_db(force=False):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS reports(subst text, year integer, report_id text, date text, age_group text, sex text, is_c19_vax boolean, severe boolean, death boolean)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS reactions(subst text, year integer, report_id text, type text, duration text, outcome text, seriousness text)''')
        if force:
            cur.execute('''DELETE FROM reports''')
            cur.execute('''DELETE FROM reactions''')

def import_data():
    with db_connect() as conn:
        for fname in os.listdir(DATA_DIR):
            basefname, fext = os.path.splitext(fname)
            if fext != '.csv': continue
            pos = basefname.rfind('_')
            subst = basefname[:pos]
            year = int(basefname[pos+1:])
            row = conn.cursor().execute(
                '''SELECT COUNT(*) FROM reports WHERE subst=? AND year=?''',
                [subst, year]
            ).fetchone()
            if row[0] > 0:
                continue
            with open(os.path.join(DATA_DIR, fname)) as csvfile:
                reader = csv.DictReader(csvfile)
                reports_rows, reactions_rows = [], []
                def _insert():
                    nonlocal reports_rows, reactions_rows
                    db_bulk_insert(conn, "reports", reports_rows)
                    db_bulk_insert(conn, "reactions", reactions_rows)
                    reports_rows, reactions_rows = [], []
                for row in reader:
                    reactions = row["Reaction List PT (Duration – Outcome - Seriousness Criteria)"]
                    severe, death = False, False
                    if reactions:
                        reactions = reactions.split(",<BR><BR>")
                        for reaction in reactions:
                            reaction = reaction.strip()
                            _type = reaction[:reaction.rfind('(')]
                            other = reaction[reaction.rfind('(')+1:reaction.rfind(')')]
                            duration, outcome, seriousness = other.split("-")
                            reactions_rows.append({
                                "subst": subst,
                                "year": year,
                                "report_id": row["\ufeff\ufeffEU Local Number"],
                                "type": _type.strip(),
                                "duration": duration.strip(),
                                "outcome": outcome.strip(),
                                "seriousness": seriousness.strip(),
                            })
                            death = death or ("Results in Death" in seriousness)
                            severe = death or severe or ("Life Threatening" in seriousness) or ("Caused/Prolonged Hospitalisation" in seriousness)
                    reports_rows.append({
                        "subst": subst,
                        "year": year,
                        "report_id": row["\ufeff\ufeffEU Local Number"],
                        "date": row["EV Gateway Receipt Date"],
                        "age_group": row["Patient Age Group"],
                        "sex": row["Patient Sex"],
                        "is_c19_vax": ("COVID_19" in subst),
                        "severe": severe,
                        "death": death
                    })
                    if len(reports_rows) >= 100:
                        _insert()
                _insert()

def db_bulk_insert(conn, table_name, values):
    if len(values) == 0:
        return
    conn.cursor().executemany(
        f"INSERT INTO {table_name} ({','.join(values[0].keys())}) VALUES ({','.join('?' for _ in range(len(values[0])))})",
        [list(v.values()) for v in values])



@main.command("plot_reactions_by_year_c19")
@click.option("--severe", is_flag=True)
@click.option("--death", is_flag=True)
def cmd_plot_reactions_by_year_c19(severe, death):
    plot_reactions_by_year_c19(severe=severe, death=death)

def plot_reactions_by_year_c19(severe=False, death=False):
    plt.clf()
    title = "Nombre de réactions post-vaccinales"
    if severe:
        title = "Nombre de réactions sévères post-vaccinales"
    if death:
        title = "Nombre de morts post-vaccinales"
    plt.title(title)
    with db_connect() as conn:
        req = 'SELECT is_c19_vax, year, count(*) FROM reports'
        if severe:
            req += f' WHERE severe=1'
        if death:
            req += f' WHERE death=1'
        req += ' GROUP BY is_c19_vax, year'
        rows = conn.cursor().execute(req)
        res = {}
        for is_c19_vax, year, nb in rows:
            res.setdefault(bool(is_c19_vax), {})[year] = nb
        plt.bar(res[True].keys(), [a+b for a, b in zip(res[False].values(), res[True].values())], label="C19 vax")
        plt.bar(res[False].keys(), res[False].values(), label="All other vax")
        plt.legend()
        fname = 'reactions_by_year_c19.png'
        if severe:
            fname = 'reactions_by_year_c19_severe.png'
        if death:
            fname = 'reactions_by_year_c19_death.png'
        plt.savefig(os.path.join(HERE, f'results/{fname}'))



if __name__ == "__main__":
    main()