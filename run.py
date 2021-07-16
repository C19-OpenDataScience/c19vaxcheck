import os
import re
import time
import shutil
import unidecode
from contextlib import contextmanager
import selenium
from selenium.webdriver.common.by import By
#from selenium.webdriver.chrome.options import Options

HERE = os.path.abspath(os.path.dirname(__file__))
DOWNLOAD_DIR = os.path.join(HERE, '_download')
DATA_DIR = os.path.join(HERE, 'data')

YEARS_RANGE=range(2010, 2021+1)

def execute(driver):
    driver.get('https://www.adrreports.eu/en/search_subst.html')
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
                subst_link_el.click()
                with switch_to_next_tab(driver, 1):
                    linelist_el = wait_el(lambda: find_el(driver, 'td[title="Line Listing"]'))
                    linelist_el.click()
                    for year in YEARS_RANGE:
                        ofpath = os.path.join(DATA_DIR, f"{norm_fname(subst)}_{year}.xls")
                        if os.path.exists(ofpath):
                            continue
                        year_inputs = wait_el(lambda: find_el(driver, 'input[title="2021"]'))
                        year_inputs.click()
                        year_input = wait_el(lambda: find_el(driver, f'.promptMenuOption[title="{year}"]'))
                        year_input.click()
                        time.sleep(1)
                        run_but = find_el(driver, 'a[name="SectionElements"]')
                        run_but.click()
                        with switch_to_next_tab(driver, 2):
                            report_but = wait_el(lambda: find_el(driver, 'a[title="Export to different format"]'))
                            report_but.click()
                            data_but = find_el(driver, 'a[aria-label="Data"]')
                            data_but.click()
                            csv_but = find_el(driver, 'a[aria-label="CSV"]')
                            csv_but.click()
                            fname = wait_download(timeout=300)
                            shutil.move(
                                os.path.join(DOWNLOAD_DIR, fname),
                                ofpath
                            )
                            return
    #global driver
    #driver.get('https://www.adrreports.eu/en/search_subst.html')
    #time.sleep(7)
    #element = driver.find_element_by_xpath('//*[@id="app"]/div/div/div/section/div/div[2]/article/div[2]/button')
    #element.click()
    #time.sleep(3)

def is_vax_subst(subst):
    return "vaccine" in subst.lower()

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


if __name__ == "__main__":
    if os.path.exists(DOWNLOAD_DIR):
        shutil.rmtree(DOWNLOAD_DIR)
    driver = init_driver()
    execute(driver)
    #driver.quit()