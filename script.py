# Standard library imports
import argparse
import datetime
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Third party imports
import pandas as pd
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# --- MongoDB setup ---
load_dotenv()  # Load environment variables from .env file
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI must be set")
client = MongoClient(MONGO_URI)
db = client["stock_db"]
collection = db["stock_data"]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.FileHandler("stock_scraper_debug.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Lock for console prints
print_lock = threading.Lock()

def insert_stock_row(stock_data):
    """
    Inserts a single stock data dict into MongoDB.
    Expects keys: CODE, DATE, OPEN, INTRADAY_HIGH, INTRADAY_LOW, CLOSE, P/E, VOLUME, STATUS
    """
    # parse DATE string into datetime if possible
    date_val = stock_data.get("DATE")
    if isinstance(date_val, str):
        try:
            stock_data["DATE"] = datetime.datetime.strptime(date_val, "%Y%m%d")
        except Exception:
            pass
    collection.insert_one(stock_data)

def export_to_csv(filepath="stock_data_export.csv"):
    """Dump all MongoDB documents to a CSV file."""
    docs = list(collection.find({}, {"_id": 0}))
    df = pd.DataFrame(docs)
    if df.empty:
        logger.info("No data in MongoDB to export.")
        return
    # convert date back to string
    if "DATE" in df.columns:
        df["DATE"] = df["DATE"].astype(str).str.replace(" 00:00:00", "")
    df.to_csv(filepath, index=False)
    logger.info(f"Exported {len(df)} records to {filepath}")

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        f"--user-agent=Mozilla/5.0 StockScraper Thread-{threading.get_ident()}"
    )
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(25)
    return driver

def is_404_page(driver):
    try:
        if "404.aspx" in driver.current_url:
            return True
        page_text = driver.page_source.lower()
        return "page requested" in page_text
    except:
        return False

def wait_for_stock_data(driver, max_wait_time=20):
    start = time.time()
    selectors = [
        (By.CLASS_NAME, "col_open"),
        (By.CLASS_NAME, "col_high"),
        (By.CLASS_NAME, "col_low"),
        (By.CLASS_NAME, "col_prevcls"),
        (By.CLASS_NAME, "col_volume"),
    ]
    while time.time() - start < max_wait_time:
        found = 0
        for by, sel in selectors:
            try:
                elems = driver.find_elements(by, sel)
                if any(e.text and "HK$" in e.text for e in elems):
                    found += 1
            except:
                pass
        if found >= 2:
            return True
        time.sleep(1.5)
    return False

def get_text(driver, by, selector):
    try:
        elems = driver.find_elements(by, selector)
        return elems[0].text if elems and elems[0].text else ""
    except:
        return ""

def extract_with_regex(text, pattern, default="N/A"):
    match = re.search(pattern, text or "")
    return match.group(1) if match else default

def scrape_stock_data(stock_code, base_url):
    thread_id = threading.get_ident()
    log = logging.getLogger(f"Thread-{thread_id}")
    log.info(f"Scraping {stock_code}")
    today = datetime.datetime.now().strftime("%Y%m%d")
    data = {
        "CODE": stock_code,
        "DATE": today,
        "OPEN": "N/A",
        "INTRADAY_HIGH": "N/A",
        "INTRADAY_LOW": "N/A",
        "CLOSE": "N/A",
        "P/E": "N/A",
        "VOLUME": "N/A",
        "STATUS": "Success",
    }
    driver = None
    try:
        driver = setup_driver()
        driver.get(f"{base_url}?sym={stock_code}")
    except Exception as e:
        log.info(f"Load timeout: {e}")
        data["STATUS"] = "Error"
        with print_lock:
            print(",".join(data.values()))
        insert_stock_row(data)
        return False

    if is_404_page(driver) or not wait_for_stock_data(driver):
        data["STATUS"] = "Error"
        with print_lock:
            print(",".join(data.values()))
        insert_stock_row(data)
        return False

    data["OPEN"] = extract_with_regex(get_text(driver, By.CLASS_NAME, "col_open"), r"HK\$(\d+\.\d+)")
    data["INTRADAY_HIGH"] = extract_with_regex(get_text(driver, By.CLASS_NAME, "col_high"), r"HK\$(\d+\.\d+)")
    data["INTRADAY_LOW"] = extract_with_regex(get_text(driver, By.CLASS_NAME, "col_low"), r"HK\$(\d+\.\d+)")
    data["CLOSE"] = extract_with_regex(get_text(driver, By.CLASS_NAME, "col_prevcls"), r"HK\$(\d+\.\d+)")
    data["P/E"] = extract_with_regex(get_text(driver, By.CLASS_NAME, "col_pe"), r"(\d+\.\d+)x")

    vol_text = get_text(driver, By.CLASS_NAME, "col_volume")
    vm = re.search(r"([\d\.]+)([BMK]?)", vol_text)
    if vm:
        val, unit = vm.groups()
        multiplier = {"B": 1e9, "M": 1e6, "K": 1e3}.get(unit, 1)
        data["VOLUME"] = str(float(val) * multiplier)

    log.info(f"Done {stock_code}")
    with print_lock:
        print(",".join(data.values()))
    insert_stock_row(data)
    return True

def scrape_stocks_multithreaded(base_url, stock_codes, max_workers=4):
    logger.info(f"Starting with {max_workers} threads")
    successful = failed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(scrape_stock_data, code, base_url): code for code in stock_codes}
        for fut in as_completed(futures):
            if fut.result():
                successful += 1
            else:
                failed += 1
    logger.info(f"Finished. Success: {successful}, Failed: {failed}")
    return successful, failed

def main():
    base_url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote"
    stock_codes = [str(i) for i in range(1000, 4001)] + [str(i) for i in range(9500, 10000)]
    max_threads = os.cpu_count() or 4
    scrape_stocks_multithreaded(base_url, stock_codes, max_threads)
    export_to_csv("stock_data.csv")
    logger.info("All done.")

if __name__ == "__main__":
    main()
