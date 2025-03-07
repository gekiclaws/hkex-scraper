# Standard library imports
import argparse
import csv
import datetime
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Third party imports
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                    handlers=[logging.FileHandler("stock_scraper_debug.log"), logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Use locks for shared resources
print_lock = threading.Lock()
csv_lock = threading.Lock()

# CSV file paths
CSV_TEMP_PATH = 'stock_data_temp.csv'
CSV_FINAL_PATH = 'stock_data.csv'

def setup_driver():
    """Create and return a configured WebDriver instance"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f"--user-agent=Mozilla/5.0 StockScraper Thread-{threading.get_ident()}")
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(25)  # 25 second timeout for page loads
    return driver

def is_404_page(driver):
    """Check if the current page is definitely a 404 error page"""
    try:
        # Check URL for error page
        if "404.aspx" in driver.current_url:
            return True
        
        # Check for explicit error messages 
        page_text = driver.page_source.lower()
        error_phrases = ["page requested"]
        if any(phrase in page_text for phrase in error_phrases):
            return True
    except Exception as e:
        logger.debug(f"Error checking for 404 page: {e}")
    
    return False

def wait_for_stock_data(driver, max_wait_time=20):
    """
    Wait for stock data elements to appear on the page
    Returns True if valid stock data was found, False otherwise
    """
    start_time = time.time()
    check_interval = 1.5  # seconds between checks
    
    # Define the key data elements we're looking for
    data_selectors = [
        (By.CLASS_NAME, "col_open"),
        (By.CLASS_NAME, "col_high"),
        (By.CLASS_NAME, "col_low"),
        (By.CLASS_NAME, "col_prevcls"),
        (By.CLASS_NAME, "col_volume")
    ]
    
    logger.debug(f"Starting wait for stock data (max {max_wait_time}s)")
    
    while time.time() - start_time < max_wait_time:
        # Count how many element types we've found
        elements_found = 0
        
        for by_method, selector in data_selectors:
            try:
                elements = driver.find_elements(by_method, selector)
                if elements and any(element.text and 'HK$' in element.text for element in elements):
                    elements_found += 1
            except Exception:
                pass
        
        # If we found some elements with actual price data, consider it valid
        if elements_found >= 2:  # At least 2 different types of data elements
            logger.debug(f"Found {elements_found} valid data elements after {time.time() - start_time:.1f}s")
            return True
            
        # Wait before checking again
        time.sleep(check_interval)
    
    logger.debug(f"No valid stock data found after {max_wait_time}s wait")
    return False

def get_element_text(driver, by_method, selector, default=""):
    """Get text from an element or return default if not found"""
    try:
        elements = driver.find_elements(by_method, selector)
        if elements and elements[0].text:
            return elements[0].text
    except Exception:
        pass
    return default

def extract_value_with_regex(text, pattern, default='N/A'):
    """Extract value using regex pattern with a default value if not found"""
    if not text:
        return default
        
    match = re.search(pattern, text)
    if match:
        return match.group(1)
    return default

def write_to_csv(data):
    """Write a single row of data to the CSV file with thread-safe locking"""
    with csv_lock:
        file_exists = os.path.isfile(CSV_TEMP_PATH)
        with open(CSV_TEMP_PATH, 'a', newline='') as csvfile:
            fieldnames = ['CODE', 'DATE', 'OPEN', 'INTRADAY_HIGH', 'INTRADAY_LOW', 'CLOSE', 'P/E', 'VOLUME', 'STATUS']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header only if file is new
            if not file_exists:
                writer.writeheader()
            
            # Write the data row
            writer.writerow(data)

def scrape_stock_data(stock_code, base_url):
    """
    Scrapes stock data for a single stock code and writes directly to CSV
    """
    thread_id = threading.get_ident()
    thread_logger = logging.getLogger(f"Thread-{thread_id}")
    thread_logger.info(f"Started scraping stock code: {stock_code}")
    
    driver = None
    url = f"{base_url}?sym={stock_code}"
    
    try:
        # Use system date
        today = datetime.datetime.now()
        formatted_date = today.strftime('%Y%m%d')
        
        # Initialize data with defaults
        data = {
            'CODE': stock_code,
            'DATE': formatted_date,
            'OPEN': 'N/A',
            'INTRADAY_HIGH': 'N/A',
            'INTRADAY_LOW': 'N/A',
            'CLOSE': 'N/A',
            'P/E': 'N/A',
            'VOLUME': 'N/A',
            'STATUS': 'Success'
        }

        # Initialize driver
        driver = setup_driver()
        
        try:
            # Load the page with timeout
            driver.get(url)
        except Exception as e:
            thread_logger.info(f"Timeout loading stock {stock_code}: {e}")
            data['STATUS'] = 'Error'
            write_to_csv(data)
            return False
        
        # Quick check for 404 to fail fast
        if is_404_page(driver):
            thread_logger.info(f"Stock {stock_code} returned a 404 error - quick fail")
            data['STATUS'] = 'Error'
            
            with print_lock:
                print(f"{stock_code},{formatted_date},N/A,N/A,N/A,N/A,N/A,N/A,Error")
                
            write_to_csv(data)
            return False
        
        # Wait for stock data to appear on the page
        if not wait_for_stock_data(driver, max_wait_time=20):
            thread_logger.info(f"No valid stock data found for {stock_code} after waiting")
            data['STATUS'] = 'Error'
            
            with print_lock:
                print(f"{stock_code},{formatted_date},N/A,N/A,N/A,N/A,N/A,N/A,Error")
                
            write_to_csv(data)
            return False
        
        # Extract stock data
        data['OPEN'] = extract_value_with_regex(
            get_element_text(driver, By.CLASS_NAME, "col_open"),
            r'HK\$(\d+\.\d+)'
        )
        
        data['INTRADAY_HIGH'] = extract_value_with_regex(
            get_element_text(driver, By.CLASS_NAME, "col_high"),
            r'HK\$(\d+\.\d+)'
        )
        
        data['INTRADAY_LOW'] = extract_value_with_regex(
            get_element_text(driver, By.CLASS_NAME, "col_low"),
            r'HK\$(\d+\.\d+)'
        )
        
        data['CLOSE'] = extract_value_with_regex(
            get_element_text(driver, By.CLASS_NAME, "col_prevcls"),
            r'HK\$(\d+\.\d+)'
        )
        
        data['P/E'] = extract_value_with_regex(
            get_element_text(driver, By.CLASS_NAME, "col_pe"),
            r'(\d+\.\d+)x'
        )
        
        volume_text = get_element_text(driver, By.CLASS_NAME, "col_volume")
        volume_match = re.search(r'(\d+\.?\d*)M?', volume_text)
        if volume_match:
            volume = volume_match.group(1)
            if 'B' in volume_text:
                volume = float(volume) * 1000000000
            elif 'M' in volume_text:
                volume = float(volume) * 1000000
            elif 'K' in volume_text:
                volume = float(volume) * 1000
            data['VOLUME'] = str(volume)
        
        # Log completion
        thread_logger.info(f"Scraping completed for stock {stock_code}")
        
        # Format the output string for console display
        output = f"{data['CODE']},{data['DATE']},{data['OPEN']},{data['INTRADAY_HIGH']},{data['INTRADAY_LOW']},{data['CLOSE']},{data['P/E']},{data['VOLUME']},{data['STATUS']}"
        
        # Print to console with lock
        with print_lock:
            print(output)
        
        # Write directly to CSV
        write_to_csv(data)
        
        return True
    
    except Exception as e:
        thread_logger.error(f"Error scraping stock {stock_code}: {e}", exc_info=True)
        
        # Create data entry for error case
        data = {
            'CODE': stock_code,
            'DATE': formatted_date,
            'OPEN': 'N/A',
            'INTRADAY_HIGH': 'N/A',
            'INTRADAY_LOW': 'N/A',
            'CLOSE': 'N/A',
            'P/E': 'N/A',
            'VOLUME': 'N/A',
            'STATUS': 'Error'
        }
        
        # Write to CSV so we have a record of the failure
        write_to_csv(data)
        
        return False
    
    finally:
        if driver:
            driver.quit()

def sort_csv_file():
    """Sort the CSV file by stock code"""
    try:
        # Check if the temporary file exists
        if not os.path.exists(CSV_TEMP_PATH):
            logger.error("No CSV file found to sort")
            return False
            
        # Read the CSV into a pandas DataFrame
        df = pd.read_csv(CSV_TEMP_PATH)
        
        # Sort by CODE (convert to integer if possible)
        try:
            df['CODE'] = df['CODE'].astype(int)
        except:
            # If conversion fails, keep as string
            pass
            
        # Sort the DataFrame
        df_sorted = df.sort_values(by=['CODE'])
        
        # Write the sorted DataFrame to the final CSV file
        df_sorted.to_csv(CSV_FINAL_PATH, index=False)
        
        # Optionally remove the temporary file
        os.remove(CSV_TEMP_PATH)
        
        # Generate some statistics
        total_records = len(df)
        success_records = len(df[df['STATUS'] == 'Success'])
        error_records = len(df[df['STATUS'] == 'Error'])
        
        logger.info(f"CSV file sorted successfully. Total records: {total_records}")
        logger.info(f"  Success: {success_records}, Errors: {error_records}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error sorting CSV file: {e}", exc_info=True)
        return False

def scrape_stocks_multithreaded(base_url, stock_codes, max_workers=4):
    """
    Scrape multiple stock codes using multi-threading
    """
    logger.info(f"Starting multi-threaded scraping with {max_workers} workers")
    
    # Create a fresh CSV file (will overwrite if exists)
    if os.path.exists(CSV_TEMP_PATH):
        os.remove(CSV_TEMP_PATH)
    
    successful_scrapes = 0
    failed_scrapes = 0
    
    # Using ThreadPoolExecutor to manage threads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit scraping jobs to the thread pool
        future_to_stock = {executor.submit(scrape_stock_data, code, base_url): code for code in stock_codes}
        
        # Process results as they complete
        for future in as_completed(future_to_stock):
            stock_code = future_to_stock[future]
            try:
                result = future.result()
                if result:
                    successful_scrapes += 1
                else:
                    failed_scrapes += 1
            except Exception as e:
                logger.error(f"Exception with stock {stock_code}: {e}")
                failed_scrapes += 1
    
    logger.info(f"Multi-threaded scraping completed. Success: {successful_scrapes}, Failed: {failed_scrapes}")
    return successful_scrapes, failed_scrapes

def main():
    """
    Main function that runs the multi-threaded scraper
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Stock market data scraper')
    parser.add_argument('-n', type=int, default=50,
                      help='Number of stock codes to scrape (default: 50)')
    args = parser.parse_args()

    # Base URL
    base_url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote"
    
    stock_codes = [str(i) for i in range(1, args.n + 1)]
    logger.info(f"Generated {len(stock_codes)} sequential stock codes")
    
    # Determine optimal thread count based on system
    max_threads = min(10, os.cpu_count() or 4)
    
    # Start scraping
    successful, failed = scrape_stocks_multithreaded(base_url, stock_codes, max_threads)
    
    # Sort the CSV file after all scraping is complete
    logger.info("Sorting CSV file...")
    sort_csv_file()
    logger.info(f"CSV data sorted and written to {CSV_FINAL_PATH}")
    
    logger.info(f"Script execution completed. Processed {successful + failed} stocks.")

if __name__ == "__main__":
    main()