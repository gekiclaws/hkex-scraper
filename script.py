import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import csv
import logging
import datetime
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

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
    
    # Add page load timeout
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(20)  # 20 second timeout for page loads
    return driver

def is_404_page(driver):
    """
    Enhanced check for 404 error pages using multiple indicators
    """
    try:
        # Method 1: Check URL redirect to error page
        if "404.aspx" in driver.current_url or "error" in driver.current_url.lower():
            return True
        
        # Method 2: Check page title for error indicators
        page_title = driver.title.lower()
        if "404" in page_title or "not found" in page_title or "error" in page_title:
            return True
        
        # Method 3: Check for common error messages in page content
        page_text = driver.page_source.lower()
        error_indicators = ["page not found", "404 error", "does not exist", "unavailable"]
        if any(indicator in page_text for indicator in error_indicators):
            return True
        
        # Method 4: Check for mandatory stock elements
        # If none of these essential elements are found, it's likely not a valid stock page
        essential_elements = [
            driver.find_elements(By.CLASS_NAME, "col_open"),
            driver.find_elements(By.CLASS_NAME, "col_high"),
            driver.find_elements(By.CLASS_NAME, "col_low")
        ]
        
        # If all essential elements are missing, consider it a 404
        if all(len(elements) == 0 for elements in essential_elements):
            return True
            
    except Exception as e:
        logger.debug(f"Error checking for 404 page: {e}")
    
    return False

def validate_stock_page(driver):
    """
    Validates if the page contains actual stock data
    Returns True if valid, False otherwise
    """
    try:
        # Check if at least one of these elements contains numeric data
        data_indicators = [
            (By.CLASS_NAME, "col_open"),
            (By.CLASS_NAME, "col_high"),
            (By.CLASS_NAME, "col_prevcls")
        ]
        
        for by_method, selector in data_indicators:
            elements = driver.find_elements(by_method, selector)
            if elements:
                for element in elements:
                    # Check if element contains a price value
                    if element.text and re.search(r'HK\$\d+\.\d+', element.text):
                        return True
        
        # If we get here, no valid stock data was found
        return False
        
    except Exception as e:
        logger.debug(f"Error validating stock page: {e}")
        return False

def get_element_with_retry(driver, by_method, selector, retry_time=15):
    """Try to get an element for up to retry_time seconds"""
    start_time = time.time()
    
    while time.time() - start_time < retry_time:
        try:
            elements = driver.find_elements(by_method, selector)
            if elements and elements[0].text:
                return elements[0]
        except:
            pass
        time.sleep(0.5)
    
    return None

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
    thread_logger.info(f"STARTED scraping stock code: {stock_code}")
    
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
            thread_logger.info(f"Timeout or error loading stock {stock_code}: {e}")
            data['STATUS'] = 'TimeoutOrError'
            write_to_csv(data)
            return False
        
        # Short wait for initial page load
        time.sleep(1)
        
        # Check for 404 immediately
        if is_404_page(driver):
            thread_logger.info(f"Stock {stock_code} returned a 404 error - skipping")
            data['STATUS'] = 'InvalidStockCode'
            
            # Write to CSV and console even for 404 cases
            with print_lock:
                print(f"{stock_code},{formatted_date},N/A,N/A,N/A,N/A,N/A,N/A,404_NotFound")
                
            write_to_csv(data)
            return False
        
        # Validate that the page contains actual stock data
        if not validate_stock_page(driver):
            thread_logger.info(f"Stock {stock_code} page loaded but no valid stock data found")
            data['STATUS'] = 'NoStockData'
            
            with print_lock:
                print(f"{stock_code},{formatted_date},N/A,N/A,N/A,N/A,N/A,N/A,NoStockData")
                
            write_to_csv(data)
            return False
        
        # Extract open price
        open_element = get_element_with_retry(driver, By.CLASS_NAME, "col_open")
        if open_element:
            data['OPEN'] = extract_value_with_regex(open_element.text, r'HK\$(\d+\.\d+)')
        
        # Extract intraday high
        high_element = get_element_with_retry(driver, By.CLASS_NAME, "col_high")
        if high_element:
            data['INTRADAY_HIGH'] = extract_value_with_regex(high_element.text, r'HK\$(\d+\.\d+)')
        
        # Extract intraday low
        low_element = get_element_with_retry(driver, By.CLASS_NAME, "col_low")
        if low_element:
            data['INTRADAY_LOW'] = extract_value_with_regex(low_element.text, r'HK\$(\d+\.\d+)')
        
        # Extract close price
        close_element = get_element_with_retry(driver, By.CLASS_NAME, "col_prevcls")
        if close_element:
            data['CLOSE'] = extract_value_with_regex(close_element.text, r'HK\$(\d+\.\d+)')
        
        # Extract P/E ratio
        pe_element = get_element_with_retry(driver, By.CLASS_NAME, "col_pe")
        if pe_element:
            data['P/E'] = extract_value_with_regex(pe_element.text, r'(\d+\.\d+)x')
        
        # Extract volume
        volume_element = get_element_with_retry(driver, By.CLASS_NAME, "col_volume")
        if volume_element:
            volume_match = re.search(r'(\d+\.?\d*)M?', volume_element.text)
            if volume_match:
                volume = volume_match.group(1)
                if 'M' in volume_element.text:
                    volume = float(volume) * 1000000
                data['VOLUME'] = str(volume)
        
        # Final data validation check - if all data values are N/A, mark as error
        if all(value == 'N/A' for key, value in data.items() if key not in ['CODE', 'DATE', 'STATUS']):
            thread_logger.info(f"Stock {stock_code} returned all N/A values - marking as invalid")
            data['STATUS'] = 'AllDataNA'
            
            with print_lock:
                print(f"{stock_code},{formatted_date},N/A,N/A,N/A,N/A,N/A,N/A,AllDataNA")
                
            write_to_csv(data)
            return False
        
        # Log completion
        thread_logger.info(f"Scraping completed for stock {stock_code}. Results: {data}")
        
        # Format the output string for console display
        output = f"{data['CODE']},{data['DATE']},{data['OPEN']},{data['INTRADAY_HIGH']},{data['INTRADAY_LOW']},{data['CLOSE']},{data['P/E']},{data['VOLUME']},{data['STATUS']}"
        
        # Print to console with lock
        with print_lock:
            print(output)
        
        # Write directly to CSV
        write_to_csv(data)
        
        return True
    
    except Exception as e:
        thread_logger.error(f"Error in scraping stock {stock_code}: {e}", exc_info=True)
        
        # Create data entry for error case
        data = {
            'CODE': stock_code,
            'DATE': datetime.datetime.now().strftime('%Y%m%d'),
            'OPEN': 'N/A',
            'INTRADAY_HIGH': 'N/A',
            'INTRADAY_LOW': 'N/A',
            'CLOSE': 'N/A',
            'P/E': 'N/A',
            'VOLUME': 'N/A',
            'STATUS': f'Error: {str(e)[:50]}'  # Truncate very long error messages
        }
        
        # Still write to CSV so we have a record of the failure
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
            logger.error("No temp CSV file found to sort")
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
        invalid_records = len(df[df['STATUS'].isin(['InvalidStockCode', '404_NotFound', 'NoStockData', 'AllDataNA'])])
        error_records = total_records - success_records - invalid_records
        
        logger.info(f"CSV file sorted successfully. Total records: {total_records}")
        logger.info(f"  Success: {success_records}, Invalid: {invalid_records}, Errors: {error_records}")
        
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
    # Base URL
    base_url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote"
    
    num_stocks = 100  # Adjust as needed
    stock_codes = [str(i) for i in range(1, num_stocks + 1)]
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