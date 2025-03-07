import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import re
import csv
import logging
import datetime
import os
import threading
import pandas as pd
import sys
from queue import Queue

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

# Thread-safe queue for tracking stock codes to process
stock_queue = Queue()

def setup_driver():
    """Create and return a configured WebDriver instance"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.page_load_strategy = 'eager'
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(30)
    return driver

def get_element_with_retry(driver, by_method, selector, retry_time=15):
    """Try to get an element with explicit timeout"""
    start_time = time.time()
    
    while time.time() - start_time < retry_time:
        try:
            element = driver.find_element(by_method, selector)
            if element and element.text:
                return element
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
            fieldnames = ['CODE', 'DATE', 'OPEN', 'INTRADAY_HIGH', 'INTRADAY_LOW', 'CLOSE', 'P/E', 'VOLUME']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerow(data)
            csvfile.flush()
            os.fsync(csvfile.fileno())

def scrape_stock_data(base_url, thread_id):
    """Worker function that continuously pulls stock codes from the queue and processes them"""
    thread_logger = logging.getLogger(f"Thread-{thread_id}")
    thread_logger.info(f"Worker thread {thread_id} started")
    
    driver = None
    
    try:
        # Initialize driver just once per thread
        driver = setup_driver()
        
        while not stock_queue.empty():
            try:
                # Get the next stock code from the queue with a timeout
                try:
                    stock_code = stock_queue.get(timeout=1)
                except:
                    # Queue is empty or timeout occurred
                    break
                
                thread_logger.info(f"STARTED scraping stock code: {stock_code}")
                url = f"{base_url}?sym={stock_code}"
                
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
                    'VOLUME': 'N/A'
                }
                
                # Load the page with a retry mechanism
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        driver.get(url)
                        break
                    except Exception as e:
                        if retry == max_retries - 1:
                            thread_logger.error(f"Failed to load page for stock {stock_code} after {max_retries} attempts: {e}")
                            raise
                        else:
                            thread_logger.warning(f"Retrying page load for stock {stock_code} (attempt {retry+1}/{max_retries})")
                            time.sleep(2)
                
                # Wait for initial page load
                time.sleep(2)
                
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
                
                # Log completion
                thread_logger.info(f"Scraping completed for stock {stock_code}. Results: {data}")
                
                # Format the output string for console display
                output = f"{data['CODE']},{data['DATE']},{data['OPEN']},{data['INTRADAY_HIGH']},{data['INTRADAY_LOW']},{data['CLOSE']},{data['P/E']},{data['VOLUME']}"
                
                # Print to console with lock
                with print_lock:
                    print(output)
                
                # Write directly to CSV
                write_to_csv(data)
                
                # Mark task as done in the queue
                stock_queue.task_done()
                
            except Exception as e:
                thread_logger.error(f"Error in scraping stock: {e}", exc_info=True)
                # Even if there's an error, mark the task as done
                try:
                    stock_queue.task_done()
                except:
                    pass
                
                # Create a new driver if the current one is in a bad state
                try:
                    if driver:
                        driver.quit()
                except:
                    pass
                driver = setup_driver()
    
    finally:
        # Always close the driver
        if driver:
            try:
                driver.quit()
                thread_logger.debug(f"Driver closed for worker {thread_id}")
            except:
                pass
        
        thread_logger.info(f"Worker thread {thread_id} finished")

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
            pass
            
        # Sort the DataFrame
        df_sorted = df.sort_values(by=['CODE'])
        
        # Write the sorted DataFrame to the final CSV file
        df_sorted.to_csv(CSV_FINAL_PATH, index=False)
        
        # Remove the temporary file
        os.remove(CSV_TEMP_PATH)
        
        logger.info(f"CSV file sorted successfully. Total records: {len(df)}")
        return True
        
    except Exception as e:
        logger.error(f"Error sorting CSV file: {e}", exc_info=True)
        return False

def process_stocks_with_fixed_threads(base_url, stock_codes, num_threads=4):
    """
    Process stocks using a fixed number of worker threads pulling from a queue
    This approach is more reliable than ThreadPoolExecutor for this specific scenario
    """
    logger.info(f"Starting stock processing with {num_threads} worker threads")
    
    # Create a fresh CSV file
    if os.path.exists(CSV_TEMP_PATH):
        os.remove(CSV_TEMP_PATH)
    
    # Fill the queue with all stock codes
    for code in stock_codes:
        stock_queue.put(code)
    
    # Create and start the worker threads
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=scrape_stock_data, args=(base_url, i))
        thread.daemon = True  # Make threads daemon so they exit when main thread exits
        threads.append(thread)
        thread.start()
    
    # Monitor progress
    total_stocks = len(stock_codes)
    
    try:
        # Monitor until the queue is empty
        while not stock_queue.empty():
            remaining = stock_queue.qsize()
            completed = total_stocks - remaining
            logger.info(f"Progress: {completed}/{total_stocks} stocks processed ({completed/total_stocks*100:.1f}%)")
            
            # Print the 3 most recent stocks being processed
            time.sleep(5)
        
        # Wait for all threads to complete any remaining work
        stock_queue.join()  # This blocks until all items in the queue have been processed
        
        logger.info("All stocks have been processed")
        return True
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Stopping gracefully...")
        return False

def main():
    """Main function that runs the stock scraper"""
    # Base URL
    base_url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote"
    
    # Number of stocks to scrape
    num_stocks = 15  # Adjust as needed
    
    # Generate list of stock codes
    stock_codes = [str(i) for i in range(1, num_stocks + 1)]
    
    logger.info(f"Starting stock scraper for {num_stocks} stocks")
    
    # Determine thread count 
    num_threads = min(4, os.cpu_count() or 2)
    
    # Start processing
    success = process_stocks_with_fixed_threads(base_url, stock_codes, num_threads)
    
    # Sort the CSV file after all processing is complete
    if success:
        logger.info("Sorting CSV file...")
        sort_csv_file()
        logger.info(f"CSV data sorted and written to {CSV_FINAL_PATH}")
    else:
        logger.warning("Processing did not complete successfully, but attempting to sort any collected data...")
        if os.path.exists(CSV_TEMP_PATH):
            sort_csv_file()
    
    logger.info("Script execution completed.")

if __name__ == "__main__":
    main()