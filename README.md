# HKEX Stock Scraper

This project is a multi-threaded web scraper for collecting equity quote data from the Hong Kong Exchange (HKEX).
It uses **Playwright** for browser automation and is designed for speed, reliability, and scale.

---

## Overview

* Scrapes HKEX equity quote pages by stock code
* Uses Playwright instead of Selenium for better stability and performance
* Runs multiple worker threads safely
* Writes results incrementally to CSV
* Sorts and finalizes output after completion

---

## Prerequisites

* Python 3.8 or higher
* pip
* Internet connection

No browser drivers are required. Playwright manages browsers automatically.

---

## Installation

1. Clone the repository:

```bash
git clone <your-repository-url>
cd scraper
```

2. Create and activate a virtual environment:

```bash
# macOS / Linux
python3 -m venv venv
source venv/bin/activate

# Windows
python -m venv venv
.\venv\Scripts\activate
```

3. Install Python dependencies:

```bash
pip install -r requirements.txt
```

4. Install Playwright browsers:

```bash
playwright install chromium
```

---

## Project Structure

```
.
├── main.py                # Orchestration and threading
├── scraper.py             # Playwright scraping logic
├── io_utils.py            # CSV writing and finalization
├── requirements.txt
├── stock_data.csv         # Final output (not tracked)
├── stock_scraper_debug.log
```

---

## Usage

Activate your virtual environment, then run:

```bash
python main.py
```

The script will:

* Generate a predefined set of HKEX stock codes
* Scrape each code using Playwright
* Write results to a temporary CSV
* Sort and output the final `stock_data.csv`

Concurrency is controlled internally via a worker pool.

---

## Architecture Notes

* **Playwright replaces Selenium**

  * Faster startup
  * Fewer flaky waits
  * No WebDriver dependency
* **Thread-safe design**

  * One Playwright instance per worker thread
  * One browser per worker
  * New context and page per stock
* **I/O separation**

  * Scraping logic and file I/O are isolated into separate modules

---

## Output

The final CSV contains one row per stock code with the following fields:

* CODE
* DATE
* OPEN
* INTRADAY_HIGH
* INTRADAY_LOW
* CLOSE
* P/E
* VOLUME
* STATUS

Rows are written even on failure to ensure completeness.

---

## Dependencies

Main dependencies:

* playwright
* pandas
* concurrent.futures
* logging

Selenium and ChromeDriver are no longer used.

---

## Notes

* Output CSV and log files are not tracked in Git
* The scraper is designed for large batch runs
* Network or site-side throttling may still occur

---

## Troubleshooting

If issues occur:

1. Check `stock_scraper_debug.log`
2. Confirm Playwright browsers are installed
3. Verify network connectivity
4. Reduce worker count if the target site rate-limits