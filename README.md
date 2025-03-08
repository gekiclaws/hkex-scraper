# HKEX Stock Scraper

This project is a web scraping tool that uses various Python libraries to collect and process stock data from HKEX.

## Prerequisites

- Python 3.7 or higher
- pip (Python package installer)
- Chrome browser (for Selenium)

## Installation

1. Clone this repository:
```bash
git clone <your-repository-url>
cd scraper
```

2. Create and activate a virtual environment (recommended):
```bash
# On macOS/Linux
python3 -m venv venv
source venv/bin/activate

# On Windows
python -m venv venv
.\venv\Scripts\activate
```

3. Install the required dependencies:
```bash
pip install -r requirements.txt
```

4. Install Chrome WebDriver for Selenium:
   - Download ChromeDriver from [https://sites.google.com/chromium.org/driver/](https://sites.google.com/chromium.org/driver/)
   - Make sure the ChromeDriver version matches your Chrome browser version
   - Add ChromeDriver to your system PATH or place it in the project directory

## Project Structure
- `script.py`: Main scraping script
- `requirements.txt`: List of Python dependencies
- `stock_data.csv`: Output file for scraped data (not tracked in Git)
- `stock_scraper_debug.log`: Debug log file (not tracked in Git)

## Usage

1. Make sure your virtual environment is activated:
```bash
# On macOS/Linux
source venv/bin/activate

# On Windows
.\venv\Scripts\activate
```

2. Run the scraper:
```bash
# Basic usage (will scrape 50 stocks by default)
python3 script.py

# Specify number of stocks to scrape (e.g., scrape 100 stocks)
python3 script.py -n 100
```

Command-line arguments:
- `-n`: Number of stock codes to scrape (default: 50)
  Example: `python3 script.py -n 75` will scrape 75 stocks

The script will:
- Scrape data from the specified websites
- Save the results to `stock_data.csv`
- Generate debug logs in `stock_scraper_debug.log`

## Dependencies

The project uses the following main dependencies:
- beautifulsoup4 (>=4.9.3): For parsing HTML
- requests (>=2.25.1): For making HTTP requests
- pandas (>=1.2.0): For data manipulation and CSV handling
- concurrent.futures (>=3.7.0): For parallel processing
- logging (>=0.5.1): For debug logging
- selenium (>=4.0.0): For browser automation
- argparse (>=1.4.0): For command-line argument parsing

## Notes

- The script generates CSV and log files which are not tracked in Git
- Make sure you have sufficient disk space for storing scraped data
- The script includes error handling and logging for debugging purposes

## Troubleshooting

If you encounter any issues:
1. Check the `stock_scraper_debug.log` file for error messages
2. Ensure all dependencies are correctly installed
3. Verify that ChromeDriver is properly installed and matches your Chrome version
4. Make sure you have a stable internet connection

## License

[Your chosen license] 