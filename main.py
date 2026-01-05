import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil

from io_utils import reset_temp_csv, sort_and_finalize_csv
from scraper import scrape_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def chunk(lst, n):
    size = ceil(len(lst) / n)
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def load_stock_codes_from_file(path: str) -> list[str]:
    codes: list[str] = []

    with open(path, "r") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            if "-" not in line:
                raise ValueError(f"Invalid range line: {line}")

            start_s, end_s = line.split("-", 1)

            try:
                start = int(start_s)
                end = int(end_s)
            except ValueError:
                raise ValueError(f"Invalid integers in line: {line}")

            if start >= end:
                raise ValueError(f"Invalid range (start >= end): {line}")

            codes.extend(str(i) for i in range(start, end))

    return codes

def main():
    base_url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote"

    RANGES_FILE = "stock_ranges.txt"

    stock_codes = load_stock_codes_from_file(RANGES_FILE)
    logger.info(f"Loaded {len(stock_codes)} stock codes from {RANGES_FILE}")

    reset_temp_csv()

    max_workers = 8
    chunks = list(chunk(stock_codes, max_workers))

    total_success = 0
    total_fail = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(scrape_worker, c, base_url)
            for c in chunks
        ]

        for f in as_completed(futures):
            s, e = f.result()
            total_success += s
            total_fail += e

    logger.info(f"Scrape complete. Success={total_success} Fail={total_fail}")
    sort_and_finalize_csv()
    logger.info("Done")


if __name__ == "__main__":
    main()