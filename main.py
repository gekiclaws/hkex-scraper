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


def main():
    base_url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote"

    stock_codes = (
        [str(i) for i in range(1, 1501)]
        + [str(i) for i in range(1501, 2650)]
        + [str(i) for i in range(3300, 3400)]
        + [str(i) for i in range(3600, 3700)]
        + [str(i) for i in range(9850, 10000)]
    )

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