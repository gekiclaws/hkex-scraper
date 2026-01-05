import csv
import logging
import os
import threading

import pandas as pd

logger = logging.getLogger(__name__)

CSV_TEMP_PATH = "stock_data_temp.csv"
CSV_FINAL_PATH = "stock_data.csv"

FIELDNAMES = [
    "CODE",
    "DATE",
    "OPEN",
    "INTRADAY_HIGH",
    "INTRADAY_LOW",
    "CLOSE",
    "P/E",
    "VOLUME",
    "STATUS",
]

_csv_lock = threading.Lock()


def reset_temp_csv() -> None:
    """Remove temp CSV if it exists."""
    with _csv_lock:
        if os.path.exists(CSV_TEMP_PATH):
            os.remove(CSV_TEMP_PATH)


def write_row(row: dict) -> None:
    """Append one row to temp CSV. Thread-safe."""
    with _csv_lock:
        file_exists = os.path.isfile(CSV_TEMP_PATH)
        with open(CSV_TEMP_PATH, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDNAMES)
            if not file_exists:
                w.writeheader()
            w.writerow(row)


def sort_and_finalize_csv() -> bool:
    """Sort temp CSV by CODE and write final CSV. Deletes temp file."""
    try:
        if not os.path.exists(CSV_TEMP_PATH):
            logger.error("No temp CSV found to finalize")
            return False

        df = pd.read_csv(CSV_TEMP_PATH)

        # Try to sort numerically if possible
        try:
            df["CODE"] = df["CODE"].astype(int)
        except Exception:
            pass

        df_sorted = df.sort_values(by=["CODE"])
        df_sorted.to_csv(CSV_FINAL_PATH, index=False)

        os.remove(CSV_TEMP_PATH)

        total = len(df_sorted)
        success = int((df_sorted["STATUS"] == "Success").sum())
        error = int((df_sorted["STATUS"] == "Error").sum())
        logger.info(f"Finalized CSV. Total={total} Success={success} Error={error}")

        return True

    except Exception as e:
        logger.error("Failed to finalize CSV", exc_info=e)
        return False