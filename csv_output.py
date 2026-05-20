import csv
import logging
import os
import threading

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


def code_sort_key(row: dict):
    code = row.get("CODE", "")
    try:
        return (0, int(code), code)
    except ValueError:
        return (1, code)


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


def sort_and_finalize_csv(output_path: str = CSV_FINAL_PATH) -> bool:
    """Sort temp CSV by CODE and write final CSV. Deletes temp file."""
    try:
        if not os.path.exists(CSV_TEMP_PATH):
            logger.error("No temp CSV found to finalize")
            return False

        with open(CSV_TEMP_PATH, newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = list(reader)

        if fieldnames is None:
            logger.error("No CSV headers found to finalize")
            return False

        rows.sort(key=code_sort_key)

        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        os.remove(CSV_TEMP_PATH)

        total = len(rows)
        success = sum(1 for row in rows if row.get("STATUS") == "Success")
        error = sum(1 for row in rows if row.get("STATUS") == "Error")
        logger.info(f"Finalized CSV. Path={output_path} Total={total} Success={success} Error={error}")

        return True

    except Exception as e:
        logger.error("Failed to finalize CSV", exc_info=e)
        return False
