import os
import logging
from pymongo import MongoClient
import pandas as pd
import argparse
from dotenv import load_dotenv

# — Logging setup —
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

# — Args —
p = argparse.ArgumentParser(description="Export stock_data from MongoDB to CSV")
p.add_argument(
    "--outfile", "-o",
    default="stock_data_export.csv",
    help="Path for the output CSV"
)
args = p.parse_args()

# — MongoDB setup —
load_dotenv()  # Load environment variables from .env file
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI must be set")
client = MongoClient(MONGO_URI)
db = client["stock_db"]
collection = db["stock_data"]

def export_to_csv(filepath: str):
    docs = list(collection.find({}, {"_id": 0}))
    if not docs:
        logger.warning("No documents found in MongoDB.")
        return
    df = pd.DataFrame(docs)
    # ─── sort by CODE ───
    # if CODE is numeric:
    df['CODE'] = pd.to_numeric(df['CODE'], errors='ignore')
    df.sort_values(by='CODE', inplace=True)
    # ─────────────────────

    # convert DATE back to YYYYMMDD
    if "DATE" in df.columns:
        df["DATE"] = df["DATE"].astype(str).str.replace(" 00:00:00", "")
    df.to_csv(filepath, index=False)

    # If DATE is a datetime, convert back to YYYYMMDD
    if "DATE" in df.columns:
        df["DATE"] = df["DATE"].astype(str).str.replace(" 00:00:00", "")
    df.to_csv(filepath, index=False)
    logger.info(f"Exported {len(df)} records to {filepath}")

if __name__ == "__main__":
    logger.info("Starting export…")
    export_to_csv(args.outfile)
    logger.info("Done.")