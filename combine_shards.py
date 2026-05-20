import argparse
import csv
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Combine HKEX scraper shard CSVs.")
    parser.add_argument(
        "--input-dir",
        default="shard-artifacts",
        help="Directory containing stock_data_shard_*.csv files.",
    )
    parser.add_argument(
        "--output",
        default="stock_data.csv",
        help="Path for the combined CSV output.",
    )
    return parser.parse_args()


def code_sort_key(row: dict):
    code = row.get("CODE", "")
    try:
        return (0, int(code), code)
    except ValueError:
        return (1, code)


def combine_shards(input_dir: str, output_path: str) -> None:
    shard_dir = Path(input_dir)
    shard_paths = sorted(shard_dir.glob("stock_data_shard_*.csv"))

    if not shard_paths:
        raise SystemExit(f"No shard CSVs found in {shard_dir}")

    rows = []
    fieldnames = None

    for path in shard_paths:
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            if fieldnames is None:
                fieldnames = reader.fieldnames
            elif reader.fieldnames != fieldnames:
                raise SystemExit(f"Unexpected CSV headers in {path}: {reader.fieldnames}")

            rows.extend(reader)

    if fieldnames is None:
        raise SystemExit("No CSV headers found")

    rows.sort(key=code_sort_key)

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Combined {len(shard_paths)} shards into {output_path} with {len(rows)} rows")


def main():
    args = parse_args()
    combine_shards(args.input_dir, args.output)


if __name__ == "__main__":
    main()
