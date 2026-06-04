#Quick script to search for a transaction key in a results CSV file.
#Usage: python lookup_tx_key.py
import csv
import re
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR.parent

TARGET_FILE = SRC_DIR / "results_arbitrum" / "arbitrum_fake_results_filtered_False.csv"
#TARGET_FILE = SRC_DIR / "results_arbitrum" / "arbitrum_zero_results_filtered_False.csv"
SEARCH_KEY = "('0x048ce1c0d44dfbf004d78ac7332ec1945dc6cc77cde9988b4c48bbec2c2dc408', 62)"
#TARGET_FILE = SRC_DIR / "results_arbitrum" / "arbitrum_fake_step1_False.csv"


def normalize_key(raw: str) -> str | None:
    text = raw.strip()
    match = re.match(
        r"^\(\s*['\"]?(0x[a-fA-F0-9]{64})['\"]?\s*,\s*([0-9]+(?:\.[0-9]+)?)\s*\)$",
        text,
    )
    if match:
        return f"({match.group(1).lower()},{int(float(match.group(2)))})"
    return None


def main():
    target_norm = normalize_key(SEARCH_KEY)
    if target_norm is None:
        print(f"Could not parse search key: {SEARCH_KEY!r}", file=sys.stderr)
        sys.exit(1)

    if not TARGET_FILE.exists():
        print(f"File not found: {TARGET_FILE}", file=sys.stderr)
        sys.exit(1)

    found = []
    with TARGET_FILE.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  #start=2: row 1 is header
            raw_key = row.get("transaction_key", "")
            if normalize_key(raw_key) == target_norm:
                found.append((row_num, row))

    if not found:
        print(f"Key not found: {SEARCH_KEY}")
    else:
        for row_num, row in found:
            print(f"Found at row {row_num}:")
            for col, val in row.items():
                print(f"  {col}: {val}")
            print()


if __name__ == "__main__":
    main()
