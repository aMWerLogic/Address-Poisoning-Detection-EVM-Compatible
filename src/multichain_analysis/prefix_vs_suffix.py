import csv
from collections import Counter, defaultdict
from pathlib import Path

import polars as pl

reverse_arr = [False, True]
chains = ["opbnb", "polygonzk", "arbitrum", "optimism", "avalanche", "gnosis"]
attack_types = ["zero", "fake"]
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent


def _normalize_address(value) -> str:
	if value is None:
		return ""
	address = str(value).strip().lower()
	if address.startswith("0x"):
		return address[2:]
	return address


def _matching_prefix_length(left, right) -> int:
	a = _normalize_address(left)
	b = _normalize_address(right)
	limit = min(len(a), len(b))
	count = 0
	for i in range(limit):
		if a[i] != b[i]:
			break
		count += 1
	return count


def _matching_suffix_length(left, right) -> int:
	a = _normalize_address(left)
	b = _normalize_address(right)
	limit = min(len(a), len(b))
	count = 0
	for i in range(1, limit + 1):
		if a[-i] != b[-i]:
			break
		count += 1
	return count


def _collect_heatmap_data():
	"""Collect prefix vs suffix length data for heatmap."""
	heatmap_counter = defaultdict(lambda: defaultdict(int))
	prefix_counter = Counter()
	suffix_counter = Counter()
	seen_transaction_keys = set()

	for chain in chains:
		for attack_type in attack_types:
			for reversed_direction in reverse_arr:
				path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_results_filtered_{reversed_direction}.csv"
				if not path.exists():
					print(f"File not found: {path}")
					continue

				df = pl.read_csv(path, infer_schema_length=False)
				required_columns = {"attacker", "top_address", "transaction_key"}
				if not required_columns.issubset(set(df.columns)):
					print(f"Skipping {path}: missing columns {required_columns - set(df.columns)}")
					continue

				for transaction_key, attacker, top_address in df.select(["transaction_key", "attacker", "top_address"]).iter_rows():
					if transaction_key in seen_transaction_keys:
						continue
					seen_transaction_keys.add(transaction_key)
					prefix_len = _matching_prefix_length(attacker, top_address)
					suffix_len = _matching_suffix_length(attacker, top_address)
					prefix_counter[prefix_len] += 1
					suffix_counter[suffix_len] += 1
					heatmap_counter[prefix_len][suffix_len] += 1

	return heatmap_counter, prefix_counter, suffix_counter


def analyse_prefix_vs_suffix_lengths(
	prefix_output_path: str = "prefix_length_distribution.csv",
	suffix_output_path: str = "suffix_length_distribution.csv",
	heatmap_output_path: str = "prefix_vs_suffix_heatmap.csv",
):
	heatmap_counter, prefix_counter, suffix_counter = _collect_heatmap_data()

	prefix_rows = [[length, count] for length, count in sorted(prefix_counter.items())]
	suffix_rows = [[length, count] for length, count in sorted(suffix_counter.items())]

	with open(prefix_output_path, "w", newline="", encoding="utf-8") as f:
		writer = csv.writer(f)
		writer.writerow(["prefix_length", "count"])
		writer.writerows(prefix_rows)

	with open(suffix_output_path, "w", newline="", encoding="utf-8") as f:
		writer = csv.writer(f)
		writer.writerow(["suffix_length", "count"])
		writer.writerows(suffix_rows)

	if heatmap_counter:
		all_prefix_lengths = sorted(heatmap_counter.keys())
		all_suffix_lengths = sorted(set(suffix for lengths in heatmap_counter.values() for suffix in lengths.keys()))

		with open(heatmap_output_path, "w", newline="", encoding="utf-8") as f:
			writer = csv.writer(f)
			writer.writerow(["prefix_length"] + all_suffix_lengths)
			for prefix_len in all_prefix_lengths:
				row = [prefix_len]
				for suffix_len in all_suffix_lengths:
					row.append(heatmap_counter[prefix_len][suffix_len])
				writer.writerow(row)

		print(f"Heatmap data saved to: {heatmap_output_path}")

	print(f"Prefix length distribution saved to: {prefix_output_path}")
	print(f"Suffix length distribution saved to: {suffix_output_path}")



