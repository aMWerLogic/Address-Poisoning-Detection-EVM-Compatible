import polars as pl
from pathlib import Path
import requests
import csv
from dotenv import load_dotenv
import os

reverse_arr = [False,True]
chains = ["opbnb", "polygonzk", "arbitrum", "optimism", "avalanche", "gnosis"]
BASE_DIR = Path(__file__).resolve().parent  
PROJECT_ROOT = BASE_DIR.parent

def export_unique_tokens(output_path: str = "fake_tokens_from_results.csv"):
    for chain in chains:
        unique_tokens = {}
        for reversed in reverse_arr:
            path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_fake_results_filtered_{reversed}.csv"
            if not path.exists():
                print(f"File not found: {path}")
                continue
            df = pl.read_csv(path, infer_schema_length=False)
            for row in df.iter_rows(named=True):
                address = row.get("contract")
                name = row.get("contract_name") or ""
                symbol = row.get("contract_symbol") or ""
                if not symbol:
                    symbol = name
                if address:
                    unique_tokens[address] = (name, symbol)
        output_file = Path(f"{chain}_fake_tokens_from_results.csv")
        with output_file.open("w", encoding="utf-8") as f:
            f.write("address\tname\tsymbol\n")
            for address, (name, symbol) in unique_tokens.items():
                f.write(f"{address}\t{name}\t{symbol}\n")
    print(f"Wrote {len(unique_tokens)} unique tokens to {output_file}")


def analyse_fake_tokens_count():
    count = 0
    unique_tokens = set()
    for chain in chains:
        chain_tokens = set()
        for reversed in reverse_arr:
            path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_fake_results_filtered_{reversed}.csv"
            if not path.exists():
                continue
            df = pl.read_csv(path, infer_schema_length=False)
            df = df.filter(
                (pl.col("possible_utility_victim") == "False") &
                (pl.col("possible_utility_attacker") == "False") 
                )
            unique_addresses = (
                df.select("contract")
                  .unique()
                  .to_series()
            )
            print(len(unique_addresses))
            for address in unique_addresses:
                if address in unique_tokens and not address in chain_tokens:
                    count+=1
                    print(chain)
                    print(address)
                unique_tokens.add(address)
                chain_tokens.add(address)
    print("Total fake tokens detected across chains:", len(unique_tokens))
    print("Number of reused tokens:", count)


def analyse_frequency_of_tokens(output_path: str, attack_type: str = "fake"):
    token_frequency = {}
    for chain in chains:
        for reversed in reverse_arr:
            path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_results_filtered_{reversed}.csv"
            if not path.exists():
                print(f"File not found: {path}")
                continue
            df = pl.read_csv(
                path,
                infer_schema_length=False
            ).with_columns(
                pl.all().cast(pl.String)
            )
            df = df.filter(
                (pl.col("possible_utility_victim") == "False") &
                (pl.col("possible_utility_attacker") == "False") 
                )
            df = df.with_columns(
                pl.when(
                    pl.col("contract_symbol").is_null() |
                    (pl.col("contract_symbol") == "")
                )
                .then(pl.col("contract_name"))
                .otherwise(pl.col("contract_symbol"))
                .alias("token_id")
            )
            result = (
                df
                .group_by("token_id")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
            )
            for token_id, count in result.iter_rows():
                token_frequency[token_id] = token_frequency.get(token_id, 0) + count
    with open(output_path, "w", encoding="utf-8") as f:
        for token_id, count in token_frequency.items():
            f.write(f"{token_id}\t{count}\n")
