"""
For each row in labeling.csv, fetches the transaction receipt from the
appropriate chain RPC (determined by the 'chain' column) and appends:
    gas_used, effective_gas_price_wei, cost_wei, cost_eth

Input:  src/labeling.csv
Output: src/labeling_with_gas.csv

labeling.csv expected columns:
    transaction_key, cumulative_value_usd, verdict, rationale, chain
"""

import ast
import os
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from web3 import Web3

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_CSV = BASE_DIR / "labeling.csv"
OUTPUT_CSV = BASE_DIR / "labeling_with_gas.csv"

load_dotenv()
_alchemy = os.getenv("alchemyToken", "")

RPC_URLS: dict[str, list[str]] = {
    "arbitrum": [
        f"https://arb-mainnet.g.alchemy.com/v2/{_alchemy}",
        "https://arb1.arbitrum.io/rpc",
    ],
    "optimism": [
        f"https://opt-mainnet.g.alchemy.com/v2/{_alchemy}",
        "https://mainnet.optimism.io",
    ],
    "avalanche": [
        f"https://avax-mainnet.g.alchemy.com/v2/{_alchemy}",
        "https://api.avax.network/ext/bc/C/rpc",
    ],
    "gnosis": [
        "https://rpc.gnosischain.com",
        "https://gnosis-mainnet.public.blastapi.io",
    ],
    "opbnb": [
        "https://opbnb-mainnet-rpc.bnbchain.org",
        "https://opbnb.publicnode.com",
    ],
    "polygonzk": [
        f"https://polygonzkevm-mainnet.g.alchemy.com/v2/{_alchemy}",
        "https://zkevm-rpc.com",
    ],
}

_w3_cache: dict[str, Web3] = {}

def get_w3(chain: str) -> Web3:
    if chain in _w3_cache:
        return _w3_cache[chain]
    urls = RPC_URLS.get(chain)
    if not urls:
        raise ValueError(f"Unknown chain '{chain}'. Add it to RPC_URLS.")
    for url in urls:
        w3 = Web3(Web3.HTTPProvider(url))
        if w3.is_connected():
            _w3_cache[chain] = w3
            return w3
    raise ConnectionError(f"Could not connect to any RPC endpoint for '{chain}'")


def get_tx_cost(w3: Web3, tx_hash: str) -> dict:
    receipt = w3.eth.get_transaction_receipt(tx_hash)
    gas_used: int = receipt["gasUsed"]
    effective_gas_price: int = receipt["effectiveGasPrice"]
    cost_wei = gas_used * effective_gas_price
    cost_eth = Web3.from_wei(cost_wei, "ether")

    return {
        "gas_used": gas_used,
        "effective_gas_price_wei": effective_gas_price,
        "cost_wei": cost_wei,
        "cost_eth": str(cost_eth),
    }

def extract_tx_hash(transaction_key: str) -> str:
    """Parse ``"('0xabc…', 6)"`` → ``'0xabc…'``."""
    tup = ast.literal_eval(transaction_key)
    return str(tup[0])

def main() -> None:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_CSV}")

    df = pl.read_csv(INPUT_CSV, infer_schema_length=0)
    rows = df.to_dicts()
    total = len(rows)
    results: list[dict] = []

    for i, row in enumerate(rows, start=1):
        tx_key = row.get("transaction_key") or ""
        chain = (row.get("chain") or "").strip().lower()
        if not tx_key.strip():
            print(f"[{chain}] {i}/{total}  SKIP — empty transaction_key")
            results.append({**row, "tx_hash": None, "gas_used": None,
                            "effective_gas_price_wei": None, "cost_wei": None, "cost_eth": None})
            continue
        tx_hash = extract_tx_hash(tx_key)
        print(f"[{chain}] {i}/{total}  {tx_hash[:20]}…")
        try:
            w3 = get_w3(chain)
            cost = get_tx_cost(w3, tx_hash)
        except Exception as exc:
            print(f"  ERROR: {exc}")
            cost = {
                "gas_used": None,
                "effective_gas_price_wei": None,
                "cost_wei": None,
                "cost_eth": None,
            }
        results.append({**row, "tx_hash": tx_hash, **cost})

    out_df = pl.DataFrame(results)
    out_df.write_csv(OUTPUT_CSV)
    print(f"\nDone. {total} rows saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
