import polars as pl
import os
import sys
from poisoning_detector.get_tokens import GetTokens
from web3 import Web3
import csv

pl.Config.set_tbl_cols(20)

def get_dump_data(rollup):
    code_dir = os.path.realpath(os.path.join(os.getcwd(), "../../"))
    sys.path.append(code_dir)
    data_dir = os.path.abspath(os.path.join(os.getcwd(), f"parquet_data_{rollup}"))
    path_data = dict()
    path_data['parquet_data'] = os.path.abspath(os.path.join(
            data_dir, "*.parquet"))
    return path_data


def dust_transfer(logs,ERC20_decimals_map,ERC20_price_map,ERC20_name_map,ERC20_symbol_map):
    dust_logs = logs.filter(
    pl.col("contract").str.to_lowercase().is_in(ERC20_decimals_map)
        ).with_columns([
            pl.col("contract")
              .str.to_lowercase()
              .replace(ERC20_decimals_map, return_dtype=pl.Utf8)
              .alias("decimals"),
            pl.col("contract")
              .str.to_lowercase()
              .replace(ERC20_price_map, return_dtype=pl.Float64)
              .alias("price"),
            pl.col("contract")
              .str.to_lowercase()
              .replace(ERC20_name_map, return_dtype=pl.Utf8)
              .alias("contract_name"),
            pl.col("contract")
              .str.to_lowercase()
              .replace(ERC20_symbol_map, return_dtype=pl.Utf8)
              .alias("contract_symbol") 
        ])
    result = dust_logs.filter(
    (pl.col("amount").cast(pl.Float64) / (10 ** pl.col("decimals").cast(pl.Float64))) * pl.col("price") < 0.01
    )
    return result
    
def zero_transfer(logs, ERC20_decimals_map,ERC20_name_map,ERC20_symbol_map,ERC20_price_map):
    logs = logs.filter(
    pl.col("contract").str.to_lowercase().is_in(ERC20_decimals_map)
        )
    return logs.filter(logs["amount"].map_elements(lambda x: int(x) == 0 if x is not None else False, return_dtype=pl.Boolean)).with_columns(
        [
            pl.col("contract")
              .str.to_lowercase()
              .replace(ERC20_decimals_map, return_dtype=pl.Utf8)
              .alias("decimals"),
            pl.col("contract")
              .str.to_lowercase()
              .replace(ERC20_price_map, return_dtype=pl.Float64)
              .alias("price"),
            pl.col("contract")
              .str.to_lowercase()
              .replace(ERC20_name_map, return_dtype=pl.Utf8)
              .alias("contract_name"),
            pl.col("contract")
              .str.to_lowercase()
              .replace(ERC20_symbol_map, return_dtype=pl.Utf8)
              .alias("contract_symbol") 
        ]
    )


def fake_transfer(logs, ERC20_addresses_lower,ERC20_name,ERC20_symbol,fake_tokens,cached_tokens,API,API2,rollup,fake_name_map,fake_symbol_map):
    contractInfoExtractor = GetTokens(API,API2)
    logs = logs.filter(~pl.col("contract").str.to_lowercase().is_in(ERC20_addresses_lower))
    unique_addresses = logs["contract"].to_list()
    
    unique_addresses = [Web3.to_checksum_address(addr) for addr in unique_addresses]
    for addr in set(unique_addresses):
        if addr.lower() in cached_tokens:
            continue
        else:
            name, symbol, _ = contractInfoExtractor.safe_get_token_info(addr=addr)
            if ( name in ERC20_name or symbol in ERC20_symbol or symbol in ERC20_name or name in ERC20_symbol ):
                fake_name_map[addr.lower()] = name
                fake_symbol_map[addr.lower()] = symbol
                fake_tokens.add(addr.lower())
                csv_file = f"{rollup}_fake_tokens.csv"
                file_exists = os.path.exists(csv_file)
                with open(csv_file, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["address", "name", "symbol"])
                    writer.writerow([addr.lower(), name, symbol])
            with open(f"{rollup}_cached_tokens.csv", "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([addr.lower()])
            cached_tokens.add(addr.lower())
    return logs.filter(pl.col("contract").str.to_lowercase().is_in(list(fake_tokens))).with_columns(
        [
            pl.col("contract")
              .str.to_lowercase()
              .replace(fake_name_map, return_dtype=pl.Utf8)
              .alias("contract_name"),
            pl.col("contract")
              .str.to_lowercase()
              .replace(fake_symbol_map, return_dtype=pl.Utf8)
              .alias("contract_symbol")  
        ]
    )