import csv
import os
import polars as pl
from poisoning_detector.categorize import dust_transfer, zero_transfer, fake_transfer, get_dump_data
from datetime import datetime
import time
from poisoning_detector.steps_runner import StepsRunner
from poisoning_detector.system_contracts import EVM_SYSTEM_CONTRACTS
import sys
from dotenv import load_dotenv
from poisoning_detector.analyse_results import analyse_results, to_remove, filter_payouts, get_verified_contracts, obtain_fake_token_list, remove_legit_transfers
from datetime import datetime, timedelta
import gc

load_dotenv()
alchemy_token = os.getenv("alchemyToken")
etherscan_token = os.getenv("etherscanToken")
ram_load = "high" #default
if len(sys.argv) not in (2, 3):
    print("wrong arguments; usage: main.py <blockchain> [ram_load=high|lower]")
    sys.exit(1)

if len(sys.argv) == 3:
    if sys.argv[2].startswith("ram_load="):
        ram_load = sys.argv[2].split("=", 1)[1]
        if ram_load not in ("high", "lower"):
            print("ram_load must be 'high' or 'lower'")
            sys.exit(1)
    else:
        print("unknown argument:", sys.argv[2])
        sys.exit(1)

###if len(sys.argv) != 2:
###    print("wrong arguments")
###    exit(1)

if sys.argv[1]=="arbitrum": 
    rpc_url2=f"https://arb-mainnet.g.alchemy.com/v2/{alchemy_token}"
    rpc_url="https://arb1.arbitrum.io/rpc"
elif sys.argv[1]=="optimism": #
    rpc_url2=f"https://opt-mainnet.g.alchemy.com/v2/{alchemy_token}"
    rpc_url="https://mainnet.optimism.io"
elif sys.argv[1]=="avalanche":  #
    rpc_url="https://api.avax.network/ext/bc/C/rpc"
    rpc_url2 = f"https://avax-mainnet.g.alchemy.com/v2/{alchemy_token}"
elif sys.argv[1]=="gnosis": #
    rpc_url="https://rpc.gnosischain.com"
    rpc_url2 = f"https://gnosis-mainnet.g.alchemy.com/v2/{alchemy_token}"
elif sys.argv[1]=="polygonzk": #
    rpc_url="https://zkevm-rpc.com"
    rpc_url2 = f"https://polygonzkevm-mainnet.g.alchemy.com/v2/{alchemy_token}"  
elif sys.argv[1]=="opbnb": #
    rpc_url="https://opbnb-mainnet-rpc.bnbchain.org"
    rpc_url2 = f"https://opbnb-mainnet.g.alchemy.com/v2/{alchemy_token}"  
elif sys.argv[1]=="ethereum":
    rpc_url = "https://ethereum-rpc.publicnode.com"
    rpc_url2 = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_token}" 
else:
    print("wrong blockchain argument")
    exit(1)


SYSTEM_CONTRACTS = EVM_SYSTEM_CONTRACTS.get(sys.argv[1], set())
os.makedirs("results", exist_ok=True)

pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(3)
if __name__ == "__main__":
    ERC20_addresses_lower = set()
    with open(f"tokens/{sys.argv[1]}_token_symbols_prices.txt", "r") as f:
        ERC20_price_map = {}
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 3:
                continue
            addr, symbol, price = parts
            if price and price != "None":
                ERC20_price_map[addr.lower()] = price
            ERC20_addresses_lower.add(addr.lower())

    ERC20_name = set() #name and symbol are not lower (some legit tokens have case sensitive symbols like USDS and USDs)
    ERC20_symbol = set()
    ERC20_decimals_map = {}
    ERC20_symbol_map = {}
    ERC20_name_map = {}
    with open(f"tokens/{sys.argv[1]}_token_info.txt", "r", encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 4:
                continue
            addr, name, symbol, decimals = parts
            decimals_str = "1" * int(decimals)
            if name and name != "None":
                ERC20_name_map[addr.lower()] = name
                ERC20_name.add(name)
            if symbol and symbol != "None":
                ERC20_symbol_map[addr.lower()] = symbol
                ERC20_symbol.add(symbol)
            if decimals and decimals != "None":
                ERC20_decimals_map[addr.lower()] = decimals

    path_data = get_dump_data(sys.argv[1])
    print(path_data)
    
    fake_tokens = set()
    cached_tokens = set()
    fake_name_map = {}
    fake_symbol_map = {}
    if os.path.exists(f"{sys.argv[1]}_fake_tokens.csv"):
        with open(f"{sys.argv[1]}_fake_tokens.csv", "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            #if first row is header, skip it
            first_row = next(reader, None)
            if first_row and first_row[0].lower() == "address":
                first_row = None  #if header found, skip it
            if first_row:
                addr = first_row[0].strip().lower()
                fake_tokens.add(addr)
                if len(first_row) > 1:
                    fake_name_map[addr] = first_row[1].strip()
                if len(first_row) > 2:
                    fake_symbol_map[addr] = first_row[2].strip()
            for row in reader:
                if row:
                    addr = row[0].strip().lower()
                    fake_tokens.add(addr)
                    if len(row) > 1:
                        fake_name_map[addr] = row[1].strip()
                    if len(row) > 2:
                        fake_symbol_map[addr] = row[2].strip()
    if os.path.exists(f"{sys.argv[1]}_cached_tokens.csv"):
        with open(f"{sys.argv[1]}_cached_tokens.csv", "r", encoding="utf-8") as f:
            cached_tokens = {row[0].strip().lower() for row in csv.reader(f) if row}
    
    logs = (
        pl.scan_parquet(path_data['parquet_data'], missing_columns="insert").with_columns([
            pl.col("contract").str.to_lowercase().alias("contract"),
            pl.col("sender").str.to_lowercase().alias("sender"),
            pl.col("receiver").str.to_lowercase().alias("receiver")
        ])
        .filter(
            ~(pl.col("sender").is_in(list(SYSTEM_CONTRACTS))) &
            ~(pl.col("receiver").is_in(list(SYSTEM_CONTRACTS)))
        )
    )

    start_date = datetime(2022, 1, 1, 0, 0, 0)  
    end_date = datetime(2025, 7, 1, 0, 0, 0)
    
    logs = logs.filter(
        (pl.col("time") >= pl.lit(start_date)) &
        (pl.col("time") <= pl.lit(end_date))
    )
    window = timedelta(hours=1)
    t = start_date
    k = 0
    print(f"RAM LOAD: {ram_load}")
    
    while t < end_date:
        t2 = t + window
        print("Processing window:", t, "→", t2)
        start = time.perf_counter()
        logs_batch = (
            logs
            .filter((pl.col("time") >= t) & (pl.col("time") < t2))
            .collect()
        )
        print("logs_batch.height:", logs_batch.height)
        fake_df = fake_transfer(logs_batch, ERC20_addresses_lower, ERC20_name, ERC20_symbol, fake_tokens, cached_tokens, rpc_url,rpc_url2, sys.argv[1], fake_name_map, fake_symbol_map)
        logs_batch = logs_batch.join(fake_df, on=["time","transactionHash", "contract", "amount", "id"], how="anti")
        zero_df = zero_transfer(logs_batch,ERC20_decimals_map,ERC20_name_map,ERC20_symbol_map,ERC20_price_map)
        print("fake_df.height: ",fake_df.height)
        print("zero_df.height: ",zero_df.height)
        del logs_batch
        gc.collect()
        
        batch = StepsRunner(zero_df=zero_df,fake_df=fake_df,path_data=path_data,
                            ERC20_decimals_map=ERC20_decimals_map,SYSTEM_CONTRACTS_LOWER=SYSTEM_CONTRACTS,
                            rollup_name=sys.argv[1],rpc=rpc_url,rpc2=rpc_url2,ERC20_price_map=ERC20_price_map,ram_load=ram_load)
        batch.run_detection()
        del batch
        end = time.perf_counter()
        print("Elapsed time: ", end - start, "seconds")
        t = t2
    
    analyse_results(sys.argv[1],rpc_url,rpc_url2,ERC20_decimals_map,ERC20_price_map,ERC20_symbol_map,ERC20_name_map)
    #to_remove(sys.argv[1],rpc_url,rpc_url2)
    #filter_payouts(sys.argv[1])
    #obtain_fake_token_list(sys.argv[1])
    #get_verified_contracts(sys.argv[1])
    #remove_legit_transfers(sys.argv[1])
    
