import polars as pl
pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(20)
import csv
import math
import polars as pl
from web3 import Web3
import os
import requests
import time
from poisoning_detector.categorize import get_dump_data
from dotenv import load_dotenv

def filter_payouts(name):
    reverse_arr = [False,True]
    for reversed in reverse_arr:
        path1 = f"results_{name}/{name}_zero_results_filtered_{reversed}.csv"
        path2 = f"results_{name}/{name}_fake_results_filtered_{reversed}.csv"
        if os.path.exists(path1):
            df = pl.read_csv(path1, infer_schema_length=False)
            df = df.with_columns(pl.all().cast(pl.Utf8))
        else:
            print(f"no {path1} file detected")
        if os.path.exists(path2):
            df2 = pl.read_csv(path2, infer_schema_length=False)
            df2 = df2.with_columns(pl.all().cast(pl.Utf8))
        else:
            print(f"no {path2} file detected")
        if not os.path.exists(path1) and not os.path.exists(path2):
            print("no filtered results files detected, skipping payouts calculation")
            return None, None
        elif os.path.exists(path1) and not os.path.exists(path2):
            df = df
        elif not os.path.exists(path1) and os.path.exists(path2):
            df = df2
        else:
            df = pl.concat([df, df2], how="vertical")    
        df = df.filter(
            (pl.col("possible_utility_victim") == "False") &
            (pl.col("possible_utility_attacker") == "False") 
        )
        df_keys = df.select("transaction_key").unique()
        path3 = f"results_{name}/{name}_payouts_{reversed}.csv"
        df3 = pl.read_csv(path3, infer_schema_length=False)
        df3 = df3.join(
            df_keys,
            on="transaction_key",
            how="semi"
        )
        df3.write_csv(f"results_{name}/{name}_payouts_{reversed}_new.csv")
        df3 = df3.with_columns(pl.col("usd_value").cast(pl.Float64))
        cumulative_payouts = (
            df3
            .group_by(["transaction_key"])
            .agg(
                pl.sum("usd_value").fill_null(0).alias("cumulative_value_usd")
            )
        )
        if os.path.exists(f"results_{name}/{name}_cumulative_payouts_{reversed}.csv"):
            os.remove(f"results_{name}/{name}_cumulative_payouts_{reversed}.csv")
        cumulative_payouts.write_csv(f"results_{name}/{name}_cumulative_payouts_{reversed}.csv")

        

def get_payouts(name, path_data, ERC20_decimals_map,ERC20_price_map,ERC20_symbol_map,ERC20_name_map,reversed=False):
        path1 = f"results/{name}_zero_results_filtered_{reversed}.csv"
        path2 = f"results/{name}_fake_results_filtered_{reversed}.csv"
        if os.path.exists(path1):
            df = pl.read_csv(path1, infer_schema_length=0)
            df = df.with_columns(pl.all().cast(pl.Utf8))
        else:
            print(f"no {path1} file detected")
        if os.path.exists(path2):
            df2 = pl.read_csv(path2, infer_schema_length=0)
            df2 = df2.with_columns(pl.all().cast(pl.Utf8))
        else:
            print(f"no {path2} file detected")

        if not os.path.exists(path1) and not os.path.exists(path2):
            print("no filtered results files detected, skipping payouts calculation")
            return None, None
        elif os.path.exists(path1) and not os.path.exists(path2):
            df = df
        elif not os.path.exists(path1) and os.path.exists(path2):
            df = df2
        else:
            df = pl.concat([df, df2], how="vertical")
        df = df.sort("time").unique(subset=["victim", "attacker"], keep="first") #keeps only oldest attack per victim-attacker pair
        logs_path = path_data['parquet_data']

        attackers = df["attacker"].unique().to_list()
        victims = df["victim"].unique().to_list()
        all_transfers = pl.scan_parquet(logs_path).filter(
            (pl.col("receiver").str.to_lowercase().is_in(attackers)) &
            (pl.col("sender").str.to_lowercase().is_in(victims)) &
            (pl.col("contract").str.to_lowercase().is_in(ERC20_decimals_map)) &
            (pl.col("amount").cast(float) > 0)
        ).with_columns([
            pl.col("contract").str.to_lowercase()
                .replace(ERC20_decimals_map, return_dtype=pl.Utf8)
                .alias("decimals"),
            pl.col("contract").str.to_lowercase()
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

        all_transfers = all_transfers.with_columns([
            pl.col("sender").str.to_lowercase().alias("sender_l"),
            pl.col("receiver").str.to_lowercase().alias("receiver_l"),
            pl.col("time").alias("time_after"),
        (
            (pl.col("amount").cast(pl.Float64) /
             (10 ** pl.col("decimals").cast(pl.Float64))) * pl.col("price")
        ).alias("usd_value"),
        ]).select(
            ["transactionHash", "amount", "contract_symbol", "time_after", "sender_l", "receiver_l", "usd_value"]
        ).collect(engine="streaming")

        df = df.with_columns([
            pl.col("victim").alias("victim_l"),
            pl.col("attacker").alias("attacker_l"),
        ])

        joined = df.join(
            all_transfers,
            left_on=["victim_l", "attacker_l"],
            right_on=["sender_l", "receiver_l"], 
            how="left",
        )

        joined = joined.filter(pl.col("time_after").cast(pl.Datetime) > pl.col("time").str.to_datetime(strict=False))

        cumulative_payouts = (
            joined
            .group_by(["transaction_key"])
            .agg(
                pl.sum("usd_value").fill_null(0).alias("cumulative_value_usd")
            )
        )
        joined.write_csv(f"results/{name}_payouts_{reversed}.csv")
        cumulative_payouts.write_csv(f"results/{name}_cumulative_payouts_{reversed}.csv")
        return cumulative_payouts, joined

def filter_steps_by_step1_keys(type: str, name: str, steps: tuple[int, ...] = (2, 3), reversed=False) -> None:
    step1_path = f"results/{name}_{type}_step1_{reversed}.csv"
    try: 
        with open(step1_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            step1_keys = {row["transaction_key"].strip() for row in reader if row.get("transaction_key")}
    except FileNotFoundError:
        print(f"missing {step1_path}")
        return

    for step in steps:
        path = f"results/{name}_{type}_step{step}_{reversed}.csv"
        try:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or []
                rows = [row for row in reader if row.get("transaction_key") and row["transaction_key"].strip() in step1_keys]
            with open(path, "w", newline="", encoding="utf-8") as f_out:
                writer = csv.DictWriter(f_out, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        except FileNotFoundError:
            print(f"missing {path} in filter_steps_by_step1_keys")
            continue 
    

def dedupe_csv(path_in: str, path_out: str | None = None):
    try:
        path_out = path_out or path_in
        df = pl.read_csv(path_in, infer_schema_length=0)
        df = df.with_columns(pl.all().cast(pl.Utf8))
        before = df.height
        df_unique = df.unique()
        after = df_unique.height
        deleted = before - after
        if deleted > 0:
            print(f"{path_in}: removed {deleted} duplicate rows (from {before} to {after})")
        df_unique.write_csv(path_out)
    except FileNotFoundError:
        print(f"missing {path_in}")

def calc_score(x):
    if x == 3:
        return 1
    b = 1.5
    c = 2
    return math.floor(math.log(x + c, b)) - 3  #based on how hard it is to find longer similar addresses

def extract_txhash_from_key(key):
    if isinstance(key, tuple) and len(key) > 0:
        return str(key[0]).strip()
    if isinstance(key, str):
        s = key.strip()
        if s.startswith("(") and "," in s:
            first = s[1:].split(",", 1)[0]
            return first.strip().strip("'").strip('"')
        return s
    return str(key)

def get_interactions(path_data, name, type, reversed):
    df = pl.read_csv(f"results/{name}_{type}_results_{reversed}.csv", infer_schema_length=0)
    df = df.with_columns(pl.all().cast(pl.Utf8))
    addresses = pl.concat([
        df["attacker"],
        df["victim"]
    ]).unique().to_list()
    logs = pl.scan_parquet(path_data['parquet_data']).filter(
        (pl.col("sender").str.to_lowercase().is_in([a.lower() for a in addresses])) |
        (pl.col("receiver").str.to_lowercase().is_in([a.lower() for a in addresses]))
    )

    senders = logs.select([
        pl.col("sender").alias("address"),
        pl.col("receiver").alias("counterparty")
    ])
    
    receivers = logs.select([
        pl.col("receiver").alias("address"),
        pl.col("sender").alias("counterparty")
    ])
    combined = pl.concat([senders, receivers], how="vertical")
    interactions = (
        combined
        .group_by("address")
        .agg(pl.col("counterparty").n_unique().alias("unique_accounts"))
        .collect(engine="streaming")
    )

    interaction_map = dict(interactions.select("address", "unique_accounts").iter_rows())
    return interaction_map 


def safe_get_code(w3, address, rpc_url2, retries=float("inf"), delay=30):
    attempt = 1
    while attempt < retries:
        if attempt%2==0:
            delay+=1
            w3 = Web3(Web3.HTTPProvider(rpc_url2))
        try:
            address_code = w3.eth.get_code(Web3.to_checksum_address(address))
            return address_code
        except (requests.exceptions.ConnectionError, ConnectionResetError) as e:
            attempt += 1
            print(f"[{attempt}] Connection error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            #catch anything unexpected
            attempt += 1
            print(f"[{attempt}] Unexpected error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Max retries reached, still failing.")

def write_rows(filepath, rows):
    if rows:
        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

def first_byte(code):
    return code[:1] if isinstance(code, (bytes, bytearray)) else b''


def to_remove(name, rpc1, rpc2):
    attack_types = ["zero", "fake"]
    reverse_arr = [False,True]
    w3 = Web3(Web3.HTTPProvider(rpc1))
    cached_accounts = {}
    for type in attack_types:
        removed_attackers_rows = []
        for reversed in reverse_arr:
            i=1
            filtered_rows = []
            try:
                with open(f"results_{name}/{name}_{type}_results_filtered_{reversed}.csv", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    print(f"results_{name}/{name}_{type}_results_filtered_{reversed}.csv")
                    for row in reader:
                        if i%10000==0:
                            print("name:",name,"type:", type," i= ", i)
                        i+=1
                        if row["unique_accounts_attacker"] is not None:
                            unique_accounts_attacker = int(row["unique_accounts_attacker"].strip())
                        else:
                            unique_accounts_attacker = 0
                        if row["unique_accounts_victim"] is not None:
                            unique_accounts_victim = int(row["unique_accounts_victim"].strip())
                        else:
                            unique_accounts_victim = 0
                        attacker = row["attacker"].strip()
                        victim = row["victim"].strip()
                        contract_victim_bool = None
                        contract_attacker_bool = None
                        possible_utility_attacker = False
                        if reversed == True and unique_accounts_attacker > 100:
                            removed_attackers_rows.append([row["transaction_key"], attacker, row["score"]])
                            continue

                        if unique_accounts_attacker > 200:
                            if attacker not in cached_accounts:
                                attacker_code = safe_get_code(w3=w3, address=attacker, delay=1, rpc_url2=rpc2)
                                if attacker_code != b'':
                                    cached_accounts[attacker] = {"is_contract": True}
                                else:
                                    cached_accounts[attacker] = {"is_contract": False}
                            contract_attacker_bool = cached_accounts[attacker]["is_contract"]
                            if contract_attacker_bool:
                                possible_utility_attacker = True

                        possible_utility_victim = False
                        if unique_accounts_victim > 200:
                            if victim not in cached_accounts:
                                victim_code = safe_get_code(w3=w3, address=victim, delay=1, rpc_url2=rpc2)
                                if victim_code != b'':
                                    cached_accounts[victim] = {"is_contract": True}
                                else:
                                    cached_accounts[victim] = {"is_contract": False}
                            contract_victim_bool = cached_accounts[victim]["is_contract"]
                            if contract_victim_bool:
                                possible_utility_victim = True

                        row["possible_utility_attacker"] = possible_utility_attacker
                        row["possible_utility_victim"] = possible_utility_victim
                        row["contract_victim_bool"] = contract_victim_bool
                        row["contract_attacker_bool"] = contract_attacker_bool
                        filtered_rows.append([
                            row["transaction_key"].strip(), 
                            row["score"].strip(), 
                            row["victim"].strip(), 
                            row["attacker"].strip(),
                            row["top_address"].strip(),
                            row["contract"].strip(),
                            row["amount"].strip(),
                            row["time"].strip(),
                            row["blockNumber"].strip(),
                            row["contract_name"].strip(),
                            row["contract_symbol"].strip(),
                            row["delta"].strip(),
                            row["similarity_match"].strip(),
                            row["poor_activity_attacker"].strip(),
                            row["poor_activity_victim"].strip(),
                            possible_utility_victim,
                            possible_utility_attacker,
                            contract_victim_bool,
                            contract_attacker_bool,
                            row["batched_bool"].strip(),
                            row["unique_accounts_victim"].strip(),
                            row["unique_accounts_attacker"].strip()])
                os.rename(f"results_{name}/{name}_{type}_results_filtered_{reversed}.csv", f"results_{name}/{name}_{type}_results_filtered_{reversed}_old.csv")
                if not os.path.exists(f"results_{name}/{name}_{type}_results_filtered_{reversed}.csv"):
                    with open(f"results_{name}/{name}_{type}_results_filtered_{reversed}.csv", "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(["transaction_key", "score", "victim", "attacker", "top_address", "contract", 
                            "amount", "time", "blockNumber", "contract_name", "contract_symbol", "delta",  "similarity_match",
                            "poor_activity_attacker", "poor_activity_victim", "possible_utility_victim", "possible_utility_attacker", 
                            "contract_victim_bool", "contract_attacker_bool", "batched_bool", "unique_accounts_victim", "unique_accounts_attacker"])
                write_rows(f"results_{name}/{name}_{type}_results_filtered_{reversed}.csv", filtered_rows)
                if reversed == True:
                    write_rows(f"results_{name}/{name}_{type}_removedAttackers_True_new.csv", removed_attackers_rows)
            except FileNotFoundError:
                print(f"results_{name}/{name}_{type}_results_filtered_{reversed}.csv")
            
def obtain_fake_token_list(name: str):
    reverse_arr = [False,True]
    all_tokens = []
    if not os.path.exists(f"results_{name}/{name}_fake_contracts_poisoning.csv"):
        with open(f"results_{name}/{name}_fake_contracts_poisoning.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["address", "name", "symbol", "direction"])
    for reversed in reverse_arr:
        path1 = f"results_{name}/{name}_fake_results_filtered_{reversed}.csv"
        if os.path.exists(path1):
            df = pl.read_csv(path1, infer_schema_length=False)
            df = df.with_columns(pl.all().cast(pl.Utf8))
        else:
            print(f"no {path1} file detected")
            continue

        tokens_df = (
            df
            .select(
                pl.col("contract").alias("address"),
                pl.col("contract_name").alias("name"),
                pl.col("contract_symbol").alias("symbol"),
            )
            .unique()
            .with_columns(
                pl.lit(str(reversed)).alias("direction")
            )
        )
        all_tokens.append(tokens_df)
    if all_tokens:
        final_df = pl.concat(all_tokens)
        final_df.write_csv(
            f"results_{name}/{name}_fake_contracts_poisoning.csv"
        )

        


def get_verified_contracts(name:str):
    load_dotenv(dotenv_path="../.env")
    API_KEY = os.getenv("etherscanToken")
    if name=="arbitrum": 
        chain_id = 42161
    elif name=="optimism": 
        chain_id = 10
    elif name=="avalanche":  
        chain_id = 43114
    elif name=="gnosis": 
        chain_id = 100
    elif name=="polygonzk": 
        chain_id = 166700
    elif name=="opbnb": 
        chain_id = 204
 
    results = []
    with open(f"results_{name}/{name}_fake_contracts_poisoning.csv", newline="", encoding="utf-8") as f: #if sender or receiver is a contract, get its name and add given row for furthere verification
        reader = csv.DictReader(f)   #constant memory
        for row in reader:
            c_name = row["name"].strip()
            symbol = row["symbol"].strip()
            direction = row["direction"].strip()
            c = row["address"].strip()
            url = f"https://api.etherscan.io/v2/api?chainid={chain_id}&module=contract&action=getsourcecode&address={c}&apikey={API_KEY}"
            response = requests.get(url)

            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                r = response.json()
            except Exception as e:
                print(f"{c}: Request failed ({e})")
                results.append((c, False))
                continue
            
            if r.get("status") != "1" or not r.get("result"):
                print(f"{c}: No result found")
                results.append((c, False))
                continue

            entry = r["result"][0]

            source_code = entry.get("SourceCode", "")
            abi = entry.get("ABI", "")

            verified = (
                source_code not in ("", None)
                and abi != "Contract source code not verified"
            )

            print(f"{c}: {'Verified' if verified else 'Not Verified'}")
            results.append((c,c_name,symbol,direction,verified))
            
    with open(f"results_{name}/{name}_contracts_verification.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["address","name","symbol","direction","verified"])
        writer.writerows(results)

    print(f"Results saved to results_{name}/{name}_contracts_verification.csv")

def remove_legit_transfers(name:str):
    with open(f"results_{name}/{name}_contracts_verification.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)   #constant memory
        legit_contracts = set()
        for row in reader:
            if row["fp"].strip().lower() == "legit":
                legit_contracts.add(row["address"].strip().lower())
    for reversed in [False, True]:
        path1 = f"results_{name}/{name}_fake_results_filtered_{reversed}.csv"
        if os.path.exists(path1):
            df = pl.read_csv(path1, infer_schema_length=False)
            df = df.with_columns(pl.all().cast(pl.Utf8))
        else:
            print(f"no {path1} file detected")
            continue
        df2 = df.filter(
            pl.col("contract").str.to_lowercase().is_in(legit_contracts)
        ).select(pl.col("transaction_key")).unique()
        df = df.filter(
            ~pl.col("contract").str.to_lowercase().is_in(legit_contracts)
        )
        os.remove(path1)
        df.write_csv(path1)
        path2 = f"results_{name}/{name}_payouts_{reversed}.csv"
        path3 = f"results_{name}/{name}_cumulative_payouts_{reversed}.csv"
        if os.path.exists(path2):
            payouts = pl.read_csv(path2, infer_schema_length=False)
            payouts = payouts.with_columns(pl.all().cast(pl.Utf8))
        else:
            print(f"no {path1} file detected")
            continue
        cummulative_payouts  = pl.read_csv(path3, infer_schema_length=False)
        payouts = payouts.join(
            df2,
            on="transaction_key",
            how="anti"
        )
        cummulative_payouts = cummulative_payouts.join(
            df2,
            on="transaction_key",
            how="anti"
        )
        os.remove(path2)
        os.remove(path3)
        payouts.write_csv(path2)
        cummulative_payouts.write_csv(path3)

attack_types = ["zero", "fake"]
reverse_arr = [False,True]
def analyse_results(name, rpc1, rpc2,ERC20_decimals_map,ERC20_price_map,ERC20_symbol_map,ERC20_name_map):
    w3 = Web3(Web3.HTTPProvider(rpc1))

    ###clears result datasets in case of some error
    for i in range(1,3):
        dedupe_csv(f"results/{name}_zero_step{i}_True.csv",f"results/{name}_zero_step{i}_True.csv")
        dedupe_csv(f"results/{name}_fake_step{i}_True.csv",f"results/{name}_fake_step{i}_True.csv")
        dedupe_csv(f"results/{name}_zero_step{i}_False.csv",f"results/{name}_zero_step{i}_False.csv")
        dedupe_csv(f"results/{name}_fake_step{i}_False.csv",f"results/{name}_fake_step{i}_False.csv")
    
    for type in attack_types:
        for reversed in reverse_arr:
            if not os.path.exists(f"results/{name}_{type}_step1_{reversed}.csv"):
                continue
            filter_steps_by_step1_keys(type, name, (2,3), reversed)
    
    
    print("removing duplicates and filtering done")
    for type in attack_types:
        print("analysing type:", type)
        for reversed in reverse_arr:
            print("analysing reversed:", reversed)
            if not os.path.exists(f"results/{name}_{type}_step1_{reversed}.csv"):
                continue
            result = {}
            with open(f"results/{name}_{type}_step1_{reversed}.csv", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = row["transaction_key"].strip()
                    attacker = row["attacker"].strip()
                    victim = row["victim"].strip()
                    score = calc_score(int(float(row["score"])))
                    result[key] = {
                        "score": score,
                        "top_address": row["top_address"],
                        "attacker": attacker,
                        "victim": victim,
                        "contract": row["contract"],
                        "amount": row["amount"],
                        "time": row["time"],
                        "blockNumber": row["blockNumber"],
                        "contract_name": row["contract_name"],
                        "contract_symbol": row["contract_symbol"],
                        "prev_txHash": row["prev_txHash"],
                        "time_prev": row["time_prev"],
                        "delta": row["delta"],
                        "similarity_match": row["score"],
                        "batched_bool": False
                    }
                    if row["delta"] and row["delta"] != "None" and row["delta"] != '':  #in case the attacker is similar to victim
                        if float(row["delta"])/60 <= 20.0:
                            result[key]["score"] += 1
            try:
                with open(f"results/{name}_{type}_step2_{reversed}.csv", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        key = row["transaction_key"].strip() 
                        result[key]["score"] += 1
                        result[key]["batched_bool"] = True
            except FileNotFoundError:
                print(f"file results/{name}_{type}_step2_{reversed}.csv does not exist")

            try:
                with open(f"results/{name}_{type}_step3_{reversed}.csv", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        key = row["transaction_key"].strip() 
                        result[key]["score"] += 1   
            except FileNotFoundError:
                print(f"file results/{name}_{type}_step3_{reversed}.csv does not exist")

            with open(f"results/{name}_{type}_results_{reversed}.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["transaction_key", "score", "top_address", "attacker", "victim", "contract", "amount", "time", "blockNumber",
                                "contract_name", "contract_symbol", "prev_txHash", "time_prev", "delta", "similarity_match", "batched_bool"])
                for key in result:
                    writer.writerow([key, result[key]["score"], result[key]["top_address"], result[key]["attacker"],
                    result[key]["victim"], result[key]["contract"], result[key]["amount"], result[key]["time"],
                    result[key]["blockNumber"], result[key]["contract_name"], result[key]["contract_symbol"], result[key]["prev_txHash"],
                    result[key]["time_prev"], result[key]["delta"], result[key]["similarity_match"], result[key]["batched_bool"]])

            print("beggining advanced filtering")
            ###################
            #ADVANCED FILTERING
            cached_accounts = {}
            cached_attackers_removed = set()
            cached_victims_removed = set()
            path_data = get_dump_data(name)
            results_filtered_path = f"results/{name}_{type}_results_filtered_{reversed}.csv"
            if not os.path.exists(results_filtered_path):
                with open(results_filtered_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["transaction_key", "score", "victim", "attacker", "top_address", "contract", 
                        "amount", "time", "blockNumber", "contract_name", "contract_symbol", "delta",  "similarity_match",
                        "poor_activity_attacker", "poor_activity_victim", "possible_utility_victim", "possible_utility_attacker", 
                        "contract_victim_bool", "contract_attacker_bool", "batched_bool", "unique_accounts_victim", "unique_accounts_attacker"])
            i=0
            removed_attackers_rows = []
            removed_victims_rows = []
            filtered_rows = []
            interaction_map = get_interactions(path_data, name, type, reversed)
            with open(f"results/{name}_{type}_results_{reversed}.csv", newline="") as f: #if sender or receiver is a contract, get its name and add given row for furthere verification
                reader = csv.DictReader(f)   #constant memory
                for row in reader:
                    contract_victim_bool = False
                    contract_attacker_bool = False
                    if i%10000==0:
                        print("name:",name,"type:", type," i= ", i)
                    i+=1
                    key = row["transaction_key"].strip()
                    score = row["score"].strip()
                    victim = row["victim"].strip()
                    attacker = row["attacker"].strip()
                    if attacker in cached_attackers_removed:
                        removed_attackers_rows.append([key, attacker, score])
                        continue
                    if victim in cached_victims_removed:
                        removed_victims_rows.append([key, victim, score])
                        continue
                    
                    unique_accounts_attacker = interaction_map.get(attacker, 0)
                    unique_accounts_victim = interaction_map.get(victim, 0)
                    
                    poor_activity_attacker = False
                    if unique_accounts_attacker<2:
                        poor_activity_attacker = True
                    poor_activity_victim = False
                    if unique_accounts_victim < 2:
                        poor_activity_victim = True

                    if unique_accounts_attacker > 60000:
                        cached_attackers_removed.add(attacker)
                        removed_attackers_rows.append([key, attacker, score])
                        continue

                    if unique_accounts_victim > 60000:
                        cached_victims_removed.add(victim)
                        removed_victims_rows.append([key, victim, score])
                        continue

                    if reversed == True and unique_accounts_attacker > 100:
                        cached_attackers_removed.add(attacker)
                        removed_attackers_rows.append([key, attacker, score])
                        continue

                    contract_attacker_bool = None
                    contract_victim_bool = None 

                    possible_utility_attacker = False
                    if unique_accounts_attacker > 200:
                        if attacker not in cached_accounts:
                            attacker_code = safe_get_code(w3=w3, address=attacker, delay=1, rpc_url2=rpc2)
                            if attacker_code != b'':
                                cached_accounts[attacker] = {"is_contract": True}
                            else:
                                cached_accounts[attacker] = {"is_contract": False}
                        contract_attacker_bool = cached_accounts[attacker]["is_contract"]
                        if contract_attacker_bool:
                            possible_utility_attacker = True

                    possible_utility_victim = False
                    if unique_accounts_victim > 200:
                        if victim not in cached_accounts:
                            victim_code = safe_get_code(w3=w3, address=victim, delay=1, rpc_url2=rpc2)
                            if victim_code != b'':
                                cached_accounts[victim] = {"is_contract": True}
                            else:
                                cached_accounts[victim] = {"is_contract": False}
                        contract_victim_bool = cached_accounts[victim]["is_contract"]
                        if contract_victim_bool:
                            possible_utility_victim = True


                    filtered_rows.append([
                        key, 
                        score, 
                        victim, 
                        attacker,
                        row["top_address"].strip(),
                        row["contract"].strip(),
                        row["amount"].strip(),
                        row["time"].strip(),
                        row["blockNumber"].strip(),
                        row["contract_name"].strip(),
                        row["contract_symbol"].strip(),
                        row["delta"].strip(),
                        row["similarity_match"].strip(),
                        poor_activity_attacker,
                        poor_activity_victim,
                        possible_utility_victim,
                        possible_utility_attacker,
                        contract_victim_bool,
                        contract_attacker_bool,
                        row["batched_bool"].strip(),
                        unique_accounts_victim,
                        unique_accounts_attacker
                    ])
            write_rows(f"results/{name}_{type}_removedAttackers_{reversed}.csv", removed_attackers_rows)
            write_rows(f"results/{name}_{type}_removedVictims_{reversed}.csv", removed_victims_rows)
            write_rows(results_filtered_path, filtered_rows)
    print("advanced filtering done, moving to payouts calculation...")
    for reversed in reverse_arr:                                          
        cumulative_payouts, all_payouts = get_payouts(name, path_data, ERC20_decimals_map,ERC20_price_map,ERC20_symbol_map,ERC20_name_map,reversed)
    