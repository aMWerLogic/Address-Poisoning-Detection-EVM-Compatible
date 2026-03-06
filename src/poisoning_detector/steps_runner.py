import polars as pl
from poisoning_detector.find_previous_transfers import find_previous_interactions_victim_attacker,find_previous_victim_interactors,find_previous_victim_interactors_ram_lower
import csv
import os
import time
from web3 import Web3
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import HTTPError
import gc
from datetime import datetime
import psutil, os

def mem():
    p = psutil.Process(os.getpid())
    print(f"RSS: {p.memory_info().rss / 1024**2:.2f} MB")

class StepsRunner:
    def __init__(self, zero_df=None, fake_df=None, path_data=None, ERC20_decimals_map=None, SYSTEM_CONTRACTS_LOWER=None, previous_df=None, rollup_name=None, rpc=None, rpc2=None, ERC20_price_map=None, ram_load="high"):
        self.zero_df = zero_df
        self.fake_df = fake_df
        self.path_data = path_data
        self.ERC20_decimals_map = ERC20_decimals_map
        self.SYSTEM_CONTRACTS_LOWER = SYSTEM_CONTRACTS_LOWER
        self.previous_df = previous_df
        self.rollup_name = rollup_name
        self.rpc = rpc
        self.rpc2 = rpc2
        self.ERC20_price_map = ERC20_price_map
        self.ram_load = ram_load

    @staticmethod
    def calculate_similarity_score(attacker, victim):
        if not attacker or not victim:
            return -1000, victim
        if len(attacker) != len(victim):
            return -1000, victim
        attacker = attacker.lower().removeprefix("0x")
        victim = victim.lower().removeprefix("0x")
        if attacker == victim:
            return 0, "0x"+victim
        prefix_match = 0
        for i in range(len(attacker)):
            if attacker[i] == victim[i]:
                prefix_match += 1
            else:
                break
        if prefix_match <= 2:
            return 0, "0x"+victim
        suffix_match = 0
        for i in range(len(attacker)-1, -1, -1):
            if i < prefix_match:
                break
            if attacker[i] == victim[i]:
                suffix_match += 1
            else:
                break
        return (prefix_match + suffix_match), "0x"+victim
    

    def transfer_similarity(self, victim_address, receiver_addresses, attacker_address):
        score, top_address = self.calculate_similarity_score(attacker_address, victim_address)
        for addr in receiver_addresses:
            a, b = self.calculate_similarity_score(attacker_address, addr)
            if a > score:
                score = a
                top_address = b
        return score, top_address
    
    def get_last_ten_interactors(self, prev_df=None, attacker_col=None, attack_time=None):
        prev = pl.DataFrame({
            attacker_col: prev_df[attacker_col][0],
            "time": prev_df["time"][0],
            "transactionHash": prev_df["transactionHash"][0]
        })

        prev = prev.filter(pl.col("time") < attack_time).sort("time", descending=True).head(10)
        receivers_list = prev[attacker_col].to_list()
        hashes_list = prev["transactionHash"].to_list()
        times_list = prev["time"].to_list()
        return receivers_list, hashes_list, times_list


    def block_similarity_score(self, attack_df=None, transfers_df=None, reversed=False):
        if reversed == True:
            victim_col, attacker_col = "receiver", "sender"
        else:
            victim_col, attacker_col = "sender", "receiver"  
        result_map = {}
        for row in attack_df.iter_rows(named=True):
            victim = row[victim_col]
            attacker = row[attacker_col]
            attack_time = row["time"]
            tx_hash = row["transactionHash"]
            log_id = row["id"]

            if not transfers_df.is_empty():
                prev = (
                    transfers_df
                    .filter(pl.col(victim_col) == victim)
                )
            else:
                prev = pl.DataFrame()
            if prev.height>0:
                receivers_list, hashes_list, times_list = self.get_last_ten_interactors(prev_df=prev, attacker_col=attacker_col, attack_time=attack_time)   
            else:
                receivers_list = []
                hashes_list = []
                times_list = []
            score, top_address = self.transfer_similarity(victim, receivers_list, attacker)
            try:
                idx = receivers_list.index(top_address)
                txhash_prev = hashes_list[idx]
                time_prev = times_list[idx]
                delta = row["time"] - time_prev
                delta = delta.total_seconds()
            except ValueError:
                idx = None
                txhash_prev=None
                time_prev=None
                delta=None

            if score > 2:
                result_map[(tx_hash, log_id)] = {
                    "score": score,
                    "top_address": top_address,
                    "attacker": attacker,
                    "victim": victim,
                    "contract": row["contract"],
                    "amount": row["amount"],
                    "time": row["time"],
                    "blockNumber": row["blockNumber"],
                    "contract_name": row["contract_name"],
                    "contract_symbol": row["contract_symbol"],
                    "prev_txHash": txhash_prev,
                    "time_prev": time_prev,
                    "delta": delta
                }

        attack_df = attack_df.filter(
            (pl.col("transactionHash") + pl.col("id").cast(pl.Utf8)).is_in([key[0] + str(key[1]) for key in result_map.keys()])
        )
        
        return attack_df, result_map

    
    def to_datetime(t):
        if isinstance(t, str):
            return datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
        return t

    #if victim has sent something (legit tokens) to attacker before poisoning attack
    #if victim has sent smth to attacker before the oldes attack then all attacks in batch are righfully to be removed
    #however if he did not sent anything then we have approximation within the batch (for example batch=one day) that victim did not sent anything
    #also if oldest attack happened and victim did not send anything before it then victim did not know attacker before it and all other attacks within batch can be cosnidered as such also
    #only in next batch new attacks are dropped if victim has sent something # the approximation is done because it is possible for victim to send tokens to attacker as a payout, meaning the attack was successful and therefore it should not be removed and victim does not know attacker
    def check_victim_attacker_familiarity(self, attack_df=None, transfers_df=None, reversed=False, ram_load="high"):
        if reversed == True:
            victim_col, attacker_col = "receiver", "sender"
        else:
            victim_col, attacker_col = "sender", "receiver"

        if ram_load=="high":
            if transfers_df is None or transfers_df.is_empty():
                return attack_df

        attack_df_oldest = attack_df.sort(["time"], descending=False).group_by([victim_col, attacker_col]).head(1)
        victims = attack_df_oldest[victim_col].unique().to_list()
        attackers = attack_df_oldest[attacker_col].unique().to_list()

        previous_transfers = transfers_df.filter(
            (pl.col("receiver").is_in(attackers)) &
            (pl.col("sender").is_in(victims))
        )
        previous_transfers = previous_transfers.rename({
            "time": "time_prev",
        })

        previous_transfers = previous_transfers.with_columns([
            pl.col("sender").alias("sender_l"),
            pl.col("receiver").alias("receiver_l")
        ])

        attack_df_oldest = attack_df_oldest.with_columns([
            pl.col(victim_col).alias("victim_l"),
            pl.col(attacker_col).alias("attacker_l")
        ])

        # Sort attack_df_oldest by victim-attacker and time ascending
        attack_df_oldest = (
            attack_df_oldest
            .sort(["victim_l", "attacker_l", "time"])
        ).lazy()

        previous_transfers = (
                previous_transfers
                .sort(["sender_l", "receiver_l", "time_prev"])
            ).lazy()

        joined = attack_df_oldest.join_asof(
            previous_transfers,
            left_on="time",
            right_on="time_prev",
            by_left=["victim_l", "attacker_l"],
            by_right=["sender_l", "receiver_l"],
            strategy="backward"  #only pick one previous transfer
        )
        attack_df_lazy = attack_df.lazy()
        joined = joined.filter(pl.col("time") > pl.col("time_prev")) #if timeprev is null the row is dropped from joined meaning if no previous interacion then we wont drop it from attakc_df
        attack_df  = attack_df_lazy.join(joined, on=["transactionHash","id"], how="anti").collect(engine="streaming") #joined has only rows where v sent to attacker
        return  attack_df

    #if attacker has sent more than 10$ in legit tokens to victim before poisoning attack
    #LETS SAY WE have batch=1DAY; lets say we have three attacks by the same attacker on the same victim in the same batch;
    #if attacker has sent more than10$ to victim between those attacks it means; attacker knows victim and we should remove suspicious transfers from df
    #therefore we can check if attacker has sent tokens previous to newest attack in the batch
    def check_attacker_victim_familiarity(self, attack_df=None, transfers_df=None, reversed=False, ram_load="high"):
        if reversed == True:
            victim_col, attacker_col = "receiver", "sender"
        else:
            victim_col, attacker_col = "sender", "receiver"

        if ram_load=="high":
            if transfers_df is None or transfers_df.is_empty():
                return attack_df
        victims = attack_df[victim_col].unique().to_list()
        attackers = attack_df[attacker_col].unique().to_list()


        pair_totals = (
            transfers_df.filter(
                (pl.col("receiver").is_in(victims)) &
                (pl.col("sender").is_in(attackers))
            )
            .with_columns(
                (
                    (pl.col("amount").cast(pl.Float64) /
                     (10 ** pl.col("decimals").cast(pl.Float64)))
                    * pl.col("price")
                ).alias("usd_value")
            )
            .group_by(["receiver", "sender"])
            .agg(pl.sum("usd_value").alias("total_usd"))
            .with_columns(
                (pl.col("total_usd") >= 10).alias("has_received_over_10usd")
            )
        )
        if ram_load=="lower":
            pair_totals=pair_totals.collect(engine="streaming")

        attack_df = attack_df.join(
            pair_totals.filter(pl.col("has_received_over_10usd")),
            left_on=[victim_col, attacker_col],
            right_on=["receiver", "sender"],
            how="anti",
        )
        
        return attack_df

    #we can do the same way as in check_victim_attacker_familiarity, because of possible payouts during batch period
    def check_behaviour_zero(self, path_data,ERC20_decimals_map,ERC20_price_map,current_time_min, current_time_max, attack_df=None, reversed=False,scan_parquet_fn=pl.scan_parquet):
        logs_path = path_data["parquet_data"]
        if reversed == True:
            victim_col, attacker_col = "receiver", "sender"
        else:
            victim_col, attacker_col = "sender", "receiver"
        #######
        #Following is checking if num_in>0 for each attacker
        #######
        attack_df_oldest = attack_df.sort(["time"], descending=False).group_by([victim_col, attacker_col]).head(1)

        attackers = attack_df_oldest[attacker_col].unique().to_list()

        previous_transfers = scan_parquet_fn(logs_path).with_columns([
            pl.col("sender").str.to_lowercase(),
            pl.col("receiver").str.to_lowercase(),
        ]).filter(
            (pl.col("receiver").is_in(attackers)) &
            pl.col("contract").is_in(ERC20_decimals_map)
        )

        previous_transfers = previous_transfers.with_columns([
            pl.col("contract")
                .replace_strict(ERC20_decimals_map, return_dtype=pl.Utf8)
                .alias("decimals"),
            pl.col("contract")
                .replace_strict(ERC20_price_map, return_dtype=pl.Float64)
                .alias("price")
        ])

        previous_transfers = previous_transfers.filter( 
            (((pl.col("amount").cast(pl.Float64) / (10 ** pl.col("decimals").cast(pl.Float64))) * pl.col("price")) >= 0.01)
        )

        previous_transfers_old = previous_transfers.filter( 
            (pl.col("time") < current_time_min) )

        previous_transfers_old = previous_transfers_old.select("receiver").unique().collect(engine="streaming")

        if not previous_transfers_old.is_empty():
            missing = list(
                set(attackers) - set(previous_transfers_old["receiver"].to_list())
            ) #those that did not receive before time_min
        else:
            missing = list(
                set(attackers)
            )
        previous_transfers_new = previous_transfers.filter( 
            (pl.col("time") >= current_time_min) &
            (pl.col("time") < current_time_max)  &
            (pl.col("receiver").is_in(missing))
        ).select("receiver","time")

        previous_transfers_new = previous_transfers_new.rename({
            "time": "time_prev",
        })

        previous_transfers_new = previous_transfers_new.with_columns([
            pl.col("receiver").alias("receiver_l")
        ]).collect(engine="streaming")

        attack_df_missing = attack_df.with_columns([
            pl.col(victim_col).alias("victim_l"),
            pl.col(attacker_col).alias("attacker_l")
        ]).filter(
            (pl.col(attacker_col).is_in(missing))
        )
        
        if not previous_transfers_new.is_empty():
            previous_transfers_new = previous_transfers_new.sort(["receiver", "time_prev"])

        attack_df_missing = attack_df_missing.sort([attacker_col, "time"])
        result = attack_df_missing.join_asof(
            previous_transfers_new,
            left_on="time",
            right_on="time_prev",
            by_left="attacker_l",
            by_right="receiver_l",
            strategy="backward"
        )
        
        result = result.filter(pl.col("time_prev").is_null()).select(["transactionHash","id","attacker_l"]) #te które nie dostały w batchu

        attack_df = attack_df.join(
            result,
            on=["transactionHash","id"],
            how="anti"
        )

        step3_map = {
            (row["transactionHash"], row["id"]): {
                "attacker": row["attacker_l"]
            }
            for row in result.iter_rows(named=True)
        }

        #######
        #Following is checking if num_out>0
        #######
        attack_df_oldest = attack_df.sort(["time"], descending=False).group_by([victim_col, attacker_col]).head(1)
        attackers = attack_df_oldest[attacker_col].unique().to_list()

        previous_transfers = scan_parquet_fn(logs_path).with_columns([
            pl.col("sender").str.to_lowercase(),
            pl.col("receiver").str.to_lowercase(),
        ]).filter(
            (pl.col("sender").is_in(attackers)) &
            pl.col("contract").is_in(ERC20_decimals_map)
        )

        previous_transfers = previous_transfers.with_columns([
            pl.col("contract")
                .replace_strict(ERC20_decimals_map, return_dtype=pl.Utf8)
                .alias("decimals"),
            pl.col("contract")
                .replace_strict(ERC20_price_map, return_dtype=pl.Float64)
                .alias("price")
        ])

        previous_transfers = previous_transfers.filter( 
            (((pl.col("amount").cast(pl.Float64) / (10 ** pl.col("decimals").cast(pl.Float64))) * pl.col("price")) >= 0.01)
        )

        previous_transfers_old = previous_transfers.filter( 
            (pl.col("time") < current_time_min) )

        previous_transfers_old = previous_transfers_old.select("sender").unique().collect(engine="streaming")
        if not previous_transfers_old.is_empty():
            missing = list(
                set(attackers) - set(previous_transfers_old["sender"].to_list())
            )
        else:
            missing = list(
                set(attackers)
            )
        
        previous_transfers_new = previous_transfers.filter( 
            (pl.col("time") >= current_time_min) &
            (pl.col("time") < current_time_max)  &
            (pl.col("sender").is_in(missing))
        ).select("sender","time")

        previous_transfers_new = previous_transfers_new.rename({
            "time": "time_prev",
        })

        previous_transfers_new = previous_transfers_new.with_columns([
            pl.col("sender").alias("sender_l")
        ]).collect(engine="streaming")

        attack_df_missing = attack_df.with_columns([
            pl.col(victim_col).alias("victim_l"),
            pl.col(attacker_col).alias("attacker_l")
        ]).filter(
            (pl.col(attacker_col).is_in(missing))
        )
        if not previous_transfers_new.is_empty():
            previous_transfers_new = previous_transfers_new.sort(["sender", "time_prev"])
        attack_df_missing = attack_df_missing.sort([attacker_col, "time"])

        result = attack_df_missing.join_asof(
            previous_transfers_new,
            left_on="time",
            right_on="time_prev",
            by_left="attacker_l",
            by_right="sender_l",
            strategy="backward"
        )

        result = result.filter(pl.col("time_prev").is_null()).select(["transactionHash","id","attacker_l"]) #te które nie dostały w batchu
        
        for row in result.iter_rows(named=True):
            key = (row["transactionHash"], row["id"])
            step3_map[key] = {"attacker": row["attacker_l"]}

        return step3_map

      
    #if there are many transfers to different receipents in the same transaction +score
    def check_if_batched(self, attack_df=None, reversed=False):
        if reversed == True:
            victim_col, attacker_col = "receiver", "sender"
        else:
            victim_col, attacker_col = "sender", "receiver"
        grouped = (
            attack_df
            .group_by("transactionHash")
            .agg(pl.col(victim_col).n_unique().alias("unique_count"))
            .filter(pl.col("unique_count") > 1)  #only batched transfers
        )
        batched_df = attack_df.join(grouped, on="transactionHash", how="inner")
        step2_map = {
            (row["transactionHash"], row["id"]): {"attacker": row[attacker_col]}
            for row in batched_df.iter_rows(named=True)
        }
        
        return step2_map

    @staticmethod
    def save_map_to_csv(data_map, filename, key_name="key", chain_name="Arbitrum"):
        if not data_map:
            return
        all_fields = set()
        for key, value in data_map.items():
            if isinstance(value, dict):
                all_fields.update(value.keys())
            else:
                all_fields.add('value')
        all_fields.add(key_name)
        fieldnames = [key_name] + sorted([f for f in all_fields if f != key_name])
        filename = os.path.join(f"results_{chain_name}", filename)
        file_exists = os.path.exists(filename)
        rows = []
        for key, value in data_map.items():
            row = {key_name: key}
            if isinstance(value, dict):
                row.update(value)
            else:
                row['value'] = value
            rows.append(row)
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)
    
    def get_tx_sender(self,tx_hash,provider):
        i=1
        retries=float("inf")
        delay=10
        attempt = 1
        #provider = Web3(Web3.HTTPProvider(self.rpc2))
        while attempt < retries:
            if attempt%10==0:
                delay+=10
                provider = Web3(Web3.HTTPProvider(self.rpc))
            elif attempt%5==0:
                delay+=10
                provider = Web3(Web3.HTTPProvider(self.rpc2))
            try:
                tx = provider.eth.get_transaction(tx_hash)
                return tx["from"]
            except ChunkedEncodingError:
                attempt += 1
                print(f"ChunkedEncodingError failed for {tx_hash}, retrying...")
                time.sleep(delay)
                delay+=5
                continue
            except HTTPError as e:
                print("HTTPError:", e)
                time.sleep(delay)
                attempt += 1
                delay+=5
                continue
            except Exception as e:
                print(f"[{attempt}] Unexpected error: {e}. Retrying in {delay}s...")
                time.sleep(delay)
                attempt += 1
                delay+=5

              
    def check_if_sender_of_tx(self, attack_df=None, reversed=False):
        hash_map_senders = {}
        txhashes = attack_df["transactionHash"].unique().to_list()
        w3 = Web3(Web3.HTTPProvider(self.rpc, request_kwargs={"timeout": 120}))
        for txhash in txhashes:
            sender_address = self.get_tx_sender(txhash, w3).lower()
            hash_map_senders[txhash]=sender_address

        sender_df = pl.DataFrame({
            "transactionHash": list(hash_map_senders.keys()),
            "sender_address": list(hash_map_senders.values())
        },
        schema={
            "transactionHash": pl.Utf8,
            "sender_address": pl.Utf8
        })
        if reversed == True:
            victim_col, attacker_col = "receiver", "sender"
        else:
            victim_col, attacker_col = "sender", "receiver"
        if not hash_map_senders:
            return attack_df, hash_map_senders
        sender_df = sender_df.rename({"sender_address": victim_col})
        #remove rows where the sender matches the victim
        attack_df = attack_df.join(sender_df, on=["transactionHash", victim_col], how="anti")
        return attack_df, hash_map_senders
       
    def run_detection(self):
        print("self.ram_load:")
        print(self.ram_load)
        pl.Config.set_tbl_rows(5)
        reverse_arr = [False,True]
        for attack_type in ("fake", "zero"):
            df_type = self.fake_df if attack_type == "fake" else self.zero_df
            if df_type.height == 0:
                continue
            print(f"processing {attack_type}")
            print("df_types[i].height:", df_type.height)
            self.previous_df = find_previous_interactions_victim_attacker(
                attack_df=df_type,
                path_data=self.path_data,
                current_time_max=df_type["time"].max(),
                ERC20_decimals_map=self.ERC20_decimals_map,
                ERC20_price_map=self.ERC20_price_map,
                ram_load=self.ram_load
            )
            print("AFTER")
            for reversed in reverse_arr:
                attack_df = df_type
                attack_df = self.check_victim_attacker_familiarity(attack_df,self.previous_df,reversed,self.ram_load)
                print("AFTER2")
                attack_df = self.check_attacker_victim_familiarity(attack_df,self.previous_df,reversed,self.ram_load)
                print("AFTER FAMILIARITY CHECKS")
                if attack_df.height == 0:
                    continue
                if self.ram_load=="lower":
                    ten_previous_df = find_previous_victim_interactors_ram_lower(attack_df,self.path_data,df_type["time"].min(),df_type["time"].max(),self.ERC20_decimals_map,self.ERC20_price_map,reversed)
                else:
                    ten_previous_df = find_previous_victim_interactors(attack_df,self.path_data,df_type["time"].max(),self.ERC20_decimals_map,self.ERC20_price_map,reversed)
                print("ten_previous_df.height",ten_previous_df.height)
                attack_df, step1_map = self.block_similarity_score(attack_df,ten_previous_df,reversed)
                attack_df, hash_map_senders = self.check_if_sender_of_tx(attack_df,reversed) #after block_similarity_score to reduce api calls
                if len(hash_map_senders) > 0:
                    keys_to_delete = [
                        key
                        for key, data in step1_map.items()
                        if key[0] in hash_map_senders
                        and data.get("victim") == hash_map_senders[key[0]]
                    ]
                    for key in keys_to_delete:
                        del step1_map[key]
                self.save_map_to_csv(step1_map,f"{self.rollup_name}_{attack_type}_step1_{reversed}.csv",key_name="transaction_key",chain_name=self.rollup_name)

                del ten_previous_df, step1_map, hash_map_senders
                if attack_df.height > 0:  
                    step2_map=self.check_if_batched(attack_df,reversed) 
                    self.save_map_to_csv(step2_map, f"{self.rollup_name}_{attack_type}_step2_{reversed}.csv", key_name="transaction_key",chain_name=self.rollup_name)
                    del step2_map
                    step3_map = self.check_behaviour_zero(self.path_data,self.ERC20_decimals_map,self.ERC20_price_map,attack_df["time"].min(),attack_df["time"].max(), attack_df, reversed)
                    self.save_map_to_csv(step3_map,f"{self.rollup_name}_{attack_type}_step3_{reversed}.csv",key_name="transaction_key",chain_name=self.rollup_name) 
                    del step3_map
                    mem()
                print(attack_df.height)
                #df_type = df_type.join(attack_df,how="anti",on=["transactionHash","id"]) #using this would possibly not capture all attacks, because of filtering check_if_sender_of_tx etc.
                del attack_df
            del self.previous_df
            gc.collect()
            if attack_type == "fake":
                del self.fake_df
            if attack_type == "zero":
                del self.zero_df 
                
