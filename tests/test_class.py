import polars as pl
from poisoning_detector.steps_runner import StepsRunner
import sys
import pytest
from poisoning_detector.find_previous_transfers import find_previous_interactions_victim_attacker,find_previous_victim_interactors,find_previous_victim_interactors_ram_lower
from poisoning_detector.get_tokens import GetTokens
from dotenv import load_dotenv
import os
from datetime import datetime
class TestClass:
    pl.Config.set_fmt_str_lengths(50)
    pl.Config.set_tbl_cols(15)

    def test_victim_attacker_similarities(self,empty_dataframe,batch_dataframe,decimals_map,price_map,logs_dataframe):
        path_data = dict()
        path_data['parquet_data'] = "Mock"
        runner = StepsRunner(zero_df=empty_dataframe,
                             fake_df=batch_dataframe,
                            ERC20_decimals_map=decimals_map,
                            rollup_name="optimism",
                            rpc="https://mainnet.optimism.io",
                            rpc2="https://mainnet.optimism.io",
                            ERC20_price_map=price_map)
        logs_result = find_previous_interactions_victim_attacker(
            attack_df=batch_dataframe,
            path_data=path_data,
            current_time_max=batch_dataframe["time"].max(),
            ERC20_decimals_map=decimals_map,
            ERC20_price_map=price_map,
            scan_parquet_fn=lambda _: logs_dataframe.lazy()
        )
        #exclude_times = pl.Series(
        #    ["2025-12-01 00:00:11", "2025-12-01 00:00:13"]
        #).str.strptime(pl.Datetime(time_unit='us', time_zone=None), format="%Y-%m-%d %H:%M:%S")
        #good_result = logs_dataframe.filter(~pl.col("time").is_in(exclude_times))
        good_result = logs_dataframe.filter(pl.col("transactionHash")
                                            .is_in(["attacker_10_dollars",
                                                    "victim_has_send_to_attacker",
                                                    "victim_transfer_at_the_end_of_batch"]))
        assert logs_result is not None
        
        compare = logs_result.clone()
        compare = compare.drop("decimals")
        compare = compare.drop("price")
        assert compare.equals(good_result)

        attack_df = runner.check_victim_attacker_familiarity(batch_dataframe,logs_result,False)

        assert attack_df is not None   
        good_result = batch_dataframe.filter(~pl.col("transactionHash")
                                            .is_in(["victim_knows_attacker_attack_in_batch"]))
        assert attack_df.equals(good_result)

        attack_df = runner.check_attacker_victim_familiarity(attack_df,logs_result,False)

        assert attack_df is not None
        good_result = good_result.filter(~pl.col("transactionHash")
                                            .is_in(["attacker_knows_victim_attack_in_batch"]))
        assert attack_df.equals(good_result)


    def test_victim_attacker_similarities_ram_load_lower(self,empty_dataframe,batch_dataframe,decimals_map,price_map,logs_dataframe):
        path_data = dict()
        path_data['parquet_data'] = "Mock"
        runner = StepsRunner(zero_df=empty_dataframe,
                             fake_df=batch_dataframe,
                            ERC20_decimals_map=decimals_map,
                            rollup_name="optimism",
                            rpc="https://mainnet.optimism.io",
                            rpc2="https://mainnet.optimism.io",
                            ERC20_price_map=price_map)
        logs_result: pl.LazyFrame = find_previous_interactions_victim_attacker(
            attack_df=batch_dataframe,
            path_data=path_data,
            current_time_max=batch_dataframe["time"].max(),
            ERC20_decimals_map=decimals_map,
            ERC20_price_map=price_map,
            scan_parquet_fn=lambda _: logs_dataframe.lazy(),
            ram_load="lower"
        )
        attack_df = runner.check_victim_attacker_familiarity(batch_dataframe,logs_result,False,ram_load="lower")
        assert attack_df is not None   
        good_result = batch_dataframe.filter(~pl.col("transactionHash")
                                            .is_in(["victim_knows_attacker_attack_in_batch"]))
        assert attack_df.equals(good_result)
        attack_df = runner.check_attacker_victim_familiarity(attack_df,logs_result,False,ram_load="lower")
        assert attack_df is not None
        good_result = good_result.filter(~pl.col("transactionHash")
                                            .is_in(["attacker_knows_victim_attack_in_batch"]))
        assert attack_df.equals(good_result)

    def test_find_previous_victim_interactors_ram_lower(self,empty_dataframe,batch_dataframe,decimals_map,price_map,eleven_logs_dataframe):
        path_data = dict()
        path_data['parquet_data'] = "Mock"
        print(batch_dataframe["time"].min())
        print(batch_dataframe["time"].max())
        logs_result = find_previous_victim_interactors_ram_lower(
            attack_df=batch_dataframe, 
            path_data=path_data,
            current_time_min=batch_dataframe["time"].min(),
            current_time_max=batch_dataframe["time"].max(),
            ERC20_decimals_map=decimals_map,
            ERC20_price_map=price_map,
            reversed=False,
            scan_parquet_fn=lambda _: eleven_logs_dataframe.lazy(),
            number_of_inter=2
            )
        assert logs_result.height==12
        seconds = 25
        logs_result = logs_result.sort("time", descending=True)
        time_list = logs_result["time"].to_list()
        for i in range(len(time_list)):
            if i==0:
                dt = datetime(2025, 12, 1, 0, 0, 3)
                assert time_list[i] == dt 
                continue
            if i==1:
                dt = datetime(2025, 12, 1, 0, 0, 2)
                assert time_list[i] == dt
                continue
            dt = datetime(2025, 11, 30, 0, 0, seconds)
            assert time_list[i] == dt 
            seconds-=1


    def test_similarity_score_and_time(self,empty_dataframe,batch_dataframe,decimals_map,price_map,logs_dataframe):
        path_data = dict()
        path_data['parquet_data'] = "Mock"
        #Simulating attack_df state as it would exit test_victim_attacker_similarities 
        attack_df = batch_dataframe.filter(~pl.col("transactionHash") 
                                            .is_in(["victim_knows_attacker_attack_in_batch",
                                                    "attacker_knows_victim_attack_in_batch"]))
        runner = StepsRunner(zero_df=empty_dataframe,
                             fake_df=attack_df,
                            ERC20_decimals_map=decimals_map,
                            rollup_name="optimism",
                            rpc="https://mainnet.optimism.io",
                            rpc2="https://mainnet.optimism.io",
                            ERC20_price_map=price_map)
        
        logs_result = find_previous_victim_interactors(
            attack_df=attack_df, 
            path_data=path_data,
            current_time_max=attack_df["time"].max(),
            ERC20_decimals_map=decimals_map,
            ERC20_price_map=price_map,
            reversed=False,
            scan_parquet_fn=lambda _: logs_dataframe.lazy()
            )

        assert logs_result is not None
        #print(logs_result)
        good_result = pl.DataFrame({
            "sender": [ 
                "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 0
                "0x16e5bb573768b325ccd72016136d7092a309c79e",  #victim 1
                "0x26e5bb573768b325ccd72016136d7092a309c79e",  #victim 2
                "0x36e5bb573768b325ccd72016136d7092a309c79e",  #victim 3
                "0x46e5bb573768b325ccd72016136d7092a309c79e",  #victim 4
            ], 
            "receiver": [
                ["0x000333123768b325ccd72016136d709123333000","0x444333123768b325ccd72016136d709440033777"],
                ["0x111222fff768b325ccd72016136d709333fff111"],
                ["0x22ffff444768b325ccd72016136d709444333fff"],
                ["0x333fff333768b325ccd72016136d709333fff111"],
                ["0x333abc333768b325ccd72016136d709333ffff11"]
            ],
            "time": [
                ["2025-11-30 23:59:51","2025-11-30 23:59:53"],
                ["2025-11-30 23:59:53"],
                ["2025-11-30 23:59:53"],
                ["2025-11-30 23:59:53"],
                ["2025-11-30 23:59:53"]
            ],
            "transactionHash": [
                ["intended_transfer_0","intended_transfer_1"],
                ["intended_transfer_2"],
                ["intended_transfer_3"],
                ["intended_transfer_4"],
                ["intended_transfer_5"]    
            ]        
        },
        schema={
            "sender": pl.String,
            "receiver": pl.List(pl.String),
            "time": pl.List(pl.Datetime(time_unit='us', time_zone=None)),
            "transactionHash": pl.List(pl.String)
        })
        good_result=good_result.sort(["sender"], descending=True)
        logs_result = logs_result.sort(["sender"], descending=True)
        #assert logs_result.equals(good_result)
        for row_a, row_b in zip(good_result.iter_rows(named=True),logs_result.iter_rows(named=True)):
            assert row_a["sender"] == row_b["sender"]
            assert row_a["receiver"].sort() == row_b["receiver"].sort()
            assert row_a["time"].sort() == row_b["time"].sort()
            assert row_a["transactionHash"].sort() == row_b["transactionHash"].sort()

        #print(attack_df)
        good_result = attack_df.clone().filter(~pl.col("transactionHash")
                                            .is_in(["victim2_attack_df"]))
        attack_df, result_map = runner.block_similarity_score(attack_df,logs_result,False)
        assert attack_df.height == 6
        assert good_result.equals(attack_df)            
        assert result_map[('victim3_attack_df', 0)]["score"] == 6      
        assert result_map[('victim1_attack_df', 0)]["score"] == 9  
        assert result_map[('victim0_attack_df_v2', 0)]["score"] == 11
        assert result_map[('victim1and0_attack_df_same_txHash', 0)]["score"] == 9
        assert result_map[('victim1and0_attack_df_same_txHash', 2)]["score"] == 12
        assert result_map[('victim4_attack_df', 0)]["score"] == 5
    

        assert result_map[('victim3_attack_df', 0)]["delta"] == 8
        assert result_map[('victim1_attack_df', 0)]["delta"] == 8
        assert result_map[('victim0_attack_df_v2', 0)]["delta"] == 16
        assert result_map[('victim1and0_attack_df_same_txHash', 0)]["delta"] == 12
        assert result_map[('victim1and0_attack_df_same_txHash', 2)]["delta"] == 14
        assert result_map[('victim4_attack_df', 0)]["delta"] == 14


    def test_check_if_batched(self,empty_dataframe,batch_dataframe,decimals_map,price_map,logs_dataframe):
        attack_df = batch_dataframe.filter(~pl.col("transactionHash") 
                        .is_in(["victim_knows_attacker_attack_in_batch",
                                "attacker_knows_victim_attack_in_batch",
                                "victim2_attack_df"])) #simulate attack_df as it would be before check_if_batched method
        runner = StepsRunner(zero_df=empty_dataframe,
                         fake_df=attack_df,
                        ERC20_decimals_map=decimals_map,
                        rollup_name="optimism",
                        rpc="https://mainnet.optimism.io",
                        rpc2="https://mainnet.optimism.io",
                        ERC20_price_map=price_map)
        result_map = runner.check_if_batched(attack_df,False) 
        assert len(result_map)==2
        for key, value in result_map.items():
            assert key in [("victim1and0_attack_df_same_txHash", 0),("victim1and0_attack_df_same_txHash", 2)]
            attacker = value["attacker"]
            assert attacker in ["0x000333444768b325ccd72016136d709444333000","0x111222333768b325ccd72016136d709333222111"]

    def test_check_behaviour_zero(self,empty_dataframe,batch_dataframe,decimals_map,price_map,logs_dataframe):
        path_data = dict()
        path_data['parquet_data'] = "Mock"
        attack_df = batch_dataframe.filter(~pl.col("transactionHash") 
                    .is_in(["victim_knows_attacker_attack_in_batch",
                            "attacker_knows_victim_attack_in_batch",
                            "victim2_attack_df"]))
        runner = StepsRunner(zero_df=empty_dataframe,
                     fake_df=attack_df,
                    ERC20_decimals_map=decimals_map,
                    rollup_name="optimism",
                    rpc="https://mainnet.optimism.io",
                    rpc2="https://mainnet.optimism.io",
                    ERC20_price_map=price_map)
        
        result_map = runner.check_behaviour_zero(path_data,decimals_map,price_map,attack_df["time"].min(),attack_df["time"].max(), attack_df, False, scan_parquet_fn=lambda _: logs_dataframe.lazy())
        assert len(result_map)==3

        for key, value in result_map.items():
            assert key in [("victim1and0_attack_df_same_txHash",2),("victim1_attack_df",0),("victim3_attack_df",0)]
            attacker = value["attacker"]
            assert attacker in ["0x000333444768b325ccd72016136d709444333000","0x111222333768b325ccd72016136d709333222111","0x333222333768b325ccd72016136d709333222111"]


    def test_empty_df(self,empty_dataframe,batch_dataframe,decimals_map,price_map,empty_lazyframe):
        path_data = dict()
        path_data['parquet_data'] = "Mock"
        runner = StepsRunner(zero_df=None,
                    fake_df=batch_dataframe,
                    ERC20_decimals_map=decimals_map,
                    rollup_name="optimism",
                    rpc="https://mainnet.optimism.io",
                    rpc2="https://mainnet.optimism.io",
                    ERC20_price_map=price_map)
        
        attack_df = runner.check_victim_attacker_familiarity(attack_df=batch_dataframe,transfers_df=empty_dataframe,reversed=False)
        assert attack_df.equals(batch_dataframe)
        attack_df = runner.check_attacker_victim_familiarity(attack_df=batch_dataframe,transfers_df=empty_dataframe,reversed=False)
        assert attack_df.equals(batch_dataframe)
        attack_df, result_map = runner.block_similarity_score(attack_df,empty_dataframe,False)
        assert attack_df.height == 2
        assert result_map[('attacker_knows_victim_attack_in_batch', 0)]["score"] == 3
        assert result_map[('victim_knows_attacker_attack_in_batch', 0)]["score"] == 3

        attack_df = runner.check_victim_attacker_familiarity(attack_df=batch_dataframe,transfers_df=empty_lazyframe,reversed=False,ram_load="lower")
        assert attack_df.equals(batch_dataframe)
        attack_df = runner.check_attacker_victim_familiarity(attack_df=batch_dataframe,transfers_df=empty_lazyframe,reversed=False,ram_load="lower")
        assert attack_df.equals(batch_dataframe)


    def test_sender_of_tx_dict_removal(self):
        result_map={}
        result_map[("aaaaa", 0)] = {
                    "score": "1",
                    "victim": "aaaa",
                }
        result_map[("bbbbb", 0)] = {
                    "score": "2",
                    "victim": "bbbb",
                }
        result_map[("bbbbb", 2)] = {
                    "score": "3",
                    "victim": "cccc",
                }
        result_map[("fffff", 0)] = {
                    "score": "4",
                    "victim": "jjjj",
                }
        hash_map_senders = {}
        hash_map_senders["bbbbb"]="bbbb"
        hash_map_senders["fffff"]="jjjj"
        keys_to_delete = [
            key
            for key, data in result_map.items()
            if key[0] in hash_map_senders
            and data.get("victim") == hash_map_senders[key[0]]
        ]
        for key in keys_to_delete:
            del result_map[key]
        assert len(result_map) == 2
        for key in result_map:
            assert key in [("aaaaa", 0), ("bbbbb", 2)]
            assert result_map[key]["victim"] in ["aaaa", "cccc"]
            assert result_map[key]["score"] in ["1", "3"]


    def test_length_of_previous_interactors(self,previous_receivers_dataframe,batch_dataframe,decimals_map,price_map):
        path_data = dict()
        path_data['parquet_data'] = "Mock"
        runner = StepsRunner()
        logs_result = find_previous_victim_interactors(
            attack_df=batch_dataframe, 
            path_data=path_data,
            current_time_max=batch_dataframe["time"].max(),
            ERC20_decimals_map=decimals_map,
            ERC20_price_map=price_map,
            reversed=False,
            scan_parquet_fn=lambda _: previous_receivers_dataframe.lazy()
            )
        i=0
        for row in batch_dataframe.iter_rows(named=True):
            victim = row["sender"]
            prev = (
                    logs_result
                    .filter(pl.col("sender") == victim)
            )
            if prev.height>0:
                i+=1
                attack_time = row["time"]
                receivers_list, hashes_list, times_list = runner.get_last_ten_interactors(prev_df=logs_result, attacker_col="receiver", attack_time=attack_time) 
                assert logs_result is not None
                assert len(hashes_list) <= 10
                assert len(receivers_list) <= 10
                assert len(times_list) <= 10
                x=111
                for h in hashes_list:
                    assert int(h)==x 
                    x-=1
        assert i==2


    def test_get_token_contracts(self, rpc_map, tokens_map):
        for name, rpc in rpc_map.items():
            tokenator = GetTokens(RPC=rpc, RPC2=rpc)
            if name=="optimism":
                token_name, symbol, decimals = tokenator.get_token_info(tokens_map[name], rpc)
            token_name, symbol, decimals = tokenator.get_token_info(tokens_map[name], rpc)
            print(f"Chain: {name}, Token: {token_name}, Symbol: {symbol}, Decimals: {decimals}")
            assert token_name is not None
            assert symbol is not None
            assert decimals is not None
        