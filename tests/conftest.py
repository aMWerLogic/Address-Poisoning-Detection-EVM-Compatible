import pytest
import polars as pl

@pytest.fixture
def logs_dataframe():
    df_test = pl.DataFrame({
        "blockNumber": [ 95,  95,  #victim attacker similarity 
                        95,96, 96, 96, 96, 96, #victims' intended transfers (v0 has two)
                        97,98, #a4
                        98, #a2
                        101,98, #a1
                        102,98, #a3
                        100, 100, 101, 102, 102, 103, 104,
                        104, 
                        105, #a2 should not be picked
                        106,
                        104,104],
        "transactionHash": ["attacker_10_dollars",
                            "victim_has_send_to_attacker",
                            "intended_transfer_0",
                            "intended_transfer_1",
                            "intended_transfer_2",
                            "intended_transfer_3",
                            "intended_transfer_4",
                            "intended_transfer_5",
                            "attacker4_interaction_previous_out",
                            "attacker4_interaction_previous_in",
                            "attacker2_interaction_previous_in",
                            "attacker1_interaction_previous_in",
                            "attacker1_interaction_previous_out",
                            "attacker3_interaction_previous_out",
                            "attacker3_interaction_previous_in",
                            "victim3_attack_df", 
                            "victim1_attack_df",
                            "victim2_attack_df",
                            "victim1and0_attack_df_same_txHash", #same txhash
                            "victim1and0_attack_df_same_txHash", #same txhash
                            "victim4_attack_df",
                            "victim0_attack_df_v2",
                            "victim_transfer_at_the_end_of_batch",
                            "attacker_transfer_after_batch_(should_not_consider)",
                            "transfer_after_batch_(should_not_consider)",
                            "attacker_knows_victim_attack_in_batch",
                            "victim_knows_attacker_attack_in_batch"],
        "id": [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0],
        "time": ["2025-11-30 23:59:51",
                "2025-11-30 23:59:51",
                "2025-11-30 23:59:51", #Intended transfers start here
                "2025-11-30 23:59:53",
                "2025-11-30 23:59:53",
                "2025-11-30 23:59:53",
                "2025-11-30 23:59:53",
                "2025-11-30 23:59:53",  #Intended transfers end here
                 "2025-11-30 23:59:55", #a4
                 "2025-11-30 23:59:57", #a4
                 "2025-11-30 23:59:57", #a2
                 "2025-12-01 00:00:03", #a1
                 "2025-11-30 23:59:57", #a1
                 "2025-12-01 00:00:05", #a3
                 "2025-11-30 23:59:57", #a3
                 "2025-12-01 00:00:01",  #100 a3
                 "2025-12-01 00:00:01",  #100 a1    
                 "2025-12-01 00:00:03",  #101
                 "2025-12-01 00:00:05",  #102 a1
                 "2025-12-01 00:00:05",  #102
                 "2025-12-01 00:00:07",  #103
                 "2025-12-01 00:00:09",  #104 a3
                 "2025-12-01 00:00:09", #should not take into account after this one 
                 "2025-12-01 00:00:11", 
                 "2025-12-01 00:00:13",#till this one 
                "2025-12-01 00:00:09",#attacker that has send over 10$ to victim in prev
                "2025-12-01 00:00:09"],
        "sender": [ "0xaaa333444768b325ccd72016136d709444333777",  #attacker that has send over 10$ to victim in prev
                    "0xbbb5bb573768b325ccd72016136d7092a309c79e",  #victim has send tokens to attacker in prev
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #v0 #Intended transfers start here
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #v0
                    "0x16e5bb573768b325ccd72016136d7092a309c79e", #v1
                    "0x26e5bb573768b325ccd72016136d7092a309c79e", #v2
                    "0x36e5bb573768b325ccd72016136d7092a309c79e", #v3
                    "0x46e5bb573768b325ccd72016136d7092a309c79e", #v4 #Intended transfers ends here
                    "0x444333444768b325ccd72016136d709444333777", #a4
                    "0xa4a4bb573768b325ccd72016136d7092a309c79e", #a4 interactor
                    "0xa2a2bb573768b325ccd72016136d7092a309c79e", #a2 interactor
                    "0xa1a1bb573768b325ccd72016136d7092a309c79e", #a1 interactor
                    "0x111222333768b325ccd72016136d709333222111", #a1
                    "0x333222333768b325ccd72016136d709333222111", #a3 IS
                    "0xa3a3bb573768b325ccd72016136d7092a309c79e",  #a3 interactor  15
                    "0x36e5bb573768b325ccd72016136d7092a309c79e",  #victim 3  a3  ###BATCH starts here
                    "0x16e5bb573768b325ccd72016136d7092a309c79e",  #victim 1  a1
                    "0x26e5bb573768b325ccd72016136d7092a309c79e",  #victim 2  a2
                    "0x16e5bb573768b325ccd72016136d7092a309c79e",  #victim 1  a1
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 0  a0
                    "0x46e5bb573768b325ccd72016136d7092a309c79e",  #victim 4  a3
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 0  a4  ###BATCH ends here
                    "0xaaa5bb573768b325ccd72016136d7092a309c79e",  #victim aaa send legit tokens to a0 (during batch)
                    "0x222333444768b325ccd72016136d709444333222",    #a2 #NOT in batch - should not be picked
                    "0xccc5bb573768b325ccd72016136d7092a309c79e",
                    "0xaaa5bb573768b325ccd72016136d7092a309c79e",  #victim aaa
                    "0xbbb5bb573768b325ccd72016136d7092a309c79e",  #victim bbb
                    ], 
        "contract": ["0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607", 
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607", 
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31000", "0x7f5c764cbc14f9669b88837ca1490cca17c31000", #fake contracts
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31000","0x7f5c764cbc14f9669b88837ca1490cca17c31000",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31000","0x7f5c764cbc14f9669b88837ca1490cca17c31000",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31000",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31000",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31000"],
        "amount": ["11000000","2000000","100000000", "100000000", "100000000", "100000000", "100000000", "100000000","1000000","1000000","1000000", "1000000", "1000000", "1000000", "1000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "1000000", "1000000000", "100000000", "100000000"],
        "valid": ["1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1"], #5 different atackers
        

        "receiver": [
                    "0xaaa5bb573768b325ccd72016136d7092a309c79e",
                    "0xbbb333444768b325ccd72016136d709444333777",
                    "0x000333123768b325ccd72016136d709123333000", #v0 #Intended transfers start here
                    "0x444333123768b325ccd72016136d709440033777", #v0
                    "0x111222fff768b325ccd72016136d709333fff111", #v1
                    "0x22ffff444768b325ccd72016136d709444333fff", #v2
                    "0x333fff333768b325ccd72016136d709333fff111", #v3
                    "0x333abc333768b325ccd72016136d709333ffff11", #v4 #Intended transfers end here
                     "0xa4a4bb573768b325ccd72016136d7092a309c79e", #a4 interactor
                     "0x444333444768b325ccd72016136d709444333777", #a4
                     "0x222333444768b325ccd72016136d709444333222", #a2
                     "0x111222333768b325ccd72016136d709333222111", #a1
                     "0xa1a1bb573768b325ccd72016136d7092a309c79e",  #a1 interactor  13
                     "0xa3a3bb573768b325ccd72016136d7092a309c79e",  #a3 interactor
                     "0x333222333768b325ccd72016136d709333222111", #a3 IR #NOT in batch  15
                     "0x333222333768b325ccd72016136d709333222111", # 1 in 0 out but 1 out in batch DONE  ###BATCH starts here
                     "0x111222333768b325ccd72016136d709333222111", # 0 in 1 out but 1 in in batch DONE
                     "0x222333444768b325ccd72016136d709444333222", # 1 in 0 out DONE
                     "0x111222333768b325ccd72016136d709333222111", # 0 in 1 out but 1 in in batch DONE
                     "0x000333444768b325ccd72016136d709444333000", # 0 prev interaction DONE
                     "0x333222333768b325ccd72016136d709333222111", # 1 in 0 out but 1 out in batch DONE
                     "0x444333444768b325ccd72016136d709444333777", #1 in 1 out in previous DONE   ###BATCH ends here
                     "0x000333444768b325ccd72016136d709444333000", #NOT in batch
                     "0xa2a2bb573768b325ccd72016136d7092a309c79e", #a2 interactor #NOT in batch  - should not be picked
                     "0xccc5bb573768b325ccd72016136d7092a309cccc",  #NOT in batch
                     "0xaaa333444768b325ccd72016136d709444333777", #attacker that has send over 10$ to victim in prev
                     "0xbbb333444768b325ccd72016136d709444333777"  #attacker that received tokens from victim in prev
                     ], 
    },
    schema={
        "blockNumber": pl.UInt64,
        "transactionHash": pl.String,
        "id":  pl.UInt32,
        "time": pl.Datetime(time_unit='us', time_zone=None),
        "sender": pl.String,
        "contract": pl.String,
        "amount": pl.String,
        "valid": pl.String,
        "receiver": pl.String,
    })
    return df_test

@pytest.fixture
def eleven_logs_dataframe():
    df_test = pl.DataFrame({
        "blockNumber": [ 95,  
                        95,  #victim attacker similarity 
                        95,
                        96, 
                        96, 
                        96, 
                        96, 
                        96, #victims' intended transfers (v0 has two)
                        97,
                        98, #a4
                        98, #a2
                        101,
                        98, #a1
                        102,
                        98, #a3
                        100, 
                        100, 
                        101, 
                        102, 
                        102, 
                        103, 
                        104,
                        104, 
                        105, #a2 should not be picked
                        106],
        "transactionHash": ["attacker_10_dollars",
                            "victim_has_send_to_attacker",
                            "intended_transfer_0",
                            "intended_transfer_1",
                            "intended_transfer_2",
                            "intended_transfer_3",
                            "intended_transfer_4",
                            "intended_transfer_5",
                            "attacker4_interaction_previous_out",
                            "attacker4_interaction_previous_in",
                            "attacker2_interaction_previous_in",
                            "attacker1_interaction_previous_in",
                            "attacker1_interaction_previous_out",
                            "attacker3_interaction_previous_out",
                            "attacker3_interaction_previous_in",
                            "victim3_attack_df", 
                            "victim1_attack_df",
                            "victim2_attack_df",
                            "victim1and0_attack_df_same_txHash", #same txhash
                            "victim1and0_attack_df_same_txHash", #same txhash
                            "victim4_attack_df",
                            "victim0_attack_df_v2",
                            "victim_transfer_at_the_end_of_batch",
                            "attacker_transfer_after_batch_(should_not_consider)",
                            "transfer_after_batch_(should_not_consider)"],
        "id": [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0],
        "time": ["2025-12-01 00:00:01",
                 "2025-12-01 00:00:02",
                 "2025-12-01 00:00:03", #Intended transfers start here
                 "2025-11-30 00:00:04",
                 "2025-11-30 00:00:05",
                 "2025-11-30 00:00:06",
                 "2025-11-30 00:00:07",
                 "2025-11-30 00:00:08",  #Intended transfers end here
                 "2025-11-30 00:00:09", #a4
                 "2025-11-30 00:00:10", #a4
                 "2025-11-30 00:00:11", #a2
                 "2025-11-30 00:00:12", #a1
                 "2025-11-30 00:00:13", #a1
                 "2025-11-30 00:00:14", #a3
                 "2025-11-30 00:00:15", #a3
                 "2025-11-30 00:00:16",  #100 a3
                 "2025-11-30 00:00:17",  #100 a1    
                 "2025-11-30 00:00:18",  #101
                 "2025-11-30 00:00:19",  #102 a1
                 "2025-11-30 00:00:20",  #102
                 "2025-11-30 00:00:21",  #103
                 "2025-11-30 00:00:22",  #104 a3
                 "2025-11-30 00:00:23", #should not take into account after this one 
                 "2025-11-30 00:00:24",
                 "2025-11-30 00:00:25"
                ],
        "sender": [ "0x06e5bb573768b325ccd72016136d7092a309c79e",  #attacker that has send over 10$ to victim in prev
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim has send tokens to attacker in prev
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #v0 #Intended transfers start here
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #v0
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #v1
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #v2
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #v3
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #v4 #Intended transfers ends here
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #a4
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #a4 interactor
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #a2 interactor
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #a1 interactor
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #a1
                    "0x06e5bb573768b325ccd72016136d7092a309c79e", #a3 IS
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #a3 interactor  15
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 3  a3  ###BATCH starts here
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 1  a1
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 2  a2
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 1  a1
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 0  a0
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 4  a3
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 0  a4  ###BATCH ends here
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim aaa send legit tokens to a0 (during batch)
                    "0x06e5bb573768b325ccd72016136d7092a309c79e",    #a2 #NOT in batch - should not be picked
                    "0x06e5bb573768b325ccd72016136d7092a309c79e"
                    ], 
        "contract": ["0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                    "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607", 
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607", 
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607", "0x7f5c764cbc14f9669b88837ca1490cca17c31607", #fake contracts
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607","0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607","0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607"
                     ],
        "amount": ["1000000000",
                   "1000000000",
                   "1000000000", 
                   "1000000000", 
                   "1000000000",
                   "1000000000",
                   "1000000000",
                   "1000000000",
                   "1000000000",
                   "1000000000",
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000", 
                   "1000000000"],
        "valid": ["1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1"], #5 different atackers
        

        "receiver": [
                    "0xaaa5bb573768b325ccd72016136d7092a309c79e1",
                    "0xbbb333444768b325ccd72016136d7094443337772",
                    "0x000333123768b325ccd72016136d7091233330003", #v0 #Intended transfers start here
                    "0x444333123768b325ccd72016136d7094400337774", #v0
                    "0x111222fff768b325ccd72016136d709333fff1115", #v1
                    "0x22ffff444768b325ccd72016136d709444333fff6", #v2
                    "0x333fff333768b325ccd72016136d709333fff1117", #v3
                    "0x333abc333768b325ccd72016136d709333ffff118", #v4 #Intended transfers end here
                     "0xa4a4bb573768b325ccd72016136d7092a309c79e9", #a4 interactor
                     "0x444333444768b325ccd72016136d70944433377700", #a4
                     "0x222333444768b325ccd72016136d709444333222000", #a2
                     "0x111222333768b325ccd72016136d7093332221110000", #a1
                     "0xa1a1bb573768b325ccd72016136d7092a309c79e15",  #a1 interactor  13
                     "0xa3a3bb573768b325ccd72016136d7092a309c79e16",  #a3 interactor
                     "0x333222333768b325ccd72016136d70933322211117", #a3 IR #NOT in batch  15
                     "0x333222333768b325ccd72016136d70933322211118", # 1 in 0 out but 1 out in batch DONE  ###BATCH starts here
                     "0x111222333768b325ccd72016136d70933322211119", # 0 in 1 out but 1 in in batch DONE
                     "0x222333444768b325ccd72016136d70944433322220", # 1 in 0 out DONE
                     "0x111222333768b325ccd72016136d70933322211122", # 0 in 1 out but 1 in in batch DONE
                     "0x000333444768b325ccd72016136d70944433300023", # 0 prev interaction DONE
                     "0x333222333768b325ccd72016136d70933322211124", # 1 in 0 out but 1 out in batch DONE
                     "0x444333444768b325ccd72016136d70944433377744", #1 in 1 out in previous DONE   ###BATCH ends here
                     "0x000333444768b325ccd72016136d70944433300055", #NOT in batch
                     "0xa2a2bb573768b325ccd72016136d7092a309c79e66", #a2 interactor #NOT in batch  - should not be picked
                     "0xccc5bb573768b325ccd72016136d7092a309cccc77"
                     ], 
    },
    schema={
        "blockNumber": pl.UInt64,
        "transactionHash": pl.String,
        "id":  pl.UInt32,
        "time": pl.Datetime(time_unit='us', time_zone=None),
        "sender": pl.String,
        "contract": pl.String,
        "amount": pl.String,
        "valid": pl.String,
        "receiver": pl.String,
    })
    return df_test


@pytest.fixture
def batch_dataframe():
    df_test = pl.DataFrame({
        "blockNumber": [100, 100, 101, 102, 102, 103, 104, 104, 104],
        "transactionHash": ["victim3_attack_df", 
                            "victim1_attack_df",
                            "victim2_attack_df",
                            "victim1and0_attack_df_same_txHash", #same txhash
                            "victim1and0_attack_df_same_txHash", #same txhash
                            "victim4_attack_df",
                            "victim0_attack_df_v2",
                            "attacker_knows_victim_attack_in_batch",
                            "victim_knows_attacker_attack_in_batch"],
        "id": [0,0,0,0,2,0,0,0,0],
        "time": ["2025-12-01 00:00:01", 
                 "2025-12-01 00:00:01", 
                 "2025-12-01 00:00:03", 
                 "2025-12-01 00:00:05", 
                 "2025-12-01 00:00:05", 
                 "2025-12-01 00:00:07", 
                 "2025-12-01 00:00:09",
                 "2025-12-01 00:00:09",
                 "2025-12-01 00:00:09"],
        "sender": ["0x36e5bb573768b325ccd72016136d7092a309c79e",  #victim 3
                   "0x16e5bb573768b325ccd72016136d7092a309c79e",  #victim 1
                   "0x26e5bb573768b325ccd72016136d7092a309c79e",  #victim 2
                   "0x16e5bb573768b325ccd72016136d7092a309c79e",  #victim 1
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 0
                   "0x46e5bb573768b325ccd72016136d7092a309c79e",  #victim 4
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",  #victim 0
                   "0xaaa5bb573768b325ccd72016136d7092a309c79e",  #victim aaa
                   "0xbbb5bb573768b325ccd72016136d7092a309c79e",  #victim bbb
                   ], 
        "contract": ["0x7f5c764cbc14f9669b88837ca1490cca17c31000", "0x7f5c764cbc14f9669b88837ca1490cca17c31000", #fake contracts
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31000","0x7f5c764cbc14f9669b88837ca1490cca17c31000",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31000","0x7f5c764cbc14f9669b88837ca1490cca17c31000",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31000", "0x7f5c764cbc14f9669b88837ca1490cca17c31000",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31000"],
        "amount": ["100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000"],
        "valid": ['1', '1', '1', '1', '1', '1', '1', '1', '1'], #5 different atackers
        "receiver": ["0x333222333768b325ccd72016136d709333222111", # 1 in 0 out but 1 out in batch  v3
                     "0x111222333768b325ccd72016136d709333222111", # 0 in 1 out but 1 in in batch   v1
                     "0x222333444768b325ccd72016136d709444333222", # 1 in 0 out                     v2
                     "0x111222333768b325ccd72016136d709333222111", # 0 in 1 out but 1 in in batch   v1
                     "0x000333444768b325ccd72016136d709444333000", # 0 prev interaction             v0
                     "0x333222333768b325ccd72016136d709333222111", # 1 in 0 out but 1 out in batch  v4
                     "0x444333444768b325ccd72016136d709444333777", #1 in 1 out in previous          v0
                     "0xaaa333444768b325ccd72016136d709444333777", #attacker that has send over 10$ to victim in prev
                     "0xbbb333444768b325ccd72016136d709444333777" #attacker that received tokens from victim in prev
                     ], 
        "contract_name": ["USD Coin","USD Coin","USD Coin","USD Coin","USD Coin","USD Coin","USD Coin","USD Coin","USD Coin"],
        "contract_symbol":["USDC","USDC","USDC","USDC","USDC","USDC","USDC","USDC","USDC"]
    },
    schema={
        "blockNumber": pl.UInt64,
        "transactionHash": pl.String,
        "id":  pl.UInt32,
        "time": pl.Datetime(time_unit='us', time_zone=None),
        "sender": pl.String,
        "contract": pl.String,
        "amount": pl.String,
        "valid": pl.String,
        "receiver": pl.String,
        "contract_name": pl.String,
        "contract_symbol": pl.String
    })

    return df_test


@pytest.fixture
def previous_receivers_dataframe():
    df_test = pl.DataFrame({
        "transactionHash": ["100",
                            "101",
                            "102",
                            "103",
                            "104",
                            "105",
                            "106",
                            "107",
                            "108",
                            "109",
                            "110",
                            "111"],           
        "time": ["2025-10-01 00:00:00", 
                 "2025-10-01 00:00:01", 
                 "2025-10-01 00:00:02", 
                 "2025-10-01 00:00:03", 
                 "2025-10-01 00:00:04", 
                 "2025-10-01 00:00:05", 
                 "2025-10-01 00:00:06",
                 "2025-10-01 00:00:07",
                 "2025-10-01 00:00:08",
                 "2025-10-01 00:00:09",
                 "2025-10-01 00:00:10",
                 "2025-10-01 00:00:11"],
        "sender": ["0x06e5bb573768b325ccd72016136d7092a309c79e",  
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",  
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",  
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",  
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",  
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",  
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",  
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",  
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",
                   "0x06e5bb573768b325ccd72016136d7092a309c79e",
                   ], 
        "contract": ["0x7f5c764cbc14f9669b88837ca1490cca17c31607", "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607","0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607","0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607", "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607","0x7f5c764cbc14f9669b88837ca1490cca17c31607",
                     "0x7f5c764cbc14f9669b88837ca1490cca17c31607","0x7f5c764cbc14f9669b88837ca1490cca17c31607"],
        "amount": ["100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000", "100000000"],
        "receiver": ["0x000", 
                     "0x001",
                     "0x002",
                     "0x003",
                     "0x004",
                     "0x005",
                     "0x006",
                     "0x007",
                     "0x008",
                     "0x009",
                     "0x010",
                     "0x011"
                     ]
    },
    schema={
        "transactionHash": pl.String,
        "time": pl.Datetime(time_unit='us', time_zone=None),
        "sender": pl.String,
        "contract": pl.String,
        "amount": pl.String,
        "receiver": pl.String
    })

    return df_test

@pytest.fixture
def empty_dataframe():
    df_empty = pl.DataFrame()
    return df_empty

@pytest.fixture
def empty_lazyframe():
    lf_empty = pl.LazyFrame(schema={
        "blockNumber": pl.UInt64,
        "transactionHash": pl.String,
        "id":  pl.UInt32,
        "time": pl.Datetime(time_unit='us', time_zone=None),
        "sender": pl.String,
        "contract": pl.String,
        "amount": pl.String,
        "valid": pl.String,
        "receiver": pl.String,
        "decimals": pl.Utf8,
        "price": pl.Float64
    })
    return lf_empty
    
@pytest.fixture
def decimals_map():
    ERC20_decimals_map = {}
    ERC20_decimals_map["0x7f5c764cbc14f9669b88837ca1490cca17c31607".lower()] = "6"
    return ERC20_decimals_map
    
@pytest.fixture
def price_map():
    ERC20_price_map = {}
    ERC20_price_map["0x7f5c764cbc14f9669b88837ca1490cca17c31607".lower()] = "1"
    return ERC20_price_map

@pytest.fixture
def rpc_map():
    return {
        "arbitrum": "https://arb1.arbitrum.io/rpc",
        "optimism": "https://mainnet.optimism.io",
        "gnosis": "https://rpc.gnosischain.com/",
        "avalanche": "https://api.avax.network/ext/bc/C/rpc",
        "ethereum": "https://ethereum-rpc.publicnode.com",
        "polygonzk": "https://zkevm-rpc.com",
        "opbnb": "https://opbnb-mainnet-rpc.bnbchain.org"
    }

@pytest.fixture
def tokens_map():
    return {
        "arbitrum": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
        "optimism": "0x7f5c764cbc14f9669b88837ca1490cca17c31607", 
        "gnosis": "0x9c58bacc331c9aa871afd802db6379a98e80cedb",
        "avalanche": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
        "ethereum": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "polygonzk": "0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035",
        "opbnb": "0x9e5aac1ba1a2e6aed6b32689dfcf62a509ca96f3"
    }