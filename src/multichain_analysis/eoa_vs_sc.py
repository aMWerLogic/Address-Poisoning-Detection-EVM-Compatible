import polars as pl
from pathlib import Path

attack_types = ["zero", "fake"]
reverse_arr = [False,True]
chains = ["ethereum", "opbnb", "polygonzk", "arbitrum", "optimism", "avalanche", "gnosis"]

def analyse_eoa_count(output_path: str):
    eoas_victims = 0
    eoas_attackers = 0
    sc_victims = 0
    sc_attackers = 0
    for chain in chains:
        for attack_type in attack_types:
            for reversed in reverse_arr:
                path = Path(f"../src/results/{chain}_{attack_type}_results_filtered_{reversed}.csv")
                if not path.exists():
                    continue
                df = pl.read_csv(path)
                df = df.filter(
                    (pl.col("possible_utility_victim") == "False") &
                    (pl.col("possible_utility_attacker") == "False") 
                )
                eoas_victims += df.filter(pl.col("contract_victim_bool")==False).height
                eoas_attackers += df.filter(pl.col("contract_attacker_bool")==False).height
                sc_victims += df.filter(pl.col("contract_victim_bool")==True).height
                sc_attackers += df.filter(pl.col("contract_attacker_bool")==True).height

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"EOAS victims: {eoas_victims}\n")
        f.write(f"EOAS attackers: {eoas_attackers}\n")
        f.write(f"SC victims: {sc_victims}\n")
        f.write(f"SC attackers: {sc_attackers}\n")
