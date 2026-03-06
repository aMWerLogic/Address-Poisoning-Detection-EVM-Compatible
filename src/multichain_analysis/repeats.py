import os
import polars as pl
from pathlib import Path
from poisoning_detector.steps_runner import StepsRunner
from collections import Counter, defaultdict

reverse_arr = [False,True]
chains = ["opbnb", "polygonzk", "arbitrum", "optimism", "avalanche", "gnosis"] #TODO: check if those are compatible between each other
attack_types = ["zero", "fake"]
BASE_DIR = Path(__file__).resolve().parent  
PROJECT_ROOT = BASE_DIR.parent

def analyse_reused_attackers():
    reused_attackers = {}
    attackers_counter = Counter()
    attacker_chains = defaultdict(set)
    for chain in chains:
        dfs=[]
        for reversed in reverse_arr:
            path1 = PROJECT_ROOT / f"results_{chain}" / f"{chain}_fake_results_filtered_{reversed}.csv"
            path2 = PROJECT_ROOT / f"results_{chain}" / f"{chain}_zero_results_filtered_{reversed}.csv"
            if os.path.exists(path1):
                df = pl.read_csv(path1, infer_schema_length=0)
                df = df.with_columns(pl.all().cast(pl.Utf8))
                df = df.filter(
                    (pl.col("possible_utility_victim") == "False") &
                    (pl.col("possible_utility_attacker") == "False") 
                )
                dfs.append(df)
            else:
                print(f"no {path1} file detected")
            if os.path.exists(path2):
                df2 = pl.read_csv(path2, infer_schema_length=0)
                df2 = df2.with_columns(pl.all().cast(pl.Utf8))
                df2 = df2.filter(
                    (pl.col("possible_utility_victim") == "False") &
                    (pl.col("possible_utility_attacker") == "False") 
                )
                dfs.append(df2)
            else:
                print(f"no {path2} file detected")
        if not dfs:
            continue
        combined = pl.concat(dfs, how="vertical").select("attacker").unique()   
        attackers_list  = combined["attacker"].to_list()
        attackers_counter.update(attackers_list)
        for attacker in attackers_list:
            attacker_chains[attacker].add(chain)
    reused_attackers = dict(attackers_counter)

    rows = []
    for attacker, count in attackers_counter.items():
        if count >= 2:  # only reused attackers
            rows.append({
                "attacker": attacker,
                "count": count,
                "chains": ",".join(sorted(attacker_chains[attacker]))
            })

    df_reused = pl.DataFrame(rows).sort("count", descending=True)
    df_reused = df_reused.filter(pl.col("count") >= 2)
    two_df = df_reused.filter(pl.col("count") == 2).height
    three_df = df_reused.filter(pl.col("count") == 3).height
    print(f"Number of reused addresses at least 2 times: {two_df}")
    print(f"Number of reused addresses at least 3 times: {three_df}")
    #df_reused.write_csv("reused_attackers.csv")

    """
    df_reused = pl.DataFrame({
        "attacker": list(reused_attackers.keys()),
        "count": list(reused_attackers.values())
    })
    df_reused = df_reused.filter(pl.col("count") >= 2)
    two_df = df_reused.filter(pl.col("count") == 2).height
    three_df = df_reused.filter(pl.col("count") == 3).height
    print(f"Number of reused addresses at least 2 times: {two_df}")
    print(f"Number of reused addresses at least 3 times: {three_df}")
    df_reused.write_csv(f"reused_attackers.csv")
    """