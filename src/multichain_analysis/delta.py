import polars as pl
from pathlib import Path

attack_types = ["zero", "fake"]
reverse_arr = [False,True]
chains = ["ethereum", "opbnb", "polygonzk", "arbitrum", "optimism", "avalanche", "gnosis"]
time_periods = [0, 0, 0, 0, 0]

BASE_DIR = Path(__file__).resolve().parent  
PROJECT_ROOT = BASE_DIR.parent

#0-2 min; 2-20 min; 20min-2h; 2h-1d; 1d+
def analyse_delta(output_path: str):
    for chain in chains:
        for attack_type in attack_types:
            for reversed in reverse_arr:
                path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_results_filtered_{reversed}.csv"
                if not path.exists():
                    continue
                df = pl.read_csv(path, infer_schema_length=False)

                df = df.with_columns(
                    pl.col("delta").cast(pl.Float64)
                ).filter(
                    (pl.col("possible_utility_victim") == "False") &
                    (pl.col("possible_utility_attacker") == "False")
                )
                first = df.filter(pl.col("delta")<=120)
                second = df.filter((pl.col("delta")>120) & (pl.col("delta")<=1200))
                third = df.filter((pl.col("delta")>1200) & (pl.col("delta")<=7200))
                fourth = df.filter((pl.col("delta")>7200) & (pl.col("delta")<=86400))
                fifth = df.filter(pl.col("delta")>86400)
                time_periods[0] = time_periods[0] + first.height
                time_periods[1] = time_periods[1] + second.height   
                time_periods[2] = time_periods[2] + third.height
                time_periods[3] = time_periods[3] + fourth.height
                time_periods[4] = time_periods[4] + fifth.height
    print("0-2 min:", time_periods[0])
    print("2-20 min:", time_periods[1])
    print("20min-2h:", time_periods[2])
    print("2h-1d:", time_periods[3])
    print("1d+:", time_periods[4])
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"0-2 min: {time_periods[0]}\n")
        f.write(f"2-20 min: {time_periods[1]}\n")
        f.write(f"20 min-2 h: {time_periods[2]}\n")
        f.write(f"2 h-1 d: {time_periods[3]}\n")
        f.write(f"1 d+: {time_periods[4]}\n")