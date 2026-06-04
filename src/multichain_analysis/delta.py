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


def analyse_delta_per_chain(output_path: str):
    bins = [
        (0,        120,     "0-2 min"),
        (120,      300,    "2-5 min"),
        (300,      600,    "5-10 min"),
        (600,      1200,   "10-20 min"),
        (1200,     1800,   "20-30 min"),
        (1800,     3600,   "30-60 min"),
        (3600,     7200,   "1h-2h"),
        (7200,     43200,  "2h-12h"),
        (43200,    86400,  "12h-1d"),
        (86400,    None,   "1d+"),
    ]

    lines = []
    print(reverse_arr)
    for chain in chains:
        counts = [0] * len(bins)
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
                for i, (lo, hi, _) in enumerate(bins):
                    if hi is None:
                        counts[i] += df.filter(pl.col("delta") > lo).height
                    else:
                        counts[i] += df.filter((pl.col("delta") > lo) & (pl.col("delta") <= hi)).height

        lines.append(chain.upper())
        for i, (_, _, label) in enumerate(bins):
            lines.append(f"  {label}: {counts[i]}")

    output = "\n".join(lines) + "\n"
    print(output)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(output)


def analyse_delta_successful_per_chain(output_path: str):
    bins = [
        (0,        120,     "0-2 min"),
        (120,      300,    "2-5 min"),
        (300,      600,    "5-10 min"),
        (600,      1200,   "10-20 min"),
        (1200,     1800,   "20-30 min"),
        (1800,     3600,   "30-60 min"),
        (3600,     7200,   "1h-2h"),
        (7200,     43200,  "2h-12h"),
        (43200,    86400,  "12h-1d"),
        (86400,    None,   "1d+"),
    ]

    print(reverse_arr)
    lines = []
    for chain in chains:
        counts = [0] * len(bins)
        for reversed in reverse_arr:
            cumulative_path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_cumulative_payouts_{reversed}.csv"
            payouts_path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_payouts_{reversed}.csv"
            if not cumulative_path.exists() or not payouts_path.exists():
                print(f"Missing files for {chain} (reversed={reversed}), skipping...")
                continue

            successful_keys = pl.read_csv(cumulative_path, infer_schema_length=False).select("transaction_key")
            payouts = pl.read_csv(payouts_path, infer_schema_length=False).select(["transaction_key", "delta"])

            df = successful_keys.join(payouts, on="transaction_key", how="inner").with_columns(
                pl.col("delta").cast(pl.Float64)
            )

            for i, (lo, hi, _) in enumerate(bins):
                if hi is None:
                    counts[i] += df.filter(pl.col("delta") > lo).height
                else:
                    counts[i] += df.filter((pl.col("delta") > lo) & (pl.col("delta") <= hi)).height

        lines.append(chain.upper())
        for i, (_, _, label) in enumerate(bins):
            lines.append(f"  {label}: {counts[i]}")

    output = "\n".join(lines) + "\n"
    print(output)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(output)