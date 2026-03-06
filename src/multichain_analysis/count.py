import csv
import polars as pl
from pathlib import Path
import matplotlib.pyplot as plt

reverse_arr = [False,True]
chains = ["opbnb", "polygonzk", "arbitrum", "optimism", "avalanche", "gnosis"]
attack_types = ["zero", "fake"]
BASE_DIR = Path(__file__).resolve().parent  
PROJECT_ROOT = BASE_DIR.parent

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

#column_name = "similarity_match" or "score"
def analyse_count_column(output_path: str, column_name: str = "similarity_match"): 
    frequency = {}
    for chain in chains:
        for attack_type in attack_types:
            #for reversed in reverse_arr:
            path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_results_filtered_False.csv"
            if not path.exists():
                print(f"File not found: {path}")
                continue
            df = pl.read_csv(path, infer_schema_length=False)
            df = df.filter(
                (pl.col("possible_utility_victim") == "False") &
                (pl.col("possible_utility_attacker") == "False") 
            )
            result = (
                df
                .group_by(column_name)
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
            )
            #print(result)
            for name, count in result.iter_rows():
                frequency[int(float(name))] = frequency.get(int(float(name)), 0) + int(float(count))
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([column_name, "count"])
        for name, count in frequency.items():
            writer.writerow([name, count])
            #print(name, count)
            #f.write(f"{name}\t{count}\n")

def analyse_count_column_fast(output_path: str, column_name: str):
    pl.Config.set_fmt_str_lengths(100)
    pl.Config.set_tbl_rows(20)
    dfs = []
    bins = [0, 0, 0, 0, 0, 0]
    for chain in chains:
        for attack_type in attack_types:
            for rev in reverse_arr:
                path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_results_filtered_{rev}.csv"
                if not path.exists():
                    continue
                df = ( 
                    pl.read_csv(path, infer_schema_length=False)
                    .filter(
                        (pl.col("possible_utility_victim") == "False") &
                        (pl.col("possible_utility_attacker") == "False")
                    )
                )
                dfs.append(df)
    df = pl.concat(dfs, how="vertical")
    df = df.unique(subset=[column_name], keep="first").select(pl.col(f"unique_accounts_{column_name}").cast(pl.Int64))
    first = df.filter(pl.col(f"unique_accounts_{column_name}")<=100)
    second = df.filter((pl.col(f"unique_accounts_{column_name}")>100) & (pl.col(f"unique_accounts_{column_name}")<=200))
    third = df.filter((pl.col(f"unique_accounts_{column_name}")>200) & (pl.col(f"unique_accounts_{column_name}")<=500))
    fourth = df.filter((pl.col(f"unique_accounts_{column_name}")>500) & (pl.col(f"unique_accounts_{column_name}")<=1000))
    fifth = df.filter((pl.col(f"unique_accounts_{column_name}")>1000) & (pl.col(f"unique_accounts_{column_name}")<=10000))
    sixth = df.filter(pl.col(f"unique_accounts_{column_name}")>10000)
    bins[0] = bins[0] + first.height
    bins[1] = bins[1] + second.height   
    bins[2] = bins[2] + third.height
    bins[3] = bins[3] + fourth.height
    bins[4] = bins[4] + fifth.height
    bins[5] = bins[5] + sixth.height
    print(f"0-100: {bins[0]}\n")
    print(f"100-200: {bins[1]}\n")
    print(f"200-500: {bins[2]}\n")
    print(f"500-1000: {bins[3]}\n")
    print(f"1000-10000: {bins[4]}\n")
    print(f"10000+: {bins[5]}\n")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"0-100: {bins[0]}\n")
        f.write(f"100-200: {bins[1]}\n")
        f.write(f"200-500: {bins[2]}\n")
        f.write(f"500-1000: {bins[3]}\n")
        f.write(f"1000-10000: {bins[4]}\n")
        f.write(f"10000+: {bins[5]}\n")


def obtain_distribution(column_name, with_utility: bool = False):
    dfs = []
    bins = [0, 0, 0, 0, 0, 0]
    for chain in chains:
        for attack_type in attack_types:
            for rev in reverse_arr:
                path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_results_filtered_{rev}.csv"
                if not path.exists():
                    continue
                if with_utility == False:
                    df = ( 
                        pl.read_csv(path, infer_schema_length=False)
                        .filter(
                            (pl.col("possible_utility_victim") == "False") &
                            (pl.col("possible_utility_attacker") == "False")
                        )
                    ).with_columns(pl.col(f"unique_accounts_{column_name}").cast(pl.Int64)).filter(pl.col(f"unique_accounts_{column_name}")<=1000)
                else:
                    df = ( 
                        pl.read_csv(path, infer_schema_length=False)
                        .with_columns(pl.col(f"unique_accounts_{column_name}").cast(pl.Int64)).filter(pl.col(f"unique_accounts_{column_name}")<=1000)
                    )
                dfs.append(df)
    if not dfs:
        print("No data found.")
        return
    combined = pl.concat(dfs, how="vertical")
    combined = combined.unique(subset=[column_name], keep="first").select(pl.col(f"unique_accounts_{column_name}").cast(pl.Int64))
    distribution = (
        combined
        .group_by(f"unique_accounts_{column_name}")
        .agg(pl.len().alias("count"))
        .sort(f"unique_accounts_{column_name}")
    )
    return distribution

def analyse_combined_plot(column_name="victim"):
    distribution = obtain_distribution(column_name, False)
    distribution_utility = obtain_distribution(column_name, True)
    x_red = distribution_utility[f"unique_accounts_{column_name}"].to_list()
    y_red = distribution_utility["count"].to_list()

    x_top = distribution[f"unique_accounts_{column_name}"].to_list()
    y_top = distribution["count"].to_list()
    plt.figure(figsize=(10, 6))
    plt.bar(x_red, y_red, color='red', label='Utility Victims', 
            alpha=0.6, width=1.0, zorder=1)
    plt.bar(x_top, y_top, color='blue', label='Non Utility Victims', 
            alpha=0.6, width=1.0, zorder=2)
    plt.yscale("log")
    all_x = x_red + x_top
    if len(all_x) > 0:
        min_x = min(all_x)
        max_x = max(all_x)
        plt.xticks(range(min_x, max_x + 5, 50))

    plt.xlabel("Unique counterparties")
    plt.ylabel("Count (log scale)")
    plt.legend()
    
    plt.tight_layout()
    plt.show()

def count_attackers_and_victims():
    attackers_df  = pl.DataFrame(schema={"attacker": pl.Utf8})
    victims_df  = pl.DataFrame(schema={"victim": pl.Utf8})
    for chain in chains:
        for attack_type in attack_types:
            for reversed in reverse_arr:
                path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_results_filtered_{reversed}.csv"
                if not path.exists():
                    print(f"File not found: {path}")
                    continue
                df = pl.read_csv(path, infer_schema_length=False)
                df = df.filter(
                    (pl.col("possible_utility_victim") == "False") &
                    (pl.col("possible_utility_attacker") == "False") 
                )
                attackers_df = pl.concat([attackers_df, df.select("attacker")], how="vertical")
                attackers_df = attackers_df.select(pl.col("attacker").unique())
                victims_df = pl.concat([victims_df, df.select("victim")], how="vertical")
                victims_df = victims_df.select(pl.col("victim").unique())

    unique_attackers = attackers_df.select(pl.col("attacker").unique()).height
    unique_victims = victims_df.select(pl.col("victim").unique()).height
    print(f"Number of attackers: {unique_attackers}")
    print(f"Number of victims: {unique_victims}")

def count_attackers_similarity():
    similarity_dfs = []
    for chain in chains:
        print(chain)
        cumulative_path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_payouts_similarity_scores.csv"
        if not cumulative_path.exists():
            print(f"File not found: {cumulative_path}")
            continue
        cumulative_df = (
            pl.read_csv(cumulative_path, infer_schema_length=False)
            .select([
                "transaction_key",
                "cumulative_value_usd",
                "similarity_match"
            ])
            .with_columns(pl.col("cumulative_value_usd").cast(pl.Float64, strict=False))
            #.unique(subset=["transaction_key"], keep="first")
        )
        similarity_dfs.append(cumulative_df)
    if not similarity_dfs:
        print("No payout files with similarity_match data found.")
        return
    combined_similarity = (
        pl.concat(similarity_dfs, how="vertical")
        .drop_nulls(["similarity_match"])
    )
    similarity_distribution = (
        combined_similarity
        .group_by("similarity_match")
        .agg([
            pl.len().alias("count"),
            pl.col("cumulative_value_usd").sum().alias("total_cumulative_value_usd")
        ])
        .sort("similarity_match")
    )
    print("Distribution of similarity_match for transaction_key values in all {chain}_cumulative_payouts_{direction}.csv files:")
    print(similarity_distribution)
    output_path = PROJECT_ROOT / "distribution_similarity_cumulative_payouts.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("similarity_match\tcount\ttotal_cumulative_value_usd\n")
        for row in similarity_distribution.iter_rows(named=True):
            f.write(
                f"{row['similarity_match']}\t{row['count']}\t{row['total_cumulative_value_usd']}\n"
            )
    print(f"Distribution saved to: {output_path}")

    
def analyse_count_number_of_attacks(output_path: str):
    rows = []
    for chain in chains:
        for rev in reverse_arr:
            total_count = 0
            for attack_type in attack_types:
                path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_results_filtered_{rev}.csv"
                if not path.exists():
                    print(f"File not found: {path}")
                    continue
                df = pl.read_csv(path, infer_schema_length=False)
                df = df.filter(
                    (pl.col("possible_utility_victim") == "False") &
                    (pl.col("possible_utility_attacker") == "False")
                )
                total_count += df.height
            rows.append({
                "chain": chain,
                "reversed": rev,
                "count": total_count
            })
    result_df = pl.DataFrame(rows)
    result_df.write_csv(output_path)

def analyse_get_cumulative_payouts(output_path: str):
    rows = []
    for chain in chains:
        for rev in reverse_arr:
            path = (PROJECT_ROOT / f"results_{chain}" / f"{chain}_cumulative_payouts_{rev}.csv")
            if not path.exists():
                print(f"File not found: {path}")
                continue
            df = pl.read_csv(path, infer_schema_length=False)
            df = df.with_columns(
                pl.col("cumulative_value_usd").cast(pl.Float64)
            )
            payout = df.select(
                pl.col("cumulative_value_usd").sum()
            ).item()
            rows.append({
                "chain": chain,
                "reversed": rev,
                "cumulative_payout_usd": payout
            })
    result_df = pl.DataFrame(rows)
    result_df.write_csv(output_path)


def payouts_similarity_scores(rev: str = "False"):
    chains = ["arbitrum", "optimism", "avalanche", "gnosis"]
    for chain in chains:
        print(chain)
        path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_cumulative_payouts_{rev}.csv"
        path_pay = PROJECT_ROOT / f"results_{chain}" / f"{chain}_payouts_{rev}.csv"
        if not path.exists():
            print(f"File not found: {path}")
            continue
        symbols = (
            pl.read_csv(path_pay, infer_schema_length=False).with_columns(
                pl.col("usd_value").cast(pl.Float64)
            )
            .sort("usd_value", descending=True)
            .unique(subset=["transaction_key"], keep="first")
            .select(["transaction_key", "contract_symbol_right"])
        )
        payouts = pl.read_csv(path, infer_schema_length=False).select(["transaction_key", "cumulative_value_usd"])
        merged = payouts.join(symbols, on="transaction_key", how="left")
        #merged.write_csv(PROJECT_ROOT / f"results_{chain}" / f"{chain}_payouts_similarity_scores_{rev}.csv")
        merged = merged.with_columns(pl.col("cumulative_value_usd").cast(pl.Float64))
        merged_filtered = merged
        #merged_filtered = merged.filter(pl.col("cumulative_value_usd")>0.01)
        symbol_counts = (
            merged_filtered
            .group_by("contract_symbol_right")
            .agg(pl.count().alias("count"),
                pl.col("cumulative_value_usd").sum().alias("total_usd_value"))
            .sort("count", descending=True)
        )
        print(symbol_counts)
        

def analyse_get_batched_transfers(output_path: str):
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("chain\tcount_of_batched_transfers\tnumber_of_tx\tmean\tmedian\n")
    for chain in chains:
        batched_count = 0
        number_of_tx = 0
        txhashes = pl.DataFrame(schema={"tx_hash": pl.Utf8})
        for attack_type in attack_types:
            for reversed in reverse_arr:
                path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_results_filtered_{reversed}.csv"
                if not path.exists():
                    print(f"File not found: {path}")
                    continue
                df = pl.read_csv(path, infer_schema_length=False)
                df = df.filter(
                    (pl.col("possible_utility_victim") == "False") &
                    (pl.col("possible_utility_attacker") == "False") 
                )
                df = df.filter(pl.col("batched_bool") == "True").with_columns(
                        pl.col("transaction_key")
                        .map_elements(extract_txhash_from_key)
                        .alias("tx_hash")
                    ).select(["batched_bool","tx_hash"])
                batched_count = batched_count + df.height
                txhashes = pl.concat([txhashes,df.select("tx_hash")],how="vertical")   
        per_tx = (
            txhashes
            .group_by("tx_hash")
            .agg(pl.len().alias("rows_per_tx"))
            .sort("rows_per_tx", descending=True)
        )
        median_batched = per_tx.select(pl.col("rows_per_tx").median()).item()
        number_of_tx = txhashes.unique().height
        mean = batched_count/number_of_tx if number_of_tx>0 else 0
        with open(output_path, "a", encoding="utf-8") as f:
            f.write(f"{chain}\t{batched_count}\t{number_of_tx}\t{mean}\t{median_batched}\n")



def count_removed_accounts():
    removed_accounts=0
    for chain in chains:
        for attack_type in attack_types:
            for reversed in reverse_arr:
                path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_removedVictims_{reversed}.csv"
                if not path.exists():
                    print(f"File not found: {path}")
                    continue
                df = pl.read_csv(path, infer_schema_length=False, has_header=False)
                df = df.select(pl.col("column_2").unique())
                removed_accounts = removed_accounts + df.height 
    for chain in chains:
        for attack_type in attack_types:
            for reversed in reverse_arr:
                path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_removedAttackers_{reversed}.csv"
                if not path.exists():
                    print(f"File not found: {path}")
                    continue
                df = pl.read_csv(path, infer_schema_length=False, has_header=False)
                df = df.select(pl.col("column_2").unique())
                removed_accounts = removed_accounts + df.height
    print(f"Total removed accounts: {removed_accounts}")


def export_first_addresses_by_similarity(
    similarity_score: int,
    address_column: str = "transaction_key",
    max_addresses: int = 20,
):
    for chain in chains:
        for attack_type in attack_types:
            for rev in reverse_arr:
                path = PROJECT_ROOT / f"results_{chain}" / f"{chain}_{attack_type}_results_filtered_{rev}.csv"
                if not path.exists():
                    print(f"File not found: {path}")
                    continue

                df = pl.read_csv(path, infer_schema_length=False)

                if "similarity_match" not in df.columns:
                    print(f"Missing column 'similarity_match' in {path}")
                    continue
                if address_column not in df.columns:
                    print(f"Missing address column '{address_column}' in {path}")
                    continue

                matched = (
                    df
                    .with_columns(pl.col("similarity_match").cast(pl.Int64, strict=False).alias("similarity_match"))
                    .filter(pl.col("similarity_match") == similarity_score)
                    .filter((pl.col("possible_utility_victim") == "False") &
                            (pl.col("possible_utility_attacker") == "False"))
                    .select(pl.col(address_column))
                    .drop_nulls()
                    .unique(maintain_order=True)
                    .head(max_addresses)
                )

                output_path = (
                    f"{chain}_{attack_type}_{address_column}_similarity_{similarity_score}_first_{max_addresses}_{rev}.csv"
                )
                if matched.height == 0:
                    #print(f"No addresses found with similarity_match = {similarity_score} in {path}")
                    continue
                matched.write_csv(output_path)
                print(f"Saved {matched.height} addresses to: {output_path}")