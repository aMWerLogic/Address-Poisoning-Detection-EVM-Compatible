import polars as pl

def find_previous_interactions_victim_attacker(attack_df, path_data,current_time_max,ERC20_decimals_map,ERC20_price_map,scan_parquet_fn=pl.scan_parquet,ram_load="high"):
    logs_path = path_data["parquet_data"]
    address_lc = (
        pl.concat([attack_df["receiver"], attack_df["sender"]])
        .str.to_lowercase()
        .unique()
    )
    address_list = address_lc.to_list()
    logs = (
        scan_parquet_fn(logs_path)
        .with_columns([
            pl.col("sender").str.to_lowercase(),
            pl.col("receiver").str.to_lowercase(),
        ])
    )

    logs =  logs.filter(
        (pl.col("sender").is_in(address_list))
        & (pl.col("receiver").is_in(address_list))
    )
    
    logs = logs.filter(
        (pl.col("time") <= current_time_max) &
        pl.col("contract").is_in(ERC20_decimals_map)
    )

    logs = logs.with_columns([
        pl.col("contract")
            .replace_strict(ERC20_decimals_map, return_dtype=pl.Utf8)
            .alias("decimals"),
        pl.col("contract")
            .replace_strict(ERC20_price_map, return_dtype=pl.Float64)
            .alias("price")
    ])

    logs = logs.filter( 
        (((pl.col("amount").cast(pl.Float64) / (10 ** pl.col("decimals").cast(pl.Float64))) * pl.col("price")) >= 0.01)
    )
    if ram_load=="high":
        return logs.collect(engine="streaming") #DF
    else:
        return logs #Lazy 

#It gets number_of_inter (default 100) unique interactors within batch time period + 10 unique interactors before batch (those two periods can have same interactors)
#maximal memory complexity = len(attack_df[victim_col].unique()) * (10+number_of_inter)
#this method is less accurate than find_previous_victim_interactors, however it should not make a difference for relatively small batches, such as 1 to 7 days
#moreover for high number_of_inter (e.g. 100) it is accurate for "normal" accounts which create/receive reasonable amount of transfers (based on ethereum statistics)
def find_previous_victim_interactors_ram_lower(attack_df, path_data,current_time_min,current_time_max,ERC20_decimals_map,ERC20_price_map,reversed=False,scan_parquet_fn=pl.scan_parquet,number_of_inter=10):
    logs_path = path_data["parquet_data"]
    if reversed == True:
        victim_col, attacker_col = "receiver", "sender"
    else:
        victim_col, attacker_col = "sender", "receiver"

    victims = attack_df[victim_col].unique().to_list()

    logs = (
        scan_parquet_fn(logs_path)
        .with_columns([
            pl.col("sender").str.to_lowercase(),
            pl.col("receiver").str.to_lowercase(),
        ])
        .filter(
            pl.col(victim_col).is_in(victims)
        )
    )

    logs = logs.filter(
        (pl.col("time") <= current_time_max) &
        pl.col("contract").is_in(ERC20_decimals_map)
    )

    logs = logs.with_columns([
        pl.col("contract")
            .replace_strict(ERC20_decimals_map, return_dtype=pl.Utf8)
            .alias("decimals"),
        pl.col("contract")
            .replace_strict(ERC20_price_map, return_dtype=pl.Float64)
            .alias("price")
    ])

    logs10 = logs.filter(
        (pl.col("time") < current_time_min)
    )

    logs_batch = logs.filter(
        (pl.col("time") >= current_time_min)
    )

    
    logs10 = logs10.filter( 
        (((pl.col("amount").cast(pl.Float64) / (10 ** pl.col("decimals").cast(pl.Float64))) * pl.col("price")) >= 0.01)
    ).sort(["time"], descending=True).unique(subset=[victim_col, attacker_col], keep="first")

    logs_batch = logs_batch.filter( 
        (((pl.col("amount").cast(pl.Float64) / (10 ** pl.col("decimals").cast(pl.Float64))) * pl.col("price")) >= 0.01)
    ).sort(["time"], descending=True).unique(subset=[victim_col, attacker_col], keep="first")

    result_batch = (
        logs_batch
        .sort("time", descending=True) 
        .group_by(victim_col, maintain_order=True)
        .head(number_of_inter)
    )
    
    result_10 = (
        logs10
        .sort("time", descending=True)  
        .group_by(victim_col, maintain_order=True)
        .head(10)
    )
    
    result = pl.concat([result_batch, result_10]).collect(engine="streaming")
    return result


#returns all previous interactions (intended transfers) V->IS or IS->V (if reversed) for all victims from attack_df
#100% accurate but can take significant amount of memory for utility contracts (if proccess is being killed use find_previous_victim_interactors_ram_lower or decrease batch time period)
def find_previous_victim_interactors(attack_df, path_data,current_time_max,ERC20_decimals_map,ERC20_price_map,reversed=False,scan_parquet_fn=pl.scan_parquet):
    logs_path = path_data["parquet_data"]
    if reversed == True:
        victim_col, attacker_col = "receiver", "sender"
    else:
        victim_col, attacker_col = "sender", "receiver"

    victims = attack_df[victim_col].unique().to_list()

    logs = (
        scan_parquet_fn(logs_path)
        .with_columns([
            pl.col("sender").str.to_lowercase(),
            pl.col("receiver").str.to_lowercase(),
        ])
        .filter(
            pl.col(victim_col).is_in(victims)
        )
    )
    logs = logs.filter(
        (pl.col("time") <= current_time_max) &
        pl.col("contract").is_in(ERC20_decimals_map)
    )

    logs = logs.with_columns([
        pl.col("contract")
            .replace_strict(ERC20_decimals_map, return_dtype=pl.Utf8)
            .alias("decimals"),
        pl.col("contract")
            .replace_strict(ERC20_price_map, return_dtype=pl.Float64)
            .alias("price")
    ])

    logs = logs.filter( 
        (((pl.col("amount").cast(pl.Float64) / (10 ** pl.col("decimals").cast(pl.Float64))) * pl.col("price")) >= 0.01)
    ).sort(["time"], descending=True).unique(subset=[victim_col, attacker_col], keep="first")

    result = (
        logs
        .group_by(victim_col)
        .agg([
            pl.col(attacker_col),
            pl.col("time"),
            pl.col("transactionHash"),
        ])).collect(engine="streaming")
    return result