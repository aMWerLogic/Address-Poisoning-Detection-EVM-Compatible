from multichain_analysis.tokens import analyse_fake_tokens_count, analyse_frequency_of_tokens
from multichain_analysis.delta import analyse_delta, analyse_delta_per_chain, analyse_delta_successful_per_chain
from multichain_analysis.count import analyse_combined_plot, analyse_count_column, analyse_count_column_fast, analyse_count_number_of_attacks, analyse_get_batched_transfers, analyse_get_cumulative_payouts, analyse_separate_utility_plots, count_attackers_and_victims, count_attackers_similarity, count_removed_accounts, export_first_addresses_by_similarity, payouts_similarity_scores
from multichain_analysis.repeats import analyse_reused_attackers
from multichain_analysis.prefix_vs_suffix import analyse_prefix_vs_suffix_lengths
import polars as pl
if __name__ == "__main__":
    pl.Config.set_fmt_str_lengths(100)
    pl.Config.set_tbl_rows(20)
    print("Starting analysis...")
    analyse_fake_tokens_count()
    analyse_frequency_of_tokens("fake_token_frequencies.txt", attack_type="fake")
    analyse_frequency_of_tokens("zero_token_frequencies.txt", attack_type="zero")
    analyse_delta("delta_analysis.txt")
    analyse_delta_per_chain("delta_analysis_per_chain.txt")
    analyse_delta_successful_per_chain("delta_analysis_successful_per_chain.txt")
    analyse_count_column("similarity_match_count.csv", column_name="similarity_match")
    analyse_count_column("score_count.csv", column_name="score")
    analyse_count_number_of_attacks("number_of_attacks_per_chain.csv")
    analyse_get_cumulative_payouts("cumulative_payouts_per_chain_and_attack_type.csv")
    analyse_reused_attackers()
    count_attackers_and_victims()
    analyse_get_batched_transfers("batched_transfers.txt")
    analyse_count_column_fast("distribution_unique_accounts_victims.txt", "victim")
    analyse_count_column_fast("distribution_unique_accounts_attackers.txt", "attacker")
    payouts_similarity_scores()
    count_removed_accounts()
    analyse_combined_plot() 
    count_attackers_similarity()
    export_first_addresses_by_similarity(similarity_score=16)
    analyse_separate_utility_plots()
    analyse_prefix_vs_suffix_lengths("prefix_length_distribution.csv", "suffix_length_distribution.csv")
