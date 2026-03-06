### PROJECT SETUP
**Python 12^** is recommended. <br>
In terminal: <br>
    `python -m venv .venv` <br>
Or in VS Code:<br>
    `Crtl+Shift+P` -> `Python: Create Environment` <br>
Then activate python environment with (in git bash): <br>
    `source .venv/Scripts/activate` (windows) <br>
    `source .venv/bin/activate` (linux) <br>
To install dependencies: <br>
`pip install -e .[dev]` <br>

Run tests with: <br>
`pytest -v -s` <br>

Example of deploying runner for optimism rollup: <br>
Create your own *.env* file based on *.env-example* and place it in **poisoning_detector** dir. <br>
The directory named **parquet_data_optimism** containing parquet files should be located directly under **src** dir. <br>
You can obtain parquet files using scripts located under **getData** dir; first run *get_rollup_data.py <rollup_name>*, then *extract.py <rollup_name>*. WARNING: Your blockchain dumps can be huge - ensure enough free disk space.<br>
Start the detector from **src** dir with:  <br>
`python -m poisoning_detector.main optimism` <br>
`python -m poisoning_detector.main arbitrum ram_load=lower` <br>
Running multichain analysis (after poisoning detector was run on all networks): <br>
`python -m multichain_analysis.main`

### WHAT *REVERSED* MEANS IN THE CONTEXT OF POISONING DETECTOR
For each transfer in steps_runner class, we perform analysis regarding both directions. This means that, at first, we treat the receiver of a suspicious transfer as a lookalike address and perform every step of our detection system. In the second iteration, we treat the sender as a lookalike address. This distinction is important because attackers can also target the receiver of intended transfer if the behavioural characteristics of the victim's account suggest that such an attack is more likely to succeed.
#### Visually the difference can be shown as:
IR - intended receiver, IS - intended sender, V - victim, L - lookalike (lookalike L ~ IS | L ~ IR) <br>
normal direction - reversed = **False**: <br>
1. V -> IR  <br>
2. V -> L <br>

reversed = **True**: <br>

1. IS -> V <br>
2. L -> V <br>

so essentially here we have attacker always as a sender and victim always as a receiver.
We check if such scenario gives better results 

### MODULES OVERVIEW

#### `src/poisoning_detector/`
- Core detection pipeline for address-poisoning analysis.
- `main.py`: CLI entrypoint for running detection per chain.
- `steps_runner.py`: orchestrates end-to-end detection steps.
- `categorize.py`, `analyse_results.py`: post-processing and labeling of detected events.
- `system_contracts.py`: contract filtering and contract classification helpers.
- `get_tokens.py`, `get_token_info.py`: token metadata enrichment.
- `find_previous_transfers.py`: historical transfer lookups used by the detector.

#### `src/multichain_analysys/`
- Utilities for cross-chain result analysis and summary metrics.
- Includes scripts such as 
- `main.py`: orchestrates methods.
- `count.py`: many statistics inlcuding counting payouts, transfers, obtaining distributions, drawing plots.
- `delta.py`: time analysis between poisoning attack and genuine transfer. 
- `repeats.py`: statistics about reaused attackers on multiple blockchains.
- `tokens.py`: statistics about fake tokens adn the frequency of all tokens.
- `eoa_vs_sc.py`: if one have all accounts marked whether they are SC or EOA then he can run methods in this file.

#### `tests/`
- Pytest-based checks for core project behavior (`test_class.py`, shared fixtures in `conftest.py`).


#### `getData/`
It is not really a module but rather some loose scripts <br>
- `get_rollup_data.py`: downloads raw rollup transaction data.
- `extract.py`: extracts relevant transfer records from raw dumps.
- `to_parquet.py`: converts extracted datasets into parquet format for analysis.

### The State of the Art of Address Poisoning Attacks in EVM-Compatible Blockchains
The repository was used to obtain the results in the article: <br> 
`The State of the Art of Address Poisoning Attacks in EVM-Compatible Blockchains` <br>
Also steps were profoundly explained in a theoretical way in this article. <br>
File `Poisoning-Results-Final.rar` contains results discussed in the article.