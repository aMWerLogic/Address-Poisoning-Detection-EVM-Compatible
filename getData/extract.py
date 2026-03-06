import os
import zstandard as zstd
import sys
from pathlib import Path
from to_parquet import to_parquet

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

if sys.argv[1] not in ["arbitrum", "optimism", "avalanche", "ethereum", "gnosis", "polygonzk", "base", "opbnb", "bnb"]:
    print("wrong blockchain argument")
    exit(1)

parquet_dir = Path(__file__).parent / f"../parquet_data_{sys.argv[1]}"

def extract_all_zst(source_dir, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    parquet_dir.mkdir(exist_ok=True)

    for filename in os.listdir(source_dir):
        if filename.endswith(".tsv.zst"):
            zst_path = os.path.join(source_dir, filename)
            tsv_filename = filename.replace(".tsv.zst", ".tsv")
            tsv_path = os.path.join(target_dir, tsv_filename)

            with open(zst_path, "rb") as compressed:
                dctx = zstd.ZstdDecompressor()
                with open(tsv_path, "wb") as decompressed:
                    dctx.copy_stream(compressed, decompressed)

            print(f"Extracted: {tsv_path}")
            tsv_path=Path(tsv_path)
            to_parquet(tsv_path,sys.argv[1])
            tsv_path.unlink()


folder_name = f"{sys.argv[1]}_erc20_dumps"
source_folder = os.path.join("..", folder_name)

folder_name2 = f"data_{sys.argv[1]}"
target_folder = os.path.join("..", folder_name2)

extract_all_zst(source_folder, target_folder)
