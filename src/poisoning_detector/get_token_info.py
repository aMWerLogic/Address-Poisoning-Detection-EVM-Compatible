import sys
from tkinter import E
from get_tokens import GetTokens 
from dotenv import load_dotenv
import os

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

load_dotenv()
alchemy_token = os.getenv("alchemyToken")

if sys.argv[1]=="arbitrum":
    rpc_url=f"https://arb-mainnet.g.alchemy.com/v2/{alchemy_token}"
elif sys.argv[1]=="optimism":
    rpc_url=f"https://opt-mainnet.g.alchemy.com/v2/{alchemy_token}"
elif sys.argv[1]=="avalanche":
    rpc_url = f"https://avax-mainnet.g.alchemy.com/v2/{alchemy_token}"
elif sys.argv[1]=="ethereum":
    rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_token}"
elif sys.argv[1]=="gnosis":
    rpc_url = f"https://gnosis-mainnet.g.alchemy.com/v2/{alchemy_token}"
elif sys.argv[1]=="polygonzk":
    rpc_url = f"https://polygonzkevm-mainnet.g.alchemy.com/v2/{alchemy_token}"  
elif sys.argv[1]=="opbnb":
    rpc_url = f"https://opbnb-mainnet.g.alchemy.com/v2/{alchemy_token}"   
else:
    print("wrong blockchain argument")
    exit(1)


if __name__ == "__main__":
    input_file = f"top_{sys.argv[1]}_erc20.txt"

    with open(input_file, "r") as f:
        ERC20_addresses = {line.strip().lower() for line in f if line.strip()}

    filename = f"{sys.argv[1]}_token_info.txt"
    tokenInfo = GetTokens(rpc_url,rpc_url)
    with open(filename, "a", encoding="utf-8") as out_f:
        out_f.write("address,name,symbol,decimals\n")
        for addr in ERC20_addresses:
            #time.sleep(1)
            name, symbol, decimals = tokenInfo.safe_get_token_info(addr=addr, wait_time=3, info=1)
            out_f.write(f"{addr},{name},{symbol},{decimals}\n")
            print(f"Address: {addr}, Name: {name}, Symbol: {symbol}, Decimals: {decimals}")