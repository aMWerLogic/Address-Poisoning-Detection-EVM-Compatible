import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import sys
import re
from dotenv import load_dotenv

#TODO: add individual begin date for each rollup (based on launch date)
if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

if sys.argv[1]=="arbitrum":
    url = "https://3xpl.com/data/dumps/arbitrum-one-erc-20"
    start_date = datetime(2021, 8, 9)  
elif sys.argv[1]=="optimism":
    url = "https://3xpl.com/data/dumps/optimism-erc-20"
    start_date = datetime(2021, 11, 12) 
elif sys.argv[1]=="avalanche":
    url = "https://3xpl.com/data/dumps/avalanche-erc-20"
    start_date = datetime(2021, 1, 25)
elif sys.argv[1]=="ethereum": 
    url = "https://3xpl.com/data/dumps/ethereum-erc-20"
    start_date = datetime(2016, 2, 21)
elif sys.argv[1]=="gnosis":
    url = "https://3xpl.com/data/dumps/gnosis-chain-erc-20"
    start_date = datetime(2020, 4, 1)
elif sys.argv[1]=="polygonzk":
    url = "https://3xpl.com/data/dumps/polygon-zkevm-erc-20"  
    start_date = datetime(2023, 3, 24)
elif sys.argv[1]=="opbnb":
    url = "https://3xpl.com/data/dumps/opbnb-bep-20"   
    start_date = datetime(2023, 8, 15)
else:
    print("wrong blockchain argument")
    exit(1)

load_dotenv(dotenv_path="../src/poisoning_detector/.env")

TOKEN = os.getenv("xplToken")
headers = {"Authorization": f"Bearer {TOKEN}"}

#because datasets are huge we must divide approaches in batches (download, exctract, convert to parquet per 1/3 of dataset for example) - parquets are way smaller than tsv (compression)
#start_date = datetime(966, 4, 14) #2022, 1, 1  ; 2021, 11, 12 <- begin date; we get data since near the given network launch  ###2023, 10, 23   ####2024, 10, 23
end_date = datetime(2025, 7, 1) #2025, 7,1 <- end date
response = requests.get(url, headers=headers)
response.raise_for_status()
soup = BeautifulSoup(response.content, "html.parser")

def extract_date_from_link(link): 
    match = re.search(r'_(\d{8})\.tsv\.zst$', link)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d')
    return None

links = []
for a in soup.find_all("a", href=True):
    href = a['href']
    if href.endswith(".tsv.zst"):
        file_date = extract_date_from_link(href)
        if file_date and start_date <= file_date <= end_date:
            links.append(href)

folder_name = f"{sys.argv[1]}_erc20_dumps"
parent_path = os.path.join("..", folder_name)
os.makedirs(parent_path, exist_ok=True)

for link in links:
    if not link.startswith("http"):
        file_url = "https://3xpl.com" + link
    else:
        file_url = link

    filename = os.path.join(parent_path, os.path.basename(file_url))
    print(f"Downloading {file_url} -> {filename}")

    with requests.get(file_url, stream=True, headers=headers) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

