#IT IS USED TO EXTRACT NAME SYMBOL AND DECIMALS OF A SMART CONTRACT BY ADDRESS
#RAW JSON-RPC is used to avoid middleware eth_chainId calls which could hit provider limits
from web3 import Web3
from requests.exceptions import HTTPError
import time
from eth_abi import decode
from eth_abi import encode
from eth_utils import to_checksum_address
import requests
class GetTokens:
    def __init__(self, RPC= None, RPC2 = None):
        self.RPC = RPC
        self.RPC2 = RPC2
        self.ERC20_ABI = [
            {"constant":True,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"type":"function"},
            {"constant":True,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},
            {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint256"}],"type":"function"},
        ]
        self.MULTICALL_ADDRESS = Web3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")

    
    @staticmethod
    def decode_str(success, data):
            if not success or data == b"":
                return None
            try:
                return decode(["string"], data)[0]
            except Exception:
                return None

    @staticmethod
    def decode_uint(success, data):
        if not success or data == b"":
            return None
        try:
            return decode(["uint256"], data)[0]
        except Exception:
            return None

    def get_token_info(self, address, rpc_url):
        try:
            result = self.raw_get_token_info(
                rpc_url,
                self.MULTICALL_ADDRESS,
                address,
            )
            name, symbol, decimals = self.decode_multicall_result(result)
        except HTTPError:
            raise
        except Exception as e:
            print(e)
            return None, None, None

        return name, symbol, decimals
    
    def raw_get_token_info(self,rpc_url, multicall_address, token):
        data = self.build_multicall_calldata(token)

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": multicall_address,
                    "data": data,
                },
                "latest",
            ],
        }
        r = requests.post(rpc_url, json=payload, timeout=5)
        r.raise_for_status()

        return r.json()["result"]

    def build_multicall_calldata(self,token):
        token = to_checksum_address(token)

        calls = [
            (token, True, bytes.fromhex("06fdde03")),  # name()
            (token, True, bytes.fromhex("95d89b41")),  # symbol()
            (token, True, bytes.fromhex("313ce567")),  # decimals()
        ]

        selector = bytes.fromhex("82ad56cb") #aggregate3 selector

        encoded_args = encode(
            ["(address,bool,bytes)[]"],
            [calls]
        )

        return "0x" + (selector + encoded_args).hex()
 

    def decode_multicall_result(self,result_hex):
        raw = bytes.fromhex(result_hex[2:])

        results = decode(
            ["(bool,bytes)[]"],
            raw
        )[0]

        def decode_str(success, data):
            if not success or not data:
                return None
            return decode(["string"], data)[0]

        def decode_uint(success, data):
            if not success or not data:
                return None
            return decode(["uint256"], data)[0]

        name = decode_str(*results[0])
        symbol = decode_str(*results[1])
        decimals = decode_uint(*results[2])

        return name, symbol, decimals

    def safe_get_token_info(self, addr, wait_time=1, info=0):
        i=1
        while True:
            try:
                if i%2==0:
                    return self.get_token_info(addr,self.RPC2)
                else:
                    return self.get_token_info(addr,self.RPC)   
            except HTTPError as e:
                if info==1:
                    print(f"Network error {e}, retrying in {wait_time*i} seconds...")
                time.sleep(wait_time*i)
                i+=1
                continue
    
