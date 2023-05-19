from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta
import os

import polars as pl
pl.Config.set_fmt_str_lengths(200)

# HOSTED
# sgi = SubgraphInterface(endpoints=['https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-ethereum'])

# Decentralized
sgi = SubgraphInterface(endpoints={
    # https://thegraph.com/explorer/subgraphs/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7?view=Overview&chain=mainnet
    'uniswap-v3-ethereum': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7',
    # 'balancer-v2-ethereum': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/Ei5typKWPepPSgqkaKf3p5bPhgJesnu1RuRpyt69Pcrx',
    # 'curve': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/GAGwGKc4ArNKKq9eFTcwgd1UGymvqhTier9Npqo1YvZB',
    # 'sushi' : 'https://api.playgrounds.network/v1/proxy/subgraphs/id/7h1x51fyT5KigAhXd8sdE3kzzxQDJxxz1y66LTFiC3mS',
    
    })

query_paths = [
    'hash',
    'to',
    'from',
    'blockNumber',
    'timestamp',
    'tokenIn_symbol',
    'tokenOut_symbol',
    'amountIn',
    'amountOut',
    'pool_id'
]

filter = {
    'timestamp_gte': int((datetime(2023, 5, 17).timestamp())),
    'timestamp_lte': int(datetime(2023, 5, 18).timestamp()),
    # 'amountInUSD_gte': 10,
    # 'amountOutUSD_gte': 10
    # 'hash': "0xb2d071e74709bddc8ab005aa42ce4251ad77453fed195f856b46d055c88cb556"
}

query_size = 55000

for subgraph in list(sgi.subject.subgraphs.keys()):
    swaps = sgi.query_entity(
        query_size=query_size,
        entity='swaps',
        name=subgraph, 
        query_paths=query_paths,
        filter_dict = filter,
        orderBy='timestamp',
        saved_file_name=f'{subgraph}_swaps'
        )
    print(swaps.shape[0])