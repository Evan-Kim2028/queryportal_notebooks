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
    'balancer-v2-ethereum': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/Ei5typKWPepPSgqkaKf3p5bPhgJesnu1RuRpyt69Pcrx',
    'curve': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/GAGwGKc4ArNKKq9eFTcwgd1UGymvqhTier9Npqo1YvZB'
    })


# there appears to be something corrupted in these fieldpaths...
# query_paths = ['hash', 'to', 'from', 'blockNumber', 'timestamp', 'tokenIn', 'amountIn', 'amountInUSD', 'tokenOut', 'amountOut', 'amountOutUSD', 'pool']

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

# if a data folder doesn't exist, then make one
if not os.path.exists('data'):
    os.makedirs('data')

query_size = 100000

for i in range(30000, query_size, 1500):    # NOTE - keeps crashing around this number - Querying from 30000 - 33000 keeps crashing...all around 30000 - 40000 keeps crashing
    print(i)
    date_range_a = datetime(2023, 5, 18).timestamp() - (i + 1500)

    # start date
    date_range_b = datetime(2023, 5, 18).timestamp() - i

    print(f'Querying from {date_range_b} to {date_range_a}')
    # calculate percent left
    percent_left = (i + 1500) / query_size
    print(f'{percent_left:.2f}% complete')

    filter = {
        'timestamp_gte': int(date_range_a),
        'timestamp_lte': int(date_range_b),
        # 'amountInUSD_gte': 10,
        # 'amountOutUSD_gte': 10
        # 'hash': "0xb2d071e74709bddc8ab005aa42ce4251ad77453fed195f856b46d055c88cb556"
    }

    # set query_size to a high upper bound
    query_size = 4500

    univ3 = sgi.query_entity(
        query_size=2250,
        entity='swaps',
        name='uniswap-v3-ethereum', 
        query_paths=query_paths,
        filter_dict = filter,
        orderBy='timestamp',
        saved_file_name=f'data/univ3_swaps_{i}',
        )
    
    print(univ3.shape)