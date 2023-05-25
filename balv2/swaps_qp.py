from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta

import polars as pl
pl.Config.set_fmt_str_lengths(200)

# HOSTED
# sgi = SubgraphInterface(endpoints=['https://api.thegraph.com/subgraphs/name/messari/balancer-v2-ethereum'])

# Decentralized
sgi = SubgraphInterface(endpoints={
    # https://thegraph.com/explorer/subgraphs/Ei5typKWPepPSgqkaKf3p5bPhgJesnu1RuRpyt69Pcrx?view=Overview&chain=mainnet
    'balancer-v2-ethereum': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/Ei5typKWPepPSgqkaKf3p5bPhgJesnu1RuRpyt69Pcrx'
    })


# print subgraph keys
balv2_dict = sgi.subject.getQueryPaths(sgi.subject.subgraphs['balancer-v2-ethereum'], 'swaps')

# query_paths = ['id', 'hash', 'to', 'from', 'blockNumber', 'timestamp', 'tokenIn', 'amountIn', 'amountInUSD', 'tokenOut', 'amountOut', 'amountOutUSD', 'pool']


# print fields for swaps entity
# print(f'my dict fields for balv2_decentralized swaps entity: {list(balv2_dict.keys())}')

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
    'timestamp_gte': int((datetime(2023, 5, 11).timestamp())),
    'timestamp_lte': int(datetime(2023, 5, 18).timestamp()),
    # 'amountInUSD_gte': 10,
    # 'amountOutUSD_gte': 10
}

query_size = 15000 # started around 7141

balv2 = sgi.query_entity(
    query_size=query_size,
    entity='swaps',
    orderBy='timestamp',
    query_paths=query_paths,
    filter_dict = filter,
    name='balancer-v2-ethereum',
    saved_file_name='balv2_swaps'
    )

print(balv2.head(5))