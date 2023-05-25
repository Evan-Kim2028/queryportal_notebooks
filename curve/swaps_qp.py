from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta

import polars as pl
pl.Config.set_fmt_str_lengths(200)

# Decentralized
sgi = SubgraphInterface(endpoints={
    # https://thegraph.com/explorer/subgraphs/GAGwGKc4ArNKKq9eFTcwgd1UGymvqhTier9Npqo1YvZB?view=Overview&chain=mainnet
    'curve': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/GAGwGKc4ArNKKq9eFTcwgd1UGymvqhTier9Npqo1YvZB'
    })


# print subgraph keys
univ3_dict = sgi.subject.getQueryPaths(sgi.subject.subgraphs['curve'], 'swaps')


# print fields for swaps entity
print(f'my dict fields for univ3_decentralized swaps entity: {list(univ3_dict.keys())}')

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

filter = {
    'timestamp_gte': int((datetime(2023, 5, 11).timestamp())),
    'timestamp_lte': int(datetime(2023, 5, 18).timestamp()),
    # 'amountInUSD_gte': 10,
    # 'amountOutUSD_gte': 10
}

query_size = 15000

curve = sgi.query_entity(
    query_size=query_size,
    entity='swaps',
    name='curve', 
    query_paths=query_paths,
    filter_dict = filter,
    orderBy='timestamp',
    saved_file_name='curve_swaps'
    )


print(curve.head(5))