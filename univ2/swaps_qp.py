from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta

import polars as pl
pl.Config.set_fmt_str_lengths(200)

# HOSTED
# sgi = SubgraphInterface(endpoints=['https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-ethereum'])

# Decentralized
sgi = SubgraphInterface(endpoints={
    # https://thegraph.com/explorer/subgraphs/2szAn45skWZFLPUbxFEtjiEzT1FMW8Ff5ReUPbZbQxtt?view=Overview&chain=mainnet
    'uniswap-v2-ethereum': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/2szAn45skWZFLPUbxFEtjiEzT1FMW8Ff5ReUPbZbQxtt'
    })


# print subgraph keys
univ3_dict = sgi.subject.getQueryPaths(sgi.subject.subgraphs['uniswap-v2-ethereum'], 'swaps')


query_paths = [
    'transaction_id',
    'pair_token0_symbol',
    'pair_token1_symbol',
]

filter = {
    'timestamp_gte': int((datetime(2023, 5, 2).timestamp())),
    'timestamp_lte': int(datetime(2023, 5, 18).timestamp()),
    'amountInUSD_gte': 10,
    'amountOutUSD_gte': 10
}

query_size = 50000 # started around 7141

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


# # print fields for swaps entity
# print(f'my dict fields for univ3_decentralized swaps entity: {list(univ3_dict.keys())}')

# query_size = 1500

# univ2 = sgi.query_entity(
#     query_size=query_size,
#     entity='swaps',
#     orderBy='timestamp',
#     name='uniswap-v2-ethereum', 
#     )

# print(univ2)
