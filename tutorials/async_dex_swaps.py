import asyncio
import itertools
import os
import polars as pl

from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta

pl.Config.set_fmt_str_lengths(200)


# Load decentralized endpoints
sgi = SubgraphInterface(endpoints={
    # 'univ3': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7',
    'balv2': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/QmVBhgtdcaM25eVPLzL8Ra6oriy71yeS5otXMFdKshwxDx',
    # 'curve': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/GAGwGKc4ArNKKq9eFTcwgd1UGymvqhTier9Npqo1YvZB',
    # 'sushi': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/7h1x51fyT5KigAhXd8sdE3kzzxQDJxxz1y66LTFiC3mS',
})

# make a folder called data
if not os.path.exists('data'):
    os.makedirs('data')

# make a subfolder for each subgraph.
for subgraph in list(sgi.subject.subgraphs.keys()):
    os.makedirs(f'data/{subgraph}', exist_ok=True)

# Fields to be returned from the query
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

query_size = 1250
# Create a tuple of daily date ranges 
date_ranges = [(start_date, start_date + timedelta(days=1)) for start_date in [datetime(2023, 5, 22) + timedelta(days=i) for i in range(0, 7, 1)]]


# Define a function to be run in a thread
def process_subgraph(subgraph, start_date, end_date):

    filter = {
        'timestamp_gte': int(start_date.timestamp()),
        'timestamp_lte': int(end_date.timestamp()),
    }

    sgi.query_entity(
        query_size=query_size,
        entity='swaps',
        name=subgraph,
        query_paths=query_paths,
        filter_dict=filter,
        orderBy='timestamp',
        # graphql_query_fmt=True,
        # saved_file_name=f'data/{subgraph}/{subgraph}_swaps_{start_date.strftime("%m-%d")}_{end_date.strftime("%m-%d")}'
        )


# Define the async loop
async def main():
    subgraph_keys = list(sgi.subject.subgraphs.keys())
    date_ranges = [(start_date, start_date + timedelta(days=1)) for start_date in [datetime(2023, 5, 22) + timedelta(days=i) for i in range(0, 7, 1)]]

    await asyncio.gather(*[asyncio.to_thread(process_subgraph, subgraph, start_date, end_date) for subgraph, (start_date, end_date) in itertools.product(subgraph_keys, date_ranges)])


# Run the asyncio event loop
asyncio.run(main())

