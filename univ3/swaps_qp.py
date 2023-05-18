from queryportal.subgraphinterface import SubgraphInterface

import polars as pl
pl.Config.set_fmt_str_lengths(200)

# HOSTED
# sgi = SubgraphInterface(endpoints=['https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-ethereum'])

# Decentralized
sgi = SubgraphInterface(endpoints={
    # https://thegraph.com/explorer/subgraphs/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7?view=Overview&chain=mainnet
    'uniswap-v3-ethereum': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7'
    })


# print subgraph keys
univ3_dict = sgi.subject.getQueryPaths(sgi.subject.subgraphs['uniswap-v3-ethereum'], 'swaps')


# print fields for swaps entity
print(f'my dict fields for univ3_decentralized swaps entity: {list(univ3_dict.keys())}')

 # NOTE - getting bad gateway 502 error for some column data corruption here after larger query size (~15000)
query_paths = [
               'hash', 
               'from', 
               'to',
               'pool_name', 
               'blockNumber', 
               'timestamp', 
               'tokenIn_symbol', 
               'amountIn', 
               'amountInUSD', 
               'tokenOut_symbol', 
               'amountOut', 
               'amountOutUSD'
               ]

query_size = 100000

univ3 = sgi.query_entity(
    query_size=query_size,
    entity='swaps',
    name='uniswap-v3-ethereum', 
    query_paths=query_paths,
    saved_file_name='univ3_swaps'
    )


print(univ3.head(5))