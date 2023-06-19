import os
import polars as pl

from queryportal.subgraphinterface import SubgraphInterface

pl.Config.set_fmt_str_lengths(200)


# Load decentralized endpoints
sgi = SubgraphInterface(endpoints={
    'univ3': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7',
    'balv2': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/Ei5typKWPepPSgqkaKf3p5bPhgJesnu1RuRpyt69Pcrx',
    'curve': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/GAGwGKc4ArNKKq9eFTcwgd1UGymvqhTier9Npqo1YvZB',
    'sushi': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/7h1x51fyT5KigAhXd8sdE3kzzxQDJxxz1y66LTFiC3mS',
    'univ2': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/2szAn45skWZFLPUbxFEtjiEzT1FMW8Ff5ReUPbZbQxtt'
})



########################################################
# Query params

# Fields to be returned from the query
query_paths = [
    'id',
    'symbol',
    'name',
    'decimals'
]

data_list = []

for endpoint in sgi.subject.subgraphs.keys():
    token_df = sgi.query_entity(
        query_size=500000,
        entity='tokens',
        name=endpoint,
        query_paths=query_paths
        )
    
    # add endpoint column to df
    token_df = token_df.with_columns(pl.lit(endpoint).alias('dex'))

    # append to data_list
    data_list.append(token_df)

# concat data_list polars dfs
all_tokens = pl.concat(data_list)

# make a folder called data
if not os.path.exists('data'):
    os.makedirs('data')

# save to parquet in data folder
all_tokens.write_csv('data/all_tokens.csv')