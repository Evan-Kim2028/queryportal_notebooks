from dotenv import load_dotenv
import os
from subgrounds import Subgrounds

from datetime import datetime, timedelta

load_dotenv()

# LOAD Graph Network Subgraph ID and Playgrounds API Key
sg = Subgrounds(headers={"Playgrounds-Api-Key": os.environ['PG_KEY']})
univ3 = sg.load_subgraph(
f"https://api.playgrounds.network/v1/proxy/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7" 
)


univ3_qp = univ3.Query.swaps(
    orderBy=univ3.Swap.timestamp,   # or "tokenIn__symbol"
    orderDirection='desc',
    where={    
    'timestamp_gte': int((datetime(2023, 5, 17).timestamp())),
    'timestamp_lte': int(datetime(2023, 5, 18).timestamp()),
    },
    first=100000
)


print(sg.mk_request(univ3_qp).graphql)

# # Run query
df1 = sg.query_df(
    [
    univ3_qp.hash,
    univ3_qp.to,
    # univ3_qp.from,
    univ3_qp.pool.id,
    univ3_qp.blockNumber,
    univ3_qp.timestamp,
    univ3_qp.amountIn,
    univ3_qp.tokenIn.symbol,
    univ3_qp.tokenOut.symbol,
    # univ3_qp.amountInUSD,
    univ3_qp.amountOut,
    # univ3_qp.amountOutUSD,
    ]
    )

# save df1 to csv
df1.to_csv('univ3_swaps.csv', index=False)

