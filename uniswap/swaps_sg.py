from dotenv import load_dotenv
import os
from subgrounds import Subgrounds

load_dotenv()

# LOAD Graph Network Subgraph ID and Playgrounds API Key
sg = Subgrounds(headers={"Playgrounds-Api-Key": os.environ['PG_KEY']})
univ3 = sg.load_subgraph(
f"https://api.playgrounds.network/v1/proxy/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7" 
)

# Select query path, field paths, and define query
univ3_qp = univ3.Query.swaps
univ3_swaps = univ3_qp(first=5000)  #  works at 100, 1111, but not at 5000

# Run query
df = sg.query_df(univ3_swaps)
print(df.shape)