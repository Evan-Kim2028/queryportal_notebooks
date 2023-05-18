from dotenv import load_dotenv
import os
from subgrounds import Subgrounds

load_dotenv()

# LOAD Graph Network Subgraph ID and Playgrounds API Key
sg = Subgrounds(headers={"Playgrounds-Api-Key": os.environ['PG_KEY']})
balv2 = sg.load_subgraph(
f"https://api.playgrounds.network/v1/proxy/subgraphs/id/Ei5typKWPepPSgqkaKf3p5bPhgJesnu1RuRpyt69Pcrx"   # balancer v2 ID
)

# Select query path, field paths, and define query
balv2_qp = balv2.Query.swaps
balv2_swaps = balv2_qp(first=5000)  #  works at 100, 1111, but not at 5000

# Run query
df = sg.query_df(balv2_swaps)
print(df.shape)