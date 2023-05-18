from queryportal.subgraphinterface import SubgraphInterface
from subutil.schema_utils import *

import polars as pl
pl.Config.set_fmt_str_lengths(200)

sgi = SubgraphInterface(endpoints={
    'olympus-rbs': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/Cy4Y1UCyitfBZATr3PjC3s2eUwj1XxqZMN2tTGgZCNGe' # indexed on Ethereum. Arbitrum isn't supported yet
    }
)

# get all queryable columns
# 2b) Load subgraph object
sg = sgi.subject.subgraphs['olympus-rbs']

# 2c) Get all schema entities
schema_entity_list = getSubgraphSchema(sg)

# 3) Select queryable schema entities
query_field = getQueryFields(sg, schema_entity_list[schema_entity_list.index('Query')])
print(query_field.keys())

ohm_rbs = sgi.query_entity(
    query_size=150000,
    entity='rangeSnapshots',
    name='olympus-rbs',
    saved_file_name='ohm_rbs_range_snapshots'
)

print(ohm_rbs.head(5))