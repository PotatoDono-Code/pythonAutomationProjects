import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

def batch_to_file(relational_dir, table_rows, relational_name, batch_id):
    table = pa.Table.from_pandas(pd.DataFrame(table_rows))
    relational_dir.mkdir(parents = True, exist_ok = True)
        
    with pq.ParquetWriter(f"{relational_dir}/{batch_id}--{relational_name}", table.schema, use_dictionary = True) as writer:
        writer.write_table(table)

def write_ledger_file(i_ledger, i_metadata_dir, i_batch_id):
    table = pa.Table.from_pandas(pd.DataFrame(i_ledger))
    event_dir = i_metadata_dir/"ledger"
    event_dir.mkdir(parents = True, exist_ok = True)

    with pq.ParquetWriter(f"{event_dir}/Batch--{i_batch_id}", table.schema, use_dictionary = True) as writer:
         writer.write_table(table)

def write_master_table(i_master_table, i_master_parq_dir, i_batch_id):
    for types in i_master_table:
            sub_table_dir = i_master_parq_dir/f"{types}"
            sub_table_dir.mkdir(parents = True, exist_ok = True)
            
            for sub_tables in i_master_table[types]:
                rows = i_master_table[types][sub_tables]
                if not rows:
                    continue
                relational_table_dir = sub_table_dir/f"{sub_tables}"        
                batch_to_file(relational_table_dir, i_master_table[types][sub_tables], sub_tables, i_batch_id)

            print(f"Entry type <{types}> completed")