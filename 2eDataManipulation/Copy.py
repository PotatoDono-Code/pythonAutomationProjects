## main.py

from Pipeline.process_all import process_all

process_all("2e Datasets/packs/ancestries")
process_all("2e Datasets/packs/spells")

## process_all.py

import glob
import os
import pandas as pd
from pathlib import Path
from Schema.schema_registry import TYPE_REGISTRY
from Extractor.extractor_registry import EXTRACTOR_REGISTRY
from Pipeline.process_item import process_item_file
from Pipeline.writer import write_master_table

extractor_reg = {
    extractor_name: extractor
    for extractor_name, extractor in EXTRACTOR_REGISTRY.items()
}

def process_all(input_dir):

    master_table = {
        type_name: {
            table_name: []
            for table_name in relations
        }
        for type_name, relations in TYPE_REGISTRY.items()
    }

    master_parq_dir = Path("2eDataManipulation/Content")
    master_parq_dir.mkdir(parents = True, exist_ok = True)

    metadata_dir = master_parq_dir/"metadata"
    metadata_dir.mkdir(parents = True, exist_ok = True)

    id_check_path = metadata_dir/"id_checklist.pkl"
    
    # Retrieve small metadata set of IDs currently recorded to expedite data ingestion
    # if it doesn't already exist, create a new file for use
    if id_check_path.exists():
        id_df = pd.read_pickle(id_check_path)
        known_files = set(id_df['_id'])

    else:
        id_df = pd.DataFrame(columns = ["_id", "type"])
        known_files = set()

    # Create Variable to keep track of how many entries are updated.
    updated = 0

    # Pull all of the file paths to use for the traversing and converting to dataframe information
    # Pull the number of files to use as a counting reference
    json_files = glob.glob(os.path.join(input_dir, "**/*.json"), recursive = True)
    file_count = len(json_files)

    # Storage for updating df at the end
    new_id_records = []

    for i, file_path in enumerate(json_files, 1):
        try:
            new_record = process_item_file(
                i_file_path = file_path, 
                i_known_files = known_files,
                i_extractor_registry = extractor_reg,
                i_master_table = master_table,
                i_new_id_records = new_id_records 
            )

            if new_record:
                updated += 1

        except Exception as e:
            print(f"{file_path} failed with :: {e}")

        if (i%500) == 0:     
            print(f"Processed Files: {i} of {file_count}")
        
    if updated > 0:
        print(f"Beginning saving {updated} new files")

        write_master_table(
            i_master_table = master_table, 
            i_master_parq_dir = master_parq_dir, 
            i_id_df = id_df, 
            i_new_id_records = new_id_records, 
            i_id_check_path = id_check_path)
        
    else: 
        print("No new entries to process")

## writer.py

import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timezone

def generate_batch_id():
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def batch_to_file(relational_dir, table_rows, relational_name, batch_id):
    table = pa.Table.from_pandas(pd.DataFrame(table_rows))
    relational_dir.mkdir(parents=True, exist_ok = True)
        
    with pq.ParquetWriter(f"{relational_dir}/{batch_id}--{relational_name}", table.schema, use_dictionary = True) as writer:
        writer.write_table(table)

def write_master_table(i_master_table, i_master_parq_dir, i_id_df, i_new_id_records, i_id_check_path):
    batch_id = generate_batch_id()
    for types in i_master_table:
            sub_table_dir = i_master_parq_dir/f"{types}"
            sub_table_dir.mkdir(parents = True, exist_ok = True)
            
            for sub_tables in i_master_table[types]:
                relational_table_dir = sub_table_dir/f"{sub_tables}"        
                batch_to_file(relational_table_dir, i_master_table[types][sub_tables], sub_tables, batch_id)

            print(f"Entry type <{types}> completed")


    pd.concat([i_id_df, pd.DataFrame(i_new_id_records)]).to_pickle(i_id_check_path)


## process_item.py

import orjson

def process_item_file(i_file_path, i_known_files, i_extractor_registry, i_master_table, i_new_id_records):
    with open(i_file_path, "rb") as file:
        read_file = orjson.loads(file.read())
        if read_file['_id'] not in i_known_files:
            type_check = read_file['type']
            
            Extractor = i_extractor_registry[type_check]
            extracted_data = Extractor(read_file).extract_all()

            for sub_table, table_row in extracted_data.items():
                
                if table_row is None:
                    continue

                target_table = i_master_table[type_check][sub_table]

                if isinstance(table_row, dict):
                    target_table.append(table_row)

                elif isinstance(table_row, list):
                    target_table.extend(table_row)

                else:
                    raise TypeError(f"Unexpected type: {type(table_row)} for table {target_table}")
                
            i_new_id_records.append({
                "_id" : read_file['_id'],
                "type" : type_check
            })

            return True
                
