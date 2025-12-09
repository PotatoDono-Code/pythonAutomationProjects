import glob
import orjson
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import traceback

from Extractor.spell_extractor import SpellExtractor

def data_to_table(table_file, table_rows):
    table = pa.Table.from_pandas(pd.DataFrame(table_rows))

    if not table_file.exists():
        pq.write_table(table, table_file)
    
    with pq.ParquetWriter(table_file, table.schema, use_dictionary = True) as writer:
        writer.write_table(table)

extractor_reg = {
    "spell" : SpellExtractor,
}

def process_all(input_dir):

    master_table = {
        "spell" : {
            "main" : [],
            "meta" : [],
            "details" : [],
            "damage" : [],
            "heightening": [],
            "heighten_interval" : [],
            "heighten_level" : [],
            "heighten_level_damage" : [],
            "traits" : [],
            "traditions" : [],
            "ritual" : []
        }
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
            with open(file_path, "rb") as file:
                read_file = orjson.loads(file.read())
                if read_file['_id'] not in known_files:
                    type_check = read_file['type']
                    
                    Extractor = extractor_reg[type_check]
                    extracted_data = Extractor(read_file).extract_all()

                    for sub_table, table_row in extracted_data.items():
                        
                        if table_row is None:
                            continue

                        target_table = master_table[type_check][sub_table]

                        if isinstance(table_row, dict):
                            target_table.append(table_row)
                        elif isinstance(table_row, list):
                            target_table.extend(table_row)
                        else:
                            raise TypeError(f"Unexepcted type: {type(table_row)} for table {target_table}")
                        
                    new_id_records.append({
                        "_id" : read_file['_id'],
                        "type" : type_check
                    })

                    updated += 1

        except Exception as e:
            print(f"{file_path} failed with :: {e}")
            traceback.print_exc()

        if (i%500) == 0:     
            print(f"Processed Files: {i} of {file_count}")
        
    if updated > 0:
        print(f"Beginning saving {updated} new files")

        for types in master_table:
            sub_table_dir = master_parq_dir/f"{types}"
            sub_table_dir.mkdir(parents = True, exist_ok = True)
            
            for sub_tables in master_table[types]:
                sub_table_file = sub_table_dir/f"{sub_tables}.parquet"
           
                data_to_table(sub_table_file, master_table[types][sub_tables])

            print(f"Entry type <{types}> completed")


        pd.concat([id_df, pd.DataFrame(new_id_records)]).to_pickle(id_check_path)

    else: 
        print("No new entries to process")

        