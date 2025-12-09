import glob
import orjson
import os
import pandas as pd

from Extractor.spell_extractor import SpellExtractor

extractor_reg = {
    "spell" : SpellExtractor,
}

def process_all(input_dir):

    master_table = {
        "spells" : {
            "main" : [],
            "meta" : [],
            "details" : [],
            "damage" : [],
            "heighten": [],
            "heighten_interval" : [],
            "heighten_level" : [],
            "heighten_level_damage" : [],
            "traits" : [],
            "traditions" : [],
            "ritual" : []
        }
    }

    master_parq_dir = Path("../2eScrubbin/2e_master_parquet")
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
                read_file = orjson.loads(file_path.read())
                if read_file['_id'] not in known_files['_id']:
                    type_check = read_file.get("type")
                    
                    Extractor = extractor_reg[type_check]
                    extracted_data = Extractor(read_file)

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
                        "_id" : extracted_data.get("_id"),
                        "type" : extracted_data.get("type")
                    })