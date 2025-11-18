from git import Repo
import os
import pandas as pd
import json
import orjson
import glob
from collections import Counter
from pathlib import Path
import gc
import re
import numpy as np

# # ~~~~~~ Doesn't need to run every time. Un-comment and run periodically ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# # -- Target the directory for the 2e Data Repo
# rd = "2e Datasets"

# # -- Either clone from scratch or update. Only deal with files listed in the 'packs' subfolder
# if not os.path.exists(rd):
#     repo = Repo.clone_from("https://github.com/foundryvtt/pf2e.git", rd, no_checkout = True, branch="release")
# else:
#     repo = Repo(rd)

# git_cmd = repo.git
# git_cmd.sparse_checkout('init', '--cone')
# git_cmd.sparse_checkout('set', 'packs')
# git_cmd.checkout('release')



# -- Convert entire .json file repo in a singular pandas dataframe
# ################### - THIS NEEDS TO BE REWORKED/OPTIMIZED. VERY SLOW ####################
def json_to_parquet_dir(file_directory):
    
    # Reference the master directory. If it doesn't not exist, create a new one at that location
    master_parq_dir = Path("2eScrubbin/2e_master_parquet")
    master_parq_dir.mkdir(parents = True, exist_ok = True)
    metadata_dir = master_parq_dir/"metadata"
    metadata_dir.mkdir(parents = True, exist_ok = True)
    id_check_path = metadata_dir/"id_checklist.pkl"
    
    if id_check_path.exists():
        id_df = pd.read_pickle(id_check_path)
        # Get a collection of all '_id's to use later for skipping the flattening process if it is already d    one.
        known_files = set(id_df['_id'])

    else:
        id_df = pd.DataFrame(columns = ["_id", "type"])
        known_files = set()

    # Create Variable to keep track of how many entries are updated.
    updated = 0

    # Pull all of the file paths to use for the traversing and converting to dataframe information
    json_files = glob.glob(os.path.join(file_directory, "**/*.json"), recursive = True)
    file_count = len(json_files)

    # Storage for updating df at the end
    new_records = []
    new_id_records = []

    # Check through every file in the directory. If the _id matches an _id in the known_files, skip it. Otherwise, load the 
    # file, convert it into a dataframe, and add to the master df. Iterate i each time and report every 500 files on progress
    for i, file_path in enumerate(json_files, 1):
        try:
            with open(file_path, "rb") as file:
                id_check = orjson.loads(file.read())
                if id_check['_id'] not in known_files:
                    record = compress_fields(pd.json_normalize(id_check).iloc[0])
                    new_records.append(record)
                    new_id_records.append({"_id": id_check.get('_id'), "type": record['type']})
                    known_files.add(id_check['_id'])
                    updated += 1
                
                if i % 500 == 0:
                    print(f"{i} of {file_count} Processed")

        except Exception as e:
            print(f"{file_path} failed with {e}")

    # If any values have been updated, rewrite the file and report the number of updates. If not, report nothing changed
    if new_records:
        new_df = pd.DataFrame(new_records)
        # Write parquet to memory in groups, not all at once.
        # timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        for t, subset in new_df.groupby("type"):
            #sub_dir = master_parq_dir/f"{t}"
            #sub_dir.mkdir(parents = True, exist_ok = True)
            subset.dropna(axis = 1, how = 'all').to_pickle(master_parq_dir/f"{t}.pkl")
            print(f"Completed type: {t}")

        id_df = pd.concat([id_df, pd.DataFrame(new_id_records)])
        id_df.to_pickle(id_check_path)

        print(f"Updated {updated} file entries.")
    
    else:
        print(f"No new files to update")

# -- Using a df, report the top n keys that are used the most across all .json files
def sort_common_keys(input_dataframe, top_n = 50):
    total_count = input_dataframe.notna().sum().sort_values(ascending = False)
    return total_count.head(top_n)

# -- For a given df, pull all rows that match a key/value pair. Remove any blank columns from the new df
def extract_json_data_by_key(input_df, target_key, target_value):
    filtered_data = input_df[input_df[target_key] == target_value]
    filtered_data = filtered_data.dropna(axis = 1, how = 'all')
    return filtered_data

# -- For a given df, extract all unique values for a key. Then, create a collection of new dfs filtered down 
# -- for each unique value
def dfs_by_key_values(input_df, target_key):
    df_collection = {}
    pickles_dir = Path("2eScrubbin/TempPickls")
    pickles_dir.mkdir(parents = True, exist_ok = True)
    unique_values = input_df[target_key].unique()
    for value in unique_values:
        df_collection[value] = extract_json_data_by_key(input_df, target_key, value)
        df_collection[value].to_pickle(pickles_dir/f"{value}.pkl")
        print(f"Completed DF for {value}")
    return df_collection

# -- Data Optimization and sorting
def sort_common_fields(idf):

    null_value = idf.notna().sum().sort_values(ascending = False)
    null_ratio = (null_value/len(idf)).sort_values(ascending = False)

    sort_table = pd.DataFrame({"Contain Value" : null_value, "Ratio" : null_ratio})
    sort_table = sort_table.reset_index().rename(columns={"index": "column_name"})

    return sort_table

# -- Clearing out unwanted columns
def compress_fields(input_series):
    compressed_fields = {}
    keep_fields = {}
    num_pattern= re.compile(r"^(.+?)\.(\d+)\.(.+)$")
    drop_pattern = re.compile(r"(\.rule)|(\.selected)|(\.overlays)|(\.heightening)|(\.[A-Za-z0-9]{12,})")

    for c in input_series.index:
        num_pattern_match = num_pattern.search(c)
        drop_pattern_match = drop_pattern.search(c)
        
        if not drop_pattern_match:
        
            if num_pattern_match:
                new_field = f"{num_pattern_match.group(1)}.{num_pattern_match.group(3)}" 
                pulled_value = input_series[c]

                # if (
                #     pulled_value is None 
                #     or pulled_value != pulled_value
                #     or (isinstance(pulled_value, list) and any(v is not v for v in pulled_value))
                #     or (isinstance(pulled_value, list) and len(pulled_value) == 0)
                #     or (isinstance(pulled_value, np.ndarray))
                #     or isinstance(pulled_value, (dict, tuple, set))):

                #     continue
                # print(pulled_value)

                if not pulled_value != pulled_value :      
                    compressed_fields.setdefault(new_field, []).append(input_series[c])

            else:
                keep_fields[c] = input_series[c]

    regulate_compressed = {k: v if len(v) > 1 else v[0] for k, v in compressed_fields.items()}
    combined_fields = keep_fields | regulate_compressed
    
    return pd.Series(combined_fields)






# -- Create a dataframe to hold future dataframes
# df_collection = {}

# -- Re-read the main pickle into memory
# df = pd.read_pickle("2eScrubbin/2e_master_pickle.pkl")

# -- Pull seperate dfs for each type
# df_collection = dfs_by_key_values(df, 'type')

# sort_common_fields(df_collection['spell']).to_csv("spell_common_fields.csv", index = False)
# print(df[df['_id'] == "YLzufF5UKRGjT83M"].dropna(axis = 1, how = "all"))
# spell_collection = pd.read_pickle("2eScrubbin/TempPickls/spell.pkl")


# bad_cols = []

# compressed_spells = spell_collection.apply(compress_fields, axis = 1)
# compressed_spells.to_csv("compressed_spell_export.csv", index = False)

# json_to_parquet_dir("2e Datasets/packs")

print(pd.read_pickle("2eScrubbin/2e_master_parquet/spell.pkl").columns)
print(pd.read_pickle("2eScrubbin/2e_master_parquet/vehicle.pkl").columns)
print(pd.read_pickle("2eScrubbin/2e_master_parquet/class.pkl").columns)


