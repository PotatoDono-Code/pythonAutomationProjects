from git import Repo
import os
import pandas as pd
import json
import orjson
import glob
from collections import Counter
from pathlib import Path

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
def json_to_pickle(file_directory):
    
    # Reference the master file. If it doesn't not exist, create a new one at that location
    master_pickle_path = Path("2eScrubbin/2e_master_pickle.pkl")
    
    if master_pickle_path.exists():
        df = pd.read_pickle(master_pickle_path)
    else:
        df = pd.DataFrame()

    # Get a collection of all '_id's to use later for skipping the flattening process if it is already done.
    # Create Variable to keep track of how many entries are updated.
    known_files = set(df.get('_id', []))
    updated = 0

    # Pull all of the file paths to use for the traversing and converting to dataframe information
    json_files = glob.glob(os.path.join(file_directory, "**/*.json"), recursive = True)
    file_count = len(json_files)

    # Storage for updating df at the end
    new_records = []

    # Check through every file in the directory. If the _id matches an _id in the known_files, skip it. Otherwise, load the 
    # file, convert it into a dataframe, and add to the master df. Iterate i each time and report every 500 files on progress
    for i, file_path in enumerate(json_files, 1):
        try:
            with open(file_path, "r") as file:
                id_check = orjson.load(file.read())
                if id_check['_id'] not in known_files:
                    new_records.append(pd.json_normalize(id_check))
                    known_files.add(id_check['_id'])
                    updated += 1
                
                if i % 500 == 0:
                    print(f"{i} of {file_count} Processed")

        except Exception as e:
            print(f"{file_path} failed with {e}")

    # If any values have been updated, rewrite the file and report the number of updates. If not, report nothing changed
    if new_records:
        df = pd.concat([df, *new_records], ignore_index = True, sort = False)
        df.to_pickle(master_pickle_path)
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
    unique_values = input_df[target_key].unique()
    for value in unique_values:
        df_collection[value] = extract_json_data_by_key(input_df, target_key, value)
        print(f"Completed DF for {value}")
    return df_collection

# -- Data Optimization and sorting
def sort_common_fields(idf):

    null_value = idf.notna().sum().sort_values(ascending = False)
    null_ratio = (null_value/len(idf)).sort_values(ascending = False)

    sort_table = pd.DataFrame({"Contain Value" : null_value, "Ratio" : null_ratio})
    sort_table = sort_table.reset_index().rename(columns={"index": "column_name"})

    return sort_table

# -- Create a dataframe to hold future dataframes
df_collection = {}

# -- Re-read the main pickle into memory
df = pd.read_pickle("2eScrubbin/2e_master_pickle.pkl")

# -- Pull seperate dfs for each type
df_collection = dfs_by_key_values(df, 'type')

spell_columns = ['system.target.value', 'system.requirements', 'system.description.value', 'system.level.value', '_id', 'name', 
                 'system.traits.rarity', 'system.counteraction', 'system.traits.value', 'system.duration.sustained', 
                 'system.duration.value', 'system.range.value', 'type', 'system.cost.value', 'system.traits.traditions', 
                 'system.time.value', 'system.defense.save.statistic', 'system.defense.save.basic', 'system.area.type', 
                 'system.area.value', 'system.area.details']

# sort_common_fields(df_collection['spell']).to_csv("spell_common_fields.csv", index = False)
# print(df[df['_id'] == "YLzufF5UKRGjT83M"].dropna(axis = 1, how = "all"))
spell_collection = df_collection['spell'][spell_columns]

spell_collection.to_csv("spell_export.csv", index = False)