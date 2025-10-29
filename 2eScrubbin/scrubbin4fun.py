from git import Repo
import os
import pandas as pd
import json
import glob
from collections import Counter
from pathlib import Path

# -- Target the directory for the 2e Data Repo
rd = "2e Datasets"

# -- Either clone from scratch or update. Only deal with files listed in the 'packs' subfolder
if not os.path.exists(rd):
    repo = Repo.clone_from("https://github.com/foundryvtt/pf2e.git", rd, no_checkout = True, branch="release")
else:
    repo = Repo(rd)

git_cmd = repo.git
git_cmd.sparse_checkout('init', '--cone')
git_cmd.sparse_checkout('set', 'packs')
git_cmd.checkout('release')

df_collection = {}

# 1. Create a dictionary of common search terms
# 2. Flatten and Search through single JSON file and store all entries into pandas data frame
# 3. Store only needed entries into data frame
# 4. Search through multiple files and store all entries in data frame
# 5. Search through multiple files and store selected entries into a data fram

# def flatten_json(file_directory):

#     file_paths = os.path.join(file_directory, "**/*.json")
#     json_files = glob.glob(file_paths, recursive=True)

#     for file in json_files:
        
#         try:
#             with open(file, "r", encoding = "UTF-8") as d:
#                 file_data = json.load(d)
#                 df_collection[file]=pd.json_normalize(file_data)

#         except(json.JSONDecodeError, KeyError) as error:
#             print(f"Skipped {file} due to error: {error}")

# flatten_json("2e Datasets/packs")

# all_types = []
# for each in df_collection:
#     try:
#         all_types.extend(df_collection[each]['type'].dropna().tolist())
#     except:
#         print(f"Error for {each}")
# print(set(all_types))

# ~~~~~~~~~~ Schema Frequency Mapping?

def gen_frequency_map(file_directory):
    all_keys = Counter()

    json_files = glob.glob(os.path.join(file_directory, "**/*.json"), recursive=True)
    file_count = len(json_files)

    for i, json_file in enumerate(json_files, 1):
        try:
            with open(json_file, "r", encoding="UTF-8") as file:
                data = json.load(file)
                df = pd.json_normalize(data)
                all_keys.update(df.columns)

        except Exception as e:
            print(f"Skipped {json_file} for {e}")

        if i % 500 == 0:
            print(f"{i} of {file_count} Processed")

    return all_keys

def map_to_df(file_directory, top_n = 50):
    all_keys = gen_frequency_map(file_directory)
    df = pd.DataFrame(all_keys.items(), columns = ['Reference', 'Count'])
    df = df.sort_values(['Count'], ascending = False).reset_index(drop = True)
    return df.head(top_n)

# -- Export full dataframe to file

def json_to_pickle(file_directory):
    master_pickle_path = Path("2eScrubbin/2e_master_pickle.pkl")
    
    if master_pickle_path.exists():
        df = pd.read_pickle(master_pickle_path)
    else:
        df = pd.DataFrame()

    known_files = set(df.get('_id', []))
    updated = 0

    json_files = glob.glob(os.path.join(file_directory, "**/*.json"), recursive = True)
    file_count = len(json_files)

    for i, file_path in enumerate(json_files, 1):
        try:
            with open(file_path, "r", encoding = "UTF-8") as file:
                id_check = json.load(file)
                if id_check['_id'] not in known_files:
                    df = pd.concat([df, pd.json_normalize(id_check)], ignore_index = True, sort = False)
                    known_files.add(id_check['_id'])
                    updated += 1
                
                if i % 500 == 0:
                    print(f"{i} of {file_count} Processed")

        except Exception as e:
            print(f"{file_path} failed with {e}")

    if updated > 0:
        df.to_pickle(master_pickle_path)
        print(f"Updated {updated} file entries.")

    else:
        print(f"No new files to update")

def sort_common_keys(input_dataframe, top_n = 50):
    total_count = input_dataframe.notna().sum().sort_values(ascending = False)
    return total_count.head(top_n)

df = pd.read_pickle("2eScrubbin/2e_master_pickle.pkl")

def extract_json_data_by_key(input_df, target_key, target_value):
    filtered_data = input_df[input_df[target_key] == target_value]
    filtered_data = filtered_data.dropna(axis = 1, how = 'all')
    return filtered_data

def dfs_by_key_values(input_df, target_key):
    df_collection = {}
    unique_values = input_df[target_key].unique()
    for value in unique_values:
        df_collection[value] = extract_json_data_by_key(input_df, target_key, value)
        print(f"Completed DF for {value}")
    return df_collection

type_dfs = dfs_by_key_values(df, 'type')
    



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# # -- Testing out files
# # test_json = "2e Datasets/packs/abomination-vaults-bestiary/abomination-vaults-hardcover-compilation/beluthus.json"

# # with open(test_json) as json_file:
# #     data = json.load(json_file)

# # tdf = pd.json_normalize(data)
# # print(tdf.head())

# # -- Search through a file and pull values from nested targets
# def nested_value_search(file, keys):
    
#     for i in keys:
#         if isinstance(file, dict):
#             file = file.get(i)
#     return file if file else None

# def multi_value_return(json_data, key_group):
#     values_collection = []
#     filtered_data = ""
    
#     for keys in key_group:
#         filtered_data = json_data
#         for key in keys:
#             if isinstance(filtered_data, dict):
#                 filtered_data = filtered_data.get(key)
#         values_collection.append(filtered_data) if filtered_data else ""
    
#     return values_collection

# # -- Recurse through directory, pulling all valid entries for entered search keys
# def pull_all_cat_values(file_directory, search_key, df_name):

#     cat_values = []

#     file_paths = os.path.join(file_directory, "**/*.json")
#     json_files = glob.glob(file_paths, recursive=True)

#     for path in json_files:
#         try:
#             with open(path, "r", encoding = "UTF-8") as file:
#                 data = json.load(file)
#                 captured_value = multi_value_return(data, search_key)
#                 cat_values.append(captured_value)

#         except(json.JSONDecodeError, KeyError) as error:

#             print(f"Skipped {path} due to error: {error}")
    
#     df_collection[df_name] = pd.DataFrame(cat_values)

#     target_names = []
    
#     for key in search_key:
#         if isinstance(key, str):
#             target_names.append(key)
#         elif isinstance(key, list):
#             target_names.append(key[-1])

#     df_collection[df_name].columns = target_names

#     return df_collection[df_name]

# all_types = pull_all_cat_values("2e Datasets/packs", ["_id", "type", ["system", "details", "publication", "title"]], "Test DF")
# print(all_types)
