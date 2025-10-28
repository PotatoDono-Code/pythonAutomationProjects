from git import Repo
import os
import pandas as pd
import json
import glob

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

def flatten_json(file_directory):

    file_paths = os.path.join(file_directory, "**/*.json")
    json_files = glob.glob(file_paths, recursive=True)

    for file in json_files:
        
        try:
            with open(file, "r", encoding = "UTF-8") as d:
                file_data = json.load(d)
                df_collection[file]=pd.json_normalize(file_data)

        except(json.JSONDecodeError, KeyError) as error:
            print(f"Skipped {file} due to error: {error}")

flatten_json("2e Datasets/packs")

all_types = []
for each in df_collection:
    try:
        all_types.extend(df_collection[each]['type'].dropna().tolist())
    except:
        print(f"Error for {each}")
print(set(all_types))



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
