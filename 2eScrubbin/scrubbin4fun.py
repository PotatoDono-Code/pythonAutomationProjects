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

# -- Testing out files
# test_json = "2e Datasets/packs/abomination-vaults-bestiary/abomination-vaults-hardcover-compilation/beluthus.json"

# with open(test_json) as json_file:
#     data = json.load(json_file)

# tdf = pd.json_normalize(data)
# print(tdf.head())

# -- Search through a file and pull values from nested targets
def nested_value_search(file, keys):
    
    for i in keys:
        if isinstance(file, dict):
            file = file.get(i)
    return file if file else None

def multi_value_return(file, keys):
    values_collection = []
    
    for i in keys:
        for j in keys[i]:
            if isinstance(file, dict):
                file = file.get(j)
        values_collection.append(file) if file else None
    
    return values_collection

# -- Recurse through directory checking each .json file
def pull_all_cat_values(file_directory, search_key):

    cat_values = []

    file_paths = os.path.join(file_directory, "**/*.json")
    json_files = glob.glob(file_paths, recursive=True)

    for path in json_files:
        try:
            with open(path, "r", encoding = "UTF-8") as file:
                data = json.load(file)
                captured_value = multi_value_return(data, search_key)
                if isinstance(captured_value, str):
                    cat_values.append(captured_value)

        except(json.JSONDecodeError, KeyError) as error:

            print(f"Skipped {path} due to error: {error}")
    
    rdf = pd.DataFrame(cat_values)
    key_conversion = []
    for i in search_key:
        if isinstance(search_key, str):
            search_key = [search_key]
        key_conversion.append(search_key)
    target_names = [last[-1] for last in key_conversion]
    rdf.columns = target_names

    return rdf

all_types = pull_all_cat_values("2e Datasets/packs", ["_id", "type", ["system", "details", "publication", "title"]])
