from Pipeline.process_all import process_all
import pandas as pd
import glob
import os

# process_all("2e Datasets/packs/ancestries")
input_dir = ("2eDataManipulation/Content/ancestry")

json_files = glob.glob(os.path.join(input_dir, "**/*.parquet"), recursive = True)

for each in json_files:
    test = pd.read_parquet(each)
    print(f"-------{each}-------")
    print(test)
