from Pipeline.process_all import process_all
import pandas as pd

process_all("2e Datasets/packs/spells")

test = pd.read_parquet("../2eDataManipulation/Content/spell/ritual.parquet")

test = test.sort_values('secondary_casters', ascending= True)
print(test)