from Pipeline.process_all import process_all

process_all("2e Datasets/packs/spells")

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

root = Path("../2eDataManipulation/Content/spell")

for file in root.glob("*.parquet"):
    print("File:", file.name)
    df = pd.read_parquet(file)
    print("Rows:", len(df))
    print("Columns:", df.columns.tolist())
    print(df.head(), "\n")