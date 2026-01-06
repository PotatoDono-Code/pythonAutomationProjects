from db.connection import conn
#from db.view_registration import
from pathlib import Path

type_list = ["spell", "ancestry"]

for type in type_list:
    file_dir = Path(__file__).resolve().parent.parent / "Content" / type

    path_dict = {
        p.stem : p.as_posix()
        for p in file_dir.glob("*.parquet")
    }

    for name, path in path_dict.items():
        conn.execute(f"CREATE VIEW {type}_{name} AS SELECT * FROM '{path}'")

print(conn.execute("SHOW TABLES").fetch_df)