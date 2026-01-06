import duckdb as db
from pathlib import Path

# Create an in-memory DuckDB instance
conn = db.connect(database=":memory:")

type_list = ["spell", "ancestry"]

for type in type_list:
    file_dir = Path(__file__).resolve().parent.parent / "Content" / f"{type}"

    path_dict = {
        p.stem : p.as_posix()
        for p in spell_dir.glob("*.parquet")
    }

    for name, path in path_dict:
        conn.execute(f"CREATE VIEW {type}_{name} AS SELECT * FROM '{path}'")


spell_dir = Path(__file__).resolve().parent.parent / "Content" / "spell"
# Create an in-memory DuckDB instance
conn = db.connect(database=":memory:")

path_dict = {
    p.stem : p.as_posix()
    for p in spell_dir.glob("*.parquet")
}

for name, path in path_dict.items():
    conn.execute(f"CREATE VIEW {name} AS SELECT * FROM '{path}'")

query = ""

q_select = "SELECT m.name"
q_from = " FROM main m"
q_join = " "
q_where = " WHERE 1=1 "

q_select += ", m.level, d.description"
q_join += "JOIN details d ON m.id = d.id "
q_join += "JOIN meta me ON m.id = me.id "
q_where += "AND m.level = 6 AND me.remaster = FALSE "
query = q_select + q_from + q_join + q_where

print(conn.execute(query).fetch_df())