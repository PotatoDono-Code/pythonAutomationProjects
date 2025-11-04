from fastapi import FastAPI
import pandas as pd

app = FastAPI()
df = pd.read_pickle("2eScrubbin/2e_master_pickle.pkl")

def filter_by_value(input_df, key = str, value = str):
    filtered_df = input_df[input_df[key].str.lower() == value.lower()]
    filtered_df = filtered_df.dropna(axis = 1, how = 'all')
    return filtered_data

@app.get("/")
def read_root():
    return {"message": "Welcome to the search API" }

@app.get("/all_type")
def return_all_by_type(value:str):
    if value.isin(df['type']):
        result = filter_by_value(df, 'type', value)
        return result.to_dict(orient='records')
    else:
        {"error": "No entries matching that type"}

