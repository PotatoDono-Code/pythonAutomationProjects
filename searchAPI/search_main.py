from fastapi import FastAPI
import pandas as pd

app = FastAPI()
df = pd.read_pickle("2eScrubbin/2e_master_pickle.pkl")

def filter_by_value(input_df, key = str, value = str):
    filtered_df = input_df[input_df[key].str.lower() == value.lower()]
    filtered_df = filtered_df.dropna(axis = 1, how = 'all')
    return filtered_df

@app.get("/")
def read_root():
    return {"message": "Welcome to the search API" }

@app.get("/all_type")
def return_all_by_type(value:str):
    if value in df['type']:
        result = filter_by_value(df, 'type', value)
        return result.to_dict(orient='records')
    else:
        return {"error": "No entries matching that type"}

# system.defense.save.statistic - save type
# system.trait.tradition - spell tradition
# system.trait.value - spell traits (used to identify cantrip as well)
# system.level.value - spell level
@app.get("/spell_filter")
def complete_spell_filter(list: str = None, level: int = None, trait: str = None, rare: str == None, save: str = None):
    query_df = df.clone()
    
    if list:
        query_df = query_df[query_df['system.traits.traditions'] == list]
    if level:
        if level > 1:
            query_df = query_df[query_df['system.level.value'] == level]
        elif level == 1:
            query_df = query_df[query_df['systen.level.value'] == level & "cantrip" not in query_df['system.traits.value']]
        elif level == 0:
            query_df = query_df["cantrip" in query_df['system.traits.value']]
        else:
            return {"error": "Invalid Spell Level Entry"}
    if trait:
        query_df = query_df[trait in query_df['system.traits.value']]
    if rare:
        query_df = query_df[query_df['system.traits.rarity'] == rare]
    if save:
        query_df = query_df[query_df['system.defense.save.statistic'] == save]
