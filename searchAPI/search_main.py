from fastapi import FastAPI
import pandas as pd
import json

app = FastAPI()
df = pd.read_pickle("../2eScrubbin/2e_master_pickle.pkl")

def filter_by_value(input_df, key = str, search_value = str):
    filtered_df = input_df[input_df[key].str.lower() == search_value.lower()]
    filtered_df = filtered_df.dropna(axis = 1, how = 'all')
    return filtered_df

@app.get("/")
def read_root():
    return {"message": "Welcome to the search API" }

@app.get("/all_type")
def return_all_by_type(value:str):
    if value in df['type'].unique():
        result = filter_by_value(df, 'type', value).to_json(orient = 'records')
        return json.loads(result)
    else:
        return {"error": "No entries matching that type"}

# system.defense.save.statistic - save type
# system.trait.tradition - spell tradition
# system.trait.value - spell traits (used to identify cantrip as well)
# system.level.value - spell level
@app.get("/spell_filter")
def complete_spell_filter(tradition: str = None, level: int = None, trait: str = None, spell_rarity: str = None, save: str = None):
    mask = pd.Series(True, index = df.index)
    
    if tradition:
        mask &= df['system.traits.traditions'] == tradition
    if level is not None:
        if level > 1:
            mask &= df['system.level.value'] == level
        elif level == 1:
            mask &= (df['system.level.value'] == level) & (~df['system.traits.value'].apply(lambda traits: isinstance(traits, (list, str)) and "cantrip" in traits))
        elif level == 0:
            mask &= df['system.traits.value'].apply(lambda traits: isinstance(traits, (list, str)) and "cantrip" in traits)
        else:
            return {"error": "Invalid Spell Level Entry"}
    if trait:
        mask &= df['system.traits.value'].apply(lambda traits: isinstance(traits, (list, str)) and trait in traits)
    if spell_rarity:
        mask &= df['system.traits.rarity'] == spell_rarity
    if save:
        mask &= df['system.defense.save.statistic'] == save

    query_df = df[mask].dropna(axis = 1, how = "all").to_json(orient = "records")

    return json.loads(query_df)
