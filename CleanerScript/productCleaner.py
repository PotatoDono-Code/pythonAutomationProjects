import pandas as pd
import numpy as np
import kagglehub
from kagglehub import KaggleDatasetAdapter
from rapidfuzz import fuzz
from rapidfuzz import process
from rapidfuzz.process import extractOne

# Import Dirty Dataset from Kaggle
ddf = kagglehub.dataset_load(KaggleDatasetAdapter.PANDAS, "ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training", "dirty_cafe_sales.csv")

# Setup Product Reference Table
rt = pd.DataFrame({
    'item' : ["Coffee", "Tea", "Sandwich", "Salad", "Cake", "Cookie", "Smoothie", "Juice"],
    'price' : [2, 1.5, 4, 5, 3, 1, 4, 3]
})

# -- Define function to retrieve mode
def get_mode(data):
    data.mode().iloc[0] if not data.mode().empty() else None

# -- Fuzzy match function
def fuzzy_match(compare_value, key_series, match_ratio=85):
    
    if pd.isna(compare_value):
        return np.nan
    elif compare_value in key_series:
        return compare_value
    
    match = process.extractOne(compare_value, key_series, scorer=fuzz.token_sort_ratio)
    if match and match[1] >= match_ratio:
        return match[0]
    else:
        return np.nan


# -- Define function to clean items by referencing a key
def replace_by_key(idf, target_column, key_series):
    
    valid_mask = idf[target_column].isin(key_series)
    replacements = idf.loc[~valid_mask, target_column].apply(lambda x: fuzzy_match(x, key_series))
    idf.loc[~valid_mask, target_column]= replacements

    print(f"Fuzzy-matched and replaced {replacements.notna().sum()} values in '{target_column}'.")
  
# -- Numerical Values to Floats
cols = ['Price Per Unit', 'Quantity', 'Total Spent']
ddf[cols] = ddf[cols].apply(pd.to_numeric, errors='coerce')


# -- Replace all Non-Standard items with NaN
replaced_item_mask = ddf['Price Per Unit'].isin(rt['price'])
ddf['Price Per Unit'] = ddf['Price Per Unit'].where(replaced_item_mask)

replace_by_key(ddf, 'Item', rt['item'])

# -- Update Price Via Reference Table
merged_df = ddf.merge(rt, how='left', left_on='Item', right_on='item')
ddf['Price Per Unit'] = merged_df['price'].combine_first(merged_df["Price Per Unit"])


# -- Remove rows where price values are unrecoverable
ur_col = ['Price Per Unit', 'Quantity', 'Total Spent', 'Item']
ur_df = ddf[ur_col].isna()
unrecoverable_mask = (ur_df.sum(axis=1) >= 3)

ddf = ddf.drop(ddf[unrecoverable_mask].index)


# -- Recover Price/Quantity/Total
no_price_mask = (ddf['Price Per Unit'].isna() & ddf['Quantity'].notna() & ddf['Total Spent'].notna())
no_qty_mask = (ddf['Price Per Unit'].notna() & ddf['Quantity'].isna() & ddf['Total Spent'].notna())
no_ttl_mask = (ddf['Price Per Unit'].notna() & ddf['Quantity'].notna() & ddf['Total Spent'].isna())

ddf.loc[no_price_mask, 'Price Per Unit'] = ddf.loc[no_price_mask, 'Total Spent'] / ddf.loc[no_price_mask, 'Quantity']
ddf.loc[no_qty_mask, 'Quantity'] = ddf.loc[no_qty_mask, 'Total Spent'] / ddf.loc[no_qty_mask, 'Price Per Unit']
ddf.loc[no_ttl_mask, 'Total Spent'] = ddf.loc[no_ttl_mask, 'Quantity'] * ddf.loc[no_ttl_mask, 'Price Per Unit']

bad_math_mask = (ddf['Price Per Unit'] * ddf['Quantity'] != ddf['Total Spent']) & ~replaced_item_mask
bad_math_no_price = (ddf['Price Per Unit'] * ddf['Quantity'] != ddf['Total Spent']) & replaced_item_mask

ddf.loc[bad_math_mask, ['Price Per Unit']] = ddf.loc[bad_math_mask, 'Total Spent'] / ddf.loc[bad_math_mask, 'Quantity']
ddf.loc[bad_math_no_price, ['Total Spent']] = ddf.loc[bad_math_no_price, 'Price Per Unit'] * ddf.loc[bad_math_no_price, 'Quantity']


# -- Determine most common item by unit price, and replace null values with mode
df_mode = (ddf.groupby('Price Per Unit')['Item'].agg(lambda x : x.mode().iloc[0] if not x.mode().empty else None))
ddf['Item'] = ddf['Item'].combine_first(ddf['Price Per Unit'].map(df_mode))


# -- If remaining values have NaN Quantity, replace with 1 and recalcuate total
no_qty_mask = ddf['Quantity'].isna()
ddf.loc[no_qty_mask, 'Quantity'] = 1
ddf.loc[no_qty_mask, 'Total Spent'] = ddf.loc[no_qty_mask, 'Price Per Unit']


# -- Replace Invalid Methods with Mode
ddf["Payment Method"] = ddf["Payment Method"].where(ddf['Payment Method'].isin(['Credit Card', 'Cash', 'Digital Wallet']))
pm_mode = ddf['Payment Method'].mode().iloc[0]
print(pm_mode)
ddf.loc[ddf['Payment Method'].isna(), 'Payment Method'] = pm_mode

print(ddf['Payment Method'].unique())


# -- Replace Invalid Locations with weighted Takeaway or In-Store
rng = np.random.default_rng(248)
ddf['Location'] = ddf['Location'].where(ddf['Location'].isin(['Takeaway', 'In-store']))
loc_mask = ddf['Location'].isna()

ta_chance =   ddf['Location'].value_counts().get('Takeaway', 0) / (len(ddf)-loc_mask.sum())
is_chance = ddf['Location'].value_counts().get('In-store', 0) /(len(ddf)-loc_mask.sum())

if loc_mask.sum() > 0:
    ddf.loc[loc_mask, 'Location'] = rng.choice(['Takeaway', 'In-store'], size=loc_mask.sum(), p=[ta_chance, (1 - ta_chance)])

ta_chance =   ddf['Location'].value_counts().get('Takeaway', 0) / (len(ddf))
is_chance = ddf['Location'].value_counts().get('In-store', 0) /(len(ddf))


# -- Date Cleaning
dates = pd.to_datetime(ddf['Transaction Date'], errors= 'coerce')
dates = dates.ffill()
ddf['Transaction Date'] = dates
