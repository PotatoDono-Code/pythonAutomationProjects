import pandas as pd
import numpy as np
import kagglehub
from kagglehub import KaggleDatasetAdapter

# Import Dirty Dataset from Kaggle
ddf = kagglehub.dataset_load(KaggleDatasetAdapter.PANDAS, "ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training", "dirty_cafe_sales.csv")

# Setup Product Reference Table
rt = pd.DataFrame({
    'item' : ["Coffee", "Tea", "Sandwich", "Salad", "Cake", "Cookie", "Smoothie", "Juice"],
    'price' : [2, 1.5, 4, 5, 3, 1, 4, 3]
})

# -- Numerical Values to Floats
cols = ['Price Per Unit', 'Quantity', 'Total Spent']
ddf[cols] = ddf[cols].apply(pd.to_numeric, errors='coerce')

# -- Replace all Non-Standard items with NaN
ddf['Item'] = ddf['Item'].where(ddf["Item"].isin(rt["item"]))
replaced_item_mask = ddf['Price Per Unit'].isin(rt['price'])
ddf['Price Per Unit'] = ddf['Price Per Unit'].where(replaced_item_mask)


# -- Update Price Via Reference Table
merged_df = ddf.merge(rt, how='left', left_on='Item', right_on='item')
ddf['Price Per Unit'] = merged_df['price'].combine_first(merged_df["Price Per Unit"])

# -- Remove rows where price values are unrecoverable
ddf = ddf.drop(ddf[ddf['Price Per Unit'].isna() & ddf['Quantity'].isna() & ddf['Total Spent'].isna()].index)

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

# print(merged_df.head())

# ddf['Price Per Unit'] = ddf['Price Per Unit'].where(ddf['Price Per Unit'].isin(rt['price']))
# print(ddf['Price Per Unit'].unique())

# # Pull Unique Items
# u_items = ddf['Item'].unique()
# print(u_items)

# # Replace all entries not matching a reference key with NaN
# def remove_wrong_by_key(idf_column, key_column):
#     idf_column = idf_column.where(idf_column.isin(key_column))
#     return idf_column

# ddf['Item'] = remove_wrong_by_key(ddf['Item'], rt['item'])

# u_items = ddf['Item'].unique()
# print(u_items)

# merged_df = ddf.merge(rt, how='left', left_on = 'Item', right_on = "item")

# print(merged_df.head())

# merged_df['Price Per Unit'] = merged_df['price'].combine_first(merged_df['Price Per Unit'])

# cdf = merged_df.drop(columns = ['item', 'price'])

# print(cdf.head())

# print(cdf['Price Per Unit'].unique())

# cdf_mode = (
#     cdf.groupby('Price Per Unit')['Item']
#     .agg(lambda x: x.mode().iloc[0]
#          if not x.mode().empty else None)
#          .reset_index()
# )

# print(cdf_mode.head())

# cdf_mode.columns = ['mo_ppu', 'mo_item']
# merged_df = cdf.merge(cdf_mode, how = 'left', left_on = 'Price Per Unit', right_on = 'mo_ppu')
# merged_df['Item'] = merged_df['mo_item'].combine_first(merged_df['Item'])

# cdf = merged_df.drop(columns = ['mo_item', 'mo_ppu'])

# print(cdf['Item'].unique())

# print(cdf.head())

# print(cdf['Price Per Unit'].dtype)
# print(cdf_mode['mo_ppu'].dtype)

# ## All this is useless...
# ##def update_value_by_key(idf, irt):
# ##    idf.loc[idf['Item'].isin(irt['item']), 'Price Per Unit'] = irt.loc['price', this]
# ##    return idf

# ##ddf = update_value_by_key(ddf, rt)

# ##print(ddf.head())

# print(cdf_mode.columns)

# missing = cdf[cdf['Item'].isna()]
# print("Rows with missing items:", len(missing))
# print(missing['Price Per Unit'].value_counts())
# print(missing)

# print(cdf['Price Per Unit'].unique())
# print(cdf_mode['mo_ppu'].unique())