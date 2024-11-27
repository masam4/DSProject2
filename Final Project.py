import pandas as pd
from sqlalchemy import create_engine

#Step 1, load data:
global_map_file = 'Global_Map_Full_Data_data.csv'
cumulative_sum_file = 'Cummulative_Sum_by_Year_Full_Data_data.csv'

global_map_data = pd.read_csv(global_map_file)
cumulative_sum_data = pd.read_csv(cumulative_sum_file)

print("\nTransforming 'Global_Map_Full_Data_data.csv'...")

global_map_data.rename(columns={
    "Project/Plant Name": "project_name",
    "Rated Capacity (kWh)": "rated_capacity_kwh",
    "Rated Power (kW)": "rated_power_kw",
    "Discharge Duration at Rated Power (hrs)": "discharge_duration_hrs",
    "Storage Capacity (kWh)": "storage_capacity_kwh"
}, inplace=True)

global_map_data_cleaned = global_map_data.drop_duplicates()

print(f"Transformed 'Global_Map_Full_Data_data.csv': {global_map_data_cleaned.shape} rows and columns")

print("\nTransforming 'Cummulative_Sum_by_Year_Full_Data_data.csv'...")

cumulative_sum_data.rename(columns={
    "Year": "year",
    "Subsystem 1 - Technology Broad Category": "technology_broad_category",
    "Subsystem 1 - Technology Mid-Type": "technology_mid_type",
    "Subsystem 1 - Technology Sub-Type": "technology_sub_type",
    "Rated Power (kW)": "rated_power_kw",
    "Storage Capacity (kWh)": "storage_capacity_kwh"
}, inplace=True)

columns_to_remove = ["Country.1", "Subsystem 1 - Technology Sub-Type.1"]
cumulative_sum_data_cleaned = cumulative_sum_data.drop(columns=columns_to_remove).drop_duplicates()

print(f"Transformed 'Cummulative_Sum_by_Year_Full_Data_data.csv': {cumulative_sum_data_cleaned.shape} rows and columns")

cumulative_sum_data_cleaned["year"] = pd.to_numeric(cumulative_sum_data_cleaned["year"], errors="coerce")
cumulative_sum_data_cleaned.dropna(subset=["year"], inplace=True)

if cumulative_sum_data_cleaned["storage_capacity_kwh"].isnull().sum() > 0:
    cumulative_sum_data_cleaned["storage_capacity_kwh"].fillna(0, inplace=True)

#just a preview of the data
print("\nPreview of transformed datasets:")
print(global_map_data_cleaned.head(), "\n")
print(cumulative_sum_data_cleaned.head())

# 3 Load data into sql
print("\nLoading data into SQL database...")

engine = create_engine('sqlite:///energy_storage.db')

global_map_data_cleaned.to_sql('table1', con=engine, if_exists='replace', index=False)

cumulative_sum_data_cleaned.to_sql('table2', con=engine, if_exists='replace', index=False)

#sample sql query
with engine.connect() as conn:
    # Fetch samples using SQL queries from the renamed tables
    table1_sample = pd.read_sql_query("SELECT * FROM table1 LIMIT 5", conn)
    table2_sample = pd.read_sql_query("SELECT * FROM table2 LIMIT 5", conn)

    print("\nSample data from `table1`:")
    print(table1_sample)

    print("\nSample data from `table2`:")
    print(table2_sample)
