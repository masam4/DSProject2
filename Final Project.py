import os
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery

# Step 1: Authenticate with Service Account
SERVICE_ACCOUNT_KEY = r"C:\Users\obrie\Documents\Local_Python\DS_2001_Project_2\ds-2001-final-project-ec3f2dea8e7b.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY

# Step 2: Read in CSVs
global_map_data = pd.read_csv('Global_Map_Full_Data_data.csv')
cumulative_sum_data = pd.read_csv('Cummulative_Sum_by_Year_Full_Data_data.csv')

# Step 3: Validate Columns
expected_columns_global_map = [
    "Project/Plant Name", "Rated Capacity (kWh)", "Rated Power (kW)",
    "Discharge Duration at Rated Power (hrs)", "Storage Capacity (kWh)"
]

expected_columns_cumulative_sum = [
    "Year", "Subsystem 1 - Technology Broad Category",
    "Subsystem 1 - Technology Mid-Type", "Subsystem 1 - Technology Sub-Type",
    "Rated Power (kW)", "Storage Capacity (kWh)", "Country.1"
]

def validate_columns(df, expected_columns, dataset_name):
    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"{dataset_name} is missing expected columns: {missing_columns}")
    print(f"All expected columns are present in {dataset_name}.")

validate_columns(global_map_data, expected_columns_global_map, "Global Map Data")
validate_columns(cumulative_sum_data, expected_columns_cumulative_sum, "Cumulative Sum Data")

# Step 4: Rename Columns
global_map_data.rename(columns={
    "Project/Plant Name": "project_name",
    "Rated Capacity (kWh)": "rated_capacity_kwh",
    "Rated Power (kW)": "rated_power_kw",
    "Discharge Duration at Rated Power (hrs)": "discharge_duration_hrs",
    "Storage Capacity (kWh)": "storage_capacity_kwh"
}, inplace=True)
global_map_data_cleaned = global_map_data.drop_duplicates()

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

cumulative_sum_data_cleaned["storage_capacity_kwh"] = cumulative_sum_data_cleaned["storage_capacity_kwh"].fillna(0)

# 5 create a bucket in the project to store these newly made json files
# also prepare the files to be stored in big query
BUCKET_NAME = "ds-2001-final-bucket"
PROJECT_ID = "ds-2001-final-project"
DATASET_ID = "ABC123"

global_map_json_file = "global_map_data.json"
cumulative_sum_json_file = "cumulative_sum_data.json"

FILES_TO_UPLOAD = [
    (global_map_json_file, "global_map_data.json"),
    (cumulative_sum_json_file, "cumulative_sum_data.json"),
]


client = storage.Client()
try:
    bucket = client.create_bucket(BUCKET_NAME)
    print(f"Bucket {BUCKET_NAME} created.")
except Exception as e:
    print(f"Bucket creation error (if it already exists, this is safe to ignore): {e}")


# 6 upload the files to google cloud

for local_file, cloud_file in FILES_TO_UPLOAD:
    try:
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(cloud_file)
        blob.upload_from_filename(local_file)
        print(f"File {local_file} uploaded to {cloud_file} in bucket {BUCKET_NAME}.")
    except Exception as e:
        print(f"Failed to upload {local_file} to GCS: {e}")

# Step 7, load JSON files into BigQuery
client = bigquery.Client()
TABLES = {
    "global_map_data": f"gs://{BUCKET_NAME}/global_map_data.json",
    "cumulative_sum_data": f"gs://{BUCKET_NAME}/cumulative_sum_data.json",
}

for table_name, file_path in TABLES.items():
    try:
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_uri(file_path, table_id, job_config=job_config)
        load_job.result()
        print(f"Loaded data into BigQuery table {table_id}.")
    except Exception as e:
        print(f"Failed to load data into BigQuery table {table_name}: {e}")


print("All data uploaded to BigQuery successfully!")
