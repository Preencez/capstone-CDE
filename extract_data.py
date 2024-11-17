import boto3
import requests
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# Create the S3 client
s3 = boto3.client('s3', region_name='us-east-1')  # Ensure the region matches your S3 bucket's region

def fetch_country_data():
    url = "https://restcountries.com/v3.1/all"
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data, status code: {response.status_code}")
    
    data = response.json()
    return data

def process_data(data):
    # Process data to handle missing or inconsistent fields
    processed_data = []
    for country in data:
        country_info = {
            "name": country.get("name", {}).get("common", ""),
            "region": country.get("region", ""),
            "subregion": country.get("subregion", ""),
            "population": country.get("population", 0),
            # Add more fields as needed
        }
        processed_data.append(country_info)
    return processed_data

def save_to_s3(data, bucket_name, file_name):
    # Convert the data into a pandas DataFrame
    df = pd.DataFrame(data)

    # Convert the DataFrame into a PyArrow Table
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    
    # Write the table to Parquet format in memory
    pq.write_table(table, buffer, compression='SNAPPY')  # Add compression for smaller file size
    
    # Seek back to the start of the buffer before uploading
    buffer.seek(0)
    
    # Upload the file to S3
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.read())

def generate_filename():
    # Generate a unique filename with the current timestamp
    date_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"country_data_{date_str}.parquet"

# Fetch data, process it, and upload to S3
data = fetch_country_data()
processed_data = process_data(data)
file_name = generate_filename()
save_to_s3(processed_data, "country-raw-data-bucket", file_name)

print(f"Data successfully uploaded to S3 as {file_name}")
