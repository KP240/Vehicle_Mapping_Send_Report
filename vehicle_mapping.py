import os
import shutil
import pandas as pd
from sqlalchemy import create_engine
import requests
from io import StringIO
from simple_salesforce import Salesforce
from dotenv import load_dotenv  # Add this line to load environment variables

# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection details
pg_host = '34.100.223.97'  # Consider putting this in the .env file
pg_port = '5432'            # Consider putting this in the .env file
pg_database = 'master_prod' # Consider putting this in the .env file
pg_username = os.getenv('PG_USERNAME')  # Load from .env
pg_password = os.getenv('PG_PASSWORD')  # Load from .env

# Create SQLAlchemy engine
pg_engine = create_engine(f'postgresql+psycopg2://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_database}')

# SQL query to fetch data from PostgreSQL
query = """
SELECT
    lithium_id AS SP_id,
    name AS SP_Name,
    mobile_number,
    driving_license_number AS License_No,
    Status,
    city_code,
    billing_model AS Model,
    assigned_vehicle AS Assigned_Vehicle,
    revenue_model AS Revenue_Model,
    area_of_residence AS Address
FROM
    etms_service_providers
WHERE
    billing_model IN ('16 Hour Model', 'Trip Model')
    AND status = 'Active'
"""

# Load data into DataFrame from PostgreSQL
df_pg = pd.read_sql(query, pg_engine)

# Close the engine connection
pg_engine.dispose()

# Salesforce connection details
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),  # Load from .env
    password=os.getenv('SF_PASSWORD'),  # Load from .env
    security_token=os.getenv('SF_SECURITY_TOKEN')  # Load from .env
)

sf_instance = 'https://project-lithium.my.salesforce.com/'
report_id = '00OC5000000Kks4MAC'
export = '?isdtp=p1&export=1&enc=UTF-8&xf=csv'
sf_url = sf_instance + report_id + export

# Fetch Salesforce report
response = requests.get(sf_url, headers=sf.headers, cookies={'sid': sf.session_id})
download_report = response.content.decode('utf-8')

# Load Salesforce report into DataFrame
df_sf = pd.read_csv(StringIO(download_report))

# Renaming columns from Salesforce DataFrame for easier access
df_sf.rename(columns={
    'Lithium ID': 'sp_id',
    'Service Provider: Service Provider Name': 'SP_Name',
    'SP Current Status': 'Current_Status',
    'SP\'s Mobile No': 'Mobile_Number',
    'Status': 'SP_Status',
    'SP Model': 'SP_Model',
    'Campus Name': 'Campus_Name'
}, inplace=True)

# Merging DataFrames on SP_id
df_merged = pd.merge(df_pg, df_sf[['sp_id', 'Current_Status']],
                     on='sp_id', how='left')

# Define cities folder path
cities_folder = 'cities'

# Delete the existing 'cities' folder if it exists
if os.path.exists(cities_folder):
    shutil.rmtree(cities_folder)

# Create a new cities folder
os.makedirs(cities_folder, exist_ok=True)

# Get unique cities from df_merged
unique_cities = df_merged['city_code'].unique()

# Filter df_merged data for each city and save to respective folders
for city in unique_cities:
    # Create a folder for each city
    city_folder = os.path.join(cities_folder, city)
    os.makedirs(city_folder, exist_ok=True)

    # Filter df_merged for the current city and active status
    city_data = df_merged[(df_merged['city_code'] == city) & (df_merged['Current_Status'] == 'Active')]

    # Save to CSV file in the respective city folder
    if not city_data.empty:  # Only save if there is data to save
        file_path = os.path.join(city_folder, f'{city}_data.csv')
        city_data.to_csv(file_path, index=False)
        print(f"Saved data for {city} to {file_path}")

# Finished processing
print("All city data has been processed and saved.")
