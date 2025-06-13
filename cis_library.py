import requests
from dotenv import load_dotenv
import os
import time
import pandas as pd
import mysql.connector
from mysql.connector import Error
import random
import string
random.seed(100)

load_dotenv()

API_KEY = os.getenv('API_KEY')

MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_USERNAME=os.getenv('MYSQL_USERNAME')
MYSQL_DATABASE=os.getenv('MYSQL_DATABASE')
MYSQL_HOST=os.getenv('MYSQL_HOST')

headers = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": "cis-automotive.p.rapidapi.com"
}

#API urls
brands_url = "https://cis-automotive.p.rapidapi.com/getBrands"
regions_url = "https://cis-automotive.p.rapidapi.com/getRegions"
price_url = "https://cis-automotive.p.rapidapi.com/salePrice"
list_url = "https://cis-automotive.p.rapidapi.com/listPrice"
top_models_url = "https://cis-automotive.p.rapidapi.com/topModels"


def get_all_top_models():
    # List of regions
    regions = ["REGION_CENTRAL_NEW_YORK"]
    regions1 = [
        "REGION_CENTRAL_NEW_YORK",
        "REGION_EAST_NEW_YORK",
        "REGION_SOUTHERN_NEW_YORK",
        "REGION_STATE_NY",
        "REGION_WEST_NEW_YORK",
        "REGION_NORTHERN_CALIFORNIA",
        "REGION_SOUTHERN_CALIFORNIA",
        "REGION_STATE_CA",
        "REGION_STATE_VA",
        "REGION_STATE_TX",
        "REGION_STATE_IL",
        "REGION_STATE_FL",
    ]
    all_regions_dfs = [] # Empty list for storing dataframes
   
    for region in regions: # Loop through each region
        print(f"Fetching data for {region}...")
       
        querystring = {"regionName": region} # Query parameters
        response = requests.get(top_models_url, headers=headers, params=querystring) # Make API request
       
        if response.status_code == 200: # Check if request was successful
            region_data = response.json() # Parse JSON response
           
            if 'data' in region_data and region_data['data']: # Create dataframe from the data
                df = pd.DataFrame(region_data['data'])
                df['Region'] = region # Add region column
               
                all_regions_dfs.append(df) # Add to list of dataframes
               
                print(f"Successfully added data for {region}")
                time.sleep(1.1) # Add delay to avoid hitting rate limits
            else:
                print(f"No data available for {region}")
        else:
            print(f"Failed to get data for {region}. Status code: {response.status_code}")
        time.sleep(1.1) # Add delay to avoid hitting rate limits
   
    # Combine all dataframes
    if all_regions_dfs:
        combined_df = pd.concat(all_regions_dfs, ignore_index=True)
        print(f"Successfully created combined dataframe with {len(combined_df)} rows")
        
        # Generate unique ID for each row
        def generate_unique_id(row):
            model_first_letter = row['modelName'][0].upper() if len(row['modelName']) > 0 else 'X'
            brand_first_letter = row['brandName'][0].upper() if len(row['brandName']) > 0 else 'X'
            random_numbers = ''.join(random.choices(string.digits, k=8))
            return f"{model_first_letter}{brand_first_letter}{random_numbers}"
        
        # Apply the function to create the unique ID column
        combined_df['id'] = combined_df.apply(generate_unique_id, axis=1)
        
        # Reorder columns to put ID first
        columns = combined_df.columns.tolist()
        columns.remove('id')
        combined_df = combined_df[['id'] + columns]
        
        # Rename columns
        df = combined_df.rename(columns={
            'percentOfTopSales': 'top_sales_percentage',
            'modelName': 'model_name',
            'brandName': 'brand',
            'percentOfBrandSales': 'brand_sales_percentage',
            'brandMarketShare': 'brand_market_share',
            'Region' : 'region'
        })
        
        # Format percentage columns
        df['top_sales_percentage'] = df['top_sales_percentage'].map('{:,.2f}'.format)
        df['brand_sales_percentage'] = df['brand_sales_percentage'].map('{:,.2f}'.format)
        
        return df
    else:
        print("No data available for any region")
        return None
    
def create_price_df(top_models_df, url, max_rows=None):
    sale_price_data = [] # Create an empty list to store the price data

    if max_rows is not None: # Determine how many rows to process
        rows_to_process = top_models_df.head(max_rows).index
    else:
        rows_to_process = top_models_df.index

    total_rows = len(rows_to_process)     # Track progress
    print(f"Processing {total_rows} models...")
   
    queried_combinations = {} # Store brands and regions we've already queried to avoid redundant API calls
   
    for index in rows_to_process: # Process each row
        try: # Get the model data and ID
            model_name = top_models_df.at[index, 'model_name']
            brand_name = top_models_df.at[index, 'brand']
            region = top_models_df.at[index, 'region']
            id = top_models_df.at[index, 'id']
           
            print(f"Processing {list(rows_to_process).index(index)+1}/{total_rows}: {brand_name} {model_name} in {region}")
           
            # Check if we've already queried this brand/region combination
            brand_region_key = f"{brand_name}_{region}"
            if brand_region_key in queried_combinations:
                price_data = queried_combinations[brand_region_key]
            else:
                # Get sale price data for the brand in the region
                querystring = {"brandName": brand_name, "regionName": region}
               
                # Add delay before API call
                time.sleep(1.1)
                response = requests.get(url, headers=headers, params=querystring)
               
                if response.status_code == 200:
                    price_data = response.json()
                    # Store this response to avoid redundant API calls
                    queried_combinations[brand_region_key] = price_data
                else:
                    print(f"Failed to get price data. Status code: {response.status_code}")
                    continue
           
            if 'data' in price_data and price_data['data']: # Check if we have data
                # Find the price data for our specific model
                model_found = False
                for item in price_data['data']:
                    if item.get('name') == model_name:
                        # Create a dictionary with the price data and model info
                        model_price_data = {
                            'id': id,
                            'average_price': item.get('average'),
                            'median_price': item.get('median'),
                            'standard_deviation': item.get('stdDev'),
                            'price_variance': item.get('pVariance')
                        }
                        
                        sale_price_data.append(model_price_data) # Add to our list of data
                        model_found = True
                        print(f"  Found price data for {model_name}")
                        break
               
                if not model_found:
                    print(f"No price data found for {model_name}")
            else:
                print(f"No data available for {brand_name} in {region}")
       
        except Exception as e:
            print(f"Error processing row {index}: {e}")
   
    if sale_price_data: # Create a new dataframe from the collected sale price data
        price_df = pd.DataFrame(sale_price_data)
        print(f"Created price dataframe with {len(price_df)} rows")
        return price_df
    else:
        print("No sale price data found")
        return pd.DataFrame()  # Return empty dataframe

def create_db_connection():
    """
    Establish a connection to the MySQL database
    """
    db_connection = None
    try:
        db_connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USERNAME,
            passwd=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        print("MySQL Database connection successful ✅")

    except Error as e:
        print(f"❌ [DATABASE CONNECTION ERROR]: '{e}'")

    return db_connection

def create_top_models_table(db_connection):
    CREATE_TABLE_SQL_QUERY = """
    CREATE TABLE IF NOT EXISTS top_models (
        `id` VARCHAR(255),
        `top_sales_percentage` DOUBLE, 
        `model_name` VARCHAR(255),
        `brand` VARCHAR(255),
        `brand_sales_percentage` DOUBLE,
        `brand_market_share` DOUBLE,
        `region` VARCHAR(255), 
        PRIMARY KEY (`id`)
    );
    """
    try:
        cursor = db_connection.cursor()
        cursor.execute(CREATE_TABLE_SQL_QUERY)
        db_connection.commit()
        print("Table created successfully ✅")

    except Error as e:
        print(f"❌ [CREATING TABLE ERROR]: '{e}'")

def create_sale_prices_table(db_connection):
    CREATE_TABLE_SQL_QUERY = """
    CREATE TABLE IF NOT EXISTS sale_prices (
        `id` VARCHAR(255),
        `average_price` DOUBLE,
        `median_price` INT,
        `standard_deviation` DOUBLE,
        `price_variance` DOUBLE,
        PRIMARY KEY (`id`)
    );
    """#INT Does not work with % type variables which caused our errors from before
    try:
        cursor = db_connection.cursor()
        cursor.execute(CREATE_TABLE_SQL_QUERY)
        db_connection.commit()
        print("Sale Prices Table created successfully ✅")

    except Error as e:
        print(f"❌ [CREATING TABLE ERROR]: '{e}'")

def create_list_prices_table(db_connection):
    CREATE_TABLE_SQL_QUERY = """
    CREATE TABLE IF NOT EXISTS listed_prices (
        `id` VARCHAR(255),
        `average_price` DOUBLE,
        `median_price` INT,
        `standard_deviation` DOUBLE,
        `price_variance` DOUBLE,
        PRIMARY KEY (`id`)
    );
    """#INT Does not work with % type variables which caused our errors from before
    try:
        cursor = db_connection.cursor()
        cursor.execute(CREATE_TABLE_SQL_QUERY)
        db_connection.commit()
        print("List Prices Table created successfully ✅")

    except Error as e:
        print(f"❌ [CREATING TABLE ERROR]: '{e}'")    

def insert_top_models(db_connection, df):
    cursor = db_connection.cursor()
    INSERT_DATA_SQL_QUERY = """
    INSERT INTO top_models (`id`, `top_sales_percentage`, `model_name`, `brand`, `brand_sales_percentage`, `brand_market_share`, `region`)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `top_sales_percentage` = VALUES(`top_sales_percentage`),
        `brand_sales_percentage` = VALUES(`brand_sales_percentage`),
        `brand_market_share` = VALUES(`brand_market_share`)
    """
    # Create a list of tuples from the dataframe values
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]
    # Execute the query
    cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
    db_connection.commit()
    print("Data inserted or updated successfully for Top Models ✅")

def insert_sale_prices(db_connection, df):
    cursor = db_connection.cursor()
    INSERT_DATA_SQL_QUERY = """
    INSERT INTO sale_prices (`id`, `average_price`, `median_price`, `standard_deviation`, `price_variance`)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `average_price` = VALUES(`average_price`),
        `median_price` = VALUES(`median_price`),
        `standard_deviation` = VALUES(`standard_deviation`),
        `price_variance` = VALUES(`price_variance`)
    """
    # Create a list of tuples from the dataframe values
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]
    # Execute the query
    cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
    db_connection.commit()
    print("Data inserted or updated successfully for sale prices ✅")

def insert_list_prices(db_connection, df):
    cursor = db_connection.cursor()
    INSERT_DATA_SQL_QUERY = """
    INSERT INTO listed_prices (`id`, `average_price`, `median_price`, `standard_deviation`, `price_variance`)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `average_price` = VALUES(`average_price`),
        `median_price` = VALUES(`median_price`),
        `standard_deviation` = VALUES(`standard_deviation`),
        `price_variance` = VALUES(`price_variance`)
    """
    # Create a list of tuples from the dataframe values
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]
    # Execute the query
    cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
    db_connection.commit()
    print("Data inserted or updated successfully for listed prices ✅")


def get_list(url):
    response = requests.get(url, headers=headers) # Make request to the API
    if response.status_code == 200: # Check if request was successful
        list_data = response.json() # Parse the JSON response
        if 'data' in list_data: # Check if 'data' key exists in the response
            items_list = list_data['data'] # Get the list of brand names
            print("Available brands:") # Print the list with numbering
            for i, item in enumerate(items_list):
                print(f"{i+1}. {item}")
            return items_list # Return the list in case you need it elsewhere
        else:
            print("No 'data' key found in the response")
            return []
    else:
        print(f"Failed to get brands. Status: {response.status_code}")
        return []    