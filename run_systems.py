import cis_library
import time

regions_url = "https://cis-automotive.p.rapidapi.com/getRegions"

get_list(regions_url)

'''#For convenience of loading into our function that creates a price dataframe, our API urls for the sold and listed prices are called here in our main system
sale_price_url = "https://cis-automotive.p.rapidapi.com/salePrice"
list_price_url = "https://cis-automotive.p.rapidapi.com/listPrice"

'''
#Creating our dataframes

'''
top_models_df = cis_library.get_all_top_models() #This creates our dataframe of the most popular car models from each region. This is convenient to create because it immediately provides the top 25 selling cars per region
time.sleep(1.1)
sale_price_df = cis_library.create_price_df(top_models_df, sale_price_url, max_rows=10) #This will create a dataframe of descriptive statistics for vehicles sold among the most popular from top_models_df
time.sleep(1.1)
list_price_df = cis_library.create_price_df(top_models_df, list_price_url, max_rows=10) #This will create a dataframe of descriptive statistics for listed vehicle prices among the most popular from top_models_df

'''
#Establishing database connection, then creating tables within our database

'''
db_connection = cis_library.create_db_connection() #Establish our connection to the database. This function is contingent on the user creating a database and server in MYSQL
cis_library.create_top_models_table(db_connection)
cis_library.create_sale_prices_table(db_connection)
cis_library.create_list_prices_table(db_connection)

cis_library.insert_top_models(db_connection, top_models_df) #This will insert the data pulled from the API. It will also update other figures based on the latest data
cis_library.insert_sale_prices(db_connection, sale_price_df) 
cis_library.insert_list_prices(db_connection, list_price_df)
'''