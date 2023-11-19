from io import StringIO
from database_utils import DatabaseConnector as dconnect
from data_cleaning import DataCleaning as dclean
import pandas as pd
import tabula
import requests
import numpy as np
import boto3
import ntpath
import sys
from sqlalchemy import create_engine
from botocore.exceptions import NoCredentialsError, ClientError
from boto3.exceptions import S3UploadFailedError

#Develop a method inside your DataExtractor class to read the data from the RDS database.
# TO DO: modularise the cleaning actions and make them general for all databases
class DataExtractor:
    def __init___(self, file_name):
        self.file_name = file_name
    @classmethod
    def read_dbs_method(self, file_name, table_name):
        # It will take in an instance of your DatabaseConnector class and the table name as an argument and return a pandas DataFrame.
        # Use your list_db_tables method to get the name of the table containing user data.
        # Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
        #db = data_cleaning.DatabaseConnector()
        yaml_object = dconnect.read_db_creds(file_name)
        engine = dconnect.init_db_engine(yaml_object)
        db_list = dconnect.list_db_tables(engine)   
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        df = pd.read_sql_query('select * from '+table_name,con=engine)
        engine.dispose()
        engine.clear_compiled_cache()
        print(df.head())
        print(df.info())
        return df
    def retrieve_pdf_data(self, pdf_link):
        """
        Extracts data from a PDF and returns a Pandas DataFrame.

        Parameters:
        - pdf_link: URL or local file path of the PDF document.

        Returns:
        - Pandas DataFrame containing the extracted data.
        """
        try:
            # Use tabula to extract tables from the PDF
            df_list = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)

            # Concatenate all DataFrames into a single DataFrame
            extracted_data = pd.concat(df_list, ignore_index=True)

            return extracted_data
        except Exception as e:
            print(f"Error extracting data from PDF: {str(e)}")
            return pd.DataFrame()
    def list_number_of_stores(self, endpoint, headers):
        """
        Retrieve the number of stores to extract from a web API.

        Parameters:
        - endpoint: The API endpoint that provides the number of stores information.
        - headers: A dictionary containing any required headers for the API request.

        Returns:
        - Number of stores to extract (integer).
        """
        try:
            # Make a GET request to the API endpoint
            response = requests.get(endpoint, headers=headers)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # the API response is in JSON format and contains the number of stores information
                data = response.json()
                print(data)

                # Extract the number of stores from the API response
                num_stores = data.get('number_stores', 0)

                return num_stores
            else:
                print(f"Error retrieving number of stores. Status code: {response.status_code}")
                return 0

        except Exception as e:
            print(f"Error retrieving number of stores: {str(e)}")
            return 0
    def retrieve_stores_data(self, endpoint, headers):
        """
        Retrieve the stores data, given a store number from a web API.

        Parameters:
        - endpoint: The API endpoint that provides the store information.
        - headers: A dictionary containing any required headers for the API request.

        Returns:
        - dataframe.
        """
        try:
            df = pd.DataFrame()
            list = []
            for i in range(0,451):
                # Make a GET request to the API endpoint
                print(i)
                response = requests.get(endpoint+str(i), headers=headers)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # the API response is in JSON format and contains the number of stores information
                    data = response.json()
                    print(data)
                    print(type(data))
                    # append dict to list
                    list.append(data)
                    print('print a list')
                    print(list)
                else:
                    print(f"Error retrieving store data. Status code: {response.status_code}")
            # convert list of dictionaries into a dataframe
            df = pd.DataFrame.from_dict(list) 
            print(df.head(5))
            return df
        except Exception as e:
            print(f"Error retrieving store data: {str(e)}")
            return 0
    @classmethod
    def _parse_s3_address(self, s3_address):
        """
        Parse the S3 address into bucket name and object key.

        Parameters:
        - s3_address: The S3 address (e.g., 's3://bucket_name/object_key').

        Returns:
        - Tuple (bucket_name, object_key).
        """
        # Remove 's3://' prefix and split into bucket name and object key
        s3_parts = s3_address.replace('s3://', '').split('/')
        bucket_name = s3_parts[0]
        object_key = '/'.join(s3_parts[1:])
        return bucket_name, object_key
    @classmethod   
    def extract_from_s3(self, address, type):
        try:
             # Split the S3 address into bucket name and object key
            bucket_name, object_key = self._parse_s3_address(address)

            # Initialize a Boto3 S3 client
            s3_client = boto3.client('s3')

            # Download the object from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            data = response['Body'].read().decode('utf-8')
            print('the type of date is ')
            #print(type(data))

            # Read the CSV data into a Pandas DataFrame
            if type == 'csv':
                df = pd.read_csv(StringIO(data))
            else:
                #assume json
                df = pd.read_json(StringIO(data))
            #print(df['weight'].head(5))
            print(df.head(5))
            return df
        except NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print('Oops no buckets exist')
            else:
                print('error message occured:',e)



fileName = "/Users/zafuabera/Documents/code/AiCoreEngineering/multinational-retail-data-centralisation/db_creds.yaml"
de = DataExtractor()
"""

headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
df = de.retrieve_stores_data(endpoint, headers)

df = dclean.clean_store_data(df)
dconnect.upload_to_db(df,'dim_store_details')

"""
# milestone 3 Task 1



"""milestone 2 Task 8
url = 's3://data-handling-public/date_details.json'
df = de.extract_from_s3(url, 'json')
df = dclean.clean_date_times_data(df)
print('before printing the df')
dconnect.upload_to_db(df,'dim_date_times')
"""
# milestone 2 Task 7 
#orders_table = de.read_dbs_method(fileName, "orders_table")
#orders_table = dclean.clean_orders_data(orders_table)
#dconnect.upload_to_db(orders_table,'orders_table')


#Task 6
#address = 's3://data-handling-public/products.csv'
#df = de.extract_from_s3(address, 'csv')
#print('after extracting from s3')
#print(df.head(10))
#print(df.info())
#print(df.describe())
#products_data = dclean.convert_product_weights(df)
#print(products_data.head(5))
#print(products_data.info())
#print(products_data.describe())
#products_data = dclean.clean_products_data(products_data)
#dconnect.upload_to_db(products_data,'dim_products')

#print('after converting the weight values')
#print(df['weight'].head(5))

'''
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
df = de.retrieve_stores_data(endpoint, headers)

df = dclean.clean_store_data(df)
dconnect.upload_to_db(df,'dim_store_details')
print(df.head(10))
print(df.info())
print(df.describe())
'''
#yaml_object = dconnect.read_db_creds(fileName)
#engine_1 = dconnect.init_db_engine(yaml_object)
#db_list = dconnect.list_db_tables(engine_1)
#df = de.read_dbs_method(fileName)
#df = dclean.clean_user_data(df)
#dconnect.upload_to_db(df,'dim_users')
pdf_link = "/Users/zafuabera/Downloads/card_details.pdf"
card_data = de.retrieve_pdf_data(pdf_link)
card_data = dclean.clean_card_data(card_data)
dconnect.upload_to_db(card_data,'dim_card_details')
#endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
#headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
#num_stores = de.list_number_of_stores(endpoint, headers)
#print(num_stores)

"""
-- Alter the table to change data types
-- Assuming UUID is a standard UUID type, and VARCHAR length is 255 for card_number, store_code, and product_code
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(255),
    ALTER COLUMN product_code TYPE VARCHAR(255),
    ALTER COLUMN product_quantity TYPE SMALLINT;
    """
