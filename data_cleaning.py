from dateutil.parser import parse
import pandas as pd
import numpy as np
import re
from dateutil.parser import parse
class DataCleaning:
    #Create a method called clean_user_data in the DataCleaning class which will perform the cleaning of the user data.
    #You will need clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
    def __init___(self):
        self.val = val
    def clean_user_data(df):
        # drop na if all values in the row are na
        df.dropna(how = 'all') 
        # update the dates
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df['date_of_birth'] = df['date_of_birth'].dt.strftime('%Y-%m-%d')
        print("\n number of null values:\n")
        print(df.isnull().sum())
        # replace null values with default date 1990-01-01
        df['date_of_birth'].fillna('1900-01-01', inplace=True) 
        print("\nModified dataframe:\n")
        print(df['date_of_birth'])
        print("\nData types:\n")
        print(df.dtypes)
        return df
    def clean_card_data(card_data):
        """
        Clean card data by removing erroneous values, handling NULL values, and addressing formatting errors.

        Parameters:
        - card_data: Pandas DataFrame containing card data.

        Returns:
        - Cleaned Pandas DataFrame.
        """
        try:
            
            # Drop rows with NULL values
            card_data = card_data.dropna(how = 'all')

            #check that the date_payment_confirmed and expiry_date and card_number is numerical
            #numeric_cols = ['date_payment_confirmed', 'expiry_date', 'card_number']
            #card_data[numeric_cols] = card_data[numeric_cols].apply(pd.to_numeric, errors='coerce')

            # Convert date_payment to be in the same format
            card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], errors='coerce')
            card_data['date_payment_confirmed'] = card_data['date_payment_confirmed'].dt.strftime('%Y-%m-%d')

            card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], errors='coerce')
            card_data['expiry_date'] = card_data['expiry_date'].dt.strftime('%m-%Y')
            print(card_data.shape)
            card_data = card_data.drop_duplicates(subset=['card_number'])
            print('after clearing duplicates')
            print(card_data.shape)

            print(card_data.head(10))
            return card_data
        
        except Exception as e:
            print(f"Error cleaning card data: {str(e)}")
            return pd.DataFrame()
        
    def clean_store_data(store_data):
        """
        Clean store data by removing erroneous values, handling NULL values, and addressing formatting errors.

        Parameters:
        - store_data: Pandas DataFrame containing card data.

        Returns:
        - Cleaned Pandas DataFrame.
        """
        try:
            
            # Drop rows with NULL values
            store_data = store_data.dropna(how = 'all') # hashed out due to milestone 3 task 3

            #check that the longitude and latitude is numerical
            #numeric_cols = ['longitude', 'latitude']
            #store_data[numeric_cols] = store_data[numeric_cols].apply(pd.to_numeric, errors='coerce')      

            return store_data

        except Exception as e:
            print(f"Error cleaning store data: {str(e)}")
            return pd.DataFrame()
    @classmethod
    def convert_product_weights(self, products_data):
        """
        Clean and convert the weights in the products DataFrame to a standardized format (e.g., kg).

        Parameters:
        - products_df: Pandas DataFrame containing product data.

        Returns:
        - Cleaned Pandas DataFrame.
        """
        try:
            # Make a copy of the DataFrame to avoid modifying the original
            cleaned_df = products_data.copy()
            print('CLEANED_DF after making the copy')
            print(cleaned_df['weight'].head(5))

            # Clean and convert the 'weight' column
            cleaned_df['weight'] = cleaned_df['weight'].apply(self._clean_and_convert_weight)
            print('before returning convert_products_weight and returning')
            print(cleaned_df['weight'].head(5))

            return cleaned_df

        except Exception as e:
            print(f"Error converting product weights: {str(e)}")
            return pd.DataFrame()
    @classmethod
    def _clean_and_convert_weight(self, weight_str):
        """
        Clean and convert a single weight string to a standardized format (e.g., kg).

        Parameters:
        - weight_str: String representing the weight.

        Returns:
        - Float representing the converted weight.
        """
        try:
            # Extract numeric values from the string
            numeric_values = re.findall(r'\d+\.?\d*', str(weight_str))

            if not numeric_values:
                return None

            # Convert to float
            weight_value = float(numeric_values[0])

            # Check for unit indicators and convert to kg
            if 'g' in weight_str.lower() and 'k' not in weight_str.lower():
                # Convert grams to kilograms
                weight_value /= 1000
            elif 'ml' in weight_str.lower():
                # Use a 1:1 ratio for ml to g (rough estimate), then convert to kilograms
                print('print value that contains for ml')
                print(weight_str.lower())
                weight_value /= 1000
                print('print new value')
                print(weight_value)
                print(type(weight_value))

            return weight_value

        except Exception as e:
            print(f"Error cleaning and converting weight: {str(e)}")
            return None
    
    def clean_products_data(products_data):
        """
        Clean product data by removing erroneous values, handling NULL values, and addressing formatting errors.
        Parameters:
        - products_data: Pandas DataFrame containing card data.
        Returns:
        - Cleaned Pandas DataFrame.
        """
        try: 
            # Drop rows with NULL values
            products_data = products_data.dropna(how = 'any')
            #check that the weight is numerical
            numeric_cols = ['weight']
            products_data[numeric_cols] = products_data[numeric_cols].apply(pd.to_numeric, errors='coerce')
            # Convert date_payment to be in the same format
            ## need to use the parse function from the dateutil library, in conjunction with the .apply method to format
            ## the date '2018 October 22' correctly
            #products_data['date_added'] = pd.to_datetime(products_data['date_added'], errors='coerce')
            #products_data['date_added'] = products_data['date_added'].dt.strftime('%Y-%m-%d')
            print(products_data.info())
            print('before parsing')
            #dff = products_data['date_added'].select_dtypes(include=[np.float])
            #print(dff)
            date_format = "%yyyy%mm%dd"
            #Drop errorenous rows
            print(products_data.shape)
            products_data = products_data[products_data.date_added != 'CCAVRB79VV']
            products_data = products_data[products_data.date_added != '09KREHTMWL']
            products_data = products_data[products_data.date_added != 'PEPWA0NCVH']
            print(products_data.shape)
            #pd.to_datetime(products_data["date_added"], format='mixed')
            products_data['date_added'] = products_data['date_added'].apply(parse)
            products_data['date_added'] = pd.to_datetime(products_data['date_added'], errors='coerce', format='mixed')
            
            return products_data

        except Exception as e:
            print(f"Error cleaning products data: {str(e)}")
            return pd.DataFrame()
    def clean_orders_data(orders_data):
        #remove column 1 as it is null
        orders_data = orders_data.drop('1', axis=1) 
        # remove the columns, first_name, last_name as they repeat in the table
        # axis =1 is column , axis=0 for rows
        orders_data = orders_data.drop('first_name', axis=1) 
        orders_data = orders_data.drop('last_name', axis=1) 
        return orders_data
    
    def clean_date_times_data(date_times):
        date_times = date_times.dropna(how = 'all')
        numeric_cols = ['month', 'year', 'day']
        date_times[numeric_cols] = date_times[numeric_cols].apply(pd.to_numeric, errors='coerce')
        date_times = date_times[date_times.timestamp != 'SAAZHF87TI']  
        date_times = date_times[date_times.timestamp != '75E4ECDVH6']  
        date_times = date_times[date_times.timestamp != 'NULL']  
        date_times = date_times[date_times.timestamp != 'JUVMW8TKUC'] 
        date_times = date_times[date_times.timestamp != 'J8CSDZCCRZ'] 
        return date_times