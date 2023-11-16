from dateutil.parser import parse
import pandas as pd
import re
class DataCleaning:
    #Create a method called clean_user_data in the DataCleaning class which will perform the cleaning of the user data.
    #You will need clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
    def __init___(self):
        self.val = val
    def clean_user_data(df):
        # get dataframe
        # look for null values
        #fileName = "/Users/zafuabera/Documents/code/AiCoreEngineering/multinational-retail-data-centralisation/db_creds.yaml"
        #df = dextract.read_dbs_method(fileName)
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
            numeric_cols = ['date_payment_confirmed', 'expiry_date', 'card_number']
            card_data[numeric_cols] = card_data[numeric_cols].apply(pd.to_numeric, errors='coerce')

            # Convert date_payment to be in the same format
            card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], errors='coerce')
            card_data['date_payment_confirmed'] = card_data['date_payment_confirmed'].dt.strftime('%Y-%m-%d')

            card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], errors='coerce')
            card_data['expiry_date'] = card_data['expiry_date'].dt.strftime('%m-%Y')

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
            store_data = store_data.dropna(how = 'all')

            #check that the longitude and latitude is numerical
            numeric_cols = ['longitude', 'latitude', 'lat']
            store_data[numeric_cols] = store_data[numeric_cols].apply(pd.to_numeric, errors='coerce')

            #remove lat column as it is null
            store_data = store_data.drop('lat', axis=1)       

            return store_data

        except Exception as e:
            print(f"Error cleaning store data: {str(e)}")
            return pd.DataFrame()
        
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

            return cleaned_df

        except Exception as e:
            print(f"Error converting product weights: {str(e)}")
            return pd.DataFrame()
    
    def _clean_and_convert_weight(weight_str):
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
            if 'g' in weight_str.lower():
                # Convert grams to kilograms
                weight_value /= 1000
            elif 'ml' in weight_str.lower():
                # Use a 1:1 ratio for ml to g (rough estimate), then convert to kilograms
                weight_value /= 1000

            return weight_value

        except Exception as e:
            print(f"Error cleaning and converting weight: {str(e)}")
            return None
