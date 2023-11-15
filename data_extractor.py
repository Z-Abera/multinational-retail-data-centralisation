from database_utils import DatabaseConnector as dconnect
from data_cleaning import DataCleaning as dclean
import pandas as pd
import tabula
#Develop a method inside your DataExtractor class to read the data from the RDS database.
class DataExtractor:
    def __init___(self, file_name):
        self.file_name = file_name
    @classmethod
    def read_dbs_method(self, file_name):
        # It will take in an instance of your DatabaseConnector class and the table name as an argument and return a pandas DataFrame.
        # Use your list_db_tables method to get the name of the table containing user data.
        # Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
        #db = data_cleaning.DatabaseConnector()
        yaml_object = dconnect.read_db_creds(file_name)
        engine = dconnect.init_db_engine(yaml_object)
        db_list = dconnect.list_db_tables(engine)   
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        df = pd.read_sql_query('select * from "legacy_users"',con=engine)
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

fileName = "/Users/zafuabera/Documents/code/AiCoreEngineering/multinational-retail-data-centralisation/db_creds.yaml"
de = DataExtractor()
#yaml_object = dconnect.read_db_creds(fileName)
#engine_1 = dconnect.init_db_engine(yaml_object)
#db_list = dconnect.list_db_tables(engine_1)
#df = de.read_dbs_method(fileName)
#df = dclean.clean_user_data(df)
#dconnect.upload_to_db(df,'dim_users')
pdf_link = "/Users/zafuabera/Downloads/card_details.pdf"
card_data = de.retrieve_pdf_data(pdf_link)
print(card_data)
card_data = dclean.clean_card_data(card_data)
dconnect.upload_to_db(card_data,'dim_card_details')


