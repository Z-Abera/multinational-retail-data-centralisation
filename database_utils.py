import yaml
import json
import sqlalchemy as dbb
from sqlalchemy import create_engine
#import psycopg2
#/Users/zafuabera/Documents/code/AiCoreEngineering/multinational-retail-data-centralisation/db_creds.yaml
class DatabaseConnector:
    def __init___(self, fileName):
        self.fileName = fileName

    @classmethod
    def read_db_creds(self, fileName):
        with open(fileName, 'r') as yaml_in:
            yaml_object = yaml.safe_load(yaml_in) # yaml_object will be a list or a dict
            print(yaml_object)
            return yaml_object
    #Now create a method init_db_engine which will read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine.
    @classmethod
    def init_db_engine(self, yaml_object):
        #TO DO - construct the string programmatically from yaml file
        engine = dbb.create_engine('postgresql://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/postgres')
        #engine = db.create_engine('dialect+driver://user:pass@host:port/db')
        return engine
    #Using the engine from init_db_engine create a method list_db_tables to list all the tables in the database so you know which tables you can extract data from.
    @classmethod
    def list_db_tables(self, engine):
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        inspector = dbb.inspect(engine)
        inspector.get_table_names()
        print(inspector.get_table_names())
        engine.dispose()

    #Now create a method in your DatabaseConnector class called upload_to_db. 
    #This method will take in a Pandas DataFrame and table name to upload to as an argument.
    @classmethod
    def upload_to_db(self, df, table_name):
        """
        Uploads a Pandas DataFrame to a specified table in the database.

        Parameters:
        - df: Pandas DataFrame
        - table_name: Name of the table in the database
        """
        try:
            # TO DO extract this from the db_creds file
            db_username = 'postgres'
            db_password = 'Admin'
            db_host = 'localhost'
            db_port = '5432'
            db_name = 'sales_data'

            # SQLAlchemy engine creation
            db_url = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
            print(f"DataFrame just before uploading.")
            engine1 = create_engine(db_url)
            # upload to db
            print(f"DataFrame uploaded to {table_name} in the PostgreSQL database successfully.")
            df.to_sql(name=table_name, con=engine1, if_exists='replace', index=False)

            print(f"DataFrame uploaded to {table_name} successfully.")
        except Exception as e:
            print(f"Error uploading DataFrame to {table_name}: {str(e)}")






fileName = "/Users/zafuabera/Documents/code/AiCoreEngineering/multinational-retail-data-centralisation/db_creds.yaml"
db = DatabaseConnector()
#yaml_object = db.read_db_creds(fileName)
#engine_1 = db.init_db_engine(yaml_object)
#db_list = db.list_db_tables(engine_1)
#print(db_list)
