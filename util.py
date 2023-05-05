import boto3
import psycopg2
import pandas as pd
from dotenv import dotenv_values
dotenv_values()

# Get credentials from environment variable file
config = dotenv_values('.env')

# Create a boto3 s3 client for bucket operations
s3_client = boto3.client('s3')

# function to get Reshift data warehouse connection
def get_redshift_connection():
    user = config.get('USER')
    password = config.get('PASSWORD')
    host = config.get('HOST')
    database_name = config.get('DATABASE_NAME')
    port = config.get('PORT')
    conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
    return conn

def generate_schema(data, table_name):
    create_table_statement = f'CREATE TABLE IF NOT EXISTS {table_name}(\n'
    column_type_query = ''
    
    types_checker = {
        'INT':pd.api.types.is_integer_dtype,
        'VARCHAR':pd.api.types.is_string_dtype,
        'FLOAT':pd.api.types.is_float_dtype,
        'TIMESTAMP':pd.api.types.is_datetime64_any_dtype,
        'OBJECT':pd.api.types.is_dict_like,
        'ARRAY':pd.api.types.is_list_like,
    }
    for column in data: # Iterate through all the columns in the dataframe
        last_column = list(data.columns)[-1] # Get the name of the last column
        for type_ in types_checker:
            mapped = False
            if types_checker[type_](data[column]):
                mapped = True
                if column != last_column:
                    column_type_query += f'{column} {type_},\n'
                else:
                    column_type_query += f'{column} {type_}\n'
                break
        if not mapped:
            raise ('Type not found')
    column_type_query += ');'
    output_query = create_table_statement + column_type_query
    return output_query

def execute_sql(sql_query, conn):
    conn = get_redshift_connection()
    cur = conn.cursor()
    cur.execute(sql_query)
    conn.commit()
    cur.close() # Close cursor
    conn.close() # Close connection

def list_files_in_folder(bucket_name, folder):
    bucket_list = s3_client.list_objects(Bucket = bucket_name, Prefix = folder) # List the objects in the bucket
    bucket_content_list = bucket_list.get('Contents')
    files_list = [file.get('Key') for file in bucket_content_list][1:]
    return files_list
