# import libraries
from util import generate_schema, get_redshift_connection,\
execute_sql, list_files_in_folder
import pandas as pd
import requests
import boto3
from datetime import datetime
from io import StringIO
import psycopg2
import ast
from dotenv import dotenv_values
dotenv_values()


def get_data():
    url = "https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch/"
    headers = ast.literal_eval(config.get('HEADERS'))

    querystring = {
        "query": "Data Engineer in USA",
        "query": "Data Engineer in UK",
        "query": "Data Engineer in Canada",
        "query": "Data Analyst in USA",
        "query": "Data Analyst in UK",
        "query": "Data Analyst in Canada",
        "page":"1",
        "num_pages":"1",
        "date_posted":"today"

    }

    response = requests.request("GET", url, headers=headers, params=querystring).json()
    job_data = response.get('data').get('jobs')
    columns = ['employer_website', 'job_id', 'job_employment_type', 'job_title', 'job_apply_link', 'job_description', 'job_city',
               'job_country', 'job_posted_at_timestamp','employer_company_type']
    job_data = pd.DataFrame(job_data)[columns]
    return job_data


# Transform the data
def transform_data(data):
    data['price'] = data['price'].apply(lambda x: float(x)) # convert string column to float value
    return data

# Write data to S3 Bucket
def write_to_s3(data, bucket_name, folder):
    file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}" # Create a file name
    csv_buffer = StringIO() # Create a string buffer to collect csv string
    data.to_csv(csv_buffer, index=False) # Convert dataframe to CSV file and add to buffer
    csv_str = csv_buffer.getvalue() # Get the csv string
    # using the put_object(write) operation to write the data into s3
    s3_client.put_object(Bucket=bucket_name, Key=f'{folder}/{file_name}', Body=csv_str ) 

    
def load_to_redshift(bucket_name, folder, redshift_table_name):
    iam_role = config.get('IAM_ROLE')
    conn = get_redshift_connection()
    file_paths = [f's3://{bucket_name}/{file_name}' for file_name in list_files_in_folder(bucket_name, folder)]
    for file_path in file_paths:
        copy_query = f"""
        copy {redshift_table_name}
        from '{file_path}'
        IAM_ROLE '{iam_role}'
        csv
        IGNOREHEADER 1;
        """
        execute_sql(copy_query, conn)
    print('Data successfully loaded to Redshift')


def move_files_to_processed_folder(bucket_name, raw_data_folder, processed_data_folder):
    file_paths = list_files_in_folder(bucket_name, raw_data_folder)
    for file_path in file_paths:
        file_name = file_path.split('/')[-1]
        copy_source = {'Bucket': bucket_name, 'Key': file_path}
        # Copy files to processed folder
        s3_resource.meta.client.copy(copy_source, bucket_name, processed_data_folder + '/' + file_name)
        s3_resource.Object(bucket_name, file_path).delete()
    print("Files successfully moved to 'processed_data' folder in S3")

