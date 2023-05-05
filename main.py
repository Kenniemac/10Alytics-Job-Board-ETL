
# Import libraries
from time import sleep
from util import generate_schema, execute_sql
from etl import get_data_from_api, write_to_s3, load_to_redshift, move_files_to_processed_folder, transform_data

# Main method to run the pipeline
def main():
    bucket_name = 'crypto-price-data'
    raw_data_folder = 'raw_data'
    processed_data_folder = 'processed_data'
    redshift_table_name = 'crypto_price_data'
    counter = 0
    # A while loop to send 5 requests to the API
    while counter < 3:
        data = get_data_from_api() # Extract data from API
        transformed_data = transform_data(data) # Transform the data
        write_to_s3(transformed_data, bucket_name, raw_data_folder)
        counter+= 1
        sleep(10) # Wait 30 seconds before sending another request to the API
    print('API data pulled and written written to s3 bucket')
    #create_table_query = generate_schema(data, redshift_table_name) # generate ddl of target table
    #execute_sql(create_table_query) # Create a taget table for in Redshift
    #load_to_redshift(bucket_name, raw_data_folder, redshift_table_name)
    move_files_to_processed_folder(bucket_name, raw_data_folder, processed_data_folder)
    
main()