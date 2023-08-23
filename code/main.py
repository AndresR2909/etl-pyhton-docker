import logging
import time
import traceback


from config.config import get_config
from utils.utils import setup_logging

from etl.extract import extract_data, extract_data_from_azure_sa
from etl.transform import transform_data
from etl.load  import load_data, load_data_to_azure_sa

def handle_error(error):
    logging.error(error)
    logging.error(traceback.format_exc())

def run():

    setup_logging()

    config  = get_config()
    input_file = config.get('DATA','INPUT_FILE')
    output_file = config.get('DATA','OUTPUT_FILE')

    sa_connection_str = config.get('AZURE-SA','CONNECTION_STRING')
    sa_input_container = config.get('AZURE-SA','INPUT_CONTAINER_NAME')
    sa_input_blob = config.get('AZURE-SA','INPUT_BLOB_NAME')
    sa_output_container = config.get('AZURE-SA','OUTPUT_CONTAINER_NAME')
    sa_output_blob = config.get('AZURE-SA','OUTPUT_BLOB_NAME')
    
    logging.info("ETL process started.")
    start_time = time.time()

    try:
        # Call extract
        #data = extract_data(input_file)
        data = extract_data_from_azure_sa(sa_connection_str, sa_input_container, sa_input_blob)

    
        # Call transform
        data_transformed = transform_data(data)

        # Call load
        #load_data(data_transformed, output_file)
        load_data_to_azure_sa(sa_connection_str, sa_output_container, sa_output_blob, data_transformed)

        logging.info(f"ETL process completed.")
    except Exception as e:
        handle_error(e)
        logging.info(f"ETL process stoped.")

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Elapsed time: {elapsed_time:.2f} seconds.")
    
if __name__ == '__main__':
    run()