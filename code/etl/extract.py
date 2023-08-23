import pandas as pd
import logging

from io import StringIO
from azure.storage.blob import BlobServiceClient

def extract_data(input_file):
    """
    Get data from local file     

    """
    logging.info(f'Start reading file "{input_file}"')
    data = pd.read_csv(input_file, sep='|', low_memory=False)

    return data

def extract_data_from_azure_sa(connection_string, container_name, blob_name):

    """
    Get data from azure storage account     
        
    """

    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)
    
    # Get a reference to the blob
    blob_client = container_client.get_blob_client(blob_name)
    
    # Download the blob as a byte stream
    blob_file = blob_client.download_blob()

    file = blob_file.readall()
    content = file.decode('utf-8')
    fileString = StringIO(content)
            
    data = pd.read_csv(fileString, sep='|', low_memory=False)
    
    extension = blob_name.split('.')[-1]

    return data
