import pandas as pd
import logging

from azure.storage.blob import BlobServiceClient

def load_data(data, file_name):
    """
    Load data in local directory
        
    """
    data.to_csv(file_name, index=False, sep='|')
    logging.info(f'Result file "{file_name}" saved!')

def load_data_to_azure_sa(connection_string, container_name, blob_name, data):

    """
    Load data in Azure storage account container.
    
    """

    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)

    csv_data = data.to_csv(index=False, sep='|')

    blob_client.upload_blob(csv_data, overwrite=True)