import logging
import json
from azure.storage.blob import BlobServiceClient

def upload_data(acess_token: str, 
               container: str, 
               path: str, 
               data):
    '''
    Function to save a data into a Azure blobstorage container
    input:
        -acess_token: acess token to enter the azure storage
        -container: container name that the folder will be saved
        -path: path and file name that the file should be saved
        -data: the information that will be saved
    
    output:
        no output
    '''
    logger = logging.getLogger("save_pkl")
    logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")
    
    logger.info("Connecting to the blob storage.")
    blob_service_client = BlobServiceClient.from_connection_string(acess_token)
    
    logger.info("Check if the container exists")
    container_client  = blob_service_client.get_container_client(container)
    # Create the container if it doesn't exist
    if not container_client.exists():
        logger.info(f"Creating the container {container}")
        container_client.create_container()
        
    logger.info(f"Saving the file on the path: {path}")
    blob_client = blob_service_client.get_blob_client(container=container,blob=path)
    blob = blob_client.upload_blob(data=data)
    
    
def read_json(acess_token: str, 
               container: str, 
               file_name: str):
    '''
    Function to read a json file from a Azure blobstorage container
    input:
        -acess_token: acess token to enter the azure storage
        -container: container name that the folder will be saved
        -file_name: path and file name that need to read
    
    output:
        -json_data: json data
    '''
    logger = logging.getLogger("save_pkl")
    logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")
    
    logger.info("Connecting to the blob storage.")
    blob_service_client = BlobServiceClient.from_connection_string(acess_token)

        
    logger.info(f"Reading the file: {file_name}")
    container_client = blob_service_client.get_container_client(container)
    blob_client = container_client.get_blob_client(file_name)
    
    # Download the blob content as a string
    blob_content = blob_client.download_blob().readall()

    # Parse the JSON content
    json_data = json.loads(blob_content)

    return json_data