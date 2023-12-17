import logging
import json
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from pyspark.sql import SparkSession

def upload_data(acess_token: str, 
               container: str, 
               path: str, 
               data):
    '''
    Function to save a data into a Azure blobstorage container
    input:
        - acess_token: acess token to enter the azure storage
        - container: container name that the folder will be saved
        - path: path and file name that the file should be saved
        - data: the information that will be saved
    
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
        - acess_token: acess token to enter the azure storage
        - container: container name that the folder will be saved
        - file_name: path and file name that need to read
    
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


def files_from_blob(access_token: str, container: str):
    '''
    Function to read the name of all files from a container in blob
    input:
        - ACCESS_TOKEN: access token
        - container: container name
    output
        - blobs: list containing all the paths on the container
    '''
    # Connecting in the blob storage
    blob_service_client = BlobServiceClient.from_connection_string(access_token)
    container_client = blob_service_client.get_container_client(container)
    blobs = container_client.list_blobs()
    return blobs

def read_parquet(container: str, 
                 account_name: str, 
                 access_key: str,
                 path: str):
    '''
    Function to read a parquet file
    input
        - container: container where the file is
        - account_name: account name
        - access_key: access_key
        - path: path that the file is on the container
    output:
        - df: the parquet in a pandas dataframe
    '''
    
    # creating a spark cluster with the access on the blob storage
    spark = SparkSession.builder \
        .appName("Grouping the data") \
        .config("fs.azure.account.key.{0}.blob.core.windows.net".format(account_name), access_key) \
        .getOrCreate()

    parquet_path = f"wasbs://{container}@{account_name}.blob.core.windows.net/{path}"
    df = spark.read.parquet(parquet_path)
    #tranforming into pandas
    df = df.toPandas()
    return df

def save_parquet(df: pd.DataFrame,
                 access_token: str, 
                 container: str, 
                 path: str):
    '''
    Function to save a parquet into a container
    input:
        - df: pandas to be saved
        - access_token: access_token
        - container: container where the file will be
        - path: path to put the file
    output:
        no output
    '''
    buffer = io.BytesIO()
    df.to_parquet(buffer)
    upload_data(access_token, container, path, buffer.getvalue())




