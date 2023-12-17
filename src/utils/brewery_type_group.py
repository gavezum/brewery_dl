from src.utils.azure_storage import  (upload_data, 
                                      files_from_blob, 
                                      read_parquet, 
                                      save_parquet)
import logging
import pandas as pd
import io 
from azure.storage.blob import BlobServiceClient, BlobClient
from pyspark.sql import SparkSession

def state_group(ACCESS_TOKEN: str, 
                SILVER_CONTAINER: str, 
                ACCOUNT_NAME: str,
                ACCESS_KEY: str, 
                GOLD_CONTAINER: str,
                DATE_PIPELINE: str ):  
    '''
    Function to group and save the breweries by type and state
    input
        - ACCESS_TOKEN: token to access the blob storage
        - SILVER_CONTAINER: container that has the tabular format
        - ACCOUNT_NAME: account name to be used to read the data
        - ACCESS_KEY: access key to the blob
        - GOLD_CONTAINER: container that the grouped data will be saved
        - DATE_PIPELINE: date to run the pipeline
    output:
        countries: set that contains the countries on the dataset
    '''
    # Connecting in the blob storage
    logger = logging.getLogger("grouping state")
    logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")

    logger.info("Reading the blobs")
    blobs = files_from_blob(ACCESS_TOKEN, SILVER_CONTAINER)

    #creating an empty list to track the countries
    countries = []

    # checking all the files on a blob storage
    logger.info("Grouping the states and producing the countries list")
    for blob in blobs:
        # removing the .parquet to get only the path
        path = blob.name.split('.')[0]
        # only read the datasets that are on the date pipeline
        if DATE_PIPELINE in path:
            #reading the parquet file into a pandas dataframe
            df = read_parquet(SILVER_CONTAINER, ACCOUNT_NAME, ACCESS_KEY, blob.name)
            brewery_type = df.groupby('brewery_type').size().reset_index().rename(columns = {0: 'quantity'})

            #spliting the path into different parts to be used during the saving
            path = path.split('/')
            countries.append(path[1])
            save_parquet(brewery_type, ACCESS_TOKEN, GOLD_CONTAINER, f'{path[0]}/{path[1]}/{path[2]}_brewery_type.parquet')

    # returning the set of the countries so we have only the unique values
    return set(countries)

def country_group(ACCESS_TOKEN: str, 
                  SILVER_CONTAINER: str, 
                  ACCOUNT_NAME: str, 
                  GOLD_CONTAINER: str,
                  ACCESS_KEY:str,
                  countries: list,
                  DATE_PIPELINE: str ):  
    '''
    Function to group and save the breweries by type and country
    input
        - ACCESS_TOKEN: token to access the blob storage
        - SILVER_CONTAINER: container that has the tabular format
        - ACCOUNT_NAME: account name to be used to read the data
        - GOLD_CONTAINER: container that the grouped data will be saved
        - ACCESS_KEY: access key to the blob
        - countries: list containing the countries to be grouped
        - DATE_PIPELINE: date to run the pipeline
    output:
        no output
    ''' 
    logger = logging.getLogger("grouping countries")
    logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")
    
    logger.info("Grouping by countries")
    for country in ['Ireland','United_States']:
        country_df = pd.DataFrame()
        
        logger.info("Reading the blobs")
        blobs = files_from_blob(ACCESS_TOKEN, SILVER_CONTAINER)
        for blob in blobs:
            # only read the datasets that are on the date pipeline
            if (DATE_PIPELINE in blob.name)& (country in blob.name):
                # removing the .parquet to get only the path
                path = blob.name.split('.')[0]
                #reading the parquet file into a pandas df
                df = read_parquet(SILVER_CONTAINER, ACCOUNT_NAME, ACCESS_KEY, blob.name)
                # concating the state dataframe to the whole df
                country_df = pd.concat([country_df,df])
        # grouping thw whole country df to have the grouped information      
        brewery_type = country_df.groupby('brewery_type').size().reset_index().rename(columns = {0: 'quantity'})
        #spliting the path into different parts to be used during the saving
        path = path.split('/')
        save_parquet(brewery_type, ACCESS_TOKEN, GOLD_CONTAINER, f'{path[0]}/{path[1]}_brewery_type.parquet')