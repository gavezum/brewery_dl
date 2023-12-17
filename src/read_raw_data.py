import logging
import datetime
from src.utils.request_api import request_api
from src.utils.azure_storage import upload_data
from src.config import ACCESS_TOKEN, URL_API, BRONZE_CONTAINER, DATE_PIPELINE
import json

def read_raw_df():
    logger = logging.getLogger("reading the raw data")
    logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")
    logger.info(f"Date pipeline is {DATE_PIPELINE}")

    logger.info(f"Requesting the API with the data ")
    data = request_api(api_url = URL_API)

    logger.info(f"Transforming the data into a json format")
    json_bytes = json.dumps(data).encode('utf-8')

    logger.info(f"Saving the raw date into container: {BRONZE_CONTAINER}")

    upload_data(ACCESS_TOKEN, BRONZE_CONTAINER,f'raw_data_{DATE_PIPELINE}.json', json_bytes)