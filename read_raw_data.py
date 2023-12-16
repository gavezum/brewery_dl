import logging
import datetime
from utils.request_api import request_api
from utils.azure_storage import upload_data
from config import ACESS_TOKEN, URL_API, BRONZE_CONTAINER, DATE_PIPELINE
import json

logger = logging.getLogger("reading the raw data")
logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")
logger.info(f"Date pipeline is {DATE_PIPELINE}")

logger.info(f"Requesting the API with the data ")
data = request_api(api_url = URL_API)

logger.info(f"Transforming the data into a json format")
json_bytes = json.dumps(data).encode('utf-8')

logger.info(f"Saving the raw date into container: {BRONZE_CONTAINER}")

upload_data(ACESS_TOKEN, BRONZE_CONTAINER,f'raw_data_{DATE_PIPELINE}.json', json_bytes)