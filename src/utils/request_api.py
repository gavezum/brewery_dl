import requests
import logging

def request_api(api_url: str):
    '''
    Function to return an API
    input:
        -api_url: api url in string format
    output:
        -data: the json response from the api
    '''
    logger = logging.getLogger("api_reader")
    logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            # Log the successful response
            logger.info("Request successful:", response.text)
            # Retrieving the data from the api
            data = response.json()
            return data
        elif response.status_code == 400:
            # Log the error response
            logger.error("Request error:", response.status_code, response.text)
        else:
            # Log other error responses
            logger.error("Unexpected error:", response.status_code, response.text)
    except Exception as e:
        # Log if any other type of error appear
        logger.exception("Exception:", e)