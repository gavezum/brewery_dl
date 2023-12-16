import logging
import pandas as pd
from utils.brewery_type_group import  state_group, country_group
from config import (ACCESS_TOKEN, 
                    ACCESS_KEY,
                    DATE_PIPELINE, 
                    SILVER_CONTAINER,
                    GOLD_CONTAINER,
                    ACCOUNT_NAME)

def group_data():
    logger = logging.getLogger("grouping data")
    logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")

    logger.info("Grouping brewery type by state.")
    countries = state_group(ACCESS_TOKEN, 
                                SILVER_CONTAINER, 
                                ACCOUNT_NAME, 
                                GOLD_CONTAINER)

    logger.info("Grouping brewery type by country.")
    country_group(ACCESS_TOKEN, 
                SILVER_CONTAINER, 
                ACCOUNT_NAME, 
                GOLD_CONTAINER,
                countries)

    logger.info("End of the pipeline.")