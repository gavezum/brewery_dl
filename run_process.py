from datetime import datetime, timedelta
from src.read_raw_data import read_raw_df
from src.transform_data import save_tabular_df
from src.group_data import group_data
import logging 

logger = logging.getLogger("Manual process")
logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")

logger.info("Running the process to read the raw data and save it.")
read_raw_df()

logger.info("Running the process transform the raw data and save as parquet.")
save_tabular_df()

logger.info("Running the process grouping the information.")
group_data()

logger.info("End of process")