import logging
import datetime
import pandas as pd
import io 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.azure_storage import read_json, upload_data
from config import (ACCESS_TOKEN, 
                    DATE_PIPELINE, 
                    BRONZE_CONTAINER,
                    SILVER_CONTAINER)


def save_tabular_df():
    logger.info("Transforming the json into spark df.")
    json_data = read_json(ACCESS_TOKEN, BRONZE_CONTAINER, f'raw_data_{DATE_PIPELINE}.json')
    spark = SparkSession.builder.appName("transform_dataframe").getOrCreate()
    df =spark_df = spark.createDataFrame(pd.DataFrame(json_data)) 

    logger.info("Changing the variable type for some columns")
    df = df.withColumn('latitude', col('latitude').cast('float'))
    df = df.withColumn('longitude', col('longitude').cast('float'))
    df = df.withColumn('phone', col('phone').cast('int'))
    df = df.withColumn('address_3', col('address_3').cast('string'))

    logger.info("Saving the transformed dataframes")
    unique_country = df.select('country').distinct().rdd.flatMap(lambda x: x).collect()
    for country in unique_country:
        df_filter = df.filter(col('country') == country)
        unique_state = df_filter.select('state').distinct().rdd.flatMap(lambda x: x).collect()
        country = country.replace(' ','_')
        for state in unique_state:
            df_filter_2 = df_filter.filter(col('state') == state)
            df_pandas = df_filter_2.toPandas()
            state = state.replace(' ','_')
            save_parquet(df, ACCESS_TOKEN, SILVER_CONTAINER, f'{DATE_PIPELINE}/{country}/{state}.parquet')

    logger.info("Finishing the transformed")