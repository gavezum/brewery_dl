from airflow import DAG
import airflow
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from read_raw_data import read_raw_df
from transform_data import save_tabular_df
from group_data import group_data
import logging 

logger = logging.getLogger("Airflow process")
logging.Formatter("%(asctime)s:%(levelname)s: %(message)s")

logger.info('Defining args')
default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger.info('Defining DAG')
dag = DAG('dbrewery_dl_dag', default_args=default_args, schedule_interval='@daily')

logger.info('Creating PythonOperator to read the raw data (Bronze Container)')
bc_task = PythonOperator(
    task_id='Bronze container construiction',
    python_callable=read_raw_df,
    dag=dag,
)

logger.info('Creating PythonOperator to transform and save the raw data (Silver Container)')
sc_task = PythonOperator(
    task_id='Silver container construiction',
    python_callable=save_tabular_df,
    dag=dag,
)

logger.info('Creating PythonOperator to group by brewery type (Gold Container)')
gc_task = PythonOperator(
    task_id='Gold container construiction',
    python_callable=group_data,
    dag=dag,
)

bc_task >> sc_task >> gc_task