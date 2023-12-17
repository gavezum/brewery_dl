# brewery_dl

This repository is dedicated to building a datalake with a three-layer structure, where all the codes were runned in a Databricks enviroment and the datasets are saved on a blobstorage.
The idea is to run the process daily and save all three-layer separated by the date, so we can navigate between the days.
The layers are the Bronze, Silver, and Gold layers:

## **BRONZE**
The **Bronze layer** contains raw and uncurated data, typically persisted in its native format.

## **Silver Layer**
In the **Silver layer**, data is transformed into a columnar storage format, such as Parquet or Delta, and is partitioned by brewery location. This layer serves as an intermediate step for more efficient querying and analysis.

## **Gold Layer**
The **Gold layer** is designed to create an aggregated view that provides insights into the quantity of stores per type and location. This layer adds value by offering a consolidated and organized perspective on the data.

## **Codes organization**
The folders are organized in the following way:

- **airflow**: *Folder containing the necessary information to run the airflow* **(Stil not working, the process keeps stoping because of timeout)**
    - **dags**: *Folder with the dag*
         --**my_dag.py**: *DAG to be executed*
    - **logs/scheduler**: *Logs to resulted from the dags runs*
  
- **src**: folder containing all the necessary code to run the process. if run manually put on the first folder, if run airlfow this folder needs to be move to the **airflow/dags** folder.
    - **utils**: *Folder with auxiliary functions*
    - **read_raw_data**: Script to run the **Bronze layer**
    - **transform_data**: Script to run the **Silver layer**
    - **group_data**: Script to run the **Gold layer**

- **airflow_run**: Notebook containg the necessary commands to run the airflow

- **requirements.txt**: All the necessary packages to run the codes on the Databricks env.

- **run_process**: Python script to run the process manually
