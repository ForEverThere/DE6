import logging
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from Queries import insSTAGE_TABLE, insMASTER_TABLE, sql_query, insert_query

dag_config = Variable.get("snowflakeConfig", deserialize_json=True)
connection = dag_config["conID"]
schema = dag_config["schema"]
wareHouse = dag_config["warehouse"]
database = dag_config["database"]
role = dag_config["role"]
rawTable = dag_config["rawTable"]
stageTable = dag_config["stageTable"]
masterTable = dag_config["masterTable"]
account = dag_config["account"]
user = dag_config["user"]
password = dag_config["password"]
csvPath = dag_config["csvPath"]

default_args = {
    'start_date': datetime(2015, 12, 1),
    'snowflake_conn_id': connection
}


def check_table(sf_conn):
    sf_cur = sf_conn.cursor()
    try:
        sf_cur.execute(sql_query)
        logging.info(sf_cur.fetchall())
    except Exception as eUploadingFailed:
        logging.info(eUploadingFailed)
        raise eUploadingFailed
    finally:
        sf_cur.close()


def generate_query(table_name: str) -> str:
    return insert_query


def get_data():
    sf_conn = SnowflakeHook(snowflake_conn_id=connection).get_conn()
    check_table(sf_conn=sf_conn)
    sf_cur = sf_conn.cursor()
    for i in pd.read_csv(csvPath, chunksize=10000):
        i[['IOS_App_Id', 'Developer_IOS_Id', 'Total_Average_Rating', 'Total_Number_of_Ratings',
           'Average_Rating_For_Version', 'Number_of_Ratings_For_Version', 'Price_USD']] = i[
            ['IOS_App_Id', 'Developer_IOS_Id', 'Total_Average_Rating', 'Total_Number_of_Ratings',
             'Average_Rating_For_Version', 'Number_of_Ratings_For_Version', 'Price_USD']].fillna(-1)
        i[['Description', 'Languages', 'All_Genres', 'Primary_Genre', 'Current_Version_Release_Date',
           'Original_Release_Date', 'Age_Rating', 'Seller_Official_Website', 'IOS_Store_Url', 'Developer_Name',
           'Title']] = i[['Description', 'Languages', 'All_Genres', 'Primary_Genre', 'Current_Version_Release_Date',
                          'Original_Release_Date', 'Age_Rating', 'Seller_Official_Website', 'IOS_Store_Url',
                          'Developer_Name', 'Title']].fillna("-1")
        df_to_list = i.values.tolist()
        sf_cur.executemany(generate_query(table_name=rawTable), df_to_list)
    sf_cur.close()
    sf_conn.close()


with DAG('snowflakeDag', default_args=default_args, schedule='@once') as dag:
    t0 = EmptyOperator(task_id="start_process")
    load_task = PythonOperator(task_id="load_task", python_callable=get_data)
    insStage = SnowflakeOperator(task_id='stageTableInsert', sql=insSTAGE_TABLE, warehouse=wareHouse,
                                 database=database, schema=schema, role=role)
    insMaster = SnowflakeOperator(task_id='masterTableInsert', sql=insMASTER_TABLE, warehouse=wareHouse,
                                  database=database, schema=schema, role=role)

t0 >> load_task >> insStage >> insMaster
