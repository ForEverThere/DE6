from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from Queries import insSTAGE_TABLE, insMASTER_TABLE
from func import get_data

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

with DAG('snowflakeDag', default_args=default_args, schedule='@once') as dag:
    t0 = EmptyOperator(task_id="start_process")
    load_task = PythonOperator(task_id="load_task", python_callable=get_data)
    insStage = SnowflakeOperator(task_id='stageTableInsert', sql=insSTAGE_TABLE, warehouse=wareHouse,
                                 database=database, schema=schema, role=role)
    insMaster = SnowflakeOperator(task_id='masterTableInsert', sql=insMASTER_TABLE, warehouse=wareHouse,
                                  database=database, schema=schema, role=role)

t0 >> load_task >> insStage >> insMaster
