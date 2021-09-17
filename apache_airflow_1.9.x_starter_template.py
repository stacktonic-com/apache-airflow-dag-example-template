############################################################
# Author    Krisjan Oldekamp / Stacktonic.com              #
# Email     krisjan@stacktonic.com                         #
############################################################

from airflow import models

# Airflow operators https://airflow.apache.org/docs/apache-airflow/1.10.14/_api/airflow/operators/index.html
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Contributed Airflow operators https://airflow.apache.org/docs/apache-airflow/1.10.14/_api/airflow/contrib/operators/index.html
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator

# Other common imports
import datetime
import os
import json


#################################################
# DAG Configuration
#################################################

DAG_NAME = "example_dag" # DAG name (proposed format: lowercase underscore)
DAG_START_DATE = datetime.datetime(2021, 2, 12) # Startdate (when enabling the "catchup" parameter, you can perform a backfill)
DAG_SCHEDULE_INTERVAL = "@daily" # Cron notation -> see https://airflow.apache.org/scheduler.html#dag-runs
DAG_CATCHUP = False # When set to true, DAG will start running from DAG_START_DATE instead of current date

#################################################
# Default DAG arguments
#################################################

default_dag_args = {
    "owner": "airflow",
    "start_date": DAG_START_DATE,
    "depends_on_past": False,
    "email": models.Variable.get("email_monitoring"), # Make sure you create this variable
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1, # Number of retries
    "retry_delay": datetime.timedelta(minutes=60), # Retry delay
    "max_active_runs": 1 # Number of max. active runs
}

#################################################
# Custom DAG Configuration
#################################################

TASKS =  [{
    "account": "123"
},{
    "account": "456"
}]

#################################################
# Custom Python functions
#################################################

# Custom Python function, to be executed by PythonOperator (see https://cloud.google.com/composer/docs/how-to/using/writing-dags#pythonoperator)
def custom_python_function_whatever(ds, **kwargs):
    param_execution_date = kwargs["execution_date"]
    param_something = kwargs["something_else"]
    print("Custom function python, do whatever you want " + param_something)

#################################################
# Operator / repeatable functions
#################################################

# Repeatable / dynamic task
def dynamic_task(i, **kwargs):
    taskname = TASKS[i]["account"]

    return DummyOperator(
        task_id="dynamic_task_" + str(i),
        dag=dag)

#################################################
# Main DAG
#################################################

# Create DAG
with models.DAG(
        DAG_NAME,
        schedule_interval=DAG_SCHEDULE_INTERVAL,
        catchup=DAG_CATCHUP,
        max_active_runs=1,
        default_args=default_dag_args) as dag:

    # Start
    start = DummyOperator(
        task_id="start")

    # Bash Operator
    print_start = BashOperator(
        task_id="print_start",
        bash_command="echo 'hello'")

    # Custom Python function
    custom_python_function = PythonOperator(
        task_id="custom_python_function",
        provide_context=True,
        op_kwargs={
            "execution_date": "{{ ds }}", # Macro for execution date, see other macros https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
            "something_else": "hello!"
        },
        python_callable=custom_python_function_whatever)  

    # Complete
    complete = DummyOperator(
        task_id="complete")

    # Set order of execution (see also https://airflow.apache.org/concepts.html#bitshift-composition)
    start >> print_start >> custom_python_function

    # Add dynamic / repeating tasks (a lot of ways to do this)
    for i, val in enumerate(TASKS):
        custom_python_function >> dynamic_task(i) >> complete