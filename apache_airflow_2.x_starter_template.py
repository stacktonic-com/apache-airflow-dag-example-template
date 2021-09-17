############################################################
# Author    Krisjan Oldekamp / Stacktonic.com              #
# Email     krisjan@stacktonic.com                         #
############################################################

# Libraries
import json
import os
from datetime import datetime, timedelta

# Airflow
from airflow.models import DAG, Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.decorators import task

# Default Airflow operators 
# https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Contributed Airflow operators (providers) 
# https://airflow.apache.org/docs/apache-airflow-providers/operators-and-hooks-ref/index.html

#from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator
#from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

############################################################
# DAG settings
############################################################

DAG_NAME = "stacktonic_example_dag" # DAG name (proposed format: lowercase underscore). Should be unique.
DAG_DESCRIPTION = "Example DAG by Krisjan Oldekamp / stacktonic.com"
DAG_START_DATE = days_ago(2) # Startdate. When setting the "catchup" parameter to True, you can perform a backfill when you insert a specific date here like datetime(2021, 6, 20)
DAG_SCHEDULE_INTERVAL = "@daily" # Cron notation -> see https://airflow.apache.org/scheduler.html#dag-runs
DAG_CATCHUP = False # When set to true, DAG will start running from DAG_START_DATE instead of current date
DAG_PAUSED_UPON_CREATION = True # Defaults to False. When set to True, uploading a DAG for the first time, the DAG doesn't start directly 
DAG_MAX_ACTIVE_RUNS = 5 # Configure efficiency: Max. number of active runs for this DAG. Scheduler will not create new active DAG runs once this limit is hit. 

############################################################
# Default DAG arguments
############################################################

default_args = {
    "owner": "airflow",
    "start_date": DAG_START_DATE,
    "depends_on_past": False,
    "email": Variable.get("email_monitoring", default_var="<FALLBACK-EMAIL>"), # Make sure you create the "email_monitoring" variable in the Airflow interface
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2, # Max. number of retries before failing
    "retry_delay": timedelta(minutes=60) # Retry delay
}

############################################################
# DAG configuration (custom)
############################################################

SOME_CUSTOM_CONFIG = "yes"

DYNAMIC_TASKS =  [{
    "account": "123",
    "some_setting": True
},{
    "account": "456",
    "some_setting": False
}]

############################################################
# Python functions (custom) using the Taskflow API decorators (@task)
############################################################

@task
def python_function_with_input(value: str):
    print("Custom Python function, print whatever you want!")
    print(value)

############################################################
# Repeatable Airflow Operator functions (for use in dynamic tasks)
############################################################

def dynamic_operator_task(i):
    return DummyOperator(
        task_id="dynamic_task_" + DYNAMIC_TASKS[i]["account"]
    )

############################################################
# Main DAG
#############################################################

# Create DAG
with DAG(
    DAG_NAME,
    description=DAG_DESCRIPTION,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    catchup=DAG_CATCHUP,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    is_paused_upon_creation=DAG_PAUSED_UPON_CREATION,
    default_args=default_args) as dag:

    # Start
    start = DummyOperator(
        task_id="start")

    # Bash Operator
    print_start_bash = BashOperator(
        task_id="print_start_bash",
        bash_command="echo 'hello {{ ds }}'") # Using a macro for execution date, see other macros https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html

    # Wait till finished
    wait_till_finished = python_function_with_input("All finished")

    # Complete
    complete = DummyOperator(
        task_id="complete")

    # Define a taskgroup so tasks will be grouped in the interface -> usefull when you have a lot of repeated task sequences for example.
    with TaskGroup(group_id='task_group_with_two_tasks') as task_group_with_two_tasks:
        # Set order of execution within taskgroup (see also https://airflow.apache.org/concepts.html#bitshift-composition)
        python_function_with_input('taskgroup task #1') >> python_function_with_input('taskgroup task #2')

    # Set task depencency using >> (see also https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#task-dependencies)
    start >> print_start_bash

    # Add dynamic (repeating) tasks (https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#dynamic-dags), looping the dicts from DYNAMIC_TASK and defining order of execution
    for i, dct in enumerate(DYNAMIC_TASKS):

        print_start_bash \
        >> python_function_with_input(dct["account"]) \
        >> [python_function_with_input("Execute together. This is the execution date: {{ ds }} #1"), python_function_with_input("Execute together. This is the execution date minus 5 days: {{ macros.ds_add(ds, -5) }} #2")] \
        >> wait_till_finished \
        >> task_group_with_two_tasks \
        >> dynamic_operator_task(i) \
        >> complete