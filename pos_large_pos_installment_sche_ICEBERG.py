import datetime
import json
import os
import sys
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.oracle.hooks.oracle  import OracleHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from kubernetes.client import models as k8s
from airflow.settings import AIRFLOW_HOME
import pandas as pd
from smart_open import open

default_args = {
    'owner': 'pos'
}


dag = DAG(
        dag_id='pos_large_installment_sche_ICEBERG', #NEED TO CHANGE HERE
        start_date=datetime.datetime(2021, 12, 1),
        schedule_interval="@once",
        default_args = default_args,
        catchup=False,
      )


# Step 3 - Declare dummy start and stop tasks
start_task = DummyOperator(task_id='start', dag=dag)
start_insert_job = DummyOperator(task_id='start_insert_job', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)


# Step 4 - Read the list of elements from the airflow variable

parallel_task_count = 1
min_executors = "2"
max_executors = "6"
executor_min_cpu = "1"
executor_max_cpu = "2"
executor_memory = "12G"
s3_datasource_base_path = "bigdata-dev-cmfcknil/raw/idl/posdb/POS_INSTALLMENT_SCHE/POS"# AND HERE (Object storage location of the file)
part_start_index = 1
part_end_index = 37 # NEED TO CHANGE THIS ACCORDING TO NUMBER OF CHUNKS

tasks = []

i = 0
j = part_start_index

start_task >> start_insert_job

parent_task = start_insert_job

while j <= part_end_index:
    
    #val = val.lower()
    #res = val.split(".")
    #schema_name = res[0]
    #table = res[1]

    conf = {}
    conf["min_executors"] = min_executors
    conf["max_executors"] = max_executors
    conf["executor_min_cpu"] = executor_min_cpu
    conf["executor_max_cpu"] = executor_max_cpu
    conf["executor_memory"] = executor_memory
    conf["s3_datasource_path"] = f"{s3_datasource_base_path}/POS_INSTALLMENT_SCHE__{j}.parquet" #ALSO HERE, Take the table name from S3 object storage
    conf["target_database"] = "pos" # Creates this folder under warehouse if not exists
    conf["target_schema"] = "pos" # Appends this schema name with the tablename
    conf["target_table"] = "pos_installment_sche" #AND HERE, DESTINATION (keep the name in small letter)
    conf["table_keys"] = "ID"
    conf["update_date_column"] = "MODIFIED" #Taken from control tableb
    conf["partition_column"] = "INSTALLMENT_DUE_DATE" #Taken from control table
    conf["partition_column_transformation"] = ""


    task = TriggerDagRunOperator(
        task_id=f"merge__part_{j}",
        trigger_dag_id="spark_submit_merge_parquet_to_iceberg",
        conf=conf,
        dag=dag,
        wait_for_completion=True,
        executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template_2.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources = k8s.V1ResourceRequirements(
                                requests= {
                                    "cpu": "200m",
                                    "memory": "1G"
                                },
                                limits = {
                                    "cpu": "1",
                                    "memory": "1G"
                                }
                            ),
                        ),
                    ],
                )
            ),
        },
    )

    i = i + 1
    j = j + 1
    tasks.append(task)


# SEQUENTIAL TASKS
if len(tasks) > 0:
    j = 0
    for task in tasks:
        if j < parallel_task_count:
            parent_task.set_downstream(tasks[j])
        else:
            tasks[j-parallel_task_count].set_downstream(tasks[j]) 

        j = j + 1

    x = parallel_task_count
    while x > 0:
        tasks[j-parallel_task_count].set_downstream(end_task)
        x = x - 1
        j = j + 1
    
    #convert_tasks[j-1].set_downstream(end_task)
else:
    parent_task >> end_task