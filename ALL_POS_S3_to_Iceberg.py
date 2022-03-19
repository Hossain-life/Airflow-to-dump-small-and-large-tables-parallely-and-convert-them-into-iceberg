import datetime
import json
import os
import sys
import random
import string
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


dag = DAG(
        dag_id='ALL_POS_S3_to_Iceberg_dag',
        start_date=datetime.datetime(2021, 12, 1),
        schedule_interval="@once",
        catchup=False,
      )

# Step 3 - Declare dummy start and stop tasks
start_task = DummyOperator(task_id='start', dag=dag)
start_merge_job = DummyOperator(task_id='start_merge_job', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

checkpoint_count = 1

def GenerateRandomString():
    return ''.join(random.choices(string.ascii_lowercase, k=16))

def GetCheckPointTask(name):
    return DummyOperator(task_id=name, dag=dag)


# Step 4 - Read the list of elements from the airflow variable
global_var = Variable.get("all_pos_s3_to_iceberg", deserialize_json=True)
inputs = global_var['inputs']
parallel_task_count = global_var['parallel_task_count']
min_executors = global_var['min_executors']
max_executors = global_var['max_executors']
executor_min_cpu = global_var['executor_min_cpu']
executor_max_cpu = global_var['executor_max_cpu']
executor_memory = global_var['executor_memory']
s3_datasource_base_path = global_var['s3_datasource_base_path']

merge_tasks = []
i = 0

start_task >> start_merge_job

parent_task = start_merge_job

for input in inputs:
    #if i == 0:
        #merge_tasks_group[dump_task_group_index] = []
    
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
    conf["s3_datasource_path"] = f"{s3_datasource_base_path}/{input['src']['db'].lower()}/{input['src']['schema'].lower()}/{input['src']['table'].lower()}.parquet"
    conf["target_database"] = input['dst']['db'].lower()
    conf["target_schema"] = input['dst']['schema'].lower()
    conf["target_table"] = input['dst']['table'].lower()
    conf["table_keys"] = input['table_keys'].lower()
    conf["update_date_column"] = input['update_date_column'].lower() if 'update_date_column' in input else ''
    conf["partition_column"] = input['partition_column'].lower() if 'partition_column' in input else ''
    conf["partition_column_transformation"] = input['partition_column_transformation'].lower() if 'partition_column_transformation' in input else ''


    task = TriggerDagRunOperator(
        task_id=f"merge__{input['src']['db'].lower()}.{input['src']['schema'].lower()}.{input['src']['table'].lower()}",
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
                        ),
                    ],
                )
            ),
        },
    )

    merge_tasks.append(task)
 


# SEQUENTIAL TASKS
if len(merge_tasks) > 0:
    j = 0
    for task in merge_tasks:
        if j < parallel_task_count:
            parent_task.set_downstream(merge_tasks[j])
        else:
            merge_tasks[j-parallel_task_count].set_downstream(merge_tasks[j]) 

        j = j + 1

    x = parallel_task_count
    while x > 0:
        merge_tasks[j-parallel_task_count].set_downstream(end_task)
        x = x - 1
        j = j + 1
    
    #merge_tasks[j-1].set_downstream(end_task)
else:
    parent_task >> end_task