import datetime
import json
import os
import sys
import re

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.oracle.hooks.oracle  import OracleHook
from kubernetes.client import models as k8s
from airflow.settings import AIRFLOW_HOME
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd

from airflow.utils.dates import days_ago
from smart_open import open

default_args = {
    'owner': 'pos'
}

dag = DAG(
        dag_id='pos_multiple_dump',
        start_date=days_ago(0),
        schedule_interval=None,
        catchup=False,
      )


def get_column_data_type(original_schema_dict, column_name):
    return original_schema_dict[column_name]["DATA_TYPE"]

def get_column_data_scale(original_schema_dict, column_name):
    return original_schema_dict[column_name]["DATA_SCALE"]

def remove_special_characters_from_all_column_names(df):
    print("")
    print("Removing Special Characters From All Column Names ....")
    for col in df.columns:
        print(f"{col} --> {df[col].dtype}")
        df = df.rename(columns={col: re.sub("[!@#$&*^%~<>?+=]", "", col)})
    
    return df


def calculate_original_schema_dict(conn, schema_name, table_name):
    original_schema_dict = {}
    try:
        schema_sql = f"SELECT COLUMN_NAME, DATA_TYPE, DATA_SCALE, DATA_PRECISION, COLUMN_ID FROM sys.all_tab_columns where OWNER = '{schema_name.upper()}' AND TABLE_NAME = '{table_name.upper()}' ORDER BY COLUMN_ID"
        print(schema_sql)
        ndf = pd.read_sql(schema_sql, conn)
        for idx, row in ndf.iterrows():
            print(row['COLUMN_NAME'], row['DATA_TYPE'], "DL:", row['DATA_SCALE'], " , DP:", row['DATA_PRECISION'])
            val = {
                "DATA_TYPE": row["DATA_TYPE"],
                "DATA_PRECISION": row["DATA_PRECISION"],
                "DATA_SCALE": row["DATA_SCALE"]
            }
            original_schema_dict[row["COLUMN_NAME"]] = val
    except Exception as e:
        print("[ERROR] while fetching table meta data....")
        print(str(e))
    
    return original_schema_dict


def convert_data_types_to_original(original_schema_dict, df):
    print("")
    print("Converting Data Types ....")
    try:
        for col in df.columns:
            data_type = get_column_data_type(original_schema_dict, col)
            if data_type == "CHAR":
                data_type = "str"
            elif data_type == "ROWID":
                data_type = "str"
            elif data_type == "VARCHAR":
                data_type = "str"
            elif data_type == "VARCHAR2":
                data_type = "str"
            elif data_type == "DATE":
                data_type = "datetime64[ns]"
            elif data_type == "LONG":
                df[col] = df[col].fillna(0)
                data_type = "int64"
            elif data_type == "FLOAT":
                df[col] = df[col].fillna(0)
                data_type = "float64"
            elif data_type == "NUMBER":
                df[col] = df[col].fillna(0)
                df[col] = pd.to_numeric(df[col])
                data_type = "float64"
            else:
                data_type = df[col].dtype
            
            if data_type == "object":
                data_type = "str"

            if data_type != df[col].dtype:
                print(f"{col} ({df[col].dtype}) --> {data_type}")
                if data_type == "datetime64[ns]":
                    df[col] = pd.to_datetime(df[col])
                else:
                    df = df.astype({col: data_type})
        
        print("")
        print("Conversion Completed")
        return df

    except Exception as e:
        print(str(e))
        print("[ERROR] Error occurred while schema converstion.")
        print(f"[ERROR] {col} ({df[col].dtype}) --> {data_type}")
        raise Exception(str(e))



def OracleToParquetToS3(schema_name, table_name, target_bucket, file_key):
        SQL= f"SELECT * FROM {schema_name}.{table_name}"
        oracle_conn = OracleHook(oracle_conn_id='con-ora-pos').get_conn()

        print("")
        print("Executing SELECT Query...")
        df = pd.read_sql(SQL, oracle_conn)

        ## Calculate Original Schema Dictionary
        print("")
        print("Fetching Table Meta Data...")
        original_schema_dict = calculate_original_schema_dict(oracle_conn, schema_name, table_name)
        if len(original_schema_dict.keys()) == 0:
            raise Exception("Could not calculate original schema")
        
        oracle_conn.close()  

        ## Renaming columns without special characters
        df = remove_special_characters_from_all_column_names(df)

        ## Convert Column Data Types To Original
        df = convert_data_types_to_original(original_schema_dict, df)

        s3 = S3Hook(aws_conn_id='con-s3')
        ## Dump as parquet directly to S3:
        with open(f"s3://{target_bucket}/{file_key}", 'wb', transport_params={'client': s3.get_conn()}) as out_file:
            df.to_parquet(out_file, engine='pyarrow', index=False)
            


# Step 3 - Declare dummy start and stop tasks
start_task = DummyOperator(task_id='start', dag=dag)
start_dumping = DummyOperator(task_id='start_dumping', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

checkpoint_count = 1


def GetCheckPointTask(name):
    return DummyOperator(task_id=name, dag=dag)


# Step 4 - Read the list of elements from the airflow variable
global_var = Variable.get("pos_multiple_dump_var", deserialize_json=True)
table_names = global_var['table_names']
s3_dump_base_path = global_var['s3_dump_base_path']
parallel_task_count = global_var['parallel_task_count']

dump_tasks = []
i = 0

start_task >> start_dumping

parent_task = start_dumping

for val in table_names:
    
    val = val.lower()
    res = val.split(".")
    schema_name = res[0]
    table = res[1]

    task = PythonOperator(
        task_id=f"dump__{schema_name}.{table}",
        #trigger_rule=TriggerRule.ALL_DONE,
        python_callable=OracleToParquetToS3, 
        op_kwargs={
              "schema_name": schema_name,
              "table_name": table,
              "target_bucket": "bigdata-dev-cmfcknil", #Had to make a change here
              "file_key":f"{s3_dump_base_path}/{schema_name}/{table}.parquet"
        },
        executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template_2.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    #node_selector={
                     #   "node-group": "master"
                    #},
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    )

    i = i + 1
    dump_tasks.append(task)



# SEQUENTIAL TASKS
if len(dump_tasks) > 0:
    j = 0
    for task in dump_tasks:
        if j < parallel_task_count:
            parent_task.set_downstream(dump_tasks[j])
        else:
            dump_tasks[j-parallel_task_count].set_downstream(dump_tasks[j]) 

        j = j + 1

    x = parallel_task_count
    while x > 0:
        dump_tasks[j-parallel_task_count].set_downstream(end_task)
        x = x - 1
        j = j + 1
    
    #merge_tasks[j-1].set_downstream(end_task)
else:
    parent_task >> end_task