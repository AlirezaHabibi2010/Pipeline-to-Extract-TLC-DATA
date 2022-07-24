from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
import datetime

#import sys, os
#sys.path.append(os.path.abspath('../subs'))
from csv_to_parquet import *

def _convert()-> None:
    '''
    Download CSV file from dataset and convert it to parquet file.
    Arguments:
        none
    Return:
        none
    '''
    
    #Read the directory path, url path from info.json file, and taxi kinds. 
    with open("./dags/info.json") as json_file:
        files_info = json.load(json_file)
    print(files_info)

    #Read the date of the execution date and extract the year and month 
    run_date=os.environ["AIRFLOW_CTX_EXECUTION_DATE"]
    date=datetime.datetime.fromisoformat(run_date)
    
    #print year and month of execution date
    print(date.strftime("%Y"),date.strftime("%m"))
    
    #Download CSV file from dataset and convert it to parquet file.
    convert_format(date.strftime("%Y"),date.strftime("%m"),files_info)


with DAG(
    "csv_convert_dag",
    default_args={
        'depends_on_past': False,
        #'email': ['airflow@example.com'],
        #'email_on_failure': False,
        #'email_on_retry': False,
        'retries': 3,
        #'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    start_date=datetime.datetime(2018, 8, 1),#starting date of the dag
    schedule_interval="@monthly",
    catchup=True #if write the missing runs from the last run
) as dag:
        
        #convert function for Airflow
        convert = PythonOperator(
            task_id="csv_convert",
            python_callable=_convert
        )
        
        #run
        convert
