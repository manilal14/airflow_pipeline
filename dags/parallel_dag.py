from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdag.subdag_parallel_dag import subdag_parallel_dag
 
from datetime import datetime


default_args = {
    'start_date' : datetime(2022, 7, 13)
}
 
with DAG('parallel_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
 
    extract_a = BashOperator(
        task_id='extract_a',
        bash_command='sleep 3'
    )
 
    # extract_b = BashOperator(
    #     task_id='extract_b',
    #     bash_command='sleep 1'
    # )

    subdag_processing = SubDagOperator(
        task_id = 'subdag_processing',
        subdag = subdag_parallel_dag('parallel_dag','subdag_processing', default_args)
    )

    transform = BashOperator(
        task_id='transform',
        bash_command='sleep 1'
    )
 
    extract_a >> subdag_processing >> transform