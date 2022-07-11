from airflow import DAG
from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

import json
from pandas import json_normalize


def _process_user(ti):

    '''
        ti => task instance
        processed_user => a pandas dataframe
    
    '''
    res = ti.xcom_pull(task_ids = 'extract_user')
    user = res['results'][0]
    user_df = json_normalize({
        'firstname' : user['name']['first'],
        'lastname'  : user['name']['last'],
        'country'   : user['location']['country'],
        'username'  : user['login']['username'],
        'password'  : user['login']['password'],
        'email'     : user['email']
    })

    user_df.to_csv('/tmp/processed_user.csv', index=None, header=False)


def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql = "COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )


with DAG( 'user_processing', start_date = datetime(2022, 7, 11), schedule_interval = '@daily', catchup = False) as dag:

    # create table in postgres database
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres',
        sql = '''
            CREATE TABLE IF NOT EXISTS users(
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        '''
    )

    # check api availablity
    # base_url is defined as host during connection creation
    is_api_available =  HttpSensor(
        task_id = 'is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    # get the data from the api as a text
    extract_user = SimpleHttpOperator(
        task_id = 'extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter= lambda response : json.loads(response.text),   # convert json response to python object (dict and list)
        log_response=True
    )

    # convert the text into user data -> pandas_dataframe ->  save it as csv
    process_user = PythonOperator(
        task_id = 'process_user',
        python_callable=_process_user
    )

    # copy user data from csv into postgress database
    store_user = PythonOperator(
        task_id = 'store_user',
        python_callable=_store_user
    )


    create_table >> is_api_available >> extract_user >> process_user >> store_user



 