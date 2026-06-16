from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.utils.dates import days_ago
import json

#define dag
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    
    #step1 : create table if it doesnot exist

    @task
    def create_table():
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        #sql query
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data(
        id SERIAL PRIMARY KEY,
        title VARCHAR(255),
        explanation TEXT,
        url TEXT,
        date DATE,
        media_type VARCHAR(50)
        );


        
        """

        #execute table creation query
        postgres_hook.run(create_table_query)

    #step2: extract nasa api data(APOD)
    # https://api.nasa.gov/planetary/apod?api_key=aJbMRmfabXMfMQShwfsPthofJBMJG8RxeSorG6jY
    # https://api.nasa.gov/planetary/apod?api_key=aJbMRmfabXMfMQShwfsPthofJBMJG8RxeSorG6jY
    extract_apod=HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',
        endpoint='planetary/apod',
        method='GET',
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response:response.json(),
    )

    #step 3: transform data 
    @task
    def transform_apod_data(response):
        apod_data={
            'title':response.get('title',''),
            'explanation':response.get('explanation',''),
            'url':response.get('url',''),
            'date':response.get('date',''),
            'media_type':response.get('media_type','')

        }
        return apod_data


    #step 4: loading data 
    @task
    def load_data_to_postgres(apod_data):
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        insert_query="""
        INSERT INTO apod_data (title,explanation,url,date,media_type)
        VALUES (%s,%s,%s,%s,%s);
        """

        postgres_hook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type'],
        ))

    #step 5: verify data DBViewer

    #step 6: task dependencies
    create_table() >> extract_apod
    api_response=extract_apod.output

    transformed_data=transform_apod_data(api_response)

    load_data_to_postgres(transformed_data)