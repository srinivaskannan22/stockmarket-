from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import pandas as pd
import boto3
import io



def get_data_stock(**kwargs):
    ti = kwargs['ti']
    data=requests.get('http://alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&outputsize=full&apikey=VPTRZY7BQ9MEIOZA')
    stock_data=data.json()
    ti.xcom_push(key='stock_data', value=stock_data)

def push_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_data_stock', key='stock_data')
    stock=pd.DataFrame(data['Time Series (5min)'])
    client=boto3.client('s3')
    csv_buffer=io.StringIO()
    stock.to_csv(csv_buffer,index=False)
    client.put_object(Bucket='uploaddata213', Key='stock.csv', Body=csv_buffer.getvalue())
    


    



    


with DAG(
    dag_id="nasa_mars_photos_dag",
    start_date=datetime(2025, 4, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["nasa", "postgres", "api"],
) as dag:

    task_get_data1 = PythonOperator(
        task_id="get_data_stock",
        python_callable=get_data_stock,
    )

    task_get_data2 = PythonOperator(
        task_id="push_data",
        python_callable=push_data,
    )

    task_get_data1 >> task_get_data2

