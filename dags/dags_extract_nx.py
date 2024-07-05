from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.extract_nx import ExtractNxOperator
from datetime import datetime, timedelta
import requests
import json
import os
from dotenv import load_dotenv
import pendulum

load_dotenv()  # .env 파일에서 환경 변수 로드

with DAG(
    dag_id='dags_extract_nx',
    description='Fetch MapleStory data daily and save as JSON',
    start_date=pendulum.datetime(2024,5,1,tz="Asia/Seoul"),
    schedule_interval='*/1 * * * *',  # 1분 간격으로 실행
    catchup=False,  # 지나간 스케줄 무시
)as dag:

    nx_raw_data = ExtractNxOperator(
        task_id='nx_raw_data',
        api_key=os.getenv('API_KEY'),
        uri=os.getenv('URI'),
        file_path=os.getenv('FILE_PATH'),
    )




