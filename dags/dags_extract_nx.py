from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()  # .env 파일에서 환경 변수 로드


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def fetch_data(**kwargs):
    api_key = os.getenv('API_KEY')
    date_str = kwargs['execution_date'].strftime('%Y-%m-%d')
    file_date_str = kwargs['execution_date'].strftime('%m%d')
    url = f'https://open.api.nexon.com/maplestory/v1/ranking/overall?date={date_str}'
    headers = {'x-nxopen-api-key': api_key}

    result = requests.get(url, headers=headers)
    if result.status_code == 200:
        data = result.json()
        with open(f'/path/to/save/{file_date_str}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f'Successfully saved data for {date_str} to {file_date_str}.json')
    else:
        print(f'Failed to fetch data for {date_str}: {result.status_code}')


dag = DAG(
    dag_id='dags_extract_nx',
    default_args=default_args,
    description='Fetch MapleStory data daily and save as JSON',
    schedule_interval=timedelta(minutes=1),
)

for i in range(31):
    date = datetime(2024, 5, 1) + timedelta(days=i)
    task = PythonOperator(
        task_id=f'fetch_data_{date.strftime("%m%d")}',
        provide_context=True,
        python_callable=fetch_data,
        dag=dag,
        execution_date=date
    )
