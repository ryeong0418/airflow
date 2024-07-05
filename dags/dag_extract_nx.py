from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta
import pendulum
import os


def generate_url(execution_date):

    created_url = f"{os.getenv('MAP_URI')}?date={execution_date.strftime('%Y-%m-%d')}"
    print(created_url)

    return created_url


with DAG(
    dag_id='dag_extract_nx',
    start_date=pendulum.datetime(2024,5,1,tz='Asia/Seoul'), #start_date는 해당 날짜 포함하지만
    end_date=pendulum.datetime(2024,6,1,tz='Asia/Seoul'), #end_date는 해당 날짜 포함 안 함
    catchup=False,
    schedule_interval=timedelta(minutes=10)
) as dag:

    # URL 생성 작업을 PythonOperator로 정의
    def generate_url_task(**kwargs):
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        url = generate_url(execution_date)
        ti.xcom_push(key='url', value=url)

    generate_url_operator = PythonOperator(
        task_id='generate_url_task',
        python_callable=generate_url_task,
        provide_context=True,
        dag=dag,
    )

    nx_info = SimpleHttpOperator(
        task_id='nx',
        http_conn_id='nx_api',
        endpoint="{{task_instance.xcom_pull(task_ids='generate_url_task', key='url') }}",
        method='GET',
        headers={'x-nxopen-api-key': os.getenv('API_KEY'),
        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='nx')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    generate_url_operator >> nx_info >> python_2()
