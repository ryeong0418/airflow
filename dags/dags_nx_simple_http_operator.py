from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_nx_simple_http_operator',
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:

    '''서울시 공공 자전거 대여소 정보'''

    nx_info = SimpleHttpOperator(
        task_id='nx_info',
        http_conn_id='nx_api',
        endpoint='{{var.value.apikey_openapi_nx}}/maplestory/v1/ranking/overall?date=2024-05-01',
        method='GET',
        headers={'Content-Type':'application/json',
                 'charset':'utf-8',
                 'Accept':'*/*'
        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='nx_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    nx_info >> python_2()
