from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

# 모든 template 날짜 변수들을 확인할 수 있는 Python Operator

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024,4,20,tz="Asia/Seoul"),
    catchup=True
) as dag:

    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()