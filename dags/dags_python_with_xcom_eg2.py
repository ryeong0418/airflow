from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id='dags_python_with_xcom_eg2',
    schedule= '30 6 * * *',
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup=False
) as dag:

    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'

    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return') #task_id값만 주면 디폴트로 키 값은 return value를 찾아옴. -->success라는 값이 나올 것
        print('xcom_pull 메서드로 직접 찾은 리턴 값:'+value1)

    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status,**kwargs):
        print('함수 입력값으로 받은 a값:'+status)

    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()