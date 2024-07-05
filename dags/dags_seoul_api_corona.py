from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum
import os
from airflow.decorators import task

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)


with DAG(
    dag_id='dags_seoul_api_corona',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup=False
) as dag:

    @task(task_id='create_tb_corona19_count_status_directory')
    def create_tb_corona19_count_status_directory(**kwargs):
        path = f'/opt/airflow/files/TbCorona19CountStatus/{kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y%m%d")}'
        create_directory(path)

    @task(task_id='create_tv_corona19_vaccine_stat_new_directory')
    def create_tv_corona19_vaccine_stat_new_directory(**kwargs):
        path = f'/opt/airflow/files/tvCorona19VaccinestatNew/{kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y%m%d")}'
        create_directory(path)

    '''서울시 코로나 19 확진자 발생 동향'''
    tb_corona19_count_status=SeoulApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm='TbCorona19CountStatus',
        path='/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul")|ds_nodash}}',
        file_name='TbCorona19CountStatus.csv'
    )

    '''서울시 코로나19 백신 예방 접종 현황'''
    tv_corona19_vaccine_stat_new=SeoulApiToCsvOperator(
        task_id='tv_corona19_vaccine_stat_new',
        dataset_nm='TvCorona19CountStatus',
        path='/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul")|ds_nodash}}',
        file_name='tvCorona19VaccinestatNew.csv'
    )

    create_tb_corona19_count_status_directory() >> tb_corona19_count_status
    create_tv_corona19_vaccine_stat_new_directory() >> tv_corona19_vaccine_stat_new
    tb_corona19_count_status >> tv_corona19_vaccine_stat_new


