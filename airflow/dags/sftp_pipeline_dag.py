from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lv',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id = 'sftp_pipeline_dagv2',
    default_args = default_args,
    description = 'this is my dag',
    start_date = datetime(2024, 4, 1),
    schedule_interval = '@daily'
)

first_task = DockerOperator(
    task_id='sftp_pipeline_task',
    docker_url='tcp://docker-socket-proxy:2375',
    api_version='auto',
    auto_remove=True,
    image='sftp_pipeline:latest',
    container_name='sftp_pipeline',
    command=["python", "-u", "sftp.py"],
    environment={},
    dag=dag
)

first_task

