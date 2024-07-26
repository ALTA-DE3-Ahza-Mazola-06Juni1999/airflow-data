from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


dag = DAG(
    'zola_task1_airflow',
    description='DAG sederhana yang berjalan setiap 5 jam',
    schedule_interval=timedelta(hours=5),
    start_date=datetime(2023, 7, 1),
    catchup=False,
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

def push_xcom(**kwargs):
    value = 'nilai_saya'
    kwargs['ti'].xcom_push(key='kunci_saya', value=value)

push_task = PythonOperator(
    task_id='push_task',
    provide_context=True,
    python_callable=push_xcom,
    dag=dag,
)

def pull_xcoms(**kwargs):
    ti = kwargs['ti']
    value1 = ti.xcom_pull(task_ids='push_task', key='kunci_saya')
    value2 = ti.xcom_pull(task_ids='another_task', key='kunci_lain')
    print(f'Nilai yang ditarik: {value1}, {value2}')

pull_task = PythonOperator(
    task_id='pull_task',
    provide_context=True,
    python_callable=pull_xcoms,
    dag=dag,
)

start >> push_task >> pull_task
