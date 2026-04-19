from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from get_link_raw_data import get_raw_object
from processed_data import process_data
from load_data import load_data
from create_insight_views import create_insight_views

# task 1 hàm đầu,task2 nối 2 hàm tiếp theo, task3 nối 2 hàm tiếp theo nữa
def run_process(ti):
    raw_file_minio = ti.xcom_pull(task_ids = "get_raw_object_task")
    processed_file = process_data(raw_file_minio)
    print(f"processed_file: {processed_file}")
    return processed_file
def run_load(ti):
    processed_file_minio = ti.xcom_pull(task_ids = "process_data_task")
    load_data(processed_file_minio)
    print("Run pipeline successful")

#task4 là task cuối sau khi xong tất cả cái kia tự động chạy
def run_create_insight():
    create_insight_views()
    print("Create insight views successful")

#Tạo một cái dag(đồ thị các task có hướng nhưng không có chu trình)
with DAG(
    dag_id = "fahasa_etl_pipeline",
    start_date = datetime(2025,3,24),
    schedule = None,
    catchup = False,
    tags = ["etl","fahasa"]
) as dag:
    crawl_task = PythonOperator(
        task_id = "get_raw_object",
        python_callable = get_raw_object,
        provide_context=True
    )
    process_task = PythonOperator(
        task_id = "process_data",
        python_callable = run_process
    )
    load_task = PythonOperator(
        task_id = "load_data",
        python_callable = run_load
    )
    create_insight_views_task = PythonOperator(
        task_id = "create_insight_views_task",
        python_callable = run_create_insight
    )
    
    #Thứ tự các task
    crawl_task >> process_task >> load_task >> create_insight_views_task