from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from datetime import datetime

from get_link_raw_data import get_raw_object
from processed_data import process_data
from load_data import load_data
from create_insight_views import create_insight_views
from alert_service import check_and_send_alert,get_process_run_id_from_processed_file


# task 1 hàm đầu,task2 nối 2 hàm tiếp theo, task3 nối 2 hàm tiếp theo nữa
def run_process(ti):
    raw_file_minio = ti.xcom_pull(task_ids = "get_raw_object")
    processed_file = process_data(raw_file_minio)
    print(f"processed_file: {processed_file}")
    return processed_file
def run_load(ti):
    processed_file_minio = ti.xcom_pull(task_ids = "process_data")
    load_data(processed_file_minio)
    print("Run pipeline successful")
    
#task4 alert lỗi về pipeline nếu có 
def run_alert_check(ti):
    import psycopg2

    processed_file_minio = ti.xcom_pull(task_ids="process_data")
    
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        user="admin",
        password="123456",
        database="data_warehouse"
    )
    try:
        if processed_file_minio:
            process_run_id=get_process_run_id_from_processed_file(processed_file_minio)
        else:    
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT process_run_id
                    FROM pipeline_runs
                    ORDER BY started_at DESC
                    LIMIT 1;
                            """)
                row=cur.fetchone()
                
            if not row:
                raise ValueError("Cannot find latest process_run_id in pipeline_runs")
                
            process_run_id=row[0]
        check_and_send_alert(conn, process_run_id)
    finally:
        conn.close()
        
    print("Alert check successful")

#task5 là task cuối sau khi xong tất cả cái kia tự động chạy
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
    alert_task = PythonOperator(
        task_id = "alert_check",
        python_callable=run_alert_check,
        trigger_rule=TriggerRule.ALL_DONE
    )
    create_insight_views_task = PythonOperator(
        task_id = "create_insight_views",
        python_callable = run_create_insight
    )
    
    #Thứ tự các task
    crawl_task >> process_task >> load_task >> alert_task >> create_insight_views_task