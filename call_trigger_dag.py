import requests
import json

#api của airflow và tên dag_id
AIRFLOW_BASE_URL = "http://localhost:8080"
DAG_ID = "fahasa_etl_pipeline"

def trigger_airflow_dag(raw_object:str):
    #link gọi api airflow kết hợp với phần dag_id
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    
    #cấu hình đầu vào khi chạy
    payload = {
        "conf":{
            "raw_object" : raw_object
            }
    }
    
    #cấu hình đầu ra khi kết thúc
    response = requests.post(
        url,
        auth=("admin", "admin"),
        json= payload
    
    )
    
    #print thông tin dag đã chạy
    print("Trigger status: ",response.status_code)
    print("Trigger response: ",response.text)
    
    
    
    
    
