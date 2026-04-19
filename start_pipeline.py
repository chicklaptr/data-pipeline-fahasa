from crawl_data_toi_uu import crawl_raw_catalog_data
from call_trigger_dag import trigger_airflow_dag

#chạy crawl ở local sau đó gọi api chạy tự động airfow
def main():
    result = crawl_raw_catalog_data()
    raw_object = result["raw_object_name"]
    print("RAW_OBJECT: ",raw_object)
    trigger_airflow_dag(raw_object) # bo la kieu A, co la kieu B
    
# chạy đầu tiên trong file
if __name__ == "__main__":
    main()