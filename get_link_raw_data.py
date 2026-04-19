#lấy link data đã crawl lưu trên tầng raw data của minio từ đầu vào trigger của airflow
def get_raw_object(**context):
    raw_object = context["dag_run"].conf.get("raw_object")
    if not raw_object:
        raise ValueError("Không có raw_object trả về")
    print("Đã có raw_object chuẩn bị")
    return raw_object