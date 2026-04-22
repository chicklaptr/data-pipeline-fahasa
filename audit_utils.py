import os
import psycopg2

#cấu hình của dịch vụ cần kết nối đến
POSTGRES_HOST = os.getenv("POSTGRES_HOST","postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT","5432")
POSTGRES_USER = os.getenv("POSTGRES_USER","admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB","data_warehouse")

#hàm trả về một kết nối đến dịch vụ
def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    
#hàm chèn dữ liệu liên quan đến pipeline vào bảng
def upsert_pipeline_run(process_run_id, processed_at=None, source_name=None,
                        raw_file_minio=None, total_raw_records=None,
                        processed_file_minio=None,status="running", error_message=None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_runs(
            process_run_id,
            processed_at,
            source_name,
            raw_file_minio,
            total_raw_records,
            processed_file_minio,
            started_at,
            status,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s)
        ON CONFLICT (process_run_id)
        DO UPDATE SET
            processed_at = COALESCE(EXCLUDED.processed_at, pipeline_runs.processed_at),
            source_name = COALESCE(EXCLUDED.source_name, pipeline_runs.source_name),
            raw_file_minio = COALESCE(EXCLUDED.raw_file_minio, pipeline_runs.raw_file_minio),
            total_raw_records = COALESCE(EXCLUDED.total_raw_records, pipeline_runs.total_raw_records),
            processed_file_minio = COALESCE(EXCLUDED.processed_file_minio, pipeline_runs.processed_file_minio),
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message;
    """, (
        process_run_id,
        processed_at,
        source_name,
        raw_file_minio,
        total_raw_records,
        processed_file_minio,
        status,
        error_message
    ))
    conn.commit()
    cur.close()
    conn.close()
    
def finish_pipeline_run(process_run_id,status,error_message=None):
    conn=get_connection()
    cur=conn.cursor()
    cur.execute("""
        UPDATE pipeline_runs
        SET status = %s,
        finished_at = CURRENT_TIMESTAMP,
        error_message = %s
        WHERE process_run_id = %s;
                """,(status,error_message,process_run_id))
    conn.commit()
    cur.close()
    conn.close()

def update_task_audit(process_run_id,task_name,status,
                      records_in=0,records_out=0,records_rejected=0,
                      message=None):
    conn=get_connection()
    cur=conn.cursor()
    cur.execute("""
        UPDATE pipeline_task_audit
        SET status =%s,
            records_in=%s,
            records_out=%s,
            records_rejected=%s,
            message=%s,
            finished_at=CURRENT_TIMESTAMP
        WHERE process_run_id=%s
        AND task_name = %s
        AND finished_at IS NULL;
                """,(
                    status,
                    records_in,
                    records_out,
                    records_rejected,
                    message,
                    process_run_id,
                    task_name
                ))
    conn.commit()
    cur.close()
    conn.close()
    
    
def insert_task_audit(process_run_id,task_name,status,
                      records_in=0,records_out=0,records_rejected=0,
                      message=None):
    conn=get_connection()
    cur=conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_task_audit(
            process_run_id,
            task_name, 
            status,
            records_in,
            records_out,
            records_rejected,
            message
        )
        VALUES (%s, %s, %s, %s, %s ,%s, %s);
        """,(
            process_run_id,
            task_name,
            status,
            records_in,
            records_out,
            records_rejected,
            message
        ))
    conn.commit()   
    cur.close()
    conn.close()