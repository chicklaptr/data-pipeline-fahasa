import psycopg2
import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST","postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT","5432")
POSTGRES_USER = os.getenv("POSTGRES_USER","admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD","123456")
POSTGRES_DB = os.getenv("POSTGRES_DB","data_warehouse")

def create_insight_views():
    sql_files=[
        "/opt/airflow/dags/sql/insights/product_insights.sql",
        "/opt/airflow/dags/sql/insights/category_insights.sql",
        "/opt/airflow/dags/sql/insights/pipeline_insights.sql",
    ]
    
    conn=None
    cur=None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB
        )
        cur = conn.cursor()
        
        for file_path in sql_files:
            print(f"Dang chay file: {file_path}")
            with open(file_path,"r",encoding="utf-8") as f:
                sql_script=f.read()
                cur.execute(sql_script)
                
        conn.commit()
        print("Da tao/cap nhat toan bo insight views")
        
    except Exception as e:
        if conn:
            conn.rollback()
            print(f"Loi khi tao insight views: {e}")
            raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            