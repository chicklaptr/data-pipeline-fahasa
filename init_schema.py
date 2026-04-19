import os
import psycopg2

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123456")
POSTGRES_DB = os.getenv("POSTGRES_DB", "data_warehouse")


def init_schema():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS categories(
        category_id BIGINT PRIMARY KEY,
        category_name TEXT,
        parent_category_id BIGINT,
        category_level INTEGER,
        crawl_time TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id BIGINT PRIMARY KEY,
        product_name TEXT,
        product_url TEXT,
        image_src TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS product_snapshot(
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        product_id BIGINT NOT NULL,
        price INTEGER,
        final_price INTEGER,
        discount_percent INTEGER,
        stock_available BOOLEAN NULL,
        sold_qty INTEGER NULL,
        processed_time TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        UNIQUE (product_id, processed_time)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS product_category_map(
        product_id BIGINT,
        category_id BIGINT,
        crawl_time TIMESTAMP,
        PRIMARY KEY(product_id,category_id),
        FOREIGN KEY(category_id) REFERENCES categories(category_id),
        FOREIGN KEY(product_id) REFERENCES products(product_id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS suppliers (
        supplier_id BIGINT PRIMARY KEY,
        supplier_name TEXT,
        supplier_param TEXT,
        product_count INTEGER,
        crawl_time TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        process_run_id TEXT PRIMARY KEY,
        processed_at TIMESTAMP,
        source_name TEXT,
        raw_file_minio TEXT,
        total_raw_records INTEGER,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status TEXT,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        error_message TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS data_quality_reports(
        process_run_id TEXT PRIMARY KEY,
        total_products_seen INTEGER,
        products_passed INTEGER,
        products_failed INTEGER,
        missing_product_id INTEGER,
        missing_product_name INTEGER,
        missing_product_url INTEGER,
        invalid_price_negative INTEGER,
        invalid_final_price_negative INTEGER,
        final_price_greater_than_price INTEGER,
        duplicate_product_id INTEGER,
        FOREIGN KEY (process_run_id) REFERENCES pipeline_runs(process_run_id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_task_audit(
        audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        process_run_id TEXT,
        task_name TEXT,
        status TEXT,
        records_in INTEGER,
        records_out INTEGER,
        records_rejected INTEGER,
        message TEXT,
        start_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS rejected_products(
        rejected_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        process_run_id TEXT,
        product_id_raw TEXT,
        product_name_raw TEXT,
        product_url_raw TEXT,
        reject_type TEXT,
        reject_reason TEXT,
        crawl_time TIMESTAMP,
        raw_payload JSONB,
        rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Schema initialized successfully.")


if __name__ == "__main__":
    init_schema()