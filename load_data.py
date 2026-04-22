import json
from minio import Minio
import psycopg2
import os
from audit_utils import finish_pipeline_run,insert_task_audit,update_task_audit

#thông tin liên quan đến nơi lưu trữ processed data (vị trí, tk,mk xác nhận kết nối)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT","minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY","minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY","minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET","data-lake")

#thông tin liên quan đến nơi nạp dữ liệu vào để chuẩn bị phân tích(vị trí,tk,mk xác minh để kết nối)
POSTGRES_HOST = os.getenv("POSTGRES_HOST","postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT","5432")
POSTGRES_USER = os.getenv("POSTGRES_USER","admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB","data_warehouse")

#hàm thực thi chính
def load_data(processed_file_minio:str):
    #tạo client kết nối với service nơi lưu lưu processed_data để lấy data về 
    client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)   
    
    conn=None
    cur=None
    response=None
    process_run_id=None

    try:
        #lấy processed data từ minio
        response=client.get_object(MINIO_BUCKET,processed_file_minio)
        processed_data=json.loads(response.read())
        
        #lấy ra từng phần của processed data
    
        #thông tin về data lấy được
        process_metadata = processed_data.get("process_metadata") or {}
        
        #lấy tất cả task trong cùng một pipeline có run_id chung
        process_run_id = process_metadata.get("process_run_id")
        if not process_run_id:
            raise ValueError("Missing process_run_id in processed_data")
        
        insert_task_audit(
            process_run_id=process_run_id,
            task_name="load_data",
            status="running"
        )
        
        #tóm tắt chất lượng của tất cả products
        quality_report = processed_data.get("quality_report")
        #chất lượng của cả pipeline có tốt không suy ra từ tất cả products
        quality_gate = processed_data.get("quality_gate",{})
        
        #những products không clean
        rejected_data = processed_data.get("rejected_data",{})
        #những sản phẩm bị failed
        bad_products = rejected_data.get("bad_products",[])
        #những sản phẩm bị duplicate
        duplicate_products = rejected_data.get("duplicate_products",[])
        
        if not isinstance(quality_report, dict):
            raise ValueError("Processed file không có 'quality_report' hợp lệ")
        
        # clean data
        clean_data = processed_data.get("clean_data")
        if not isinstance(clean_data, dict):
            raise ValueError("Processed file không có 'clean_data' hợp lệ")
        
        #chia các phần clean data
        categories = clean_data.get("categories",[])
        products = clean_data.get("products",[])
        product_category_map = clean_data.get("product_category_map",[])
        suppliers = clean_data.get("suppliers",[])
        
        #kết nối với nơi cần để load data
        conn=psycopg2.connect(
            host = POSTGRES_HOST,
            port = POSTGRES_PORT,
            user = POSTGRES_USER,
            password = POSTGRES_PASSWORD,
            database = POSTGRES_DB
        )

        #khi kết nối xong thì dùng cur để thao tác
        cur=conn.cursor()


        #insert into tables
        #insert clean data
        insert_categories_sql ="""
        INSERT INTO categories(
            category_id,
            category_name,
            parent_category_id,
            category_level,
            crawl_time
        )
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT(category_id)
        DO UPDATE SET
            category_name = EXCLUDED.category_name,
            parent_category_id = EXCLUDED.parent_category_id,
            category_level = EXCLUDED.category_level,
            crawl_time = EXCLUDED.crawl_time;
        """
        
        insert_products_sql = """
        INSERT INTO products (
            product_id,
            product_name,
            product_url, 
            image_src    
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            product_url = EXCLUDED.product_url,
            image_src = EXCLUDED.image_src;
        """
        
        insert_product_snapshot_sql = """
        INSERT INTO product_snapshot(
            product_id,
            price,
            final_price,
            discount_percent,
            stock_available,
            sold_qty,
            processed_time
        )
        VALUES(%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (product_id, processed_time)
        DO UPDATE SET
            price = EXCLUDED.price,
            final_price = EXCLUDED.final_price,
            discount_percent = EXCLUDED.discount_percent,
            stock_available = EXCLUDED.stock_available,
            sold_qty = EXCLUDED.sold_qty;
        """
        insert_product_category_map_sql = """
        INSERT INTO product_category_map(
            product_id,
            category_id,
            crawl_time
        )
        VALUES (%s,%s,%s)
        ON CONFLICT (product_id,category_id)
        DO UPDATE SET
            crawl_time = EXCLUDED.crawl_time;
        """
        
        insert_suppliers_sql = """
        INSERT INTO suppliers(
            supplier_id,
            supplier_name,
            supplier_param,
            product_count,
            crawl_time
        )
        VALUES(%s,%s,%s,%s,%s)
        ON CONFLICT (supplier_id)
        DO UPDATE SET
            supplier_name = EXCLUDED.supplier_name,
            supplier_param = EXCLUDED.supplier_param,
            product_count = EXCLUDED.product_count,
            crawl_time = EXCLUDED.crawl_time;
        """
        
        #summary all products of pipeline
        insert_data_quality_reports_sql= """
        INSERT INTO data_quality_reports(
            process_run_id,
            total_products_seen,
            products_passed,
            products_failed,
            missing_product_id,
            missing_product_name,
            missing_product_url,
            invalid_price_negative,
            invalid_final_price_negative,
            final_price_greater_than_price,
            missing_or_zero_price_with_final_price,
            duplicate_product_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (process_run_id)
        DO UPDATE SET
            total_products_seen = EXCLUDED.total_products_seen,
            products_passed = EXCLUDED.products_passed,
            products_failed = EXCLUDED.products_failed,
            missing_product_id = EXCLUDED.missing_product_id,
            missing_product_name = EXCLUDED.missing_product_name,
            missing_product_url = EXCLUDED.missing_product_url,
            invalid_price_negative = EXCLUDED.invalid_price_negative,
            invalid_final_price_negative = EXCLUDED.invalid_final_price_negative,
            final_price_greater_than_price = EXCLUDED.final_price_greater_than_price,
            missing_or_zero_price_with_final_price = EXCLUDED.missing_or_zero_price_with_final_price,
            duplicate_product_id = EXCLUDED.duplicate_product_id;
        """ 
        #insert rejected products
        insert_rejected_products_sql='''
        INSERT INTO rejected_products(
            process_run_id,
            product_id_raw,
            product_name_raw,
            product_url_raw,
            reject_type,
            reject_reason,
            crawl_time,
            raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb);
        '''
        
        select_latest_snapshot_sql='''
        SELECT
            price,
            final_price,
            discount_percent,
            stock_available,
            sold_qty
        FROM product_snapshot
        WHERE product_id = %s
        ORDER BY processed_time DESC
        LIMIT 1;
        '''
        
        #load data
        #load quality of all products of pipeline
        cur.execute(
            insert_data_quality_reports_sql,
            (
                process_metadata.get("process_run_id"),
                quality_report.get("total_products_seen"),
                quality_report.get("products_passed"),
                quality_report.get("products_failed"),
                quality_report.get("missing_product_id"),
                quality_report.get("missing_product_name"),
                quality_report.get("missing_product_url"),
                quality_report.get("invalid_price_negative"),
                quality_report.get("invalid_final_price_negative"),
                quality_report.get("final_price_greater_than_price"),
                quality_report.get("missing_or_zero_price_with_final_price"),
                quality_report.get("duplicate_product_id"),
            )
        )
        #load clean data
        for item in categories:
            cur.execute(
                insert_categories_sql,
                (
                    item.get("category_id"),
                    item.get("category_name"),
                    item.get("parent_category_id"),
                    item.get("category_level"),
                    item.get("crawl_time")
                )
            )
        
        snapshot_inserted=0
        snapshot_skipped=0
        for item in products:
            cur.execute(
                insert_products_sql,
                (
                    item.get("product_id"),
                    item.get("product_name"),
                    item.get("product_url"),
                    item.get("image_src")
                )
            )
            
            product_id=item.get("product_id")
            new_price=item.get("price")
            new_final_price=item.get("final_price")
            new_discount_percent=item.get("discount_percent")
            new_stock_available=item.get("stock_available")
            new_sold_qty=item.get("sold_qty")
            new_processed_time=item.get("processed_time")
            
            cur.execute(select_latest_snapshot_sql,(product_id,))
            latest_snapshot = cur.fetchone()
            
            should_insert_snapshot = False
            
            if latest_snapshot is None:
                should_insert_snapshot = True
                
            else:
                old_price,old_final_price,old_discount_percent,old_stock_available,old_sold_qty=latest_snapshot
                
                if (
                    old_price != new_price or
                    old_final_price != new_final_price or
                    old_discount_percent != new_discount_percent or
                    old_stock_available != new_stock_available or 
                    old_sold_qty != new_sold_qty
                ):
                    should_insert_snapshot = True
            
            if should_insert_snapshot:
                cur.execute(
                    insert_product_snapshot_sql,
                    (
                        product_id,
                        new_price,
                        new_final_price,
                        new_discount_percent,
                        new_stock_available,
                        new_sold_qty,
                        new_processed_time
                    )
                )
                snapshot_inserted+=1
            else:
                snapshot_skipped+=1
                       
        for item in product_category_map:
            cur.execute(
                insert_product_category_map_sql,(
                    item.get("product_id"),
                    item.get("category_id"),
                    item.get("crawl_time")
                )
            )
            
        for item in suppliers:
            cur.execute(
                insert_suppliers_sql,
                (
                    item.get("supplier_id"),
                    item.get("supplier_name"),
                    item.get("supplier_param"),
                    item.get("product_count"),
                    item.get("crawl_time")
                )
            )
     
        #load rejected products
        for item in bad_products:
            cur.execute(
                insert_rejected_products_sql,
                (
                    process_run_id,
                    str(item.get("product_id")) if item.get("product_id") is not None else None,
                    item.get("product_name"),
                    item.get("product_url"),
                    "bad_product",
                    ",".join(item.get("errors",[])),
                    item.get("crawl_time"),
                    json.dumps(item.get("raw_product",{}),ensure_ascii=False)
                )
            )
        for item in duplicate_products:
            cur.execute(
                insert_rejected_products_sql,
                (
                    process_run_id,
                    str(item.get("product_id")) if item.get("product_id") is not None else None,
                    item.get("product_name"),
                    item.get("product_url"),
                    "duplicate_product",
                    item.get("duplicate_type"),
                    item.get("crawl_time"),
                    json.dumps(item.get("raw_product",{}),ensure_ascii=False)
                )
            )
        
        conn.commit()
        #load trạng thái task va pipeline(có thể success)
        update_task_audit(
            process_run_id=process_run_id,
            task_name="load_data",
            status="success",
            records_in=quality_report.get("total_products_seen",0),
            records_out=len(products),
            records_rejected=len(bad_products)+len(duplicate_products),
            message=(f"quality_gate_status={quality_gate.get('status','unknown')}; "
                     f"snapshot_inserted={snapshot_inserted}; "
                     f"snapshot_skipped={snapshot_skipped}"        
            )
        )
        
        finish_pipeline_run(
            process_run_id=process_run_id,
            status="success",
            error_message=None
        )
        
        #print information liên quan đến task này
        print("Load data thành công")
        
        print("Da load 1 pipeline_run vao Postgres")
        print("Da load 1 data_quality_report vao Postgres")
        
        print(f"Da load {len(categories)} categories vao Postgres")
        print(f"Da load {len(products)} products vao Postgres")
        print(f"Da load {len(product_category_map)} product_category_map vao Postgres")
        print(f"Da load {len(suppliers)} suppliers vao Postgres")
        
        print(f"Da load {snapshot_inserted} snapshots moi")
        print(f"Da skip {snapshot_skipped} snapshots khong thay doi")
        
    except Exception as e:
        if conn is not None:
            conn.rollback()
        try:
            #load thông tin chi tiết về task va pipeline(kể cả có lỗi thì vẫn là thông tin xác nhận tốt xấu cho pipeline)
            update_task_audit(
                process_run_id=process_run_id,
                task_name="load_data",
                status="failed",
                records_in=0,
                records_out=0,
                records_rejected=0,
                message=str(e)
            )
            finish_pipeline_run(
                process_run_id=process_run_id,
                status="failed",
                error_message=str(e)
            )
            
        except Exception:
            pass
        print(f"Lỗi khi load data: {e}")
        raise
    
    #đóng kết nối
    finally:
        if response is not None:
            response.close()
            response.release_conn()
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()