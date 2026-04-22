import json
from datetime import datetime
import os 
from minio import Minio 
import io
from audit_utils import insert_task_audit,upsert_pipeline_run,finish_pipeline_run,update_task_audit

#cấu hình liên quan đến địa chỉ và tk,mk đăng nhập cho các service đã được setup
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT","minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY","minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY","minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET","data-lake")

#các cái ngưỡng để có thể kiểm soát chất lượng dữ liệu để có thể dự đoán có nên chạy tiếp hay không 
MAX_FAILURE_RATE = float(os.getenv("MAX_FAILURE_RATE","0.05"))
MIN_VALID_PRODUCTS = int(os.getenv("MIN_VALID_PRODUCTS","10"))
MIN_TOTAL_PRODUCTS  = int(os.getenv("MIN_TOTAL_PRODUCTS","10"))

#lấy thời gian hiện tại để phân biệt giữa các lần crawl
def now_iso():
    return datetime.now().isoformat()
def now_str():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

#hàm chuyển đổi dữ liệu thành dạng phù hợp
def to_int(value):
    try:
        return int(float(value))
    except Exception:
        return None
 
 #hàm này thì sẽ validate lên một sản phẩm và trả về các cái errors của nó    
def validate_product(p:dict):
    errors=[]
    
    product_id =  p.get("product_id")
    product_name = p.get("product_name")
    product_url = p.get("product_url")
    
    price = to_int(p.get("product_price"))
    final_price=to_int(p.get("product_finalprice"))
    
    
    if not product_id:
        errors.append("missing_product_id")
    if not product_name:
        errors.append("missing_product_name")
    if not product_url:
        errors.append("missing_product_url")
    if price is not None and price <0:
        errors.append("invalid_price_negative")
    if final_price is not None and final_price <0:
        errors.append("invalid_final_price_negative")
    if(
        price is not None
        and final_price is not None
        and final_price>price
    ):
        errors.append("final_price_greater_than_price")
    if (price is None or price ==0) and final_price is not None and final_price > 0:
        errors.append("missing_or_zero_price_with_final_price")
    return errors

#hàm này từ cái phần báo cáo lấy ra các cái thông tin tổng hợp và sử dụng nó để kiểm tra xem các cái phần 
#ngưỡng so sánh thế nào từ đó cập nhật trạng thái pipeline ok hay không
def evaluate_quality_gate(quality_report:dict):
    total_seen = quality_report.get("total_products_seen",0)
    passed = quality_report.get("products_passed",0)
    failed = quality_report.get("products_failed",0)
    duplicates = quality_report.get("duplicate_product_id",0)
    
    failure_rate = failed/total_seen if total_seen > 0 else 1.0
    duplicate_rate = duplicates/total_seen if total_seen >0 else 0.0
    
    gate ={
        "total_products_seen":total_seen,
        "products_passed":passed,
        "products_failed":failed,
        "failure_rate":round(failure_rate,4),
        "duplicate_rate":round(duplicate_rate,4),
        "status":"passed",
        "reasons":[]
    }
    if total_seen < MIN_TOTAL_PRODUCTS:
        gate["status"] = "failed"
        gate["reasons"].append(
            f"total_products_seen={total_seen}<MIN_TOTAL_PRODUCTS={MIN_TOTAL_PRODUCTS}"
        )
    if passed < MIN_VALID_PRODUCTS:
        gate["status"]="failed"
        gate["reasons"].append(
            f"products_passed={passed}<MIN_VALID_PRODUCTS={MIN_VALID_PRODUCTS}"
        )
    if failure_rate > MAX_FAILURE_RATE:
        gate["status"]="failed"
        gate["reasons"].append(
            f"failure_rate={failure_rate:.2%}>MAX_FAILURE_RATE={MAX_FAILURE_RATE:.2%}"
        )
    return gate


def process_data(raw_file_minio:str):
    #từ service đã được setup thì sẽ tạo client kết nối đến cái đó để thao tác
    client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)
    #lấy data đã crawl trên minio về để xử lí 
    response = client.get_object(MINIO_BUCKET,raw_file_minio)
    raw_data = json.loads(response.read())
    response.close()
    response.release_conn()

    processed_at = now_iso()    
    # store kind of data
    #clean data
    categories_dict = {}
    products_dict = {}
    product_category_set = set()
    suppliers_dict = {}
    
    #failed data
    bad_products=[]
    #duplicate data
    duplicate_products=[]
    
    #summary happen
    quality_report={
        "total_products_seen":0,
        "products_passed":0,
        "products_failed":0,
        "missing_product_id":0,
        "missing_product_name":0,
        "missing_product_url":0,
        "invalid_price_negative":0,
        "invalid_final_price_negative":0,
        "final_price_greater_than_price":0,
        "duplicate_product_id":0,
        "missing_or_zero_price_with_final_price":0
    }
    
    source_name = None
    
    raw_run_ids=set()
    
    for response_item in raw_data:
        if source_name is None:
            source_name=response_item.get("source_name")
        run_id=response_item.get("run_id")
        if run_id:
            raw_run_ids.add(run_id)
    process_run_id = next(iter(raw_run_ids)) if raw_run_ids else now_str() 

    insert_task_audit(
        process_run_id=process_run_id,
        task_name="process_data",
        status="running"
    )
    for response_item in raw_data:
        body = response_item.get("body",{})
        crawl_time = response_item.get("crawl_time",datetime.now().isoformat())
        
        if not isinstance(body,dict):
            continue
    
        # CATEGRIES
        #kind of category from body and save
        parent_categories = body.get("parent_categories",[])
        current_category = body.get("category")
        children_categories = body.get("children_categories",[])
    
        #parent_category
        for idx,cat in  enumerate(parent_categories):
            if cat:
                categories_dict[cat["id"]] = {
                    "category_id" : cat.get("id"),
                    "category_name" : cat.get("name"),
                    "parent_category_id" : None if idx == 0 else parent_categories[idx-1].get("id"),
                    "category_level" : idx + 1, 
                    "crawl_time" : crawl_time
                    }
                
        #current_category
        current_category_id = current_category.get("id") if current_category else None
        if current_category:
            parent_id = None
            if parent_categories:
                parent_id = parent_categories[-1].get("id")
                
            categories_dict[current_category["id"]] = {
                "category_id" : current_category.get("id"),
                "category_name" : current_category.get("name"),
                "parent_category_id" : parent_id,
                "category_level" : len(parent_categories)+1,
                "crawl_time" : crawl_time
            }
        
        # children_category
        if children_categories:
            for child in children_categories:
                categories_dict[child["id"]] = {
                    "category_id" : child.get("id"),
                    "category_name" : child.get("name"),
                    "parent_category_id" : current_category_id,
                    "category_level" :  len(parent_categories) +2,
                    "crawl_time" : crawl_time
                }
        
            
        # PRODUCTS
        # take product from body and save
        #update report about products
        
        product_list = body.get("product_list",[])
        for p in product_list:
            quality_report["total_products_seen"]+=1
            
            product_id=p.get("product_id")
            errors=validate_product(p)
            
            if errors:
                quality_report["products_failed"]+=1
                for err in errors:
                    if err in quality_report:
                        quality_report[err]+=1
                
                bad_products.append(
                    {
                        "product_id":product_id,
                        "product_name":p.get("product_name"),
                        "product_url":p.get("product_url"),
                        "errors":errors,
                        "crawl_time":crawl_time,
                        "raw_product":p
                    }
                )
                continue
            
            
            if product_id in products_dict:
                quality_report["duplicate_product_id"]+=1
                duplicate_products.append({
                    "product_id":product_id,
                    "product_name":p.get("product_name"),
                    "product_url":p.get("product_url"),
                    "crawl_time":crawl_time,
                    "duplicate_type":"duplicate_product_id",
                    "raw_product":p
                })
                continue
            
            products_dict[product_id]={
            "product_id": product_id,
            "product_name": p.get("product_name"),
            "product_url": p.get("product_url"),
            "price": to_int(p.get("product_price")),
            "final_price": to_int(p.get("product_finalprice")),
            "discount_percent": to_int(p.get("discount")),
            "stock_available": to_int(p.get("stock_available")),
            "sold_qty": to_int(p.get("sold_qty")),
            "image_src": p.get("image_src"),
            "processed_time": crawl_time
            }
            
            quality_report["products_passed"]+=1
            
            if current_category_id:
                product_category_set.add((product_id,current_category_id,crawl_time))
            
        # SUPPLIERS    
            
        # take suppliers from body and save
        attributes =  body.get("attributes",[])
        for attr in attributes:
            if attr.get("code") == "supplier_list":
                options = attr.get("options",[])
                for s in options:
                    supplier_id = s.get("id")
                    if not supplier_id :
                        continue
                    suppliers_dict[s.get("id")] = {
                        "supplier_id" : supplier_id,
                        "supplier_name" : s.get("label"),
                        "supplier_param" : s.get("param"),
                        "product_count" : s.get("count"),
                        "crawl_time" : crawl_time
                    }   
    
    #from report about all products to control quality both pipeline
    quality_gate=evaluate_quality_gate(quality_report)       
    # add information needed about processed_data         
    processed_data = {
        #information about data
        "process_metadata":{
            "process_run_id":process_run_id,
            "processed_at":processed_at,
            "source_name":source_name,
            "raw_file_minio":raw_file_minio,
            "total_raw_records":len(raw_data),
            "raw_run_ids":list(raw_run_ids),
            
        },
        #clean data
        "clean_data":{
            "categories" : list(categories_dict.values()),
            "products" : list(products_dict.values()),
            "product_category_map" : [
                {
                    "product_id" : item[0],
                    "category_id" : item[1],
                    "crawl_time" : item[2]
                } for item in product_category_set
            ],
            "suppliers" : list(suppliers_dict.values())
            
        },
        #rejected data
        "rejected_data":{
        "bad_products":bad_products,
        "duplicate_products":duplicate_products
        },
        #quality report all products
        "quality_report":quality_report,
        #quality pipeline from quality report
        "quality_gate":quality_gate
    }   
    
    # change the form of data to push
    data_bytes = json.dumps(processed_data, ensure_ascii=False).encode("utf-8")
    data_stream = io.BytesIO(data_bytes)

    #position to push
    object_name = f"processed/fahasa_processed_{process_run_id}.json"
    # push processed data to minio
    client.put_object(
        MINIO_BUCKET,
        object_name,
        data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )
    upsert_pipeline_run(
        process_run_id=process_run_id,
        processed_at=processed_at,
        source_name=source_name,
        raw_file_minio=raw_file_minio,
        total_raw_records=len(raw_data),
        processed_file_minio=object_name,
        status="running",
        error_message=None
    )
    #print information about task process
    print(f"Đã có {len(processed_data['clean_data']['categories'])} categories được xử lí")
    print(f"Đã có {len(processed_data['clean_data']['products'])} products được xử lí")
    print(f"Đã có {len(processed_data['clean_data']['product_category_map'])} product-category được xử lí")
    print(f"Đã có {len(processed_data['clean_data']['suppliers'])} suppliers được xử lí")
    print(f"Bad products: {len(processed_data['rejected_data']['bad_products'])}")
    print(f"Duplicate products: {len(processed_data['rejected_data']['duplicate_products'])}")
            
    print(f"Total products seen: {quality_report['total_products_seen']}")
    print(f"Products passed: {quality_report['products_passed']}")
    print(f"Products failed: {quality_report['products_failed']}")
    print(f"Duplicate product_id: {quality_report['duplicate_product_id']}")
    print(f"Đã upload processed data lên minio {object_name}")
    
    print(f"Quality gate status: {quality_gate['status']}")
    print(f"Quality gate reasons: {quality_gate['reasons']}")
    
    #throw exception if pipeline is bad (check from gate)
    if quality_gate["status"]!="passed":
        update_task_audit(
        process_run_id=process_run_id,
        task_name="process_data",
        status="failed",
        records_in=quality_report.get("total_products_seen",0),
        records_out=len(products_dict),
        records_rejected=quality_report.get("products_failed",0)+quality_report.get("duplicate_product_id",0),
        message=f"Quality gate failure: " + "; ".join(quality_gate["reasons"])
    )
        finish_pipeline_run(
            process_run_id=process_run_id,
            status="failed",
            error_message="Quality gate failure: " + "; ".join(quality_gate['reasons'])
        )
        raise ValueError(
            "Quality gate failure: " + "; ".join(quality_gate["reasons"])
            )
    
    update_task_audit(
        process_run_id=process_run_id,
        task_name="process_data",   
        status="success",
        records_in=quality_report.get("total_products_seen",0),
        records_out=len(products_dict),
        records_rejected=quality_report.get("products_failed",0)+quality_report.get("duplicate_product_id",0),
        message=f"quality_gate_status={quality_gate.get('status','unknown')}"
    )
    return object_name