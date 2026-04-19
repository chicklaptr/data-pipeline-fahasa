from playwright.sync_api import sync_playwright
from datetime import datetime
import json
import os 
from minio import Minio


CATEGORY_URLS = [
    {
        "name" : "VĂN HỌC",
        "url" : "https://www.fahasa.com/sach-trong-nuoc/van-hoc-trong-nuoc.html"
    },{
        "name" : "KINH TẾ",
        "url" : "https://www.fahasa.com//sach-trong-nuoc/kinh-te-chinh-tri-phap-ly.html"
    },{
        "name" : "TÂM LÝ - KỸ NĂNG SỐNG",
        "url" : "https://www.fahasa.com/sach-trong-nuoc/tam-ly-ky-nang-song.html"
    }
]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY","minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY","minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET","data-lake")

def crawl_raw_catalog_data():
# tao mot client thao tac tu xa    
    client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


# tao folfer tren minio neu ko co

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        
    raw_data=[]
    seen_urls = set()
    current_context = {"url":None,"category_name" : None}
    def handle_response(response):
        try: 
            if "loadCatalog" in response.url and response.status == 200:
                key = (current_context["category_name"],response.url)
                if key in seen_urls:
                    return
                
                seen_urls.add(key)
                print("FOUND API:", response.url)              
                data = response.json()
            
                raw_data.append({
                    "source_category_name": current_context["category_name"],
                    "source_category_url": current_context["url"],
                    "response_url": response.url,
                    "status": response.status,
                    "headers": dict(response.headers),
                    "crawl_time": datetime.now().isoformat(),
                    "body": data
                })
                
        except Exception as e:
                print("cannot read body:",e)
        
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless= False)
        context = browser.new_context()
        page = context.new_page()
        
        page.on("response",handle_response)
        check_list = {
            "VĂN HỌC" : ["Tiểu Thuyết","Truyện Ngắn - Tản Văn","Ngôn Tình"],
            "KINH TẾ" :["Quản Trị - Lãnh Đạo","Marketing - Bán Hàng","Phân Tích Kinh Tế"],
            "TÂM LÝ - KỸ NĂNG SỐNG" : ["Kỹ Năng Sống","Tâm Lý","Rèn Luyện Nhân Cách"]
        }
        for category in CATEGORY_URLS:
            current_context["url"] = category["url"]
            
            print(f"\n===Crawling category: {category['name']}===")
            page.goto(current_context["url"], wait_until="domcontentloaded")
            page.wait_for_timeout(5000)
            
            #lay text của các phần con
            category_texts = page.locator("a").all_inner_texts()
            #loc
            targets = []
            
            for t in category_texts:
                t = t.strip()
                if t in check_list[category["name"]] and t not in targets:
                    targets.append(t)
            print("cac muc se duoc click: ",targets)
            
            for name in targets:
                current_context["category_name"] = name
                print("Dang click:",name)
                
                page.goto(category["url"],wait_until = "domcontentloaded")
                page.wait_for_timeout(3000)
                try:
                    locator = page.locator("a", has_text=name)
                    clicked = False
                    
                    print("count =", locator.count())
                    for i in range(locator.count()):
                        item = locator.nth(i)
                        if item.is_visible():
                            item.scroll_into_view_if_needed()
                            item.click(force = True)
                            page.wait_for_load_state("domcontentloaded")
                            clicked = True
                            break
                        
                    if not clicked:
                        print(f"Khong tim thay phan tu visible cho: {name}")
                        
                    page.wait_for_timeout(3000)
                
                except Exception as e:
                    print(f"Loi khi click {name}: {e}")
                
                # for _ in range(8):
                #     page.mouse.wheel(0, 1500)
                #     page.wait_for_timeout(1500)
                
            page.wait_for_timeout(5000)
        
        for response in raw_data:
            data = response["body"]
            for p in data["product_list"]:
                try:
                        with page.expect_response(lambda r: "https://www.fahasa.com/node_api/flashsale/product" in r.url and r.status == 200) as resp_info:
                            page.goto(p["product_url"], wait_until="domcontentloaded")
                        resp = resp_info.value
                        res = resp.json()
                        print("kiem tra response")
                        print(res)#có response
                        product = res.get("product",{})#có nếu là flashsale, còn ko thì ko có
                        if not product:
                            print("khong co phan chi tiet san pham")
                        p["stock_available"] = 1 if product.get("buffer_value",0) > 0 else 0
                        p["sold_qty"] = product.get("total_sold")
                except Exception as e:
                    print(f"Loi la: {e}")
                    p["stock_available"] = None
                    p["sold_qty"] = None
                page.wait_for_timeout(1000)
        
        os.makedirs("data/raw",exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file=f"data/raw/fahasa_raw_{timestamp}.json"
        
        with open(raw_file,"w",encoding="utf-8") as f:
            json.dump(raw_data,f,ensure_ascii=False,indent=2)
            
        object_name = f"raw/fahasa_raw_{timestamp}.json"
        client.fput_object(MINIO_BUCKET,object_name,raw_file)
        
        print(f"\nĐã lưu raw data local: {raw_file}")
        print(f"Đã upload MinIO: {object_name}")
        print(f"Số response raw: {len(raw_data)}")
   
        
        browser.close()
        
        return object_name
    
        
    
    
    
    