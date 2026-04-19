from playwright.sync_api import sync_playwright
from datetime import datetime

products=[]
with sync_playwright() as p:
    
    browser = p.chromium.launch(headless=False, channel="chrome")
    context = browser.new_context()
    page = context.new_page()
    
    def to_int(value):
        try:
            return int(float(value))
        except:
            return None
        
    def handle_response(response):
          if "tabslider/index/getdata" in response.url and response.status==200:
            print("FOUND API:", response.url)
            print("STATUS:", response.status)
            print("CONTENT-TYPE:", response.headers.get("content-type"))

            try:
                data = response.json()
                if not isinstance(data, list):
                    return
                for item in data:
                    p={}
                    p["product_id"] = item.get("id")
                    p["product_name"] = item.get("name_a_title")
                    p["product_url"] = item.get("product_url")
                    p["price"] = to_int(item.get("price"))
                    p["final_price"] = to_int(item.get("final_price"))
                    p["discount_percent"] = to_int(item.get("discount_percent"))
                    p["stock__available"] = item.get("stock_available")=="in_stock"
                    p["sold_qty"] = to_int(item.get("sold_qty"))
                    p["image_src"] = item.get("image_src")
                    p["crawl_time"] = datetime.now().isoformat()
                    products.append(p)
                    
            except Exception as e:
                print("cannot read body:", e)

    page.on("response", handle_response)

    page.goto("https://www.fahasa.com/")

    for i in range(5):     
        page.mouse.wheel(0, 3000)
        page.wait_for_timeout(2000)
    
    input("press enter to exit...")
    print(len(products))
    print(products)
    browser.close()