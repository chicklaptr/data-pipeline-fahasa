from playwright.sync_api import sync_playwright

def handle_reponse(response):
    try:
        if "flash_sale_get_items" in response.url:
            data =  response.json()
            
            items = data.get("data",{}).get("items",[])
            print(f"So item lay duoc: {len(items)}")
    except Exception as e:
        print("loi",e)
        
with sync_playwright() as p:
    browser = p.chromium.launch(headless=False,channel="chrome")
    context = browser.new_context()
    page = browser.new_page()
    
    page.on("response",handle_reponse)
    
    page.goto("https://shopee.vn/", wait_until="domcontentloaded")
    
    page.wait_for_timeout(10000)
    
    input()
    browser.close()