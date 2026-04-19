from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    #khởi tạo tình duyệt
    browser = p.chromium.launch(headless = False,slow_mo=200)
    #Tab mới
    page = browser.new_page()
    
    # setup profile browser giống user thật hơn 
    context=browser.new_context(
        viewport={"width": 1366, "height": 768},
        locale="vi-VN",
        timezone_id="Asia/Ho_Chi_Minh",
    )
    #Gửi yêu cầu
    page.goto("https://shopee.vn/search?keyword=iphone",wait_until="domcontentloaded")
    
    
    #Chờ chạy js để load data vào html 
    page.wait_for_timeout(8000)
    
    #In nội dung
    html = page.content()
    print(page.title())
    
    #Đóng trình duyệt giải phóng tài nguyên
    browser.close()
    
