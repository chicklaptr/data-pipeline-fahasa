from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, channel="chrome")
    page = browser.new_page()

    page.goto("https://www.fahasa.com/")

    # 🔥 đợi product xuất hiện
    page.wait_for_selector("img.lazyloaded", timeout=15000)

    html = page.content()
    print("HTML length:", len(html))

    soup = BeautifulSoup(html, "html.parser")

    # test product
    images = soup.find_all("img", class_="lazyloaded")

    print("Products found:", len(images))
    

    browser.close()