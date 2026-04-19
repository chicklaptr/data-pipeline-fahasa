import requests

url = "https://www.fahasa.com/fahasa_catalog/product/loadCatalog?category_id=4&currentPage=1&limit=24&order=num_orders&series_type=0"

params = {
    "category_id": 6426,
    "filters[price]": "150000,300000",
    "currentPage": 1,
    "limit": 24,
    "order": "num_orders",
    "series_type": 0
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.fahasa.com/",
    "Origin": "https://www.fahasa.com"
}

res = requests.get(url, params=params, headers=headers)

print(res.status_code)
print(res.text[:200])