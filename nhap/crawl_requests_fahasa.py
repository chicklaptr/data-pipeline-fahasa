import requests

url  = "https://www.fahasa.com"

headers = {
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.fahasa.com/",
    "Cookie":"frontend=3a575d734da34da38dbd07bf493a4fda; utm_source=google; _ga=GA1.1.652440339.1773820251; _gcl_gs=2.1.k1$i1773820248$u250953233; _gcl_au=1.1.1826881778.1773820251; _fbp=fb.1.1773820250895.68131327111374487; _clck=qjh3zd%5E2%5Eg4g%5E0%5E2268; _tt_enable_cookie=1; _ttp=01KKZYT43S96S45HV5WEVADGQQ_.tt.1; moe_c_s=1; moe_uuid=fa5061dc-7074-4d5c-ae2f-8dd4e873c3fa; moe_u_d=HcdBCoAgEAXQu8w6N5OT2mVk5I8QCEHmKrp70vI91PPZQHvV1m0hZAVs-r7Gz3FMkPm4CjM7CJvzSYIrouq8AQGpplg2ej8; _clsk=yed6g0%5E1773820258658%5E1%5E1%5Eq.clarity.ms%2Fcollect; moe_s_a_s=5; moe_o_s_t=1773820268413; moe_h_a_s=0; moe_s_n=RZBdT8MgFIb_Cxe7Wh3rt0saUzc759RZ48fUGEIp7ZiDzgKt0fjfpTVTbuB9eHnP4XwBid7ABBSeQz1YjC0vw77lUmJbuPALi2A3hK6fQZvkYGjMEikwGQeBE9rQ9kInsDvKexpCOAQEMaQ6-fLa3dB_vw0DN3D8IRCoQhJMnM5sdtOD6aCsqnJHTQ1uBNmTvpo2541SezkZjdq2PSrwBkt8RCo-0trKMbMko9pS1YlWHMlK14RGv0mDjnCaM80jE9dLgvkes1JEN1fxGt2tkvtfXAlFhYp6oWjNo0GJ80PcuBeHpyyP7PHxsQu90HPhoMxqbBCMu2U943mT6rVIfHKzumvrj4skXNjzC8auByXZGeN0C5fTdNtyLa5PN7Msvl3IeCXy5Z4nXM6269jxUqmzZFe_ze9zd_rZNMnt43v5tMBOw5cPmSjwQzE7j2fNpZvimKX1WXzZopacmokRJMzE_n4Hvr9_AA; _ga_D3YYPWQ9LN=GS2.1.s1773820250$o1$g1$t1773820281$j29$l0$h0; _gcl_aw=GCL.1773820282.Cj0KCQjwmunNBhDbARIsAOndKpmFmsDjXA35QsubFlrkGUd4CzvvFRWqgYIa3vmKVbnfaVfDHADvL4QaAiQrEALw_wcB; _ga_460L9JMC2G=GS2.1.s1773820250$o1$g1$t1773820282$j28$l0$h893875343; ttcsid=1773820252284::aLRdiRrpJJ29PYRSfXz9.1.1773820282477.0; ttcsid_CCUF92BC77U9S7CCBOSG=1773820252284::OjALlgjYydI9phdJxJqb.1.1773820282477.1"
}

r = requests.get(url, headers=headers, timeout=20)

print(r.status_code)
print(r.headers.get("content-type"))
print(r.text[:1000])