import pandas as pd
import numpy as np

df = pd.read_json("data/processed/fahasa_processed_20260319_010114.json")
# print(df.loc[(df.product_id >852295) |(df.product_id <852292),["product_id"] ])


# print(df.iloc[[4]],type(df.iloc[[4]]))

# print(df.iloc[3:11])

# # gia su train xong predict module ta muon test gia tri o cuoi khi co dau vao roi
# x=df.iloc[3:5,0:-1] # gia tri dau vao de test
# y=df.iloc[3:5,-1] # gia tri can du doan
# print(x)
# print(y)

# print(df.product_id.dtype)

# df.product_id.apply(lambda x: string(x).replace('8','0'))
# print(df.product_id)
# df["copy_product_id"] = df["product_id"]
# # print(df.copy_product_id)

# # tong so product
# total_product=df["copy_product_id"].sum()
# print(total_product)

# a=df.groupby("product_name")["final_price"].sum()
# print(a.sort_values(ascending=False).head(5))

print(df.product_name.value_counts().count())
print(df.product_name.nunique())

