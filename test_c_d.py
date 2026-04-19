import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5544,
    user="admin",
    password="123456",
    database="data_warehouse"
)

print("Connected PostgreSQL OK")
conn.close()