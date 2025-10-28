# query_sqlite.py
import sqlite3
import pandas as pd

conn = sqlite3.connect("cod_store_data.db")

print("Top 5 Stores by Volume (AT + REWE)")
print(pd.read_sql_query("""
    SELECT Store_Name, City, Volume, Value, Unique_Store_ID
    FROM store_offtake
    WHERE Country = 'AT' AND Source = 'SOF'
    ORDER BY Volume DESC
    LIMIT 5
""", conn))

conn.close()