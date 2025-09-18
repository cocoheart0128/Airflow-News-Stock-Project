import os
import warnings
from tqdm import tqdm
warnings.filterwarnings("ignore")
import sys
import time
import urllib.request
from datetime import datetime,timedelta
import json
import re
import pandas as pd
import requests
from pydantic import BaseModel
from bs4 import BeautifulSoup
from google import genai
from pydantic import BaseModel
from typing import List
import sqlite3
import yfinance as yf

def get_data_from_db(DB_PATH,TABLE_NAME):

    conn = sqlite3.connect(DB_PATH,timeout=30)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {TABLE_NAME}")

    # Fetch all results
    rows = cursor.fetchall()
    # Get column names
    columns = [description[0] for description in cursor.description]
    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=columns)
    # Close connection
    conn.close()

    # Display the DataFrame
    return df

def get_stock_data(stock_cd,date):
    end_dt = datetime.strptime(date, "%Y-%m-%d")
    start_dt = (end_dt - timedelta(days=1)).strftime('%Y-%m-%d')

    df_stock = yf.download(stock_cd, start=start_dt, end=end_dt).reset_index(drop=False)
    # df_stock = yf.download(stock_cd, start='2010-01-01', end='2025-09-01').reset_index(drop=False)
    df_stock.columns = [(col[0] if col[1] == '' else col[0]).lower() for col in df_stock.columns.values]
    df_stock['date'] = df_stock['date'].dt.strftime('%Y-%m-%d')
    return df_stock


def save_to_db(DB_PATH,TABLE_NAME,df_ai):

    # conn = sqlite3.connect("/home/kexin/Desktop/code/news_room/data/news.sqlite")
    # conn.close()
    conn = sqlite3.connect(DB_PATH,timeout=30)
    cursor = conn.cursor()

    abs_path = os.path.abspath(DB_PATH)
    print("SQLite DB Path:", abs_path)

# Insert ignoring duplicates (requires custom SQL)
    try:
        df_ai.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
    except sqlite3.IntegrityError as e:
        print(f"Primary key error (duplicate): {e}. Skipping duplicate rows.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    else:
        print(f"{len(df_ai)} rows inserted into {TABLE_NAME}.")

    # df_ai.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
    # print(f"{len(df_ai)} rows inserted into {TABLE_NAME}.")

    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    count = cursor.fetchone()[0]
    print(f"{TABLE_NAME} 테이블 레코드 수: {count}")

    conn.close()


def main():

    DB_PATH = '/home/kexin/database/news.db'
    TABLE_NAME = 'dim_comp_stock_mapping'

    df_dim = get_data_from_db(DB_PATH,TABLE_NAME)

    # Suppose df_dim has columns: 'comp_nm', 'stock_cd'
    all_stock_data = []

    for i, row in df_dim.iterrows():
        stock_cd = row['stock_cd']
        comp_nm = row['comp_nm']
        
        # Fetch stock data for this stock code
        df_stock = get_stock_data(stock_cd,'2025-09-01')
        
        # Optional: add the company name and stock code to the data
        df_stock['comp_nm'] = comp_nm
        df_stock['stock_cd'] = stock_cd

        # Append to the list
        all_stock_data.append(df_stock)

    # Combine all stock data into a single DataFrame
    df_all_stocks = pd.concat(all_stock_data, ignore_index=True)
    df_all_stocks = df_all_stocks[['date','comp_nm','stock_cd','close','high','low','open','volume']]
    df_all_stocks['create_dt']=pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    print(df_all_stocks)
    save_to_db(DB_PATH,'tb_stock_prices',df_all_stocks)


if __name__ == "__main__":
    main()





