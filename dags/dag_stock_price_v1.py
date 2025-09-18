from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os

# Import your existing module
from stock_collect import get_data_from_db, get_stock_data, save_to_db

# =====================
# Configuration
# =====================
DB_PATH = '/home/kexin/database/news.db'
DIM_TABLE = 'dim_comp_stock_mapping'
TARGET_TABLE = 'tb_stock_prices'
TMP_PATH = '/home/kexin/tmp'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_stock_pipeline_v1',
    default_args=default_args,
    description='Stock data ETL pipeline using stock_collect functions',
    schedule=timedelta(days=1),  # run daily
    catchup=True
)

# =====================
# Task 1: Extract
# =====================
def task_extract_dim_table(**kwargs):
    df_dim = get_data_from_db(DB_PATH, DIM_TABLE)[['comp_nm','stock_cd']]
    # Push to XCom
    kwargs['ti'].xcom_push(key='df_dim', value=df_dim.to_dict(orient='records'))

extract_dim_table_task = PythonOperator(
    task_id='extract_dim_table',
    python_callable=task_extract_dim_table,
    dag=dag
)

# =====================
# Task 2: Transform
# =====================
def task_transform_stock_data(**kwargs):
    ti = kwargs['ti']
    etl_date = kwargs['logical_date'].strftime('%Y-%m-%d')
    print(f"Execution date (run_date): {etl_date}")

    dim_records = ti.xcom_pull(key='df_dim', task_ids='extract_dim_table')
    
    all_stock_data = []
    for row in dim_records:
        stock_cd = row['stock_cd']
        comp_nm = row['comp_nm']
        df_stock = get_stock_data(stock_cd,etl_date)
        df_stock['comp_nm'] = comp_nm
        df_stock['stock_cd'] = stock_cd
        all_stock_data.append(df_stock)
    
    if all_stock_data:
        df_all_stocks = pd.concat(all_stock_data, ignore_index=True)
        df_all_stocks = df_all_stocks[['date','comp_nm','stock_cd','close','high','low','open','volume']]
        df_all_stocks['create_dt']=pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    else:
        print("No stock data fetched.")

    df_all_stocks.to_csv(f'{TMP_PATH}/stock.csv',index=False)
        # Push to XCom
    ti.xcom_push(key='stock_path', value=f'{TMP_PATH}/stock.csv')

transform_stock_data_task = PythonOperator(
    task_id='transform_stock_data',
    python_callable=task_transform_stock_data,
    dag=dag
)

# =====================
# Task 3: Load
# =====================
def task_load_to_db(**kwargs):
    ti = kwargs['ti']
    stock_path = ti.xcom_pull(key='stock_path', task_ids='transform_stock_data')
    
    if stock_path:
        df_stock = pd.read_csv(stock_path)
        save_to_db(DB_PATH, TARGET_TABLE, df_stock)
    else:
        print("No stock data to insert.")

load_to_db_task = PythonOperator(
    task_id='load_to_db',
    python_callable=task_load_to_db,
    dag=dag
)

def task_stock_file(**kwargs):
    ti = kwargs['ti']
    stock_path = ti.xcom_pull(key='stock_path', task_ids='transform_stock_data')
    if stock_path and os.path.exists(stock_path):
        os.remove(stock_path)
        print(f"Deleted file: {stock_path}")
    else:
        print("File not found or path is None.")

delete_stock_task = PythonOperator(
        task_id='delete_stock_file',
        python_callable=task_stock_file
    )



dag_start = EmptyOperator(task_id='dag_start')
dag_end = EmptyOperator(task_id='dag_end')

# =====================
# DAG Dependencies
# =====================
dag_start >> extract_dim_table_task >> transform_stock_data_task >> load_to_db_task >> delete_stock_task  >> dag_end