from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import os
import time
from tqdm import tqdm
import sqlite3

# 기존 함수 import
from news_fetch import get_news_data, ai_summary_data, save_to_db  # 위 코드를 news_module.py로 저장했다고 가정

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DB_PATH = '/home/kexin/database/news.db'
TABLE_NAME = 'tb_news_clipping'
KEYWORD_LIST = ['삼성', 'LG', '현대', 'SK', '롯데', '테슬라', '애플']
TMP_PATH = '/home/kexin/tmp'

dag = DAG(
    'dag_news_clipping_pipeline_v1',
    default_args=default_args,
    description='뉴스 수집, AI 요약, DB 저장 파이프라인',
    schedule=timedelta(minutes=120),
    catchup=False
)

# 1️⃣ 뉴스 수집
def task_get_news_data(**kwargs):
    naver_key = Variable.get("NAVER_API_KEY")
    df_raw = get_news_data(KEYWORD_LIST,naver_key).sample(5).reset_index(drop=True)
    df_raw.to_csv(f'{TMP_PATH}/raw.csv',index=False)
    print('RAW DATA SAVED!')
    # XCom으로 다음 task에 전달
    kwargs['ti'].xcom_push(key='raw_path', value=f'{TMP_PATH}/raw.csv')

get_news_task = PythonOperator(
    task_id='get_news_data',
    python_callable=task_get_news_data,
    dag=dag
)

# 2️⃣ AI 요약
def task_ai_summary_data(**kwargs):
    ti = kwargs['ti']
    gemini_key = Variable.get("GEMINI_API_KEY")
    raw_path = ti.xcom_pull(key='raw_path', task_ids='get_news_data')
    df_raw = pd.read_csv(raw_path)
    # df_raw = pd.DataFrame(df_records)

    df_ai = ai_summary_data(df_raw,gemini_key)
    df_ai.to_csv(f'{TMP_PATH}/ai.csv',index=False)
    print('AI DATA SAVED!')
    ti.xcom_push(key='ai_path', value=f'{TMP_PATH}/ai.csv')

ai_summary_task = PythonOperator(
    task_id='ai_summary_data',
    python_callable=task_ai_summary_data,
    dag=dag
)

# 3️⃣ DB 저장
def task_save_to_db(**kwargs):
    ti = kwargs['ti']
    ai_path = ti.xcom_pull(key='ai_path', task_ids='ai_summary_data')
    df_ai = pd.read_csv(ai_path)
    # df_ai = pd.DataFrame(df_records)

    save_to_db(DB_PATH, TABLE_NAME, df_ai)

save_to_db_task = PythonOperator(
    task_id='save_to_db',
    python_callable=task_save_to_db,
    dag=dag
)

def task_delete_ai_file(**kwargs):
    ti = kwargs['ti']
    ai_path = ti.xcom_pull(key='ai_path', task_ids='ai_summary_data')
    if ai_path and os.path.exists(ai_path):
        os.remove(ai_path)
        print(f"Deleted file: {ai_path}")
    else:
        print("File not found or path is None.")

delete_ai_task = PythonOperator(
        task_id='delete_ai_file',
        python_callable=task_delete_ai_file
    )

def task_delete_raw_file(**kwargs):
    ti = kwargs['ti']
    raw_path = ti.xcom_pull(key='raw_path', task_ids='get_news_data')
    if raw_path and os.path.exists(raw_path):
        os.remove(raw_path)
        print(f"Deleted file: {raw_path}")
    else:
        print("File not found or path is None.")

delete_raw_task = PythonOperator(
        task_id='delete_raw_file',
        python_callable=task_delete_raw_file
    )

dag_start = EmptyOperator(task_id='dag_start')
dag_end = EmptyOperator(task_id='dag_end')

# Task 순서 지정
dag_start >> get_news_task >> ai_summary_task >> delete_raw_task >> save_to_db_task >> delete_ai_task >> dag_end