import os
import warnings
from tqdm import tqdm
warnings.filterwarnings("ignore")
import sys
import time
import urllib.request
from datetime import datetime
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


naver_key = os.environ["NAVER_API_KEY"]
gemini_key = os.environ["GEMINI_API_KEY"]
client_id = "79KGKOz94s8jK6bv0wV2"
client_secret = naver_key


def parsing_webpage(url):
    res = requests.get(url, verify=False)
    soup = BeautifulSoup(res.text, "html.parser")

    meta = {}
    # 1. 제목
    meta["title"] = soup.select_one('meta[property="og:title"]')["content"]

    # 2. 설명 (기사 요약)
    meta["description"] = soup.select_one('meta[property="og:description"]')["content"]

    # 3. 기사 URL
    meta["url"] = soup.select_one('meta[property="og:url"]')["content"]

    # 4. 대표 이미지
    meta["image"] = soup.select_one('meta[property="og:image"]')["content"]

    # 5. 기자/언론사 정보
    meta["media"] = soup.select_one('meta[property="og:article:author"]')["content"].split('|')[0].replace(' ','')
    
    #6. 뉴스본문 
    meta['content'] =soup.select_one("article#dic_area").get_text(" ", strip=True) if soup.select_one("article#dic_area") else ""

    # 7. 기자 정보
    report_tx=soup.select_one("div.byline span.byline_s").get_text(strip=True) if soup.select_one("div.byline span.byline_s") else ""
    meta['reporter_nm'] = "".join(re.findall(r'[가-힣\s]+', report_tx)).strip().replace('기자','')
    meta['reporter_email'] = "".join(re.findall(r'[A-Za-z@._]+', report_tx)).strip()
    # print(meta)

    return meta


class NewsSummary(BaseModel):
    # 3단 요약 문자열, 각 단락에 키워드 레이블 포함
    summary: str  
    # 3단계 키워드를 '키워드1 >> 키워드2 >> 키워드3' 형태로 저장
    keyword: str  
    # 뉴스 감정 분석: '긍정', '부정', '중립'
    sentiment: str
    # 뉴스 감정 분석: '긍정', '부정', '중립'
    sentiment_score: float

def gemini_ai_summary(news_text,gemini_key):
    # ----------------------------
    # 2️⃣ Client 초기화
    # ----------------------------
    client = genai.Client(api_key=gemini_key)
    # news_text = df['content'][0]

    # ----------------------------
    # 4️⃣ 프롬프트 (JSON 구조 요청)
    # ----------------------------
    prompt = f"""
    뉴스 내용을 분석하여 JSON으로 출력하세요. 
    반드시 아래 형식만 사용하고, 추가 설명 금지.
    산업분야 키워드에 대해서 아래와 같은 구조로 되어 있어야하고 아래 키워드말고 추가적으로 해도 된다.
    대분류(키워드1),중분류(키워드2),소분류(키워드3)
    재무·실적,실적 발표,매출
    재무·실적,실적 발표,영업이익
    재무·실적,실적 발표,순이익
    신사업·R&D,기술 개발,AI
    신사업·R&D,기술 개발,반도체

    1. 뉴스 3단 요약 (summary) - 각 단락 앞에 '· [핵심문구N]' 레이블 붙이기 - [핵심문구]는 6글자 이상 15글자 미만 - /n로 단락 바꾸기
    2. 3단계 뉴스 산업 분야 키워드(keyword) - '키워드1 >> 키워드2 >> 키워드3', 각 키워드 5글자 이하
    3. 뉴스 감정(sentiment) - sentiment는 뉴스가 해당 기업 주가에 미치는 영향으로 판단 (긍정, 부정, 중립)
    4. 뉴스 감정 SCORE - -1.00 ~ 1.00 사이 수치 (부정에 가까울수록 -1.00, 긍정에 가까울수록 +1.00, 중립은 0.00)


    뉴스 내용:
    {news_text}

    출력 예시(JSON):
    JSON 출력 예시:
        {{
        "summary": "· [블루패스] 새로운 구독 서비스의 핵심 요소 중 하나인 '블루패스'가 도입되어 제품 설치부터 AS까지 모든 과정에서 고객 편의를 높인다. 이 서비스에는 횟수 제한 없이 우선적으로 AS를 받을 수 있는 'AS 패스트트랙'과 구독 제품 방문 케어 시 다른 삼성 가전을 추가 비용 없이 점검해주는 '하나 더 서비스'가 포함됨./n
    · [고객 편의 강화] 이 외에도 제품 이상 징후를 미리 감지하고 수리 접수까지 해주는 'AI 사전케어 알림', 제품 설치 시 스마트싱스 연결을 돕는 '스마트싱스 세팅' 서비스 등 다양한 고객 맞춤형 기능들이 제공된므로 삼성전자 관계자는 이번 새로운 구독 서비스가 고객 편의 강화와 선택의 폭 확대를 최우선으로 고려했다고 밝혔음."
    ",
        "keyword": "가스/에너지 >> 모빌리티 >> 택시",
        "sentiment": "긍정",
        "sentiment_score": 0.73
    
        }}
    """

    # ----------------------------
    # 5️⃣ 모델 호출
    # ----------------------------
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config={
            "response_mime_type": "application/json",
            "response_schema": NewsSummary,
        }
    )

    return response.parsed



def get_news_data(keyword_list,client_secret):

    all_dfs =[]

    for keyword in keyword_list:

        params = {"query": keyword, "sort": "sim"}
        url = "https://openapi.naver.com/v1/search/news"
        headers = {
            "X-Naver-Client-Id": client_id,
            "X-Naver-Client-Secret": client_secret
        }

        res = requests.get(url, headers=headers, params=params,verify=False)

        df = pd.DataFrame(res.json()['items'])
        df['image']=''
        df['content']=''
        df['media']=''
        df['reporter_nm']=''
        df['reporter_email']=''
        df = df[df['link'].str.startswith('https://n.news.naver.com/')].reset_index(drop=True)

        add_cols = ['image','content','media','reporter_nm','reporter_email']

        for i in range(len(df)):
            url = df['link'][i]
            data = parsing_webpage(url)
            for col in add_cols:
                df.loc[i, col]=data[col]

        
        df['pubDate'] = pd.to_datetime(df['pubDate'], utc=True)
        df['pubDate'] = df['pubDate'].dt.tz_convert("Asia/Seoul")
        # df['date'] = df['pubDate'].dt.strftime("%Y-%m-%d")
        df['pubDate'] = df['pubDate'].dt.strftime("%Y-%m-%d %H:%M:%S")
        df['comp'] = keyword  

        all_dfs.append(df)

        # 여러 키워드 데이터프레임 합치기
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame()

    time.sleep(5)

    return final_df 

def ai_summary_data(df,gemini_key):

    for i in tqdm(range(len(df))):
        content = df.loc[i,'content']
        res = gemini_ai_summary(content,gemini_key)
        df.loc[i,'ai_summary'] = res.summary
        df.loc[i,'ai_keyword'] = res.keyword
        df.loc[i,'ai_sentiment'] = res.sentiment
        df.loc[i,'ai_sentiment_score'] = res.sentiment_score
        df.loc[i,'insert_dt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df.loc[i,'insert_user'] = 'src'

        time.sleep(5)


    df.columns=['title', 'org_link', 'link', 'description', 'pub_dt', 'image',
        'content', 'media', 'reporter_nm', 'reporter_email', 'comp_nm',
        'ai_summary', 'ai_keyword', 'ai_sentiment','ai_sentiment_score', 'insert_dt', 'insert_user']
    df=df[['comp_nm','title', 'org_link', 'link', 'description', 'pub_dt', 'image',
        'content', 'media', 'reporter_nm', 'reporter_email', 
        'ai_summary', 'ai_keyword', 'ai_sentiment','ai_sentiment_score', 'insert_dt', 'insert_user']]
    
    return df

def save_to_db(DB_PATH,TABLE_NAME,df_ai):

    # conn = sqlite3.connect("/home/kexin/Desktop/code/news_room/data/news.sqlite")
    # conn.close()
    conn = sqlite3.connect(DB_PATH,timeout=30)
    cursor = conn.cursor()

    abs_path = os.path.abspath(DB_PATH)
    print("SQLite DB Path:", abs_path)

    df_ai.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
    print(f"{len(df_ai)} rows inserted into {TABLE_NAME}.")

    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    count = cursor.fetchone()[0]
    print(f"{TABLE_NAME} 테이블 레코드 수: {count}")

    conn.close()


def main():

    DB_PATH = 'Desktop/code/news_room/data/news.db'
    TABLE_NAME = 'tb_news_clipping'

    keyword_list = ['삼성', 'LG', '현대', 'SK', '롯데', '테슬라', '애플']

    df_raw = get_news_data(keyword_list)
    # df_raw.to_csv('file_raw.csv',index=False,encoding='utf-8-sig')
    # print(df_raw)


    # ai_summary_data_and_insert_db(df_raw,DB_PATH,TABLE_NAME)

    df_ai = ai_summary_data(df_raw.sample(1).reset_index(drop=True))
    # print(df_ai)
    # df_ai.to_csv('file_ai.csv',index=False,encoding='utf-8-sig')
    save_to_db(DB_PATH,TABLE_NAME,df_ai)


if __name__ == "__main__":
    main()





