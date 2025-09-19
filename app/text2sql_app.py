import streamlit as st
import sqlite3
import pandas as pd
from google import genai  # Google Gemini SDK
import os
from pydantic import BaseModel

class ResQuery(BaseModel):
    logic : str
    query: str  


##config
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# database/news.db 경로
DB_PATH = os.path.join(BASE_DIR, "database", "news.db")
gemini_key = os.environ["GEMINI_API_KEY"]
client = genai.Client(api_key=gemini_key)

# Example schema (you should retrieve dynamically from DB)
schema = """
Table: tb_stock_prices
Columns: 
        date TEXT NOT NULL comment 기준일,
        comp_nm text NOT NULL comment 회사명,
        stock_cd TEXT NOT NULL comment 주식코드,
        close num comment 종가,
        high num comment 최고가,
        low num comment 최저가,
        open num comment 시장가,
        volume num comment 거래수, 
        create_dt DATETIME,
        PRIMARY KEY (date,stock_cd)
"""

# ----------------------------
# 3. Streamlit UI
# ----------------------------
st.title("🧠 Text-to-SQL with Gemini + Streamlit")
# st.write("Ask a question in natural language and get SQL + results!")
st.write("질문 입력해주세요 !")

# user_question = st.text_input("Enter your stock question:")

if "user_question" not in st.session_state:
    st.session_state.user_question = ""

user_input = st.text_input("Enter your stock question:", st.session_state.user_question)

if st.button("Use example question"):
    st.session_state.user_question = "2025년 제일 비싼 주식 상위 5개 제시해주시오."
    # st.session_state.user_question = "Show top 5 most expensive stocks in 2025"

user_question = st.session_state.user_question


if st.button("Generate SQL and Run"):
    with st.spinner("Generating SQL query..."):
        # Build prompt for Gemini
        prompt = f"""
                You are an expert in SQL and the database engine is SQLite3.

                The database schema is as follows:
                {schema}

                Rules:
                1. Generate only valid SQLite3 SQL queries.
                2. Use SQLite3 functions if needed (e.g., STRFTIME for dates).
                3. Return only the SQL query, no explanations.
                4. Make sure the query matches the user's request accurately.
                5. Answer my question in Korean.

                User question:
                {user_question}

        """

        response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
                config={
            "response_mime_type": "application/json",
            "response_schema": ResQuery,
        }
    )
        sql_query = response.parsed.query

    st.subheader("🔍 Generated SQL Query")
    st.code(sql_query, language="sql")

    # Execute SQL query
    try:
        conn = sqlite3.connect(DB_PATH,timeout=30)
        cursor = conn.cursor()

        cursor.execute(sql_query)
        rows = cursor.fetchall()  # 결과를 리스트로 가져오기
        columns = [desc[0] for desc in cursor.description]  # 컬럼명 추출
        df = pd.DataFrame(rows, columns=columns)

        conn.close()

        st.subheader("📊 Query Results")
        st.dataframe(df)

    except Exception as e:
        st.error(f"Error executing SQL: {e}")
