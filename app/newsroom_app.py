import streamlit as st
import pandas as pd
import re
import sqlite3
import os 
import json
from io import BytesIO
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import rcParams
import matplotlib.font_manager as fm


# # 설치한 나눔폰트 사용
# font_path = "/usr/share/fonts/truetype/nanum/NanumGothicCoding.ttf"  # 설치한 폰트 경로
# font_prop = fm.FontProperties(fname=font_path)
# plt.rcParams['font.family'] = font_prop.get_name()
# plt.rcParams['axes.unicode_minus'] = False  # 마이너스 깨짐 방지

def read_db_news_data():

    DB_PATH = os.path.join(os.path.dirname(__file__), "database/news.db")
    print(DB_PATH)

    # DB_PATH = '/home/kexin/database/news.db'
    TABLE_NAME = 'tb_news_clipping'

    conn = sqlite3.connect(DB_PATH,timeout=20)
    cursor = conn.cursor()

    cursor.execute(f"""select * from {TABLE_NAME}""")
    conn.commit()
    columns = [desc[0] for desc in cursor.description]

    # 결과 fetch 후 DataFrame 변환
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=columns).drop_duplicates(subset=['org_link'], keep='first', inplace=False).reset_index(drop=True)
    print(df)

    # conn.colse()

    return df
df = read_db_news_data()
df['date'] = pd.to_datetime(df['pub_dt']).dt.strftime("%Y-%m-%d")


# df = pd.read_csv('file2.csv')
# df['pubDate'] = pd.to_datetime(df['pubDate'], utc=True)
# df['pubDate'] = df['pubDate'].dt.tz_convert("Asia/Seoul")
# df['date'] = df['pubDate'].dt.strftime("%Y-%m-%d")
# df['pubDate'] = df['pubDate'].dt.strftime("%Y-%m-%d %H:%M")

# st.set_page_config(page_title="뉴스 클리핑", layout="wide")
st.title("📰 AI 뉴스")

# ------------------------------
# 3️⃣ 사이드바 필터
# ------------------------------
st.sidebar.header("🔍상세조건 검색")

# 기준일자 선택 (Selectbox)
date_filter = st.sidebar.selectbox(
    "기준일자",
    options=["전체"] + sorted(list(df['date'].unique()))
)

# 감정 선택 (Selectbox)
sentiment_filter = st.sidebar.selectbox(
    "감정 선택",
    options=["전체"] + list(df['ai_sentiment'].unique())
)

# 언론사 선택 (Selectbox)
media_filter = st.sidebar.selectbox(
    "매체 분류",
    options=["전체"] + list(df['media'].unique())
)

# 회사 선택 (Selectbox)
comp_filter = st.sidebar.selectbox(
    "기업명",
    options=["전체"] + list(df['comp_nm'].unique())
)

# #필터링
filtered_df = df.copy()
if sentiment_filter != "전체":
    filtered_df = filtered_df[filtered_df['ai_sentiment'] == sentiment_filter]
if media_filter != "전체":
    filtered_df = filtered_df[filtered_df['media'] == media_filter]
if date_filter != "전체":
    filtered_df = filtered_df[filtered_df['date'] == date_filter]
if comp_filter != "전체":
    filtered_df = filtered_df[filtered_df['comp_nm'] == comp_filter]


# 사이드바에 정렬 옵션 추가
sort_option = st.sidebar.selectbox(
    "정렬 기준",
    ["날짜 ↑", "날짜 ↓", "제목 ↑", "제목 ↓"]
)

# 정렬 적용
if sort_option == "날짜 ↑":
    filtered_df = filtered_df.sort_values("pub_dt", ascending=True)
elif sort_option == "날짜 ↓":
    filtered_df = filtered_df.sort_values("pub_dt", ascending=False)
elif sort_option == "제목 ↑":
    filtered_df = filtered_df.sort_values("title", ascending=True)
elif sort_option == "제목 ↓":
    filtered_df = filtered_df.sort_values("title", ascending=False)



def strip_html(text):
    clean = re.sub(r"<.*?>", "", text)  # 모든 <> 태그 제거
    return clean

tab1, tab2 = st.tabs(["뉴스 통계", "뉴스 클리핑"])

with tab1:

            # 3️⃣ 일자별 뉴스 건수
    date_counts = filtered_df.groupby('date').size().reset_index(name='뉴스 건수')
    fig3, ax3 = plt.subplots(figsize=(5,2))
    sns.lineplot(data=date_counts, x='date', y='뉴스 건수', marker='o', ax=ax3)
    ax3.set_title("일자별 뉴스 건수")
    ax3.set_xlabel("날짜")
    ax3.set_ylabel("뉴스 건수")
    plt.xticks(rotation=45)
    st.pyplot(fig3)

    col1, col2 = st.columns([6,4])
    with col1:

        # 1️⃣ 기업별 뉴스 건수 막대그래프
        company_counts = filtered_df['comp_nm'].value_counts().reset_index()
        company_counts.columns = ['회사', '뉴스 건수']
        
        fig, ax = plt.subplots(figsize=(6,4))
        sns.barplot(data=company_counts, x='회사', y='뉴스 건수', ax=ax, palette='Set2')
        ax.set_title("기업별 뉴스 건수")
        ax.set_ylabel("뉴스 건수")
        ax.set_xlabel("회사")
        st.pyplot(fig)
    
    with col2:
        # 2️⃣ 감정 비율 파이차트
        sentiment_counts = filtered_df['ai_sentiment'].value_counts()
        fig2, ax2 = plt.subplots(figsize=(4,4))
        ax2.pie(sentiment_counts, labels=sentiment_counts.index, autopct='%1.1f%%', startangle=90, colors=['#66c2a5','#fc8d62','#8da0cb'])
        ax2.set_title("감정 비율")
        st.pyplot(fig2)




with tab2:

    # # 선택 관리용 session_state 초기화
    if 'selected_news' not in st.session_state:
        st.session_state.selected_news = set()

    col_count, col_toggle, col_file, col_download = st.columns([0.3, 0.2, 0.2, 0.3])

    # 선택 건수 표시
    with col_count:
        selected_count_placeholder = st.empty()
        selected_count_placeholder.write(f"##### **총 {len(filtered_df)}건  선택 {len(st.session_state.selected_news)}건**")

    # 전체 선택/해제 토글 버튼
    with col_count:
        # 버튼 상태를 session_state에 저장
        if 'select_all' not in st.session_state:
            st.session_state.select_all = False

        if st.button("전체선택/해제"):
            if not st.session_state.select_all:
                st.session_state.selected_news = set(filtered_df.index)  # 전체 선택
                st.session_state.select_all = True
            else:
                st.session_state.selected_news = set()  # 전체 해제
                st.session_state.select_all = False

    # 파일 형식 선택
    with col_file:
        file_format = st.selectbox("파일 형식", ["CSV", "Excel"], key="file_format_select")

    # 다운로드 버튼
    with col_download:
        if st.session_state.selected_news:
            download_df = filtered_df.loc[filtered_df.index.isin(st.session_state.selected_news)]
            
            if file_format == "CSV":
                data = download_df.to_csv(index=False).encode("utf-8-sig")
                st.download_button("📥 다운로드", data=data, file_name="selected_news.csv", mime="text/csv")
            elif file_format == "Excel":
                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    download_df.to_excel(writer, index=False, sheet_name="News")
                st.download_button("📥 다운로드", data=output.getvalue(), file_name="selected_news.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("다운로드할 뉴스를 선택하세요.")

    # ========================
    # 뉴스 카드 렌더링
    # ========================
    for idx, row in filtered_df.iterrows():
        col1, col2 = st.columns([0.1, 10])
        with col1:
            # 체크박스 상태를 session_state와 동기화
            checked = st.checkbox("", key=f"cb_{idx}", value=(idx in st.session_state.selected_news))
            if checked:
                st.session_state.selected_news.add(idx)
            else:
                st.session_state.selected_news.discard(idx)
            
            # 선택 건수 업데이트
            selected_count_placeholder.write(f"##### **총 {len(filtered_df)}건  선택 {len(st.session_state.selected_news)}건**")

        with col2:
            # 기존 뉴스 카드 내용
            with st.container(border=True):
                c1, c2 = st.columns([1, 5])
                with c1:
                    st.markdown("**AI 키워드**")
                    st.info(row["ai_keyword"])
                    st.markdown("**AI 감정**")
                    if row["ai_sentiment"] == "긍정":
                        st.success(row["ai_sentiment"])
                    elif row["ai_sentiment"] == "부정":
                        st.error(row["ai_sentiment"])
                    else:
                        st.warning(row["ai_sentiment"])
                with c2:
                    st.markdown(f"### {strip_html(row['title'])}")
                    st.caption(f"{row['media']} | {row['pub_dt']}")
                    st.markdown(f"**AI 요약:**<br>{row['ai_summary'].replace('/n','').replace('·', '<br>·')}", unsafe_allow_html=True)
                    with st.popover("원문 보기"):
                        st.markdown(f"#### [{strip_html(row['title'])}]({row['link']})")
                        st.markdown("---")
                        st.markdown(row["content"])
