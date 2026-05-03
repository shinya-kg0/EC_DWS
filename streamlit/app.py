import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
import os

load_dotenv()

# ── 接続 ──────────────────────────────────────────
@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        account   = os.environ['SNOWFLAKE_ACCOUNT'],
        user      = os.environ['SNOWFLAKE_USER'],
        password  = os.environ['SNOWFLAKE_PASSWORD'],
        database  = 'ec_dwh',
        schema    = 'dbt_dev',
        warehouse = 'ec_dwh_wh',
    )

@st.cache_data(ttl=3600)
def load(query: str) -> pd.DataFrame:
    df = pd.read_sql(query, get_conn())
    df.columns = [c.lower() for c in df.columns]
    return df

# ── レイアウト ─────────────────────────────────────
st.set_page_config(page_title='EC Analytics + TttC', layout='wide')
st.title('EC Analytics Dashboard')

tab1, tab2, tab3, tab4 = st.tabs(
    ['売上概要', '顧客分析', 'レビュー分析（TttC）', '統合分析'])

# ── Tab 1: 売上概要 ────────────────────────────────
with tab1:
    st.subheader('月次売上トレンド')
    df_trend = load('''
        SELECT order_month, order_year,
               SUM(revenue) AS revenue,
               SUM(order_count) AS order_count
        FROM agg_daily_sales
        GROUP BY 1, 2
        ORDER BY order_year, order_month
    ''')
    df_trend['period'] = df_trend['order_year'].astype(str) + '-' + \
                         df_trend['order_month'].astype(str).str.zfill(2)
    fig1 = px.line(df_trend, x='period', y='revenue',
                   labels={'period': '月', 'revenue': '売上'},
                   markers=True)
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader('カテゴリ別売上 TOP20')
    df_cat = load('''
        SELECT category_en,
               SUM(revenue) AS revenue,
               SUM(order_count) AS order_count
        FROM agg_category_sales
        GROUP BY 1
        ORDER BY revenue DESC
        LIMIT 20
    ''')
    fig2 = px.bar(df_cat, x='category_en', y='revenue',
                  labels={'category_en': 'カテゴリ', 'revenue': '売上'},
                  color='revenue', color_continuous_scale='Blues')
    st.plotly_chart(fig2, use_container_width=True)

# ── Tab 2: 顧客分析 ────────────────────────────────
with tab2:
    st.subheader('州別注文数')
    df_state = load('''
        SELECT customer_state,
               COUNT(DISTINCT order_id) AS order_count,
               SUM(product_revenue) AS revenue
        FROM fct_orders
        GROUP BY 1
        ORDER BY order_count DESC
        LIMIT 15
    ''')
    fig3 = px.bar(df_state, x='customer_state', y='order_count',
                  labels={'customer_state': '州', 'order_count': '注文数'},
                  color='order_count', color_continuous_scale='Greens')
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader('レビュースコア分布')
    df_score = load('''
        SELECT review_score, COUNT(*) AS cnt
        FROM fct_orders
        WHERE review_score IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    ''')
    fig4 = px.bar(df_score, x='review_score', y='cnt',
                  labels={'review_score': 'スコア', 'cnt': '件数'},
                  color='review_score', color_continuous_scale='RdYlGn')
    st.plotly_chart(fig4, use_container_width=True)

# ── Tab 3: レビュー分析（TttC） ───────────────────
with tab3:
    st.subheader('クラスタ別レビュー分布')
    df_clusters = load('''
        SELECT cluster_id, cluster_label, cluster_summary,
               COUNT(*) AS review_count
        FROM tttc_clusters
        GROUP BY 1, 2, 3
        ORDER BY review_count DESC
    ''')
    fig5 = px.bar(df_clusters, x='cluster_label', y='review_count',
                  hover_data=['cluster_summary'],
                  labels={'cluster_label': 'クラスタ', 'review_count': '件数'},
                  color='review_count', color_continuous_scale='Blues')
    st.plotly_chart(fig5, use_container_width=True)

    st.subheader('クラスタ一覧')
    st.dataframe(
        df_clusters[['cluster_label', 'cluster_summary', 'review_count']],
        use_container_width=True
    )

# ── Tab 4: 統合分析 ────────────────────────────────
with tab4:
    st.subheader('RFMセグメント × レビュークラスタ')
    df_integrated = load('''
        WITH rfm AS (
            SELECT order_id, customer_id,
                NTILE(3) OVER (ORDER BY MAX(order_date) DESC) AS r,
                NTILE(3) OVER (ORDER BY COUNT(*))             AS f
            FROM fct_orders
            GROUP BY 1, 2
        )
        SELECT
            CASE WHEN r=1 AND f=3 THEN 'Champions'
                 WHEN r=1         THEN 'Recent'
                 WHEN f=3         THEN 'Loyal'
                 ELSE 'Others'    END AS rfm_segment,
            t.cluster_label,
            COUNT(*)                 AS cnt
        FROM tttc_clusters t
        JOIN rfm ON t.order_id = rfm.order_id
        GROUP BY 1, 2
    ''')
    if df_integrated.empty:
        st.warning('データが取得できませんでした')
    else:
        pivot = df_integrated.pivot(
            index='rfm_segment', columns='cluster_label', values='cnt'
        ).fillna(0)
        fig6 = px.imshow(pivot, color_continuous_scale='Blues',
                         title='どのセグメントがどんな傾向のレビューをしているか',
                         labels={'color': '件数'})
        st.plotly_chart(fig6, use_container_width=True)