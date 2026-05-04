import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
import os

load_dotenv()

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

st.set_page_config(page_title='EC Analytics + TttC', layout='wide')
st.title('EC Analytics Dashboard')
st.caption('Olist Brazilian E-Commerce｜Databricks / Snowflake / dbt / TttC（Groq + sentence-transformers）')

# ── データ取得 ─────────────────────────────────────
df_trend = load('''
    SELECT order_month, order_year,
           SUM(revenue) AS revenue
    FROM agg_daily_sales
    GROUP BY 1, 2
    ORDER BY order_year, order_month
''')
df_trend['period'] = df_trend['order_year'].astype(str) + '-' + \
                     df_trend['order_month'].astype(str).str.zfill(2)

df_state = load('''
    SELECT customer_state,
           COUNT(DISTINCT order_id) AS order_count
    FROM fct_orders
    GROUP BY 1
    ORDER BY order_count DESC
    LIMIT 10
''')

df_tsne = load('''
    SELECT
        t.tsne_x, t.tsne_y,
        t.cluster_label, t.cluster_summary,
        r.category_en
    FROM tttc_clusters t
    JOIN stg_reviews r ON t.review_id = r.review_id
''')

df_clusters = load('''
    SELECT cluster_label, cluster_summary,
           COUNT(*) AS review_count
    FROM tttc_clusters
    GROUP BY 1, 2
    ORDER BY review_count DESC
''')

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
        COUNT(*) AS cnt
    FROM tttc_clusters t
    JOIN rfm ON t.order_id = rfm.order_id
    GROUP BY 1, 2
''')

# ── 1段目：売上 / 州別 / クラスタ件数 ──────────────
st.subheader('定量分析')
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown('**月次売上トレンド**')
    fig1 = px.line(df_trend, x='period', y='revenue',
                   labels={'period': '月', 'revenue': '売上'},
                   markers=True)
    fig1.update_layout(height=250, margin=dict(t=10, b=10))
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.markdown('**州別注文数 TOP10**')
    fig2 = px.bar(df_state, x='customer_state', y='order_count',
                  labels={'customer_state': '州', 'order_count': '注文数'},
                  color='order_count', color_continuous_scale='Greens')
    fig2.update_layout(height=250, margin=dict(t=10, b=10),
                       coloraxis_showscale=False)
    st.plotly_chart(fig2, use_container_width=True)

with col3:
    st.markdown('**クラスタ別レビュー件数**')
    fig3 = px.bar(df_clusters, x='cluster_label', y='review_count',
                  hover_data=['cluster_summary'],
                  labels={'cluster_label': 'クラスタ', 'review_count': '件数'},
                  color='review_count', color_continuous_scale='Blues')
    fig3.update_layout(height=250, margin=dict(t=10, b=10),
                       coloraxis_showscale=False)
    st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── 2段目：t-SNE / RFMヒートマップ ────────────────
st.subheader('定性分析（TttC）')
col4, col5 = st.columns([3, 2])

with col4:
    st.markdown('**レビュークラスタ分布（t-SNE）**')
    st.caption('点にホバーするとカテゴリ・内容が表示されます')
    fig4 = px.scatter(
        df_tsne,
        x='tsne_x', y='tsne_y',
        color='cluster_label',
        hover_data={
            'tsne_x': False,
            'tsne_y': False,
            'category_en': True,
            'cluster_label': True,
            'cluster_summary': True,
        },
        labels={
            'category_en': 'カテゴリ',
            'cluster_label': 'クラスタ',
            'cluster_summary': '内容',
            'tsne_x': '', 'tsne_y': '',
        },
    )
    fig4.update_layout(
        height=350,
        margin=dict(t=10, b=10),
        legend_title_text='クラスタ',
        xaxis=dict(showticklabels=False),
        yaxis=dict(showticklabels=False),
    )
    st.plotly_chart(fig4, use_container_width=True)

with col5:
    st.markdown('**RFMセグメント × レビュークラスタ**')
    st.caption('どのセグメントがどんな傾向のレビューをしているか')
    if df_integrated.empty:
        st.warning('データが取得できませんでした')
    else:
        pivot = df_integrated.pivot(
            index='rfm_segment', columns='cluster_label', values='cnt'
        ).fillna(0)
        fig5 = px.imshow(pivot, color_continuous_scale='Blues',
                         labels={'color': '件数'})
        fig5.update_layout(height=350, margin=dict(t=10, b=10))
        st.plotly_chart(fig5, use_container_width=True)
        