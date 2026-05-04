# EC Data Warehouse + TttC

**Snowflake / dbt / TttC（Groq + sentence-transformers）によるデータ基盤構築**

ブラジルの EC オープンデータ（[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)）を題材に、レビューの自由記述テキストを AI でクラスタリングする **TttC（Talk to the City）パイプライン**を組み込み、定量分析と定性分析を統合した Streamlit ダッシュボードを実装しています。


## セットアップ

### 必要アカウント

- AWS（S3）
- Databricks
- Snowflake
- Groq（[無料枠あり](https://console.groq.com/)）

### 環境変数

`.env` ファイルをリポジトリルートに作成

```env
# AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=ap-northeast-1

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password

# Groq
GROQ_API_KEY=your_groq_api_key
```

### インストール

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

主な依存ライブラリ：

```
sentence-transformers
groq                    
scikit-learn            
snowflake-connector-python
pyspark
delta-spark
boto3
dbt-snowflake
streamlit
plotly
prefect
python-dotenv
```



## パイプライン全体の実行

Prefect フローで全フェーズを一括実行

```bash
python flows/ec_pipeline.py
```

実行順序：Bronze → Silver → Gold → Snowflake外部テーブルリフレッシュ → dbt run → dbt test → TttC パイプライン


## 使用技術

| カテゴリ | 技術 |
|---------|------|
| Lakehouse | Databricks + Delta Lake |
| DWH | Snowflake |
| データ変換 | dbt |
| 分散処理 | PySpark |
| オーケストレーション | Prefect |
| Embedding | sentence-transformers（paraphrase-multilingual-MiniLM-L12-v2） |
| LLM | Groq API（llama-3.3-70b-versatile） |
| クラスタリング | scikit-learn（KMeans） |
| 次元削減 | scikit-learn（t-SNE） |
| 可視化 | Streamlit + Plotly |
| ストレージ | AWS S3 |
