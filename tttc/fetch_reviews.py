import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

def fetch_reviews(limit: int = 1000) -> pd.DataFrame:
    conn = snowflake.connector.connect(
        account   = os.environ['SNOWFLAKE_ACCOUNT'],
        user      = os.environ['SNOWFLAKE_USER'],
        password  = os.environ['SNOWFLAKE_PASSWORD'],
        database  = 'ec_dwh',
        schema    = 'dbt_dev',
        warehouse = 'ec_dwh_wh',
    )
    query = f'''
        SELECT
            r.review_id,
            r.order_id,
            r.review_score,
            r.review_text
        FROM stg_reviews r
        LEFT JOIN tttc_clusters t
            ON r.review_id = t.review_id
        WHERE r.review_text IS NOT NULL
            AND LENGTH(r.review_text) > 10
            AND t.review_id IS NULL        
        ORDER BY r.review_creation_date  
        LIMIT {limit}
    '''
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    print(f'Fetched {len(df)} unprocessed reviews (limit={limit})')
    return df


def fetch_review_count() -> dict:
    conn = snowflake.connector.connect(
        account   = os.environ['SNOWFLAKE_ACCOUNT'],
        user      = os.environ['SNOWFLAKE_USER'],
        password  = os.environ['SNOWFLAKE_PASSWORD'],
        database  = 'ec_dwh',
        schema    = 'dbt_dev',
        warehouse = 'ec_dwh_wh',
    )
    query = '''
        SELECT
            COUNT(r.review_id)                       AS total,
            COUNT(t.review_id)                       AS processed,
            COUNT(r.review_id) - COUNT(t.review_id)  AS unprocessed
        FROM stg_reviews r
        LEFT JOIN tttc_clusters t ON r.review_id = t.review_id
        WHERE r.review_text IS NOT NULL
            AND LENGTH(r.review_text) > 10
    '''
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]

    result = df.iloc[0].to_dict()
    print(
        f"[Review count] "
        f"total={int(result['total'])} / "
        f"processed={int(result['processed'])} / "
        f"unprocessed={int(result['unprocessed'])}"
    )
    return result


if __name__ == '__main__':
    fetch_review_count()
    df = fetch_reviews(limit=10)
    print(df.head())
