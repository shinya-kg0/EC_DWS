import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

def fetch_reviews(limit: int = 300) -> pd.DataFrame:
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
            review_id,
            order_id,
            review_score,
            review_text
        FROM stg_reviews
        WHERE review_text IS NOT NULL
        AND LENGTH(review_text) > 10
        LIMIT {limit}
    '''
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    return df

if __name__ == '__main__':
    df = fetch_reviews(limit=10)
    print(df.head())
    print(f'Fetched {len(df)} rows')
