import snowflake.connector
import pandas as pd
import os

def write_clusters_to_snowflake(df: pd.DataFrame):
    conn = snowflake.connector.connect(
        account   = os.environ['SNOWFLAKE_ACCOUNT'],
        user      = os.environ['SNOWFLAKE_USER'],
        password  = os.environ['SNOWFLAKE_PASSWORD'],
        database  = 'ec_dwh',
        schema    = 'dbt_dev',
        warehouse = 'ec_dwh_wh',
    )
    cur = conn.cursor()
    cur.execute('''
        CREATE OR REPLACE TABLE tttc_clusters (
            review_id       VARCHAR,
            order_id        VARCHAR,
            cluster_id      INTEGER,
            cluster_label   VARCHAR,
            cluster_summary VARCHAR,
            tsne_x          FLOAT,
            tsne_y          FLOAT
        )
    ''')
    cur.execute('TRUNCATE TABLE tttc_clusters')
    rows = df[['review_id', 'order_id', 'cluster_id',
               'cluster_label', 'cluster_summary',
               'tsne_x', 'tsne_y']].values.tolist()
    cur.executemany(
        'INSERT INTO tttc_clusters VALUES (%s,%s,%s,%s,%s,%s,%s)', rows)
    conn.commit()
    conn.close()
    print(f'Wrote {len(rows)} rows to tttc_clusters')
