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
        CREATE TABLE IF NOT EXISTS tttc_clusters (
            review_id       VARCHAR,
            order_id        VARCHAR,
            cluster_id      INTEGER,
            cluster_label   VARCHAR,
            cluster_summary VARCHAR,
            tsne_x          FLOAT,
            tsne_y          FLOAT
        )
    ''')

    cur.execute('''
        CREATE TEMPORARY TABLE tttc_clusters_staging (
            review_id       VARCHAR,
            order_id        VARCHAR,
            cluster_id      INTEGER,
            cluster_label   VARCHAR,
            cluster_summary VARCHAR,
            tsne_x          FLOAT,
            tsne_y          FLOAT
        )
    ''')

    rows = df[[
        'review_id', 'order_id', 'cluster_id',
        'cluster_label', 'cluster_summary',
        'tsne_x', 'tsne_y'
    ]].values.tolist()

    cur.executemany(
        'INSERT INTO tttc_clusters_staging VALUES (%s,%s,%s,%s,%s,%s,%s)',
        rows
    )

    cur.execute('''
        MERGE INTO tttc_clusters AS target
        USING tttc_clusters_staging AS source
            ON target.review_id = source.review_id
        WHEN MATCHED THEN
            UPDATE SET
                cluster_id      = source.cluster_id,
                cluster_label   = source.cluster_label,
                cluster_summary = source.cluster_summary,
                tsne_x          = source.tsne_x,
                tsne_y          = source.tsne_y
        WHEN NOT MATCHED THEN
            INSERT (
                review_id, order_id, cluster_id,
                cluster_label, cluster_summary,
                tsne_x, tsne_y
            )
            VALUES (
                source.review_id, source.order_id, source.cluster_id,
                source.cluster_label, source.cluster_summary,
                source.tsne_x, source.tsne_y
            )
    ''')

    conn.commit()
    conn.close()
    print(f'Merged {len(rows)} rows into tttc_clusters')