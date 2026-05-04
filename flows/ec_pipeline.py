from prefect import flow, task
import subprocess
import os

PYTHON  = '/Users/kogashinya/01_work_space/EC_DWS/.venv/bin/python'
PROJECT = '/Users/kogashinya/01_work_space/EC_DWS'
DBT     = '/Users/kogashinya/01_work_space/EC_DWS/.venv/bin/dbt'

def _run(script: str):
    result = subprocess.run(
        [PYTHON, script],
        cwd=PROJECT,
        capture_output=True,
        text=True,
        env={**os.environ},
    )
    print(result.stdout[-3000:])
    if result.returncode != 0:
        raise Exception(f'Script failed:\n{result.stderr[-2000:]}')

@task(name="Bronze ingestion", log_prints=True)
def bronze():
    print('=== Bronze start ===')
    _run(f'{PROJECT}/databricks/bronze_ingestion.py')
    print('=== Bronze done ===')

@task(name="Silver transform", log_prints=True)
def silver():
    print('=== Silver start ===')
    _run(f'{PROJECT}/databricks/silver_transform.py')
    print('=== Silver done ===')

@task(name="Gold aggregation", log_prints=True)
def gold():
    print('=== Gold start ===')
    _run(f'{PROJECT}/databricks/gold_aggregation.py')
    print('=== Gold done ===')

@task(name="dbt run", log_prints=True)
def dbt_run():
    print('=== dbt run start ===')
    result = subprocess.run(
        [DBT, 'run'],
        cwd=f'{PROJECT}/dbt_project',
        capture_output=True,
        text=True,
        env={**os.environ},
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f'dbt run failed:\n{result.stderr}')
    print('=== dbt run done ===')

@task(name="dbt test", log_prints=True)
def dbt_test():
    print('=== dbt test start ===')
    result = subprocess.run(
        [DBT, 'test'],
        cwd=f'{PROJECT}/dbt_project',
        capture_output=True,
        text=True,
        env={**os.environ},
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f'dbt test failed:\n{result.stderr}')
    print('=== dbt test done ===')

@task(name="TttC pipeline", log_prints=True)
def tttc():
    print('=== TttC start ===')
    _run(f'{PROJECT}/tttc/run_pipeline.py')
    print('=== TttC done ===')

@task(name="Refresh Snowflake external tables", log_prints=True)
def refresh_external_tables():
    import snowflake.connector
    conn = snowflake.connector.connect(
        account   = os.environ['SNOWFLAKE_ACCOUNT'],
        user      = os.environ['SNOWFLAKE_USER'],
        password  = os.environ['SNOWFLAKE_PASSWORD'],
        database  = 'ec_dwh',
        schema    = 'raw',
        warehouse = 'ec_dwh_wh',
    )
    cur = conn.cursor()
    for table in ['fact_orders', 'reviews_for_tttc', 'dim_products']:
        cur.execute(f'ALTER EXTERNAL TABLE raw.{table} REFRESH')
        print(f'Refreshed: raw.{table}')
    conn.close()


@flow(name="EC Pipeline", log_prints=True)
def ec_pipeline():
    bronze()
    silver()
    gold()
    refresh_external_tables()
    dbt_run()
    dbt_test()
    tttc()

if __name__ == '__main__':
    ec_pipeline()