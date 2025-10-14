from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator
from datetime import datetime
import logging

# from cosmos import DbtDag

def _create_duck_db():
    import duckdb
    sql_query_import_1 = '''
    CREATE OR REPLACE TABLE parking_violation_codes AS
    SELECT *
    FROM read_csv_auto(
      '/usr/local/airflow/include/dbt/data/dof_parking_violation_codes.csv',
      normalize_names=True
      )
    '''

    sql_query_import_2 = '''
    CREATE OR REPLACE TABLE parking_violations_2023 AS
    SELECT *
    FROM read_csv_auto(
      '/usr/local/airflow/include/dbt/data/parking_violations_issued_fiscal_year_2023_sample.csv',
      normalize_names=True
      )
    '''

    with duckdb.connect('/usr/local/airflow/include/dbt/data/nyc_parking_violations.db') as con:
        logging.info("sql_query_import_1 start")
        con.sql(sql_query_import_1)
        logging.info("sql_query_import_1 end")
        logging.info("sql_query_import_2 start")
        con.sql(sql_query_import_2)
        logging.info("sql_query_import_2 end")

# Define the DAG using Cosmos' DbtDag

with DAG(dag_id="my_first_dbt_dag",schedule=None,start_date=datetime(2025,10,1)) as dag:
    _prepare_duck_db_task=PythonOperator(task_id="prepare_duck_db_task",python_callable=_create_duck_db)
    _dbt_run_task = DbtRunOperator(task_id="dbt_run_task",
                                   profiles_dir="/usr/local/airflow/include/dbt/nyc_parking_violations",
                                   project_dir="/usr/local/airflow/include/dbt/nyc_parking_violations")
    _prepare_duck_db_task>>_dbt_run_task

"""dbt_cosmos_dag = DbtDag(
    dag_id='dbt_run_with_cosmos',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    # Define the dbt project location
    dbt_project_name='my_dbt_project',
    dbt_root_path='/path/to/your/dbt_project',
    # Define the Airflow connection to your data warehouse
    conn_id='your_db_connection_id')"""