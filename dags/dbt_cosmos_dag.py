from airflow import DAG
from airflow.operators.python import PythonOperator
# from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos import DbtDag,DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior, ExecutionMode, LoadMode
from pathlib import Path

# import boto3
import os
import textwrap
import tempfile
from datetime import datetime
import subprocess

# TARGET_ROLE_ARN = "arn:aws:iam::739275465799:role/svc-glue-role-astro-dbt"
# SESSION_NAME = "airflow-dbt-session"
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt/nyc_parking_violations"  # adjust
DBT_PROFILES_DIR = "/usr/local/airflow/dags/dbt/nyc_parking_violations"
PROFILE_NAME = "nyc_parking_violations"
TARGET_NAME = "dev"
REGION = "us-east-1"
DBT_EXECUTABLE_PATH = f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/dbt_venv/bin/dbt"

def something(context):

    import logging
    task_instance = context["task_instance"]
    logging.info(f"--- PRE-EXECUTION LOG ---")
    logging.info(f"Task {task_instance.task_id} is about to run!")
    logging.info(f"DAG Run ID: {task_instance.run_id}")
    logging.info(f"-------------------------")

# Cosmos configs
project_cfg = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH)
profile_cfg = ProfileConfig(
    profile_name=PROFILE_NAME,
    target_name=TARGET_NAME,
    profiles_yml_filepath=Path(DBT_PROFILES_DIR+"/profiles.yml")
)
exec_cfg = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
    execution_mode=ExecutionMode.LOCAL
)
render_cfg = RenderConfig(
    load_method=LoadMode.DBT_LS,  # ensures render uses dbt CLI
    # dbt_executable_path=DBT_EXECUTABLE_PATH,  # <-- important
)

dag = DbtDag(
    dag_id='trial_dbt_dag',
    project_config=project_cfg,
    profile_config=profile_cfg, # or profile_mapping=profile_mapping,
    execution_config=exec_cfg,
    operator_args={"dbt_executable_path": DBT_EXECUTABLE_PATH},
    start_date=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args = {
    'owner': 'anuj',
    'on_execute_callback': something, # <-- This is the key

    }
)

# dbt_group = DbtTaskGroup(
#     # group_id="dbt_tasks",
#     project_config=project_cfg,
#     profile_config=profile_cfg,
#     execution_config=exec_cfg,
#     # render_config=render_cfg,
#     operator_args={"dbt_executable_path": DBT_EXECUTABLE_PATH},
#     # Optionally limit which commands/models:
#     # select=["my_model"],
# )

