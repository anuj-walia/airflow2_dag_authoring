from airflow import DAG
from airflow.operators.python import PythonOperator
# from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
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

# def _assume_role():
#     sts = boto3.client("sts")
#     resp = sts.assume_role(
#         RoleArn=TARGET_ROLE_ARN,
#         RoleSessionName=SESSION_NAME,
#         DurationSeconds=3600,
#     )
#     return resp["Credentials"]

# def _write_profiles():
#     profiles_dir = tempfile.mkdtemp(prefix="dbt_profiles_")
#     profiles_path = os.path.join(profiles_dir, "profiles.yml")
#     content = textwrap.dedent(f"""
#     {PROFILE_NAME}:
#       target: {TARGET_NAME}
#       outputs:
#         {TARGET_NAME}:
#           type: glue
#           region: {REGION}
#           catalog: AwsDataCatalog
#           database: my_database
#           schema: my_database
#           query_concurrency: 10
#     """).strip()
#     with open(profiles_path, "w") as f:
#         f.write(content)
#     return profiles_dir

# def dynamic_env(context):
#     creds = _assume_role()
#     profiles_dir = _write_profiles()
#     return {
#         "AWS_ACCESS_KEY_ID": creds["AccessKeyId"],
#         "AWS_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
#         "AWS_SESSION_TOKEN": creds["SessionToken"],
#         "AWS_DEFAULT_REGION": REGION,
#         "DBT_PROFILES_DIR": profiles_dir,
#     }

def dbt_ls(**kwargs):
    print("running dbt ls")

    cmd = [DBT_EXECUTABLE_PATH, "ls", "--project-dir", DBT_PROJECT_PATH, "--profiles-dir", DBT_PROFILES_DIR]
    print("************************************"+str(cmd)+"************************************")
    # proc = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, **env})
    proc = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ})
    print("dbt ls code", proc.returncode)
    if proc.stdout:
        print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    if proc.returncode != 0:
        raise Exception("dbt ls failed")

    """
       creds = _assume_role()
    env = {
        "AWS_ACCESS_KEY_ID": creds["AccessKeyId"],
        "AWS_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
        "AWS_SESSION_TOKEN": creds["SessionToken"],
        "AWS_DEFAULT_REGION": REGION,
        "DBT_PROFILES_DIR": profiles_dir,
    }
    """

with DAG(
    dag_id="dbt_glue_cosmos_dag",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
) as dag:

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

    dbt_group = DbtTaskGroup(
        # group_id="dbt_tasks",
        project_config=project_cfg,
        profile_config=profile_cfg,
        execution_config=exec_cfg,
        # render_config=render_cfg,
        operator_args={"dbt_executable_path": DBT_EXECUTABLE_PATH},
        # Optionally limit which commands/models:
        # select=["my_model"],
    )

    dbt_ls_task = PythonOperator(task_id="dbt_ls", python_callable=dbt_ls)

    dbt_ls_task >> dbt_group