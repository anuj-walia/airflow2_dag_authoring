"""Minimal DBT DAG: separate dbt ls and dbt run tasks.

This DAG is a simplified variant of the original assume-role DBT demo.
It only performs two actions against the existing dbt project located
under /usr/local/airflow/dags/dbt-project inside the Airflow container:
    1. dbt ls
    2. dbt run

Both tasks obtain AWS credentials from the Airflow connection specified
by default_conn_id (assumed-role style) and export them to the environment
for the dbt CLI invocation. No extra logging, loops, or XCom usage.

To use: ensure the connection id below exists and that the dbt project &
profiles are mounted into the scheduler/webserver & worker image at the
paths indicated.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import os

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode

from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from pathlib import Path


DBT_EXECUTABLE_PATH = f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/dbt_venv/bin/dbt"

DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt-project-no-arn"  # container path
DBT_PROFILES_PATH = "/usr/local/airflow/dags/dbt-project-no-arn"
PROFILE_NAME = "optm_eu_reader"
AWS_REGION = Variable.get("dbt_region", default_var="us-east-2")
TARGET_NAME = "dev"
# ---------------------------------------------------------------------------
# DBT / AWS settings
# ---------------------------------------------------------------------------
default_conn_id = "aws-assumerole-test"  # existing Airflow connection id

# DBT_PROJECT_PATH = "/Users/anuj.walia/PycharmProjects/AF-sample/dags/dbt-project-no-arn"  # container path
# DBT_PROFILES_PATH = "/Users/anuj.walia/PycharmProjects/AF-sample/dags/dbt-project-no-arn"
# DBT_EXECUTABLE_PATH = "/Users/anuj.walia/PycharmProjects/AF-sample/.venv/bin/dbt"

# ---------------------------------------------------------------------------
# Basic configuration
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 17),  # arbitrary fixed start
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}



def _prepare_env(context) -> dict:
    """Fetch AWS creds from the Airflow connection and build env dict.

    Returns a dict suitable for passing to subprocess with required AWS and
    dbt environment variables. Keeps implementation minimal.
    """
    print("********************************** performing assume role for dbt***************************************************")
    hook = AwsBaseHook(aws_conn_id=default_conn_id, client_type="sts")
    creds = hook.get_credentials()
    if not creds or not creds.access_key:
        raise RuntimeError(f"Failed to obtain AWS credentials from connection '{default_conn_id}'")
    print("**********************************received cred***************************************************")
    print(creds)
    print("**********************************end of received cred***************************************************")
    env = dict(os.environ)  # inherit existing env
    env.update(
        {
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
            "AWS_REGION": AWS_REGION,
            "AWS_DEFAULT_REGION": AWS_REGION,
            "DBT_PROFILES_DIR": DBT_PROFILES_PATH,
        }
    )

    print("**********************************end of updating os env***************************************************")

    print("**********************************setting token in env ***************************************************")
    token = getattr(creds, "token", None)
    if token:
        env["AWS_SESSION_TOKEN"] = token
    print("**********************************printing all env in env dict ***************************************************")
    print(env)
    print("**********************************printed all env in env dict ***************************************************")
    print(env)
    os.environ.update(env)
    print( "**********************************printing all env varaiables ***************************************************")

    print(f'AWS_ACCESS_KEY_ID:{os.environ.get("AWS_ACCESS_KEY_ID")}')
    print(f'AWS_SECRET_ACCESS_KEY:{os.environ.get("AWS_SECRET_ACCESS_KEY")}')
    print(f'AWS_REGION:{os.environ.get("AWS_REGION")}')
    print(f'AWS_DEFAULT_REGION:{os.environ.get("AWS_DEFAULT_REGION")}')
    print(f'DBT_PROFILES_DIR:{os.environ.get("DBT_PROFILES_DIR")}')
    print(f'DBT_PROFILES_DIR:{os.environ.get("AWS_SESSION_TOKEN")}')

    print( "**********************************printed all env varaiables ***************************************************")
    return env




project_cfg = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH)
profile_cfg = ProfileConfig(
    profile_name=PROFILE_NAME,
    target_name=TARGET_NAME,
    profiles_yml_filepath=Path(DBT_PROFILES_PATH+"/profiles.yml")
)
exec_cfg = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
    execution_mode=ExecutionMode.LOCAL
)
render_cfg = RenderConfig(
    load_method=LoadMode.DBT_LS,  # ensures render uses dbt CLI
    # dbt_executable_path=DBT_EXECUTABLE_PATH,  # <-- important
)

def get_env_with_aws_creds(**context):
    """Get environment with AWS credentials for each task execution."""
    return _prepare_env(context)

@dag(
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args = {
        'owner': 'Anuj3',
    },
    tags=["dbt", "athena", "optm_eu", "country", "reader"]
)
def optm_eu_country_reader_task_group():
    @task
    def start():
        print("**********************************starting task***************************************************")
        return "Starting task"

    @task
    def end():
        print("**********************************finishing task***************************************************")
        return "Finishing task"

    dbt_task_group = DbtTaskGroup(
        project_config=project_cfg,
        profile_config=profile_cfg,
        execution_config=exec_cfg,
        render_config=render_cfg,
        operator_args={
            "dbt_executable_path": DBT_EXECUTABLE_PATH,
            "env": get_env_with_aws_creds,
            "emit_datasets": False,  # Disable OpenLineage dataset emission
        },
    )

    start_task = start()
    end_task = end()

    start_task >> dbt_task_group >> end_task


optm_eu_country_reader_task_group()
if __name__ == "__main__":
    optm_eu_country_reader_task_group().test(execution_date=datetime.today())
