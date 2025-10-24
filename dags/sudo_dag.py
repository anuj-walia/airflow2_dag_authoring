from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import os
import textwrap
from datetime import datetime
import subprocess
from pathlib import Path

TARGET_ROLE_ARN = "arn:aws:iam::739275465799:role/svc-glue-role-astro-dbt"
SESSION_NAME = "airflow-dbt-session"
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dev/buk/bukastroplat/dbt/dbtbuktestproject"  # adjust
PROFILE_NAME = "astro_test"
TARGET_NAME = "devglue"
REGION = "eu-west-1"
DBT_EXECUTABLE_PATH = f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/dbt_venv/bin/dbt"

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
PERSISTENT_PROFILES_DIR = Path(DBT_PROJECT_PATH)
PERSISTENT_PROFILES_PATH = PERSISTENT_PROFILES_DIR / "profiles.yml"


def _print_caller_identity(sts_client, context_msg):
    """Print STS caller identity for debugging."""
    try:
        identity = sts_client.get_caller_identity()
        print(f"[{context_msg}] Caller Identity:")
        print(f"  Account: {identity.get('Account')}")
        print(f"  Arn: {identity.get('Arn')}")
        print(f"  UserId: {identity.get('UserId')}")
        return identity
    except Exception as e:
        print(f"[{context_msg}] Failed to get caller identity: {e}")
        return None


def _print_aws_credentials(creds, context_msg):
    """Print AWS credentials for debugging (sensitive info)."""
    print(f"[{context_msg}] AWS Credentials:")
    print(f"  AccessKeyId: {creds.get('AccessKeyId', 'NOT_FOUND')}")
    print(f"  SecretAccessKey: {creds.get('SecretAccessKey', 'NOT_FOUND')[:10]}...")
    print(f"  SessionToken: {creds.get('SessionToken', 'NOT_FOUND')[:20]}...")
    if 'Expiration' in creds:
        print(f"  Expiration: {creds['Expiration']}")


def _verify_profiles_file():
    """Verify and print profiles.yml content."""
    print(f"[PROFILE_CHECK] Checking profiles.yml at: {PERSISTENT_PROFILES_PATH}")
    try:
        if PERSISTENT_PROFILES_PATH.exists():
            content = PERSISTENT_PROFILES_PATH.read_text()
            print(f"[PROFILE_CHECK] profiles.yml exists and contains:")
            print("--- START profiles.yml ---")
            print(content)
            print("--- END profiles.yml ---")
            return True
        else:
            print(f"[PROFILE_CHECK] profiles.yml does NOT exist at {PERSISTENT_PROFILES_PATH}")
            return False
    except Exception as e:
        print(f"[PROFILE_CHECK] Error reading profiles.yml: {e}")
        return False


def _verify_dbt_project():
    """Verify DBT project directory and contents."""
    print(f"[PROJECT_CHECK] Checking DBT project at: {DBT_PROJECT_PATH}")
    try:
        project_path = Path(DBT_PROJECT_PATH)
        if not project_path.exists():
            print(f"[PROJECT_CHECK] DBT project directory does NOT exist: {DBT_PROJECT_PATH}")
            return False

        print(f"[PROJECT_CHECK] DBT project directory exists. Contents:")
        for item in project_path.iterdir():
            file_type = "DIR" if item.is_dir() else "FILE"
            print(f"  {file_type}: {item.name}")

        # Check for dbt_project.yml
        dbt_project_yml = project_path / "dbt_project.yml"
        if dbt_project_yml.exists():
            print(f"[PROJECT_CHECK] dbt_project.yml found. Contents:")
            content = dbt_project_yml.read_text()
            print("--- START dbt_project.yml ---")
            print(content)
            print("--- END dbt_project.yml ---")
        else:
            print(f"[PROJECT_CHECK] dbt_project.yml NOT found in {DBT_PROJECT_PATH}")

        return True
    except Exception as e:
        print(f"[PROJECT_CHECK] Error checking DBT project: {e}")
        return False


def _verify_dbt_executable():
    """Verify DBT executable exists and is accessible."""
    print(f"[DBT_EXEC_CHECK] Checking DBT executable at: {DBT_EXECUTABLE_PATH}")
    try:
        dbt_exec = Path(DBT_EXECUTABLE_PATH)
        if not dbt_exec.exists():
            print(f"[DBT_EXEC_CHECK] DBT executable does NOT exist: {DBT_EXECUTABLE_PATH}")
            return False

        if not os.access(DBT_EXECUTABLE_PATH, os.X_OK):
            print(f"[DBT_EXEC_CHECK] DBT executable is NOT executable: {DBT_EXECUTABLE_PATH}")
            return False

        # Try to get dbt version
        try:
            version_proc = subprocess.run([DBT_EXECUTABLE_PATH, "--version"],
                                          capture_output=True, text=True, timeout=30)
            print(f"[DBT_EXEC_CHECK] DBT version check return code: {version_proc.returncode}")
            if version_proc.stdout:
                print(f"[DBT_EXEC_CHECK] DBT version output:")
                print(version_proc.stdout)
            if version_proc.stderr:
                print(f"[DBT_EXEC_CHECK] DBT version stderr:")
                print(version_proc.stderr)
        except subprocess.TimeoutExpired:
            print(f"[DBT_EXEC_CHECK] DBT version command timed out")
        except Exception as ve:
            print(f"[DBT_EXEC_CHECK] Error getting DBT version: {ve}")

        return True
    except Exception as e:
        print(f"[DBT_EXEC_CHECK] Error checking DBT executable: {e}")
        return False


def _ensure_profiles_file():
    """Create profiles.yml if needed. Avoid raising import-time errors.
    Returns path (str) or None if creation failed.
    """
    try:
        # Check if parent directory is writable before attempting to create
        parent_dir = PERSISTENT_PROFILES_DIR.parent
        if not parent_dir.exists() or not os.access(parent_dir, os.W_OK):
            print(f"[dbt_glue_cosmos_dag] Warning: Cannot write to {parent_dir}")
            return None

        PERSISTENT_PROFILES_DIR.mkdir(parents=True, exist_ok=True)
        if not PERSISTENT_PROFILES_PATH.exists():
            profile_content = textwrap.dedent(f"""
            {PROFILE_NAME}:
              target: {TARGET_NAME}
              outputs:
                {TARGET_NAME}:
                  type: glue
                  role_arn: {TARGET_ROLE_ARN}
                  region: {REGION}
                  workers: 2
                  worker_type: G.1X
                  schema: "test_db"
                  idle_timeout: 10
                  location: s3://dev-oth-platform-739275465799-eu-west-1/tmp/
                  connections: dtx-glue-connection1
            """).strip()
            PERSISTENT_PROFILES_PATH.write_text(profile_content)
            print(f"[dbt_glue_cosmos_dag] Created profiles.yml at {PERSISTENT_PROFILES_PATH}")
        return str(PERSISTENT_PROFILES_PATH)
    except (OSError, PermissionError, ValueError) as e:
        print(f"[dbt_glue_cosmos_dag] Warning: unable to prepare profiles.yml: {e}")
        return None
    except Exception as e:  # noqa: BLE001 broad to avoid DAG import failure
        print(f"[dbt_glue_cosmos_dag] Unexpected error preparing profiles.yml: {e}")
        return None


def _assume_role():
    """Assume role with detailed debugging."""
    print(f"[ASSUME_ROLE] Starting role assumption process")
    print(f"[ASSUME_ROLE] Target Role ARN: {TARGET_ROLE_ARN}")
    print(f"[ASSUME_ROLE] Session Name: {SESSION_NAME}")

    # Create initial STS client and check identity
    sts = boto3.client("sts")
    print(f"[ASSUME_ROLE] Created STS client with default credentials")

    # Print caller identity BEFORE assume role
    _print_caller_identity(sts, "BEFORE_ASSUME_ROLE")

    try:
        print(f"[ASSUME_ROLE] Attempting to assume role...")
        resp = sts.assume_role(
            RoleArn=TARGET_ROLE_ARN,
            RoleSessionName=SESSION_NAME,
            DurationSeconds=3600,
        )
        print(f"[ASSUME_ROLE] Successfully assumed role")

        creds = resp["Credentials"]

        # Print the assumed role credentials
        _print_aws_credentials(creds, "ASSUMED_ROLE_CREDENTIALS")

        # Create new STS client with assumed role credentials and verify
        assumed_sts = boto3.client(
            "sts",
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"]
        )

        # Print caller identity AFTER assume role
        _print_caller_identity(assumed_sts, "AFTER_ASSUME_ROLE")

        return creds

    except Exception as e:
        print(f"[ASSUME_ROLE] Failed to assume role: {e}")
        print(f"[ASSUME_ROLE] Exception type: {type(e).__name__}")
        raise


def _write_profiles():
    # For backward compatibility; here we just ensure the persistent file exists.
    path = _ensure_profiles_file()
    return str(PERSISTENT_PROFILES_DIR) if path else str(PERSISTENT_PROFILES_DIR)


def dbt_ls(**kwargs):
    """Run dbt ls with comprehensive debugging."""
    print("[DBT_LS] ===== STARTING DBT LS TASK =====")

    # Step 1: Verify all prerequisites
    print("[DBT_LS] Step 1: Verifying prerequisites...")
    _verify_dbt_executable()
    _verify_dbt_project()

    # Step 2: Assume role and get credentials
    print("[DBT_LS] Step 2: Assuming AWS role...")
    creds = _assume_role()

    # Step 3: Ensure profiles file exists (create if missing)
    print("[DBT_LS] Step 3: Setting up profiles...")
    profiles_dir = _write_profiles()

    # Step 4: Just before execution - Check if profiles.yml exists, if not create it
    print("[DBT_LS] Step 4: Checking profiles.yml existence before execution...")
    if not PERSISTENT_PROFILES_PATH.exists():
        print("[DBT_LS] profiles.yml does not exist, creating it...")
        profile_path = _ensure_profiles_file()
        if not profile_path:
            raise Exception("Failed to create profiles.yml file")

    # Step 5: Validate profiles.yml content
    print("[DBT_LS] Step 5: Validating profiles.yml content...")
    _verify_profiles_file()

    # Step 6: Set up environment
    print("[DBT_LS] Step 6: Setting up environment variables...")
    env = {
        "AWS_ACCESS_KEY_ID": creds["AccessKeyId"],
        "AWS_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
        "AWS_SESSION_TOKEN": creds["SessionToken"],
        "AWS_DEFAULT_REGION": REGION,
        "DBT_PROFILES_DIR": profiles_dir,
    }

    print("[DBT_LS] Environment variables set:")
    for key, value in env.items():
        if 'SECRET' in key or 'TOKEN' in key:
            print(f"  {key}: {value[:20]}...")
        else:
            print(f"  {key}: {value}")

    # Step 7: Prepare and print the command
    cmd = [DBT_EXECUTABLE_PATH, "ls",
        "--log-level debug", "--project-dir", DBT_PROJECT_PATH, "--profiles-dir", profiles_dir]
    print("[DBT_LS] Step 7: Executing command...")
    print(f"[DBT_LS] Command to execute: {' '.join(cmd)}")
    print(f"[DBT_LS] Working directory: {os.getcwd()}")

    # Step 8: Execute command with full debugging
    try:
        print("[DBT_LS] Step 8: Running dbt ls command...")
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env={**os.environ, **env},
            timeout=300  # 5 minute timeout
        )

        print(f"[DBT_LS] Command completed with return code: {proc.returncode}")

        if proc.stdout:
            print("[DBT_LS] STDOUT:")
            print("--- START STDOUT ---")
            print(proc.stdout)
            print("--- END STDOUT ---")
        else:
            print("[DBT_LS] No STDOUT output")

        if proc.stderr:
            print("[DBT_LS] STDERR:")
            print("--- START STDERR ---")
            print(proc.stderr)
            print("--- END STDERR ---")
        else:
            print("[DBT_LS] No STDERR output")

        if proc.returncode != 0:
            print("[DBT_LS] ===== DBT LS FAILED =====")
            raise Exception(f"dbt ls failed with return code {proc.returncode}")
        else:
            print("[DBT_LS] ===== DBT LS COMPLETED SUCCESSFULLY =====")

    except subprocess.TimeoutExpired:
        print("[DBT_LS] Command timed out after 300 seconds")
        raise Exception("dbt ls command timed out")
    except Exception as e:
        print(f"[DBT_LS] Exception during command execution: {e}")
        raise


def dbt_debug(**kwargs):
    """Run dbt debug with comprehensive output."""
    print("[DBT_DEBUG] ===== STARTING DBT DEBUG TASK =====")

    # Assume role and get credentials
    print("[DBT_DEBUG] Assuming AWS role...")
    creds = _assume_role()

    # Ensure profiles file exists (create if missing)
    print("[DBT_DEBUG] Setting up profiles...")
    profiles_dir = _write_profiles()

    # Just before execution: Check if profiles.yml exists, if not create it
    print("[DBT_DEBUG] Checking profiles.yml existence before execution...")
    if not PERSISTENT_PROFILES_PATH.exists():
        print("[DBT_DEBUG] profiles.yml does not exist, creating it...")
        profile_path = _ensure_profiles_file()
        if not profile_path:
            raise Exception("Failed to create profiles.yml file")

    # Validate profiles.yml content
    print("[DBT_DEBUG] Validating profiles.yml content...")
    _verify_profiles_file()

    # Set up environment
    env = {
        "AWS_ACCESS_KEY_ID": creds["AccessKeyId"],
        "AWS_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
        "AWS_SESSION_TOKEN": creds["SessionToken"],
        "AWS_DEFAULT_REGION": REGION,
        "DBT_PROFILES_DIR": profiles_dir,
    }

    # Prepare command with absolute paths
    cmd = [
        DBT_EXECUTABLE_PATH,
        "debug",
        "--log-level debug",
        "--profiles-dir", str(Path(profiles_dir).absolute()),
        "--project-dir", str(Path(DBT_PROJECT_PATH).absolute())
    ]

    print(f"[DBT_DEBUG] Command to execute: {' '.join(cmd)}")

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env={**os.environ, **env},
            timeout=300
        )

        print(f"[DBT_DEBUG] Debug command completed with return code: {proc.returncode}")

        if proc.stdout:
            print("[DBT_DEBUG] STDOUT:")
            print("--- START DEBUG STDOUT ---")
            print(proc.stdout)
            print("--- END DEBUG STDOUT ---")

        if proc.stderr:
            print("[DBT_DEBUG] STDERR:")
            print("--- START DEBUG STDERR ---")
            print(proc.stderr)
            print("--- END DEBUG STDERR ---")

        if proc.returncode != 0:
            print("[DBT_DEBUG] ===== DBT DEBUG FAILED =====")
            raise Exception(f"dbt debug failed with return code {proc.returncode}")
        else:
            print("[DBT_DEBUG] ===== DBT DEBUG COMPLETED SUCCESSFULLY =====")

    except subprocess.TimeoutExpired:
        print("[DBT_DEBUG] Debug command timed out after 300 seconds")
        raise Exception("dbt debug command timed out")
    except Exception as e:
        print(f"[DBT_DEBUG] Exception during debug execution: {e}")
        raise


def dbt_run(**kwargs):
    """Run dbt run with assumed role credentials."""
    print("[DBT_RUN] ===== STARTING DBT RUN TASK =====")

    # Assume role and get credentials
    print("[DBT_RUN] Assuming AWS role...")
    creds = _assume_role()

    # Ensure profiles file exists (create if missing)
    print("[DBT_RUN] Setting up profiles...")
    profiles_dir = _write_profiles()

    # Just before execution: Check if profiles.yml exists, if not create it
    print("[DBT_RUN] Checking profiles.yml existence before execution...")
    if not PERSISTENT_PROFILES_PATH.exists():
        print("[DBT_RUN] profiles.yml does not exist, creating it...")
        profile_path = _ensure_profiles_file()
        if not profile_path:
            raise Exception("Failed to create profiles.yml file")

    # Validate profiles.yml content
    print("[DBT_RUN] Validating profiles.yml content...")
    _verify_profiles_file()

    # Set up environment
    env = {
        "AWS_ACCESS_KEY_ID": creds["AccessKeyId"],
        "AWS_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
        "AWS_SESSION_TOKEN": creds["SessionToken"],
        "AWS_DEFAULT_REGION": REGION,
        "DBT_PROFILES_DIR": profiles_dir,
    }

    cmd = [DBT_EXECUTABLE_PATH, "run",
        "--log-level debug", "--project-dir", DBT_PROJECT_PATH, "--profiles-dir", profiles_dir]
    print(f"[DBT_RUN] Command to execute: {' '.join(cmd)}")

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, **env}, timeout=600)
        print(f"[DBT_RUN] Command completed with return code: {proc.returncode}")

        if proc.stdout:
            print("[DBT_RUN] STDOUT:")
            print("--- START STDOUT ---")
            print(proc.stdout)
            print("--- END STDOUT ---")

        if proc.stderr:
            print("[DBT_RUN] STDERR:")
            print("--- START STDERR ---")
            print(proc.stderr)
            print("--- END STDERR ---")

        if proc.returncode != 0:
            print("[DBT_RUN] ===== DBT RUN FAILED =====")
            raise Exception(f"dbt run failed with return code {proc.returncode}")
        else:
            print("[DBT_RUN] ===== DBT RUN COMPLETED SUCCESSFULLY =====")

    except subprocess.TimeoutExpired:
        print("[DBT_RUN] Command timed out after 600 seconds")
        raise Exception("dbt run command timed out")
    except Exception as e:
        print(f"[DBT_RUN] Exception during command execution: {e}")
        raise


def _validate_import_context():
    """Validate environment without causing import failures.
    Only performs read operations and logs warnings.
    """
    warnings = []

    # Check DBT project path
    try:
        dbt_path = Path(DBT_PROJECT_PATH)
        if not dbt_path.exists():
            warnings.append(f"DBT project path does not exist: {DBT_PROJECT_PATH}")
        elif not dbt_path.is_dir():
            warnings.append(f"DBT project path is not a directory: {DBT_PROJECT_PATH}")
    except (OSError, ValueError) as e:
        warnings.append(f"Cannot validate DBT project path: {e}")

    # Check DBT executable
    try:
        dbt_exec = Path(DBT_EXECUTABLE_PATH)
        if not dbt_exec.exists():
            warnings.append(f"DBT executable not found: {DBT_EXECUTABLE_PATH}")
        elif not os.access(DBT_EXECUTABLE_PATH, os.X_OK):
            warnings.append(f"DBT executable not executable: {DBT_EXECUTABLE_PATH}")
    except (OSError, ValueError) as e:
        warnings.append(f"Cannot validate DBT executable: {e}")

    # Try to validate profiles (non-destructive)
    try:
        if PERSISTENT_PROFILES_PATH.exists():
            if not os.access(PERSISTENT_PROFILES_PATH, os.R_OK):
                warnings.append(f"Existing profiles.yml not readable: {PERSISTENT_PROFILES_PATH}")
        else:
            # Only warn if we definitely can't create it
            parent_writable = PERSISTENT_PROFILES_DIR.parent.exists() and os.access(PERSISTENT_PROFILES_DIR.parent,
                                                                                    os.W_OK)
            if not parent_writable:
                warnings.append("Cannot create profiles.yml - parent directory not writable")
    except (OSError, ValueError) as e:
        warnings.append(f"Cannot validate profiles path: {e}")

    # Log all warnings
    for w in warnings:
        print(f"[dbt_glue_cosmos_dag][IMPORT VALIDATION] {w}")

    return len(warnings) == 0


# Defer validation until DAG creation to avoid import-time side effects
# _validate_import_context() will be called in DAG context

with DAG(
        dag_id="dbt_glue_cosmos_dag",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
) as dag:
    # Validate environment now that we're in DAG context
    validation_passed = _validate_import_context()
    if not validation_passed:
        print("[dbt_glue_cosmos_dag] Some validation checks failed - tasks may not work as expected")

    # Simple dbt tasks using PythonOperator
    dbt_debug_task = PythonOperator(
        task_id="dbt_debug",
        python_callable=dbt_debug
    )

    dbt_ls_task = PythonOperator(
        task_id="dbt_ls",
        python_callable=dbt_ls
    )

    dbt_run_task = PythonOperator(
        task_id="dbt_run",
        python_callable=dbt_run
    )

    # Set task dependencies - debug first, then ls, then run
    dbt_debug_task >> dbt_ls_task >> dbt_run_task