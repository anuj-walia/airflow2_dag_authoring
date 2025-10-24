# Barclays dbt Glue Demo (with Seeds)

This project now materializes actual tables in the AWS Glue Data Catalog by loading seed CSVs (customers, orders) and transforming them into staging and mart tables.

## Contents
- Seeds: `seeds/customers.csv`, `seeds/orders.csv`
- Staging models: `stg_customers`, `stg_orders`, `stg_barclays_sample`
- Mart model: `customer_summary`
- Tests: uniqueness & not-null tests in `_staging__models.yml` and `_marts__models.yml`
- Glue profile: `profiles.yml` (role-based execution writing Parquet to S3)

## Data Flow
```
customers.csv  --->  stg_customers  --->
								 \      
orders.csv     --->  stg_orders     (future joins)  ---> customer_summary (currently from stg_customers)
```
Seeds provide initial data so the Glue database (`barclays_glue_dbt`) contains concrete tables immediately after `dbt seed` and `dbt run`.

## Getting Started (Local / Container)
```bash
source /usr/local/airflow/dbt_venv/bin/activate
cd /usr/local/airflow/dags/dbt-project-no-arn
export DBT_PROFILES_DIR=$PWD
export AWS_REGION=${AWS_REGION:-us-east-2}
export AWS_DEFAULT_REGION=$AWS_REGION

# List resources
dbt ls --profile barclays_glue_dbt

# Load seeds (creates Glue tables for seeds as external Parquet via S3 location)
dbt seed --profile barclays_glue_dbt --full-refresh

# Run models (build staging + mart tables)
dbt run --profile barclays_glue_dbt

# Run tests
dbt test --profile barclays_glue_dbt
```

## Airflow Integration
Update or create an Airflow task sequence:
1. `dbt seed`
2. `dbt run`
3. `dbt test`

Ensure the Airflow connection/role has permissions for:
- Glue: GetDatabase, CreateDatabase, GetTable, CreateTable, UpdateTable
- S3: PutObject/GetObject/List on `s3://assumerole-s3-glue-dbt-athena/dbt/*`

## Observing Results
After running:
- Glue Catalog should list tables: `customers`, `orders`, `stg_customers`, `stg_orders`, `stg_barclays_sample`, `customer_summary` (names may be normalized per adapter rules)
- S3 prefix `dbt/` contains Parquet artifacts for tables.

## Reset / Clean
```bash
dbt clean
dbt run --full-refresh
```

## Extending
- Replace seeds with ingestion from raw Glue tables; then adjust staging models to select from `source()` definitions and re-enable `models/staging/_sources.yml`.
- Enhance `customer_summary` to join `stg_orders` for metrics (order_count, total_amount).
- Add documentation site: `dbt docs generate && dbt docs serve`.

## Troubleshooting
- If seed tables do not appear, verify IAM role can write to S3 and create Glue tables.
- Ensure `DBT_PROFILES_DIR` points to the directory containing `profiles.yml`.
- Use `--debug` flag for verbose adapter logs: `dbt seed --debug`.

## License / Notes
Demo-only configuration; not production hardened.
