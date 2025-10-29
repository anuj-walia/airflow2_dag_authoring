-- Customer summary mart with order aggregations
{{ config(materialized='table') }}

-- Simplified mart: just duplicates staging customers adding a load timestamp.
select
  customer_id,
  customer_name,
  email,
  current_timestamp as mart_loaded_at
from {{ ref('stg_customers') }}