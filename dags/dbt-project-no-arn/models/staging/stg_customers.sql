{{ config(materialized='table') }}

-- Staging customers sourced from seed (customers.csv)
select
    customer_id::int as customer_id,
    customer_name,
    email
from {{ ref('customers') }}