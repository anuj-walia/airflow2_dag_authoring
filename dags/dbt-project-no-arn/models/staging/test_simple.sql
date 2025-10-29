{{ config(materialized='table') }}

-- Simple test model to verify Glue adapter works
select
    1 as test_id,
    'test_value' as test_name,
    current_timestamp as created_at
