{{ config(materialized='table') }}

select
    1 as customer_id,
    'John Doe' as customer_name,
    'john@example.com' as email

