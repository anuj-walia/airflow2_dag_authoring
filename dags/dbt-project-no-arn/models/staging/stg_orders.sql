{{ config(materialized='table') }}

-- Staging orders sourced from seed (orders.csv)
select
	order_id::int as order_id,
	customer_id::int as customer_id,
	to_timestamp(order_date, 'YYYY-MM-DD') as order_date,
	amount::double as amount
from {{ ref('orders') }}