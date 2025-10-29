{{ config(materialized='table') }}

-- Staging orders sourced from seed (orders.csv)
select
	order_id,
	customer_id,
	order_date,
	amount
from {{ ref('orders') }}