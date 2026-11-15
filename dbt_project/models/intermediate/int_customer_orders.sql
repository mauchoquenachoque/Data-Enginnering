with orders as (
    select * from {{ ref('stg_ecommerce__orders') }}
)

select
    customer_id,
    customer_email,
    customer_country,
    count(order_id) as total_orders_placed,
    sum(case when is_successful_order then 1 else 0 end) as successful_orders_count,
    sum(case when is_successful_order then total_order_value_usd else 0 end) as lifetime_spend_usd,
    avg(case when is_successful_order then total_order_value_usd else null end) as average_order_value_usd,
    min(order_timestamp) as first_order_at,
    max(order_timestamp) as most_recent_order_at
from orders
group by
    customer_id,
    customer_email,
    customer_country
