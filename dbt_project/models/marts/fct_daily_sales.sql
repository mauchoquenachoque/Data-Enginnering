with orders as (
    select * from {{ ref('stg_ecommerce__orders') }}
)

select
    cast(date_trunc('day', order_timestamp) as date) as sales_date,
    customer_country,
    payment_method,
    count(distinct order_id) as total_transactions,
    sum(case when is_successful_order then 1 else 0 end) as successful_transactions,
    sum(case when not is_successful_order then 1 else 0 end) as failed_or_cancelled_transactions,
    sum(case when is_successful_order then total_order_value_usd else 0 end) as gross_revenue_usd,
    avg(case when is_successful_order then total_order_value_usd else null end) as avg_order_value_usd,
    count(distinct customer_id) as active_purchasing_customers
from orders
group by
    1, 2, 3
