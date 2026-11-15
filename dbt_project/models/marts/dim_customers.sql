with customer_metrics as (
    select * from {{ ref('int_customer_orders') }}
)

select
    customer_id,
    customer_email,
    customer_country,
    total_orders_placed,
    successful_orders_count,
    lifetime_spend_usd,
    round(coalesce(average_order_value_usd, 0), 2) as average_order_value_usd,
    first_order_at,
    most_recent_order_at,
    case
        when lifetime_spend_usd >= 2000 then 'VIP / High Value'
        when lifetime_spend_usd >= 500 then 'Regular / Medium Value'
        when lifetime_spend_usd > 0 then 'Occasional Buyer'
        else 'Prospect / Inactive'
    end as customer_tier,
    case
        when most_recent_order_at >= current_date - interval '30 days' then 'Active'
        when most_recent_order_at >= current_date - interval '90 days' then 'At Risk'
        else 'Churned'
    end as lifecycle_status
from customer_metrics
