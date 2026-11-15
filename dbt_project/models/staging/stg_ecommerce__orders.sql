with source_data as (
    select
        event_id,
        order_id,
        customer_id,
        lower(trim(customer_email)) as customer_email,
        trim(customer_country) as customer_country,
        trim(customer_city) as customer_city,
        cast(order_timestamp as timestamp) as order_timestamp,
        upper(trim(status)) as order_status,
        upper(trim(payment_method)) as payment_method,
        cast(total_order_value as decimal(12, 2)) as total_order_value_usd,
        currency,
        device_type,
        ingested_at,
        row_number() over (
            partition by order_id 
            order by order_timestamp desc, ingested_at desc
        ) as deduplication_rank
    from {{ source('raw_staging', 'raw_orders_stream') }}
)

select
    event_id,
    order_id,
    customer_id,
    customer_email,
    customer_country,
    customer_city,
    order_timestamp,
    order_status,
    payment_method,
    total_order_value_usd,
    currency,
    device_type,
    ingested_at,
    case 
        when order_status = 'COMPLETED' then true 
        else false 
    end as is_successful_order
from source_data
where deduplication_rank = 1
