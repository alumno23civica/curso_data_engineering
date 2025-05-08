with source as (
    select * from {{ source('sql_server_dbo', 'orders') }}
),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_sk,
        lower(trim(shipping_service)) as shipping_service,
        cast(shipping_cost as decimal(10,2)) as shipping_cost,
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} as address_sk,
        created_at,
        date_trunc('day', created_at) as order_date,
        coalesce(nullif(trim(promo_id), ''), 'unknown_promo') as promo_id,
        coalesce(estimated_delivery_at,created_at + interval '5 days') as estimated_delivery_at,
        delivered_at,
        datediff('day', created_at, delivered_at) as delivery_days,
        cast(order_cost as decimal(10,2)) as order_cost,
        cast(order_total as decimal(10,2)) as order_total,
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,
        lower(trim(status)) as status,
        case when delivered_at is not null then true else false end as is_delivered,
        coalesce(tracking_id, 'unknown') as tracking_id,
        _fivetran_deleted as is_deleted,
        _fivetran_synced as at_synced

    from source
)

select * from renamed
