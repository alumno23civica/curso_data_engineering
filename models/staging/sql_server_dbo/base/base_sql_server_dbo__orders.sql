with
    source as (select * from {{ source("sql_server_dbo", "orders") }}),

    renamed as (

        select
            order_id,
            coalesce(nullif(shipping_service, ''), 'unknown_service') as shipping_service,
            cast(shipping_cost as decimal(10,2)) as shipping_cost,
            address_id,
            created_at,
            coalesce(nullif(promo_id, ''), 'unknown_promo') as promo_id,
            estimated_delivery_at,
            cast(order_cost as decimal(10,2)) as order_cost,
            user_id,
            order_total,
            delivered_at,
            tracking_id,
            status,
            _fivetran_synced

        from source
        where _fivetran_deleted is distinct from true

    )

select *
from renamed
