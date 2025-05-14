with order_items as (
    select *
    from {{ ref('stg_sql_server__order_items') }}
),

orders as (
    select 
        order_sk,
        user_sk,
        address_sk,
        promo_id,
        created_at,
        order_total,
        shipping_cost,
        estimated_delivery_at,
        delivered_at,
        status
    from {{ ref('stg_sql_server__orders') }}
),

joined as (
    select 
        -- granularidad: una fila por línea de pedido
        oi.order_sk,
        oi.product_sk,
        oi.quantity,
        --oi._fivetran_synced as order_item_synced_at,

        -- datos del pedido (cabecera), denormalizados
        o.user_sk,
        o.address_sk,
        o.promo_id,
        o.created_at as order_created_at,
        o.estimated_delivery_at,
        o.delivered_at,
        o.order_total,
        o.shipping_cost,
        o.status,
        p.product_price,

        -- posibles métricas calculadas
        oi.quantity * p.product_price as line_total
    from order_items oi
    left join {{ ref('stg_sql_server__products') }} p
        on oi.product_sk = p.product_sk
    left join orders o
        on oi.order_sk = o.order_sk
)

select *
from joined
