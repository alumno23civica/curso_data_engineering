with budget as (
    select *
    from {{ ref('stg_google_sheets__budget') }}
),

products as (
    select *
    from {{ ref('stg_sql_server__products') }}
),

joined as (
    select
        budget.budget_id,
        budget.product_sk,
        budget.month,
        budget.month_trunc,
        budget.quantity,
        products.product_name,
        products.product_price,
        products.inventory_qty,
        budget.quantity * products.product_price as budgeted_cost,
        budget.at_synced as budget_synced_at,
        products.at_synced as product_synced_at
    from budget
    left join products
        on budget.product_sk = products.product_sk
)

select *
from joined