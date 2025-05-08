with budget as (
    select *
    from {{ ref('base_google_sheets__budget') }}
),

products as (
    select *
    from {{ ref('base_sql_server_dbo__products') }}
),

joined as (
    select
        budget.budget_id,
        budget.product_id,
        budget.month,
        budget.month_trunc,
        budget.quantity,
        products.product_name,
        products.product_price,
        products.stock,
        budget.quantity * products.product_price as budgeted_cost,
        budget.at_synced as budget_synced_at,
        products.at_synced as product_synced_at
    from budget
    left join products
        on budget.product_id = products.product_id
)

select *
from joined
