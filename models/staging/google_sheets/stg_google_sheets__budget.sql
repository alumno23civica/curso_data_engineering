with source as (

    select * from {{ source('google_sheets', 'budget') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['product_id', 'month']) }} as budget_id,
        cast(quantity as integer) as quantity,
        month,
        date_trunc('month', month) as month_trunc,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
        _fivetran_synced as at_synced

    from source

)

select * from renamed