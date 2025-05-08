with source as (

    select * from {{ source('google_sheets', 'budget') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['product_id', 'month']) }} as budget_id,
        cast(quantity as integer) as quantity,
        month,
        date_trunc('month', month) as month_trunc,
        product_id,
        _fivetran_synced as at_synced

    from source

)

select * from renamed