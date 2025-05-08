with source as (

    select * from {{ source('sql_server_dbo', 'order_items') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} as order_item_id,
        order_id,
        product_id,
        cast(quantity as integer) as quantity,
        _fivetran_deleted as is_deleted,
        _fivetran_synced as at_synced

    from source
    where _fivetran_deleted is distinct from true

)

select * from renamed
