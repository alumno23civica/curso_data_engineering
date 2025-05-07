with
    source_data as (select * from {{ source("sql_server_dbo", "products") }}),

    default_record as (
        select
            'unknown_product' as product_id,
            -1 as price,
            'missing_name' as name,
            0 as inventory,
            null as _fivetran_deleted,
            to_timestamp('1998-01-01') as _fivetran_synced
    ),

    with_default_record as (
        select *
        from source_data
        union all
        select *
        from default_record
    ),

    renamed as (

        select
            product_id,  -- Clave primaria
            cast(price as decimal) as product_price,
            trim(lower(name)) as product_name,  -- Remueve espacios y convierte a minúsculas
            inventory as stock,
            _fivetran_synced as at_synced

        from with_default_record
        where _fivetran_deleted is distinct from true

    )

select *
from renamed
