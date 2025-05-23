with
    source_data as (
        select * from {{ source("sql_server_dbo", "promos") }}
    ),

    default_record as (
        select
            'unknown_promo' as promo_id,
            0 as discount,
            'missing_status' as status,
            null as _fivetran_deleted,
            to_timestamp('1998-01-01') as _fivetran_synced
    ),

    with_default_record as (
        select * from source_data
        union all
        select * from default_record
    ),

    casted_renamed as (
        select
            {{ dbt_utils.generate_surrogate_key(["promo_id"]) }} as promo_sk,  -- Clave sustituta
            promo_id as promo_name,
            cast(discount as decimal(10,2)) as discount_eur,
            lower(status) as status,  -- Estado original normalizado
            lower(status) = 'active' as is_active,  -- Nueva columna booleana
            _fivetran_deleted as is_deleted,
            _fivetran_synced as at_synced
        from with_default_record
    )

select * from casted_renamed
