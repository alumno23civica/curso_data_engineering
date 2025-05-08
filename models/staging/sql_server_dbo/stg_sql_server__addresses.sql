with source as (

    select * from {{ source('sql_server_dbo', 'addresses') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} as address_sk,
        lpad(zipcode, 5, '0') as zipcode,
        lower(country) as country,
        lower(address),
        lower(state) as state,
        _fivetran_deleted as is_deleted,
        _fivetran_synced as at_synced

    from source

)

select * from renamed