with source as (

    select * from {{ source('sql_server_dbo', 'users') }}

),

renamed as (

    select
        user_id,
        updated_at,
        address_id,
        concat(first_name,' ',last_name) as name,
        created_at,
        phone_number,
        email,
        _fivetran_synced

    from source
    where _fivetran_deleted is distinct from true

)

select * from renamed