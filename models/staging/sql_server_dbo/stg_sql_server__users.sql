with source as (

    select * from {{ source('sql_server_dbo', 'users') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,
        updated_at,
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} as address_sk,
        lower(concat(first_name,' ',last_name)) as user_name,
        lower(first_name),
        lower(last_name),
        created_at,
        trim(phone_number)as phone_number,
        case 
        when {{ is_email('email') }} then email
        else 'Invalid Email'
        end as email,
        _fivetran_deleted as is_deleted,
        _fivetran_synced at_synced

    from source

)

select * from renamed