with source as (

    select * from {{ source('sql_server_dbo', 'events') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} as event_sk,
        page_url,
        event_type,
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
        session_id,
        created_at,
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_sk,
        _fivetran_deleted as is_deleted,
        _fivetran_synced at_synced

    from source
    

)

select * from renamed