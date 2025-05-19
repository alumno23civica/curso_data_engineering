-- models/marts/dimensions/dim_customer.sql

/**
  Dimension table for SpaceX Customers.
  Grain: One row per unique customer name.
  Primary Key: customer_sk (Surrogate Key).
  Natural Key: customer_id (derived from cleaned customer name).
  Related Fact: bridge_payload_customer (via customer_sk).
*/

with stg_payloads as (

    select
        customers_list
    from {{ ref('stg_spacex__payloads') }}
    where customers_list is not null and trim(customers_list) != ''

),

unnested_customers as (

    select
        trim(split_data.value)::varchar as customer_name
    from stg_payloads,
         {{ split_string_list('customers_list', 'split_data') }}
    where split_data.value is not null and trim(split_data.value) != ''

),

unique_customers as (

    select distinct
        customer_name
    from unnested_customers
    order by customer_name

),

unknown_member as (

    select
        {{ dbt_utils.generate_surrogate_key(['\'unknown_customer\'']) }} as customer_sk,
        cast('unknown_customer' as varchar) as customer_id,
        cast('Unknown' as varchar) as customer_name

),

final_dimension_attributes as (

    select
        customer_name as customer_id,
        customer_name
    from unique_customers
    where customer_name is not null

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
        *
    from final_dimension_attributes

    union all

    select
        customer_sk,
        customer_id,
        customer_name
    from unknown_member

)

select * from final
