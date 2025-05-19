-- models/intermediate/int_spacex_core_usage_filtered.sql

with core_usage as (
    select * from {{ ref('stg_spacex__core_usage') }}
),

launches as (
    select
        launch_id,
        is_upcoming
    from {{ ref('stg_spacex__launches') }}
),

filtered as (
    select
        cu.*
    from core_usage cu
    left join launches l
        on cu.launch_id = l.launch_id
    where cu.launch_id is not null
      and cu.core_id is not null
      and (l.is_upcoming = false or l.is_upcoming is null) -- ✅ corregido
)

select * from filtered
