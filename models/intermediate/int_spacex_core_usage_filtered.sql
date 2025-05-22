
-- models/intermediate/int_spacex_core_usage_filtered.sql

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

        cu.*, -- Seleccionamos todas las columnas de stg_spacex__core_usage
        l.is_upcoming -- Incluimos is_upcoming del lanzamiento para el filtro

    from core_usage cu
    left join launches l
        on cu.launch_id = l.launch_id
    where cu.launch_id is not null
      and cu.core_id is not null
      and (l.is_upcoming = false or l.is_upcoming is null) -- Filtramos lanzamientos no próximos o con estado desconocido
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['launch_id', 'core_id']) }} as core_usage_sk, -- Generamos la clave subrogada
        * -- Seleccionamos todas las columnas del CTE 'filtered'
    from filtered
)

select * from final
