-- models/marts/dimensions/dim_core.sql

/**
  Dimension table for Falcon 9/Heavy Cores.
  Based on the staging data (stg_spacex_cores).
  Includes descriptive attributes and total statistics for each core asset.
  Grain: One row per unique Core (booster).
  Related Fact: fact_core_usage (joined via core_id).
*/
with stg_cores as (

    -- Selecciona desde el modelo staging de cores
    -- Este staging ya selecciona, castea y renombra las columnas de la tabla raw
    select * from {{ ref('stg_spacex__cores') }}

),

-- Opcional pero recomendado: Crear una fila de "Miembro Desconocido"
-- para manejar casos en fact_core_usage donde el core_id sea NULL o inválido.
unknown_member as (

    select
        -- Define la clave primaria del miembro desconocido
        -- Usa un valor que no colisione con IDs reales de la API
        {{ dbt_utils.generate_surrogate_key(['\'unknown_core\'']) }} as core_sk,
        'unknown_core'::varchar as core_id,

        -- Atributos placeholders con tipos de dato que coincidan con stg_cores
        'Unknown'::varchar as core_serial,
        -1::number as core_block, -- Usar un valor numérico fuera del rango normal
        'Unknown'::varchar as core_status,

        -1::number as total_reuse_count,
        -1::number as total_rtls_attempts,
        -1::number as total_rtls_landings,
        -1::number as total_asds_attempts,
        -1::number as total_asds_landings,

       -- '1900-01-01'::timestamp_ntz as core_last_update_utc, -- Usar una fecha mínima o placeholder

       -- 'Unknown'::varchar as associated_launch_ids_list -- Placeholder para la lista

    -- Asegúrate de que los tipos de dato aquí coincidan EXACTAMENTE con los tipos del CTE 'stg_cores'.
    -- Revisa stg_spacex_cores.sql si no estás seguro.

),

final as (

    select
        -- Clave Primaria de la Dimensión
        -- Renombra el ID de staging a core_id
        {{ dbt_utils.generate_surrogate_key(['core_id']) }} as core_sk,
        core_id,

        -- Atributos Descriptivos del Core
        core_serial,
        block as core_block,
        status as core_status,

        -- Métricas Acumuladas/Totales del Core como Activo
        -- Estas son estadísticas sobre el ciclo de vida del core
        reuse_count as total_reuse_count,
        rtls_attempts as total_rtls_attempts,
        rtls_landings as total_rtls_landings,
        asds_attempts as total_asds_attempts,
        asds_landings as total_asds_landings,

        -- Atributos de Fecha/Hora
        --last_update as core_last_update_utc,

        -- Atributos de Lista (como string aplanado)
       -- launches as associated_launch_ids_list -- String de IDs de lanzamientos donde este core voló

    from stg_cores

    -- UNION con el miembro desconocido
    UNION ALL
    select * from unknown_member

)

-- Selección final. Puedes añadir lógica de SCD (Slowly Changing Dimensions) aquí si es necesario (ej. Type 2 para 'status').
-- Para empezar, una dimensión simple Type 1 (la última versión de los atributos) es suficiente.
select * from final