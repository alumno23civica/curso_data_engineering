-- models/marts/dimensions/dim_launchpad.sql

/**
  Dimension table for Launchpads.
  Based on the flattened staging data (stg_spacex_launchpads).
  Includes launchpad attributes, coordinates, and statistics.
  Grain: One row per unique Launchpad location.
*/
with stg_launchpads as (

    -- Selecciona desde el modelo staging aplanado de plataformas de lanzamiento
    -- Este staging ya selecciona, castea y renombra las columnas de la tabla raw
    select * from {{ ref('stg_spacex__launchpads') }}

),

unknown_member as (

    select
        {{ dbt_utils.generate_surrogate_key(['\'unknown_launchpad\'']) }} as launchpad_sk,
        'unknown_launchpad' as launchpad_id,

        -- Atributos de la Plataforma de Lanzamiento
        'unknown'::varchar as launchpad_name,
        'unknown'::varchar as launchpad_full_name,
        'unknown'::varchar as locality,
        'unknown'::varchar as region,
        'unknown'::varchar as timezone,
        -1::number as latitude, -- Coordenadas
        -1::number as longitude, -- Coordenadas
        'unknown'::varchar as status,
        false::boolean AS is_active, -- booleano rapido acceso

        -- Estadísticas totales de la plataforma (considerar como atributos descriptivos en la dimensión)
        -1::number as launch_attempts,
        -1::number as launch_successes


),

final as (
    select
    -- Clave Primaria de la Dimensión
    -- Usamos el ID renombrado del staging
    {{ dbt_utils.generate_surrogate_key(['launchpad_id']) }} as launchpad_sk,

    launchpad_id,

    -- Atributos de la Plataforma de Lanzamiento
    launchpad_name,
    launchpad_full_name,
    locality,
    region,
    timezone,
    latitude, -- Coordenadas
    longitude, -- Coordenadas
    status,
    (status = 'active') AS is_active, -- booleano rapido acceso

    -- Estadísticas totales de la plataforma (considerar como atributos descriptivos en la dimensión)
    launch_attempts,
    launch_successes,

    -- Si has añadido columnas de metadatos en staging y quieres incluirlas aquí, selecciónalas.
    -- _dbt_cdc_ts, _dbt_source_name, etc.

from stg_launchpads

-- Opcional: Puedes añadir filtros si no quieres todas las plataformas en la dimensión
-- where status = 'active' -- Ejemplo: solo plataformas activas
-- UNION con la fila del miembro desconocido.
    UNION ALL
    select * from unknown_member

)

-- Selección final
-- Puedes añadir lógica de SCD si es necesario (ej. para auto_update, is_tbd, detalles si cambian)
select * from final