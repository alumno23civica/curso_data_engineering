-- models/marts/dimensions/dim_rocket.sql

/**
  Dimension table for Rockets.
  Based on the flattened staging data (stg_spacex_rockets).
  Includes a simplified set of core rocket attributes as requested.
  Grain: One row per unique Rocket type/version.
  Includes an 'unknown' member for referential integrity.
*/
with stg_rockets as (

    -- Selecciona desde el modelo staging aplanado de cohetes
    select * from {{ ref('stg_spacex__rockets') }}

),

-- CTE para el miembro desconocido de la dimensión Rocket
unknown_rocket_member as (

    select
        {{ dbt_utils.generate_surrogate_key(['\'unknown_rocket\'']) }} as rocket_sk, -- Clave subrogada para el miembro desconocido
        'unknown_rocket' as rocket_id, -- ID natural para referencia en coalesce de los hechos
        'Unknown Rocket' as rocket_name,
        'Unknown Type' as rocket_type,
        FALSE as is_active, -- O 0, dependiendo de tu tipo de dato booleano
        0 as cost_per_launch,
        0 as success_rate_pct,
        0 as number_of_boosters,
        0 as height_meters,
        0 as mass_kg,
        0 as engines_number,
        'Unknown' as engine_type,
        FALSE as first_stage_reusable,
        0 as first_stage_engines,
        0 as second_stage_engines,
        'Unknown' as manufacturer_company,
        'Unknown' as origin_country,
        '1900-01-01'::date as first_flight -- Usamos una fecha por defecto consistente con 'dim_time'
    
    -- Usar DUAL/FROM (SELECT 1) para generar una sola fila si tu DB lo requiere (ej. Oracle)
    -- En la mayoría de DBs modernas (Snowflake, BigQuery, Postgres, Redshift), un SELECT simple es suficiente.
    -- FROM (SELECT 1) AS dummy_table -- Descomentar si tu DB necesita un FROM

)

select
    -- Clave Primaria de la Dimensión
    {{ dbt_utils.generate_surrogate_key(['rocket_id']) }} as rocket_sk, -- Generamos la clave subrogada para los cohetes reales
    rocket_id,

    -- Atributos del Cohete (el conjunto solicitado)
    rocket_name,
    rocket_type,
    is_active,
    cost_per_launch,
    success_rate_pct,
    number_of_boosters,
    height_meters,
    mass_kg,
    engines_number,
    engine_type,
    first_stage_reusable,
    first_stage_engines,
    second_stage_engines,
    manufacturer_company,
    origin_country,
    first_flight

from stg_rockets

-- Opcional: Si solo quieres incluir cohetes activos en la dimensión (¡aplicar a los cohetes REALES!)
-- where is_active is true

UNION ALL

-- Combina los cohetes reales con el miembro desconocido
select * from unknown_rocket_member