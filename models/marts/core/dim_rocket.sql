-- models/marts/dimensions/dim_rocket.sql

/**
  Dimension table for Rockets.
  Based on the flattened staging data (stg_spacex_rockets).
  Includes a simplified set of core rocket attributes as requested.
  Grain: One row per unique Rocket type/version.
*/
with stg_rockets as (

    -- Selecciona desde el modelo staging aplanado de cohetes
    select * from {{ ref('stg_spacex__rockets') }}

)

select
    -- Clave Primaria de la Dimensión
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
    engines_number, -- Número TOTAL de motores (generalmente de la primera etapa principal)
    engine_type,    -- Tipo de motor (generalmente de la primera etapa principal)
    first_stage_reusable, -- Booleano sobre la reutilización de la primera etapa
    first_stage_engines,  -- Número de motores en la primera etapa
    second_stage_engines, -- Número de motores en la segunda etapa
    manufacturer_company,
    origin_country,
    first_flight -- Fecha del primer vuelo

    -- Los campos NO incluidos en esta selección son omitidos en esta dimensión.

from stg_rockets

-- Opcional: Si solo quieres incluir cohetes activos en la dimensión
-- where is_active is true