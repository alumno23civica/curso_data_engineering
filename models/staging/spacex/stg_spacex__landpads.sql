-- models/staging/spacex/stg_spacex_landpads.sql

/**
  Staging model for SpaceX Landpads data.
  Selects, casts, and renames raw landpad data from the source.
  Grain: One row per landpad.
  Source: Raw 'landpads' table (from API endpoint /v4/landpads).
*/

with source as (

    -- Referencia la tabla raw de cores.
    -- Asegúrate que tu source 'spacex' en schema.yml apunte a esta tabla raw con la estructura mostrada.
    select * from {{ source('spacex', 'landpads') }}

),

renamed as (

select
    -- Clave Primaria del Staging
    -- Casteamos el ID a VARCHAR (asumimos que los IDs de la API son strings)
    id::varchar as landpad_id,

    -- Atributos Descriptivos (Casteados a VARCHAR si no lo son ya)
    name::varchar as landpad_name,
    full_name::varchar as landpad_full_name,
    status::varchar as landpad_status,
    type::varchar as landpad_type, -- Debería ser 'RTLS' o 'ASDS'
    locality::varchar as landpad_locality,
    region::varchar as landpad_region,
    wikipedia::varchar as landpad_wikipedia,
    details::varchar as landpad_details,
    image_url_large_first::varchar as landpad_image_url_large_first, -- URL de imagen

    -- Coordenadas Geográficas (Casteadas a NUMBER/FLOAT)
    latitude::number as landpad_latitude,
    longitude::number as landpad_longitude,

    -- Métricas Totales para el Landpad (Casteadas a NUMBER/INTEGER)
    landing_attempts::number as total_landing_attempts,
    landing_successes::number as total_landing_successes,

    -- Lista de IDs de Lanzamientos Asociados (Casteada a VARCHAR como string)
    launch_ids_list::varchar as associated_launch_ids_list -- Esto es un string coma-separado


-- Selecciona desde la fuente 'spacex', tabla 'landpads'
-- Asegúrate de que esta fuente y tabla estén definidas en tu models/staging/spacex/schema.yml
from source

)

select * from renamed