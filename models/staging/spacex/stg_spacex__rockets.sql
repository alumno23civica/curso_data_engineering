-- models/staging/stg_spacex_rockets.sql

/**
  Modelo Staging para los datos raw de cohetes.
  Selecciona y renombra columnas desde el source 'spacex.rockets',
  manteniendo las columnas semi-estructuradas para transformaciones posteriores.
*/
with source as (

    -- Referencia la tabla raw de cohetes en tu source 'spacex'
    -- Esto toma todas las columnas de esa tabla.
    select * from {{ source('spacex', 'rockets') }}

),

renamed as (

    select
        -- Clave Primaria
        id as rocket_id,

        -- Información básica
        name as rocket_name,
        type as rocket_type,
        active as is_active,
        stages as number_of_stages, -- Renombrado para claridad
        boosters as number_of_boosters, -- Renombrado para claridad

        -- Rendimiento y Costo
        cost_per_launch,
        success_rate_pct,

        -- Fechas y Origen
        first_flight, -- Asumimos que es un tipo fecha/timestamp o similar
        country as origin_country,
        company as manufacturer_company,

        -- Enlaces y Descripciones
        wikipedia as wikipedia_url,
        description as rocket_description,

        -- Columnas que contienen estructuras anidadas o listas
        -- Las mantenemos con un sufijo '_json' (o '_struct'/'_list' según convención)
        -- para indicar que son semi-estructuradas y requerirán parsing/aplanamiento
        -- en modelos posteriores si necesitas acceder a sus sub-campos.
        height as height_struct, -- O height_json si se cargó como JSON string
        diameter as diameter_struct, -- O diameter_json
        mass as mass_struct, -- O mass_json
        first_stage as first_stage_struct, -- O first_stage_json
        second_stage as second_stage_struct, -- O second_stage_json
        engines as engines_struct, -- O engines_json
        landing_legs as landing_legs_struct, -- O landing_legs_json
        payload_weights as payload_weights_struct, -- O payload_weights_json
        flickr_images as flickr_images_list, -- O flickr_images_json

        -- Si hubiera alguna columna de control de carga (ej. de DLT o Snowpipe)
        -- _loaded_at,
        -- _file_name

    from source

)

select * from renamed