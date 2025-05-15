-- models/intermediate/int_spacex_rockets_cleaned.sql

/**
  Modelo intermedio para limpiar y extraer campos clave del modelo staging de cohetes.
  Extrae campos de las columnas semi-estructuradas (tipo VARIANT/JSON).
  Usa TRY_PARSE_JSON y GET() para acceder a campos de forma segura.
*/
with stg_rockets as (

    select * from {{ ref('stg_spacex__rockets') }}

)

select
    -- Selecciona columnas básicas y renombradas
    stg_rockets.rocket_id,
    stg_rockets.rocket_name,
    stg_rockets.rocket_type,
    stg_rockets.is_active,
    stg_rockets.number_of_stages,
    stg_rockets.number_of_boosters,
    stg_rockets.cost_per_launch,
    stg_rockets.success_rate_pct,
    stg_rockets.first_flight,
    stg_rockets.origin_country,
    stg_rockets.manufacturer_company,
    stg_rockets.wikipedia_url,
    stg_rockets.rocket_description,

    -- No incluimos las columnas semi-estructuradas originales en el SELECT final
    -- stg_rockets.height_struct, ... etc.

    -- --- Extracción de campos clave ---
    -- Usamos TRY_PARSE_JSON() para convertir el VARCHAR a VARIANT,
    -- luego GET() para acceder al campo, y TRY_CAST() para el tipo final.

    -- De 'height_struct'
    try_cast(get(try_parse_json(height_struct), 'meters') as float) as height_meters,
    try_cast(get(try_parse_json(height_struct), 'feet') as float) as height_feet,

    -- De 'diameter_struct'
    try_cast(get(try_parse_json(diameter_struct), 'meters') as float) as diameter_meters,
    try_cast(get(try_parse_json(diameter_struct), 'feet') as float) as diameter_feet,

    -- De 'mass_struct'
    try_cast(get(try_parse_json(mass_struct), 'kg') as float) as mass_kg,
    try_cast(get(try_parse_json(mass_struct), 'lb') as float) as mass_lb,

    -- De 'engines_struct'
    try_cast(get(try_parse_json(engines_struct), 'number') as integer) as engines_number,
    try_cast(get(try_parse_json(engines_struct), 'type') as varchar) as engine_type,
    try_cast(get(try_parse_json(engines_struct), 'version') as varchar) as engine_version,
    try_cast(get(try_parse_json(engines_struct), 'propellant_1') as varchar) as engine_propellant_1,
    try_cast(get(try_parse_json(engines_struct), 'propellant_2') as varchar) as engine_propellant_2,
    -- Para acceder a campos anidados dentro de engines (ej. thrust_sea_level.kN)
    -- try_cast(get(get(try_parse_json(engines_struct), 'thrust_sea_level'), 'kN') as float) as engines_thrust_sea_level_kN,


    -- De 'landing_legs_struct'
    try_cast(get(try_parse_json(landing_legs_struct), 'number') as integer) as landing_legs_number,
    try_cast(get(try_parse_json(landing_legs_struct), 'material') as varchar) as landing_legs_material,

    -- Extracción de 'first_stage_struct' y 'second_stage_struct'
    try_cast(get(try_parse_json(first_stage_struct), 'reusable') as boolean) as first_stage_reusable,
    try_cast(get(try_parse_json(second_stage_struct), 'engines') as integer) as second_stage_engine_count,
    try_cast(get(try_parse_json(second_stage_struct), 'fuel_amount_tons') as float) as second_stage_fuel_tons,

    -- NOTA sobre listas:
    -- Acceder a un elemento de lista (ej. el primero, índice 0):
    -- try_cast(get(try_parse_json(flickr_images_list), 0) as varchar) as first_flickr_image_url,
    -- Acceder a un campo dentro de un objeto que está en una lista (ej. 'kg' del primer peso):
    -- try_cast(get(get(try_parse_json(payload_weights_struct), 0), 'kg') as float) as first_payload_weight_kg

from stg_rockets
-- Opcional: Añadir filtros si es necesario
-- where rocket_id is not null