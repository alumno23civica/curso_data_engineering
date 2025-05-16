-- models/staging/stg_spacex_rockets.sql

/**
  Modelo Staging para los datos raw de cohetes
*/
with source as (

    select * from {{ source('spacex', 'rockets') }}

),

renamed as (

    select
        -- Clave Primaria
        ID as rocket_id, -- Renombra ID a rocket_id

        -- Información básica
        NAME as rocket_name, -- Renombra NAME a rocket_name
        TYPE as rocket_type, -- Renombra TYPE a rocket_type
        ACTIVE as is_active, -- Renombra ACTIVE a is_active (booleano)
        BOOSTERS as number_of_boosters, -- Renombra BOOSTERS a number_of_boosters

        -- Rendimiento y Costo
        COST_PER_LAUNCH, -- Ya en buen formato
        SUCCESS_RATE_PCT, -- Ya en buen formato

        -- Fechas y Origen
        try_cast(FIRST_FLIGHT as date) as first_flight, 

        COUNTRY as origin_country, -- Renombra COUNTRY a origin_country
        COMPANY as manufacturer_company, -- Renombra COMPANY a manufacturer_company

        -- Enlaces y Descripciones
        WIKIPEDIA as wikipedia_url, -- Renombra WIKIPEDIA a wikipedia_url
        DESCRIPTION as rocket_description, -- Renombra DESCRIPTION a rocket_description

        -- --- Columnas Aplanadas Extraídas en Python ---
        -- Seleccionamos los nombres de columna tal como vienen en el source (mayúsculas)
        -- y los renombramos a snake_case minúsculas si es necesario.

        HEIGHT_METERS, -- Ya en buen formato
        HEIGHT_FEET, -- Ya en buen formato
        DIAMETER_METERS, -- Ya en buen formato
        DIAMETER_FEET, -- Ya en buen formato
        MASS_KG, -- Ya en buen formato
        MASS_LB, -- Ya en buen formato

        ENGINES_NUMBER, -- Ya en buen formato
        ENGINES_TYPE as engine_type, -- Renombra ENGINES_TYPE a engine_type
        ENGINES_VERSION as engine_version, -- Renombra ENGINES_VERSION a engine_version
        ENGINE_PROPELLANT_1, -- Ya en buen formato
        ENGINE_PROPELLANT_2, -- Ya en buen formato
        ENGINE_THRUST_SEA_LEVEL_KN, -- Ya en buen formato
        ENGINE_THRUST_VACUUM_KN, -- Ya en buen formato

        LANDING_LEGS_NUMBER, -- Ya en buen formato
        LANDING_LEGS_MATERIAL, -- Ya en buen formato

        FIRST_STAGE_REUSABLE, -- Ya en buen formato (booleano o string)
        FIRST_STAGE_ENGINES, -- Ya en buen formato
        FIRST_STAGE_FUEL_AMOUNT_TONS, -- Ya en buen formato
        FIRST_STAGE_BURN_TIME_SEC, -- Ya en buen formato

        SECOND_STAGE_ENGINES, -- Ya en buen formato
        SECOND_STAGE_FUEL_AMOUNT_TONS, -- Ya en buen formato
        SECOND_STAGE_BURN_TIME_SEC, -- Ya en buen formato

        -- --- Columnas de Listas Convertidas a String Resumen en Python ---
        PAYLOAD_WEIGHTS_SUMMARY, -- Ya en buen formato
        FLICKR_IMAGE_URLS_LIST -- Ya en buen formato


    from source

)

select * from renamed