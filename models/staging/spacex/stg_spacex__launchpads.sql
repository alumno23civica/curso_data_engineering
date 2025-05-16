-- models/staging/stg_spacex_launchpads.sql

/**
  Modelo Staging para los datos raw de plataformas de lanzamiento (Launchpads).
  Selecciona, castea y renombrado columnas de la tabla raw
*/
with source as (

    -- Referencia la tabla raw de launchpads.
    select * from {{ source('spacex', 'launchpads') }}

),

renamed as (

    select
        -- Clave Primaria
        ID::varchar as launchpad_id, -- Renombra ID a launchpad_id y cast a varchar

        -- Información básica de la plataforma
        NAME::varchar as launchpad_name, -- Renombra NAME a launchpad_name y cast a varchar
        FULL_NAME::varchar as launchpad_full_name, -- Renombra FULL_NAME a launchpad_full_name y cast a varchar
        LOCALITY::varchar as locality, -- Cast a varchar
        REGION::varchar as region, -- Cast a varchar
        TIMEZONE::varchar as timezone, -- Cast a varchar
        STATUS::varchar as status, -- Cast a varchar
        DETAILS::varchar as launchpad_details, -- Renombra DETAILS a launchpad_details y cast a varchar

        -- Coordenadas
        LATITUDE::number(38,7) as latitude, -- Cast a number(38,7)
        LONGITUDE::number(38,7) as longitude, -- Cast a number(38,7)

        -- Estadísticas de lanzamiento
        LAUNCH_ATTEMPTS::number(38,0) as launch_attempts, -- Cast a number(38,0)
        LAUNCH_SUCCESSES::number(38,0) as launch_successes, -- Cast a number(38,0)

        -- --- Columnas de listas/imágenes aplanadas a string (Varchar) ---
        -- Casteamos explícitamente a Varchar.
        ROCKET_IDS_LIST::varchar as rocket_ids_list, -- Lista de IDs de cohetes (string)
        LAUNCH_IDS_LIST::varchar as launch_ids_list, -- Lista de IDs de lanzamientos (string)
        IMAGE_URL_LARGE_FIRST::varchar as image_url_large_first -- URL de la primera imagen grande (string)

        -- Si hay alguna otra columna en tu tabla raw, inclúyela aquí y casteala.

    from source

)

select * from renamed