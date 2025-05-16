-- models/staging/stg_spacex_launches.sql

/**
  Modelo Staging para los datos raw de lanzamientos, aplanados en Python.
*/
with source as (

    -- Referencia la tabla raw que cargaste con el archivo spacex_launches_raw_flattened.csv.
    select * from {{ source('spacex', 'launches') }}

),

renamed as (

    select
        -- Clave Primaria
        -- Casteamos explícitamente al tipo que indicaste (Varchar)
        ID::varchar as launch_id,

        -- Información básica y booleanos
        FLIGHT_NUMBER::number as flight_number, -- Casteamos a Number
        NAME::varchar as launch_name, -- Renombra y cast a Varchar
        SUCCESS::boolean as is_successful, -- Renombra y cast a Boolean
        UPCOMING::boolean as is_upcoming, -- Renombra y cast a Boolean
        TBD::boolean as is_tbd, -- Renombra y cast a Boolean
        NET::boolean as is_net, -- Renombra y cast a Boolean
        AUTO_UPDATE::boolean as auto_update, -- Cast a Boolean

        -- Fechas y Horas
        DATE_UTC::date as launch_date_utc, -- Cast a Date
        STATIC_FIRE_DATE_UTC::timestamp_ntz as static_fire_date_utc, -- Cast a Timestamp_NTZ
        WINDOW::number as launch_window, -- Casteamos a Number

        -- IDs que referencian otras tablas (FKs lógicas)
        ROCKET::varchar as rocket_id, -- Renombra y cast a Varchar (FK a dim_rocket)
        LAUNCHPAD::varchar as launchpad_id, -- Renombra y cast a Varchar (FK a dim_launchpad)
        LAUNCH_LIBRARY_ID::varchar as launch_library_id, -- Cast a Varchar

        -- Descripción
        DETAILS::varchar as launch_details, -- Renombra y cast a Varchar

        -- --- Columnas de Links Aplanadas (Varchar) ---
        -- Casteamos explícitamente a Varchar y renombramos con prefijo 'link_'
        LINKS_PATCH_SMALL::varchar as link_patch_small,
        LINKS_PATCH_LARGE::varchar as link_patch_large,
        LINKS_REDDIT_CAMPAIGN::varchar as link_reddit_campaign,
        LINKS_REDDIT_LAUNCH::varchar as link_reddit_launch,
        LINKS_REDDIT_MEDIA::varchar as link_reddit_media,
        LINKS_FLICKR_SMALL::varchar as link_flickr_small,
        LINKS_FLICKR_ORIGINAL::varchar as link_flickr_original,
        LINKS_PRESSKIT::varchar as link_presskit,
        LINKS_WEBCAST::varchar as link_webcast,
        LINKS_YOUTUBE_ID::varchar as link_youtube_id,
        LINKS_ARTICLE::varchar as link_article,
        LINKS_WIKIPEDIA::varchar as link_wikipedia,

        -- --- Columnas de Listas Convertidas a String Resumen (Varchar) ---
        -- Casteamos explícitamente a Varchar y renombramos/mantenemos nombres descriptivos
        PAYLOAD_IDS::varchar as payload_ids_list, -- Renombra a payload_ids_list
        CORES_SUMMARY::varchar as cores_summary,
        CREW_SUMMARY::varchar as crew_summary,
        SHIP_IDS::varchar as ship_ids_list, -- Renombra a ship_ids_list
        CAPSULE_IDS::varchar as capsule_ids_list, -- Renombra a capsule_ids_list
        FAILURES_SUMMARY::varchar as failures_summary


    from source

)

select * from renamed