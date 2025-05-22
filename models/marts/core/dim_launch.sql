-- models/marts/dimensions/dim_launch.sql

/**
  Dimension table for SpaceX Launches as entities.
  Based on the staging data (stg_spacex_launches).
  Includes descriptive attributes of the launch event itself.
  Grain: One row per unique Launch event.
  Primary Key: launch_sk (Surrogate Key).
  Natural Key: launch_id (from API source).
  Related Facts: fact_launch_event (proporciona contexto, aunque muchos atributos están en la dim),
                 fact_payload_delivery (se une via launch_sk),
                 fact_launch_capsule_link (se une via launch_sk).
*/
with stg_launches as (

    -- Selecciona todas las columnas del modelo staging de lanzamientos
    select * from {{ ref('stg_spacex__launches') }}

),

-- Crea una fila de "Miembro Desconocido" para la dimensión de Lanzamiento.
-- Esto es crucial para manejar FKs en otras tablas de hechos (como fact_payload_delivery
-- para payloads huérfanos) donde el launch_id sea NULL o no encuentre match.
unknown_member as (

    select
        -- Genera una clave subrogada única y consistente para el miembro desconocido.
        -- Usamos un string constante ('unknown_launch') para asegurar que siempre genera la misma clave.
        -- Esta línea con la macro es a menudo la que puede interactuar sutilmente con el parser posterior.

        -- Intenta poner las selecciones de literales en menos líneas.
        -- Asegúrate de la coma después de cada elemento (excepto el último).
         {{ dbt_utils.generate_surrogate_key(['\'unknown_launch\'']) }} as launch_sk,
        -- Clave Natural (ID del origen) - Atributo placeholder
        'unknown_launch'::varchar as launch_id,

        -- Atributos placeholders básicos
        -1::number as flight_number, 'Unknown'::varchar as launch_name,

        -- Placeholders booleanos
        false::boolean as is_upcoming, true::boolean as is_tbd, false::boolean as is_net, false::boolean as auto_update,

        -- Placeholders de Fecha/Hora/Número
        '1900-01-01'::date as launch_date_utc, '1900-01-01'::timestamp_ntz as static_fire_date_utc, -1::number as launch_window,

        -- Placeholders de IDs/Detalles
        'Unknown'::varchar as launch_library_id, 'Unknown'::varchar as launch_details,

        -- Placeholders de Links
        'Unknown'::varchar as link_patch_small, 'Unknown'::varchar as link_patch_large, 'Unknown'::varchar as link_reddit_campaign,
        'Unknown'::varchar as link_reddit_launch, 'Unknown'::varchar as link_reddit_media, 'Unknown'::varchar as link_flickr_small,
        'Unknown'::varchar as link_flickr_original, 'Unknown'::varchar as link_presskit, 'Unknown'::varchar as link_webcast,
        'Unknown'::varchar as link_youtube_id, 'Unknown'::varchar as link_article, 'Unknown'::varchar as link_wikipedia,

        -- Placeholders de Listas/Summaries
        'Unknown'::varchar as payload_ids_list, 'Unknown'::varchar as cores_summary, 'Unknown'::varchar as crew_summary,
        'Unknown'::varchar as ship_ids_list, 'Unknown'::varchar as capsule_ids_list, 'Unknown'::varchar as failures_summary
        -- NO poner coma después del último elemento


    -- No se necesita cláusula FROM cuando seleccionas solo literales en un CTE
    -- ASEGÚRATE de que los nombres de columna y tipos de dato de los placeholders
    -- coinciden EXACTAMENTE con las columnas seleccionadas de stg_launches.
    -- Revisa tu modelo stg_spacex_launches.sql para confirmarlo.
),

final as (

    select
        -- Genera la Clave Primaria SUBROGADA para cada Lanzamiento real.
        -- Usa la clave natural 'launch_id' del staging para generar el hash.
        {{ dbt_utils.generate_surrogate_key(['launch_id']) }} as launch_sk,
        -- Clave Natural (ID del origen) - Atributo descriptivo
        launch_id,
        -- Atributos Descriptivos del Lanzamiento (seleccionados de stg_launches)
        flight_number,
        launch_name,
        is_upcoming,
        is_tbd,
        is_net,
        auto_update,
        launch_date_utc, -- Incluir la fecha/hora UTC completa como atributo DATE
        static_fire_date_utc, -- Incluir timestamp_ntz
        launch_window, -- Incluir número
        launch_library_id,
        launch_details, -- Incluir string

        -- Atributos de Links (URLs como VARCHAR)
        link_patch_small,
        link_patch_large,
        link_reddit_campaign,
        link_reddit_launch,
        link_reddit_media,
        link_flickr_small,
        link_flickr_original,
        link_presskit,
        link_webcast,
        link_youtube_id,
        link_article,
        link_wikipedia,

        -- Atributos de Listas Aplanadas/Summaries (como string)
        payload_ids_list,
        cores_summary,
        crew_summary,
        ship_ids_list,
        capsule_ids_list,
        failures_summary

    from stg_launches

    -- UNION con la fila del miembro desconocido.
    UNION ALL
    select * from unknown_member

)

-- Selección final
-- Puedes añadir lógica de SCD si es necesario (ej. para auto_update, is_tbd, detalles si cambian)
select * from final