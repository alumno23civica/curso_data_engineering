-- models/staging/stg_spacex_cores.sql

/**
  Modelo Staging para los datos raw de cohetes de primera etapa (Cores).
  Selecciona, castea y renombra columnas de la tabla raw,
  basándose estrictamente en la estructura EXACTA de la tabla Snowflake proporcionada.
*/
with source as (

    -- Referencia la tabla raw de cores.
    -- Asegúrate que tu source 'spacex' en schema.yml apunte a esta tabla raw con la estructura mostrada.
    select * from {{ source('spacex', 'cores') }}

),

renamed as (

    select
        -- Clave Primaria
        ID::varchar as core_id, -- Renombra ID a core_id y cast a varchar

        -- Información básica del core
        SERIAL::varchar as core_serial, -- Renombra SERIAL a core_serial y cast a varchar
        BLOCK::number(38,1) as block, -- Cast a number(38,1) según la estructura real
        STATUS::varchar as status, -- Cast a varchar
        REUSE_COUNT::number(38,0) as reuse_count, -- Cast a number(38,0)

        -- Estadísticas de aterrizaje y uso
        RTLS_ATTEMPTS::number(38,0) as rtls_attempts, -- Cast a number(38,0)
        RTLS_LANDINGS::number(38,0) as rtls_landings, -- Cast a number(38,0)
        ASDS_ATTEMPTS::number(38,0) as asds_attempts, -- Cast a number(38,0)
        ASDS_LANDINGS::number(38,0) as asds_landings, -- Cast a number(38,0)

        -- Fecha de última actualización (VARCHAR en raw, casteamos a TIMESTAMP)
        -- Usar TRY_CAST o TRY_TO_TIMESTAMP para manejar el formato de la cadena de texto
        try_cast(LAST_UPDATE as timestamp_ntz) as last_update_utc, -- Renombra y cast a timestamp_ntz

        -- --- Columna de lista de IDs de lanzamientos convertida a string resumen (VARCHAR) ---
        LAUNCHES::varchar as launch_ids_list -- Renombra LAUNCHES a launch_ids_list y cast a varchar


    from source

)

select * from renamed