-- models/staging/spacex/stg_spacex_capsules.sql

/**
  Staging model for SpaceX Capsules data.
  Selects, casts, and renames raw capsule data from the source using CTEs.
  Grain: One row per capsule.
  Source: Raw 'capsules' table (from API endpoint /v4/capsules).
*/
with source as (

    -- Referencia la tabla raw de uso de cores.
    -- Asegúrate que tu source 'spacex' en schema.yml apunte a esta nueva tabla raw (ej. name: core_usage).
    select * from {{ source('spacex', 'capsules') }}

),

casted_and_renamed as (

    -- 2. Castear tipos de dato y renombrar columnas
    select
        -- Clave Primaria del Staging (usando el ID de origen)
        -- Casteamos el ID a VARCHAR (asumimos que los IDs de la API son strings)
        id::varchar as capsule_id,

        -- Atributos Descriptivos (Casteados a VARCHAR si no lo son ya)
        serial::varchar as capsule_serial,
        status::varchar as capsule_status,
        type::varchar as capsule_type, -- Debería ser 'Dragon 1.0', 'Dragon 1.1', 'Dragon 2.0'
        last_update::varchar as capsule_last_update, -- Mantener como VARCHAR si es texto

        -- Métricas Totales para la Cápsula (Casteadas a NUMBER/INTEGER)
        -- Asegúrate de que reuse_count, water_landings, land_landings son números en tu fuente raw
        reuse_count::number as total_reuse_count,
        water_landings::number as total_water_landings,
        land_landings::number as total_land_landings,

        -- Lista de IDs de Lanzamientos Asociados (Casteada a VARCHAR como string)
        -- Asegúrate de que launch_ids_list es VARCHAR en tu fuente raw
        launch_ids_list::varchar as associated_launch_ids_list -- Esto es un string coma-separado


    from source -- Seleccionamos del CTE 'source_data' anterior

),

final as (

    -- 3. Seleccionar todas las columnas del CTE 'casted_and_renamed'
    -- Este es el CTE final antes de la selección que creará la tabla
    select * from casted_and_renamed

)

-- Selección final que construye la tabla de staging
select * from final