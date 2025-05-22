-- models/staging/stg_spacex_core_usage.sql

/**
  Modelo Staging para los datos raw del uso de Cores por Lanzamiento.
  Estos datos provienen del array 'cores' dentro del endpoint de Launches,
  aplanado a un CSV separado por el script Python.
  Selecciona, castea y renombra columnas de la tabla raw.
*/
with source as (

    -- Referencia la tabla raw de uso de cores.
    select * from {{ source('spacex', 'core_usage') }}

),

renamed as (

    select
        -- Claves de identificación
        -- Usamos CAST explícito según los tipos esperados
        LAUNCH_ID::varchar as launch_id, -- ID del lanzamiento (FK a fact_launch_event)
        CORE_ID::varchar as core_id,     -- ID del core (FK a dim_core)
        -- Nota: En la tabla de hechos fact_core_usage, la combinación launch_id + core_id
        --       probablemente formará parte de la clave primaria compuesta o se usará una clave subrogada.

        -- Atributos y métricas específicos de este uso del core en este lanzamiento
        CORE_FLIGHT_NUMBER_IN_LAUNCH::number as core_flight_number_in_launch, -- Número de vuelo de este core en esta misión (NUMBER)

        -- Indicadores booleanos específicos de este vuelo (usar try_cast por seguridad, ya que pueden venir como 'True'/'False' o nulos)
        try_cast(HAS_GRIDFINS_IN_LAUNCH as boolean) as has_gridfins_in_launch,
        try_cast(HAS_LEGS_IN_LAUNCH as boolean) as has_legs_in_launch,
        try_cast(IS_REUSED_IN_LAUNCH as boolean) as is_reused_in_launch,
        try_cast(IS_LANDING_ATTEMPT_IN_LAUNCH as boolean) as is_landing_attempt_in_launch,
        try_cast(IS_LANDING_SUCCESSFUL_IN_LAUNCH as boolean) as is_landing_successful_in_launch,

        -- Información de aterrizaje específica de este vuelo
        LANDING_TYPE_IN_LAUNCH::varchar as landing_type_in_launch, -- Tipo de aterrizaje (VARCHAR)
        LANDPAD_ID_IN_LANDING::varchar as landpad_id_in_landing, -- ID del landpad usado (FK a dim_landpad)



    from source

)

select * from renamed