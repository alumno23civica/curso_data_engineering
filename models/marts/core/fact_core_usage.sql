-- models/marts/fact_core_usage.sql

/**
  Tabla de Hechos para el uso individual de Cores en cada Lanzamiento.
  Captura eventos de uso de core y detalles de aterrizaje.
  Granularidad: Una fila por cada Core utilizado en un Lanzamiento específico.
  Primary Key: launch_id + core_id + core_flight_number_in_launch (Clave Natural Compuesta).
  Foreign Keys: Usando ID's naturales de las dimensiones.
  Basada en stg_spacex_core_usage, unida a stg_spacex_launches (para contexto de tiempo y degeneradas)
  y dimensiones (dim_launch, dim_core, dim_landpad) usando ID's Naturales.
  NOTA IMPORTANTE: Las claves foráneas (las columnas que terminan en _fk) pueden contener
        valores NULL si el ID de origen (ej. scu.core_id) es NULL o no encuentra
        un match en la tabla de dimensión correspondiente (ej. dim_core).
*/
with stg_core_usage as (

    -- Fuente principal: staging de uso de cores
    select * from {{ ref('int_spacex_core_usage_filtered') }}

),

stg_launches as (
    -- Necesitamos el staging de lanzamientos para obtener la fecha del lanzamiento (para unir a dim_time)
    -- y algunas dimensiones degeneradas del contexto del lanzamiento.
    select
        launch_id,
        launch_date_utc, -- Fecha para unir a dim_time
        flight_number, -- Dimensión degenerada del lanzamiento
        launch_name,   -- Dimensión degenerada del lanzamiento
        is_upcoming,   -- Dimensión degenerada del lanzamiento
        is_tbd         -- Dimensión degenerada del lanzamiento
        -- Incluye aquí cualquier otra dimensión degenerada de stg_launches que necesites en fact_core_usage
    from {{ ref('stg_spacex__launches') }}
),

dim_time as (
    -- Referencia a dim_time para obtener la clave time_id (que es el date_id natural basado en la fecha)
    select  date_day from {{ ref('dim_time') }}
),

dim_launch as (
    -- Referencia a dim_launch. Solo necesitamos el ID natural para la unión y la FK.
    -- Asumimos que dim_launch todavía tiene la columna launch_id.
    select launch_id from {{ ref('dim_launch') }}
),

dim_core as (
    -- Referencia a dim_core. Solo necesitamos el ID natural para la unión y la FK.
    select core_id from {{ ref('dim_core') }}
),

dim_landpad as (
    -- Referencia a dim_landpad. Solo necesitamos el ID natural para la unión y la FK.
    select landpad_id from {{ ref('dim_landpad') }}
),


final as (

    select
        -- Clave Primaria Compuesta de la Tabla de Hechos
        -- Identificadores naturales que definen la granularidad del evento de uso de core
        scu.launch_id,
        scu.core_id,
       -- scu.core_flight_number_in_launch,

        -- Claves Foráneas (Naturales) a las Dimensiones
        -- Usamos LEFT JOINs a las dimensiones. Si el ID de origen es NULL
        -- o si no encuentra un match en la dimensión, el valor de la FK será NULL.

        -- FK a dim_time: Une la fecha del lanzamiento (obtenida de stg_launches) a dim_time.
        -- time_id (date_id) es la PK natural en dim_time.
        dt.date_day as time_id,

        -- FK a dim_launch: Une usando el launch_id de stg_core_usage. Selecciona el ID natural de la dimensión.
        -- Renombramos para evitar conflicto de nombres con la PK 'launch_id'.
        dl.launch_id as launch_id_fk,

        -- FK a dim_core: Une usando el core_id de stg_core_usage. Selecciona el ID natural de la dimensión.
        -- Renombramos para evitar conflicto de nombres con la PK 'core_id'.
        dc.core_id as core_id_fk,

        -- FK a dim_landpad: Une usando el landpad_id_in_landing de stg_core_usage. Selecciona el ID natural de la dimensión.
        -- Renombramos para evitar conflicto de nombres con la columna de staging 'landpad_id_in_landing'.
        dlp.landpad_id as landpad_id_fk,


        -- Métricas del Evento de Uso de Core
        1 as count_of_core_usages, -- Métrica estándar: 1 por cada fila de hecho (evento de uso)
        scu.core_flight_number_in_launch, -- Métrica numérica (el número de vuelo de este core en esta misión)

        -- Indicadores booleanos de staging convertidos a métricas 0/1 (INTEGER) para permitir agregación (SUM, AVG)
        case when scu.is_landing_attempt_in_launch then 1 else 0 end as is_landing_attempt,
        case when scu.is_landing_successful_in_launch then 1 else 0 end as is_landing_successful,

        -- Dimensiones Degeneradas
        -- Atributos del evento de uso de core
        scu.landing_type_in_launch as landing_type,
        scu.has_gridfins_in_launch as has_gridfins,
        scu.has_legs_in_launch as has_legs,
        scu.is_reused_in_launch as is_reused_in_flight,

        -- Dimensiones degeneradas obtenidas del contexto del lanzamiento (via stg_launches)
        sl.flight_number, -- Número de vuelo general del lanzamiento asociado
        sl.launch_name,   -- Nombre del lanzamiento
        sl.is_upcoming,   -- Si el lanzamiento asociado es próximo
        sl.is_tbd         -- Si la fecha del lanzamiento asociado está TBD
        -- Incluye aquí otras degeneradas de sl que hayas seleccionado si las necesitas

    from stg_core_usage scu
    -- Unir a stg_launches para obtener la fecha del lanzamiento y degeneradas
    -- Usamos JOIN aquí asumiendo que cada uso de core *siempre* se asocia a un lanzamiento válido.
    JOIN stg_launches sl ON scu.launch_id = sl.launch_id
    -- Unir a dim_time usando la fecha del lanzamiento (obtenida de stg_launches). LEFT JOIN para seguridad.
    LEFT JOIN dim_time dt ON sl.launch_date_utc = dt.date_day
    -- Unir a dim_launch para obtener launch_id como FK. LEFT JOIN para no perder filas de hecho si el launch_id no existe en dim_launch.
    LEFT JOIN dim_launch dl ON scu.launch_id = dl.launch_id
    -- Unir a dim_core para obtener core_id como FK. LEFT JOIN para no perder filas de hecho si el core_id no existe en dim_core.
    LEFT JOIN dim_core dc ON scu.core_id = dc.core_id
    -- Unir a dim_landpad para obtener landpad_id como FK. LEFT JOIN para no perder filas de hecho si el landpad_id_in_landing no existe en dim_landpad.
    LEFT JOIN dim_landpad dlp ON scu.landpad_id_in_landing = dlp.landpad_id

)

-- Selección final de todas las columnas
select * from final