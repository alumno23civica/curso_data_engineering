-- models/marts/fact_payload_delivery.sql

/**
  Fact table recording the delivery of each payload during a launch.
  Grain: One row per Payload delivered in a Launch.
  Primary Key: payload_id (Natural Key).
  Foreign Keys: Links to dim_payload, dim_launch, dim_time, dim_rocket, dim_launchpad, dim_capsule.
  Measures: Mass, count, and Dragon-specific return metrics/flags.
  Degenerate Dimensions: Payload & Launch attributes.
  Based on stg_spacex_payloads, joined with stg_spacex_launches and dimensions.
  All FKs use COALESCE with the respective dimension's unknown member SK.
*/
with stg_payloads as (

    -- Select necessary columns from the payloads staging model
    select
        payload_id,
        launch_id, -- FK a launch (para unir a stg_launches y dim_launch)
        payload_type,
        is_reused,

        -- Atributos Orbitales y de Masa (pueden ser medidas o degeneradas)
        orbit, reference_system, regime, longitude, inclination_deg, period_min, lifespan_years, apoapsis_km, periapsis_km,
        mass_kg, mass_lbs, -- Estas son métricas clave

        -- Atributos específicos de Dragon (más bien métricas o banderas del resultado de la entrega/retorno)
        dragon_capsule_id, -- FK a capsule (condicional)
        dragon_mass_returned_kg, dragon_mass_returned_lbs, dragon_flight_time_sec, -- Métricas de retorno
        dragon_water_landing, dragon_land_landing, -- Banderas booleanas de aterrizaje (convertir a 0/1)

        -- Resúmenes de Listas (como degeneradas)
        customers_list, norad_ids_list, nationalities_list, manufacturers_list

    from {{ ref('stg_spacex_payloads') }}

),

stg_launches as (

    -- Select necessary launch context from staging launches for FKs and degenerates
    select
        launch_id, -- PK en staging, se une a sp.launch_id
        launch_date_utc, -- Para unir a dim_time
        rocket_id, -- FK a rocket
        launchpad_id, -- FK a launchpad
        flight_number, -- Degenerada
        launch_name -- Degenerada
        -- Incluir otras degeneradas del lanzamiento si son relevantes para este hecho (ej. is_upcoming, is_tbd)
        -- is_upcoming,
        -- is_tbd

    from {{ ref('stg_spacex_launches') }}
    -- Asumimos que cada payload en stg_payloads tiene un launch_id válido que existe en stg_launches
),

-- Referencias a todas las dimensiones relevantes
dim_time as (
    -- dim_time usa date_id como PK (que es el date natural)
    select date_day, date_id from {{ ref('dim_time') }}
),

dim_payload as (
    -- dim_payload usa payload_id natural para obtener payload_sk
    select payload_id, payload_sk from {{ ref('dim_payload') }}
),

dim_launch as (
    -- dim_launch usa launch_id natural para obtener launch_sk
    select launch_id, launch_sk from {{ ref('dim_launch') }}
),

dim_rocket as (
    -- dim_rocket usa rocket_id natural para obtener rocket_sk
    select rocket_id, rocket_sk from {{ ref('dim_rocket') }}
),

dim_launchpad as (
    -- dim_launchpad usa launchpad_id natural para obtener launchpad_sk
    select launchpad_id, launchpad_sk from {{ ref('dim_launchpad') }}
),

dim_capsule as (
    -- dim_capsule usa capsule_id natural para obtener capsule_sk
    -- Se unirá en capsule_id (clave natural)
    select capsule_id, capsule_sk from {{ ref('dim_capsule') }}
),

-- Obtener SKs para miembros desconocidos de las dimensiones para usarlos en COALESCE
-- Asumimos que las dimensiones tienen una fila de miembro desconocido con un ID natural conocido.
-- Reemplaza 'unknown_...' y '1900-01-01' con los IDs naturales reales que usaste para tus miembros desconocidos.
unknown_payload_sk as ( select payload_sk from {{ ref('dim_payload') }} where payload_id = 'unknown_payload' ),
unknown_launch_sk as ( select launch_sk from {{ ref('dim_launch') }} where launch_id = 'unknown_launch' ),
unknown_time_id as ( select date_id from {{ ref('dim_time') }} where date_day = '1900-01-01' ), -- ID natural (fecha) del miembro desconocido de tiempo
unknown_rocket_sk as ( select rocket_sk from {{ ref('dim_rocket') }} where rocket_id = 'unknown_rocket' ),
unknown_launchpad_sk as ( select launchpad_sk from {{ ref('dim_launchpad') }} where launchpad_id = 'unknown_launchpad' ),
unknown_capsule_sk as ( select capsule_sk from {{ ref('dim_capsule') }} where capsule_id = 'unknown_capsule' ),


final as (

    select
        -- Clave Primaria del Hecho: ID natural del payload (un payload es entregado una vez por lanzamiento)
        sp.payload_id,

        -- Claves Foráneas (Claves Subrogadas), usando COALESCE para manejar potentiales NULLs de LEFT JOINs.
        -- COALESCE mapea cualquier NULL resultante de la unión (si el ID natural de staging no encontró match)
        -- al SK del miembro desconocido de la dimensión correspondiente.
        coalesce(dp.payload_sk, ups.payload_sk) as payload_sk,
        coalesce(dl.launch_sk, uls.launch_sk) as launch_sk,
        coalesce(dt.date_id, uts.date_id) as time_id, -- time_id es la clave natural date_id
        coalesce(dr.rocket_sk, urs.rocket_sk) as rocket_sk,
        coalesce(dlp.launchpad_sk, ulps.launchpad_sk) as launchpad_sk,
        -- capsule_sk es condicional basada en dragon_capsule_id de staging. Usamos LEFT JOIN.
        coalesce(dc.capsule_sk, ucs.capsule_sk) as capsule_sk,


        -- Métricas: Cuantifican el evento de entrega del payload
        sp.mass_kg, -- Masa del Payload entregado en kilogramos (puede ser NULL en origen)
        sp.mass_lbs, -- Masa del Payload entregado en libras (puede ser NULL en origen)
        1 as count_of_payloads_delivered, -- Métrica simple de conteo de eventos (siempre 1 por fila de hecho)

        -- Métricas específicas de retorno de Dragon (aplican solo a Dragon payloads, pueden ser NULL en origen si no aplica o no hay data)
        sp.dragon_mass_returned_kg,
        sp.dragon_mass_returned_lbs,
        sp.dragon_flight_time_sec,
        -- Banderas booleanas de aterrizaje de Dragon convertidas a métricas 0/1.
        -- CASE WHEN maneja booleanos NULL del origen como FALSE, resultando en 0.
        case when sp.dragon_water_landing then 1 else 0 end as is_dragon_water_landing,
        case when sp.dragon_land_landing then 1 else 0 end as is_dragon_land_landing,


        -- Dimensiones Degeneradas: Atributos del Payload o Launch incluidos directamente en el hecho.
        -- Estos también existen en las dimensiones, pero pueden ser útiles para consultas rápidas sin unir.
        sp.payload_type, -- Tipo de payload entregado
        sp.is_reused, -- Si el payload mismo fue reutilizado

        -- Degeneradas Orbitales (desde data de payload)
        sp.orbit, sp.reference_system, sp.regime, sp.longitude, sp.inclination_deg, sp.period_min, sp.lifespan_years, sp.apoapsis_km, sp.periapsis_km,

        -- Degeneradas de Listas (desde data de payload, como strings aplanados)
        sp.customers_list, sp.norad_ids_list, sp.nationalities_list, sp.manufacturers_list,

        -- Degeneradas de Launch (desde data de launch)
        sl.flight_number, -- Número de vuelo del lanzamiento
        sl.launch_name -- Nombre del lanzamiento
        -- Incluir otras degeneradas del lanzamiento si son útiles (ej. is_upcoming, is_tbd)

    from stg_payloads sp
    -- Unir a stg_launches para obtener contexto del lanzamiento. Usamos INNER JOIN asumiendo que cada payload tiene un lanzamiento válido.
    INNER JOIN stg_launches sl ON sp.launch_id = sl.launch_id
    -- Unir a dim_time en la fecha del lanzamiento. Usar LEFT JOIN para seguridad.
    LEFT JOIN dim_time dt ON sl.launch_date_utc = dt.date_day
    -- Unir a dim_payload en payload_id para obtener payload_sk. Usar LEFT JOIN.
    LEFT JOIN dim_payload dp ON sp.payload_id = dp.payload_id
    -- Unir a dim_launch en launch_id para obtener launch_sk. Usar LEFT JOIN.
    LEFT JOIN dim_launch dl ON sp.launch_id = dl.launch_id
    -- Unir a dim_rocket en rocket_id (del lanzamiento) para obtener rocket_sk. Usar LEFT JOIN.
    LEFT JOIN dim_rocket dr ON sl.rocket_id = dr.rocket_id
    -- Unir a dim_launchpad en launchpad_id (del lanzamiento) para obtener launchpad_sk. Usar LEFT JOIN.
    LEFT JOIN dim_launchpad dlp ON sl.launchpad_id = dlp.launchpad_id
    -- Unir a dim_capsule en dragon_capsule_id para obtener capsule_sk. Usar LEFT JOIN ya que solo los payloads Dragon tienen este ID.
    LEFT JOIN dim_capsule dc ON sp.dragon_capsule_id = dc.capsule_id

    -- Cross join con los CTEs de miembros desconocidos para tener sus SKs disponibles para COALESCE
    cross join unknown_payload_sk ups
    cross join unknown_launch_sk uls
    cross join unknown_time_id uts
    cross join unknown_rocket_sk urs
    cross join unknown_launchpad_sk ulps
    cross join unknown_capsule_sk ucs

)

-- Selección final de todas las columnas para la tabla de hechos.
select * from final