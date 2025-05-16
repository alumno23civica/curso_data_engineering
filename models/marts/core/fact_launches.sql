-- models/marts/fact_launch_event.sql

/**
  Tabla de Hechos para los Eventos de Lanzamiento.
  Granularidad: Una fila por cada Lanzamiento individual.
  Contiene las métricas clave del lanzamiento y claves foráneas a las dimensiones básicas solicitadas.
*/
with stg_launches as (

    -- Selecciona desde el modelo staging de lanzamientos aplanado
    select * from {{ ref('stg_spacex__launches') }}

),

-- Selecciona las claves primarias de las dimensiones necesarias para la unión
-- (Asumimos que dim_time, dim_rocket, dim_launchpad ya están construidas o se construirán antes)
dim_time as (
    -- Necesitamos la clave primaria de la dimensión tiempo y la columna de fecha
    -- Asumimos que dim_time tiene una PK 'date_id' y una columna de tipo DATE (ej. date_day)
    select date_day from {{ ref('dim_time') }}
),

dim_rocket as (
    -- Solo necesitamos la clave primaria para la unión
    select rocket_id from {{ ref('dim_rocket') }}
),

dim_launchpad as (
    -- Solo necesitamos la clave primaria para la unión
    select launchpad_id from {{ ref('dim_launchpad') }}
)

select
    -- Clave Primaria de la Tabla de Hechos
    -- Usamos el ID de lanzamiento original como PK, es único por lanzamiento.
    stg_launches.launch_id,

    -- Claves Foráneas a las Dimensiones
    -- Unimos la fecha del lanzamiento a la dimensión tiempo para obtener la clave de tiempo.
   -- dt.date_day , -- Clave foránea a dim_time (¡IMPRESCINDIBLE para análisis temporal!)
    dr.rocket_id,         -- Clave foránea a dim_rocket
    dl.launchpad_id,      -- Clave foránea a dim_launchpad

    -- Métricas del Lanzamiento
    -- Casteamos el booleano SUCCESS a INTEGER (1 para verdadero, 0 para falso) para facilitar agregaciones (SUM, AVG)
    case when stg_launches.is_successful then 1 else 0 end as is_successful,
    1 as count_of_launches, -- Métrica estándar: 1 por cada fila para contar el número de eventos

    -- Dimensiones Degeneradas (atributos del evento que no van en tablas de dimensión separadas)
    -- Incluimos los campos solicitados que no son FKs o métricas directas.
    stg_launches.flight_number,
    stg_launches.launch_name,
    stg_launches.is_upcoming,
    stg_launches.is_tbd,
    stg_launches.launch_window, -- Si es un número, puede considerarse métrica o degenerada
    stg_launches.static_fire_date_utc -- Fecha/hora del encendido estático (Timestamp)

    -- NOTA sobre launch_date_utc, launch_year, launch_month, launch_day:
    -- launch_date_utc ya se usa para unir a dim_time.
    -- launch_year, launch_month, launch_day son atributos de dim_time.
    -- NO necesitas incluirlos aquí si ya tienes la FK time_id.
    -- Se acceden uniéndote a dim_time en tus consultas/reportes.
    -- Puedes incluir launch_date_utc (Timestamp) aquí si necesitas la hora exacta en el hecho.
    ,stg_launches.launch_date_utc -- Incluimos la fecha/hora UTC original

    -- Campos de stg_launches que NO has solicitado, pero que podrías considerar como Dimensiones Degeneradas adicionales:
    -- stg_launches.is_net,
    -- stg_launches.auto_update,
    -- stg_launches.launch_library_id,
    -- stg_launches.launch_details, -- Considera si este texto largo es útil directamente en la fact
    -- Los links (links_*) y los strings resumen/json (cores_summary, payload_ids_list, failures_json) generalmente NO van aquí.


from stg_launches
-- Realizamos JOINs con las dimensiones para obtener sus claves primarias (que serán FKs aquí)
-- Usamos JOIN para asegurar que cada lanzamiento tenga dimensiones válidas. Si un ID no existe
-- en la dimensión, esa fila de hecho no se incluye (o usas LEFT JOIN + Miembro Desconocido).
JOIN dim_time dt ON stg_launches.launch_date_utc::date = dt.date_day -- Unir fecha del lanzamiento a la columna de fecha de dim_time
LEFT JOIN dim_rocket dr ON stg_launches.rocket_id = dr.rocket_id
LEFT JOIN dim_launchpad dl ON stg_launches.launchpad_id = dl.launchpad_id

-- Opcional: Puedes añadir filtros si no quieres todos los lanzamientos en la tabla de hechos
-- where stg_launches.is_successful is true -- Ejemplo: solo lanzar exitosos como hechos