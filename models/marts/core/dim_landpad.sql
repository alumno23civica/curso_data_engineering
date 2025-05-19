-- models/marts/dimensions/dim_landpad.sql

/**
  Dimension table for SpaceX Landing Pads (Landpads).
  Based on the staging data (stg_spacex_landpads).
  Includes descriptive attributes and total landing statistics for each landpad asset.
  Grain: One row per unique Landpad.
  Primary Key: landpad_sk (Surrogate Key generated using dbt_utils.generate_surrogate_key macro).
  Natural Key: landpad_id (from API source).
  Related Fact: fact_core_usage (joined via landpad_sk).
*/
with stg_landpads as (

    -- Selecciona desde el modelo staging de landpads
    select * from {{ ref('stg_spacex__landpads') }}

),

-- Crea una fila de "Miembro Desconocido" con una clave subrogada determinística.
-- Esto es necesario si el landpad_id en la tabla de hechos fact_core_usage (o cualquier otra
-- tabla de hechos que se una a dim_landpad) es NULL o referencia un ID que no existe en stg_landpads.
unknown_member as (

    select
        -- Genera una clave subrogada única y consistente para el miembro desconocido del landpad.
        -- *** USA dbt_utils.generate_surrogate_key macro aquí: ***
        -- Usamos un string constante único y fijo ('unknown_landpad') para asegurar
        -- que siempre genera la misma clave hash, independientemente de la ejecución.
        -- Asegúrate que esta línea termina con una coma si NO es la última columna:
        {{ dbt_utils.generate_surrogate_key(['\'unknown_landpad\'']) }} as landpad_sk,

        -- Clave Natural (ID del origen) - Atributo placeholder para el miembro desconocido
        -- Revisa la sintaxis en estas líneas específicas mencionadas en el error:
        'unknown_landpad'::varchar as landpad_id, -- Error apunta aquí a veces
        -1::number as flight_number_placeholder, -- Campo extra para evitar conflicto si flight_number se usa en unknown_member CTE, aunque no deberia
                                                -- Parece que tus errores anteriores mencionaban -1::number
        'Unknown'::varchar as landpad_name, -- Error apunta aquí a veces
        'Unknown'::varchar as landpad_full_name, -- Error apunta aquí a veces
        'Unknown'::varchar as landpad_status, -- Error apunta aquí a veces
        'Unknown'::varchar as landpad_type, -- Error apunta aquí a veces
        'Unknown'::varchar as landpad_locality, -- Error apunta aquí a veces
        'Unknown'::varchar as landpad_region, -- Error apunta aquí a veces
        'Unknown'::varchar as landpad_wikipedia, -- Error apunta aquí a veces
        'Unknown'::varchar as landpad_details, -- Error apunta aquí a veces
        

        -- Métricas placeholder (valores numéricos que indican 'no aplica' o desconocido)
        -1::number as total_landing_attempts, -- Error apunta aquí a veces
        -1::number as total_landing_successes, -- Error apunta aquí a veces

        'Unknown'::varchar as associated_launch_ids_list -- Error apunta aquí a veces
        -- Asegúrate de NO poner coma después de la última columna seleccionada aquí.


    -- Asegúrate de que los nombres de columna y tipos de dato de los placeholders coincidan
    -- con las columnas seleccionadas de stg_landpads para la UNION ALL posterior.
),

final as (

    select
        -- Genera la Clave Primaria SUBROGADA para cada Landpad real.
        -- Usa la clave natural 'landpad_id' del staging para generar el hash.
        -- *** USA dbt_utils.generate_surrogate_key macro aquí: ***
        {{ dbt_utils.generate_surrogate_key(['landpad_id']) }} as landpad_sk,

        -- Clave Natural (ID del origen) - Ahora es solo un atributo descriptivo en la dimensión
        landpad_id,

        -- Atributos Descriptivos del Landpad (Seleccionar directamente desde stg_landpads)
        landpad_name,
        landpad_full_name,
        landpad_status,
        landpad_type,
        landpad_locality,
        landpad_region,
        landpad_wikipedia,
        landpad_details,
       

        -- Métricas Acumuladas/Totales del Landpad (desde la API)
        total_landing_attempts,
        total_landing_successes,

        -- Lista de IDs de Lanzamientos Asociados (como string aplanado)
        associated_launch_ids_list

    from stg_landpads

    -- UNION ALL para combinar los landpads reales con la fila del miembro desconocido.
    -- La cantidad y el orden de las columnas Y SUS TIPOS deben coincidir EXACTAMENTE
    -- entre el SELECT * from stg_landpads (implicitamente seleccionando todas las columnas
    -- de stg_landpads en el CTE final) y el SELECT del CTE unknown_member.
    UNION ALL
    select
        landpad_sk, -- SK del miembro desconocido
        landpad_id, -- ID natural del miembro desconocido
        landpad_name, landpad_full_name, landpad_status, landpad_type,
        landpad_locality, landpad_region, landpad_wikipedia, landpad_details,
        total_landing_attempts, total_landing_successes,
        associated_launch_ids_list
        -- Seleccionar explícitamente todas las columnas del CTE unknown_member
        -- Esto es más seguro que SELECT * si los CTEs tienen un orden de columnas diferente
    from unknown_member

)

-- Selección final.
select * from final