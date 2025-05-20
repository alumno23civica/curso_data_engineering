-- models/marts/dimensions/dim_capsule.sql

/**
  Dimension table for SpaceX Capsules.
  Based on the staging data (stg_spacex_capsules).
  Includes descriptive attributes and total landing statistics for each capsule asset.
  Grain: One row per unique Capsule.
  Primary Key: capsule_sk (Surrogate Key).
  Natural Key: capsule_id (from API source).
  Related Fact: fact_launch_capsule_link (will join via capsule_sk to link launches and capsules).
*/
with stg_capsules as (

    -- Selecciona desde el modelo staging de cápsulas
    select * from {{ ref('stg_spacex__capsules') }}

),

-- Crea una fila de "Miembro Desconocido" con una clave subrogada determinística.
-- Esto es necesario si el capsule_id en los datos de origen (ej. en fact_launch_capsule_link)
-- es NULL o referencia un ID que no existe en stg_capsules.
unknown_member as (

    select
        -- Genera una clave subrogada única y consistente para el miembro desconocido.
        -- Usa un string constante que no colisione con SKs reales.
        {{ dbt_utils.generate_surrogate_key(['\'unknown_capsule\'']) }} as capsule_sk,


        -- Clave Natural (ID del origen) - Atributo placeholder
        'unknown_capsule'::varchar as capsule_id, --Usar un string único y descriptivo como ID natural para el desconocido

        -- Atributos placeholders con tipos de dato que coincidan con stg_capsules
        'Unknown'::varchar as capsule_serial,
        'Unknown'::varchar as capsule_status,
        'Unknown'::varchar as capsule_type, -- Incluir 'Unknown' como un posible tipo
        'Unknown'::varchar as capsule_last_update,

        -- Métricas placeholder (valores numéricos que indican 'no aplica' o desconocido)
        -1::number as total_reuse_count,
        -1::number as total_water_landings,
        -1::number as total_land_landings,

        'Unknown'::varchar as associated_launch_ids_list --Placeholder para la lista

    -- Asegúrate de que los tipos de dato de los placeholders coincidan EXACTAMENTE
    -- con los tipos de dato de las columnas en el CTE 'stg_capsules'.
    -- Revisa tu modelo stg_spacex_capsules.sql para confirmarlo.
),

final as (

    select
        -- Genera la Clave Primaria SUBROGADA para cada Cápsula real.
        -- Usa la clave natural 'capsule_id' del staging para generar el hash.
     
        -- para cada valor único de 'capsule_id'.
        {{ dbt_utils.generate_surrogate_key(['capsule_id']) }} as capsule_sk,


        -- Clave Natural (ID del origen) - Ahora es solo un atributo descriptivo en la dimensión
        capsule_id,

        -- Atributos Descriptivos de la Cápsula
        capsule_serial,
        capsule_status,
        capsule_type,
        capsule_last_update, -- Mantener como VARCHAR

        -- Métricas Acumuladas/Totales de la Cápsula (desde la API)
        total_reuse_count,
        total_water_landings,
        total_land_landings,

        -- Lista de IDs de Lanzamientos Asociados (como string aplanado)
        associated_launch_ids_list

    from stg_capsules

    -- UNION con la fila del miembro desconocido.
    -- Esto crea el conjunto final de filas para la dimensión, incluyendo el placeholder.
    UNION ALL
    select * from unknown_member

)

-- Selección final de todas las columnas.
-- Aquí podrías añadir lógica de SCD (Slowly Changing Dimensions) si fuera necesaria
-- para manejar cambios históricos en atributos como status, last_update, etc.
-- Para empezar, este es un modelo SCD Type 1 (si un atributo cambia, el valor se actualiza).
select * from final