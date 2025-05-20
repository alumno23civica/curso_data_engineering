-- models/marts/dimensions/dim_payload.sql

/**
  Dimension table for SpaceX Payloads.
  Based on the staging data (stg_spacex_payloads).
  Includes descriptive attributes of each payload asset.
  Grain: One row per unique Payload.
  Primary Key: payload_sk (Surrogate Key generated using dbt_utils.generate_surrogate_key macro).
  Natural Key: payload_id (from API source).
  Related Fact: fact_payload_delivery (will join via payload_sk).
*/
with stg_payloads as (

    -- Selecciona desde el modelo staging de payloads
    -- *** EXCLUIR launch_id *** ya que es una FK a otro hecho/dimensión, no un atributo de la dimensión Payload.
    select
        payload_id,
        payload_name,
        payload_type,
        is_reused,
        -- launch_id, -- No incluir en la dimensión

        orbit,
        reference_system,
        regime,
        longitude,
        inclination_deg,
        period_min,
        lifespan_years,
        apoapsis_km,
        periapsis_km,
        mass_kg,
        mass_lbs,

        dragon_capsule_id,
        dragon_mass_returned_kg,
        dragon_mass_returned_lbs,
        dragon_flight_time_sec,
        dragon_water_landing,
        dragon_land_landing,

        customers_list,
        norad_ids_list,
        nation,
        manufacturers_list

    from {{ ref('stg_spacex__payloads') }}

),

-- Crea una fila de "Miembro Desconocido" con una clave subrogada determinística.
-- Esto es necesario para manejar FKs en fact_payload_delivery donde el payload_id
-- sea NULL o no exista en stg_payloads, o para payloads huérfanos si decides no crearlos.
unknown_member as (

    select
        -- Genera una clave subrogada única y consistente para el miembro desconocido del payload.
        -- Usa un string constante único y fijo ('unknown_payload').
        {{ dbt_utils.generate_surrogate_key(['\'unknown_payload\'']) }} as payload_sk,

        -- Atributos placeholders con tipos de dato que coincidan con las columnas seleccionadas de stg_payloads
        -- *** UTILIZA LA FUNCIÓN CAST() AQUÍ ***
        CAST('unknown_payload' AS varchar) as payload_id, -- Clave natural del miembro desconocido
        CAST('Unknown' AS varchar) as payload_name,
        CAST('Unknown' AS varchar) as payload_type,
        CAST(false AS boolean) as is_reused, -- Placeholder boolean

        CAST('Unknown' AS varchar) as orbit,
        CAST('Unknown' AS varchar) as reference_system,
        CAST('Unknown' AS varchar) as regime,
        CAST(-1 AS number) as longitude, -- Placeholder numérico
        CAST(-1 AS number) as inclination_deg,
        CAST(-1 AS number) as period_min,
        CAST(-1 AS number) as lifespan_years,
        CAST(-1 AS number) as apoapsis_km,
        CAST(-1 AS number) as periapsis_km,
        CAST(-1 AS number) as mass_kg,
        CAST(-1 AS number) as mass_lbs,

        CAST('Unknown' AS varchar) as dragon_capsule_id, -- Placeholder string
        CAST(-1 AS number) as dragon_mass_returned_kg, -- Placeholder numérico
        CAST(-1 AS number) as dragon_mass_returned_lbs,
        CAST(-1 AS number) as dragon_flight_time_sec,
        CAST(false AS boolean) as dragon_water_landing, -- Placeholder boolean
        CAST(false AS boolean) as dragon_land_landing,

        CAST('Unknown' AS varchar) as customers_list, -- Placeholder string para listas
        CAST('Unknown' AS varchar) as norad_ids_list,
        CAST('Unknown' AS varchar) as nation,
        CAST('Unknown' AS varchar) as manufacturers_list
        -- Asegúrate de NO poner coma después del último elemento de selección en este CTE

    -- Asegúrate de que los nombres de columna y tipos de dato de los placeholders coincidan
    -- EXACTAMENTE con las columnas seleccionadas en el CTE 'stg_payloads' y con el orden de la UNION ALL.
),

final as (

    select
        -- Genera la Clave Primaria SUBROGADA para cada Payload real.
        -- Usa la clave natural 'payload_id' del staging para generar el hash.
        {{ dbt_utils.generate_surrogate_key(['payload_id']) }} as payload_sk,

        -- Clave Natural (ID del origen) - Atributo descriptivo
        payload_id,

        -- Atributos Descriptivos del Payload (seleccionados de stg_payloads)
        payload_name,
        payload_type,
        is_reused,

        orbit,
        reference_system,
        regime,
        longitude,
        inclination_deg,
        period_min,
        lifespan_years,
        apoapsis_km,
        periapsis_km,
        mass_kg,
        mass_lbs,

        dragon_capsule_id,
        dragon_mass_returned_kg,
        dragon_mass_returned_lbs,
        dragon_flight_time_sec,
        dragon_water_landing,
        dragon_land_landing,

        customers_list,
        norad_ids_list,
        nation,
        manufacturers_list

    from stg_payloads

    -- UNION ALL para combinar los payloads reales con la fila del miembro desconocido.
    -- Asegúrate de que el SELECT del CTE unknown_member tiene EXACTAMENTE
    -- el mismo número y orden de columnas que el SELECT de stg_payloads en este CTE.
    UNION ALL
    select
        payload_sk, payload_id, payload_name, payload_type, is_reused,
        orbit, reference_system, regime, longitude, inclination_deg, period_min,
        lifespan_years, apoapsis_km, periapsis_km, mass_kg, mass_lbs,
        dragon_capsule_id, dragon_mass_returned_kg, dragon_mass_returned_lbs,
        dragon_flight_time_sec, dragon_water_landing, dragon_land_landing,
        customers_list, norad_ids_list, nation, manufacturers_list
    from unknown_member

)

-- Selección final
select * from final