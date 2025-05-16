-- models/staging/stg_spacex_payloads.sql

/**
  Modelo Staging para los datos raw de cargas útiles (Payloads).
*/
with source as (

    -- Referencia la tabla raw de payloads.
    select * from {{ source('spacex', 'payloads') }}

),

renamed as (

    select
        -- Clave Primaria
        ID::varchar as payload_id, -- Renombra ID a payload_id

        -- Información básica de la carga útil
        NAME::varchar as payload_name, -- Renombra NAME a payload_name
        TYPE::varchar as payload_type, -- Renombra TYPE a payload_type
        REUSED::boolean as is_reused, -- Renombra REUSED a is_reused y cast a boolean

        -- ID que referencia a la tabla Launches (FK)
        -- Este es el ID del lanzamiento al que pertenece esta carga útil.
        LAUNCH::varchar as launch_id, -- Renombra LAUNCH a launch_id y cast a varchar (FK a dim_launches o fact_launches)

        -- Información orbital y de carga
        ORBIT::varchar as orbit, -- Cast a varchar
        REFERENCE_SYSTEM::varchar as reference_system, -- Cast a varchar
        REGIME::varchar as regime, -- Cast a varchar
        LONGITUDE::number(38,2) as longitude, -- Cast a number(38,2)
        INCLINATION_DEG::number(38,4) as inclination_deg, -- Cast a number(38,4)
        PERIOD_MIN::number(38,3) as period_min, -- Cast a number(38,3)
        LIFESPAN_YEARS::number(38,8) as lifespan_years, -- Cast a number(38,8)
        APOAPSIS_KM::number(38,3) as apoapsis_km, -- Cast a number(38,3)
        PERIApSIS_KM::number(38,3) as periapsis_km, -- Cast a number(38,3) -- Corregido: Typo en PERIASIS_KM
        MASS_KG::number(38,2) as mass_kg, -- Cast a number(38,2)
        MASS_LBS::number(38,3) as mass_lbs, -- Cast a number(38,3)

        -- --- Columnas extraídas de la estructura anidada 'dragon' ---
        DRAGON_CAPSULE_ID::varchar as dragon_capsule_id, -- Cast a varchar (Puede ser FK a dim_capsules si la creas)
        DRAGON_MASS_RETURNED_KG::number(38,1) as dragon_mass_returned_kg, -- Cast a number(38,1)
        DRAGON_MASS_RETURNED_LBS::number(38,1) as dragon_mass_returned_lbs, -- Cast a number(38,1)
        DRAGON_FLIGHT_TIME_SEC::number(38,1) as dragon_flight_time_sec, -- Cast a number(38,1)
        DRAGON_WATER_LANDING::boolean as dragon_water_landing, -- Cast a boolean
        DRAGON_LAND_LANDING::boolean as dragon_land_landing, -- Cast a boolean

        -- --- Columnas de listas convertidas a string resumen ---
        -- Casteamos explícitamente a Varchar
        CUSTOMERS_LIST::varchar as customers_list, -- Cast a varchar
        NORAD_IDS_LIST::varchar as norad_ids_list, -- Cast a varchar
        NATIONALITIES_LIST::varchar as nationalities_list, -- Cast a varchar
        MANUFACTURERS_LIST::varchar as manufacturers_list -- Cast a varchar

        -- Si hay alguna otra columna en tu tabla raw, inclúyela aquí y casteala.

    from source

)

select * from renamed