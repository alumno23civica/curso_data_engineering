-- models/marts/fact_payload_delivery.sql

with stg_payloads as (

    select
        payload_id,
        launch_id,
        payload_type,
        is_reused,
        orbit, reference_system, regime, longitude, inclination_deg, period_min, lifespan_years, apoapsis_km, periapsis_km,
        mass_kg, mass_lbs,
        dragon_capsule_id,
        dragon_mass_returned_kg, dragon_mass_returned_lbs, dragon_flight_time_sec,
        dragon_water_landing, dragon_land_landing,
        customers_list, norad_ids_list, nation, manufacturers_list

    from {{ ref('stg_spacex__payloads') }}

),

stg_launches as (

    select
        launch_id,
        launch_date_utc,
        rocket_id,
        launchpad_id,
        flight_number,
        launch_name

    from {{ ref('stg_spacex__launches') }}

)

select
    sp.payload_id,

    -- Surrogate Keys
    coalesce(dp.payload_sk,{{ dbt_utils.generate_surrogate_key(['\'unknown_payload\'']) }}) as payload_sk,
    coalesce(dl.launch_sk, {{ dbt_utils.generate_surrogate_key(['\'unknown_launch\'']) }}) as launch_sk,
    coalesce(dt.date_day, '2000-01-01'::date) as date_id,
    coalesce(dr.rocket_sk, {{ dbt_utils.generate_surrogate_key(['\'unknown_rocket\'']) }}) as rocket_sk,
    coalesce(dlp.launchpad_sk,{{ dbt_utils.generate_surrogate_key(['\'unknown_launchpad\'']) }}) as launchpad_sk,
    coalesce(dc.capsule_sk, {{ dbt_utils.generate_surrogate_key(['\'unknown_capsule\'']) }}) as capsule_sk,

    -- Métricas
    sp.mass_kg,
    sp.mass_lbs,
    1 as count_of_payloads_delivered,
    sp.dragon_mass_returned_kg,
    sp.dragon_mass_returned_lbs,
    sp.dragon_flight_time_sec,
    case when sp.dragon_water_landing then 1 else 0 end as is_dragon_water_landing,
    case when sp.dragon_land_landing then 1 else 0 end as is_dragon_land_landing,

    -- Degeneradas del Payload
    sp.payload_type,
    sp.is_reused,
    sp.orbit, sp.reference_system, sp.regime, sp.longitude, sp.inclination_deg, sp.period_min, sp.lifespan_years,
    sp.apoapsis_km, sp.periapsis_km,
    sp.customers_list, sp.norad_ids_list, sp.nation, sp.manufacturers_list,

    -- Degeneradas del Launch
    sl.flight_number,
    sl.launch_name

from stg_payloads sp

left join stg_launches sl on sp.launch_id = sl.launch_id
left join {{ ref('dim_time') }} dt on sl.launch_date_utc = dt.date_day
left join {{ ref('dim_payload') }} dp on sp.payload_id = dp.payload_id
left join {{ ref('dim_launch') }} dl on sp.launch_id = dl.launch_id
left join {{ ref('dim_rocket') }} dr on sl.rocket_id = dr.rocket_id
left join {{ ref('dim_launchpad') }} dlp on sl.launchpad_id = dlp.launchpad_id
left join {{ ref('dim_capsule') }} dc on sp.dragon_capsule_id = dc.capsule_id



