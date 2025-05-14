with source as (

    select * from {{ source('spacex', 'rockets') }}

),

renamed as (

    select
        height,
        diameter,
        mass,
        first_stage,
        second_stage,
        engines,
        landing_legs,
        payload_weights,
        flickr_images,
        name,
        type,
        active,
        stages,
        boosters,
        cost_per_launch,
        success_rate_pct,
        first_flight,
        country,
        company,
        wikipedia,
        description,
        id

    from source

)

select * from renamed