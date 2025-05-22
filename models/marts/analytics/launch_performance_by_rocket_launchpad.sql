-- models/marts/analytics/launch_performance_by_rocket_launchpad.sql

with launch_facts as (
    select
        launch_id,
        rocket_id,
        launchpad_id,
        launch_date_utc,
        case when is_successful = 1 then 1 else 0 end as is_successful_launch
    from {{ ref('fact_launches') }}
),

aggregated_performance as (
    select
        dr.rocket_name,
        dl.launchpad_name,
        dt.year_number, -- Usamos la dimensión de tiempo para agrupar por año
        count(lf.launch_id) as total_launches,
        sum(lf.is_successful_launch) as successful_launches,
        (cast(sum(lf.is_successful_launch) as number) / count(lf.launch_id)) * 100 as success_rate_pct
    from launch_facts lf
    join {{ ref('dim_rocket') }} dr on lf.rocket_id = dr.rocket_id
    join {{ ref('dim_launchpad') }} dl on lf.launchpad_id = dl.launchpad_id
    join {{ ref('dim_time') }} dt on lf.launch_date_utc = dt.date_day
    group by 1, 2, 3
    order by dt.year_number, dr.rocket_name, dl.launchpad_name
)

select * from aggregated_performance