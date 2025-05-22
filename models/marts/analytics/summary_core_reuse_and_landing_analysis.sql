select
    dlp.landpad_id,
    dlp.landpad_name,
    dlp.landpad_locality,
    dlp.landpad_region,
    count(fc.core_id) as total_landings,
    sum(fc.is_landing_successful) as successful_landings,
    round(100.0 * sum(fc.is_landing_successful) / nullif(count(fc.core_id), 0), 2) as success_rate_pct
from {{ ref('fact_core_usage') }} fc
left join {{ ref('dim_landpad') }} dlp on fc.landpad_id_fk = dlp.landpad_id
where fc.is_landing_attempt = 1
group by dlp.landpad_id, dlp.landpad_name, dlp.landpad_locality, dlp.landpad_region
order by success_rate_pct desc
