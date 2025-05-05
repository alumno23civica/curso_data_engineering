-- models/staging/stg_promos.sql

SELECT
    promo_id,
    discount,
    status,
    is_active,
    at_synced
FROM {{ ref('base_sql_server_dbo__promos') }}
WHERE is_deleted IS DISTINCT FROM TRUE
