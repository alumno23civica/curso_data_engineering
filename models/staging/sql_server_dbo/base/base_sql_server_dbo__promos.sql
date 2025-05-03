WITH source_data AS (
    SELECT 
        *
    FROM {{ source('sql_server_dbo', 'promos') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['PROMO_ID']) }} AS promo_id, -- Clave sustituta
    PROMO_ID as promo_name,
    DISCOUNT as discount,
    STATUS as status, -- Mantiene el estado original
    CASE 
        WHEN STATUS = 'active' THEN TRUE
        ELSE FALSE
    END AS is_active, -- Nueva columna booleana
    _FIVETRAN_DELETED as is_deleted,
    _FIVETRAN_SYNCED as at_synced
FROM source_data



