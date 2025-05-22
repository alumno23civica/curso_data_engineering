-- models/marts/analytics/summary_customer_payload_analysis.sql
SELECT
        dp.payload_type,
        COUNT(fpd.payload_sk) AS total_payloads_of_type,
        SUM(fpd.mass_kg) AS total_mass_of_type_kg
    FROM {{ ref('fact_payload_delivery') }} fpd
    JOIN {{ ref('dim_payload') }} dp ON fpd.payload_sk = dp.payload_sk
    GROUP BY 1
    ORDER BY total_payloads_of_type DESC

