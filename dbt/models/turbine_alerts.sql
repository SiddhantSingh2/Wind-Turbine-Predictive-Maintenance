{{ config(materialized='table') }}

WITH base_data AS (
    SELECT
        turbine_id,
        timestamp,
        bearing_temp,
        -- Window for tracking consecutive periods
        LAG(bearing_temp, 1) OVER (PARTITION BY turbine_id ORDER BY timestamp) AS prev_temp_1,
        LAG(bearing_temp, 2) OVER (PARTITION BY turbine_id ORDER BY timestamp) AS prev_temp_2,
        LAG(bearing_temp, 3) OVER (PARTITION BY turbine_id ORDER BY timestamp) AS prev_temp_3
    FROM {{ ref('silver_turbine_telemetry_edp') }} -- Adjust if needed
),

alerts AS (
    SELECT
        turbine_id,
        timestamp,
        bearing_temp,
        CASE
            WHEN bearing_temp > 85 
                 AND prev_temp_1 > 85 
                 AND prev_temp_2 > 85 
                 AND prev_temp_3 > 85 
            THEN 'Critical Overheating'
            ELSE 'Normal'
        END AS alert_status
    FROM base_data
)

SELECT
    *
FROM alerts
WHERE alert_status != 'Normal'
