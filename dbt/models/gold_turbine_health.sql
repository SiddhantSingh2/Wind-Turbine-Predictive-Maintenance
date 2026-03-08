-- dbt/models/gold_turbine_health.sql

WITH silver_telemetry AS (
    SELECT * FROM {{ ref('silver_turbine_telemetry') }} -- Reference to silver layer
),

stats AS (
    SELECT
        turbine_id,
        AVG(avg_vibration_index_10m) AS mean_vibration,
        STDDEV(avg_vibration_index_10m) AS stddev_vibration
    FROM silver_telemetry
    GROUP BY 1
),

health_status AS (
    SELECT
        t.*,
        s.mean_vibration,
        s.stddev_vibration,
        CASE 
            WHEN t.avg_vibration_index_10m > (s.mean_vibration + (2 * s.stddev_vibration)) THEN TRUE
            ELSE FALSE
        END AS maintenance_required
    FROM silver_telemetry t
    LEFT JOIN stats s ON t.turbine_id = s.turbine_id
)

SELECT
    turbine_id,
    timestamp,
    avg_gearbox_temp_10m,
    avg_vibration_index_10m,
    maintenance_required,
    current_timestamp() AS updated_at
FROM health_status
