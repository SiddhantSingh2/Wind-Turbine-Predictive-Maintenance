# Wind Turbine Predictive Maintenance Pipeline

This project implements a scalable data pipeline for monitoring wind turbine health using IoT SCADA data, leveraging a **Medallion Architecture** to ensure data quality and reliability.

## Architecture Overview (Medallion Architecture)

The pipeline processes high-frequency IoT sensor data across three layers:

1.  **Bronze (Raw):**
    *   Ingests raw SCADA IoT JSON files from edge devices.
    *   Preserves full history with minimal transformations.
    *   Stored in its original raw format for auditing.

2.  **Silver (Cleaned & Augmented):**
    *   Performs data cleaning: missing sensor values (`wind_speed`, `power_output`) are handled using interpolation (Last Observation Carried Forward).
    *   Feature Engineering: Computes 10-minute rolling averages for `gearbox_temperature` and `vibration_index` to reduce noise and identify trends.
    *   Saved as **Delta Tables** to provide ACID transactions and schema enforcement.

3.  **Gold (Aggregated & Business-Ready):**
    *   Final analytical layer for reporting and alerting.
    *   Calculates health metrics using dbt.
    *   Flags `maintenance_required` status when vibration exceeds 2 standard deviations from the historical mean, enabling predictive maintenance.

## IoT Data Lifecycle

1.  **Ingestion:** SCADA sensors stream data into a cloud storage (landing zone).
2.  **Processing:** PySpark (Batch/Streaming) cleans and aggregates the data.
3.  **Modeling:** dbt applies business logic and statistical anomaly detection.
4.  **Orchestration:** Airflow (DAGs) schedules and monitors the end-to-end pipeline.
5.  **Consumption:** Downstream dashboards (Tableau/PowerBI) or ML models consume the Gold layer for predictive alerts.

## Project Structure

- `/data`: Local placeholder for raw and processed datasets.
- `/scripts`: PySpark transformation scripts for ETL.
- `/dbt`: SQL models for business logic and statistical profiling.
- `/airflow_dags`: Workflow orchestration definitions.
- `/notebooks`: Exploratory Data Analysis (EDA) and prototyping.
